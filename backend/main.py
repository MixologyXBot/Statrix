# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

from contextlib import asynccontextmanager
from html import escape as html_escape
from fastapi import FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
import logging
import os

from .config import settings
from .database import db
from .cache import CacheUnavailableError
from .auth import get_password_hash
from .dependencies import get_current_admin
from .background.monitor_loop import start_monitor_loop, stop_monitor_loop

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}

NO_CACHE_EXTENSIONS = (".css", ".js", ".map", ".json", ".html")


class NoCacheStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        if response.status_code == 200 and path.endswith(NO_CACHE_EXTENSIONS):
            response.headers.update(NO_CACHE_HEADERS)
        return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Statrix...")
    try:
        db.init_cache_service()
    except Exception as exc:
        logger.exception("Cache service initialization failed")
        raise RuntimeError("Cache service initialization failed") from exc
    await db.connect()
    logger.info("Database connected")

    await db.create_tables()
    logger.info("Database tables created/verified")

    if db.cache_service:
        try:
            await db.cache_service.connect()
            logger.info("Cache backend connected: %s", db.cache_backend_name)
        except Exception as exc:
            logger.exception("Failed to connect cache backend; aborting startup")
            raise RuntimeError("Failed to connect cache backend during startup") from exc

    web_concurrency = 1
    raw_web_concurrency = (os.getenv("WEB_CONCURRENCY") or "").strip()
    if raw_web_concurrency:
        try:
            web_concurrency = int(raw_web_concurrency)
        except ValueError:
            logger.warning("Invalid WEB_CONCURRENCY=%r; defaulting to 1", raw_web_concurrency)

    cache_requested = settings.ENABLE_IN_MEMORY_CACHE or settings.CACHE_ONLY or settings.CACHE_BACKEND == "redis"
    if cache_requested and web_concurrency > 1 and not settings.MONITOR_LEADER_LOCK_ENABLED:
        logger.warning(
            "WEB_CONCURRENCY=%s with leader lock disabled may cause stale monitor data. "
            "Recommended: enable MONITOR_LEADER_LOCK_ENABLED=true.",
            web_concurrency,
        )

    if cache_requested:
        try:
            await db.ensure_cache_available()
            await db.load_cache()
            logger.info(
                "Cache loaded (backend=%s, ENABLE_IN_MEMORY_CACHE=%s, CACHE_ONLY=%s)",
                db.cache_backend_name,
                settings.ENABLE_IN_MEMORY_CACHE,
                db.cache_only,
            )
        except Exception as exc:
            logger.exception("Failed to load cache; aborting startup")
            raise RuntimeError("Failed to load cache during startup") from exc
    else:
        logger.info("Cache disabled")

    owner = await db.get_user_by_email(settings.OWNER_EMAIL)
    if not owner:
        await db.create_user(
            email=settings.OWNER_EMAIL,
            password_hash=get_password_hash(settings.OWNER_PASSWORD),
            role="admin",
            name=settings.OWNER_NAME
        )
        logger.info("Owner user created: %s", settings.OWNER_EMAIL)
    else:
        logger.info("Owner user exists: %s", settings.OWNER_EMAIL)

    start_monitor_loop()
    logger.info("Monitor loop started")

    yield

    logger.info("Shutting down Statrix...")
    stop_monitor_loop()
    await db.close()
    logger.info("Database connection closed")


app = FastAPI(
    title=settings.APP_NAME,
    description=f"{settings.COMPANY_NAME} Uptime Monitoring Service",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.get_cors_origins(),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        if request.url.path.startswith('/api/'):
            return JSONResponse(
                status_code=404,
                content={"detail": "Resource not found"}
            )
        return FileResponse("frontend/404.html", status_code=404, headers=NO_CACHE_HEADERS)

    if exc.status_code == 401:
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": "Unauthorized"}
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={"detail": "Invalid request data"}
    )


@app.get("/health")
async def health_check():
    cache_stats = await db.get_cache_stats()
    backend_name = cache_stats.get("backend")
    healthy = cache_stats.get("healthy", True)
    connected = cache_stats.get("connected", True)
    if backend_name == "redis" and settings.CACHE_FAIL_FAST and (not connected or not healthy):
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "app": settings.APP_NAME, "cache": cache_stats},
        )
    return {"status": "healthy", "app": settings.APP_NAME, "cache": cache_stats}


@app.middleware("http")
async def cache_fail_fast_guard(request: Request, call_next):
    path = request.url.path or ""
    guarded_path = (
        path.startswith("/api/")
        or path.startswith("/hb/")
        or path.startswith("/v2/")
        or path.startswith("/win/")
    )
    if guarded_path and settings.CACHE_FAIL_FAST and db.cache_service:
        try:
            await db.ensure_cache_available()
        except CacheUnavailableError as exc:
            return JSONResponse(status_code=503, content={"detail": str(exc)})
        except Exception as exc:
            await db.mark_cache_unhealthy(str(exc))
            return JSONResponse(status_code=503, content={"detail": "Cache backend unavailable"})
    return await call_next(request)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    # CSP for HTML pages (not API JSON responses)
    if not request.url.path.startswith("/api/"):
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
            "style-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com https://fonts.googleapis.com; "
            "img-src 'self' https: data:; "
            "font-src 'self' https://cdnjs.cloudflare.com https://fonts.gstatic.com; "
            "connect-src 'self'"
        )
    return response


app.mount("/static", NoCacheStaticFiles(directory="frontend/static"), name="static")
app.mount("/shell", StaticFiles(directory="shell"), name="shell")


def _render_status_html(path: str) -> HTMLResponse:
    logo_url = html_escape(settings.STATUS_LOGO.strip() or "/static/images/logo.png")
    raw_title = html_escape(settings.STATUS_PAGE_TITLE.strip() or "Statrix Status")
    page_title = f"{raw_title} - Powered By Statrix"
    try:
        with open(path, "r", encoding="utf-8") as f:
            html = f.read()
    except Exception:
        return FileResponse(path, headers=NO_CACHE_HEADERS)

    html = html.replace("{{STATUS_LOGO_URL}}", logo_url)
    html = html.replace("{{STATUS_PAGE_TITLE}}", page_title)
    return HTMLResponse(content=html, headers=NO_CACHE_HEADERS)


@app.get("/")
async def root():
    return _render_status_html("frontend/index.html")

@app.get("/report.html")
async def report_page():
    return _render_status_html("frontend/report.html")

@app.get("/404.html")
async def not_found_page():
    return FileResponse("frontend/404.html", headers=NO_CACHE_HEADERS)

@app.get("/manifest.webmanifest")
async def manifest():
    return FileResponse("frontend/manifest.webmanifest", media_type="application/manifest+json")


@app.get("/edit")
async def edit_page():
    return FileResponse("frontend/edit.html", headers=NO_CACHE_HEADERS)


@app.get("/edit/dashboard")
async def dashboard_page():
    return FileResponse("frontend/dashboard.html", headers=NO_CACHE_HEADERS)


from .routes import auth, uptime_monitors, server_agent_monitors, heartbeat_monitors, maintenance, system

app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])

app.include_router(
    uptime_monitors.router,
    prefix="/api/uptime-monitors",
    tags=["Uptime Monitors (Website)"],
    dependencies=[Depends(get_current_admin)]
)

app.include_router(
    server_agent_monitors.router,
    prefix="/api/heartbeat-monitors/server-agent",
    tags=["Heartbeat Monitors (Server Agent)"],
    dependencies=[Depends(get_current_admin)]
)

app.include_router(
    heartbeat_monitors.router,
    prefix="/api/heartbeat-monitors",
    tags=["Heartbeat Monitors (Cronjob)"],
    dependencies=[Depends(get_current_admin)]
)

app.include_router(
    maintenance.router,
    prefix="/api/maintenance",
    tags=["Maintenance"],
    dependencies=[Depends(get_current_admin)]
)

app.include_router(
    system.router,
    prefix="/api/system",
    tags=["System"],
    dependencies=[Depends(get_current_admin)]
)

from .routes import agent
app.include_router(agent.router, tags=["Agent"])

from .routes import heartbeat as heartbeat_ping
app.include_router(heartbeat_ping.router, tags=["Heartbeat Ping"])

from .routes import status_pages
app.include_router(status_pages.router, prefix="/api/public", tags=["Public Status"])


from .routes import incidents

app.include_router(
    incidents.router,
    prefix="/api/incidents",
    tags=["Incidents"],
    dependencies=[Depends(get_current_admin)]
)


_MAX_INSTALL_BODY_BYTES = 1024


@app.post("/")
async def install_notification(request: Request):
    body = await request.body()
    if len(body) > _MAX_INSTALL_BODY_BYTES:
        return JSONResponse(
            status_code=413, content={"detail": "Payload too large"}
        )
    logger.info("Install notification received (%d bytes)", len(body))
    return "OK"


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
