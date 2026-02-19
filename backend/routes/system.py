# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import os
import platform
import sys
from urllib.parse import parse_qs, unquote, urlparse

from fastapi import APIRouter

from ..config import settings
from ..database import db
from ..runtime import START_TIME_UTC
from ..utils.time import utcnow

router = APIRouter()


def _get_memory_mb() -> float | None:
    try:
        import psutil
        proc = psutil.Process(os.getpid())
        return proc.memory_info().rss / (1024 * 1024)
    except Exception:
        pass
    try:
        import resource
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if platform.system().lower() == "darwin":
            return rss / (1024 * 1024)
        return rss / 1024
    except Exception:
        return None


def _detect_cache_provider(host: str) -> str:
    value = str(host or "").strip().lower()
    if not value:
        return "Unknown"
    if "upstash" in value:
        return "Upstash"
    if "aiven" in value:
        return "Aiven"
    if "rediscloud" in value:
        return "Redis Cloud"
    return "Custom"


def _detect_postgres_provider(host: str) -> str:
    value = str(host or "").strip().lower()
    if not value:
        return "Unknown"
    if "supabase" in value:
        return "Supabase"
    if "neon" in value:
        return "Neon"
    if "railway" in value:
        return "Railway"
    if "render" in value:
        return "Render"
    if "aiven" in value:
        return "Aiven"
    if "digitalocean" in value:
        return "DigitalOcean"
    if "amazonaws.com" in value or ".rds." in value:
        return "AWS RDS"
    if "azure.com" in value:
        return "Azure PostgreSQL"
    if "googleapis.com" in value or "cloudsql" in value:
        return "Google Cloud SQL"
    return "Custom"


def _resolve_redis_url() -> str:
    if db.cache_service and hasattr(db.cache_service.backend, "redis_url"):
        return str(getattr(db.cache_service.backend, "redis_url") or "").strip()
    return (
        str(getattr(settings, "REDIS_URL", "") or "").strip()
        or str(os.getenv("UPSTASH_REDIS_TLS_URL") or "").strip()
        or str(os.getenv("UPSTASH_REDIS_URL") or "").strip()
    )


def _resolve_database_url() -> str:
    return str(getattr(settings, "DATABASE_URL", "") or "").strip()


def _format_size(num_bytes: int | None) -> str | None:
    if num_bytes is None:
        return None
    try:
        value = float(num_bytes)
    except Exception:
        return None
    if value < 0:
        return None
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    unit_index = 0
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024
        unit_index += 1
    return f"{value:.2f} {units[unit_index]}"


def _get_pool_usage() -> dict:
    if not db.pool:
        return {
            "pool_size": None,
            "pool_idle_connections": None,
            "pool_in_use_connections": None,
            "pool_utilization_percent": None,
        }

    try:
        pool_size = db.pool.get_size()
    except Exception:
        pool_size = None
    try:
        pool_idle = db.pool.get_idle_size()
    except Exception:
        pool_idle = None

    pool_in_use = None
    if isinstance(pool_size, int) and isinstance(pool_idle, int):
        pool_in_use = max(pool_size - pool_idle, 0)

    pool_utilization_percent = None
    if isinstance(pool_in_use, int) and isinstance(db.pool_max_size, int) and db.pool_max_size > 0:
        pool_utilization_percent = round((pool_in_use / db.pool_max_size) * 100, 2)

    return {
        "pool_size": pool_size,
        "pool_idle_connections": pool_idle,
        "pool_in_use_connections": pool_in_use,
        "pool_utilization_percent": pool_utilization_percent,
    }


async def _get_postgres_metrics() -> dict:
    metrics = {
        "database_name": None,
        "server_version": None,
        "max_connections": None,
        "total_connections": None,
        "active_connections": None,
        "idle_connections": None,
        "database_size_bytes": None,
        "database_size_pretty": None,
        "cache_hit_ratio_percent": None,
        "xact_commit": None,
        "xact_rollback": None,
        "server_uptime_seconds": None,
        "last_error": None,
    }
    if not db.pool:
        metrics["last_error"] = "PostgreSQL pool unavailable"
        return metrics

    try:
        async with db.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                    current_database() AS database_name,
                    current_setting('server_version') AS server_version,
                    current_setting('max_connections')::int AS max_connections,
                    pg_database_size(current_database())::bigint AS database_size_bytes,
                    COALESCE(stats.numbackends, 0)::int AS total_connections,
                    COALESCE(activity.active_connections, 0)::int AS active_connections,
                    COALESCE(activity.idle_connections, 0)::int AS idle_connections,
                    COALESCE(stats.xact_commit, 0)::bigint AS xact_commit,
                    COALESCE(stats.xact_rollback, 0)::bigint AS xact_rollback,
                    CASE
                        WHEN (COALESCE(stats.blks_hit, 0) + COALESCE(stats.blks_read, 0)) > 0
                        THEN ROUND((stats.blks_hit::numeric / (stats.blks_hit + stats.blks_read)) * 100, 2)
                        ELSE NULL
                    END AS cache_hit_ratio_percent,
                    EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))::bigint AS server_uptime_seconds
                FROM pg_stat_database stats
                LEFT JOIN (
                    SELECT
                        datname,
                        COUNT(*) FILTER (WHERE state = 'active')::int AS active_connections,
                        COUNT(*) FILTER (WHERE state = 'idle')::int AS idle_connections
                    FROM pg_stat_activity
                    GROUP BY datname
                ) activity ON activity.datname = stats.datname
                WHERE stats.datname = current_database()
                """
            )
        if row:
            database_size_bytes = row.get("database_size_bytes")
            cache_hit_ratio_percent = row.get("cache_hit_ratio_percent")
            try:
                cache_hit_ratio_percent = round(float(cache_hit_ratio_percent), 2) if cache_hit_ratio_percent is not None else None
            except Exception:
                cache_hit_ratio_percent = None

            metrics.update(
                {
                    "database_name": row.get("database_name"),
                    "server_version": row.get("server_version"),
                    "max_connections": row.get("max_connections"),
                    "total_connections": row.get("total_connections"),
                    "active_connections": row.get("active_connections"),
                    "idle_connections": row.get("idle_connections"),
                    "database_size_bytes": database_size_bytes,
                    "database_size_pretty": _format_size(database_size_bytes),
                    "cache_hit_ratio_percent": cache_hit_ratio_percent,
                    "xact_commit": row.get("xact_commit"),
                    "xact_rollback": row.get("xact_rollback"),
                    "server_uptime_seconds": row.get("server_uptime_seconds"),
                    "last_error": None,
                }
            )
            return metrics
        metrics["last_error"] = "No PostgreSQL metrics returned"
        return metrics
    except Exception as exc:
        metrics["last_error"] = str(exc)
        return metrics


def _get_redis_details(cache_stats: dict) -> dict:
    backend = str(cache_stats.get("backend") or "").strip().lower()
    if backend != "redis":
        return {
            "configured": False,
            "provider": None,
            "scheme": None,
            "endpoint": None,
            "tls_enabled": None,
            "key_prefix": None,
            "connected": bool(cache_stats.get("connected", False)),
            "healthy": bool(cache_stats.get("healthy", False)),
            "last_error": cache_stats.get("last_error"),
        }

    redis_url = _resolve_redis_url()
    parsed = urlparse(redis_url) if redis_url else None
    scheme = (parsed.scheme or "") if parsed else ""
    host = (parsed.hostname or "") if parsed else ""
    tls_enabled = scheme in {"rediss", "valkeys"}
    default_port = 6380 if tls_enabled else 6379
    port = parsed.port if parsed and parsed.port else default_port
    endpoint = f"{host}:{port}" if host else None

    return {
        "configured": bool(redis_url),
        "provider": _detect_cache_provider(host),
        "scheme": scheme or None,
        "endpoint": endpoint,
        "tls_enabled": tls_enabled,
        "key_prefix": str(getattr(settings, "CACHE_KEY_PREFIX", "") or "") or None,
        "connected": bool(cache_stats.get("connected", False)),
        "healthy": bool(cache_stats.get("healthy", False)),
        "last_error": cache_stats.get("last_error"),
    }


async def _get_database_details() -> dict:
    database_url = _resolve_database_url()
    parsed = urlparse(database_url) if database_url else None
    params = parse_qs(parsed.query) if parsed else {}
    scheme = (parsed.scheme or "") if parsed else ""
    host = (parsed.hostname or "") if parsed else ""
    port = parsed.port if parsed and parsed.port else 5432
    endpoint = f"{host}:{port}" if host else None
    path_db_name = unquote((parsed.path or "").lstrip("/")) if parsed else ""
    ssl_mode = (
        params.get("sslmode", [None])[0]
        or params.get("ssl", [None])[0]
        or params.get("tls", [None])[0]
    )

    pool_usage = _get_pool_usage()
    metrics = await _get_postgres_metrics()
    database_name = metrics.get("database_name") or path_db_name or None

    return {
        "configured": bool(database_url),
        "connected": bool(db.pool),
        "healthy": metrics.get("last_error") is None,
        "provider": _detect_postgres_provider(host),
        "scheme": scheme or None,
        "endpoint": endpoint,
        "database_name": database_name,
        "ssl_mode": ssl_mode,
        "pool_min_size": db.pool_min_size,
        "pool_max_size": db.pool_max_size,
        "statement_cache_size": db.pool_statement_cache_size,
        "pool_size": pool_usage["pool_size"],
        "pool_idle_connections": pool_usage["pool_idle_connections"],
        "pool_in_use_connections": pool_usage["pool_in_use_connections"],
        "pool_utilization_percent": pool_usage["pool_utilization_percent"],
        "metrics": metrics,
    }


@router.get("/resources")
async def get_system_resources():
    cache_stats = await db.get_cache_stats()
    redis_details = _get_redis_details(cache_stats)
    database_details = await _get_database_details()
    now = utcnow()
    uptime_seconds = int((now - START_TIME_UTC).total_seconds())
    memory_mb = _get_memory_mb()

    return {
        "server_time_utc": now.isoformat(),
        "app_start_utc": START_TIME_UTC.isoformat(),
        "uptime_seconds": uptime_seconds,
        "process": {
            "pid": os.getpid(),
            "python_version": sys.version.split()[0],
            "platform": platform.platform(),
            "memory_mb": round(memory_mb, 2) if memory_mb is not None else None
        },
        "database": database_details,
        "cache": cache_stats,
        "redis": redis_details,
    }
