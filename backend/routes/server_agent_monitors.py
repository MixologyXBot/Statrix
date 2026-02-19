# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import logging
import re
import secrets
import uuid
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, status

from ..config import settings
from ..database import db
from ..models import (
    ServerHistoryData,
    ServerMonitorCreate,
    ServerMonitorResponse,
    ServerMonitorUpdate,
)
from ..utils.cache import invalidate_status_cache

router = APIRouter()
logger = logging.getLogger(__name__)


def generate_sid() -> str:
    return secrets.token_hex(16)


_SAFE_SHELL_ARG_RE = re.compile(r'^[a-zA-Z0-9_.,:/\-]+$')


def _sanitize_shell_arg(value: str) -> str:
    if not value:
        return value
    if not _SAFE_SHELL_ARG_RE.match(value):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsafe characters in argument: {value!r}",
        )
    return value


def _normalize_csv_argument(value: str) -> str:
    parts = [segment.strip() for segment in str(value or "").split(",")]
    filtered = [segment for segment in parts if segment]
    result = ",".join(filtered) if filtered else "0"
    _sanitize_shell_arg(result)
    return result


def _build_unix_shell_command(script_url: str, args: str = "") -> str:
    arg_part = f" {args}".strip()
    if arg_part:
        return f"wget -4 -qO- {script_url} | sudo bash -s {args}"
    return f"wget -4 -qO- {script_url} | sudo bash"


def _build_windows_bootstrap_command(
    script_url: str, script_name: str, args: str = ""
) -> str:
    arg_part = f" {args}" if args else ""
    return (
        "powershell -NoProfile -ExecutionPolicy Bypass -Command "
        f"\"$u='{script_url}';"
        f"$f=Join-Path $env:TEMP '{script_name}';"
        "Invoke-WebRequest -UseBasicParsing -Uri $u -OutFile $f;"
        f"& $f{arg_part};"
        'Remove-Item $f -ErrorAction SilentlyContinue"'
    )


def _build_server_agent_command_payload(
    app_url: str,
    sid: str,
    platform: str,
    mode: str,
    run_as_root: bool,
    monitor_services: bool,
    services: str,
    monitor_raid: bool,
    monitor_drive: bool,
    view_processes: bool,
    overwrite_ports: bool,
    ports: str,
) -> dict:
    user_arg = "1" if run_as_root else "0"
    services_arg = _normalize_csv_argument(services) if monitor_services else "0"
    ports_arg = _normalize_csv_argument(ports) if overwrite_ports else "0"
    raid_arg = "1" if monitor_raid else "0"
    drive_arg = "1" if monitor_drive else "0"
    process_arg = "1" if view_processes else "0"
    _sanitize_shell_arg(app_url.replace("https://", "").replace("http://", ""))
    _sanitize_shell_arg(sid)
    install_args = f"{app_url} {sid} {user_arg} {services_arg} {raid_arg} {drive_arg} {process_arg} {ports_arg}"

    if platform == "windows":
        script_map = {
            "install": "statrix_install.ps1",
            "update": "statrix_update.ps1",
            "uninstall": "statrix_uninstall.ps1",
        }
        script_name = script_map[mode]
        script_url = f"{app_url}/shell/windows/{script_name}"
        command_args = (
            install_args if mode == "install" else (sid if mode == "uninstall" else "")
        )
        command = _build_windows_bootstrap_command(
            script_url, script_name, command_args
        )
    elif platform == "macos":
        script_map = {
            "install": "statrix_install.sh",
            "update": "statrix_update.sh",
            "uninstall": "statrix_uninstall.sh",
        }
        script_name = script_map[mode]
        script_url = f"{app_url}/shell/macOS/{script_name}"
        command_args = (
            install_args if mode == "install" else (sid if mode == "uninstall" else "")
        )
        command = _build_unix_shell_command(script_url, command_args)
    else:
        script_name = (
            "statrix_uninstall.sh" if mode == "uninstall" else "statrix_install.sh"
        )
        script_url = f"{app_url}/shell/linux/{script_name}"
        command_args = sid if mode == "uninstall" else install_args
        command = _build_unix_shell_command(script_url, command_args)

    return {
        "platform": platform,
        "mode": mode,
        "sid": sid,
        "script_url": script_url,
        "command": command,
        "options": {
            "run_as_root": run_as_root,
            "monitor_services": monitor_services,
            "services": _normalize_csv_argument(services) if monitor_services else "",
            "monitor_raid": monitor_raid,
            "monitor_drive": monitor_drive,
            "view_processes": view_processes,
            "overwrite_ports": overwrite_ports,
            "ports": _normalize_csv_argument(ports) if overwrite_ports else "",
        },
    }


@router.get("", response_model=list[ServerMonitorResponse])
async def get_server_monitors(
    enabled_only: bool = Query(False, description="Filter to only enabled monitors"),
):
    try:
        monitors = await db.get_server_monitors(enabled_only=enabled_only)
        return [ServerMonitorResponse(**m) for m in monitors]
    except Exception:
        logger.exception("get_server_monitors failed")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch server monitors",
        )


@router.post(
    "", response_model=ServerMonitorResponse, status_code=status.HTTP_201_CREATED
)
async def create_server_monitor(monitor: ServerMonitorCreate):
    if await db.is_monitor_name_taken(monitor.name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A monitor with this name already exists",
        )

    sid = generate_sid()
    while await db.get_server_monitor_by_sid(sid):
        sid = generate_sid()

    monitor_id = await db.create_server_monitor(
        sid=sid, name=monitor.name, category=monitor.category
    )

    created = await db.get_server_monitor_by_id(monitor_id)
    if not created:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create server monitor",
        )

    invalidate_status_cache()
    return ServerMonitorResponse(**created)


@router.get("/{server_id}", response_model=ServerMonitorResponse)
async def get_server_monitor(server_id: uuid.UUID):
    monitor = await db.get_server_monitor_by_id(server_id)
    if not monitor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Server monitor not found"
        )
    return ServerMonitorResponse(**monitor)


@router.patch("/{server_id}", response_model=ServerMonitorResponse)
async def update_server_monitor(server_id: uuid.UUID, update: ServerMonitorUpdate):
    existing = await db.get_server_monitor_by_id(server_id)
    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Server monitor not found"
        )

    update_data = update.model_dump(exclude_unset=True)
    if "name" in update_data:
        requested_name = str(update_data.get("name") or "").strip().lower()
        current_name = str(existing.get("name") or "").strip().lower()
        if requested_name and requested_name != current_name:
            if await db.is_monitor_name_taken(
                update_data["name"], exclude_monitor_id=server_id
            ):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="A monitor with this name already exists",
                )
    if update_data:
        success = await db.update_server_monitor(server_id, **update_data)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update server monitor",
            )
        invalidate_status_cache()

    updated = await db.get_server_monitor_by_id(server_id)
    return ServerMonitorResponse(**updated)


@router.delete("/{server_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_server_monitor(server_id: uuid.UUID):
    success = await db.delete_server_monitor(server_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Server monitor not found"
        )
    invalidate_status_cache()


@router.get("/{server_id}/history", response_model=list[ServerHistoryData])
async def get_server_history(
    server_id: uuid.UUID,
    hours: int = Query(24, ge=1, le=720, description="Hours of history to retrieve"),
):
    monitor = await db.get_server_monitor_by_id(server_id)
    if not monitor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Server monitor not found"
        )

    if hours <= 24:
        history = await db.get_server_history(server_id, hours=hours)
    elif hours <= 72:
        history = await db.get_server_history_aggregated(
            server_id, hours=hours, interval="15min"
        )
    elif hours <= 336:
        history = await db.get_server_history_aggregated(
            server_id, hours=hours, interval="hour"
        )
    else:
        history = await db.get_server_history_aggregated(
            server_id, hours=hours, interval="day"
        )
    return [ServerHistoryData(**h) for h in history]


@router.get("/{server_id}/install")
async def get_server_install_command(
    server_id: uuid.UUID,
    platform: Literal["linux", "macos", "windows"] = Query("linux"),
):
    payload = await get_server_agent_command(
        server_id=server_id,
        platform=platform,
        mode="install",
        run_as_root=False,
        monitor_services=False,
        services="",
        monitor_raid=False,
        monitor_drive=False,
        view_processes=False,
        overwrite_ports=False,
        ports="",
    )
    return {
        "server_id": payload["server_id"],
        "sid": payload["sid"],
        "platform": payload["platform"],
        "install_command": payload["command"],
        "install_url": payload["script_url"],
    }


@router.get("/{server_id}/command")
async def get_server_agent_command(
    server_id: uuid.UUID,
    platform: Literal["linux", "macos", "windows"] = Query("linux"),
    mode: Literal["install", "update", "uninstall"] = Query("install"),
    run_as_root: bool = Query(False),
    monitor_services: bool = Query(False),
    services: str = Query(""),
    monitor_raid: bool = Query(False),
    monitor_drive: bool = Query(False),
    view_processes: bool = Query(False),
    overwrite_ports: bool = Query(False),
    ports: str = Query(""),
):
    monitor = await db.get_server_monitor_by_id(server_id)
    if not monitor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Server monitor not found"
        )

    app_url = settings.APP_URL.rstrip("/")
    payload = _build_server_agent_command_payload(
        app_url=app_url,
        sid=monitor["sid"],
        platform=platform,
        mode=mode,
        run_as_root=run_as_root,
        monitor_services=monitor_services,
        services=services,
        monitor_raid=monitor_raid,
        monitor_drive=monitor_drive,
        view_processes=view_processes,
        overwrite_ports=overwrite_ports,
        ports=ports,
    )

    return {
        "server_id": server_id,
        "sid": monitor["sid"],
        "platform": payload["platform"],
        "mode": payload["mode"],
        "command": payload["command"],
        "script_url": payload["script_url"],
        "options": payload["options"],
    }


@router.post("/{server_id}/pause")
async def pause_server_monitor(server_id: uuid.UUID):
    success = await db.update_server_monitor(server_id, enabled=False)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Server monitor not found"
        )
    invalidate_status_cache()
    return {"message": "Server monitor paused"}


@router.post("/{server_id}/resume")
async def resume_server_monitor(server_id: uuid.UUID):
    success = await db.update_server_monitor(server_id, enabled=True)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Server monitor not found"
        )
    invalidate_status_cache()
    return {"message": "Server monitor resumed"}


@router.post("/{server_id}/make-public")
async def make_server_public(server_id: uuid.UUID):
    success = await db.update_server_monitor(server_id, is_public=True)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Server monitor not found"
        )
    invalidate_status_cache()
    return {"message": "Server monitor is now public"}


@router.post("/{server_id}/make-private")
async def make_server_private(server_id: uuid.UUID):
    success = await db.update_server_monitor(server_id, is_public=False)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Server monitor not found"
        )
    invalidate_status_cache()
    return {"message": "Server monitor is now private"}
