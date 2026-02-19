# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import uuid

from fastapi import APIRouter, HTTPException, status

from ..database import db
from ..models import MaintenanceScheduleRequest
from ..utils.cache import invalidate_status_cache

router = APIRouter()


async def _resolve_monitor_uuid(monitor_type: str, monitor_id: str) -> uuid.UUID | None:
    """Resolve UUID from path id. Falls back to SID lookup for heartbeat/server agents."""
    normalized = (monitor_type or "").strip().lower()

    def _parse_uuid(value: str) -> uuid.UUID | None:
        try:
            return uuid.UUID(str(value))
        except Exception:
            return None

    if normalized in {"website", "uptime"}:
        parsed = _parse_uuid(monitor_id)
        if not parsed:
            return None
        monitor = await db.get_uptime_monitor_by_id(parsed)
        return monitor["id"] if monitor else None

    if normalized in {"heartbeat-cronjob", "heartbeat"}:
        parsed = _parse_uuid(monitor_id)
        if parsed:
            monitor = await db.get_heartbeat_monitor_by_id(parsed)
            if monitor:
                return monitor["id"]
        monitor = await db.get_heartbeat_monitor_by_sid(monitor_id)
        return monitor["id"] if monitor else None

    if normalized in {
        "heartbeat-server-agent",
        "server_agent",
        "server",
        "server-agent",
        "agent",
    }:
        parsed = _parse_uuid(monitor_id)
        if parsed:
            monitor = await db.get_server_monitor_by_id(parsed)
            if monitor:
                return monitor["id"]
        monitor = await db.get_server_monitor_by_sid(monitor_id)
        return monitor["id"] if monitor else None

    parsed = _parse_uuid(monitor_id)
    return parsed


@router.post("/{monitor_type}/{monitor_id}/start")
async def start_maintenance_mode(
    monitor_type: str,
    monitor_id: str,
):
    try:
        resolved_id = await _resolve_monitor_uuid(monitor_type, monitor_id)
        if not resolved_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Monitor not found"
            )
        success = await db.start_monitor_maintenance_now(
            monitor_type=monitor_type, monitor_id=resolved_id
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported monitor type. Use website, heartbeat-cronjob, or heartbeat-server-agent.",
        )

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Monitor not found"
        )

    invalidate_status_cache()
    return {
        "message": "Maintenance mode enabled",
        "monitor_type": monitor_type,
        "monitor_id": str(monitor_id),
    }


@router.post("/{monitor_type}/{monitor_id}/schedule")
async def schedule_maintenance_mode(
    monitor_type: str, monitor_id: str, payload: MaintenanceScheduleRequest
):
    try:
        resolved_id = await _resolve_monitor_uuid(monitor_type, monitor_id)
        if not resolved_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Monitor not found"
            )
        success = await db.schedule_monitor_maintenance(
            monitor_type=monitor_type,
            monitor_id=resolved_id,
            start_at=payload.start_at,
            end_at=payload.end_at,
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported monitor type. Use website, heartbeat-cronjob, or heartbeat-server-agent.",
        )

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Monitor not found"
        )

    invalidate_status_cache()
    return {
        "message": "Maintenance window scheduled",
        "monitor_type": monitor_type,
        "monitor_id": str(monitor_id),
        "start_at": payload.start_at.isoformat(),
        "end_at": payload.end_at.isoformat(),
    }


@router.post("/{monitor_type}/{monitor_id}/end")
async def end_maintenance_mode(monitor_type: str, monitor_id: str):
    try:
        resolved_id = await _resolve_monitor_uuid(monitor_type, monitor_id)
        if not resolved_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Monitor not found"
            )
        success = await db.end_monitor_maintenance(
            monitor_type=monitor_type, monitor_id=resolved_id
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unsupported monitor type. Use website, heartbeat-cronjob, or heartbeat-server-agent.",
        )

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Monitor not found"
        )

    invalidate_status_cache()
    return {
        "message": "Maintenance mode ended",
        "monitor_type": monitor_type,
        "monitor_id": str(monitor_id),
    }
