# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import secrets
import uuid

from fastapi import APIRouter, HTTPException, Query, status

from ..config import settings
from ..database import db
from ..models import (
    HeartbeatMonitorCreate,
    HeartbeatMonitorResponse,
    HeartbeatMonitorUpdate,
)
from ..utils.cache import invalidate_status_cache

router = APIRouter()


def generate_heartbeat_sid() -> str:
    return secrets.token_hex(16)


@router.get("", response_model=list[HeartbeatMonitorResponse])
async def get_heartbeat_monitors(
    enabled_only: bool = Query(False, description="Filter to only enabled monitors"),
):
    monitors = await db.get_heartbeat_monitors(enabled_only=enabled_only)
    return [HeartbeatMonitorResponse(**m) for m in monitors]


@router.post(
    "", response_model=HeartbeatMonitorResponse, status_code=status.HTTP_201_CREATED
)
async def create_heartbeat_monitor(monitor: HeartbeatMonitorCreate):
    if monitor.heartbeat_type == "server_agent":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Use /api/heartbeat-monitors/server-agent for heartbeat_type='server_agent'",
        )
    if await db.is_monitor_name_taken(monitor.name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A monitor with this name already exists",
        )

    sid = generate_heartbeat_sid()
    while await db.get_heartbeat_monitor_by_sid(sid):
        sid = generate_heartbeat_sid()

    monitor_id = await db.create_heartbeat_monitor(
        sid=sid,
        name=monitor.name,
        heartbeat_type=monitor.heartbeat_type,
        timeout=monitor.timeout,
        grace_period=monitor.grace_period,
        category=monitor.category,
    )

    created = await db.get_heartbeat_monitor_by_id(monitor_id)
    if not created:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create heartbeat monitor",
        )

    invalidate_status_cache()
    return HeartbeatMonitorResponse(**created)


@router.get("/{monitor_id}", response_model=HeartbeatMonitorResponse)
async def get_heartbeat_monitor(monitor_id: uuid.UUID):
    monitor = await db.get_heartbeat_monitor_by_id(monitor_id)
    if not monitor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Heartbeat monitor not found"
        )
    return HeartbeatMonitorResponse(**monitor)


@router.patch("/{monitor_id}", response_model=HeartbeatMonitorResponse)
async def update_heartbeat_monitor(
    monitor_id: uuid.UUID, update: HeartbeatMonitorUpdate
):
    existing = await db.get_heartbeat_monitor_by_id(monitor_id)
    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Heartbeat monitor not found"
        )

    update_data = update.model_dump(exclude_unset=True)
    if update_data.get("heartbeat_type") == "server_agent":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Use /api/heartbeat-monitors/server-agent for heartbeat_type='server_agent'",
        )
    if "name" in update_data:
        requested_name = str(update_data.get("name") or "").strip().lower()
        current_name = str(existing.get("name") or "").strip().lower()
        if requested_name and requested_name != current_name:
            if await db.is_monitor_name_taken(
                update_data["name"], exclude_monitor_id=monitor_id
            ):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="A monitor with this name already exists",
                )
    if update_data:
        success = await db.update_heartbeat_monitor(monitor_id, **update_data)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update heartbeat monitor",
            )
        invalidate_status_cache()

    updated = await db.get_heartbeat_monitor_by_id(monitor_id)
    return HeartbeatMonitorResponse(**updated)


@router.delete("/{monitor_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_heartbeat_monitor(monitor_id: uuid.UUID):
    success = await db.delete_heartbeat_monitor(monitor_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Heartbeat monitor not found"
        )
    invalidate_status_cache()


@router.get("/{monitor_id}/ping-url")
async def get_heartbeat_ping_url(monitor_id: uuid.UUID):
    monitor = await db.get_heartbeat_monitor_by_id(monitor_id)
    if not monitor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Heartbeat monitor not found"
        )
    if monitor.get("heartbeat_type", "cronjob") != "cronjob":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Ping URL is only available for heartbeat_type='cronjob'",
        )

    ping_url = f"{settings.APP_URL}/hb/?s={monitor['sid']}"

    return {
        "monitor_id": monitor_id,
        "sid": monitor["sid"],
        "ping_url": ping_url,
        "curl_command": f"curl -s '{ping_url}'",
        "wget_command": f"wget -qO- '{ping_url}'",
        "php_command": f"file_get_contents('{ping_url}');",
    }


@router.post("/{monitor_id}/pause")
async def pause_heartbeat_monitor(monitor_id: uuid.UUID):
    success = await db.update_heartbeat_monitor(monitor_id, enabled=False)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Heartbeat monitor not found"
        )
    invalidate_status_cache()
    return {"message": "Heartbeat monitor paused"}


@router.post("/{monitor_id}/resume")
async def resume_heartbeat_monitor(monitor_id: uuid.UUID):
    success = await db.update_heartbeat_monitor(monitor_id, enabled=True)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Heartbeat monitor not found"
        )
    invalidate_status_cache()
    return {"message": "Heartbeat monitor resumed"}


@router.post("/{monitor_id}/make-public")
async def make_heartbeat_public(monitor_id: uuid.UUID):
    success = await db.update_heartbeat_monitor(monitor_id, is_public=True)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Heartbeat monitor not found"
        )
    invalidate_status_cache()
    return {"message": "Heartbeat monitor is now public"}


@router.post("/{monitor_id}/make-private")
async def make_heartbeat_private(monitor_id: uuid.UUID):
    success = await db.update_heartbeat_monitor(monitor_id, is_public=False)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Heartbeat monitor not found"
        )
    invalidate_status_cache()
    return {"message": "Heartbeat monitor is now private"}
