# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import uuid
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, HTTPException, Query, status

from ..database import db
from ..models import (
    UptimeMonitorCreate,
    UptimeMonitorResponse,
    UptimeMonitorUpdate,
)
from ..utils.cache import invalidate_status_cache

router = APIRouter()


def _normalize_target_url(target: str) -> str:
    candidate = str(target or "").strip()
    if not candidate:
        return candidate
    parsed = urlparse(candidate)
    if not parsed.scheme:
        candidate = f"https://{candidate}"
        parsed = urlparse(candidate)
    if not parsed.scheme or not parsed.netloc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Monitor not added, website cannot be accessed.",
        )
    return candidate


async def _probe_target(url: str, timeout_seconds: int) -> tuple[int, int | None]:
    request_timeout = max(5.0, min(60.0, float(timeout_seconds or 5)))
    try:
        async with httpx.AsyncClient(
            timeout=request_timeout, follow_redirects=True
        ) as client:
            response = await client.get(url)
            elapsed_ms = None
            try:
                elapsed_ms = int(round(response.elapsed.total_seconds() * 1000))
            except Exception:
                elapsed_ms = None
            return response.status_code, elapsed_ms
    except httpx.HTTPError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Monitor not added, website cannot be accessed.",
        )


@router.get("", response_model=list[UptimeMonitorResponse])
async def get_uptime_monitors(
    enabled_only: bool = Query(False, description="Filter to only enabled monitors"),
):
    monitors = await db.get_uptime_monitors(enabled_only=enabled_only)
    return [UptimeMonitorResponse(**m) for m in monitors]


@router.post(
    "", response_model=UptimeMonitorResponse, status_code=status.HTTP_201_CREATED
)
async def create_uptime_monitor(monitor: UptimeMonitorCreate):
    if await db.is_monitor_name_taken(monitor.name):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A monitor with this name already exists",
        )

    normalized_target = _normalize_target_url(monitor.target)

    await _probe_target(normalized_target, monitor.timeout)

    monitor_id = await db.create_uptime_monitor(
        name=monitor.name,
        monitor_type=monitor.type,
        target=normalized_target,
        port=monitor.port,
        check_interval=monitor.check_interval,
        timeout=monitor.timeout,
        category=monitor.category,
        private_notes=monitor.private_notes,
    )

    created = await db.get_uptime_monitor_by_id(monitor_id)
    if not created:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create monitor",
        )

    invalidate_status_cache()
    return UptimeMonitorResponse(**created)


@router.get("/{monitor_id}", response_model=UptimeMonitorResponse)
async def get_uptime_monitor(monitor_id: uuid.UUID):
    monitor = await db.get_uptime_monitor_by_id(monitor_id)
    if not monitor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Monitor not found"
        )
    return UptimeMonitorResponse(**monitor)


@router.put("/{monitor_id}", response_model=UptimeMonitorResponse)
async def update_uptime_monitor(monitor_id: uuid.UUID, update: UptimeMonitorUpdate):
    return await _update_uptime_monitor_internal(monitor_id, update)


@router.patch("/{monitor_id}", response_model=UptimeMonitorResponse)
async def patch_uptime_monitor(monitor_id: uuid.UUID, update: UptimeMonitorUpdate):
    return await _update_uptime_monitor_internal(monitor_id, update)


async def _update_uptime_monitor_internal(
    monitor_id: uuid.UUID, update: UptimeMonitorUpdate
) -> UptimeMonitorResponse:
    existing = await db.get_uptime_monitor_by_id(monitor_id)
    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Monitor not found"
        )

    update_data = update.model_dump(exclude_unset=True, exclude={"config"})

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
        success = await db.update_uptime_monitor(monitor_id, **update_data)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update monitor",
            )
        invalidate_status_cache()

    updated = await db.get_uptime_monitor_by_id(monitor_id)
    return UptimeMonitorResponse(**updated)


@router.delete("/{monitor_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_uptime_monitor(monitor_id: uuid.UUID):
    success = await db.delete_uptime_monitor(monitor_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Monitor not found"
        )
    invalidate_status_cache()


@router.post("/{monitor_id}/pause")
async def pause_uptime_monitor(monitor_id: uuid.UUID):
    success = await db.update_uptime_monitor(monitor_id, enabled=False)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Monitor not found"
        )
    invalidate_status_cache()
    return {"message": "Monitor paused"}


@router.post("/{monitor_id}/resume")
async def resume_uptime_monitor(monitor_id: uuid.UUID):
    success = await db.update_uptime_monitor(monitor_id, enabled=True)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Monitor not found"
        )
    invalidate_status_cache()
    return {"message": "Monitor resumed"}


@router.post("/{monitor_id}/make-public")
async def make_monitor_public(monitor_id: uuid.UUID):
    success = await db.update_uptime_monitor(monitor_id, is_public=True)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Monitor not found"
        )
    invalidate_status_cache()
    return {"message": "Monitor is now public"}


@router.post("/{monitor_id}/make-private")
async def make_monitor_private(monitor_id: uuid.UUID):
    success = await db.update_uptime_monitor(monitor_id, is_public=False)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Monitor not found"
        )
    invalidate_status_cache()
    return {"message": "Monitor is now private"}


@router.get("/{monitor_id}/stats")
async def get_uptime_monitor_stats(
    monitor_id: uuid.UUID,
    days: int = Query(
        90, ge=1, le=365, description="Number of days to calculate stats for"
    ),
):
    monitor = await db.get_uptime_monitor_by_id(monitor_id)
    if not monitor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Monitor not found"
        )

    stats = await db.get_uptime_stats(monitor_id, days=days)
    return {
        "monitor_id": monitor_id,
        "days": days,
        "up_count": stats.get("up_count", 0),
        "down_count": stats.get("down_count", 0),
        "total_count": stats.get("total_count", 0),
        "uptime_percentage": _calculate_uptime(stats),
        "avg_response_time_ms": stats.get("avg_response_time"),
    }


def _calculate_uptime(stats: dict) -> float:
    total = stats.get("total_count", 0)
    up = stats.get("up_count", 0)
    if total == 0:
        return 100.0
    return round((up / total) * 100, 4)
