# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

from fastapi import APIRouter, HTTPException, Query
import logging
import asyncio
from datetime import datetime, timedelta, time, date
from enum import Enum
import uuid

from ..database import db
from ..config import settings
from ..utils.time import utcnow
from ..utils.monitors import (
    is_placeholder_monitor_id,
    resolve_monitor_context,
)

router = APIRouter()
logger = logging.getLogger(__name__)


# Local fallback only (primary status cache is shared backend via CacheService).
_status_cache_fallback: dict = {}
_CACHE_TTL = max(0, int(getattr(settings, "PUBLIC_STATUS_CACHE_TTL_SECONDS", 10) or 0))
_PARTIAL_DOWNTIME_MINUTES = 15.0


def _get_cache_key(offset: int, sla_range: str | None) -> str:
    return f"{offset}_{sla_range or 'none'}"


async def _get_cached_status(cache_key: str) -> dict | None:
    if _CACHE_TTL <= 0:
        return None
    if db.cache_service:
        try:
            cached = await db.cache_service.get_status_live(cache_key)
            if cached is not None:
                return cached
        except Exception as exc:
            await db.mark_cache_unhealthy(f"status cache read failed: {exc}")
            logger.warning("Failed to read status cache from backend: %s", exc)
    if cache_key in _status_cache_fallback:
        entry = _status_cache_fallback[cache_key]
        if (utcnow() - entry["timestamp"]).total_seconds() < _CACHE_TTL:
            return entry["data"]
    return None


async def _set_cached_status(cache_key: str, data: dict) -> None:
    _status_cache_fallback[cache_key] = {
        "data": data,
        "timestamp": utcnow()
    }
    if db.cache_service:
        try:
            await db.cache_service.set_status_live(cache_key, data, _CACHE_TTL)
        except Exception as exc:
            await db.mark_cache_unhealthy(f"status cache write failed: {exc}")
            raise


async def _get_stale_cached_status(cache_key: str) -> dict | None:
    if db.cache_service:
        try:
            stale = await db.cache_service.get_status_stale(cache_key)
            if stale is not None:
                return stale
        except Exception as exc:
            await db.mark_cache_unhealthy(f"stale status cache read failed: {exc}")
            logger.warning("Failed to read stale status cache from backend: %s", exc)
    entry = _status_cache_fallback.get(cache_key)
    return entry.get("data") if entry else None


_background_tasks: set[asyncio.Task] = set()


def invalidate_status_cache() -> None:
    _status_cache_fallback.clear()
    if not db.cache_service:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    task = loop.create_task(db.cache_service.invalidate_status_cache())
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


class SlaRange(str, Enum):
    last7days = "last7days"
    last30days = "last30days"
    this_month = "this_month"
    last_month = "last_month"
    year_to_date = "year_to_date"
    last_year = "last_year"


def _parse_uuid(value: str) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(value))
    except Exception:
        return None


def _status_since_iso(monitor: dict, status: str) -> str | None:
    status_value = str(status or "").strip().lower()

    raw = monitor.get("status_since")
    if isinstance(raw, datetime):
        return raw.isoformat()
    if isinstance(raw, str) and raw.strip():
        return raw

    if status_value == "down":
        down_since = monitor.get("down_since")
        if isinstance(down_since, datetime):
            return down_since.isoformat()
        if isinstance(down_since, str) and down_since.strip():
            return down_since

    # Legacy fallback for monitors that predate status_since tracking.
    last_checkin = monitor.get("last_checkin_at")
    if isinstance(last_checkin, datetime):
        return last_checkin.isoformat()
    if isinstance(last_checkin, str) and last_checkin.strip():
        return last_checkin

    created_at = monitor.get("created_at")
    if isinstance(created_at, datetime):
        return created_at.isoformat()
    if isinstance(created_at, str) and created_at.strip():
        return created_at
    return None


async def _resolve_incident_monitor_payload(incident: dict) -> dict:
    monitor_id = incident.get("monitor_id")
    monitor_type = str(incident.get("monitor_type") or "").strip().lower()
    return await resolve_monitor_context(monitor_id, monitor_type)


def _fallback_incident_monitor_payload(incident: dict) -> dict:
    monitor_id = incident.get("monitor_id")
    if is_placeholder_monitor_id(monitor_id):
        monitor_id = None
    return {
        "monitor_id": str(monitor_id) if monitor_id else None,
        "monitor_source": None,
        "monitor_name": None
    }


async def _format_incident_payloads(
    incidents: list[dict],
    *,
    include_resolved_retention: bool
) -> list[dict]:
    if not incidents:
        return []

    incident_monitor_payloads = await asyncio.gather(
        *[_resolve_incident_monitor_payload(inc) for inc in incidents],
        return_exceptions=True
    )

    items = []
    for inc, monitor_payload in zip(incidents, incident_monitor_payloads):
        if isinstance(monitor_payload, Exception):
            logger.warning("Failed to enrich incident monitor payload for %s: %s", inc.get("id"), monitor_payload)
            monitor_payload = _fallback_incident_monitor_payload(inc)

        started_at = inc.get("started_at")
        payload = {
            "id": str(inc["id"]),
            "monitor_type": inc.get("monitor_type"),
            "monitor_id": monitor_payload.get("monitor_id"),
            "monitor_source": monitor_payload.get("monitor_source"),
            "monitor_name": monitor_payload.get("monitor_name"),
            "incident_type": inc.get("incident_type"),
            "source": inc.get("source") or "monitor",
            "title": inc.get("title"),
            "description": inc.get("description"),
            "status": inc.get("status"),
            "started_at": started_at.isoformat() if started_at else None
        }

        if include_resolved_retention:
            resolved_at = inc.get("resolved_at")
            resolved_expires_at = resolved_at + timedelta(hours=48) if resolved_at else None
            payload["template_key"] = inc.get("template_key")
            payload["resolved_at"] = resolved_at.isoformat() if resolved_at else None
            payload["resolved_expires_at"] = resolved_expires_at.isoformat() if resolved_expires_at else None

        items.append(payload)

    return items


def _determine_overall_status(incidents: list[dict]) -> str:
    open_incidents = [inc for inc in incidents if str(inc.get("status") or "").strip().lower() == "open"]
    has_open_down = any(str(inc.get("incident_type") or "").strip().lower() == "down" for inc in open_incidents)
    has_open_warning = any(str(inc.get("incident_type") or "").strip().lower() == "warning" for inc in open_incidents)
    if has_open_down:
        return "down"
    if has_open_warning:
        return "degraded"
    return "operational"


async def _refresh_incident_fields(payload: dict) -> dict:
    incidents, status_incidents = await asyncio.gather(
        db.get_open_incidents(),
        db.get_public_status_incidents(resolved_retention_hours=48),
        return_exceptions=False
    )
    incident_data, status_incident_data = await asyncio.gather(
        _format_incident_payloads(incidents, include_resolved_retention=False),
        _format_incident_payloads(status_incidents, include_resolved_retention=True),
        return_exceptions=False
    )

    refreshed = dict(payload)
    refreshed["incidents"] = incident_data
    refreshed["status_incidents"] = status_incident_data
    refreshed["status"] = _determine_overall_status(incidents)
    return refreshed


async def _refresh_cached_monitor_flags(payload: dict) -> dict:
    monitors = payload.get("monitors")
    if not isinstance(monitors, list) or not monitors:
        return payload

    uptime_monitors, server_monitors, heartbeat_monitors = await asyncio.gather(
        db.get_uptime_monitors(enabled_only=True),
        db.get_server_monitors(enabled_only=True),
        db.get_heartbeat_monitors(enabled_only=True),
        return_exceptions=False
    )

    uptime_by_id = {str(m.get("id")): m for m in uptime_monitors}
    server_by_id = {str(m.get("id")): m for m in server_monitors}
    heartbeat_by_id = {str(m.get("id")): m for m in heartbeat_monitors}

    refreshed_monitors = []
    for monitor in monitors:
        monitor_id = str(monitor.get("id") or "")
        monitor_type = str(monitor.get("type") or "")
        heartbeat_type = str(monitor.get("heartbeat_type") or "")

        source = None
        if monitor_type == "uptime":
            source = uptime_by_id.get(monitor_id)
        elif monitor_type == "heartbeat" and heartbeat_type == "server_agent":
            source = server_by_id.get(monitor_id)
        elif monitor_type == "heartbeat":
            source = heartbeat_by_id.get(monitor_id)

        if source is None:
            # Monitor no longer exists in the DB/cache — keep the cached
            # version as-is so it doesn't vanish mid-refresh.
            refreshed_monitors.append(dict(monitor))
            continue

        item = dict(monitor)
        item["is_public"] = source.get("is_public", False)
        item["maintenance_mode"] = source.get("maintenance_mode", False)

        live_status = source.get("status")
        if live_status is not None:
            item["status"] = live_status

        item["status_since"] = _status_since_iso(source, live_status)

        if monitor_type == "uptime":
            last_checkin = source.get("last_checkin_at")
            item["last_check_at"] = last_checkin.isoformat() if isinstance(last_checkin, datetime) else item.get("last_check_at")
            if source.get("status") == "up" and isinstance(last_checkin, datetime):
                item["last_up_at"] = last_checkin.isoformat()
        elif monitor_type == "heartbeat" and heartbeat_type == "server_agent":
            last_report_at = source.get("last_report_at")
            item["last_report_at"] = last_report_at.isoformat() if isinstance(last_report_at, datetime) else item.get("last_report_at")
        elif monitor_type == "heartbeat":
            last_ping_at = source.get("last_ping_at")
            item["last_ping_at"] = last_ping_at.isoformat() if isinstance(last_ping_at, datetime) else item.get("last_ping_at")

        refreshed_monitors.append(item)

    refreshed = dict(payload)
    refreshed["monitors"] = refreshed_monitors
    return refreshed


@router.get("/status")
async def get_public_status(
    offset: int = Query(0, description="Day offset for history (negative for past)"),
    tz_offset_minutes: int = Query(0, ge=-840, le=840, description="Browser timezone offset in minutes from UTC"),
    sla_range: SlaRange | None = Query(None, description="SLA uptime preset (affects uptime % calculations)")
):
    cache_key = _get_cache_key(offset, sla_range.value if sla_range else None)
    cached_data = await _get_cached_status(cache_key)
    if cached_data is not None:
        try:
            refreshed = await _refresh_incident_fields(cached_data)
            refreshed = await _refresh_cached_monitor_flags(refreshed)
            return refreshed
        except Exception as exc:
            logger.warning("Failed to refresh cached incident fields: %s", exc)
            return cached_data

    try:
        sla_start: datetime | None = None
        sla_end: datetime | None = None
        if sla_range:
            sla_start, sla_end = _get_sla_window(sla_range)

        uptime_monitors, server_monitors, heartbeat_monitors, incidents, status_incidents = await asyncio.gather(
            db.get_uptime_monitors(enabled_only=True),
            db.get_server_monitors(enabled_only=True),
            db.get_heartbeat_monitors(enabled_only=True),
            db.get_open_incidents(),
            db.get_public_status_incidents(resolved_retention_hours=48),
            return_exceptions=False
        )

        uptime_tasks = [_process_uptime_monitor(m, sla_start, sla_end, offset, tz_offset_minutes) for m in uptime_monitors]
        server_tasks = [_process_server_monitor(m, sla_start, sla_end, offset, tz_offset_minutes) for m in server_monitors]
        heartbeat_tasks = [
            _process_heartbeat_monitor(
                m,
                sla_start,
                sla_end,
                offset=offset,
                tz_offset_minutes=tz_offset_minutes,
            )
            for m in heartbeat_monitors
        ]

        all_tasks = uptime_tasks + server_tasks + heartbeat_tasks
        processed_monitors = await asyncio.gather(*all_tasks, return_exceptions=False)

        monitors = list(processed_monitors)
        uptime_values = [m["uptime_percentage"] for m in monitors if m["uptime_percentage"] is not None]

        incident_data, status_incident_data = await asyncio.gather(
            _format_incident_payloads(incidents, include_resolved_retention=False),
            _format_incident_payloads(status_incidents, include_resolved_retention=True),
            return_exceptions=False
        )
        overall_status = _determine_overall_status(incidents)
        overall_uptime = round(sum(uptime_values) / len(uptime_values), 4) if uptime_values else None

        result = {
            "overall_uptime": overall_uptime,
            "monitors": monitors,
            "incidents": incident_data,
            "status_incidents": status_incident_data,
            "status": overall_status
        }

        await _set_cached_status(cache_key, result)
        return result

    except Exception as e:
        stale_data = await _get_stale_cached_status(cache_key)
        if stale_data is not None:
            logger.warning(
                "Error getting public status for key=%s; serving stale cache: %s",
                cache_key,
                e
            )
            return stale_data
        logger.error("Error getting public status: %s", e)
        raise HTTPException(status_code=500, detail="Failed to retrieve status")


async def _calculate_uptime_from_minutes(
    monitor_id,
    start: datetime,
    end: datetime,
    has_data: bool = True
) -> float | None:
    """Maintenance minutes are excluded from the denominator."""
    if start >= end:
        return None if not has_data else 100.0

    counts = await db.count_monitor_minutes(monitor_id, start, end)
    up = counts.get("up", 0)
    down = counts.get("down", 0)
    total = up + down
    if total == 0:
        return None if not has_data else 100.0
    return round((up / total) * 100, 4)


async def _get_daily_status_from_minutes(
    monitor_id,
    days: int = 7,
    offset: int = 0,
    first_data_at: datetime | None = None,
    maintenance_type: str = "website",
    tz_offset_minutes: int = 0
) -> list:
    # Force UTC day boundaries for deterministic results across timezones.
    tz_offset_minutes = 0
    now_utc = utcnow()
    local_today = now_utc.date()
    base_local_date = _get_base_local_date(now_utc, offset, tz_offset_minutes)
    observed_start_local = first_data_at.date() if first_data_at else None

    range_start_local = base_local_date - timedelta(days=days - 1)
    range_start_utc = _local_day_bounds_utc(range_start_local, tz_offset_minutes)[0]
    range_end_utc = _local_day_bounds_utc(base_local_date, tz_offset_minutes)[1]

    maintenance_events = await db.get_maintenance_events(
        maintenance_type, monitor_id, range_start_utc, range_end_utc
    )
    maintenance_tooltips = _build_maintenance_tooltips(
        maintenance_events, base_local_date, days, now_utc, tz_offset_minutes
    )

    all_minutes = await db.get_monitor_minutes(monitor_id, range_start_utc, range_end_utc)

    daily_counts: dict = {}
    for m in all_minutes:
        day = m["minute"].date()
        if day not in daily_counts:
            daily_counts[day] = {"up": 0, "down": 0, "maintenance": 0}
        status = m["status"]
        daily_counts[day][status] = daily_counts[day].get(status, 0) + 1

    result: list = []
    for i in range(days - 1, -1, -1):
        day_local = base_local_date - timedelta(days=i)

        if day_local > local_today:
            result.append("not_created")
            continue

        maintenance_tooltip = maintenance_tooltips.get(day_local)

        if not observed_start_local or day_local < observed_start_local:
            result.append("not_created")
            continue

        counts = daily_counts.get(day_local, {"up": 0, "down": 0, "maintenance": 0})
        up = counts.get("up", 0)
        down = counts.get("down", 0)
        maintenance = counts.get("maintenance", 0)

        if maintenance > 0 and up == 0 and down == 0:
            tooltip = maintenance_tooltip or "Maintenance"
            result.append({"status": "maintenance", "tooltip": tooltip})
            continue

        if up == 0 and down == 0:
            if maintenance_tooltip:
                result.append({"status": "maintenance", "tooltip": maintenance_tooltip})
            else:
                result.append("not_created")
            continue

        if down == 0:
            result.append("up")
        elif down <= _PARTIAL_DOWNTIME_MINUTES:
            result.append({"status": "partial", "downtime_minutes": round(float(down), 2)})
        else:
            result.append({"status": "down", "downtime_minutes": round(float(down), 2)})

    return result


async def _get_detailed_uptime_stats_from_minutes(
    monitor_id,
    first_data_at: datetime | None,
    has_data: bool = True
) -> dict:
    now = utcnow()

    if not has_data:
        return {"24h": None, "7d": None, "30d": None, "year": None, "total": None, "first_data_at": None}

    year_start = datetime(now.year, 1, 1)
    total_start = first_data_at or year_start

    uptime_24h, uptime_7d, uptime_30d, uptime_year, uptime_total = await asyncio.gather(
        _calculate_uptime_from_minutes(monitor_id, now - timedelta(hours=24), now, has_data=has_data),
        _calculate_uptime_from_minutes(monitor_id, now - timedelta(days=7), now, has_data=has_data),
        _calculate_uptime_from_minutes(monitor_id, now - timedelta(days=30), now, has_data=has_data),
        _calculate_uptime_from_minutes(monitor_id, year_start, now, has_data=has_data),
        _calculate_uptime_from_minutes(monitor_id, total_start, now, has_data=has_data),
    )

    return {
        "24h": uptime_24h,
        "7d": uptime_7d,
        "30d": uptime_30d,
        "year": uptime_year,
        "total": uptime_total,
        "first_data_at": first_data_at.isoformat() if first_data_at else None,
    }


async def _get_monthly_archives_from_minutes(monitor_id, created_at: datetime) -> list:
    now = utcnow()
    archives = []

    for i in range(12):
        year = now.year
        month = now.month - i
        while month <= 0:
            year -= 1
            month += 12

        month_start = datetime(year, month, 1)
        if created_at and month_start < created_at.replace(day=1):
            break

        if month == 12:
            month_end = datetime(year + 1, 1, 1)
        else:
            month_end = datetime(year, month + 1, 1)

        effective_end = min(now, month_end)
        uptime = await _calculate_uptime_from_minutes(monitor_id, month_start, effective_end, has_data=True)

        archives.append({
            "month": month_start.strftime("%b %Y"),
            "year": year,
            "month_num": month,
            "uptime": uptime if uptime is not None else 100.0
        })

    return archives


async def _process_uptime_monitor(
    m: dict,
    sla_start: datetime | None,
    sla_end: datetime | None,
    offset: int,
    tz_offset_minutes: int
) -> dict:
    now = utcnow()
    monitor_id = m["id"]

    status = m.get("status", "unknown")
    last_checkin_at = m.get("last_checkin_at")
    has_data = last_checkin_at is not None

    multi_stats = await db.get_uptime_multi_period_stats(monitor_id)
    first_check = (multi_stats or {}).get("first_check")
    first_data_at = first_check or (m.get("created_at") if has_data else None)

    response_time_avg = (multi_stats or {}).get("avg_response_time")
    if response_time_avg is not None:
        try:
            response_time_avg = float(response_time_avg)
        except Exception:
            response_time_avg = None

    if sla_start and sla_end:
        uptime_pct = await _calculate_uptime_from_minutes(monitor_id, sla_start, sla_end, has_data=has_data)
    else:
        start = first_data_at or m.get("created_at") or now
        uptime_pct = await _calculate_uptime_from_minutes(monitor_id, start, now, has_data=has_data)

    history = await _get_daily_status_from_minutes(
        monitor_id, days=7, offset=offset,
        first_data_at=first_data_at,
        maintenance_type="website",
        tz_offset_minutes=tz_offset_minutes
    )

    return {
        "id": str(monitor_id),
        "name": m["name"],
        "type": "uptime",
        "monitor_kind": "website",
        "target": m["target"],
        "status": status,
        "status_since": _status_since_iso(m, status),
        "first_data_at": first_data_at.isoformat() if first_data_at else None,
        "last_check_at": last_checkin_at.isoformat() if last_checkin_at else None,
        "last_up_at": last_checkin_at.isoformat() if last_checkin_at and status == "up" else None,
        "uptime_percentage": uptime_pct,
        "response_time_avg": response_time_avg,
        "category": m.get("category"),
        "history": history,
        "is_public": m.get("is_public", False),
        "maintenance_mode": m.get("maintenance_mode", False),
        "created_at": m.get("created_at").isoformat() if m.get("created_at") else None
    }


async def _process_server_monitor(
    m: dict,
    sla_start: datetime | None,
    sla_end: datetime | None,
    offset: int,
    tz_offset_minutes: int
) -> dict:
    now = utcnow()
    monitor_id = m["id"]

    status = m.get("status", "unknown")
    last_checkin_at = m.get("last_checkin_at")
    has_data = last_checkin_at is not None
    first_data_at = m.get("created_at") if has_data else None

    history = await db.get_server_history(monitor_id, limit=1)
    metrics = {}
    if history:
        latest = history[0]
        metrics = {
            "cpu": latest.get("cpu_percent"),
            "ram": latest.get("ram_percent"),
            "network_in": latest.get("network_in"),
            "network_out": latest.get("network_out"),
            "disk_percent": latest.get("disk_percent"),
            "load_1": latest.get("load_1"),
            "load_5": latest.get("load_5"),
            "load_15": latest.get("load_15"),
            "cpu_io_wait": latest.get("cpu_io_wait"),
            "cpu_steal": latest.get("cpu_steal"),
        }

    if sla_start and sla_end:
        uptime_pct = await _calculate_uptime_from_minutes(monitor_id, sla_start, sla_end, has_data=has_data)
    else:
        start = first_data_at or m.get("created_at") or now
        uptime_pct = await _calculate_uptime_from_minutes(monitor_id, start, now, has_data=has_data)

    daily_history = await _get_daily_status_from_minutes(
        monitor_id, days=7, offset=offset,
        first_data_at=first_data_at,
        maintenance_type="server_agent",
        tz_offset_minutes=tz_offset_minutes
    )

    return {
        "id": str(monitor_id),
        "name": m["name"],
        "type": "heartbeat",
        "heartbeat_type": "server_agent",
        "monitor_kind": "heartbeat",
        "hostname": m.get("hostname"),
        "os": m.get("os"),
        "status": status,
        "status_since": _status_since_iso(m, status),
        "metrics": metrics,
        "history": daily_history,
        "last_report_at": m.get("last_report_at").isoformat() if m.get("last_report_at") else None,
        "first_data_at": first_data_at.isoformat() if first_data_at else None,
        "uptime_percentage": uptime_pct,
        "category": m.get("category"),
        "is_public": m.get("is_public", False),
        "maintenance_mode": m.get("maintenance_mode", False),
        "created_at": m.get("created_at").isoformat() if m.get("created_at") else None
    }


async def _process_heartbeat_monitor(
    m: dict,
    sla_start: datetime | None = None,
    sla_end: datetime | None = None,
    offset: int = 0,
    tz_offset_minutes: int = 0,
) -> dict:
    now = utcnow()
    monitor_id = m["id"]

    status = m.get("status", "unknown")
    last_checkin_at = m.get("last_checkin_at")
    has_data = last_checkin_at is not None
    first_data_at = m.get("created_at") if has_data else None

    if sla_start and sla_end:
        uptime_pct = await _calculate_uptime_from_minutes(monitor_id, sla_start, sla_end, has_data=has_data)
    else:
        start = first_data_at or (now - timedelta(days=90))
        uptime_pct = await _calculate_uptime_from_minutes(monitor_id, start, now, has_data=has_data)

    daily_history = await _get_daily_status_from_minutes(
        monitor_id,
        days=7,
        offset=offset,
        first_data_at=first_data_at,
        maintenance_type="heartbeat",
        tz_offset_minutes=tz_offset_minutes,
    )

    return {
        "id": str(monitor_id),
        "name": m["name"],
        "type": "heartbeat",
        "heartbeat_type": m.get("heartbeat_type", "cronjob"),
        "monitor_kind": "heartbeat",
        "status": status,
        "status_since": _status_since_iso(m, status),
        "first_data_at": first_data_at.isoformat() if first_data_at else None,
        "last_ping_at": m.get("last_ping_at").isoformat() if m.get("last_ping_at") else None,
        "category": m.get("category"),
        "uptime_percentage": uptime_pct,
        "history": daily_history,
        "is_public": m.get("is_public", False),
        "maintenance_mode": m.get("maintenance_mode", False),
        "created_at": m.get("created_at").isoformat() if m.get("created_at") else None
    }


@router.get("/monitor/heartbeat/server-agent/{monitor_id}")
async def get_public_heartbeat_server_agent_monitor(
    monitor_id: str,
    tz_offset_minutes: int = Query(0, ge=-840, le=840, description="Browser timezone offset in minutes from UTC")
):
    try:
        monitor_uuid = _parse_uuid(monitor_id)
        if not monitor_uuid:
            raise HTTPException(status_code=404, detail="Monitor not found")

        monitor = await db.get_server_monitor_by_id(monitor_uuid)
        if not monitor:
            raise HTTPException(status_code=404, detail="Monitor not found")

        if not monitor.get("is_public", False):
            raise HTTPException(status_code=404, detail="Monitor not found")
        if monitor.get("maintenance_mode", False):
            raise HTTPException(status_code=404, detail="Monitor in maintenance mode")

        status = monitor.get("status", "unknown")
        last_checkin_at = monitor.get("last_checkin_at")
        has_data = last_checkin_at is not None
        first_data_at = monitor.get("created_at") if has_data else None

        seven_day_history, uptime_stats, history = await asyncio.gather(
            _get_daily_status_from_minutes(
                monitor["id"], days=7,
                first_data_at=first_data_at,
                maintenance_type="server_agent",
                tz_offset_minutes=tz_offset_minutes
            ),
            _get_detailed_uptime_stats_from_minutes(
                monitor["id"], first_data_at, has_data
            ),
            db.get_server_history(monitor["id"], limit=1),
            return_exceptions=False
        )

        metrics = {}
        if history:
            latest = history[0]
            metrics = {
                "cpu": latest.get("cpu_percent"),
                "ram": latest.get("ram_percent"),
                "network_in": latest.get("network_in"),
                "network_out": latest.get("network_out"),
                "disk_percent": latest.get("disk_percent"),
                "load_1": latest.get("load_1"),
                "load_5": latest.get("load_5"),
                "load_15": latest.get("load_15"),
            }

        return {
            "id": str(monitor["id"]),
            "name": monitor["name"],
            "type": "heartbeat",
            "heartbeat_type": "server_agent",
            "monitor_kind": "heartbeat",
            "hostname": monitor.get("hostname"),
            "os": monitor.get("os"),
            "status": status,
            "metrics": metrics,
            "history": seven_day_history,
            "last_report_at": monitor.get("last_report_at").isoformat() if monitor.get("last_report_at") else None,
            "status_since": _status_since_iso(monitor, status),
            "first_data_at": uptime_stats.get("first_data_at") or (first_data_at.isoformat() if first_data_at else None),
            "created_at": monitor.get("created_at").isoformat() if monitor.get("created_at") else None,
            "category": monitor.get("category"),
            "uptime_percentage": uptime_stats.get("total", 100.0),
            "uptime_stats": uptime_stats
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error getting heartbeat server-agent monitor: %s", e)
        raise HTTPException(status_code=500, detail="Failed to retrieve monitor")


@router.get("/monitor/heartbeat/server-agent/{monitor_id}/history")
async def get_public_heartbeat_server_agent_history(
    monitor_id: str,
    hours: int = Query(24, ge=1, le=52560, description="Hours of history (max 6 years)"),
    start: datetime | None = Query(None, description="Start timestamp (ISO) for range mode"),
    end: datetime | None = Query(None, description="End timestamp (ISO) for range mode")
):
    try:
        monitor_uuid = _parse_uuid(monitor_id)
        if not monitor_uuid:
            raise HTTPException(status_code=404, detail="Monitor not found")

        monitor = await db.get_server_monitor_by_id(monitor_uuid)
        if not monitor or not monitor.get("is_public", False):
            raise HTTPException(status_code=404, detail="Monitor not found")
        if monitor.get("maintenance_mode", False):
            raise HTTPException(status_code=404, detail="Monitor in maintenance mode")

        if start or end:
            if not start or not end:
                raise HTTPException(status_code=400, detail="start and end are required together")
            if start >= end:
                raise HTTPException(status_code=400, detail="start must be before end")
            history = await db.get_server_history_range(monitor_uuid, start, end)
        else:
            if hours > 720:
                history = await db.get_server_history_aggregated(monitor_uuid, hours=hours, interval='day')
            elif hours > 72:
                history = await db.get_server_history_aggregated(monitor_uuid, hours=hours, interval='hour')
            elif hours > 12:
                history = await db.get_server_history_aggregated(monitor_uuid, hours=hours, interval='15min')
            else:
                history = await db.get_server_history(monitor_uuid, hours=hours)

        return [
            {
                "timestamp": h["timestamp"].isoformat(),
                "cpu_percent": h.get("cpu_percent"),
                "ram_percent": h.get("ram_percent"),
                "load_1": h.get("load_1"),
                "load_5": h.get("load_5"),
                "load_15": h.get("load_15"),
                "cpu_io_wait": h.get("cpu_io_wait"),
                "cpu_steal": h.get("cpu_steal"),
                "cpu_user": h.get("cpu_user"),
                "cpu_system": h.get("cpu_system"),
                "ram_swap_percent": h.get("ram_swap_percent"),
                "ram_buff_percent": h.get("ram_buff_percent"),
                "ram_cache_percent": h.get("ram_cache_percent"),
                "disk_percent": h.get("disk_percent"),
                "network_in": h.get("network_in"),
                "network_out": h.get("network_out"),
            }
            for h in history
        ]
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error getting heartbeat server-agent history: %s", e)
        raise HTTPException(status_code=500, detail="Failed to retrieve history")


@router.get("/monitor/heartbeat/server-agent/{monitor_id}/history/months")
async def get_heartbeat_server_agent_monthly_archives(monitor_id: str):
    try:
        monitor_uuid = _parse_uuid(monitor_id)
        if not monitor_uuid:
            raise HTTPException(status_code=404, detail="Monitor not found")

        monitor = await db.get_server_monitor_by_id(monitor_uuid)
        if not monitor or not monitor.get("is_public", False):
            raise HTTPException(status_code=404, detail="Monitor not found")
        if monitor.get("maintenance_mode", False):
            raise HTTPException(status_code=404, detail="Monitor in maintenance mode")

        created_at = monitor.get("created_at") or utcnow()
        archives = await _get_monthly_archives_from_minutes(monitor_uuid, created_at)

        return {
            "monitor_id": str(monitor_id),
            "monitor_name": monitor["name"],
            "archives": archives
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error getting heartbeat server-agent monthly archives: %s", e)
        raise HTTPException(status_code=500, detail="Failed to retrieve archives")


@router.get("/monitor/uptime/{monitor_id}")
async def get_public_uptime_monitor(
    monitor_id: str,
    tz_offset_minutes: int = Query(0, ge=-840, le=840, description="Browser timezone offset in minutes from UTC")
):
    try:
        monitor_uuid = _parse_uuid(monitor_id)
        if not monitor_uuid:
            raise HTTPException(status_code=404, detail="Monitor not found")

        monitor = await db.get_uptime_monitor_by_id(monitor_uuid)
        if not monitor:
            raise HTTPException(status_code=404, detail="Monitor not found")

        if not monitor.get("is_public", False):
            raise HTTPException(status_code=404, detail="Monitor not found")
        if monitor.get("maintenance_mode", False):
            raise HTTPException(status_code=404, detail="Monitor in maintenance mode")

        status = monitor.get("status", "unknown")
        last_checkin_at = monitor.get("last_checkin_at")
        has_data = last_checkin_at is not None

        multi_stats = await db.get_uptime_multi_period_stats(monitor["id"])
        first_check = (multi_stats or {}).get("first_check")
        first_data_at = first_check or (monitor.get("created_at") if has_data else None)

        seven_day_history, uptime_stats = await asyncio.gather(
            _get_daily_status_from_minutes(
                monitor["id"], days=7,
                first_data_at=first_data_at,
                maintenance_type="website",
                tz_offset_minutes=tz_offset_minutes
            ),
            _get_detailed_uptime_stats_from_minutes(
                monitor["id"], first_data_at, has_data
            ),
            return_exceptions=False
        )

        return {
            "id": str(monitor["id"]),
            "name": monitor["name"],
            "type": "uptime",
            "monitor_kind": "website",
            "target": monitor.get("target"),
            "status": status,
            "status_since": _status_since_iso(monitor, status),
            "uptime_percentage": uptime_stats.get("total", 100.0),
            "category": monitor.get("category"),
            "history": seven_day_history,
            "created_at": monitor.get("created_at").isoformat() if monitor.get("created_at") else None,
            "first_data_at": uptime_stats.get("first_data_at") or (first_data_at.isoformat() if first_data_at else None),
            "last_check_at": last_checkin_at.isoformat() if last_checkin_at else None,
            "last_up_at": last_checkin_at.isoformat() if last_checkin_at and status == "up" else None,
            "timeout": monitor.get("timeout", 5),
            "uptime_stats": uptime_stats
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error getting uptime monitor: %s", e)
        raise HTTPException(status_code=500, detail="Failed to retrieve monitor")


@router.get("/monitor/uptime/{monitor_id}/history/months")
async def get_uptime_monthly_archives(monitor_id: str):
    try:
        monitor_uuid = _parse_uuid(monitor_id)
        if not monitor_uuid:
            raise HTTPException(status_code=404, detail="Monitor not found")

        monitor = await db.get_uptime_monitor_by_id(monitor_uuid)
        if not monitor or not monitor.get("is_public", False):
            raise HTTPException(status_code=404, detail="Monitor not found")
        if monitor.get("maintenance_mode", False):
            raise HTTPException(status_code=404, detail="Monitor in maintenance mode")

        created_at = monitor.get("created_at") or utcnow()
        archives = await _get_monthly_archives_from_minutes(monitor_uuid, created_at)

        return {
            "monitor_id": str(monitor_id),
            "monitor_name": monitor["name"],
            "archives": archives
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error getting monthly archives: %s", e)
        raise HTTPException(status_code=500, detail="Failed to retrieve archives")


@router.get("/monitor/heartbeat/{monitor_id}")
async def get_public_heartbeat_monitor(
    monitor_id: str,
    tz_offset_minutes: int = Query(0, ge=-840, le=840, description="Browser timezone offset in minutes from UTC")
):
    try:
        monitor_uuid = _parse_uuid(monitor_id)
        if not monitor_uuid:
            raise HTTPException(status_code=404, detail="Monitor not found")

        monitor = await db.get_heartbeat_monitor_by_id(monitor_uuid)
        if not monitor:
            server_monitor = await db.get_server_monitor_by_id(monitor_uuid)
            if server_monitor:
                return await get_public_heartbeat_server_agent_monitor(
                    str(monitor_uuid),
                    tz_offset_minutes=tz_offset_minutes
                )
            raise HTTPException(status_code=404, detail="Monitor not found")

        if not monitor.get("is_public", False):
            raise HTTPException(status_code=404, detail="Monitor not found")
        if monitor.get("maintenance_mode", False):
            raise HTTPException(status_code=404, detail="Monitor in maintenance mode")

        if monitor.get("heartbeat_type") == "server_agent":
            server_monitor = await db.get_server_monitor_by_id(monitor_uuid)
            if server_monitor:
                return await get_public_heartbeat_server_agent_monitor(
                    str(monitor_uuid),
                    tz_offset_minutes=tz_offset_minutes
                )

        status = monitor.get("status", "unknown")
        last_checkin_at = monitor.get("last_checkin_at")
        has_data = last_checkin_at is not None
        first_data_at = monitor.get("created_at") if has_data else None

        seven_day_history, uptime_stats = await asyncio.gather(
            _get_daily_status_from_minutes(
                monitor["id"], days=7,
                first_data_at=first_data_at,
                maintenance_type="heartbeat",
                tz_offset_minutes=tz_offset_minutes
            ),
            _get_detailed_uptime_stats_from_minutes(
                monitor["id"], first_data_at, has_data
            ),
            return_exceptions=False
        )

        return {
            "id": str(monitor["id"]),
            "name": monitor["name"],
            "type": "heartbeat",
            "heartbeat_type": monitor.get("heartbeat_type", "cronjob"),
            "monitor_kind": "heartbeat",
            "status": status,
            "status_since": _status_since_iso(monitor, status),
            "category": monitor.get("category"),
            "last_ping_at": monitor.get("last_ping_at").isoformat() if monitor.get("last_ping_at") else None,
            "first_data_at": uptime_stats.get("first_data_at") or (first_data_at.isoformat() if first_data_at else None),
            "created_at": monitor.get("created_at").isoformat() if monitor.get("created_at") else None,
            "timeout": monitor.get("timeout", 60),
            "grace_period": monitor.get("grace_period", 5),
            "uptime_percentage": uptime_stats.get("total", 100.0),
            "history": seven_day_history,
            "uptime_stats": uptime_stats
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error getting heartbeat monitor: %s", e)
        raise HTTPException(status_code=500, detail="Failed to retrieve monitor")


def _get_sla_window(sla_range: SlaRange) -> tuple[datetime, datetime]:
    """Return an (inclusive start, exclusive end) UTC window for SLA presets."""
    now = utcnow()

    if sla_range == SlaRange.last7days:
        return now - timedelta(days=7), now
    if sla_range == SlaRange.last30days:
        return now - timedelta(days=30), now
    if sla_range == SlaRange.this_month:
        start = datetime(now.year, now.month, 1)
        return start, now
    if sla_range == SlaRange.last_month:
        if now.month == 1:
            start = datetime(now.year - 1, 12, 1)
            end = datetime(now.year, 1, 1)
        else:
            start = datetime(now.year, now.month - 1, 1)
            end = datetime(now.year, now.month, 1)
        return start, end
    if sla_range == SlaRange.year_to_date:
        return datetime(now.year, 1, 1), now
    if sla_range == SlaRange.last_year:
        return datetime(now.year - 1, 1, 1), datetime(now.year, 1, 1)

    return now - timedelta(days=7), now


def _format_utc_time(dt: datetime) -> str:
    hour = dt.hour
    minute = dt.minute
    suffix = "AM" if hour < 12 else "PM"
    hour12 = hour % 12 or 12
    if minute == 0:
        return f"{hour12}{suffix}"
    return f"{hour12}:{minute:02d}{suffix}"


def _tz_delta(tz_offset_minutes: int) -> timedelta:
    return timedelta(minutes=int(tz_offset_minutes or 0))


def _to_local(utc_dt: datetime, tz_offset_minutes: int) -> datetime:
    return utc_dt + _tz_delta(tz_offset_minutes)


def _local_day_bounds_utc(local_day: date, tz_offset_minutes: int) -> tuple[datetime, datetime]:
    """Return UTC [start, end) bounds for a local calendar day."""
    local_start = datetime.combine(local_day, time.min)
    utc_start = local_start - _tz_delta(tz_offset_minutes)
    utc_end = utc_start + timedelta(days=1)
    return utc_start, utc_end


def _get_base_local_date(now_utc: datetime, offset_days: int, tz_offset_minutes: int) -> date:
    local_now = _to_local(now_utc, tz_offset_minutes)
    return (local_now + timedelta(days=offset_days)).date()


def _build_maintenance_tooltips(
    events: list,
    base_local_date: date,
    days: int,
    now_utc: datetime,
    tz_offset_minutes: int = 0
) -> dict:
    if not events:
        return {}
    tooltips: dict = {}
    for i in range(days - 1, -1, -1):
        day_date = base_local_date - timedelta(days=i)
        day_start, day_end = _local_day_bounds_utc(day_date, tz_offset_minutes)
        ranges = []
        for ev in events:
            ev_start = ev.get("start_at")
            ev_end = ev.get("end_at") or now_utc
            if not ev_start:
                continue
            if ev_start >= day_end or ev_end <= day_start:
                continue
            overlap_start = max(ev_start, day_start)
            overlap_end = min(ev_end, day_end)
            if overlap_start >= overlap_end:
                continue
            ranges.append((overlap_start, overlap_end, ev.get("end_at") is None))
        if not ranges:
            continue
        parts = []
        for start, end, is_open in ranges:
            start_str = _format_utc_time(start)
            if is_open and day_date == _to_local(now_utc, tz_offset_minutes).date():
                parts.append(f"{start_str} UTC to present")
            else:
                end_str = _format_utc_time(end)
                parts.append(f"{start_str} UTC to {end_str} UTC")
        tooltips[day_date] = "Maintenance: " + ", ".join(parts)
    return tooltips


@router.get("/config")
async def get_status_config():
    logo_url = settings.STATUS_LOGO if settings.STATUS_LOGO else "/static/images/logo.png"

    return {
        "logo_url": logo_url,
        "app_name": settings.APP_NAME,
        "company_name": settings.COMPANY_NAME,
        "status_page_title": f"{settings.STATUS_PAGE_TITLE.strip() or 'Statrix Status'} - Powered By Statrix",
    }
