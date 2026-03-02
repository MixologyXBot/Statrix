# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import asyncio
import logging
import os
import socket
from datetime import datetime, timedelta

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from ..config import settings
from ..database import GRACE_PERIOD_MINUTES, db
from ..status_summary import status_summary_service
from ..utils.cache import invalidate_status_cache
from ..utils.email import send_down_alert, send_up_alert
from ..utils.time import utcnow

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None
_INSTANCE_ID = f"{socket.gethostname()}:{os.getpid()}"

_alerted_monitors: set[str] = set()


def _floor_to_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def _monitor_key(monitor_id) -> str:
    return str(monitor_id)


def _monitor_target(monitor: dict, monitor_type: str) -> str:
    if monitor_type == "uptime":
        return monitor.get("target") or ""
    if monitor_type == "server":
        return monitor.get("hostname") or monitor.get("sid") or ""
    return monitor.get("sid") or ""


async def _probe_uptime_target(monitor: dict) -> bool:
    url = monitor.get("target", "")
    timeout_seconds = max(5.0, min(60.0, float(monitor.get("timeout") or 5)))
    try:
        async with httpx.AsyncClient(
            timeout=timeout_seconds,
            follow_redirects=bool(monitor.get("follow_redirects", True)),
            verify=bool(monitor.get("verify_ssl", True)),
        ) as client:
            response = await client.get(url)
            elapsed_ms = None
            try:
                elapsed_ms = int(round(response.elapsed.total_seconds() * 1000))
            except Exception:
                logger.debug("Failed to compute response elapsed time for %s", url, exc_info=True)
            try:
                status_str = "up" if 200 <= response.status_code < 400 else "down"
                await db.create_uptime_check(
                    monitor["id"],
                    status=status_str,
                    response_time_ms=elapsed_ms,
                    status_code=response.status_code,
                )
            except Exception:
                logger.debug("Failed to record uptime check for monitor %s", monitor["id"], exc_info=True)
            return 200 <= response.status_code < 400
    except Exception:
        try:
            await db.create_uptime_check(
                monitor["id"], status="down", error_message="Connection failed"
            )
        except Exception:
            logger.debug("Failed to record down check after connection error for monitor %s", monitor["id"], exc_info=True)
        return False


async def _probe_all_uptime_monitors(monitors: list) -> dict[str, bool]:
    if not monitors:
        return {}
    tasks = []
    keys = []
    for m in monitors:
        key = _monitor_key(m["id"])
        keys.append(key)
        tasks.append(_probe_uptime_target(m))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return {keys[i]: (results[i] is True) for i in range(len(keys))}


async def _run_monitor_sweep() -> None:
    global _alerted_monitors
    lock_acquired = False
    try:
        if settings.MONITOR_LEADER_LOCK_ENABLED and db.cache_service:
            lock_acquired = await db.cache_service.try_acquire_leader_lock(
                lock_name="monitor_sweep",
                owner=_INSTANCE_ID,
                ttl_seconds=settings.MONITOR_LEADER_LOCK_TTL_SECONDS,
            )
            if not lock_acquired:
                logger.debug("Skipped monitor sweep on non-leader worker")
                return

        await db.ensure_cache_available()

        now = utcnow()
        now_minute = _floor_to_minute(now)
        grace_delta = timedelta(minutes=GRACE_PERIOD_MINUTES)

        uptime_monitors = await db.get_uptime_monitors(enabled_only=True)
        server_monitors = await db.get_server_monitors(enabled_only=True)
        heartbeat_monitors = await db.get_heartbeat_monitors(enabled_only=True)

        probe_results = await _probe_all_uptime_monitors(uptime_monitors)

        all_monitors = []
        for m in uptime_monitors:
            all_monitors.append((m, "uptime", "website"))
        for m in server_monitors:
            all_monitors.append((m, "server", "heartbeat-server-agent"))
        for m in heartbeat_monitors:
            if m.get("heartbeat_type") == "server_agent":
                continue
            all_monitors.append((m, "heartbeat", "heartbeat-cronjob"))

        open_incidents = await db.get_open_incidents(source="monitor")
        incidents_by_monitor: dict[str, dict] = {}
        for inc in open_incidents:
            mid = inc.get("monitor_id")
            if mid:
                incidents_by_monitor[_monitor_key(mid)] = inc

        minute_records = []

        for monitor, db_type, display_type in all_monitors:
            monitor_id = monitor.get("id")
            if not monitor_id:
                continue

            key = _monitor_key(monitor_id)
            name = monitor.get("name") or str(monitor_id)
            target = _monitor_target(monitor, db_type)
            cache_kind = (
                "uptime"
                if db_type == "uptime"
                else ("server" if db_type == "server" else "heartbeat")
            )

            notifications_enabled = monitor.get("notifications_enabled", True)

            if monitor.get("maintenance_mode"):
                minute_records.append((monitor_id, now_minute, "maintenance"))
                _alerted_monitors.discard(key)
                continue

            if db_type == "uptime":
                is_up = probe_results.get(key, False)
                if is_up:
                    last_checkin = now_minute
                    current_status = monitor.get("status", "unknown")
                    if current_status == "down":
                        down_since = monitor.get("down_since") or now_minute
                        _alerted_monitors.discard(key)
                        existing = incidents_by_monitor.get(key)
                        if existing:
                            try:
                                await db.resolve_incident(existing["id"])
                                if notifications_enabled:
                                    await send_up_alert(
                                        monitor_name=name,
                                        monitor_type=display_type,
                                        target=target,
                                        down_since=down_since,
                                        recovered_at=now,
                                    )
                                logger.info("UP: %s recovered", name)
                            except Exception:
                                logger.exception(
                                    "Failed to resolve incident for %s", name
                                )
                    await db.update_monitor_status(
                        cache_kind,
                        monitor_id,
                        "up",
                        last_checkin_at=last_checkin,
                        down_since=None,
                    )
                    minute_records.append((monitor_id, now_minute, "up"))
                    continue

            live = await db.get_cached_monitor_state(cache_kind, monitor_id)
            last_checkin_at = live.get("last_checkin_at") if live else monitor.get("last_checkin_at")
            current_status = live.get("status", "unknown") if live else monitor.get("status", "unknown")

            if last_checkin_at is None:
                continue

            elapsed = now_minute - _floor_to_minute(last_checkin_at)

            if elapsed <= grace_delta:
                minute_records.append((monitor_id, now_minute, "up"))
                if current_status != "up":
                    await db.update_monitor_status(
                        cache_kind,
                        monitor_id,
                        "up",
                        last_checkin_at=last_checkin_at,
                        down_since=None,
                    )
            else:
                if current_status != "down":
                    down_since = _floor_to_minute(last_checkin_at) + grace_delta
                    transitioned = True
                    if db_type in {"server", "heartbeat"}:
                        transitioned = await db.mark_monitor_down_if_unchanged(
                            cache_kind=cache_kind,
                            monitor_id=monitor_id,
                            expected_last_checkin_at=last_checkin_at,
                            stale_before=now_minute - grace_delta,
                            down_since=down_since,
                        )
                    else:
                        await db.update_monitor_status(
                            cache_kind,
                            monitor_id,
                            "down",
                            last_checkin_at=last_checkin_at,
                            down_since=down_since,
                        )

                    if not transitioned:
                        continue

                    minute_records.append((monitor_id, now_minute, "down"))

                    if notifications_enabled and key not in _alerted_monitors:
                        existing = incidents_by_monitor.get(key)
                        if not existing:
                            try:
                                await db.create_incident(
                                    monitor_type=(
                                        "uptime" if db_type == "uptime" else "heartbeat"
                                    ),
                                    monitor_id=monitor_id,
                                    incident_type="down",
                                    title=f"{name} is down",
                                    description=f"{name} ({target}) stopped responding.",
                                    source="monitor",
                                )
                                await send_down_alert(
                                    monitor_name=name,
                                    monitor_type=display_type,
                                    target=target,
                                    down_since=down_since,
                                )
                                _alerted_monitors.add(key)
                                logger.info("DOWN: %s (email sent)", name)
                            except Exception:
                                logger.exception(
                                    "Failed to create DOWN incident for %s", name
                                )
                        else:
                            _alerted_monitors.add(key)
                else:
                    minute_records.append((monitor_id, now_minute, "down"))

        if minute_records:
            await db.write_monitor_minutes_batch(minute_records)

        invalidate_status_cache()

    except Exception:
        logger.exception("Monitor sweep failed")
    finally:
        if lock_acquired and settings.MONITOR_LEADER_LOCK_ENABLED and db.cache_service:
            try:
                await db.cache_service.release_leader_lock(
                    lock_name="monitor_sweep",
                    owner=_INSTANCE_ID,
                )
            except Exception:
                logger.exception("Failed releasing monitor sweep leader lock")


async def _rebuild_cache_if_unhealthy() -> None:
    if not db.cache_service:
        return
    if db.cache_warming_up:
        return
    if db.cache_service.healthy:
        return
    try:
        if await db.cache_service.backend.ping():
            await db.cache_service.mark_healthy()
            logger.info("Cache recovered via ping; skipping rebuild")
            return
    except Exception:
        logger.debug("Cache pre-rebuild ping check failed", exc_info=True)
    try:
        logger.warning("Cache unhealthy; attempting rebuild from DB snapshot")
        await db.resync_cache_from_db()
        if settings.STATUS_SUMMARY_ENABLED:
            await status_summary_service.rebuild_from_redis(db)
        await db.cache_service.mark_healthy()
        logger.info("Cache rebuild succeeded")
    except Exception:
        logger.exception("Cache rebuild attempt failed")


async def _run_data_compression() -> None:
    try:
        logger.info("Starting daily data compression")
        results = await db.compress_old_data()
        logger.info("Data compression finished: %s", results)
    except Exception:
        logger.exception("Data compression failed")


def start_monitor_loop() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    _scheduler = AsyncIOScheduler()
    _scheduler.add_job(
        _run_monitor_sweep,
        "interval",
        seconds=60,
        id="monitor_sweep",
        max_instances=1,
        coalesce=True,
        next_run_time=utcnow(),  # run immediately on start
    )
    _scheduler.add_job(
        _rebuild_cache_if_unhealthy,
        "interval",
        seconds=max(5, int(settings.CACHE_REBUILD_INTERVAL_SECONDS or 30)),
        id="cache_rebuild",
        max_instances=1,
        coalesce=True,
    )
    _scheduler.add_job(
        _run_data_compression,
        "cron",
        hour=settings.DATA_COMPRESSION_HOUR_UTC,
        minute=0,
        id="data_compression",
        max_instances=1,
        coalesce=True,
    )
    _scheduler.start()
    logger.info("Monitor loop started (interval=60s, grace=%dm)", GRACE_PERIOD_MINUTES)


def stop_monitor_loop() -> None:
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None


async def handle_checkin(
    monitor_id, cache_kind: str, db_type: str, display_type: str, name: str, target: str
) -> None:
    global _alerted_monitors
    now = utcnow()
    now_minute = _floor_to_minute(now)
    key = _monitor_key(monitor_id)

    monitor = await db.get_cached_monitor_state(cache_kind, monitor_id)
    current_status = monitor.get("status", "unknown")
    down_since = monitor.get("down_since")

    if current_status == "down":
        _alerted_monitors.discard(key)
        try:
            open_incidents = await db.get_open_incidents(source="monitor")
            for inc in open_incidents:
                if _monitor_key(inc.get("monitor_id")) == key:
                    await db.resolve_incident(inc["id"])
                    notifications_enabled = True
                    m = await db.get_cached_monitor_state(cache_kind, monitor_id)
                    if m:
                        notifications_enabled = m.get("notifications_enabled", True)
                    if notifications_enabled:
                        await send_up_alert(
                            monitor_name=name,
                            monitor_type=display_type,
                            target=target,
                            down_since=down_since or now,
                            recovered_at=now,
                        )
                    logger.info("UP: %s recovered (via check-in)", name)
                    break
        except Exception:
            logger.exception("Failed to resolve incident for %s", name)

    await db.update_monitor_status(
        cache_kind, monitor_id, "up", last_checkin_at=now_minute, down_since=None
    )

    await db.write_monitor_minute(monitor_id, now_minute, "up")

    invalidate_status_cache()
