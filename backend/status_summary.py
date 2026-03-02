# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import date, datetime, time, timedelta
from typing import Any

from .config import settings
from .utils.time import utcnow

logger = logging.getLogger(__name__)


STATUS_SUMMARY_VERSION = 1
_STATUS_CODE_NOT_CREATED = 0
_STATUS_CODE_UP = 1
_STATUS_CODE_PARTIAL = 2
_STATUS_CODE_DOWN = 3
_STATUS_CODE_MAINTENANCE = 4


def _utc_day_start(day: date) -> datetime:
    return datetime.combine(day, time.min)


def _utc_day_end(day: date) -> datetime:
    return _utc_day_start(day) + timedelta(days=1)


def _as_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return None
    return None


def _as_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except Exception:
            return None
    return None


def _ensure_len(items: list[Any], size: int, fill: Any) -> list[Any]:
    src = list(items or [])
    if len(src) >= size:
        return src[:size]
    src.extend([fill for _ in range(size - len(src))])
    return src


class StatusSummaryService:
    def __init__(self) -> None:
        self.enabled = bool(getattr(settings, "STATUS_SUMMARY_ENABLED", True))
        self.day_slots = 7
        self.partial_downtime_minutes = float(
            getattr(settings, "STATUS_SUMMARY_PARTIAL_DOWNTIME_MINUTES", 15.0) or 15.0
        )
        self.warmup_delay_seconds = max(
            1, int(getattr(settings, "STATUS_SUMMARY_WARMUP_DELAY_SECONDS", 90) or 90)
        )
        self.flush_interval_seconds = max(
            1, int(getattr(settings, "STATUS_SUMMARY_FLUSH_INTERVAL_SECONDS", 10) or 10)
        )
        self.max_timeline_segments = max(
            4, int(getattr(settings, "STATUS_SUMMARY_MAX_TIMELINE_SEGMENTS", 32) or 32)
        )
        self.redis_prefix = str(
            getattr(settings, "STATUS_SUMMARY_REDIS_PREFIX", "status:summary:v1")
            or "status:summary:v1"
        ).strip() or "status:summary:v1"

        self._records: dict[str, dict[str, Any]] = {}
        self._monitor_kind: dict[str, str] = {}
        self._current_day: date = utcnow().date()
        self._days: list[date] = [
            self._current_day - timedelta(days=i)
            for i in range(self.day_slots - 1, -1, -1)
        ]

        self._dirty_monitor_ids: set[str] = set()
        self._deleted_monitor_ids: set[str] = set()
        self._registry_dirty = False

        self._ready = False
        self._ready_event = asyncio.Event()
        self._warmup_lock = asyncio.Lock()
        self._warmup_task: asyncio.Task | None = None
        self._delayed_warmup_task: asyncio.Task | None = None
        self._flush_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    def _meta_suffix(self) -> str:
        return f"{self.redis_prefix}:meta"

    def _ids_suffix(self) -> str:
        return f"{self.redis_prefix}:ids"

    def _monitor_suffix(self, monitor_id: str) -> str:
        return f"{self.redis_prefix}:m:{monitor_id}"

    def _daily_suffix(self, monitor_id: str) -> str:
        return f"{self.redis_prefix}:d:{monitor_id}"

    def _timeline_suffix(self, monitor_id: str) -> str:
        return f"{self.redis_prefix}:t:{monitor_id}"

    async def start(self, db) -> None:
        if not self.enabled:
            return
        if self._flush_task and not self._flush_task.done():
            return
        self._stop_event.clear()
        self._flush_task = asyncio.create_task(self._flush_loop(db))
        logger.info(
            "Status summary service started (flush=%ss, slots=%s)",
            self.flush_interval_seconds,
            self.day_slots,
        )

    async def stop(self) -> None:
        if not self.enabled:
            return
        self._stop_event.set()
        tasks = [self._flush_task, self._warmup_task, self._delayed_warmup_task]
        for task in tasks:
            if task and not task.done():
                task.cancel()
        for task in tasks:
            if task:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.debug("Status summary task exited with error", exc_info=True)
        self._flush_task = None
        self._warmup_task = None
        self._delayed_warmup_task = None

    def schedule_delayed_warmup(self, db, delay_seconds: int | None = None) -> None:
        if not self.enabled:
            return
        delay = self.warmup_delay_seconds if delay_seconds is None else max(1, int(delay_seconds))
        if self._delayed_warmup_task and not self._delayed_warmup_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        async def _task():
            await asyncio.sleep(delay)
            await self.warmup_from_pg(db, reason="startup_delayed")

        self._delayed_warmup_task = loop.create_task(_task())

    async def ensure_ready(self, db, wait_timeout_seconds: int = 5) -> bool:
        if not self.enabled:
            return False
        if self._ready:
            return True
        self.trigger_warmup_from_pg(db, reason="on_demand")
        if self._ready:
            return True
        timeout = max(0.0, float(wait_timeout_seconds))
        if timeout == 0:
            return self._ready
        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return self._ready
        return self._ready

    def trigger_warmup_from_pg(self, db, reason: str = "manual") -> None:
        if not self.enabled:
            return
        if self._warmup_task and not self._warmup_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._warmup_task = loop.create_task(self.warmup_from_pg(db, reason=reason))

    async def warmup_from_pg(self, db, reason: str = "manual") -> bool:
        if not self.enabled:
            return False
        async with self._warmup_lock:
            if self._ready and reason != "forced":
                return True
            started = utcnow()
            try:
                snapshot = await self._build_snapshot_from_pg(db)
                self._apply_snapshot(snapshot)
                await self._persist_full_to_redis(db)
                elapsed = (utcnow() - started).total_seconds()
                logger.info(
                    "Status summary warmup from PG complete reason=%s monitors=%s duration=%.3fs",
                    reason,
                    len(self._records),
                    elapsed,
                )
                return True
            except Exception:
                logger.exception("Status summary warmup from PG failed reason=%s", reason)
                return False

    async def rebuild_from_redis(self, db) -> bool:
        if not self.enabled:
            return False
        fallback_to_pg = False
        async with self._warmup_lock:
            started = utcnow()
            try:
                snapshot = await self._build_snapshot_from_redis(db)
                if not snapshot:
                    logger.warning("Status summary Redis snapshot missing; falling back to PG rebuild")
                    fallback_to_pg = True
                else:
                    self._apply_snapshot(snapshot)
                    elapsed = (utcnow() - started).total_seconds()
                    logger.info(
                        "Status summary rebuild from Redis complete monitors=%s duration=%.3fs",
                        len(self._records),
                        elapsed,
                    )
                    return True
            except Exception:
                logger.exception("Status summary rebuild from Redis failed")
                return False
        if fallback_to_pg:
            return await self.warmup_from_pg(db, reason="redis_missing_fallback_pg")
        return False

    async def build_monitor_payload(self, offset: int, sla_range: str | None = None) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        if offset != 0 or sla_range:
            return None
        if not self._ready:
            return None
        await self._rollover_if_needed()

        monitors: list[dict[str, Any]] = []
        records = list(self._records.values())
        records.sort(
            key=lambda m: (
                str(m.get("category") or "").lower(),
                str(m.get("name") or "").lower(),
            )
        )
        for rec in records:
            monitors.append(self._record_to_public_monitor(rec))

        uptime_values = [
            m["uptime_percentage"]
            for m in monitors
            if m.get("uptime_percentage") is not None
        ]
        overall_uptime = round(sum(uptime_values) / len(uptime_values), 4) if uptime_values else None
        return {"overall_uptime": overall_uptime, "monitors": monitors}

    def note_monitor_registry_dirty(self) -> None:
        if not self.enabled:
            return
        self._registry_dirty = True

    def note_monitor_created(self, monitor_kind: str, monitor_id: uuid.UUID) -> None:
        if not self.enabled:
            return
        monitor_id_s = str(monitor_id)
        self._monitor_kind[monitor_id_s] = str(monitor_kind)
        self._registry_dirty = True
        self._dirty_monitor_ids.add(monitor_id_s)

    def note_monitor_deleted(self, monitor_id: uuid.UUID) -> None:
        if not self.enabled:
            return
        monitor_id_s = str(monitor_id)
        self._records.pop(monitor_id_s, None)
        self._monitor_kind.pop(monitor_id_s, None)
        self._dirty_monitor_ids.discard(monitor_id_s)
        self._deleted_monitor_ids.add(monitor_id_s)

    def note_monitor_minute(self, monitor_id: uuid.UUID, minute: datetime, status: str) -> None:
        if not self.enabled:
            return
        if not isinstance(minute, datetime):
            return
        if minute.date() != self._current_day:
            return
        status_value = str(status or "").strip().lower()
        if status_value not in {"up", "down", "maintenance"}:
            return
        monitor_id_s = str(monitor_id)
        self._dirty_monitor_ids.add(monitor_id_s)
        rec = self._records.get(monitor_id_s)
        if rec is None:
            return
        if status_value == "maintenance":
            rec["maintenance_mode"] = True
        elif status_value in {"up", "down"} and rec.get("maintenance_mode"):
            rec["maintenance_mode"] = False

        previous_status = str(rec.get("status") or "unknown").strip().lower()
        if previous_status != status_value:
            self._append_today_segment(rec, status_value, minute)
            rec["status"] = status_value
            rec["status_since"] = minute

        if status_value == "up":
            rec["last_up_at"] = minute

    def note_monitor_status(
        self,
        monitor_kind: str,
        monitor_id: uuid.UUID,
        status: str,
        last_checkin_at: datetime | None = None,
        down_since: datetime | None = None,
        status_since: datetime | None = None,
    ) -> None:
        if not self.enabled:
            return
        monitor_id_s = str(monitor_id)
        self._monitor_kind[monitor_id_s] = str(monitor_kind)
        rec = self._records.get(monitor_id_s)
        if rec:
            status_value = str(status or "unknown").strip().lower() or "unknown"
            previous_status = str(rec.get("status") or "unknown").strip().lower() or "unknown"
            changed_at = status_since or down_since or last_checkin_at or utcnow()
            rec["status"] = status_value
            rec["status_since"] = changed_at
            if status_value == "maintenance":
                rec["maintenance_mode"] = True
            elif status_value in {"up", "down"}:
                rec["maintenance_mode"] = False
            if last_checkin_at is not None:
                rec["last_check_at"] = last_checkin_at
                if rec.get("monitor_kind") in {"server", "heartbeat"}:
                    rec["last_checkin_at"] = last_checkin_at
            if status_value == "up":
                rec["last_up_at"] = last_checkin_at or changed_at
            if previous_status != status_value:
                self._append_today_segment(rec, status_value, changed_at)
        self._dirty_monitor_ids.add(monitor_id_s)

    def note_uptime_check(
        self,
        monitor_id: uuid.UUID,
        status: str,
        response_time_ms: int | None,
        checked_at: datetime,
    ) -> None:
        if not self.enabled:
            return
        monitor_id_s = str(monitor_id)
        rec = self._records.get(monitor_id_s)
        if not rec:
            self._dirty_monitor_ids.add(monitor_id_s)
            return
        status_value = str(status or "").strip().lower()
        rec["last_check_at"] = checked_at
        if rec.get("first_data_at") is None:
            rec["first_data_at"] = checked_at
        previous_status = str(rec.get("status") or "unknown").strip().lower()
        if status_value:
            rec["status"] = status_value
            if previous_status != status_value:
                rec["status_since"] = checked_at
                self._append_today_segment(rec, status_value, checked_at)
        if status_value == "up":
            rec["last_up_at"] = checked_at
            if response_time_ms is not None:
                try:
                    rec["rt_sum_up"] = float(rec.get("rt_sum_up") or 0.0) + float(response_time_ms)
                    rec["rt_count_up"] = int(rec.get("rt_count_up") or 0) + 1
                except Exception:
                    logger.debug("Failed to update RT aggregate from uptime check", exc_info=True)
        self._dirty_monitor_ids.add(monitor_id_s)

    def note_server_metrics(
        self,
        monitor_id: uuid.UUID,
        metrics: dict[str, Any],
        reported_at: datetime,
    ) -> None:
        if not self.enabled:
            return
        monitor_id_s = str(monitor_id)
        rec = self._records.get(monitor_id_s)
        if not rec:
            self._dirty_monitor_ids.add(monitor_id_s)
            return
        rec["metrics"] = {
            "cpu": metrics.get("cpu_percent"),
            "ram": metrics.get("ram_percent"),
            "network_in": metrics.get("network_in"),
            "network_out": metrics.get("network_out"),
            "disk_percent": metrics.get("disk_percent"),
            "load_1": metrics.get("load_1"),
            "load_5": metrics.get("load_5"),
            "load_15": metrics.get("load_15"),
            "cpu_io_wait": metrics.get("cpu_io_wait"),
            "cpu_steal": metrics.get("cpu_steal"),
        }
        rec["last_report_at"] = reported_at
        self._dirty_monitor_ids.add(monitor_id_s)

    def note_heartbeat_ping(self, monitor_id: uuid.UUID, pinged_at: datetime) -> None:
        if not self.enabled:
            return
        monitor_id_s = str(monitor_id)
        rec = self._records.get(monitor_id_s)
        if rec:
            rec["last_ping_at"] = pinged_at
            if rec.get("first_data_at") is None:
                rec["first_data_at"] = rec.get("created_at") or pinged_at
        self._dirty_monitor_ids.add(monitor_id_s)

    async def _flush_loop(self, db) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.flush_interval_seconds)
                continue
            except asyncio.TimeoutError:
                pass
            try:
                await self.flush_pending(db)
            except Exception:
                logger.exception("Status summary flush iteration failed")

    async def flush_pending(self, db) -> None:
        if not self.enabled:
            return
        if not self._ready:
            return

        await self._rollover_if_needed()

        deleted_ids, self._deleted_monitor_ids = self._deleted_monitor_ids, set()
        dirty_ids, self._dirty_monitor_ids = self._dirty_monitor_ids, set()
        registry_dirty = self._registry_dirty
        self._registry_dirty = False

        if registry_dirty:
            registry_ids = await self._refresh_registry_from_db(db)
            dirty_ids.update(registry_ids)

        if dirty_ids:
            await self._refresh_today_counts_from_pg(db, dirty_ids)

        if deleted_ids:
            await self._delete_ids_from_redis(db, deleted_ids)
        if dirty_ids:
            await self._persist_dirty_to_redis(db, dirty_ids)

    async def _rollover_if_needed(self) -> None:
        today = utcnow().date()
        if today == self._current_day:
            return
        old_day = self._current_day
        self._current_day = today
        self._days = [today - timedelta(days=i) for i in range(self.day_slots - 1, -1, -1)]
        for rec in self._records.values():
            rec["codes"] = _ensure_len(rec.get("codes", []), self.day_slots, _STATUS_CODE_NOT_CREATED)[1:] + [_STATUS_CODE_NOT_CREATED]
            rec["up_minutes"] = _ensure_len(rec.get("up_minutes", []), self.day_slots, 0)[1:] + [0]
            rec["down_minutes"] = _ensure_len(rec.get("down_minutes", []), self.day_slots, 0)[1:] + [0]
            rec["maintenance_minutes"] = _ensure_len(rec.get("maintenance_minutes", []), self.day_slots, 0)[1:] + [0]
            rec["today_segments"] = []
            self._recompute_code(rec, self.day_slots - 1)
            self._dirty_monitor_ids.add(str(rec.get("id")))
        logger.info("Status summary day rollover complete from=%s to=%s", old_day.isoformat(), today.isoformat())

    def _apply_snapshot(self, snapshot: dict[str, Any]) -> None:
        records = snapshot.get("records") or {}
        day_list = snapshot.get("day_list") or []
        if len(day_list) != self.day_slots:
            today = utcnow().date()
            day_list = [today - timedelta(days=i) for i in range(self.day_slots - 1, -1, -1)]

        self._records = dict(records)
        self._monitor_kind = {str(mid): str(rec.get("monitor_kind") or "") for mid, rec in self._records.items()}
        self._days = list(day_list)
        self._current_day = self._days[-1]
        self._dirty_monitor_ids.clear()
        self._deleted_monitor_ids.clear()
        self._registry_dirty = False
        self._ready = True
        self._ready_event.set()

    async def _refresh_registry_from_db(self, db) -> set[str]:
        refreshed_ids: set[str] = set()
        try:
            uptime, server, heartbeat = await asyncio.gather(
                db.get_uptime_monitors(enabled_only=True),
                db.get_server_monitors(enabled_only=True),
                db.get_heartbeat_monitors(enabled_only=True),
                return_exceptions=False,
            )
        except Exception:
            logger.debug("Status summary registry refresh failed", exc_info=True)
            return refreshed_ids

        incoming: dict[str, tuple[str, dict[str, Any]]] = {}
        for m in uptime:
            incoming[str(m["id"])] = ("uptime", dict(m))
        for m in server:
            incoming[str(m["id"])] = ("server", dict(m))
        for m in heartbeat:
            incoming[str(m["id"])] = ("heartbeat", dict(m))

        existing_ids = set(self._records.keys())
        incoming_ids = set(incoming.keys())

        for removed_id in existing_ids - incoming_ids:
            self._records.pop(removed_id, None)
            self._monitor_kind.pop(removed_id, None)
            self._deleted_monitor_ids.add(removed_id)

        for monitor_id_s, (monitor_kind, monitor) in incoming.items():
            rec = self._records.get(monitor_id_s)
            if rec is None:
                if monitor_kind == "uptime":
                    rec = self._new_record_from_uptime_row(monitor, self._days)
                elif monitor_kind == "server":
                    rec = self._new_record_from_server_row(monitor, self._days)
                else:
                    rec = self._new_record_from_heartbeat_row(monitor, self._days)
                self._records[monitor_id_s] = rec
            else:
                self._update_record_from_monitor(rec, monitor_kind, monitor)
            self._monitor_kind[monitor_id_s] = monitor_kind
            refreshed_ids.add(monitor_id_s)

        return refreshed_ids

    async def _refresh_today_counts_from_pg(self, db, dirty_ids: set[str]) -> None:
        if not dirty_ids:
            return
        if not db.pool:
            return
        uuid_ids: list[uuid.UUID] = []
        for monitor_id_s in dirty_ids:
            try:
                uuid_ids.append(uuid.UUID(monitor_id_s))
            except Exception:
                continue
        if not uuid_ids:
            return

        today = self._current_day
        start_dt = _utc_day_start(today)
        end_dt = _utc_day_end(today)
        try:
            async with db.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT
                        monitor_id,
                        COUNT(*) FILTER (WHERE status = 'up')::bigint AS up_minutes,
                        COUNT(*) FILTER (WHERE status = 'down')::bigint AS down_minutes,
                        COUNT(*) FILTER (WHERE status = 'maintenance')::bigint AS maintenance_minutes
                    FROM monitor_minutes
                    WHERE monitor_id = ANY($1::uuid[])
                      AND minute >= $2
                      AND minute < $3
                    GROUP BY monitor_id
                    """,
                    uuid_ids,
                    start_dt,
                    end_dt,
                )
        except Exception:
            logger.debug("Status summary today-count refresh failed", exc_info=True)
            return

        counts_map = {
            str(row["monitor_id"]): {
                "up": int(row.get("up_minutes") or 0),
                "down": int(row.get("down_minutes") or 0),
                "maintenance": int(row.get("maintenance_minutes") or 0),
            }
            for row in rows
        }

        for monitor_id_s in dirty_ids:
            rec = self._records.get(monitor_id_s)
            if not rec:
                continue
            counts = counts_map.get(monitor_id_s) or {"up": 0, "down": 0, "maintenance": 0}
            self._set_today_counts(rec, counts["up"], counts["down"], counts["maintenance"])

    async def _persist_full_to_redis(self, db) -> None:
        cache = getattr(db, "cache_service", None)
        if not cache:
            return
        if not self._records:
            return
        try:
            existing_ids = await cache.get_prefixed_set_members(self._ids_suffix())
        except Exception:
            logger.debug("Status summary could not read existing Redis summary ids", exc_info=True)
            return

        new_ids = set(self._records.keys())
        for removed_id in existing_ids - new_ids:
            await self._delete_one_from_redis(cache, removed_id)

        for monitor_id_s, rec in self._records.items():
            await self._persist_one_to_redis(cache, monitor_id_s, rec)

        await cache.set_prefixed_json(
            self._meta_suffix(),
            {
                "version": STATUS_SUMMARY_VERSION,
                "current_day": self._current_day.isoformat(),
                "days": [d.isoformat() for d in self._days],
                "updated_at": utcnow(),
            },
        )

    async def _persist_dirty_to_redis(self, db, dirty_ids: set[str]) -> None:
        cache = getattr(db, "cache_service", None)
        if not cache:
            return
        if not dirty_ids:
            return
        for monitor_id_s in dirty_ids:
            rec = self._records.get(monitor_id_s)
            if not rec:
                continue
            try:
                await self._persist_one_to_redis(cache, monitor_id_s, rec)
            except Exception:
                logger.debug("Status summary failed to persist dirty monitor %s", monitor_id_s, exc_info=True)
        try:
            await cache.set_prefixed_json(
                self._meta_suffix(),
                {
                    "version": STATUS_SUMMARY_VERSION,
                    "current_day": self._current_day.isoformat(),
                    "days": [d.isoformat() for d in self._days],
                    "updated_at": utcnow(),
                },
            )
        except Exception:
            logger.debug("Status summary failed to persist meta key", exc_info=True)

    async def _delete_ids_from_redis(self, db, deleted_ids: set[str]) -> None:
        cache = getattr(db, "cache_service", None)
        if not cache:
            return
        for monitor_id_s in deleted_ids:
            try:
                await self._delete_one_from_redis(cache, monitor_id_s)
            except Exception:
                logger.debug("Status summary failed to delete monitor key %s", monitor_id_s, exc_info=True)

    async def _delete_one_from_redis(self, cache, monitor_id_s: str) -> None:
        await cache.delete_prefixed_key(self._monitor_suffix(monitor_id_s))
        await cache.delete_prefixed_key(self._daily_suffix(monitor_id_s))
        await cache.delete_prefixed_key(self._timeline_suffix(monitor_id_s))
        await cache.remove_prefixed_set_member(self._ids_suffix(), monitor_id_s)

    async def _persist_one_to_redis(self, cache, monitor_id_s: str, rec: dict[str, Any]) -> None:
        monitor_payload = {
            "id": monitor_id_s,
            "monitor_kind": rec.get("monitor_kind"),
            "name": rec.get("name"),
            "category": rec.get("category"),
            "status": rec.get("status"),
            "status_since": rec.get("status_since"),
            "created_at": rec.get("created_at"),
            "first_data_at": rec.get("first_data_at"),
            "last_check_at": rec.get("last_check_at"),
            "last_up_at": rec.get("last_up_at"),
            "last_ping_at": rec.get("last_ping_at"),
            "last_report_at": rec.get("last_report_at"),
            "last_checkin_at": rec.get("last_checkin_at"),
            "target": rec.get("target"),
            "heartbeat_type": rec.get("heartbeat_type"),
            "hostname": rec.get("hostname"),
            "os": rec.get("os"),
            "is_public": bool(rec.get("is_public", False)),
            "maintenance_mode": bool(rec.get("maintenance_mode", False)),
            "metrics": rec.get("metrics") or {},
            "total_up": int(rec.get("total_up") or 0),
            "total_down": int(rec.get("total_down") or 0),
            "total_maintenance": int(rec.get("total_maintenance") or 0),
            "rt_sum_up": float(rec.get("rt_sum_up") or 0.0),
            "rt_count_up": int(rec.get("rt_count_up") or 0),
            "updated_at": utcnow(),
        }
        daily_payload = {
            "codes": _ensure_len(rec.get("codes", []), self.day_slots, _STATUS_CODE_NOT_CREATED),
            "up_minutes": _ensure_len(rec.get("up_minutes", []), self.day_slots, 0),
            "down_minutes": _ensure_len(rec.get("down_minutes", []), self.day_slots, 0),
            "maintenance_minutes": _ensure_len(rec.get("maintenance_minutes", []), self.day_slots, 0),
        }
        timeline_payload = {
            "segments": list(rec.get("today_segments") or [])[-self.max_timeline_segments:],
        }
        await cache.set_prefixed_json(self._monitor_suffix(monitor_id_s), monitor_payload)
        await cache.set_prefixed_json(self._daily_suffix(monitor_id_s), daily_payload)
        await cache.set_prefixed_json(self._timeline_suffix(monitor_id_s), timeline_payload)
        await cache.add_prefixed_set_member(self._ids_suffix(), monitor_id_s)

    async def _build_snapshot_from_redis(self, db) -> dict[str, Any] | None:
        cache = getattr(db, "cache_service", None)
        if not cache:
            return None
        try:
            meta = await cache.get_prefixed_json(self._meta_suffix())
            ids = await cache.get_prefixed_set_members(self._ids_suffix())
        except Exception:
            logger.debug("Status summary Redis read failed", exc_info=True)
            return None
        if not ids:
            return None

        day_list: list[date] = []
        raw_days = (meta or {}).get("days") if isinstance(meta, dict) else None
        if isinstance(raw_days, list):
            for raw in raw_days:
                d = _as_date(raw)
                if d:
                    day_list.append(d)
        if len(day_list) != self.day_slots:
            today = utcnow().date()
            day_list = [today - timedelta(days=i) for i in range(self.day_slots - 1, -1, -1)]

        records: dict[str, dict[str, Any]] = {}
        for monitor_id_s in ids:
            try:
                base_payload = await cache.get_prefixed_json(self._monitor_suffix(monitor_id_s)) or {}
                daily_payload = await cache.get_prefixed_json(self._daily_suffix(monitor_id_s)) or {}
                timeline_payload = await cache.get_prefixed_json(self._timeline_suffix(monitor_id_s)) or {}
            except Exception:
                continue
            if not isinstance(base_payload, dict):
                continue

            monitor_kind = str(base_payload.get("monitor_kind") or "")
            if monitor_kind not in {"uptime", "server", "heartbeat"}:
                continue

            rec = dict(base_payload)
            rec["id"] = monitor_id_s
            rec["created_at"] = _as_dt(rec.get("created_at"))
            rec["status_since"] = _as_dt(rec.get("status_since"))
            rec["first_data_at"] = _as_dt(rec.get("first_data_at"))
            rec["last_check_at"] = _as_dt(rec.get("last_check_at"))
            rec["last_up_at"] = _as_dt(rec.get("last_up_at"))
            rec["last_ping_at"] = _as_dt(rec.get("last_ping_at"))
            rec["last_report_at"] = _as_dt(rec.get("last_report_at"))
            rec["last_checkin_at"] = _as_dt(rec.get("last_checkin_at"))

            rec["codes"] = _ensure_len(list((daily_payload or {}).get("codes") or []), self.day_slots, _STATUS_CODE_NOT_CREATED)
            rec["up_minutes"] = _ensure_len(list((daily_payload or {}).get("up_minutes") or []), self.day_slots, 0)
            rec["down_minutes"] = _ensure_len(list((daily_payload or {}).get("down_minutes") or []), self.day_slots, 0)
            rec["maintenance_minutes"] = _ensure_len(list((daily_payload or {}).get("maintenance_minutes") or []), self.day_slots, 0)
            rec["today_segments"] = list((timeline_payload or {}).get("segments") or [])
            for seg in rec["today_segments"]:
                if isinstance(seg, dict):
                    seg["start_at"] = _as_dt(seg.get("start_at"))
                    seg["end_at"] = _as_dt(seg.get("end_at"))

            rec["total_up"] = int(rec.get("total_up") or 0)
            rec["total_down"] = int(rec.get("total_down") or 0)
            rec["total_maintenance"] = int(rec.get("total_maintenance") or 0)
            rec["rt_sum_up"] = float(rec.get("rt_sum_up") or 0.0)
            rec["rt_count_up"] = int(rec.get("rt_count_up") or 0)
            rec["metrics"] = rec.get("metrics") or {}

            records[monitor_id_s] = rec
            self._monitor_kind[monitor_id_s] = monitor_kind

        if not records:
            return None
        return {"day_list": day_list, "records": records}

    async def _build_snapshot_from_pg(self, db) -> dict[str, Any]:
        if not db.pool:
            raise RuntimeError("Database pool is not initialized")
        today = utcnow().date()
        day_list = [today - timedelta(days=i) for i in range(self.day_slots - 1, -1, -1)]
        start_day = day_list[0]
        start_dt = _utc_day_start(start_day)
        end_dt = _utc_day_end(today)

        async with db.pool.acquire() as conn:
            await db._apply_due_maintenance_windows(conn)
            uptime_rows = await conn.fetch(
                """
                SELECT
                    um.id, um.name, um.target, um.category, um.is_public,
                    um.maintenance_mode, um.created_at, um.status, um.status_since,
                    um.last_checkin_at,
                    (SELECT checked_at FROM uptime_checks WHERE monitor_id = um.id ORDER BY checked_at DESC LIMIT 1) AS last_check_at,
                    (SELECT checked_at FROM uptime_checks WHERE monitor_id = um.id AND status = 'up' ORDER BY checked_at DESC LIMIT 1) AS last_up_at
                FROM uptime_monitors um
                WHERE um.type = 1 AND um.enabled = true
                ORDER BY um.name
                """
            )
            server_rows = await conn.fetch(
                """
                SELECT
                    sm.id, sm.name, sm.hostname, sm.os, sm.category, sm.is_public,
                    sm.maintenance_mode, sm.created_at, sm.status, sm.status_since,
                    sm.last_checkin_at, sm.last_report_at
                FROM server_monitors sm
                WHERE sm.enabled = true
                ORDER BY sm.name
                """
            )
            heartbeat_rows = await conn.fetch(
                """
                SELECT
                    hm.id, hm.name, hm.heartbeat_type, hm.category, hm.is_public,
                    hm.maintenance_mode, hm.created_at, hm.status, hm.status_since,
                    hm.last_checkin_at, hm.last_ping_at
                FROM heartbeat_monitors hm
                WHERE hm.enabled = true
                ORDER BY hm.name
                """
            )

            server_ids = [row["id"] for row in server_rows]
            latest_metrics_rows = []
            if server_ids:
                latest_metrics_rows = await conn.fetch(
                    """
                    SELECT DISTINCT ON (server_id)
                        server_id, cpu_percent, ram_percent, network_in, network_out,
                        disk_percent, load_1, load_5, load_15, cpu_io_wait, cpu_steal
                    FROM server_history
                    WHERE server_id = ANY($1::uuid[])
                    ORDER BY server_id, timestamp DESC
                    """,
                    server_ids,
                )

            monitor_ids = [row["id"] for row in uptime_rows] + [row["id"] for row in server_rows] + [row["id"] for row in heartbeat_rows]

            day_rows = []
            total_rows = []
            if monitor_ids:
                day_rows = await conn.fetch(
                    """
                    SELECT
                        monitor_id,
                        day,
                        SUM(up)::bigint AS up_minutes,
                        SUM(down)::bigint AS down_minutes,
                        SUM(maintenance)::bigint AS maintenance_minutes
                    FROM (
                        SELECT
                            monitor_id,
                            date AS day,
                            up_minutes::bigint AS up,
                            down_minutes::bigint AS down,
                            maintenance_minutes::bigint AS maintenance
                        FROM monitor_minutes_daily
                        WHERE monitor_id = ANY($1::uuid[])
                          AND date >= $2
                          AND date < $3
                        UNION ALL
                        SELECT
                            monitor_id,
                            DATE(minute) AS day,
                            COUNT(*) FILTER (WHERE status = 'up')::bigint AS up,
                            COUNT(*) FILTER (WHERE status = 'down')::bigint AS down,
                            COUNT(*) FILTER (WHERE status = 'maintenance')::bigint AS maintenance
                        FROM monitor_minutes
                        WHERE monitor_id = ANY($1::uuid[])
                          AND minute >= $4
                          AND minute < $5
                        GROUP BY monitor_id, DATE(minute)
                    ) t
                    GROUP BY monitor_id, day
                    """,
                    monitor_ids,
                    start_day,
                    today,
                    start_dt,
                    end_dt,
                )
                total_rows = await conn.fetch(
                    """
                    SELECT
                        monitor_id,
                        SUM(up)::bigint AS up_minutes,
                        SUM(down)::bigint AS down_minutes,
                        SUM(maintenance)::bigint AS maintenance_minutes
                    FROM (
                        SELECT
                            monitor_id,
                            COALESCE(SUM(up_minutes), 0)::bigint AS up,
                            COALESCE(SUM(down_minutes), 0)::bigint AS down,
                            COALESCE(SUM(maintenance_minutes), 0)::bigint AS maintenance
                        FROM monitor_minutes_daily
                        WHERE monitor_id = ANY($1::uuid[])
                        GROUP BY monitor_id
                        UNION ALL
                        SELECT
                            monitor_id,
                            COUNT(*) FILTER (WHERE status = 'up')::bigint AS up,
                            COUNT(*) FILTER (WHERE status = 'down')::bigint AS down,
                            COUNT(*) FILTER (WHERE status = 'maintenance')::bigint AS maintenance
                        FROM monitor_minutes
                        WHERE monitor_id = ANY($1::uuid[])
                        GROUP BY monitor_id
                    ) t
                    GROUP BY monitor_id
                    """,
                    monitor_ids,
                )

            uptime_ids = [row["id"] for row in uptime_rows]
            uptime_first_rows = []
            uptime_rt_rows = []
            if uptime_ids:
                uptime_first_rows = await conn.fetch(
                    """
                    SELECT monitor_id, MIN(first_check) AS first_check
                    FROM (
                        SELECT monitor_id, MIN(checked_at) AS first_check
                        FROM uptime_checks
                        WHERE monitor_id = ANY($1::uuid[])
                        GROUP BY monitor_id
                        UNION ALL
                        SELECT monitor_id, MIN(date)::timestamp AS first_check
                        FROM uptime_checks_daily
                        WHERE monitor_id = ANY($1::uuid[])
                        GROUP BY monitor_id
                    ) s
                    GROUP BY monitor_id
                    """,
                    uptime_ids,
                )
                uptime_rt_rows = await conn.fetch(
                    """
                    SELECT monitor_id, SUM(rt_sum)::double precision AS rt_sum, SUM(rt_count)::bigint AS rt_count
                    FROM (
                        SELECT
                            monitor_id,
                            COALESCE(SUM(response_time_ms) FILTER (
                                WHERE status = 'up' AND response_time_ms IS NOT NULL
                            ), 0)::double precision AS rt_sum,
                            COALESCE(COUNT(*) FILTER (
                                WHERE status = 'up' AND response_time_ms IS NOT NULL
                            ), 0)::bigint AS rt_count
                        FROM uptime_checks
                        WHERE monitor_id = ANY($1::uuid[])
                        GROUP BY monitor_id
                        UNION ALL
                        SELECT
                            monitor_id,
                            COALESCE(SUM(avg_response_time_ms * up_count) FILTER (
                                WHERE avg_response_time_ms IS NOT NULL
                            ), 0)::double precision AS rt_sum,
                            COALESCE(SUM(up_count) FILTER (
                                WHERE avg_response_time_ms IS NOT NULL
                            ), 0)::bigint AS rt_count
                        FROM uptime_checks_daily
                        WHERE monitor_id = ANY($1::uuid[])
                        GROUP BY monitor_id
                    ) q
                    GROUP BY monitor_id
                    """,
                    uptime_ids,
                )

        metrics_map = {
            str(row["server_id"]): {
                "cpu": row.get("cpu_percent"),
                "ram": row.get("ram_percent"),
                "network_in": row.get("network_in"),
                "network_out": row.get("network_out"),
                "disk_percent": row.get("disk_percent"),
                "load_1": row.get("load_1"),
                "load_5": row.get("load_5"),
                "load_15": row.get("load_15"),
                "cpu_io_wait": row.get("cpu_io_wait"),
                "cpu_steal": row.get("cpu_steal"),
            }
            for row in latest_metrics_rows
        }
        first_map = {str(row["monitor_id"]): row.get("first_check") for row in uptime_first_rows}
        rt_map = {
            str(row["monitor_id"]): {
                "rt_sum": float(row.get("rt_sum") or 0.0),
                "rt_count": int(row.get("rt_count") or 0),
            }
            for row in uptime_rt_rows
        }
        totals_map = {
            str(row["monitor_id"]): {
                "up": int(row.get("up_minutes") or 0),
                "down": int(row.get("down_minutes") or 0),
                "maintenance": int(row.get("maintenance_minutes") or 0),
            }
            for row in total_rows
        }
        day_counts_map: dict[str, dict[date, dict[str, int]]] = {}
        for row in day_rows:
            monitor_id_s = str(row["monitor_id"])
            day_value = row.get("day")
            if not isinstance(day_value, date):
                continue
            day_counts_map.setdefault(monitor_id_s, {})[day_value] = {
                "up": int(row.get("up_minutes") or 0),
                "down": int(row.get("down_minutes") or 0),
                "maintenance": int(row.get("maintenance_minutes") or 0),
            }

        day_index = {d: i for i, d in enumerate(day_list)}
        records: dict[str, dict[str, Any]] = {}

        for row in uptime_rows:
            rec = self._new_record_from_uptime_row(dict(row), day_list)
            monitor_id_s = str(rec["id"])
            first_check = _as_dt(first_map.get(monitor_id_s))
            has_data = rec.get("last_checkin_at") is not None
            rec["first_data_at"] = first_check or (rec.get("created_at") if has_data else None)
            rt = rt_map.get(monitor_id_s) or {}
            rec["rt_sum_up"] = float(rt.get("rt_sum") or 0.0)
            rec["rt_count_up"] = int(rt.get("rt_count") or 0)
            self._apply_counts_to_record(rec, day_counts_map.get(monitor_id_s, {}), day_index)
            total_counts = totals_map.get(monitor_id_s, {})
            rec["total_up"] = int(total_counts.get("up") or 0)
            rec["total_down"] = int(total_counts.get("down") or 0)
            rec["total_maintenance"] = int(total_counts.get("maintenance") or 0)
            self._recompute_all_codes(rec)
            records[monitor_id_s] = rec
            self._monitor_kind[monitor_id_s] = "uptime"

        for row in server_rows:
            rec = self._new_record_from_server_row(dict(row), day_list)
            monitor_id_s = str(rec["id"])
            has_data = rec.get("last_checkin_at") is not None
            rec["first_data_at"] = rec.get("created_at") if has_data else None
            rec["metrics"] = metrics_map.get(monitor_id_s, {})
            self._apply_counts_to_record(rec, day_counts_map.get(monitor_id_s, {}), day_index)
            total_counts = totals_map.get(monitor_id_s, {})
            rec["total_up"] = int(total_counts.get("up") or 0)
            rec["total_down"] = int(total_counts.get("down") or 0)
            rec["total_maintenance"] = int(total_counts.get("maintenance") or 0)
            self._recompute_all_codes(rec)
            records[monitor_id_s] = rec
            self._monitor_kind[monitor_id_s] = "server"

        for row in heartbeat_rows:
            rec = self._new_record_from_heartbeat_row(dict(row), day_list)
            monitor_id_s = str(rec["id"])
            has_data = rec.get("last_checkin_at") is not None
            rec["first_data_at"] = rec.get("created_at") if has_data else None
            self._apply_counts_to_record(rec, day_counts_map.get(monitor_id_s, {}), day_index)
            total_counts = totals_map.get(monitor_id_s, {})
            rec["total_up"] = int(total_counts.get("up") or 0)
            rec["total_down"] = int(total_counts.get("down") or 0)
            rec["total_maintenance"] = int(total_counts.get("maintenance") or 0)
            self._recompute_all_codes(rec)
            records[monitor_id_s] = rec
            self._monitor_kind[monitor_id_s] = "heartbeat"

        return {"day_list": day_list, "records": records}

    def _new_record_base(self, monitor_kind: str, monitor_id: str, day_list: list[date]) -> dict[str, Any]:
        return {
            "id": monitor_id,
            "monitor_kind": monitor_kind,
            "name": "",
            "category": None,
            "is_public": False,
            "maintenance_mode": False,
            "created_at": None,
            "status": "unknown",
            "status_since": None,
            "first_data_at": None,
            "last_check_at": None,
            "last_up_at": None,
            "last_ping_at": None,
            "last_report_at": None,
            "last_checkin_at": None,
            "target": None,
            "heartbeat_type": "cronjob",
            "hostname": None,
            "os": None,
            "metrics": {},
            "rt_sum_up": 0.0,
            "rt_count_up": 0,
            "total_up": 0,
            "total_down": 0,
            "total_maintenance": 0,
            "up_minutes": [0] * len(day_list),
            "down_minutes": [0] * len(day_list),
            "maintenance_minutes": [0] * len(day_list),
            "codes": [_STATUS_CODE_NOT_CREATED] * len(day_list),
            "today_segments": [],
        }

    def _new_record_from_uptime_row(self, row: dict[str, Any], day_list: list[date]) -> dict[str, Any]:
        monitor_id_s = str(row.get("id"))
        rec = self._new_record_base("uptime", monitor_id_s, day_list)
        rec["name"] = row.get("name")
        rec["target"] = row.get("target")
        rec["category"] = row.get("category")
        rec["is_public"] = bool(row.get("is_public", False))
        rec["maintenance_mode"] = bool(row.get("maintenance_mode", False))
        rec["created_at"] = _as_dt(row.get("created_at"))
        rec["status"] = str(row.get("status") or "unknown")
        rec["status_since"] = _as_dt(row.get("status_since"))
        rec["last_checkin_at"] = _as_dt(row.get("last_checkin_at"))
        rec["last_check_at"] = _as_dt(row.get("last_check_at")) or rec["last_checkin_at"]
        rec["last_up_at"] = _as_dt(row.get("last_up_at"))
        return rec

    def _new_record_from_server_row(self, row: dict[str, Any], day_list: list[date]) -> dict[str, Any]:
        monitor_id_s = str(row.get("id"))
        rec = self._new_record_base("server", monitor_id_s, day_list)
        rec["name"] = row.get("name")
        rec["hostname"] = row.get("hostname")
        rec["os"] = row.get("os")
        rec["category"] = row.get("category")
        rec["is_public"] = bool(row.get("is_public", False))
        rec["maintenance_mode"] = bool(row.get("maintenance_mode", False))
        rec["created_at"] = _as_dt(row.get("created_at"))
        rec["status"] = str(row.get("status") or "unknown")
        rec["status_since"] = _as_dt(row.get("status_since"))
        rec["last_checkin_at"] = _as_dt(row.get("last_checkin_at"))
        rec["last_check_at"] = rec["last_checkin_at"]
        rec["last_report_at"] = _as_dt(row.get("last_report_at"))
        rec["heartbeat_type"] = "server_agent"
        return rec

    def _new_record_from_heartbeat_row(self, row: dict[str, Any], day_list: list[date]) -> dict[str, Any]:
        monitor_id_s = str(row.get("id"))
        rec = self._new_record_base("heartbeat", monitor_id_s, day_list)
        rec["name"] = row.get("name")
        rec["category"] = row.get("category")
        rec["is_public"] = bool(row.get("is_public", False))
        rec["maintenance_mode"] = bool(row.get("maintenance_mode", False))
        rec["created_at"] = _as_dt(row.get("created_at"))
        rec["status"] = str(row.get("status") or "unknown")
        rec["status_since"] = _as_dt(row.get("status_since"))
        rec["last_checkin_at"] = _as_dt(row.get("last_checkin_at"))
        rec["last_check_at"] = rec["last_checkin_at"]
        rec["last_ping_at"] = _as_dt(row.get("last_ping_at"))
        rec["heartbeat_type"] = row.get("heartbeat_type") or "cronjob"
        return rec

    def _update_record_from_monitor(self, rec: dict[str, Any], monitor_kind: str, monitor: dict[str, Any]) -> None:
        rec["monitor_kind"] = monitor_kind
        rec["name"] = monitor.get("name")
        rec["category"] = monitor.get("category")
        rec["is_public"] = bool(monitor.get("is_public", False))
        rec["maintenance_mode"] = bool(monitor.get("maintenance_mode", False))
        rec["created_at"] = _as_dt(monitor.get("created_at")) or rec.get("created_at")
        status_value = str(monitor.get("status") or rec.get("status") or "unknown")
        if status_value != rec.get("status"):
            changed_at = _as_dt(monitor.get("status_since")) or utcnow()
            self._append_today_segment(rec, status_value, changed_at)
            rec["status_since"] = changed_at
        else:
            rec["status_since"] = _as_dt(monitor.get("status_since")) or rec.get("status_since")
        rec["status"] = status_value
        rec["last_checkin_at"] = _as_dt(monitor.get("last_checkin_at")) or rec.get("last_checkin_at")
        rec["last_check_at"] = _as_dt(monitor.get("last_check_at")) or rec.get("last_check_at") or rec.get("last_checkin_at")
        rec["last_up_at"] = _as_dt(monitor.get("last_up_at")) or rec.get("last_up_at")
        rec["last_ping_at"] = _as_dt(monitor.get("last_ping_at")) or rec.get("last_ping_at")
        rec["last_report_at"] = _as_dt(monitor.get("last_report_at")) or rec.get("last_report_at")
        if monitor_kind == "uptime":
            rec["target"] = monitor.get("target")
        elif monitor_kind == "server":
            rec["hostname"] = monitor.get("hostname")
            rec["os"] = monitor.get("os")
            rec["heartbeat_type"] = "server_agent"
        elif monitor_kind == "heartbeat":
            rec["heartbeat_type"] = monitor.get("heartbeat_type") or rec.get("heartbeat_type") or "cronjob"

    def _apply_counts_to_record(
        self,
        rec: dict[str, Any],
        day_counts: dict[date, dict[str, int]],
        day_index: dict[date, int],
    ) -> None:
        for day_value, counts in day_counts.items():
            idx = day_index.get(day_value)
            if idx is None:
                continue
            rec["up_minutes"][idx] = int(counts.get("up") or 0)
            rec["down_minutes"][idx] = int(counts.get("down") or 0)
            rec["maintenance_minutes"][idx] = int(counts.get("maintenance") or 0)

    def _set_today_counts(self, rec: dict[str, Any], up: int, down: int, maintenance: int) -> None:
        idx = self.day_slots - 1
        old_up = int(rec["up_minutes"][idx] or 0)
        old_down = int(rec["down_minutes"][idx] or 0)
        old_maintenance = int(rec["maintenance_minutes"][idx] or 0)
        rec["up_minutes"][idx] = int(up)
        rec["down_minutes"][idx] = int(down)
        rec["maintenance_minutes"][idx] = int(maintenance)
        rec["total_up"] = int(rec.get("total_up") or 0) - old_up + int(up)
        rec["total_down"] = int(rec.get("total_down") or 0) - old_down + int(down)
        rec["total_maintenance"] = int(rec.get("total_maintenance") or 0) - old_maintenance + int(maintenance)
        self._recompute_code(rec, idx)

    def _recompute_all_codes(self, rec: dict[str, Any]) -> None:
        for idx in range(self.day_slots):
            self._recompute_code(rec, idx)

    def _recompute_code(self, rec: dict[str, Any], idx: int) -> None:
        up = int(rec["up_minutes"][idx] or 0)
        down = int(rec["down_minutes"][idx] or 0)
        maintenance = int(rec["maintenance_minutes"][idx] or 0)
        day_value = self._days[idx] if idx < len(self._days) else self._current_day
        first_data_at = _as_dt(rec.get("first_data_at"))
        if first_data_at is None or day_value < first_data_at.date():
            rec["codes"][idx] = _STATUS_CODE_NOT_CREATED
            return
        if maintenance > 0 and up == 0 and down == 0:
            rec["codes"][idx] = _STATUS_CODE_MAINTENANCE
            return
        if up == 0 and down == 0:
            rec["codes"][idx] = _STATUS_CODE_NOT_CREATED
            return
        if down == 0:
            rec["codes"][idx] = _STATUS_CODE_UP
            return
        if down <= self.partial_downtime_minutes:
            rec["codes"][idx] = _STATUS_CODE_PARTIAL
            return
        rec["codes"][idx] = _STATUS_CODE_DOWN

    def _append_today_segment(self, rec: dict[str, Any], status: str, when: datetime) -> None:
        if not isinstance(when, datetime):
            when = utcnow()
        if when.date() != self._current_day:
            return
        segments = rec.setdefault("today_segments", [])
        if segments:
            tail = segments[-1]
            if tail.get("status") == status and tail.get("end_at") is None:
                return
            if tail.get("end_at") is None:
                tail["end_at"] = when
        segments.append({"start_at": when, "end_at": None, "status": status})
        if len(segments) > self.max_timeline_segments:
            del segments[0: len(segments) - self.max_timeline_segments]

    def _record_to_history(self, rec: dict[str, Any]) -> list[Any]:
        history: list[Any] = []
        codes = _ensure_len(rec.get("codes", []), self.day_slots, _STATUS_CODE_NOT_CREATED)
        down_values = _ensure_len(rec.get("down_minutes", []), self.day_slots, 0)
        for idx in range(self.day_slots):
            code = int(codes[idx] or 0)
            down = float(down_values[idx] or 0)
            if code == _STATUS_CODE_UP:
                history.append("up")
            elif code == _STATUS_CODE_PARTIAL:
                history.append({"status": "partial", "downtime_minutes": round(down, 2)})
            elif code == _STATUS_CODE_DOWN:
                history.append({"status": "down", "downtime_minutes": round(down, 2)})
            elif code == _STATUS_CODE_MAINTENANCE:
                history.append({"status": "maintenance", "tooltip": "Maintenance"})
            else:
                history.append("not_created")
        return history

    def _calc_uptime_percentage(self, rec: dict[str, Any]) -> float | None:
        up = int(rec.get("total_up") or 0)
        down = int(rec.get("total_down") or 0)
        total = up + down
        if total <= 0:
            return None if rec.get("first_data_at") is None else 100.0
        return round((up / total) * 100, 4)

    def _record_to_public_monitor(self, rec: dict[str, Any]) -> dict[str, Any]:
        kind = rec.get("monitor_kind")
        status_value = str(rec.get("status") or "unknown")
        uptime_pct = self._calc_uptime_percentage(rec)
        history = self._record_to_history(rec)
        status_since_iso = rec["status_since"].isoformat() if isinstance(rec.get("status_since"), datetime) else None
        first_data_iso = rec["first_data_at"].isoformat() if isinstance(rec.get("first_data_at"), datetime) else None
        created_iso = rec["created_at"].isoformat() if isinstance(rec.get("created_at"), datetime) else None

        if kind == "uptime":
            rt_avg = None
            rt_count = int(rec.get("rt_count_up") or 0)
            if rt_count > 0:
                rt_avg = float(rec.get("rt_sum_up") or 0.0) / rt_count
            return {
                "id": str(rec["id"]),
                "name": rec.get("name"),
                "type": "uptime",
                "monitor_kind": "website",
                "target": rec.get("target"),
                "status": status_value,
                "status_since": status_since_iso,
                "first_data_at": first_data_iso,
                "last_check_at": rec["last_check_at"].isoformat() if isinstance(rec.get("last_check_at"), datetime) else None,
                "last_up_at": rec["last_up_at"].isoformat() if isinstance(rec.get("last_up_at"), datetime) else None,
                "uptime_percentage": uptime_pct,
                "response_time_avg": rt_avg,
                "category": rec.get("category"),
                "history": history,
                "is_public": bool(rec.get("is_public", False)),
                "maintenance_mode": bool(rec.get("maintenance_mode", False)),
                "created_at": created_iso,
            }

        if kind == "server":
            return {
                "id": str(rec["id"]),
                "name": rec.get("name"),
                "type": "heartbeat",
                "heartbeat_type": "server_agent",
                "monitor_kind": "heartbeat",
                "hostname": rec.get("hostname"),
                "os": rec.get("os"),
                "status": status_value,
                "status_since": status_since_iso,
                "metrics": rec.get("metrics") or {},
                "history": history,
                "last_report_at": rec["last_report_at"].isoformat() if isinstance(rec.get("last_report_at"), datetime) else None,
                "first_data_at": first_data_iso,
                "uptime_percentage": uptime_pct,
                "category": rec.get("category"),
                "is_public": bool(rec.get("is_public", False)),
                "maintenance_mode": bool(rec.get("maintenance_mode", False)),
                "created_at": created_iso,
            }

        return {
            "id": str(rec["id"]),
            "name": rec.get("name"),
            "type": "heartbeat",
            "heartbeat_type": rec.get("heartbeat_type") or "cronjob",
            "monitor_kind": "heartbeat",
            "status": status_value,
            "status_since": status_since_iso,
            "first_data_at": first_data_iso,
            "last_ping_at": rec["last_ping_at"].isoformat() if isinstance(rec.get("last_ping_at"), datetime) else None,
            "category": rec.get("category"),
            "uptime_percentage": uptime_pct,
            "history": history,
            "is_public": bool(rec.get("is_public", False)),
            "maintenance_mode": bool(rec.get("maintenance_mode", False)),
            "created_at": created_iso,
        }


status_summary_service = StatusSummaryService()
