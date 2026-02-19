# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import asyncpg
import asyncio
import logging
from asyncpg import Pool
from typing import Any
import uuid
from datetime import datetime, timedelta
from .config import settings
from .cache import CacheService
from .utils.time import utcnow

MAINTENANCE_TABLE_MAP = {
    "website": "uptime_monitors",
    "uptime": "uptime_monitors",
    "heartbeat-cronjob": "heartbeat_monitors",
    "heartbeat": "heartbeat_monitors",
    "heartbeat-server-agent": "server_monitors",
    "server-agent": "server_monitors",
    "server_agent": "server_monitors",
    "server": "server_monitors",
    "agent": "server_monitors",
}

GRACE_PERIOD_MINUTES = 3
logger = logging.getLogger(__name__)

# Allowlists of columns that may be dynamically SET via **kwargs in update
# functions.  Any key not in the corresponding set is rejected to prevent
# SQL injection through column names.
_UPTIME_UPDATABLE_FIELDS: frozenset[str] = frozenset({
    "name", "target", "type", "port", "check_interval", "timeout",
    "category", "private_notes",
    "enabled", "is_public", "notifications_enabled",
    "maintenance_mode", "maintenance_start_at", "maintenance_end_at",
    "status", "last_checkin_at", "down_since", "status_since",
    "updated_at",
})

_SERVER_UPDATABLE_FIELDS: frozenset[str] = frozenset({
    "name", "sid", "os", "kernel", "hostname",
    "cpu_model", "cpu_sockets", "cpu_cores", "cpu_threads",
    "ram_size", "ram_swap_size",
    "enabled", "is_public", "notifications_enabled",
    "maintenance_mode", "maintenance_start_at", "maintenance_end_at",
    "status", "last_checkin_at", "down_since", "status_since",
    "category", "last_report_at",
})

_HEARTBEAT_UPDATABLE_FIELDS: frozenset[str] = frozenset({
    "name", "sid", "heartbeat_type", "timeout", "grace_period",
    "category", "private_notes",
    "enabled", "is_public", "notifications_enabled",
    "maintenance_mode", "maintenance_start_at", "maintenance_end_at",
    "status", "last_checkin_at", "down_since", "status_since",
    "updated_at", "last_ping_at",
})


class Database:
    """Database connection and operations."""

    def __init__(self):
        self.pool: Pool | None = None
        self.cache_enabled: bool = False
        self.cache_loaded_at: datetime | None = None
        self.cache_service: CacheService | None = None
        self._cache_resync_lock = asyncio.Lock()
        self._cache_resync_task: asyncio.Task | None = None
        self._last_cache_resync_at: datetime | None = None
        self._cache_resync_min_interval_seconds: int = 300
        self._cache_lock = asyncio.Lock()
        self._cache = {
            "uptime": {},
            "server": {},
            "heartbeat": {},
            "incidents": {},
            "users": {},
            "uptime_checks": {},
            "server_history": {},
            "heartbeat_pings": {},
            "maintenance_events": {},
            "monitor_minutes": {}
        }
        self._cache_sid = {
            "server": {},
            "heartbeat": {}
        }
        self._cache_user_email = {}
        self.pool_min_size = 5
        self.pool_max_size = 20
        self.pool_statement_cache_size = 0
        self.cache_only = settings.CACHE_ONLY

    def init_cache_service(self):
        if self.cache_service is None:
            self.cache_service = CacheService()

    async def connect(self):
        if self.cache_service is None:
            self.init_cache_service()
        self.pool_min_size = 5
        self.pool_max_size = 20
        self.pool_statement_cache_size = 0
        self.pool = await asyncpg.create_pool(
            settings.DATABASE_URL,
            min_size=self.pool_min_size,
            max_size=self.pool_max_size,
            statement_cache_size=self.pool_statement_cache_size  # Required for pgbouncer compatibility
        )

    async def close(self):
        if self.pool:
            await self.pool.close()
        if self.cache_service:
            await self.cache_service.close()

    def _resolve_maintenance_table(self, monitor_type: str) -> str | None:
        key = (monitor_type or "").strip().lower().replace(" ", "_")
        return MAINTENANCE_TABLE_MAP.get(key)

    def _normalize_maintenance_event_type(self, monitor_type: str) -> str | None:
        key = (monitor_type or "").strip().lower().replace(" ", "_")
        if key in ("website", "uptime"):
            return "website"
        if key in ("heartbeat", "heartbeat-cronjob", "cronjob"):
            return "heartbeat"
        if key in ("heartbeat-server-agent", "server_agent", "server", "server-agent", "agent"):
            return "server_agent"
        return None

    async def _apply_due_maintenance_windows(self, conn):
        for table in ("uptime_monitors", "heartbeat_monitors", "server_monitors"):
            await conn.execute(
                f"""
                UPDATE {table}
                   SET maintenance_mode = true
                 WHERE maintenance_mode = false
                   AND maintenance_start_at IS NOT NULL
                   AND maintenance_start_at <= CURRENT_TIMESTAMP
                   AND (maintenance_end_at IS NULL OR maintenance_end_at > CURRENT_TIMESTAMP)
                """
            )

            await conn.execute(
                f"""
                UPDATE {table}
                   SET maintenance_mode = false,
                       maintenance_start_at = NULL,
                       maintenance_end_at = NULL
                 WHERE maintenance_mode = true
                   AND maintenance_end_at IS NOT NULL
                   AND maintenance_end_at <= CURRENT_TIMESTAMP
                """
            )

    async def apply_due_maintenance(self):
        async with self.pool.acquire() as conn:
            await self._apply_due_maintenance_windows(conn)

    async def load_cache(self):
        if not self.pool:
            return
        snapshot = await self.get_cache_snapshot()
        await self._apply_snapshot_to_memory(snapshot)

        if self.cache_service:
            async def _loader():
                return snapshot

            await self.cache_service.warmup_from_loader(_loader)

        self.cache_enabled = True
        self.cache_loaded_at = utcnow()
        self._last_cache_resync_at = utcnow()

    async def get_cache_snapshot(self) -> dict[str, Any]:
        if not self.pool:
            return {"entities": {}, "indexes": {}, "series": {}}

        async with self.pool.acquire() as conn:
            await self._apply_due_maintenance_windows(conn)

            user_rows = await conn.fetch("SELECT * FROM users")
            uptime_rows = await conn.fetch(
                """SELECT um.id, um.name, um.type, um.target, um.port, um.check_interval, um.timeout,
                          um.category, um.private_notes, um.enabled, um.notifications_enabled, um.is_public,
                          um.maintenance_mode, um.maintenance_start_at, um.maintenance_end_at,
                          um.status, um.last_checkin_at, um.down_since, um.status_since,
                          um.created_at, um.updated_at,
                          (SELECT checked_at FROM uptime_checks WHERE monitor_id = um.id ORDER BY checked_at DESC LIMIT 1) as last_check_at,
                          (SELECT checked_at FROM uptime_checks WHERE monitor_id = um.id AND status = 'up' ORDER BY checked_at DESC LIMIT 1) as last_up_at
                   FROM uptime_monitors um
                   WHERE um.type = 1
                   ORDER BY um.name"""
            )
            server_rows = await conn.fetch(
                """SELECT id, sid, name, os, kernel, hostname, cpu_model,
                          cpu_sockets, cpu_cores, cpu_threads, ram_size, ram_swap_size,
                          'server_agent'::varchar as heartbeat_type,
                          enabled, notifications_enabled, is_public,
                          maintenance_mode, maintenance_start_at, maintenance_end_at,
                          status, last_checkin_at, down_since, status_since,
                          category,
                          created_at, last_report_at
                   FROM server_monitors
                   ORDER BY name"""
            )
            heartbeat_rows = await conn.fetch(
                """SELECT id, sid, name, heartbeat_type, timeout, grace_period,
                          enabled, notifications_enabled, is_public,
                          maintenance_mode, maintenance_start_at, maintenance_end_at,
                          status, last_checkin_at, down_since, status_since,
                          category,
                          private_notes, created_at, updated_at, last_ping_at
                   FROM heartbeat_monitors
                   ORDER BY name"""
            )
            incident_rows = await conn.fetch(
                "SELECT * FROM incidents ORDER BY started_at DESC"
            )
            uptime_check_rows = await conn.fetch(
                "SELECT * FROM uptime_checks ORDER BY monitor_id, checked_at"
            )
            server_history_rows = await conn.fetch(
                "SELECT * FROM server_history ORDER BY server_id, timestamp"
            )
            heartbeat_ping_rows = await conn.fetch(
                "SELECT * FROM heartbeat_pings ORDER BY monitor_id, pinged_at"
            )
            maintenance_event_rows = await conn.fetch(
                "SELECT * FROM maintenance_events ORDER BY monitor_type, monitor_id, start_at"
            )
            monitor_minute_rows = await conn.fetch(
                "SELECT * FROM monitor_minutes ORDER BY monitor_id, minute"
            )

        entities = {
            "users": {row["id"]: dict(row) for row in user_rows},
            "uptime": {row["id"]: dict(row) for row in uptime_rows},
            "server": {row["id"]: dict(row) for row in server_rows},
            "heartbeat": {row["id"]: dict(row) for row in heartbeat_rows},
            "incidents": {row["id"]: dict(row) for row in incident_rows},
        }
        indexes = {
            "user_email": {str(row["email"]).lower(): row["id"] for row in user_rows if row.get("email")},
            "server_sid": {row["sid"]: row["id"] for row in server_rows if row.get("sid")},
            "heartbeat_sid": {row["sid"]: row["id"] for row in heartbeat_rows if row.get("sid")},
        }

        uptime_checks: dict[Any, list[dict[str, Any]]] = {}
        for row in uptime_check_rows:
            monitor_id = row["monitor_id"]
            uptime_checks.setdefault(monitor_id, []).append(dict(row))

        server_history: dict[Any, list[dict[str, Any]]] = {}
        for row in server_history_rows:
            server_id = row["server_id"]
            server_history.setdefault(server_id, []).append(dict(row))

        heartbeat_pings: dict[Any, list[dict[str, Any]]] = {}
        for row in heartbeat_ping_rows:
            monitor_id = row["monitor_id"]
            heartbeat_pings.setdefault(monitor_id, []).append(dict(row))

        maintenance_events: dict[Any, list[dict[str, Any]]] = {}
        for row in maintenance_event_rows:
            key = (row["monitor_type"], row["monitor_id"])
            maintenance_events.setdefault(key, []).append(dict(row))

        monitor_minutes: dict[Any, list[dict[str, Any]]] = {}
        for row in monitor_minute_rows:
            monitor_id = row["monitor_id"]
            monitor_minutes.setdefault(monitor_id, []).append(dict(row))

        series = {
            "uptime_checks": uptime_checks,
            "server_history": server_history,
            "heartbeat_pings": heartbeat_pings,
            "maintenance_events": maintenance_events,
            "monitor_minutes": monitor_minutes,
        }
        return {"entities": entities, "indexes": indexes, "series": series}

    async def _apply_snapshot_to_memory(self, snapshot: dict[str, Any]) -> None:
        entities = snapshot.get("entities") or {}
        indexes = snapshot.get("indexes") or {}
        series = snapshot.get("series") or {}

        async with self._cache_lock:
            self._cache["users"] = {k: dict(v) for k, v in (entities.get("users") or {}).items()}
            self._cache["uptime"] = {k: dict(v) for k, v in (entities.get("uptime") or {}).items()}
            self._cache["server"] = {k: dict(v) for k, v in (entities.get("server") or {}).items()}
            self._cache["heartbeat"] = {k: dict(v) for k, v in (entities.get("heartbeat") or {}).items()}
            self._cache["incidents"] = {k: dict(v) for k, v in (entities.get("incidents") or {}).items()}
            self._cache_user_email = dict(indexes.get("user_email") or {})
            self._cache_sid["server"] = dict(indexes.get("server_sid") or {})
            self._cache_sid["heartbeat"] = dict(indexes.get("heartbeat_sid") or {})
            self._cache["uptime_checks"] = {
                k: [dict(item) for item in (rows or [])]
                for k, rows in (series.get("uptime_checks") or {}).items()
            }
            self._cache["server_history"] = {
                k: [dict(item) for item in (rows or [])]
                for k, rows in (series.get("server_history") or {}).items()
            }
            self._cache["heartbeat_pings"] = {
                k: [dict(item) for item in (rows or [])]
                for k, rows in (series.get("heartbeat_pings") or {}).items()
            }
            self._cache["maintenance_events"] = {
                k: [dict(item) for item in (rows or [])]
                for k, rows in (series.get("maintenance_events") or {}).items()
            }
            self._cache["monitor_minutes"] = {
                k: [dict(item) for item in (rows or [])]
                for k, rows in (series.get("monitor_minutes") or {}).items()
            }

    async def get_cache_stats(self) -> dict:
        service_stats: dict[str, Any] = {}
        if self.cache_service:
            try:
                service_stats = await self.cache_service.stats()
            except Exception as exc:
                logger.warning("Failed to collect cache backend stats: %s", exc)

        if not self.cache_enabled:
            return {
                "enabled": False,
                "backend": service_stats.get("backend", "inmemory"),
                "connected": service_stats.get("connected", False),
                "healthy": service_stats.get("healthy", False),
                "last_error": service_stats.get("last_error"),
                "loaded_at": service_stats.get("loaded_at") or self.cache_loaded_at,
                "counts": service_stats.get("counts", {}),
            }
        async with self._cache_lock:
            uptime_checks = sum(len(v) for v in self._cache["uptime_checks"].values())
            server_history = sum(len(v) for v in self._cache["server_history"].values())
            heartbeat_pings = sum(len(v) for v in self._cache["heartbeat_pings"].values())
            maintenance_events = sum(len(v) for v in self._cache["maintenance_events"].values())
            monitor_minutes = sum(len(v) for v in self._cache["monitor_minutes"].values())
            counts = {
                "users": len(self._cache["users"]),
                "uptime": len(self._cache["uptime"]),
                "server": len(self._cache["server"]),
                "heartbeat": len(self._cache["heartbeat"]),
                "incidents": len(self._cache["incidents"]),
                "uptime_checks": uptime_checks,
                "server_history": server_history,
                "heartbeat_pings": heartbeat_pings,
                "maintenance_events": maintenance_events,
                "monitor_minutes": monitor_minutes
            }
            counts["total_items"] = sum(counts.values())
        return {
            "enabled": True,
            "backend": service_stats.get("backend", "inmemory"),
            "connected": service_stats.get("connected", True),
            "healthy": service_stats.get("healthy", True),
            "last_error": service_stats.get("last_error"),
            "loaded_at": service_stats.get("loaded_at") or self.cache_loaded_at,
            "counts": counts,
        }

    async def ensure_cache_available(self) -> None:
        if not self.cache_service:
            return
        await self.cache_service.ensure_available()

    async def mark_cache_unhealthy(self, reason: str) -> None:
        if self.cache_service:
            await self.cache_service.mark_unhealthy(reason)

    @property
    def cache_backend_name(self) -> str:
        if not self.cache_service:
            return "inmemory"
        return self.cache_service.cache_backend_name

    async def get_cached_monitor_state(self, kind: str, monitor_id: uuid.UUID) -> dict:
        if self.cache_enabled:
            async with self._cache_lock:
                monitor = self._cache.get(kind, {}).get(monitor_id)
                return dict(monitor) if monitor else {}

        if kind == "uptime":
            monitor = await self.get_uptime_monitor_by_id(monitor_id)
            return dict(monitor) if monitor else {}
        if kind == "server":
            monitor = await self.get_server_monitor_by_id(monitor_id)
            return dict(monitor) if monitor else {}
        if kind == "heartbeat":
            monitor = await self.get_heartbeat_monitor_by_id(monitor_id)
            return dict(monitor) if monitor else {}
        return {}

    async def resync_cache_from_db(self) -> None:
        if not self.cache_service:
            return
        async with self._cache_resync_lock:
            snapshot = await self.get_cache_snapshot()

            async def _loader():
                return snapshot

            await self.cache_service.warmup_from_loader(_loader)
            await self._apply_snapshot_to_memory(snapshot)
            self.cache_enabled = True
            self.cache_loaded_at = utcnow()
            self._last_cache_resync_at = utcnow()

    def schedule_cache_resync(self, delay_seconds: float = 0.2) -> None:
        """Debounced background cache rebuild for write-through consistency."""
        if not self.cache_service:
            return
        if self._last_cache_resync_at:
            elapsed = (utcnow() - self._last_cache_resync_at).total_seconds()
            if elapsed < self._cache_resync_min_interval_seconds:
                return
        if self._cache_resync_task and not self._cache_resync_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        async def _task():
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
            try:
                await self.resync_cache_from_db()
            except Exception as exc:
                await self.mark_cache_unhealthy(f"cache resync failed: {exc}")
                logger.exception("Cache resync failed")

        self._cache_resync_task = loop.create_task(_task())

    def _apply_due_maintenance_cache(self, kind: str):
        now = utcnow()
        cache = self._cache.get(kind, {})
        for monitor in cache.values():
            start_at = monitor.get("maintenance_start_at")
            end_at = monitor.get("maintenance_end_at")
            if start_at and start_at <= now and (end_at is None or end_at > now):
                monitor["maintenance_mode"] = True
            elif end_at and end_at <= now:
                monitor["maintenance_mode"] = False
                monitor["maintenance_start_at"] = None
                monitor["maintenance_end_at"] = None

    def _cache_kind_from_type(self, monitor_type: str) -> str | None:
        key = (monitor_type or "").strip().lower().replace(" ", "_")
        if key in ("website", "uptime"):
            return "uptime"
        if key in ("heartbeat", "heartbeat-cronjob", "cronjob"):
            return "heartbeat"
        if key in ("heartbeat-server-agent", "server_agent", "server", "server-agent", "agent"):
            return "server"
        return None

    @staticmethod
    def _normalize_monitor_name(name: str | None) -> str:
        return str(name or "").strip().lower()

    async def _is_monitor_name_taken_in_db(
        self, normalized_name: str, exclude_monitor_id: uuid.UUID | None = None
    ) -> bool:
        if not self.pool:
            return False
        query = """
            SELECT 1
              FROM (
                    SELECT id, name FROM uptime_monitors
                    UNION ALL
                    SELECT id, name FROM server_monitors
                    UNION ALL
                    SELECT id, name FROM heartbeat_monitors
                   ) AS monitor_names
             WHERE lower(btrim(name)) = $1
        """
        params: list[Any] = [normalized_name]
        if exclude_monitor_id:
            query += " AND id <> $2"
            params.append(exclude_monitor_id)
        query += " LIMIT 1"
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, *params)
            return row is not None

    async def is_monitor_name_taken(self, name: str, exclude_monitor_id: uuid.UUID | None = None) -> bool:
        normalized = self._normalize_monitor_name(name)
        if not normalized:
            return False

        if self.cache_enabled:
            matched_cache_entries = []
            async with self._cache_lock:
                for kind in ("uptime", "server", "heartbeat"):
                    for monitor_id, monitor in self._cache.get(kind, {}).items():
                        if exclude_monitor_id and monitor_id == exclude_monitor_id:
                            continue
                        if self._normalize_monitor_name(monitor.get("name")) == normalized:
                            matched_cache_entries.append((kind, monitor_id))

            if not matched_cache_entries:
                return False

            # Confirm with DB to avoid stale cache false positives in multi-worker setups.
            try:
                exists_in_db = await self._is_monitor_name_taken_in_db(normalized, exclude_monitor_id)
            except Exception:
                # Fail safe on transient DB errors: treat as taken.
                return True
            if exists_in_db:
                return True

            # Cache contained only stale entries; prune those specific monitor IDs.
            async with self._cache_lock:
                for kind, monitor_id in matched_cache_entries:
                    live = self._cache.get(kind, {}).get(monitor_id)
                    if not live:
                        continue
                    if exclude_monitor_id and monitor_id == exclude_monitor_id:
                        continue
                    if self._normalize_monitor_name(live.get("name")) != normalized:
                        continue

                    if kind == "uptime":
                        self._cache["uptime"].pop(monitor_id, None)
                        self._cache["uptime_checks"].pop(monitor_id, None)
                    elif kind == "server":
                        sid = live.get("sid")
                        self._cache["server"].pop(monitor_id, None)
                        self._cache["server_history"].pop(monitor_id, None)
                        if sid:
                            self._cache_sid["server"].pop(sid, None)
                    elif kind == "heartbeat":
                        sid = live.get("sid")
                        self._cache["heartbeat"].pop(monitor_id, None)
                        self._cache["heartbeat_pings"].pop(monitor_id, None)
                        if sid:
                            self._cache_sid["heartbeat"].pop(sid, None)

                    self._cache["monitor_minutes"].pop(monitor_id, None)
                    for incident_id, incident in list(self._cache["incidents"].items()):
                        if str(incident.get("monitor_id")) == str(monitor_id):
                            self._cache["incidents"].pop(incident_id, None)
                    for event_key in list(self._cache["maintenance_events"].keys()):
                        if isinstance(event_key, tuple) and len(event_key) == 2 and str(event_key[1]) == str(monitor_id):
                            self._cache["maintenance_events"].pop(event_key, None)
            return False

        if self.cache_only:
            return False

        return await self._is_monitor_name_taken_in_db(normalized, exclude_monitor_id)

    @staticmethod
    def _normalize_uptime_status(status: str) -> str:
        return str(status).strip().lower()

    @staticmethod
    def _merge_minute_status(existing: str, incoming: str) -> str:
        """Resolve status conflicts for a monitor/minute tuple."""
        priority = {"down": 0, "up": 1, "maintenance": 2}
        existing_rank = priority.get(existing, -1)
        incoming_rank = priority.get(incoming, -1)
        return incoming if incoming_rank >= existing_rank else existing

    async def write_monitor_minute(self, monitor_id: uuid.UUID, minute: datetime, status: str) -> None:
        if self.cache_enabled:
            async with self._cache_lock:
                minutes_list = self._cache["monitor_minutes"].setdefault(monitor_id, [])
                for i, existing in enumerate(minutes_list):
                    if existing["minute"] == minute:
                        minutes_list[i]["status"] = self._merge_minute_status(existing["status"], status)
                        break
                else:
                    minutes_list.append({"monitor_id": monitor_id, "minute": minute, "status": status})
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO monitor_minutes (monitor_id, minute, status)
                       VALUES ($1, $2, $3)
                       ON CONFLICT (monitor_id, minute) DO UPDATE
                       SET status = CASE
                           WHEN monitor_minutes.status = 'maintenance' OR EXCLUDED.status = 'maintenance' THEN 'maintenance'
                           WHEN monitor_minutes.status = 'up' OR EXCLUDED.status = 'up' THEN 'up'
                           ELSE 'down'
                       END""",
                    monitor_id, minute, status
                )
        except Exception:
            logger.debug("Failed to write monitor minute to DB for monitor_id=%s", monitor_id, exc_info=True)

    async def write_monitor_minutes_batch(self, records: list) -> None:
        if not records:
            return
        if self.cache_enabled:
            async with self._cache_lock:
                for monitor_id, minute, status in records:
                    minutes_list = self._cache["monitor_minutes"].setdefault(monitor_id, [])
                    for i, existing in enumerate(minutes_list):
                        if existing["minute"] == minute:
                            minutes_list[i]["status"] = self._merge_minute_status(existing["status"], status)
                            break
                    else:
                        minutes_list.append({"monitor_id": monitor_id, "minute": minute, "status": status})
        try:
            async with self.pool.acquire() as conn:
                await conn.executemany(
                    """INSERT INTO monitor_minutes (monitor_id, minute, status)
                       VALUES ($1, $2, $3)
                       ON CONFLICT (monitor_id, minute) DO UPDATE
                       SET status = CASE
                           WHEN monitor_minutes.status = 'maintenance' OR EXCLUDED.status = 'maintenance' THEN 'maintenance'
                           WHEN monitor_minutes.status = 'up' OR EXCLUDED.status = 'up' THEN 'up'
                           ELSE 'down'
                       END""",
                    records
                )
        except Exception:
            logger.debug("Failed to batch write monitor minutes to DB", exc_info=True)

    def get_monitor_minutes_cached(self, monitor_id: uuid.UUID, start: datetime, end: datetime) -> list[dict]:
        """Get minute records from cache for a time range (sync, no lock — caller must hold lock)."""
        minutes_list = self._cache["monitor_minutes"].get(monitor_id, [])
        return [m for m in minutes_list if start <= m["minute"] < end]

    async def get_monitor_minutes(self, monitor_id: uuid.UUID, start: datetime, end: datetime) -> list[dict]:
        if self.cache_enabled:
            async with self._cache_lock:
                return self.get_monitor_minutes_cached(monitor_id, start, end)
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT monitor_id, minute, status FROM monitor_minutes
                   WHERE monitor_id = $1 AND minute >= $2 AND minute < $3
                   ORDER BY minute""",
                monitor_id, start, end
            )
            return [dict(r) for r in rows]

    async def count_monitor_minutes(self, monitor_id: uuid.UUID, start: datetime, end: datetime) -> dict:
        if self.cache_enabled:
            async with self._cache_lock:
                records = self.get_monitor_minutes_cached(monitor_id, start, end)
            counts = {"up": 0, "down": 0, "maintenance": 0}
            for r in records:
                s = r.get("status", "")
                if s in counts:
                    counts[s] += 1
            return counts
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT status, COUNT(*) as cnt FROM monitor_minutes
                   WHERE monitor_id = $1 AND minute >= $2 AND minute < $3
                   GROUP BY status""",
                monitor_id, start, end
            )
            counts = {"up": 0, "down": 0, "maintenance": 0}
            for row in rows:
                s = row["status"]
                if s in counts:
                    counts[s] = row["cnt"]
            return counts

    async def update_monitor_status(self, kind: str, monitor_id: uuid.UUID,
                                     status: str, last_checkin_at: datetime | None = None,
                                     down_since: datetime | None = None) -> None:
        status_lower = str(status or "").strip().lower()
        status_since: datetime | None = None
        if status_lower == "down":
            status_since = down_since or utcnow()
        elif status_lower == "up":
            status_since = last_checkin_at or utcnow()
        elif status_lower == "maintenance":
            status_since = utcnow()

        if self.cache_enabled:
            async with self._cache_lock:
                monitor = self._cache.get(kind, {}).get(monitor_id)
                if monitor:
                    previous_status = str(monitor.get("status") or "").strip().lower()
                    if status_since and (previous_status != status_lower or monitor.get("status_since") is None):
                        monitor["status_since"] = status_since
                    monitor["status"] = status
                    if last_checkin_at is not None:
                        monitor["last_checkin_at"] = last_checkin_at
                    monitor["down_since"] = down_since
        table_map = {"uptime": "uptime_monitors", "server": "server_monitors", "heartbeat": "heartbeat_monitors"}
        table = table_map.get(kind)
        if table:
            try:
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        f"""
                        UPDATE {table} AS m
                           SET status = $1::varchar,
                               last_checkin_at = $2,
                               down_since = $3,
                               status_since = CASE
                                   WHEN m.status IS DISTINCT FROM $1::varchar THEN COALESCE($4::timestamp, m.status_since, (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'))
                                   ELSE m.status_since
                               END
                         WHERE m.id = $5
                        """,
                        status, last_checkin_at, down_since, status_since, monitor_id
                    )
            except Exception:
                logger.exception(
                    "Failed to update monitor status kind=%s monitor_id=%s status=%s",
                    kind, monitor_id, status
                )

    async def mark_monitor_down_if_unchanged(
        self,
        cache_kind: str,
        monitor_id: uuid.UUID,
        expected_last_checkin_at: datetime,
        stale_before: datetime,
        down_since: datetime,
    ) -> bool:
        """Mark monitor DOWN only if last_checkin_at has not changed."""
        table_map = {
            "uptime": "uptime_monitors",
            "server": "server_monitors",
            "heartbeat": "heartbeat_monitors",
        }
        table = table_map.get(cache_kind)
        if not table or not self.pool:
            return False

        transitioned = False
        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute(
                    f"""
                    UPDATE {table}
                       SET status = 'down',
                           down_since = $1,
                           status_since = $1
                     WHERE id = $2
                       AND status <> 'down'
                       AND last_checkin_at = $3
                       AND date_trunc('minute', last_checkin_at) < $4
                    """,
                    down_since,
                    monitor_id,
                    expected_last_checkin_at,
                    stale_before,
                )
            transitioned = result == "UPDATE 1"
        except Exception:
            logger.exception("Failed to mark monitor down: kind=%s, id=%s", cache_kind, monitor_id)
            return False

        if transitioned and self.cache_enabled:
            async with self._cache_lock:
                monitor = self._cache.get(cache_kind, {}).get(monitor_id)
                if monitor:
                    monitor["status"] = "down"
                    monitor["last_checkin_at"] = expected_last_checkin_at
                    monitor["down_since"] = down_since
                    monitor["status_since"] = down_since
        return transitioned

    async def record_maintenance_start(
        self,
        monitor_type: str,
        monitor_id: uuid.UUID,
        start_at: datetime | None = None
    ) -> None:
        event_type = self._normalize_maintenance_event_type(monitor_type)
        if not event_type:
            return
        ts = start_at or utcnow()
        event_id = uuid.uuid4()
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO maintenance_events (id, monitor_type, monitor_id, start_at)
                VALUES ($1, $2, $3, $4)
                """,
                event_id, event_type, monitor_id, ts
            )
        if self.cache_enabled:
            async with self._cache_lock:
                key = (event_type, monitor_id)
                events = self._cache["maintenance_events"].setdefault(key, [])
                events.append({
                    "id": event_id,
                    "monitor_type": event_type,
                    "monitor_id": monitor_id,
                    "start_at": ts,
                    "end_at": None,
                    "created_at": ts
                })

    async def record_maintenance_end(
        self,
        monitor_type: str,
        monitor_id: uuid.UUID,
        end_at: datetime | None = None
    ) -> None:
        event_type = self._normalize_maintenance_event_type(monitor_type)
        if not event_type:
            return
        ts = end_at or utcnow()
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """
                WITH latest AS (
                    SELECT id
                    FROM maintenance_events
                    WHERE monitor_type = $1
                      AND monitor_id = $2
                      AND end_at IS NULL
                    ORDER BY start_at DESC
                    LIMIT 1
                )
                UPDATE maintenance_events
                   SET end_at = $3
                 WHERE id IN (SELECT id FROM latest)
                """,
                event_type, monitor_id, ts
            )
            if result != "UPDATE 1":
                await conn.execute(
                    """
                    INSERT INTO maintenance_events (monitor_type, monitor_id, start_at, end_at)
                    VALUES ($1, $2, $3, $4)
                    """,
                    event_type, monitor_id, ts, ts
                )
        if self.cache_enabled:
            async with self._cache_lock:
                key = (event_type, monitor_id)
                events = self._cache["maintenance_events"].setdefault(key, [])
                for event in reversed(events):
                    if event.get("end_at") is None:
                        event["end_at"] = ts
                        break
                else:
                    events.append({
                        "id": uuid.uuid4(),
                        "monitor_type": event_type,
                        "monitor_id": monitor_id,
                        "start_at": ts,
                        "end_at": ts,
                        "created_at": ts
                    })

    async def get_maintenance_events(
        self,
        monitor_type: str,
        monitor_id: uuid.UUID,
        start_at: datetime,
        end_at: datetime
    ) -> list[dict]:
        event_type = self._normalize_maintenance_event_type(monitor_type)
        if not event_type:
            return []
        if self.cache_enabled:
            async with self._cache_lock:
                events = list(self._cache["maintenance_events"].get((event_type, monitor_id), []))
            filtered = [
                dict(e) for e in events
                if e.get("start_at") is not None
                and e["start_at"] < end_at
                and (e.get("end_at") is None or e["end_at"] >= start_at)
            ]
            filtered.sort(key=lambda e: e.get("start_at") or datetime.min)
            return filtered
        if self.cache_only:
            return []
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT start_at, end_at
                FROM maintenance_events
                WHERE monitor_type = $1
                  AND monitor_id = $2
                  AND start_at < $3
                  AND (end_at IS NULL OR end_at >= $4)
                ORDER BY start_at ASC
                """,
                event_type, monitor_id, end_at, start_at
            )
            return [dict(r) for r in rows]

    async def start_monitor_maintenance_now(
        self,
        monitor_type: str,
        monitor_id: uuid.UUID,
    ) -> bool:
        table = self._resolve_maintenance_table(monitor_type)
        if not table:
            raise ValueError("Unsupported monitor type")

        async with self.pool.acquire() as conn:
            await self._apply_due_maintenance_windows(conn)
            now = utcnow()
            result = await conn.execute(
                f"""
                UPDATE {table}
                   SET maintenance_mode = true,
                       maintenance_start_at = $2,
                       maintenance_end_at = NULL
                 WHERE id = $1
                """,
                monitor_id, now
            )
            if result == "UPDATE 1":
                await self.record_maintenance_start(monitor_type, monitor_id, start_at=now)
                if self.cache_enabled:
                    kind = self._cache_kind_from_type(monitor_type)
                    if kind:
                        async with self._cache_lock:
                            cached = self._cache[kind].get(monitor_id)
                            if cached:
                                cached["maintenance_mode"] = True
                                cached["maintenance_start_at"] = now
                                cached["maintenance_end_at"] = None
                return True
            return False

    async def schedule_monitor_maintenance(
        self,
        monitor_type: str,
        monitor_id: uuid.UUID,
        start_at: datetime,
        end_at: datetime,
    ) -> bool:
        table = self._resolve_maintenance_table(monitor_type)
        if not table:
            raise ValueError("Unsupported monitor type")

        async with self.pool.acquire() as conn:
            await self._apply_due_maintenance_windows(conn)
            result = await conn.execute(
                f"""
                UPDATE {table}
                   SET maintenance_mode = false,
                       maintenance_start_at = $2,
                       maintenance_end_at = $3
                 WHERE id = $1
                """,
                monitor_id, start_at, end_at
            )
            success = result == "UPDATE 1"

        if success and self.cache_enabled:
            kind = self._cache_kind_from_type(monitor_type)
            if kind:
                async with self._cache_lock:
                    cached = self._cache[kind].get(monitor_id)
                    if cached:
                        cached["maintenance_mode"] = False
                        cached["maintenance_start_at"] = start_at
                        cached["maintenance_end_at"] = end_at
        return success

    async def end_monitor_maintenance(self, monitor_type: str, monitor_id: uuid.UUID) -> bool:
        table = self._resolve_maintenance_table(monitor_type)
        if not table:
            raise ValueError("Unsupported monitor type")

        async with self.pool.acquire() as conn:
            await self._apply_due_maintenance_windows(conn)
            now = utcnow()
            result = await conn.execute(
                f"""
                UPDATE {table}
                   SET maintenance_mode = false,
                       maintenance_start_at = NULL,
                       maintenance_end_at = NULL
                 WHERE id = $1
                """,
                monitor_id
            )
            if result == "UPDATE 1":
                await self.record_maintenance_end(monitor_type, monitor_id, end_at=now)
                if self.cache_enabled:
                    kind = self._cache_kind_from_type(monitor_type)
                    if kind:
                        async with self._cache_lock:
                            cached = self._cache[kind].get(monitor_id)
                            if cached:
                                cached["maintenance_mode"] = False
                                cached["maintenance_start_at"] = None
                                cached["maintenance_end_at"] = None
                return True
            return False

    async def create_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    email VARCHAR(255) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    role VARCHAR(20) NOT NULL DEFAULT 'admin' CHECK (role IN ('admin', 'moderator')),
                    name VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS uptime_monitors (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name VARCHAR(255) NOT NULL,
                    type INTEGER NOT NULL CHECK (type IN (1,2,3,4,5,6,7,8,9,10)),
                    target VARCHAR(500) NOT NULL,
                    port INTEGER,
                    check_interval INTEGER DEFAULT 1,
                    timeout INTEGER DEFAULT 5,
                    category VARCHAR(100),
                    private_notes TEXT,
                    enabled BOOLEAN DEFAULT true,
                    notifications_enabled BOOLEAN DEFAULT true,
                    is_public BOOLEAN DEFAULT false,
                    maintenance_mode BOOLEAN DEFAULT false,
                    maintenance_start_at TIMESTAMP,
                    maintenance_end_at TIMESTAMP,
                    status VARCHAR(20) DEFAULT 'unknown',
                    last_checkin_at TIMESTAMP,
                    down_since TIMESTAMP,
                    status_since TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS uptime_monitor_config (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    monitor_id UUID UNIQUE REFERENCES uptime_monitors(id) ON DELETE CASCADE,
                    follow_redirects BOOLEAN DEFAULT true,
                    verify_ssl BOOLEAN DEFAULT true,
                    http_method VARCHAR(10) DEFAULT 'GET',
                    custom_headers TEXT,
                    post_data TEXT,
                    http_auth_username VARCHAR(255),
                    http_auth_password VARCHAR(255),
                    expected_status_codes VARCHAR(100),
                    keyword_to_search VARCHAR(500),
                    keyword_must_contain BOOLEAN DEFAULT true
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS server_monitors (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    sid VARCHAR(32) UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    os VARCHAR(500),
                    kernel VARCHAR(500),
                    hostname VARCHAR(500),
                    cpu_model VARCHAR(500),
                    cpu_sockets INTEGER,
                    cpu_cores INTEGER,
                    cpu_threads INTEGER,
                    ram_size BIGINT,
                    ram_swap_size BIGINT,
                    enabled BOOLEAN DEFAULT true,
                    notifications_enabled BOOLEAN DEFAULT true,
                    is_public BOOLEAN DEFAULT false,
                    maintenance_mode BOOLEAN DEFAULT false,
                    maintenance_start_at TIMESTAMP,
                    maintenance_end_at TIMESTAMP,
                    status VARCHAR(20) DEFAULT 'unknown',
                    last_checkin_at TIMESTAMP,
                    down_since TIMESTAMP,
                    status_since TIMESTAMP,
                    category VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_report_at TIMESTAMP
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS server_history (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    server_id UUID REFERENCES server_monitors(id) ON DELETE CASCADE,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    cpu_percent NUMERIC(5,2),
                    cpu_io_wait NUMERIC(5,2),
                    cpu_steal NUMERIC(5,2),
                    cpu_user NUMERIC(5,2),
                    cpu_system NUMERIC(5,2),
                    ram_percent NUMERIC(5,2),
                    ram_swap_percent NUMERIC(5,2),
                    ram_buff_percent NUMERIC(5,2),
                    ram_cache_percent NUMERIC(5,2),
                    load_1 NUMERIC(6,2),
                    load_5 NUMERIC(6,2),
                    load_15 NUMERIC(6,2),
                    network_in BIGINT,
                    network_out BIGINT,
                    disk_percent NUMERIC(5,2),
                    disks TEXT,
                    nics TEXT,
                    temperature TEXT
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS heartbeat_monitors (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    sid VARCHAR(32) UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    heartbeat_type VARCHAR(20) NOT NULL DEFAULT 'cronjob'
                        CHECK (heartbeat_type IN ('cronjob', 'server_agent')),
                    timeout INTEGER DEFAULT 60,
                    grace_period INTEGER DEFAULT 5,
                    category VARCHAR(100),
                    enabled BOOLEAN DEFAULT true,
                    notifications_enabled BOOLEAN DEFAULT true,
                    is_public BOOLEAN DEFAULT false,
                    maintenance_mode BOOLEAN DEFAULT false,
                    maintenance_start_at TIMESTAMP,
                    maintenance_end_at TIMESTAMP,
                    status VARCHAR(20) DEFAULT 'unknown',
                    last_checkin_at TIMESTAMP,
                    down_since TIMESTAMP,
                    status_since TIMESTAMP,
                    private_notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_ping_at TIMESTAMP
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS maintenance_events (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    monitor_type VARCHAR(20) NOT NULL,
                    monitor_id UUID NOT NULL,
                    start_at TIMESTAMP NOT NULL,
                    end_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_maintenance_events_monitor
                    ON maintenance_events(monitor_type, monitor_id, start_at)
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS heartbeat_pings (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    monitor_id UUID REFERENCES heartbeat_monitors(id) ON DELETE CASCADE,
                    ping_source VARCHAR(100),
                    pinged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS incidents (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    monitor_type VARCHAR(20) NOT NULL CHECK (monitor_type IN ('uptime', 'heartbeat', 'ssl', 'domain')),
                    monitor_id UUID NOT NULL,
                    incident_type VARCHAR(20) NOT NULL CHECK (incident_type IN ('down', 'up', 'warning', 'info')),
                    status VARCHAR(20) NOT NULL CHECK (status IN ('open', 'resolved')),
                    source VARCHAR(20) NOT NULL DEFAULT 'monitor' CHECK (source IN ('monitor', 'admin')),
                    template_key VARCHAR(100),
                    title VARCHAR(500) NOT NULL,
                    description TEXT,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at TIMESTAMP,
                    hidden_from_status_page BOOLEAN NOT NULL DEFAULT false,
                    hidden_from_status_page_at TIMESTAMP,
                    notification_sent BOOLEAN DEFAULT false
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS uptime_checks (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    monitor_id UUID REFERENCES uptime_monitors(id) ON DELETE CASCADE,
                    status VARCHAR(20) NOT NULL CHECK (status IN ('up', 'down', 'timeout', 'error')),
                    response_time_ms INTEGER,
                    status_code INTEGER,
                    error_message TEXT,
                    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS monitor_minutes (
                    monitor_id UUID NOT NULL,
                    minute TIMESTAMP NOT NULL,
                    status VARCHAR(20) NOT NULL CHECK (status IN ('up', 'down', 'maintenance')),
                    PRIMARY KEY (monitor_id, minute)
                )
            """)

            await self._create_indexes(conn)

    async def _create_indexes(self, conn):
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_uptime_monitors_type ON uptime_monitors(type)",
            "CREATE INDEX IF NOT EXISTS idx_uptime_monitors_enabled ON uptime_monitors(enabled) WHERE enabled = true",
            "CREATE INDEX IF NOT EXISTS idx_uptime_checks_monitor_id ON uptime_checks(monitor_id)",
            "CREATE INDEX IF NOT EXISTS idx_uptime_checks_checked_at ON uptime_checks(checked_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_uptime_checks_monitor_time ON uptime_checks(monitor_id, checked_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_uptime_checks_monitor_time_status ON uptime_checks(monitor_id, checked_at DESC, status)",
            "CREATE INDEX IF NOT EXISTS idx_server_monitors_sid ON server_monitors(sid)",
            "CREATE INDEX IF NOT EXISTS idx_server_history_server_id ON server_history(server_id)",
            "CREATE INDEX IF NOT EXISTS idx_server_history_timestamp ON server_history(timestamp DESC)",
            "CREATE INDEX IF NOT EXISTS idx_server_history_server_time ON server_history(server_id, timestamp DESC)",
            "CREATE INDEX IF NOT EXISTS idx_uptime_maintenance_window ON uptime_monitors(maintenance_mode, maintenance_start_at, maintenance_end_at)",
            "CREATE INDEX IF NOT EXISTS idx_server_maintenance_window ON server_monitors(maintenance_mode, maintenance_start_at, maintenance_end_at)",
            "CREATE INDEX IF NOT EXISTS idx_heartbeat_maintenance_window ON heartbeat_monitors(maintenance_mode, maintenance_start_at, maintenance_end_at)",
            "CREATE INDEX IF NOT EXISTS idx_incidents_monitor ON incidents(monitor_type, monitor_id)",
            "CREATE INDEX IF NOT EXISTS idx_incidents_status ON incidents(status)",
            "CREATE INDEX IF NOT EXISTS idx_incidents_source ON incidents(source)",
            "CREATE INDEX IF NOT EXISTS idx_incidents_started_at ON incidents(started_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_incidents_resolved_at ON incidents(resolved_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_incidents_hidden ON incidents(hidden_from_status_page)",
            "CREATE INDEX IF NOT EXISTS idx_heartbeat_monitors_sid ON heartbeat_monitors(sid)",
            "CREATE INDEX IF NOT EXISTS idx_heartbeat_pings_monitor_id ON heartbeat_pings(monitor_id)",
            "CREATE INDEX IF NOT EXISTS idx_monitor_minutes_lookup ON monitor_minutes(monitor_id, minute DESC)",
        ]

        for index_sql in indexes:
            try:
                await conn.execute(index_sql)
            except Exception:
                logger.debug("Failed to create index: %s", index_sql, exc_info=True)

    async def get_user_by_email(self, email: str) -> dict | None:
        if self.cache_enabled:
            key = str(email).lower()
            async with self._cache_lock:
                user_id = self._cache_user_email.get(key)
                if user_id:
                    cached = self._cache["users"].get(user_id)
                    return dict(cached) if cached else None
            return None
        if self.cache_only:
            return None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM users WHERE email = $1",
                email
            )
            return dict(row) if row else None

    async def create_user(self, email: str, password_hash: str, role: str, name: str | None = None) -> uuid.UUID:
        user_id = uuid.uuid4()
        created_at = utcnow()
        cache_entry = {
            "id": user_id,
            "email": email,
            "password_hash": password_hash,
            "role": role,
            "name": name,
            "created_at": created_at
        }
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (id, email, password_hash, role, name, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
                user_id, email, password_hash, role, name, created_at
            )
        if self.cache_enabled:
            async with self._cache_lock:
                self._cache["users"][user_id] = cache_entry
                self._cache_user_email[str(email).lower()] = user_id
        return user_id

    async def get_user_by_id(self, user_id: uuid.UUID) -> dict | None:
        if self.cache_enabled:
            async with self._cache_lock:
                cached = self._cache["users"].get(user_id)
                return dict(cached) if cached else None
        if self.cache_only:
            return None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, email, role, name, created_at FROM users WHERE id = $1",
                user_id
            )
            return dict(row) if row else None

    async def get_uptime_monitors(self, enabled_only: bool = False, public_only: bool = False) -> list[dict]:
        if self.cache_enabled:
            async with self._cache_lock:
                self._apply_due_maintenance_cache("uptime")
                items = list(self._cache["uptime"].values())
            if enabled_only:
                items = [m for m in items if m.get("enabled")]
            if public_only:
                items = [m for m in items if m.get("is_public")]
            return [dict(m) for m in items]
        if self.cache_only:
            return []

        async with self.pool.acquire() as conn:
            await self._apply_due_maintenance_windows(conn)
            base_query = """SELECT um.id, um.name, um.type, um.target, um.port, um.check_interval, um.timeout,
                              um.category, um.enabled, um.notifications_enabled, um.is_public,
                              um.maintenance_mode, um.maintenance_start_at, um.maintenance_end_at,
                              um.status, um.last_checkin_at, um.down_since, um.status_since,
                              um.created_at,
                              (SELECT checked_at FROM uptime_checks WHERE monitor_id = um.id ORDER BY checked_at DESC LIMIT 1) as last_check_at,
                              (SELECT checked_at FROM uptime_checks WHERE monitor_id = um.id AND status = 'up' ORDER BY checked_at DESC LIMIT 1) as last_up_at
                       FROM uptime_monitors um"""
            conditions = ["um.type = 1"]
            if enabled_only:
                conditions.append("um.enabled = true")
            if public_only:
                conditions.append("um.is_public = true")
            
            base_query += " WHERE " + " AND ".join(conditions)
            base_query += " ORDER BY um.name"
            
            rows = await conn.fetch(base_query)
            return [dict(row) for row in rows]

    async def get_uptime_monitor_by_id(self, monitor_id: uuid.UUID) -> dict | None:
        if self.cache_enabled:
            async with self._cache_lock:
                self._apply_due_maintenance_cache("uptime")
                cached = self._cache["uptime"].get(monitor_id)
                return dict(cached) if cached else None
        if self.cache_only:
            return None

        async with self.pool.acquire() as conn:
            await self._apply_due_maintenance_windows(conn)
            row = await conn.fetchrow(
                """SELECT id, name, type, target, port, check_interval, timeout,
                          category, private_notes, is_public,
                          enabled, notifications_enabled,
                          maintenance_mode, maintenance_start_at, maintenance_end_at,
                          status, last_checkin_at, down_since, status_since,
                          created_at,
                          (SELECT checked_at FROM uptime_checks WHERE monitor_id = $1 ORDER BY checked_at DESC LIMIT 1) as last_check_at,
                          (SELECT checked_at FROM uptime_checks WHERE monitor_id = $1 AND status = 'up' ORDER BY checked_at DESC LIMIT 1) as last_up_at
                   FROM uptime_monitors WHERE id = $1""",
                monitor_id
            )
            if not row:
                return None
            data = dict(row)
            if self.cache_enabled:
                async with self._cache_lock:
                    self._cache["uptime"][monitor_id] = data
            return data

    async def create_uptime_monitor(
        self,
        name: str,
        monitor_type: int,
        target: str,
        port: int | None = None,
        check_interval: int = 1,
        timeout: int = 5,
        category: str | None = None,
        private_notes: str | None = None
    ) -> uuid.UUID:
        monitor_id = uuid.uuid4()
        now = utcnow()
        cache_entry = {
            "id": monitor_id,
            "name": name,
            "type": monitor_type,
            "target": target,
            "port": port,
            "check_interval": check_interval,
            "timeout": timeout,
            "category": category,
            "private_notes": private_notes,
            "enabled": True,
            "notifications_enabled": True,
            "is_public": False,
            "maintenance_mode": False,
            "maintenance_start_at": None,
            "maintenance_end_at": None,
            "status": "unknown",
            "last_checkin_at": None,
            "down_since": None,
            "status_since": None,
            "created_at": now,
            "updated_at": now,
            "last_check_at": None,
            "last_up_at": None,
        }

        if self.cache_enabled:
            async with self._cache_lock:
                self._cache["uptime"][monitor_id] = cache_entry

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO uptime_monitors
                       (id, name, type, target, port, check_interval, timeout, category, private_notes, created_at, updated_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)""",
                    monitor_id, name, monitor_type, target, port,
                    check_interval, timeout, category, private_notes, now, now
                )
        except Exception:
            if self.cache_enabled:
                async with self._cache_lock:
                    self._cache["uptime"].pop(monitor_id, None)
            raise
        return monitor_id

    async def update_uptime_monitor(self, monitor_id: uuid.UUID, **kwargs) -> bool:
        _bad_keys = set(kwargs) - _UPTIME_UPDATABLE_FIELDS - {"updated_at"}
        if _bad_keys:
            raise ValueError(f"Invalid uptime monitor fields: {_bad_keys}")
        updated_at = utcnow()
        old_cache = None
        if self.cache_enabled:
            async with self._cache_lock:
                cached = self._cache["uptime"].get(monitor_id)
                if cached:
                    old_cache = dict(cached)
                    for key, value in kwargs.items():
                        if value is not None:
                            cached[key] = value
                    cached["updated_at"] = updated_at

        async with self.pool.acquire() as conn:
            kwargs['updated_at'] = updated_at
            fields = []
            values = []
            idx = 1
            for key, value in kwargs.items():
                if value is not None:
                    fields.append(f"{key} = ${idx}")
                    values.append(value)
                    idx += 1
            if not fields:
                return False
            values.append(monitor_id)
            query = f"UPDATE uptime_monitors SET {', '.join(fields)} WHERE id = ${idx}"
            result = await conn.execute(query, *values)
            success = result == "UPDATE 1"

        if not success and old_cache is not None and self.cache_enabled:
            async with self._cache_lock:
                self._cache["uptime"][monitor_id] = old_cache
        return success

    async def delete_uptime_monitor(self, monitor_id: uuid.UUID) -> bool:
        old_cache = None
        old_checks = None
        old_minutes = None
        old_incidents = {}
        old_events = {}
        if self.cache_enabled:
            async with self._cache_lock:
                old_cache = self._cache["uptime"].pop(monitor_id, None)
                old_checks = self._cache["uptime_checks"].pop(monitor_id, None)
                old_minutes = self._cache["monitor_minutes"].pop(monitor_id, None)
                for incident_id, incident in list(self._cache["incidents"].items()):
                    if str(incident.get("monitor_id")) == str(monitor_id):
                        old_incidents[incident_id] = incident
                        self._cache["incidents"].pop(incident_id, None)
                for event_key in (("website", monitor_id), ("uptime", monitor_id)):
                    if event_key in self._cache["maintenance_events"]:
                        old_events[event_key] = self._cache["maintenance_events"].pop(event_key)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM monitor_minutes WHERE monitor_id = $1", monitor_id)
                await conn.execute("DELETE FROM incidents WHERE monitor_id = $1", monitor_id)
                await conn.execute(
                    """DELETE FROM maintenance_events
                       WHERE monitor_id = $1 AND monitor_type = ANY($2::text[])""",
                    monitor_id,
                    ["website", "uptime"],
                )
                result = await conn.execute("DELETE FROM uptime_monitors WHERE id = $1", monitor_id)
                success = result == "DELETE 1"

        if not success and self.cache_enabled:
            async with self._cache_lock:
                if old_cache is not None:
                    self._cache["uptime"][monitor_id] = old_cache
                if old_checks is not None:
                    self._cache["uptime_checks"][monitor_id] = old_checks
                if old_minutes is not None:
                    self._cache["monitor_minutes"][monitor_id] = old_minutes
                for incident_id, incident in old_incidents.items():
                    self._cache["incidents"][incident_id] = incident
                for event_key, event_rows in old_events.items():
                    self._cache["maintenance_events"][event_key] = event_rows
        return success

    async def create_uptime_check(
        self,
        monitor_id: uuid.UUID,
        status: str,
        response_time_ms: int | None = None,
        status_code: int | None = None,
        error_message: str | None = None
    ) -> uuid.UUID:
        now = utcnow()
        check_id = uuid.uuid4()
        old_cache = None
        old_checks = None
        if self.cache_enabled:
            async with self._cache_lock:
                cached = self._cache["uptime"].get(monitor_id)
                if cached:
                    old_cache = dict(cached)
                    cached["last_check_at"] = now
                    if str(status).lower() == "up":
                        cached["last_up_at"] = now
                    normalized_status = self._normalize_uptime_status(status)
                    prev_status = cached.get("status")
                    if prev_status != normalized_status:
                        cached["status"] = normalized_status
                        cached["status_since"] = now
                old_checks = list(self._cache["uptime_checks"].get(monitor_id, []))
                self._cache["uptime_checks"].setdefault(monitor_id, []).append({
                    "id": check_id,
                    "monitor_id": monitor_id,
                    "status": status,
                    "response_time_ms": response_time_ms,
                    "status_code": status_code,
                    "error_message": error_message,
                    "checked_at": now
                })

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO uptime_checks
                       (id, monitor_id, status, response_time_ms, status_code, error_message, checked_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7)""",
                    check_id, monitor_id, status, response_time_ms, status_code, error_message, now
                )
                return check_id
        except Exception:
            if self.cache_enabled:
                async with self._cache_lock:
                    if old_cache is not None:
                        self._cache["uptime"][monitor_id] = old_cache
                    if old_checks is not None:
                        self._cache["uptime_checks"][monitor_id] = old_checks
            raise

    async def get_uptime_checks(self, monitor_id: uuid.UUID, limit: int = 100) -> list[dict]:
        if self.cache_enabled:
            async with self._cache_lock:
                checks = self._cache["uptime_checks"].get(monitor_id, [])
                if limit:
                    slice_checks = checks[-limit:]
                else:
                    slice_checks = checks
                items = list(reversed(slice_checks))
            return [dict(row) for row in items]
        if self.cache_only:
            return []
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT * FROM uptime_checks
                   WHERE monitor_id = $1
                   ORDER BY checked_at DESC
                   LIMIT $2""",
                monitor_id, limit
            )
            return [dict(row) for row in rows]

    async def get_uptime_stats(self, monitor_id: uuid.UUID, days: int = 90) -> dict:
        if self.cache_enabled:
            now = utcnow()
            cutoff = now - timedelta(days=days)
            async with self._cache_lock:
                checks = list(self._cache["uptime_checks"].get(monitor_id, []))
            total = 0
            up = 0
            down = 0
            rt_sum = 0
            rt_count = 0
            for row in checks:
                checked_at = row.get("checked_at")
                if not checked_at or checked_at <= cutoff:
                    continue
                total += 1
                status = str(row.get("status") or "").lower()
                if status == "up":
                    up += 1
                    rt = row.get("response_time_ms")
                    if rt is not None:
                        rt_sum += rt
                        rt_count += 1
                else:
                    down += 1
            avg_rt = (rt_sum / rt_count) if rt_count else None
            return {
                "up_count": up,
                "down_count": down,
                "total_count": total,
                "avg_response_time": avg_rt
            }
        if self.cache_only:
            return {}
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT
                    COUNT(*) FILTER (WHERE status = 'up') as up_count,
                    COUNT(*) FILTER (WHERE status != 'up') as down_count,
                    COUNT(*) as total_count,
                    AVG(response_time_ms) FILTER (WHERE status = 'up') as avg_response_time
                   FROM uptime_checks
                   WHERE monitor_id = $1
                   AND checked_at > CURRENT_TIMESTAMP - INTERVAL '1 day' * $2""",
                monitor_id, days
            )
            return dict(row) if row else {}

    async def get_uptime_stats_excluding_maintenance(
        self,
        monitor_id: uuid.UUID,
        start: datetime,
        end: datetime,
        grace_minutes: float = 0.0,
        check_interval_minutes: float = 1.0
    ) -> dict:
        """
        Get uptime check aggregates for a range while excluding checks inside
        maintenance windows.

        When grace_minutes > 0, consecutive "down" checks whose total duration
        (count * check_interval_minutes) is within the grace period are
        reclassified as "up" so that brief network blips don't penalise the
        overall uptime percentage.
        """
        if start >= end:
            return {"up_count": 0, "down_count": 0, "total_count": 0, "avg_response_time": None}

        if self.cache_enabled:
            async with self._cache_lock:
                checks = list(self._cache["uptime_checks"].get(monitor_id, []))
                maintenance = list(self._cache["maintenance_events"].get(("website", monitor_id), []))

            ranges = []
            for ev in maintenance:
                ev_start = ev.get("start_at")
                ev_end = ev.get("end_at") or utcnow()
                if not ev_start:
                    continue
                overlap_start = max(start, ev_start)
                overlap_end = min(end, ev_end)
                if overlap_end <= overlap_start:
                    continue
                ranges.append((overlap_start, overlap_end))
            ranges.sort(key=lambda x: x[0])
            merged = []
            for s, e in ranges:
                if not merged:
                    merged.append((s, e))
                    continue
                prev_s, prev_e = merged[-1]
                if s <= prev_e:
                    merged[-1] = (prev_s, max(prev_e, e))
                else:
                    merged.append((s, e))

            def in_maintenance(ts: datetime) -> bool:
                return any(s <= ts < e for s, e in merged)

            filtered: list[dict] = []
            for row in checks:
                ts = row.get("checked_at")
                if not ts or ts < start or ts >= end:
                    continue
                if in_maintenance(ts):
                    continue
                filtered.append(row)

            # Apply grace: for every down streak, forgive the first
            # `grace_minutes` worth of checks so they don't count as
            # downtime.  Short outages (≤ grace) are forgiven entirely;
            # longer outages only start counting after the grace window.
            if grace_minutes > 0 and check_interval_minutes > 0:
                grace_checks = int(grace_minutes / check_interval_minutes)
                forgiven: set[int] = set()
                i = 0
                n = len(filtered)
                while i < n:
                    status = str(filtered[i].get("status") or "").lower()
                    if status != "up":
                        # Start of a down streak
                        streak_start = i
                        while i < n and str(filtered[i].get("status") or "").lower() != "up":
                            i += 1
                        # Forgive the first `grace_checks` of every streak
                        forgive_end = min(streak_start + grace_checks, i)
                        for j in range(streak_start, forgive_end):
                            forgiven.add(j)
                    else:
                        i += 1

            up_count = 0
            down_count = 0
            total_count = 0
            rt_sum = 0.0
            rt_count = 0
            for idx, row in enumerate(filtered):
                status = str(row.get("status") or "").lower()
                total_count += 1
                is_forgiven = grace_minutes > 0 and idx in forgiven
                if status == "up" or is_forgiven:
                    up_count += 1
                    rt = row.get("response_time_ms")
                    if rt is not None:
                        try:
                            rt_sum += float(rt)
                            rt_count += 1
                        except Exception:
                            logger.debug("Failed to parse response_time_ms value: %s", rt, exc_info=True)
                elif status != "up":
                    down_count += 1

            return {
                "up_count": up_count,
                "down_count": down_count,
                "total_count": total_count,
                "avg_response_time": (rt_sum / rt_count) if rt_count else None
            }

        if self.cache_only:
            return {"up_count": 0, "down_count": 0, "total_count": 0, "avg_response_time": None}

        # SQL fallback path – grace filtering is applied in Python when needed
        async with self.pool.acquire() as conn:
            if grace_minutes > 0:
                rows = await conn.fetch(
                    """
                    SELECT uc.status, uc.response_time_ms
                    FROM uptime_checks uc
                    WHERE uc.monitor_id = $1
                      AND uc.checked_at >= $2
                      AND uc.checked_at < $3
                      AND NOT EXISTS (
                          SELECT 1
                          FROM maintenance_events me
                          WHERE me.monitor_type = 'website'
                            AND me.monitor_id = $1
                            AND me.start_at <= uc.checked_at
                            AND COALESCE(me.end_at, CURRENT_TIMESTAMP) > uc.checked_at
                      )
                    ORDER BY uc.checked_at
                    """,
                    monitor_id, start, end
                )
                db_filtered = [dict(r) for r in rows] if rows else []
                grace_checks = int(grace_minutes / check_interval_minutes)
                forgiven_db: set[int] = set()
                i = 0
                n = len(db_filtered)
                while i < n:
                    st = str(db_filtered[i].get("status") or "").lower()
                    if st != "up":
                        streak_start = i
                        while i < n and str(db_filtered[i].get("status") or "").lower() != "up":
                            i += 1
                        forgive_end = min(streak_start + grace_checks, i)
                        for j in range(streak_start, forgive_end):
                            forgiven_db.add(j)
                    else:
                        i += 1
                up_c = down_c = total_c = 0
                rt_s = 0.0
                rt_n = 0
                for idx, r in enumerate(db_filtered):
                    st = str(r.get("status") or "").lower()
                    total_c += 1
                    if st == "up" or idx in forgiven_db:
                        up_c += 1
                        rt = r.get("response_time_ms")
                        if rt is not None:
                            try:
                                rt_s += float(rt)
                                rt_n += 1
                            except Exception:
                                logger.debug("Failed to parse response_time_ms value: %s", rt, exc_info=True)
                    elif st != "up":
                        down_c += 1
                return {
                    "up_count": up_c, "down_count": down_c,
                    "total_count": total_c,
                    "avg_response_time": (rt_s / rt_n) if rt_n else None
                }

            row = await conn.fetchrow(
                """
                SELECT
                    COUNT(*) FILTER (WHERE uc.status = 'up') as up_count,
                    COUNT(*) FILTER (WHERE uc.status != 'up') as down_count,
                    COUNT(*) as total_count,
                    AVG(uc.response_time_ms) FILTER (WHERE uc.status = 'up') as avg_response_time
                FROM uptime_checks uc
                WHERE uc.monitor_id = $1
                  AND uc.checked_at >= $2
                  AND uc.checked_at < $3
                  AND NOT EXISTS (
                      SELECT 1
                      FROM maintenance_events me
                      WHERE me.monitor_type = 'website'
                        AND me.monitor_id = $1
                        AND me.start_at <= uc.checked_at
                        AND COALESCE(me.end_at, CURRENT_TIMESTAMP) > uc.checked_at
                  )
                """,
                monitor_id, start, end
            )
            return dict(row) if row else {"up_count": 0, "down_count": 0, "total_count": 0, "avg_response_time": None}

    async def get_uptime_multi_period_stats(self, monitor_id: uuid.UUID) -> dict:
        """Get all uptime stats (24h, 7d, 30d, year, total) in a single query."""
        if self.cache_enabled:
            now = utcnow()
            start_24h = now - timedelta(hours=24)
            start_7d = now - timedelta(days=7)
            start_30d = now - timedelta(days=30)
            start_year = datetime(now.year, 1, 1)
            async with self._cache_lock:
                checks = list(self._cache["uptime_checks"].get(monitor_id, []))
            total_24h = up_24h = 0
            total_7d = up_7d = 0
            total_30d = up_30d = 0
            total_year = up_year = 0
            total_all = 0
            up_all = 0
            first_check = None
            rt_sum_all = 0
            rt_count_all = 0
            for row in checks:
                checked_at = row.get("checked_at")
                if not checked_at:
                    continue
                status = str(row.get("status") or "").lower()
                total_all += 1
                if first_check is None or checked_at < first_check:
                    first_check = checked_at
                if status == "up":
                    up_all += 1
                    rt = row.get("response_time_ms")
                    if rt is not None:
                        try:
                            rt_sum_all += float(rt)
                            rt_count_all += 1
                        except Exception:
                            logger.debug("Failed to parse response_time_ms value: %s", rt, exc_info=True)
                if checked_at >= start_24h:
                    total_24h += 1
                    if status == "up":
                        up_24h += 1
                if checked_at >= start_7d:
                    total_7d += 1
                    if status == "up":
                        up_7d += 1
                if checked_at >= start_30d:
                    total_30d += 1
                    if status == "up":
                        up_30d += 1
                if checked_at >= start_year:
                    total_year += 1
                    if status == "up":
                        up_year += 1
            return {
                "total_24h": total_24h,
                "up_24h": up_24h,
                "total_7d": total_7d,
                "up_7d": up_7d,
                "total_30d": total_30d,
                "up_30d": up_30d,
                "total_year": total_year,
                "up_year": up_year,
                "total_all": total_all,
                "up_all": up_all,
                "first_check": first_check,
                "avg_response_time_all": (rt_sum_all / rt_count_all) if rt_count_all else None
            }
        if self.cache_only:
            return {}
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT
                    COUNT(*) FILTER (
                        WHERE checked_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
                    ) as total_24h,
                    COUNT(*) FILTER (
                        WHERE checked_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
                        AND status = 'up'
                    ) as up_24h,
                    COUNT(*) FILTER (
                        WHERE checked_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
                    ) as total_7d,
                    COUNT(*) FILTER (
                        WHERE checked_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
                        AND status = 'up'
                    ) as up_7d,
                    COUNT(*) FILTER (
                        WHERE checked_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
                    ) as total_30d,
                    COUNT(*) FILTER (
                        WHERE checked_at >= CURRENT_TIMESTAMP - INTERVAL '30 days'
                        AND status = 'up'
                    ) as up_30d,
                    COUNT(*) FILTER (
                        WHERE checked_at >= date_trunc('year', CURRENT_TIMESTAMP)
                    ) as total_year,
                    COUNT(*) FILTER (
                        WHERE checked_at >= date_trunc('year', CURRENT_TIMESTAMP)
                        AND status = 'up'
                    ) as up_year,
                    COUNT(*) as total_all,
                    COUNT(*) FILTER (WHERE status = 'up') as up_all,
                    MIN(checked_at) as first_check,
                    AVG(response_time_ms) FILTER (WHERE status = 'up') as avg_response_time_all
                   FROM uptime_checks
                   WHERE monitor_id = $1""",
                monitor_id
            )
            return dict(row) if row else {}

    async def get_server_monitors(self, enabled_only: bool = False, public_only: bool = False) -> list[dict]:
        if self.cache_enabled:
            async with self._cache_lock:
                self._apply_due_maintenance_cache("server")
                items = list(self._cache["server"].values())
            if enabled_only:
                items = [m for m in items if m.get("enabled")]
            if public_only:
                items = [m for m in items if m.get("is_public")]
            return [dict(m) for m in items]
        if self.cache_only:
            return []

        async with self.pool.acquire() as conn:
            await self._apply_due_maintenance_windows(conn)
            base_query = """SELECT id, sid, name, os, kernel, hostname, cpu_model,
                              cpu_cores, cpu_threads, ram_size,
                              'server_agent'::varchar as heartbeat_type,
                              enabled, notifications_enabled, is_public,
                              maintenance_mode, maintenance_start_at, maintenance_end_at,
                              status, last_checkin_at, down_since, status_since,
                              category,
                              created_at, last_report_at
                       FROM server_monitors"""
            conditions = []
            if enabled_only:
                conditions.append("enabled = true")
            if public_only:
                conditions.append("is_public = true")
            
            if conditions:
                base_query += " WHERE " + " AND ".join(conditions)
            base_query += " ORDER BY name"
            
            rows = await conn.fetch(base_query)
            return [dict(row) for row in rows]

    async def get_server_monitor_by_id(self, server_id: uuid.UUID) -> dict | None:
        if self.cache_enabled:
            async with self._cache_lock:
                self._apply_due_maintenance_cache("server")
                cached = self._cache["server"].get(server_id)
                return dict(cached) if cached else None
        if self.cache_only:
            return None

        async with self.pool.acquire() as conn:
            await self._apply_due_maintenance_windows(conn)
            row = await conn.fetchrow(
                """SELECT id, sid, name, os, kernel, hostname, cpu_model,
                          cpu_sockets, cpu_cores, cpu_threads, ram_size, ram_swap_size,
                          'server_agent'::varchar as heartbeat_type,
                          enabled, notifications_enabled, is_public,
                          maintenance_mode, maintenance_start_at, maintenance_end_at,
                          status, last_checkin_at, down_since, status_since,
                          category,
                          created_at, last_report_at
                   FROM server_monitors WHERE id = $1""",
                server_id
            )
            if not row:
                return None
            data = dict(row)
            if self.cache_enabled:
                async with self._cache_lock:
                    self._cache["server"][server_id] = data
                    sid = data.get("sid")
                    if sid:
                        self._cache_sid["server"][sid] = server_id

        data["status"] = data.get("status", "unknown")
        return data

    async def get_server_monitor_by_sid(self, sid: str) -> dict | None:
        if self.cache_enabled:
            if not sid:
                return None
            async with self._cache_lock:
                server_id = self._cache_sid["server"].get(sid)
                if server_id:
                    cached = self._cache["server"].get(server_id)
                    return dict(cached) if cached else None
            return None
        if self.cache_only:
            return None

        async with self.pool.acquire() as conn:
            await self._apply_due_maintenance_windows(conn)
            row = await conn.fetchrow(
                """SELECT id, sid, name, os, kernel, hostname, cpu_model,
                          cpu_sockets, cpu_cores, cpu_threads, ram_size, ram_swap_size,
                          'server_agent'::varchar as heartbeat_type,
                          enabled, notifications_enabled, is_public,
                          maintenance_mode, maintenance_start_at, maintenance_end_at,
                          status, last_checkin_at, down_since, status_since,
                          category,
                          created_at, last_report_at
                   FROM server_monitors WHERE sid = $1""",
                sid
            )
            if not row:
                return None
            data = dict(row)
            if self.cache_enabled:
                async with self._cache_lock:
                    self._cache["server"][data["id"]] = data
                    self._cache_sid["server"][sid] = data["id"]

        return data

    async def create_server_monitor(
        self,
        sid: str,
        name: str,
        category: str | None = None
    ) -> uuid.UUID:
        server_id = uuid.uuid4()
        now = utcnow()
        cache_entry = {
            "id": server_id,
            "sid": sid,
            "name": name,
            "os": None,
            "kernel": None,
            "hostname": None,
            "cpu_model": None,
            "cpu_sockets": None,
            "cpu_cores": None,
            "cpu_threads": None,
            "ram_size": None,
            "ram_swap_size": None,
            "heartbeat_type": "server_agent",
            "enabled": True,
            "notifications_enabled": True,
            "is_public": False,
            "maintenance_mode": False,
            "maintenance_start_at": None,
            "maintenance_end_at": None,
            "status": "unknown",
            "last_checkin_at": None,
            "down_since": None,
            "status_since": None,
            "category": category,
            "created_at": now,
            "last_report_at": None,
        }

        if self.cache_enabled:
            async with self._cache_lock:
                self._cache["server"][server_id] = cache_entry
                if sid:
                    self._cache_sid["server"][sid] = server_id

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO server_monitors (id, sid, name, category, created_at)
                       VALUES ($1, $2, $3, $4, $5)""",
                    server_id, sid, name, category, now
                )
        except Exception:
            if self.cache_enabled:
                async with self._cache_lock:
                    self._cache["server"].pop(server_id, None)
                    if sid:
                        self._cache_sid["server"].pop(sid, None)
            raise
        return server_id

    async def update_server_monitor(self, server_id: uuid.UUID, **kwargs) -> bool:
        _bad_keys = set(kwargs) - _SERVER_UPDATABLE_FIELDS
        if _bad_keys:
            raise ValueError(f"Invalid server monitor fields: {_bad_keys}")
        old_cache = None
        old_sid = None
        if self.cache_enabled:
            async with self._cache_lock:
                cached = self._cache["server"].get(server_id)
                if cached:
                    old_cache = dict(cached)
                    old_sid = cached.get("sid")
                    for key, value in kwargs.items():
                        if value is not None:
                            cached[key] = value
                    new_sid = kwargs.get("sid")
                    if new_sid and new_sid != old_sid:
                        if old_sid:
                            self._cache_sid["server"].pop(old_sid, None)
                        self._cache_sid["server"][new_sid] = server_id

        async with self.pool.acquire() as conn:
            fields = []
            values = []
            idx = 1
            for key, value in kwargs.items():
                if value is not None:
                    fields.append(f"{key} = ${idx}")
                    values.append(value)
                    idx += 1
            if not fields:
                return False
            values.append(server_id)
            query = f"UPDATE server_monitors SET {', '.join(fields)} WHERE id = ${idx}"
            result = await conn.execute(query, *values)
            success = result == "UPDATE 1"

        if not success and old_cache is not None and self.cache_enabled:
            async with self._cache_lock:
                self._cache["server"][server_id] = old_cache
                new_sid = kwargs.get("sid")
                if new_sid and new_sid != old_sid:
                    self._cache_sid["server"].pop(new_sid, None)
                if old_sid:
                    self._cache_sid["server"][old_sid] = server_id
        return success

    async def delete_server_monitor(self, server_id: uuid.UUID) -> bool:
        old_cache = None
        old_sid = None
        old_history = None
        old_minutes = None
        old_incidents = {}
        old_events = {}
        if self.cache_enabled:
            async with self._cache_lock:
                old_cache = self._cache["server"].pop(server_id, None)
                if old_cache:
                    old_sid = old_cache.get("sid")
                    if old_sid:
                        self._cache_sid["server"].pop(old_sid, None)
                old_history = self._cache["server_history"].pop(server_id, None)
                old_minutes = self._cache["monitor_minutes"].pop(server_id, None)
                for incident_id, incident in list(self._cache["incidents"].items()):
                    if str(incident.get("monitor_id")) == str(server_id):
                        old_incidents[incident_id] = incident
                        self._cache["incidents"].pop(incident_id, None)
                for event_key in (
                    ("server_agent", server_id),
                    ("server", server_id),
                    ("agent", server_id),
                    ("server-agent", server_id),
                    ("heartbeat-server-agent", server_id),
                ):
                    if event_key in self._cache["maintenance_events"]:
                        old_events[event_key] = self._cache["maintenance_events"].pop(event_key)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM monitor_minutes WHERE monitor_id = $1", server_id)
                await conn.execute("DELETE FROM incidents WHERE monitor_id = $1", server_id)
                await conn.execute(
                    """DELETE FROM maintenance_events
                       WHERE monitor_id = $1 AND monitor_type = ANY($2::text[])""",
                    server_id,
                    ["server_agent", "server", "agent", "server-agent", "heartbeat-server-agent"],
                )
                result = await conn.execute("DELETE FROM server_monitors WHERE id = $1", server_id)
                success = result == "DELETE 1"

        if not success and self.cache_enabled:
            async with self._cache_lock:
                if old_cache is not None:
                    self._cache["server"][server_id] = old_cache
                if old_sid and old_cache is not None:
                    self._cache_sid["server"][old_sid] = server_id
                if old_history is not None:
                    self._cache["server_history"][server_id] = old_history
                if old_minutes is not None:
                    self._cache["monitor_minutes"][server_id] = old_minutes
                for incident_id, incident in old_incidents.items():
                    self._cache["incidents"][incident_id] = incident
                for event_key, event_rows in old_events.items():
                    self._cache["maintenance_events"][event_key] = event_rows
        return success

    async def create_server_history(
        self,
        server_id: uuid.UUID,
        cpu_percent: float,
        ram_percent: float,
        load_1: float = None,
        load_5: float = None,
        load_15: float = None,
        disks: str = None,
        nics: str = None,
        temperature: str = None,
        cpu_io_wait: float = None,
        cpu_steal: float = None,
        cpu_user: float = None,
        cpu_system: float = None,
        ram_swap_percent: float = None,
        ram_buff_percent: float = None,
        ram_cache_percent: float = None,
        network_in: int = None,
        network_out: int = None,
        disk_percent: float = None
    ) -> uuid.UUID:
        history_id = uuid.uuid4()
        now = utcnow()
        old_history = None
        if self.cache_enabled:
            async with self._cache_lock:
                old_history = list(self._cache["server_history"].get(server_id, []))
                self._cache["server_history"].setdefault(server_id, []).append({
                    "id": history_id,
                    "server_id": server_id,
                    "timestamp": now,
                    "cpu_percent": cpu_percent,
                    "ram_percent": ram_percent,
                    "load_1": load_1,
                    "load_5": load_5,
                    "load_15": load_15,
                    "disks": disks,
                    "nics": nics,
                    "temperature": temperature,
                    "cpu_io_wait": cpu_io_wait,
                    "cpu_steal": cpu_steal,
                    "cpu_user": cpu_user,
                    "cpu_system": cpu_system,
                    "ram_swap_percent": ram_swap_percent,
                    "ram_buff_percent": ram_buff_percent,
                    "ram_cache_percent": ram_cache_percent,
                    "network_in": network_in,
                    "network_out": network_out,
                    "disk_percent": disk_percent
                })

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO server_history
                       (id, server_id, timestamp, cpu_percent, ram_percent, load_1, load_5, load_15, disks, nics, temperature,
                        cpu_io_wait, cpu_steal, cpu_user, cpu_system,
                        ram_swap_percent, ram_buff_percent, ram_cache_percent,
                        network_in, network_out, disk_percent)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                               $12, $13, $14, $15,
                               $16, $17, $18,
                               $19, $20, $21)""",
                    history_id, server_id, now, cpu_percent, ram_percent, load_1, load_5, load_15, disks, nics, temperature,
                    cpu_io_wait, cpu_steal, cpu_user, cpu_system,
                    ram_swap_percent, ram_buff_percent, ram_cache_percent,
                    network_in, network_out, disk_percent
                )
                return history_id
        except Exception:
            if old_history is not None and self.cache_enabled:
                async with self._cache_lock:
                    self._cache["server_history"][server_id] = old_history
            raise

    async def get_server_history(self, server_id: uuid.UUID, hours: int = 24, limit: int = None) -> list[dict]:
        if self.cache_enabled:
            now = utcnow()
            async with self._cache_lock:
                history = self._cache["server_history"].get(server_id, [])
                if limit:
                    slice_history = history[-limit:]
                    items = list(reversed(slice_history))
                else:
                    cutoff = now - timedelta(hours=hours)
                    items = [
                        row for row in history
                        if row.get("timestamp") is not None and row["timestamp"] > cutoff
                    ]
            if not limit:
                items.sort(key=lambda r: r.get("timestamp") or datetime.min)
            return [dict(row) for row in items]
        if self.cache_only:
            return []
        async with self.pool.acquire() as conn:
            if limit:
                rows = await conn.fetch(
                    """SELECT * FROM server_history
                       WHERE server_id = $1
                       ORDER BY timestamp DESC
                       LIMIT $2""",
                    server_id, limit
                )
            else:
                rows = await conn.fetch(
                    """SELECT * FROM server_history
                       WHERE server_id = $1
                       AND timestamp > CURRENT_TIMESTAMP - INTERVAL '1 hour' * $2
                       ORDER BY timestamp ASC""",
                    server_id, hours
                )
            return [dict(row) for row in rows]

    async def get_server_history_range(self, server_id: uuid.UUID, start: datetime, end: datetime) -> list[dict]:
        if self.cache_enabled:
            async with self._cache_lock:
                history = list(self._cache["server_history"].get(server_id, []))
            items = [
                dict(row) for row in history
                if row.get("timestamp") is not None
                and row["timestamp"] >= start
                and row["timestamp"] < end
            ]
            items.sort(key=lambda r: r.get("timestamp") or datetime.min)
            return items
        if self.cache_only:
            return []
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT * FROM server_history
                   WHERE server_id = $1
                   AND timestamp >= $2 AND timestamp < $3
                   ORDER BY timestamp ASC""",
                server_id, start, end
            )
            return [dict(row) for row in rows]

    async def get_server_history_aggregated(self, server_id: uuid.UUID, hours: int = 24, interval: str = 'hour') -> list[dict]:
        """
        Get aggregated server history for longer time ranges.
        Interval can be '15min', 'hour', or 'day'
        """
        if self.cache_enabled:
            now = utcnow()
            cutoff = now - timedelta(hours=hours)
            async with self._cache_lock:
                history = list(self._cache["server_history"].get(server_id, []))

            def bucket(ts: datetime) -> datetime:
                if interval == '15min':
                    return datetime(ts.year, ts.month, ts.day, ts.hour, (ts.minute // 15) * 15)
                if interval == 'hour':
                    return datetime(ts.year, ts.month, ts.day, ts.hour)
                return datetime(ts.year, ts.month, ts.day)

            buckets = {}
            metrics = [
                "cpu_percent", "cpu_io_wait", "cpu_steal", "cpu_user", "cpu_system",
                "ram_percent", "ram_swap_percent", "ram_buff_percent", "ram_cache_percent",
                "load_1", "load_5", "load_15",
                "network_in", "network_out", "disk_percent"
            ]
            for row in history:
                ts = row.get("timestamp")
                if not ts or ts <= cutoff:
                    continue
                key = bucket(ts)
                agg = buckets.setdefault(key, {m: {"sum": 0.0, "count": 0} for m in metrics})
                for m in metrics:
                    val = row.get(m)
                    if val is None:
                        continue
                    agg[m]["sum"] += float(val)
                    agg[m]["count"] += 1

            result = []
            for ts, agg in buckets.items():
                row = {"timestamp": ts}
                for m in metrics:
                    if agg[m]["count"]:
                        row[m] = agg[m]["sum"] / agg[m]["count"]
                    else:
                        row[m] = None
                result.append(row)
            result.sort(key=lambda r: r.get("timestamp") or datetime.min)
            return result
        if self.cache_only:
            return []
        async with self.pool.acquire() as conn:
            if interval == '15min':
                bucket_expr = "date_trunc('hour', timestamp) + (floor(date_part('minute', timestamp) / 15)::int * interval '15 minutes')"
            elif interval == 'hour':
                bucket_expr = "date_trunc('hour', timestamp)"
            else:
                bucket_expr = "date_trunc('day', timestamp)"

            rows = await conn.fetch(
                f"""SELECT
                    {bucket_expr} as timestamp,
                    AVG(cpu_percent) as cpu_percent,
                    AVG(cpu_io_wait) as cpu_io_wait,
                    AVG(cpu_steal) as cpu_steal,
                    AVG(cpu_user) as cpu_user,
                    AVG(cpu_system) as cpu_system,
                    AVG(ram_percent) as ram_percent,
                    AVG(ram_swap_percent) as ram_swap_percent,
                    AVG(ram_buff_percent) as ram_buff_percent,
                    AVG(ram_cache_percent) as ram_cache_percent,
                    AVG(load_1) as load_1,
                    AVG(load_5) as load_5,
                    AVG(load_15) as load_15,
                    AVG(network_in) as network_in,
                    AVG(network_out) as network_out,
                    AVG(disk_percent) as disk_percent
                   FROM server_history
                   WHERE server_id = $1
                   AND timestamp > CURRENT_TIMESTAMP - INTERVAL '1 hour' * $2
                   GROUP BY 1
                   ORDER BY timestamp ASC""",
                server_id, hours
            )
            return [dict(row) for row in rows]

    async def get_server_history_multi_period_stats(self, server_id: uuid.UUID) -> dict:
        """Get all server history stats (24h, 7d, 30d, year, total) in a single query."""
        if self.cache_enabled:
            now = utcnow()
            start_24h = now - timedelta(hours=24)
            start_7d = now - timedelta(days=7)
            start_30d = now - timedelta(days=30)
            start_year = datetime(now.year, 1, 1)
            async with self._cache_lock:
                history = list(self._cache["server_history"].get(server_id, []))
            days_24h = set()
            days_7d = set()
            days_30d = set()
            days_year = set()
            days_total = set()
            first_report = None
            last_report = None
            for row in history:
                ts = row.get("timestamp")
                if not ts:
                    continue
                day = ts.date()
                days_total.add(day)
                if ts >= start_24h:
                    days_24h.add(day)
                if ts >= start_7d:
                    days_7d.add(day)
                if ts >= start_30d:
                    days_30d.add(day)
                if ts >= start_year:
                    days_year.add(day)
                if first_report is None or ts < first_report:
                    first_report = ts
                if last_report is None or ts > last_report:
                    last_report = ts
            return {
                "days_with_reports_24h": len(days_24h),
                "days_with_reports_7d": len(days_7d),
                "days_with_reports_30d": len(days_30d),
                "days_with_reports_year": len(days_year),
                "days_with_reports_total": len(days_total),
                "first_report": first_report,
                "last_report": last_report
            }
        if self.cache_only:
            return {}
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT
                    COUNT(DISTINCT date_trunc('day', timestamp)) FILTER (
                        WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
                    ) as days_with_reports_24h,
                    COUNT(DISTINCT date_trunc('day', timestamp)) FILTER (
                        WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
                    ) as days_with_reports_7d,
                    COUNT(DISTINCT date_trunc('day', timestamp)) FILTER (
                        WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
                    ) as days_with_reports_30d,
                    COUNT(DISTINCT date_trunc('day', timestamp)) FILTER (
                        WHERE timestamp >= date_trunc('year', CURRENT_TIMESTAMP)
                    ) as days_with_reports_year,
                    COUNT(DISTINCT date_trunc('day', timestamp)) as days_with_reports_total,
                    MIN(timestamp) as first_report,
                    MAX(timestamp) as last_report
                   FROM server_history
                   WHERE server_id = $1""",
                server_id
            )
            return dict(row) if row else {}

    async def get_heartbeat_monitors(self, enabled_only: bool = False, public_only: bool = False) -> list[dict]:
        if self.cache_enabled:
            async with self._cache_lock:
                self._apply_due_maintenance_cache("heartbeat")
                items = list(self._cache["heartbeat"].values())
            if enabled_only:
                items = [m for m in items if m.get("enabled")]
            if public_only:
                items = [m for m in items if m.get("is_public")]
            return [dict(m) for m in items]
        if self.cache_only:
            return []

        async with self.pool.acquire() as conn:
            await self._apply_due_maintenance_windows(conn)
            base_query = """SELECT id, sid, name, heartbeat_type, timeout, grace_period,
                              enabled, notifications_enabled, is_public,
                              maintenance_mode, maintenance_start_at, maintenance_end_at,
                              status, last_checkin_at, down_since, status_since,
                              category,
                              created_at, last_ping_at
                       FROM heartbeat_monitors"""
            conditions = []
            if enabled_only:
                conditions.append("enabled = true")
            if public_only:
                conditions.append("is_public = true")
            
            if conditions:
                base_query += " WHERE " + " AND ".join(conditions)
            base_query += " ORDER BY name"
            
            rows = await conn.fetch(base_query)
            return [dict(row) for row in rows]

    async def get_heartbeat_monitor_by_id(self, monitor_id: uuid.UUID) -> dict | None:
        if self.cache_enabled:
            async with self._cache_lock:
                self._apply_due_maintenance_cache("heartbeat")
                cached = self._cache["heartbeat"].get(monitor_id)
                return dict(cached) if cached else None
        if self.cache_only:
            return None

        async with self.pool.acquire() as conn:
            await self._apply_due_maintenance_windows(conn)
            row = await conn.fetchrow(
                """SELECT id, sid, name, heartbeat_type, timeout, grace_period, category,
                          enabled, notifications_enabled, is_public,
                          maintenance_mode, maintenance_start_at, maintenance_end_at,
                          status, last_checkin_at, down_since, status_since,
                          private_notes, created_at, last_ping_at
                   FROM heartbeat_monitors WHERE id = $1""",
                monitor_id
            )
            if not row:
                return None
            data = dict(row)
            if self.cache_enabled:
                async with self._cache_lock:
                    self._cache["heartbeat"][monitor_id] = data
                    sid = data.get("sid")
                    if sid:
                        self._cache_sid["heartbeat"][sid] = monitor_id

        data["status"] = data.get("status", "unknown")
        return data

    async def get_heartbeat_monitor_by_sid(self, sid: str) -> dict | None:
        if self.cache_enabled:
            if not sid:
                return None
            async with self._cache_lock:
                monitor_id = self._cache_sid["heartbeat"].get(sid)
                if monitor_id:
                    cached = self._cache["heartbeat"].get(monitor_id)
                    return dict(cached) if cached else None
            return None
        if self.cache_only:
            return None

        async with self.pool.acquire() as conn:
            await self._apply_due_maintenance_windows(conn)
            row = await conn.fetchrow(
                """SELECT id, sid, name, heartbeat_type, timeout, grace_period, category,
                          enabled, notifications_enabled, is_public,
                          maintenance_mode, maintenance_start_at, maintenance_end_at,
                          status, last_checkin_at, down_since, status_since,
                          private_notes, created_at, last_ping_at
                   FROM heartbeat_monitors WHERE sid = $1""",
                sid
            )
            if not row:
                return None
            data = dict(row)
            if self.cache_enabled:
                async with self._cache_lock:
                    self._cache["heartbeat"][data["id"]] = data
                    self._cache_sid["heartbeat"][sid] = data["id"]

        return data

    async def create_heartbeat_monitor(
        self,
        sid: str,
        name: str,
        heartbeat_type: str = "cronjob",
        timeout: int = 60,
        grace_period: int = 5,
        category: str | None = None
    ) -> uuid.UUID:
        monitor_id = uuid.uuid4()
        now = utcnow()
        cache_entry = {
            "id": monitor_id,
            "sid": sid,
            "name": name,
            "heartbeat_type": heartbeat_type,
            "timeout": timeout,
            "grace_period": grace_period,
            "category": category,
            "enabled": True,
            "notifications_enabled": True,
            "is_public": False,
            "maintenance_mode": False,
            "maintenance_start_at": None,
            "maintenance_end_at": None,
            "status": "unknown",
            "last_checkin_at": None,
            "down_since": None,
            "status_since": None,
            "private_notes": None,
            "created_at": now,
            "updated_at": now,
            "last_ping_at": None,
        }

        if self.cache_enabled:
            async with self._cache_lock:
                self._cache["heartbeat"][monitor_id] = cache_entry
                if sid:
                    self._cache_sid["heartbeat"][sid] = monitor_id

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO heartbeat_monitors
                       (id, sid, name, heartbeat_type, timeout, grace_period, category, created_at, updated_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)""",
                    monitor_id, sid, name, heartbeat_type, timeout, grace_period, category, now, now
                )
        except Exception:
            if self.cache_enabled:
                async with self._cache_lock:
                    self._cache["heartbeat"].pop(monitor_id, None)
                    if sid:
                        self._cache_sid["heartbeat"].pop(sid, None)
            raise
        return monitor_id

    async def update_heartbeat_monitor(self, monitor_id: uuid.UUID, **kwargs) -> bool:
        _bad_keys = set(kwargs) - _HEARTBEAT_UPDATABLE_FIELDS - {"updated_at"}
        if _bad_keys:
            raise ValueError(f"Invalid heartbeat monitor fields: {_bad_keys}")
        updated_at = utcnow()
        old_cache = None
        old_sid = None
        if self.cache_enabled:
            async with self._cache_lock:
                cached = self._cache["heartbeat"].get(monitor_id)
                if cached:
                    old_cache = dict(cached)
                    old_sid = cached.get("sid")
                    for key, value in kwargs.items():
                        if value is not None:
                            cached[key] = value
                    cached["updated_at"] = updated_at
                    new_sid = kwargs.get("sid")
                    if new_sid and new_sid != old_sid:
                        if old_sid:
                            self._cache_sid["heartbeat"].pop(old_sid, None)
                        self._cache_sid["heartbeat"][new_sid] = monitor_id

        async with self.pool.acquire() as conn:
            kwargs['updated_at'] = updated_at
            fields = []
            values = []
            idx = 1
            for key, value in kwargs.items():
                if value is not None:
                    fields.append(f"{key} = ${idx}")
                    values.append(value)
                    idx += 1
            if not fields:
                return False
            values.append(monitor_id)
            query = f"UPDATE heartbeat_monitors SET {', '.join(fields)} WHERE id = ${idx}"
            result = await conn.execute(query, *values)
            success = result == "UPDATE 1"

        if not success and old_cache is not None and self.cache_enabled:
            async with self._cache_lock:
                self._cache["heartbeat"][monitor_id] = old_cache
                new_sid = kwargs.get("sid")
                if new_sid and new_sid != old_sid:
                    self._cache_sid["heartbeat"].pop(new_sid, None)
                if old_sid:
                    self._cache_sid["heartbeat"][old_sid] = monitor_id
        return success

    async def delete_heartbeat_monitor(self, monitor_id: uuid.UUID) -> bool:
        old_cache = None
        old_sid = None
        old_pings = None
        old_minutes = None
        old_incidents = {}
        old_events = {}
        if self.cache_enabled:
            async with self._cache_lock:
                old_cache = self._cache["heartbeat"].pop(monitor_id, None)
                if old_cache:
                    old_sid = old_cache.get("sid")
                    if old_sid:
                        self._cache_sid["heartbeat"].pop(old_sid, None)
                old_pings = self._cache["heartbeat_pings"].pop(monitor_id, None)
                old_minutes = self._cache["monitor_minutes"].pop(monitor_id, None)
                for incident_id, incident in list(self._cache["incidents"].items()):
                    if str(incident.get("monitor_id")) == str(monitor_id):
                        old_incidents[incident_id] = incident
                        self._cache["incidents"].pop(incident_id, None)
                for event_key in (
                    ("heartbeat", monitor_id),
                    ("heartbeat-cronjob", monitor_id),
                    ("cronjob", monitor_id),
                ):
                    if event_key in self._cache["maintenance_events"]:
                        old_events[event_key] = self._cache["maintenance_events"].pop(event_key)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM monitor_minutes WHERE monitor_id = $1", monitor_id)
                await conn.execute("DELETE FROM incidents WHERE monitor_id = $1", monitor_id)
                await conn.execute(
                    """DELETE FROM maintenance_events
                       WHERE monitor_id = $1 AND monitor_type = ANY($2::text[])""",
                    monitor_id,
                    ["heartbeat", "heartbeat-cronjob", "cronjob"],
                )
                result = await conn.execute("DELETE FROM heartbeat_monitors WHERE id = $1", monitor_id)
                success = result == "DELETE 1"

        if not success and self.cache_enabled:
            async with self._cache_lock:
                if old_cache is not None:
                    self._cache["heartbeat"][monitor_id] = old_cache
                if old_sid and old_cache is not None:
                    self._cache_sid["heartbeat"][old_sid] = monitor_id
                if old_pings is not None:
                    self._cache["heartbeat_pings"][monitor_id] = old_pings
                if old_minutes is not None:
                    self._cache["monitor_minutes"][monitor_id] = old_minutes
                for incident_id, incident in old_incidents.items():
                    self._cache["incidents"][incident_id] = incident
                for event_key, event_rows in old_events.items():
                    self._cache["maintenance_events"][event_key] = event_rows
        return success

    async def record_heartbeat_ping(self, monitor_id: uuid.UUID, ping_source: str = None) -> uuid.UUID:
        now = utcnow()
        ping_id = uuid.uuid4()
        old_cache = None
        old_pings = None
        if self.cache_enabled:
            async with self._cache_lock:
                cached = self._cache["heartbeat"].get(monitor_id)
                if cached:
                    old_cache = dict(cached)
                old_pings = list(self._cache["heartbeat_pings"].get(monitor_id, []))
                self._cache["heartbeat_pings"].setdefault(monitor_id, []).append({
                    "id": ping_id,
                    "monitor_id": monitor_id,
                    "ping_source": ping_source,
                    "pinged_at": now
                })
                if cached:
                    cached["last_ping_at"] = now

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO heartbeat_pings (id, monitor_id, ping_source, pinged_at)
                       VALUES ($1, $2, $3, $4)""",
                    ping_id, monitor_id, ping_source, now
                )
                await conn.execute(
                    "UPDATE heartbeat_monitors SET last_ping_at = $2 WHERE id = $1",
                    monitor_id, now
                )
                return ping_id
        except Exception:
            if self.cache_enabled:
                async with self._cache_lock:
                    if old_cache is not None:
                        self._cache["heartbeat"][monitor_id] = old_cache
                    if old_pings is not None:
                        self._cache["heartbeat_pings"][monitor_id] = old_pings
            raise

    async def create_incident(
        self,
        monitor_type: str,
        monitor_id: uuid.UUID | None,
        incident_type: str,
        title: str,
        description: str | None = None,
        source: str = "monitor",
        template_key: str | None = None
    ) -> uuid.UUID:
        incident_id = uuid.uuid4()
        now = utcnow()
        incident_source = (source or "monitor").strip().lower()
        if incident_source not in {"monitor", "admin"}:
            incident_source = "monitor"
        incident_monitor_id = monitor_id or uuid.UUID("00000000-0000-0000-0000-000000000000")
        cache_entry = {
            "id": incident_id,
            "monitor_type": monitor_type,
            "monitor_id": incident_monitor_id,
            "incident_type": incident_type,
            "source": incident_source,
            "template_key": template_key,
            "status": "open",
            "title": title,
            "description": description,
            "started_at": now,
            "resolved_at": None,
            "hidden_from_status_page": False,
            "hidden_from_status_page_at": None,
            "notification_sent": False,
        }

        if self.cache_enabled:
            async with self._cache_lock:
                self._cache["incidents"][incident_id] = cache_entry

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO incidents
                       (id, monitor_type, monitor_id, incident_type, status, source, template_key, title, description, started_at, notification_sent)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)""",
                    incident_id,
                    monitor_type,
                    incident_monitor_id,
                    incident_type,
                    "open",
                    incident_source,
                    template_key,
                    title,
                    description,
                    now,
                    False
                )
        except Exception:
            if self.cache_enabled:
                async with self._cache_lock:
                    self._cache["incidents"].pop(incident_id, None)
            raise
        return incident_id

    async def get_incident_by_id(self, incident_id: uuid.UUID) -> dict | None:
        # Incidents are mutable operational state; prefer fresh DB reads.
        if self.pool:
            try:
                async with self.pool.acquire() as conn:
                    row = await conn.fetchrow("SELECT * FROM incidents WHERE id = $1", incident_id)
                if row:
                    item = dict(row)
                    if self.cache_enabled:
                        async with self._cache_lock:
                            self._cache["incidents"][incident_id] = dict(item)
                    return item
                if self.cache_enabled:
                    async with self._cache_lock:
                        self._cache["incidents"].pop(incident_id, None)
                return None
            except Exception:
                logger.debug("Failed to fetch incident by id=%s from DB", incident_id, exc_info=True)

        if self.cache_enabled:
            async with self._cache_lock:
                cached = self._cache["incidents"].get(incident_id)
                return dict(cached) if cached else None
        if self.cache_only:
            return None
        return None

    async def get_incidents(
        self,
        status_filter: str = None,
        monitor_type: str = None,
        monitor_id: uuid.UUID = None,
        source: str = None,
        exclude_hidden_from_status_page: bool = False,
        include_recent_resolved_hours: int | None = None,
        limit: int = 200
    ) -> list[dict]:
        status_value = (status_filter or "").strip().lower() or None
        source_value = (source or "").strip().lower() or None
        cutoff = None
        if include_recent_resolved_hours is not None:
            hours = max(1, int(include_recent_resolved_hours))
            cutoff = utcnow() - timedelta(hours=hours)

        # Incidents are mutable operational state; prefer fresh DB reads.
        if self.pool:
            try:
                async with self.pool.acquire() as conn:
                    query = "SELECT * FROM incidents WHERE 1=1"
                    params = []
                    if status_value in ("open", "resolved"):
                        params.append(status_value)
                        query += f" AND status = ${len(params)}"
                    elif cutoff is not None:
                        params.append(cutoff)
                        query += f" AND (status = 'open' OR (status = 'resolved' AND resolved_at >= ${len(params)}))"
                    if monitor_type:
                        params.append(monitor_type)
                        query += f" AND monitor_type = ${len(params)}"
                    if monitor_id:
                        params.append(monitor_id)
                        query += f" AND monitor_id = ${len(params)}"
                    if source_value:
                        params.append(source_value)
                        query += f" AND COALESCE(source, 'monitor') = ${len(params)}"
                    if exclude_hidden_from_status_page:
                        query += " AND COALESCE(hidden_from_status_page, false) = false"
                    query += " ORDER BY (CASE WHEN status = 'open' THEN 1 ELSE 0 END) DESC, COALESCE(resolved_at, started_at) DESC"
                    if limit and limit > 0:
                        params.append(limit)
                        query += f" LIMIT ${len(params)}"
                    rows = await conn.fetch(query, *params)
                return [dict(row) for row in rows]
            except Exception:
                logger.debug("Failed to fetch incidents from DB", exc_info=True)

        if self.cache_enabled:
            async with self._cache_lock:
                items = list(self._cache["incidents"].values())

            if status_value in ("open", "resolved"):
                items = [i for i in items if i.get("status") == status_value]
            elif cutoff is not None:
                items = [
                    i for i in items
                    if i.get("status") == "open"
                    or (i.get("status") == "resolved" and i.get("resolved_at") and i.get("resolved_at") >= cutoff)
                ]
            if monitor_type:
                items = [i for i in items if i.get("monitor_type") == monitor_type]
            if monitor_id:
                items = [i for i in items if i.get("monitor_id") == monitor_id]
            if source_value:
                items = [i for i in items if (i.get("source") or "monitor") == source_value]
            if exclude_hidden_from_status_page:
                items = [i for i in items if not bool(i.get("hidden_from_status_page"))]

            def _incident_sort_key(item: dict):
                is_open = 1 if item.get("status") == "open" else 0
                sort_ts = item.get("resolved_at") if item.get("status") == "resolved" else item.get("started_at")
                if sort_ts is None:
                    sort_ts = item.get("started_at") or datetime.min
                return (is_open, sort_ts)

            items.sort(key=_incident_sort_key, reverse=True)
            if limit and limit > 0:
                items = items[:limit]
            return [dict(i) for i in items]
        if self.cache_only:
            return []
        return []

    async def get_open_incidents(
        self,
        monitor_type: str = None,
        monitor_id: uuid.UUID = None,
        source: str = None
    ) -> list[dict]:
        return await self.get_incidents(
            status_filter="open",
            monitor_type=monitor_type,
            monitor_id=monitor_id,
            source=source
        )

    async def get_public_status_incidents(self, resolved_retention_hours: int = 48) -> list[dict]:
        return await self.get_incidents(
            source="admin",
            exclude_hidden_from_status_page=True,
            include_recent_resolved_hours=resolved_retention_hours,
            limit=100
        )

    async def resolve_incident(self, incident_id: uuid.UUID) -> bool:
        now = utcnow()
        old_cache = None
        if self.cache_enabled:
            async with self._cache_lock:
                cached = self._cache["incidents"].get(incident_id)
                if cached:
                    old_cache = dict(cached)
                    cached["status"] = "resolved"
                    cached["resolved_at"] = now

        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE incidents SET status = 'resolved', resolved_at = $2 WHERE id = $1",
                incident_id, now
            )
            success = result == "UPDATE 1"

        if not success and old_cache is not None and self.cache_enabled:
            async with self._cache_lock:
                self._cache["incidents"][incident_id] = old_cache
        return success

    async def hide_incident_from_status_page(self, incident_id: uuid.UUID) -> bool:
        now = utcnow()
        old_cache = None
        if self.cache_enabled:
            async with self._cache_lock:
                cached = self._cache["incidents"].get(incident_id)
                if cached:
                    old_cache = dict(cached)
                    cached["hidden_from_status_page"] = True
                    if not cached.get("hidden_from_status_page_at"):
                        cached["hidden_from_status_page_at"] = now

        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE incidents
                   SET hidden_from_status_page = true,
                       hidden_from_status_page_at = COALESCE(hidden_from_status_page_at, $2)
                 WHERE id = $1
                   AND status = 'resolved'
                """,
                incident_id,
                now
            )
            success = result == "UPDATE 1"

        if not success and old_cache is not None and self.cache_enabled:
            async with self._cache_lock:
                self._cache["incidents"][incident_id] = old_cache
        return success

    async def mark_incident_notification_sent(self, incident_id: uuid.UUID) -> bool:
        old_cache = None
        if self.cache_enabled:
            async with self._cache_lock:
                cached = self._cache["incidents"].get(incident_id)
                if cached:
                    old_cache = dict(cached)
                    cached["notification_sent"] = True

        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE incidents SET notification_sent = true WHERE id = $1",
                incident_id
            )
            success = result == "UPDATE 1"

        if not success and old_cache is not None and self.cache_enabled:
            async with self._cache_lock:
                self._cache["incidents"][incident_id] = old_cache
        return success

    async def get_unnotified_incidents(self, minutes_ago: int = 3) -> list[dict]:
        if self.cache_enabled:
            cutoff = utcnow() - timedelta(minutes=minutes_ago)
            async with self._cache_lock:
                items = list(self._cache["incidents"].values())
            items = [
                i for i in items
                if not i.get("notification_sent")
                and i.get("status") == "open"
                and (i.get("source") or "monitor") != "admin"
                and i.get("started_at") is not None
                and i.get("started_at") < cutoff
            ]
            items.sort(key=lambda i: i.get("started_at") or datetime.min)
            return [dict(i) for i in items]
        if self.cache_only:
            return []

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT * FROM incidents
                   WHERE notification_sent = false
                   AND status = 'open'
                   AND COALESCE(source, 'monitor') <> 'admin'
                   AND started_at < CURRENT_TIMESTAMP - INTERVAL '1 minute' * $1
                   ORDER BY started_at ASC""",
                minutes_ago
            )
            return [dict(row) for row in rows]

# Global database instance
db = Database()
