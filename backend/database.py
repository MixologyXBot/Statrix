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
from .cache import CacheService, CacheUnavailableError
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
# functions. Any key not in the corresponding set is rejected to prevent
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
        self.pool_min_size = settings.PG_POOL_MIN_SIZE
        self.pool_max_size = settings.PG_POOL_MAX_SIZE
        self.pool_statement_cache_size = 0
        self.cache_only = settings.CACHE_ONLY
        self.cache_warming_up: bool = False

    def init_cache_service(self):
        if self.cache_service is None:
            self.cache_service = CacheService()

    async def connect(self):
        if self.cache_service is None:
            self.init_cache_service()
        self.pool_min_size = settings.PG_POOL_MIN_SIZE
        self.pool_max_size = settings.PG_POOL_MAX_SIZE
        _db_url = settings.DATABASE_URL or ""
        uses_pgbouncer = "pgbouncer=true" in _db_url.lower()
        self.pool_statement_cache_size = 0 if uses_pgbouncer else 200
        self.pool = await asyncpg.create_pool(
            settings.DATABASE_URL,
            min_size=self.pool_min_size,
            max_size=self.pool_max_size,
            statement_cache_size=self.pool_statement_cache_size,
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


    _SERIES_QUERIES: list[tuple[str, str, str | None]] = [
        ("uptime_checks", "SELECT * FROM uptime_checks ORDER BY monitor_id, checked_at", "monitor_id"),
        ("server_history", "SELECT * FROM server_history ORDER BY server_id, timestamp", "server_id"),
        ("heartbeat_pings", "SELECT * FROM heartbeat_pings ORDER BY monitor_id, pinged_at", "monitor_id"),
        ("maintenance_events", "SELECT * FROM maintenance_events ORDER BY monitor_type, monitor_id, start_at", None),
        ("monitor_minutes", "SELECT * FROM monitor_minutes ORDER BY monitor_id, minute", "monitor_id"),
    ]

    async def _get_entity_snapshot(self) -> dict[str, Any]:
        if not self.pool:
            return {"entities": {}, "indexes": {}}

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
        return {"entities": entities, "indexes": indexes}

    @staticmethod
    def _group_series_rows(
        series_kind: str,
        rows: list,
        group_key: str | None,
    ) -> dict[Any, list[dict[str, Any]]]:
        grouped: dict[Any, list[dict[str, Any]]] = {}
        if series_kind == "maintenance_events":
            for row in rows:
                key = (row["monitor_type"], row["monitor_id"])
                grouped.setdefault(key, []).append(dict(row))
        else:
            for row in rows:
                gid = row[group_key]
                grouped.setdefault(gid, []).append(dict(row))
        return grouped

    _STREAM_BATCH_SIZE = 500

    async def _stream_series_to_cache(
        self,
        series_kind: str,
        query: str,
        group_key: str | None,
    ) -> int:
        """Stream series rows from PG to Redis in small batches via cursor.

        Uses a server-side cursor so only ~_STREAM_BATCH_SIZE rows live in
        Python memory at any time, preventing OOM on large tables.
        """
        total = 0
        batch: dict[Any, list[dict[str, Any]]] = {}
        batch_count = 0

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                async for record in conn.cursor(query, prefetch=self._STREAM_BATCH_SIZE):
                    if series_kind == "maintenance_events":
                        key = (record["monitor_type"], record["monitor_id"])
                    else:
                        key = record[group_key]

                    batch.setdefault(key, []).append(dict(record))
                    batch_count += 1

                    if batch_count >= self._STREAM_BATCH_SIZE:
                        total += await self.cache_service.write_series_kind(
                            series_kind, batch,
                        )
                        batch.clear()
                        batch_count = 0

                if batch:
                    total += await self.cache_service.write_series_kind(
                        series_kind, batch,
                    )
        return total

    async def load_cache(self):
        if not self.pool:
            return
        if not self.cache_service:
            self.init_cache_service()

        # Stage 1: entities + indexes (small data, safe in memory)
        entity_snap = await self._get_entity_snapshot()
        entity_snap["series"] = {}

        async def _entity_loader():
            return entity_snap

        await self.cache_service.warmup_from_loader(_entity_loader)
        del entity_snap

        # Stage 2: stream each series kind from PG to Redis via cursor.
        # Only ~500 rows exist in Python memory at any time.
        counts: dict[str, int] = {}
        for series_kind, query, group_key in self._SERIES_QUERIES:
            counts[series_kind] = await self._stream_series_to_cache(
                series_kind, query, group_key,
            )

        await self.cache_service.write_warmup_meta(counts)
        self.cache_enabled = True
        self.cache_loaded_at = utcnow()

    async def get_cache_snapshot(self) -> dict[str, Any]:
        snap = await self._get_entity_snapshot()
        if not self.pool:
            snap["series"] = {}
            return snap

        series: dict[str, Any] = {}
        for series_kind, query, group_key in self._SERIES_QUERIES:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query)
            series[series_kind] = self._group_series_rows(series_kind, rows, group_key)
            del rows
        snap["series"] = series
        return snap

    async def get_cache_stats(self) -> dict:
        service_stats: dict[str, Any] = {}
        if self.cache_service:
            try:
                service_stats = await self.cache_service.stats()
            except Exception as exc:
                logger.warning("Failed to collect cache backend stats: %s", exc)
        return {
            "enabled": self.cache_enabled,
            "backend": service_stats.get("backend", "inmemory"),
            "connected": service_stats.get("connected", False),
            "healthy": service_stats.get("healthy", False),
            "last_error": service_stats.get("last_error"),
            "loaded_at": service_stats.get("loaded_at") or self.cache_loaded_at,
            "counts": service_stats.get("counts", {}),
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
        if self.cache_enabled and self.cache_service:
            try:
                monitor = await self.cache_service.get_entity(kind, str(monitor_id))
                return dict(monitor) if monitor else {}
            except CacheUnavailableError:
                pass

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
        if not self.cache_service or not self.pool:
            return
        async with self._cache_resync_lock:
            # Stage 1: entities + indexes
            entity_snap = await self._get_entity_snapshot()
            entity_snap["series"] = {}

            async def _loader():
                return entity_snap

            await self.cache_service.warmup_from_loader(_loader)
            del entity_snap

            # Stage 2: stream series via cursor (same as load_cache)
            counts: dict[str, int] = {}
            for series_kind, query, group_key in self._SERIES_QUERIES:
                counts[series_kind] = await self._stream_series_to_cache(
                    series_kind, query, group_key,
                )

            await self.cache_service.write_warmup_meta(counts)
            self.cache_enabled = True
            self.cache_loaded_at = utcnow()


    @staticmethod
    def _apply_maintenance_to_list(items: list[dict]) -> list[dict]:
        now = utcnow()
        for monitor in items:
            start_at = monitor.get("maintenance_start_at")
            end_at = monitor.get("maintenance_end_at")
            if start_at and start_at <= now and (end_at is None or end_at > now):
                monitor["maintenance_mode"] = True
            elif end_at and end_at <= now:
                monitor["maintenance_mode"] = False
                monitor["maintenance_start_at"] = None
                monitor["maintenance_end_at"] = None
        return items

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
    def _get_status_summary_service():
        try:
            from .status_summary import status_summary_service
            if getattr(status_summary_service, "enabled", False):
                return status_summary_service
        except Exception:
            return None
        return None

    def _status_summary_note(self, method_name: str, *args, **kwargs) -> None:
        svc = self._get_status_summary_service()
        if not svc:
            return
        method = getattr(svc, method_name, None)
        if not callable(method):
            return
        try:
            method(*args, **kwargs)
        except Exception:
            logger.debug("Status summary notify failed: %s", method_name, exc_info=True)

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

        if self.cache_enabled and self.cache_service:
            found = False
            for kind in ("uptime", "server", "heartbeat"):
                entities = await self.cache_service.list_entities(kind)
                for monitor in entities:
                    mid = monitor.get("id")
                    if exclude_monitor_id and mid == exclude_monitor_id:
                        continue
                    if self._normalize_monitor_name(monitor.get("name")) == normalized:
                        found = True
                        break
                if found:
                    break

            if not found:
                return False

            # Confirm with DB to avoid stale cache false positives in multi-worker setups.
            try:
                return await self._is_monitor_name_taken_in_db(normalized, exclude_monitor_id)
            except Exception:
                return True

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
        score = minute.timestamp()
        effective_status = status
        if self.cache_enabled and self.cache_service:
            existing_items = await self.cache_service.range_series(
                "monitor_minutes", str(monitor_id), score, score, limit=1
            )
            merged_status = status
            if existing_items:
                for ex in existing_items:
                    if ex.get("minute") == minute:
                        merged_status = self._merge_minute_status(ex.get("status", ""), status)
                        break
            await self.cache_service.update_series_item(
                "monitor_minutes", str(monitor_id),
                {"monitor_id": monitor_id, "minute": minute, "status": merged_status},
                score,
            )
            effective_status = merged_status
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
        self._status_summary_note("note_monitor_minute", monitor_id, minute, effective_status)

    async def write_monitor_minutes_batch(self, records: list) -> None:
        if not records:
            return
        summary_records: list[tuple[uuid.UUID, datetime, str]] = []
        if self.cache_enabled and self.cache_service:
            for monitor_id, minute, status in records:
                score = minute.timestamp()
                existing_items = await self.cache_service.range_series(
                    "monitor_minutes", str(monitor_id), score, score, limit=1
                )
                merged_status = status
                if existing_items:
                    for ex in existing_items:
                        if ex.get("minute") == minute:
                            merged_status = self._merge_minute_status(ex.get("status", ""), status)
                            break
                await self.cache_service.update_series_item(
                    "monitor_minutes", str(monitor_id),
                    {"monitor_id": monitor_id, "minute": minute, "status": merged_status},
                    score,
                )
                summary_records.append((monitor_id, minute, merged_status))
        else:
            summary_records = [(monitor_id, minute, status) for monitor_id, minute, status in records]
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
        for monitor_id, minute, status in summary_records:
            self._status_summary_note("note_monitor_minute", monitor_id, minute, status)

    async def get_monitor_minutes(self, monitor_id: uuid.UUID, start: datetime, end: datetime) -> list[dict]:
        if self.cache_enabled and self.cache_service:
            try:
                items = await self.cache_service.range_series(
                    "monitor_minutes", str(monitor_id),
                    start.timestamp(), end.timestamp(),
                )
                filtered = [m for m in items if m.get("minute") is not None and start <= m["minute"] < end]
                filtered.sort(key=lambda r: r.get("minute") or datetime.min)
                return filtered
            except CacheUnavailableError:
                pass
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT monitor_id, minute, status FROM monitor_minutes
                   WHERE monitor_id = $1 AND minute >= $2 AND minute < $3
                   ORDER BY minute""",
                monitor_id, start, end
            )
            return [dict(r) for r in rows]

    async def count_monitor_minutes(self, monitor_id: uuid.UUID, start: datetime, end: datetime) -> dict:
        if self.cache_enabled and self.cache_service:
            try:
                items = await self.cache_service.range_series(
                    "monitor_minutes", str(monitor_id),
                    start.timestamp(), end.timestamp(),
                )
                counts = {"up": 0, "down": 0, "maintenance": 0}
                for r in items:
                    m = r.get("minute")
                    if m is None or m < start or m >= end:
                        continue
                    s = r.get("status", "")
                    if s in counts:
                        counts[s] += 1
                return counts
            except CacheUnavailableError:
                pass
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

        if self.cache_enabled and self.cache_service:
            monitor = await self.cache_service.get_entity(kind, str(monitor_id))
            if monitor:
                previous_status = str(monitor.get("status") or "").strip().lower()
                if status_since and (previous_status != status_lower or monitor.get("status_since") is None):
                    monitor["status_since"] = status_since
                monitor["status"] = status
                if last_checkin_at is not None:
                    monitor["last_checkin_at"] = last_checkin_at
                monitor["down_since"] = down_since
                await self.cache_service.set_entity(kind, str(monitor_id), monitor)
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
        self._status_summary_note(
            "note_monitor_status",
            kind,
            monitor_id,
            status,
            last_checkin_at=last_checkin_at,
            down_since=down_since,
            status_since=status_since,
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

        if transitioned and self.cache_enabled and self.cache_service:
            monitor = await self.cache_service.get_entity(cache_kind, str(monitor_id))
            if monitor:
                monitor["status"] = "down"
                monitor["last_checkin_at"] = expected_last_checkin_at
                monitor["down_since"] = down_since
                monitor["status_since"] = down_since
                await self.cache_service.set_entity(cache_kind, str(monitor_id), monitor)
        if transitioned:
            self._status_summary_note(
                "note_monitor_status",
                cache_kind,
                monitor_id,
                "down",
                last_checkin_at=expected_last_checkin_at,
                down_since=down_since,
                status_since=down_since,
            )
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
        if self.cache_enabled and self.cache_service:
            event_data = {
                "id": event_id,
                "monitor_type": event_type,
                "monitor_id": monitor_id,
                "start_at": ts,
                "end_at": None,
                "created_at": ts,
            }
            await self.cache_service.append_series(
                "maintenance_events", str(monitor_id), event_data,
                ts.timestamp(), monitor_type=event_type,
            )

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
        if self.cache_enabled and self.cache_service:
            # Get all maintenance events for this monitor and update the latest open one.
            all_events = await self.cache_service.range_series(
                "maintenance_events", str(monitor_id),
                0, float("inf"), monitor_type=event_type,
            )
            updated = False
            for event in reversed(all_events):
                if event.get("end_at") is None:
                    event["end_at"] = ts
                    score = event.get("start_at")
                    if isinstance(score, datetime):
                        score = score.timestamp()
                    else:
                        score = float(score or 0)
                    await self.cache_service.update_series_item(
                        "maintenance_events", str(monitor_id), event,
                        score, monitor_type=event_type,
                    )
                    updated = True
                    break
            if not updated:
                event_data = {
                    "id": uuid.uuid4(),
                    "monitor_type": event_type,
                    "monitor_id": monitor_id,
                    "start_at": ts,
                    "end_at": ts,
                    "created_at": ts,
                }
                await self.cache_service.append_series(
                    "maintenance_events", str(monitor_id), event_data,
                    ts.timestamp(), monitor_type=event_type,
                )

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
        if self.cache_enabled and self.cache_service:
            try:
                events = await self.cache_service.range_series(
                    "maintenance_events", str(monitor_id),
                    0, float("inf"), monitor_type=event_type,
                )
                filtered = [
                    dict(e) for e in events
                    if e.get("start_at") is not None
                    and e["start_at"] < end_at
                    and (e.get("end_at") is None or e["end_at"] >= start_at)
                ]
                filtered.sort(key=lambda e: e.get("start_at") or datetime.min)
                return filtered
            except CacheUnavailableError:
                pass
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
                if self.cache_enabled and self.cache_service:
                    kind = self._cache_kind_from_type(monitor_type)
                    if kind:
                        cached = await self.cache_service.get_entity(kind, str(monitor_id))
                        if cached:
                            cached["maintenance_mode"] = True
                            cached["maintenance_start_at"] = now
                            cached["maintenance_end_at"] = None
                            await self.cache_service.set_entity(kind, str(monitor_id), cached)
                self._status_summary_note("note_monitor_registry_dirty")
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

        if success and self.cache_enabled and self.cache_service:
            kind = self._cache_kind_from_type(monitor_type)
            if kind:
                cached = await self.cache_service.get_entity(kind, str(monitor_id))
                if cached:
                    cached["maintenance_mode"] = False
                    cached["maintenance_start_at"] = start_at
                    cached["maintenance_end_at"] = end_at
                    await self.cache_service.set_entity(kind, str(monitor_id), cached)
        if success:
            self._status_summary_note("note_monitor_registry_dirty")
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
                if self.cache_enabled and self.cache_service:
                    kind = self._cache_kind_from_type(monitor_type)
                    if kind:
                        cached = await self.cache_service.get_entity(kind, str(monitor_id))
                        if cached:
                            cached["maintenance_mode"] = False
                            cached["maintenance_start_at"] = None
                            cached["maintenance_end_at"] = None
                            await self.cache_service.set_entity(kind, str(monitor_id), cached)
                self._status_summary_note("note_monitor_registry_dirty")
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

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS server_history_daily (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    server_id UUID REFERENCES server_monitors(id) ON DELETE CASCADE,
                    date DATE NOT NULL,
                    avg_cpu_percent NUMERIC(5,2),
                    avg_cpu_io_wait NUMERIC(5,2),
                    avg_cpu_steal NUMERIC(5,2),
                    avg_cpu_user NUMERIC(5,2),
                    avg_cpu_system NUMERIC(5,2),
                    avg_ram_percent NUMERIC(5,2),
                    avg_ram_swap_percent NUMERIC(5,2),
                    avg_ram_buff_percent NUMERIC(5,2),
                    avg_ram_cache_percent NUMERIC(5,2),
                    avg_load_1 NUMERIC(6,2),
                    avg_load_5 NUMERIC(6,2),
                    avg_load_15 NUMERIC(6,2),
                    total_network_in BIGINT,
                    total_network_out BIGINT,
                    avg_network_in BIGINT,
                    avg_network_out BIGINT,
                    avg_disk_percent NUMERIC(5,2),
                    record_count INTEGER,
                    UNIQUE(server_id, date)
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS uptime_checks_daily (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    monitor_id UUID REFERENCES uptime_monitors(id) ON DELETE CASCADE,
                    date DATE NOT NULL,
                    up_count INTEGER DEFAULT 0,
                    down_count INTEGER DEFAULT 0,
                    total_count INTEGER DEFAULT 0,
                    avg_response_time_ms NUMERIC(8,2),
                    UNIQUE(monitor_id, date)
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS heartbeat_pings_daily (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    monitor_id UUID REFERENCES heartbeat_monitors(id) ON DELETE CASCADE,
                    date DATE NOT NULL,
                    ping_count INTEGER DEFAULT 0,
                    UNIQUE(monitor_id, date)
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS monitor_minutes_daily (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    monitor_id UUID NOT NULL,
                    date DATE NOT NULL,
                    up_minutes INTEGER DEFAULT 0,
                    down_minutes INTEGER DEFAULT 0,
                    maintenance_minutes INTEGER DEFAULT 0,
                    UNIQUE(monitor_id, date)
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
            "CREATE INDEX IF NOT EXISTS idx_server_history_daily_lookup ON server_history_daily(server_id, date DESC)",
            "CREATE INDEX IF NOT EXISTS idx_uptime_checks_daily_lookup ON uptime_checks_daily(monitor_id, date DESC)",
            "CREATE INDEX IF NOT EXISTS idx_heartbeat_pings_daily_lookup ON heartbeat_pings_daily(monitor_id, date DESC)",
            "CREATE INDEX IF NOT EXISTS idx_monitor_minutes_daily_lookup ON monitor_minutes_daily(monitor_id, date DESC)",
        ]

        for index_sql in indexes:
            try:
                await conn.execute(index_sql)
            except Exception:
                logger.debug("Failed to create index: %s", index_sql, exc_info=True)

    async def compress_old_data(self) -> dict[str, int]:
        retention_days = settings.DATA_RETENTION_DAYS
        cutoff = utcnow() - timedelta(days=retention_days)
        cutoff_ts = cutoff.timestamp()
        results: dict[str, int] = {}

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO server_history_daily (
                        server_id, date,
                        avg_cpu_percent, avg_cpu_io_wait, avg_cpu_steal,
                        avg_cpu_user, avg_cpu_system,
                        avg_ram_percent, avg_ram_swap_percent,
                        avg_ram_buff_percent, avg_ram_cache_percent,
                        avg_load_1, avg_load_5, avg_load_15,
                        total_network_in, total_network_out,
                        avg_network_in, avg_network_out,
                        avg_disk_percent, record_count
                    )
                    SELECT
                        server_id, DATE(timestamp),
                        AVG(cpu_percent), AVG(cpu_io_wait), AVG(cpu_steal),
                        AVG(cpu_user), AVG(cpu_system),
                        AVG(ram_percent), AVG(ram_swap_percent),
                        AVG(ram_buff_percent), AVG(ram_cache_percent),
                        AVG(load_1), AVG(load_5), AVG(load_15),
                        SUM(network_in), SUM(network_out),
                        AVG(network_in), AVG(network_out),
                        AVG(disk_percent), COUNT(*)
                    FROM server_history
                    WHERE timestamp < $1
                    GROUP BY server_id, DATE(timestamp)
                    ON CONFLICT (server_id, date) DO UPDATE SET
                        avg_cpu_percent = EXCLUDED.avg_cpu_percent,
                        avg_cpu_io_wait = EXCLUDED.avg_cpu_io_wait,
                        avg_cpu_steal = EXCLUDED.avg_cpu_steal,
                        avg_cpu_user = EXCLUDED.avg_cpu_user,
                        avg_cpu_system = EXCLUDED.avg_cpu_system,
                        avg_ram_percent = EXCLUDED.avg_ram_percent,
                        avg_ram_swap_percent = EXCLUDED.avg_ram_swap_percent,
                        avg_ram_buff_percent = EXCLUDED.avg_ram_buff_percent,
                        avg_ram_cache_percent = EXCLUDED.avg_ram_cache_percent,
                        avg_load_1 = EXCLUDED.avg_load_1,
                        avg_load_5 = EXCLUDED.avg_load_5,
                        avg_load_15 = EXCLUDED.avg_load_15,
                        total_network_in = EXCLUDED.total_network_in,
                        total_network_out = EXCLUDED.total_network_out,
                        avg_network_in = EXCLUDED.avg_network_in,
                        avg_network_out = EXCLUDED.avg_network_out,
                        avg_disk_percent = EXCLUDED.avg_disk_percent,
                        record_count = EXCLUDED.record_count
                    """,
                    cutoff,
                )
                tag = await conn.execute(
                    "DELETE FROM server_history WHERE timestamp < $1", cutoff
                )
                results["server_history"] = int(tag.split()[-1]) if tag else 0

                await conn.execute(
                    """
                    INSERT INTO uptime_checks_daily (
                        monitor_id, date,
                        up_count, down_count, total_count,
                        avg_response_time_ms
                    )
                    SELECT
                        monitor_id, DATE(checked_at),
                        COUNT(*) FILTER (WHERE status = 'up'),
                        COUNT(*) FILTER (WHERE status <> 'up'),
                        COUNT(*),
                        AVG(response_time_ms)
                    FROM uptime_checks
                    WHERE checked_at < $1
                    GROUP BY monitor_id, DATE(checked_at)
                    ON CONFLICT (monitor_id, date) DO UPDATE SET
                        up_count = EXCLUDED.up_count,
                        down_count = EXCLUDED.down_count,
                        total_count = EXCLUDED.total_count,
                        avg_response_time_ms = EXCLUDED.avg_response_time_ms
                    """,
                    cutoff,
                )
                tag = await conn.execute(
                    "DELETE FROM uptime_checks WHERE checked_at < $1", cutoff
                )
                results["uptime_checks"] = int(tag.split()[-1]) if tag else 0

                await conn.execute(
                    """
                    INSERT INTO heartbeat_pings_daily (
                        monitor_id, date, ping_count
                    )
                    SELECT
                        monitor_id, DATE(pinged_at), COUNT(*)
                    FROM heartbeat_pings
                    WHERE pinged_at < $1
                    GROUP BY monitor_id, DATE(pinged_at)
                    ON CONFLICT (monitor_id, date) DO UPDATE SET
                        ping_count = EXCLUDED.ping_count
                    """,
                    cutoff,
                )
                tag = await conn.execute(
                    "DELETE FROM heartbeat_pings WHERE pinged_at < $1", cutoff
                )
                results["heartbeat_pings"] = int(tag.split()[-1]) if tag else 0

                await conn.execute(
                    """
                    INSERT INTO monitor_minutes_daily (
                        monitor_id, date,
                        up_minutes, down_minutes, maintenance_minutes
                    )
                    SELECT
                        monitor_id, DATE(minute),
                        COUNT(*) FILTER (WHERE status = 'up'),
                        COUNT(*) FILTER (WHERE status = 'down'),
                        COUNT(*) FILTER (WHERE status = 'maintenance')
                    FROM monitor_minutes
                    WHERE minute < $1
                    GROUP BY monitor_id, DATE(minute)
                    ON CONFLICT (monitor_id, date) DO UPDATE SET
                        up_minutes = EXCLUDED.up_minutes,
                        down_minutes = EXCLUDED.down_minutes,
                        maintenance_minutes = EXCLUDED.maintenance_minutes
                    """,
                    cutoff,
                )
                tag = await conn.execute(
                    "DELETE FROM monitor_minutes WHERE minute < $1", cutoff
                )
                results["monitor_minutes"] = int(tag.split()[-1]) if tag else 0

        if self.cache_enabled and self.cache_service:
            try:
                for monitor_data in await self.cache_service.list_entities("server"):
                    mid = str(monitor_data.get("id", ""))
                    if mid:
                        await self.cache_service.delete_series_range(
                            "server_history", mid, cutoff_ts
                        )

                for monitor_data in await self.cache_service.list_entities("uptime"):
                    mid = str(monitor_data.get("id", ""))
                    if mid:
                        await self.cache_service.delete_series_range(
                            "uptime_checks", mid, cutoff_ts
                        )

                for monitor_data in await self.cache_service.list_entities("heartbeat"):
                    mid = str(monitor_data.get("id", ""))
                    if mid:
                        await self.cache_service.delete_series_range(
                            "heartbeat_pings", mid, cutoff_ts
                        )
            except Exception as exc:
                logger.warning("Failed to trim Redis series during compression: %s", exc)

        logger.info(
            "Data compression complete (retention=%dd): %s",
            retention_days,
            ", ".join(f"{k}={v}" for k, v in results.items()),
        )
        return results

    async def get_user_by_email(self, email: str) -> dict | None:
        if self.cache_enabled and self.cache_service:
            try:
                key = str(email).lower()
                user_id = await self.cache_service.get_index("user_email", key)
                if user_id:
                    cached = await self.cache_service.get_entity("users", str(user_id))
                    return dict(cached) if cached else None
                return None
            except CacheUnavailableError:
                pass
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
        if self.cache_enabled and self.cache_service:
            await self.cache_service.set_entity("users", str(user_id), cache_entry)
            await self.cache_service.set_index("user_email", str(email).lower(), str(user_id))
        return user_id


    async def get_uptime_monitors(self, enabled_only: bool = False, public_only: bool = False) -> list[dict]:
        if self.cache_enabled and self.cache_service:
            try:
                items = await self.cache_service.list_entities("uptime")
                self._apply_maintenance_to_list(items)
                if enabled_only:
                    items = [m for m in items if m.get("enabled")]
                if public_only:
                    items = [m for m in items if m.get("is_public")]
                return [dict(m) for m in items]
            except CacheUnavailableError:
                pass
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
        if self.cache_enabled and self.cache_service:
            try:
                cached = await self.cache_service.get_entity("uptime", str(monitor_id))
                if cached:
                    self._apply_maintenance_to_list([cached])
                    return dict(cached)
                return None
            except CacheUnavailableError:
                pass
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
            return dict(row)

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

        if self.cache_enabled and self.cache_service:
            await self.cache_service.set_entity("uptime", str(monitor_id), cache_entry)

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
            if self.cache_enabled and self.cache_service:
                await self.cache_service.delete_entity("uptime", str(monitor_id))
            raise
        self._status_summary_note("note_monitor_created", "uptime", monitor_id)
        return monitor_id

    async def update_uptime_monitor(self, monitor_id: uuid.UUID, **kwargs) -> bool:
        _bad_keys = set(kwargs) - _UPTIME_UPDATABLE_FIELDS - {"updated_at"}
        if _bad_keys:
            raise ValueError(f"Invalid uptime monitor fields: {_bad_keys}")
        updated_at = utcnow()
        old_cache = None
        if self.cache_enabled and self.cache_service:
            cached = await self.cache_service.get_entity("uptime", str(monitor_id))
            if cached:
                old_cache = dict(cached)
                for key, value in kwargs.items():
                    if value is not None:
                        cached[key] = value
                cached["updated_at"] = updated_at
                await self.cache_service.set_entity("uptime", str(monitor_id), cached)

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

        if not success and old_cache is not None and self.cache_enabled and self.cache_service:
            await self.cache_service.set_entity("uptime", str(monitor_id), old_cache)
        if success:
            self._status_summary_note("note_monitor_registry_dirty")
        return success

    async def delete_uptime_monitor(self, monitor_id: uuid.UUID) -> bool:
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

        if success and self.cache_enabled and self.cache_service:
            await self.cache_service.delete_entity("uptime", str(monitor_id))
            await self.cache_service.delete_series_group("uptime_checks", str(monitor_id))
            await self.cache_service.delete_series_group("monitor_minutes", str(monitor_id))
            all_incidents = await self.cache_service.list_entities("incidents")
            for incident in all_incidents:
                if str(incident.get("monitor_id")) == str(monitor_id):
                    await self.cache_service.delete_entity("incidents", str(incident["id"]))
            for event_type in ("website", "uptime"):
                await self.cache_service.delete_series_group(
                    "maintenance_events", str(monitor_id), monitor_type=event_type,
                )
        if success:
            self._status_summary_note("note_monitor_deleted", monitor_id)
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
        check_data = {
            "id": check_id,
            "monitor_id": monitor_id,
            "status": status,
            "response_time_ms": response_time_ms,
            "status_code": status_code,
            "error_message": error_message,
            "checked_at": now,
        }

        if self.cache_enabled and self.cache_service:
            cached = await self.cache_service.get_entity("uptime", str(monitor_id))
            if cached:
                cached["last_check_at"] = now
                if str(status).lower() == "up":
                    cached["last_up_at"] = now
                normalized_status = self._normalize_uptime_status(status)
                prev_status = cached.get("status")
                if prev_status != normalized_status:
                    cached["status"] = normalized_status
                    cached["status_since"] = now
                await self.cache_service.set_entity("uptime", str(monitor_id), cached)
            await self.cache_service.append_series(
                "uptime_checks", str(monitor_id), check_data, now.timestamp(),
            )
        self._status_summary_note(
            "note_uptime_check",
            monitor_id,
            status,
            response_time_ms,
            now,
        )

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
            logger.exception("Failed to write uptime check to PG for monitor_id=%s", monitor_id)
            return check_id


    async def get_uptime_stats(self, monitor_id: uuid.UUID, days: int = 90) -> dict:
        if self.cache_enabled and self.cache_service:
            try:
                now = utcnow()
                cutoff = now - timedelta(days=days)
                checks = await self.cache_service.range_series(
                    "uptime_checks", str(monitor_id),
                    cutoff.timestamp(), now.timestamp(),
                )
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
            except CacheUnavailableError:
                pass
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


    async def get_uptime_multi_period_stats(self, monitor_id: uuid.UUID) -> dict:
        """Get all uptime stats (24h, 7d, 30d, year, total) combining recent + daily data."""
        now = utcnow()
        start_24h = now - timedelta(hours=24)
        start_7d = now - timedelta(days=7)
        start_30d = now - timedelta(days=30)
        start_year = datetime(now.year, 1, 1)

        total_24h = up_24h = 0
        total_7d = up_7d = 0
        total_30d = up_30d = 0
        total_year = up_year = 0
        total_all = up_all = 0
        first_check: datetime | None = None
        rt_sum_all = 0.0
        rt_count_all = 0

        # Recent detailed data from cache.
        if self.cache_enabled and self.cache_service:
            try:
                checks = await self.cache_service.range_series(
                    "uptime_checks", str(monitor_id), 0, now.timestamp(),
                )
            except CacheUnavailableError:
                checks = []
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
                            pass
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

        # Daily summary data from PostgreSQL for older periods.
        if self.pool:
            try:
                async with self.pool.acquire() as conn:
                    daily_rows = await conn.fetch(
                        "SELECT date, up_count, down_count, total_count, avg_response_time_ms "
                        "FROM uptime_checks_daily WHERE monitor_id = $1",
                        monitor_id,
                    )
                    for dr in daily_rows:
                        d = dr["date"]
                        up = int(dr["up_count"] or 0)
                        total = int(dr["total_count"] or 0)
                        avg_rt = dr["avg_response_time_ms"]
                        day_dt = datetime(d.year, d.month, d.day)

                        total_all += total
                        up_all += up
                        if avg_rt is not None and total > 0:
                            rt_sum_all += float(avg_rt) * up
                            rt_count_all += up
                        if first_check is None or day_dt < first_check:
                            first_check = day_dt

                        if day_dt >= start_30d:
                            total_30d += total
                            up_30d += up
                        if day_dt >= start_year:
                            total_year += total
                            up_year += up
            except Exception:
                logger.exception("Failed to fetch uptime daily summaries")

        if not self.cache_enabled and not self.cache_service and not self.cache_only and self.pool:
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
            "avg_response_time_all": (rt_sum_all / rt_count_all) if rt_count_all else None,
        }

    async def get_server_monitors(self, enabled_only: bool = False, public_only: bool = False) -> list[dict]:
        if self.cache_enabled and self.cache_service:
            try:
                items = await self.cache_service.list_entities("server")
                self._apply_maintenance_to_list(items)
                if enabled_only:
                    items = [m for m in items if m.get("enabled")]
                if public_only:
                    items = [m for m in items if m.get("is_public")]
                return [dict(m) for m in items]
            except CacheUnavailableError:
                pass
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
        if self.cache_enabled and self.cache_service:
            try:
                cached = await self.cache_service.get_entity("server", str(server_id))
                if cached:
                    self._apply_maintenance_to_list([cached])
                    cached["status"] = cached.get("status", "unknown")
                    return dict(cached)
                return None
            except CacheUnavailableError:
                pass
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
        data["status"] = data.get("status", "unknown")
        return data

    async def get_server_monitor_by_sid(self, sid: str) -> dict | None:
        if self.cache_enabled and self.cache_service:
            try:
                if not sid:
                    return None
                server_id = await self.cache_service.get_index("server_sid", sid)
                if server_id:
                    cached = await self.cache_service.get_entity("server", str(server_id))
                    return dict(cached) if cached else None
                return None
            except CacheUnavailableError:
                pass
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

        if self.cache_enabled and self.cache_service:
            await self.cache_service.set_entity("server", str(server_id), cache_entry)
            if sid:
                await self.cache_service.set_index("server_sid", sid, str(server_id))

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO server_monitors (id, sid, name, category, created_at)
                       VALUES ($1, $2, $3, $4, $5)""",
                    server_id, sid, name, category, now
                )
        except Exception:
            if self.cache_enabled and self.cache_service:
                await self.cache_service.delete_entity("server", str(server_id))
                if sid:
                    await self.cache_service.delete_index("server_sid", sid)
            raise
        self._status_summary_note("note_monitor_created", "server", server_id)
        return server_id

    async def update_server_monitor(self, server_id: uuid.UUID, **kwargs) -> bool:
        _bad_keys = set(kwargs) - _SERVER_UPDATABLE_FIELDS
        if _bad_keys:
            raise ValueError(f"Invalid server monitor fields: {_bad_keys}")
        old_cache = None
        old_sid = None
        if self.cache_enabled and self.cache_service:
            cached = await self.cache_service.get_entity("server", str(server_id))
            if cached:
                old_cache = dict(cached)
                old_sid = cached.get("sid")
                for key, value in kwargs.items():
                    if value is not None:
                        cached[key] = value
                new_sid = kwargs.get("sid")
                if new_sid and new_sid != old_sid:
                    if old_sid:
                        await self.cache_service.delete_index("server_sid", old_sid)
                    await self.cache_service.set_index("server_sid", new_sid, str(server_id))
                await self.cache_service.set_entity("server", str(server_id), cached)

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

        if not success and old_cache is not None and self.cache_enabled and self.cache_service:
            await self.cache_service.set_entity("server", str(server_id), old_cache)
            new_sid = kwargs.get("sid")
            if new_sid and new_sid != old_sid:
                await self.cache_service.delete_index("server_sid", new_sid)
            if old_sid:
                await self.cache_service.set_index("server_sid", old_sid, str(server_id))
        if success:
            self._status_summary_note("note_monitor_registry_dirty")
        return success

    async def delete_server_monitor(self, server_id: uuid.UUID) -> bool:
        # Get SID before deleting for index cleanup.
        old_sid = None
        if self.cache_enabled and self.cache_service:
            cached = await self.cache_service.get_entity("server", str(server_id))
            if cached:
                old_sid = cached.get("sid")

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

        if success and self.cache_enabled and self.cache_service:
            await self.cache_service.delete_entity("server", str(server_id))
            if old_sid:
                await self.cache_service.delete_index("server_sid", old_sid)
            await self.cache_service.delete_series_group("server_history", str(server_id))
            await self.cache_service.delete_series_group("monitor_minutes", str(server_id))
            all_incidents = await self.cache_service.list_entities("incidents")
            for incident in all_incidents:
                if str(incident.get("monitor_id")) == str(server_id):
                    await self.cache_service.delete_entity("incidents", str(incident["id"]))
            for event_type in ("server_agent", "server", "agent", "server-agent", "heartbeat-server-agent"):
                await self.cache_service.delete_series_group(
                    "maintenance_events", str(server_id), monitor_type=event_type,
                )
        if success:
            self._status_summary_note("note_monitor_deleted", server_id)
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
        history_data = {
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
            "disk_percent": disk_percent,
        }

        if self.cache_enabled and self.cache_service:
            await self.cache_service.append_series(
                "server_history", str(server_id), history_data, now.timestamp(),
            )
        self._status_summary_note("note_server_metrics", server_id, history_data, now)

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
            logger.exception("Failed to write server history to PG for server_id=%s", server_id)
            return history_id

    async def get_server_history(self, server_id: uuid.UUID, hours: int = 24, limit: int = None) -> list[dict]:
        if self.cache_enabled and self.cache_service:
            try:
                if limit:
                    items = await self.cache_service.tail_series(
                        "server_history", str(server_id), limit,
                    )
                else:
                    now = utcnow()
                    cutoff = now - timedelta(hours=hours)
                    items = await self.cache_service.range_series(
                        "server_history", str(server_id),
                        cutoff.timestamp(), now.timestamp(),
                    )
                    items.sort(key=lambda r: r.get("timestamp") or datetime.min)
                return [dict(row) for row in items]
            except CacheUnavailableError:
                pass
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
        if self.cache_enabled and self.cache_service:
            try:
                items = await self.cache_service.range_series(
                    "server_history", str(server_id),
                    start.timestamp(), end.timestamp(),
                )
                items.sort(key=lambda r: r.get("timestamp") or datetime.min)
                return [dict(row) for row in items]
            except CacheUnavailableError:
                pass
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
        Interval can be '15min', 'hour', or 'day'.
        Combines recent detailed data from cache with daily summaries for older periods.
        """
        metrics = [
            "cpu_percent", "cpu_io_wait", "cpu_steal", "cpu_user", "cpu_system",
            "ram_percent", "ram_swap_percent", "ram_buff_percent", "ram_cache_percent",
            "load_1", "load_5", "load_15",
            "network_in", "network_out", "disk_percent",
        ]

        if self.cache_enabled and self.cache_service:
            now = utcnow()
            cutoff = now - timedelta(hours=hours)

            def bucket(ts: datetime) -> datetime:
                if interval == '15min':
                    return datetime(ts.year, ts.month, ts.day, ts.hour, (ts.minute // 15) * 15)
                if interval == 'hour':
                    return datetime(ts.year, ts.month, ts.day, ts.hour)
                return datetime(ts.year, ts.month, ts.day)

            buckets: dict[datetime, dict[str, dict[str, float]]] = {}

            try:
                history = await self.cache_service.range_series(
                    "server_history", str(server_id),
                    cutoff.timestamp(), now.timestamp(),
                )
            except CacheUnavailableError:
                history = []
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

            # For day-level aggregation beyond retention, add daily summaries.
            if interval == 'day' and self.pool:
                retention_cutoff = now - timedelta(days=settings.DATA_RETENTION_DAYS)
                if cutoff < retention_cutoff:
                    try:
                        async with self.pool.acquire() as conn:
                            daily_rows = await conn.fetch(
                                """SELECT date,
                                    avg_cpu_percent, avg_cpu_io_wait, avg_cpu_steal,
                                    avg_cpu_user, avg_cpu_system,
                                    avg_ram_percent, avg_ram_swap_percent,
                                    avg_ram_buff_percent, avg_ram_cache_percent,
                                    avg_load_1, avg_load_5, avg_load_15,
                                    avg_network_in, avg_network_out,
                                    avg_disk_percent, record_count
                                FROM server_history_daily
                                WHERE server_id = $1 AND date >= $2
                                ORDER BY date ASC""",
                                server_id, cutoff.date(),
                            )
                            daily_metric_map = {
                                "cpu_percent": "avg_cpu_percent",
                                "cpu_io_wait": "avg_cpu_io_wait",
                                "cpu_steal": "avg_cpu_steal",
                                "cpu_user": "avg_cpu_user",
                                "cpu_system": "avg_cpu_system",
                                "ram_percent": "avg_ram_percent",
                                "ram_swap_percent": "avg_ram_swap_percent",
                                "ram_buff_percent": "avg_ram_buff_percent",
                                "ram_cache_percent": "avg_ram_cache_percent",
                                "load_1": "avg_load_1",
                                "load_5": "avg_load_5",
                                "load_15": "avg_load_15",
                                "network_in": "avg_network_in",
                                "network_out": "avg_network_out",
                                "disk_percent": "avg_disk_percent",
                            }
                            for dr in daily_rows:
                                d = dr["date"]
                                day_key = datetime(d.year, d.month, d.day)
                                if day_key in buckets:
                                    continue
                                cnt = int(dr["record_count"] or 1)
                                agg = buckets.setdefault(day_key, {m: {"sum": 0.0, "count": 0} for m in metrics})
                                for m in metrics:
                                    col = daily_metric_map[m]
                                    val = dr[col]
                                    if val is not None:
                                        agg[m]["sum"] += float(val) * cnt
                                        agg[m]["count"] += cnt
                    except Exception:
                        logger.exception("Failed to fetch server history daily for aggregation")

            result = []
            for ts, agg in buckets.items():
                row_out: dict[str, Any] = {"timestamp": ts}
                for m in metrics:
                    if agg[m]["count"]:
                        row_out[m] = agg[m]["sum"] / agg[m]["count"]
                    else:
                        row_out[m] = None
                result.append(row_out)
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


    async def get_heartbeat_monitors(self, enabled_only: bool = False, public_only: bool = False) -> list[dict]:
        if self.cache_enabled and self.cache_service:
            try:
                items = await self.cache_service.list_entities("heartbeat")
                self._apply_maintenance_to_list(items)
                if enabled_only:
                    items = [m for m in items if m.get("enabled")]
                if public_only:
                    items = [m for m in items if m.get("is_public")]
                return [dict(m) for m in items]
            except CacheUnavailableError:
                pass
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
        if self.cache_enabled and self.cache_service:
            try:
                cached = await self.cache_service.get_entity("heartbeat", str(monitor_id))
                if cached:
                    self._apply_maintenance_to_list([cached])
                    cached["status"] = cached.get("status", "unknown")
                    return dict(cached)
                return None
            except CacheUnavailableError:
                pass
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
        data["status"] = data.get("status", "unknown")
        return data

    async def get_heartbeat_monitor_by_sid(self, sid: str) -> dict | None:
        if self.cache_enabled and self.cache_service:
            try:
                if not sid:
                    return None
                monitor_id = await self.cache_service.get_index("heartbeat_sid", sid)
                if monitor_id:
                    cached = await self.cache_service.get_entity("heartbeat", str(monitor_id))
                    return dict(cached) if cached else None
                return None
            except CacheUnavailableError:
                pass
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

        if self.cache_enabled and self.cache_service:
            await self.cache_service.set_entity("heartbeat", str(monitor_id), cache_entry)
            if sid:
                await self.cache_service.set_index("heartbeat_sid", sid, str(monitor_id))

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO heartbeat_monitors
                       (id, sid, name, heartbeat_type, timeout, grace_period, category, created_at, updated_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)""",
                    monitor_id, sid, name, heartbeat_type, timeout, grace_period, category, now, now
                )
        except Exception:
            if self.cache_enabled and self.cache_service:
                await self.cache_service.delete_entity("heartbeat", str(monitor_id))
                if sid:
                    await self.cache_service.delete_index("heartbeat_sid", sid)
            raise
        self._status_summary_note("note_monitor_created", "heartbeat", monitor_id)
        return monitor_id

    async def update_heartbeat_monitor(self, monitor_id: uuid.UUID, **kwargs) -> bool:
        _bad_keys = set(kwargs) - _HEARTBEAT_UPDATABLE_FIELDS - {"updated_at"}
        if _bad_keys:
            raise ValueError(f"Invalid heartbeat monitor fields: {_bad_keys}")
        updated_at = utcnow()
        old_cache = None
        old_sid = None
        if self.cache_enabled and self.cache_service:
            cached = await self.cache_service.get_entity("heartbeat", str(monitor_id))
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
                        await self.cache_service.delete_index("heartbeat_sid", old_sid)
                    await self.cache_service.set_index("heartbeat_sid", new_sid, str(monitor_id))
                await self.cache_service.set_entity("heartbeat", str(monitor_id), cached)

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

        if not success and old_cache is not None and self.cache_enabled and self.cache_service:
            await self.cache_service.set_entity("heartbeat", str(monitor_id), old_cache)
            new_sid = kwargs.get("sid")
            if new_sid and new_sid != old_sid:
                await self.cache_service.delete_index("heartbeat_sid", new_sid)
            if old_sid:
                await self.cache_service.set_index("heartbeat_sid", old_sid, str(monitor_id))
        if success:
            self._status_summary_note("note_monitor_registry_dirty")
        return success

    async def delete_heartbeat_monitor(self, monitor_id: uuid.UUID) -> bool:
        old_sid = None
        if self.cache_enabled and self.cache_service:
            cached = await self.cache_service.get_entity("heartbeat", str(monitor_id))
            if cached:
                old_sid = cached.get("sid")

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

        if success and self.cache_enabled and self.cache_service:
            await self.cache_service.delete_entity("heartbeat", str(monitor_id))
            if old_sid:
                await self.cache_service.delete_index("heartbeat_sid", old_sid)
            await self.cache_service.delete_series_group("heartbeat_pings", str(monitor_id))
            await self.cache_service.delete_series_group("monitor_minutes", str(monitor_id))
            all_incidents = await self.cache_service.list_entities("incidents")
            for incident in all_incidents:
                if str(incident.get("monitor_id")) == str(monitor_id):
                    await self.cache_service.delete_entity("incidents", str(incident["id"]))
            for event_type in ("heartbeat", "heartbeat-cronjob", "cronjob"):
                await self.cache_service.delete_series_group(
                    "maintenance_events", str(monitor_id), monitor_type=event_type,
                )
        if success:
            self._status_summary_note("note_monitor_deleted", monitor_id)
        return success

    async def record_heartbeat_ping(self, monitor_id: uuid.UUID, ping_source: str = None) -> uuid.UUID:
        now = utcnow()
        ping_id = uuid.uuid4()
        ping_data = {
            "id": ping_id,
            "monitor_id": monitor_id,
            "ping_source": ping_source,
            "pinged_at": now,
        }

        if self.cache_enabled and self.cache_service:
            await self.cache_service.append_series(
                "heartbeat_pings", str(monitor_id), ping_data, now.timestamp(),
            )
            cached = await self.cache_service.get_entity("heartbeat", str(monitor_id))
            if cached:
                cached["last_ping_at"] = now
                await self.cache_service.set_entity("heartbeat", str(monitor_id), cached)
        self._status_summary_note("note_heartbeat_ping", monitor_id, now)

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
            logger.exception("Failed to write heartbeat ping to PG for monitor_id=%s", monitor_id)
            return ping_id

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

        if self.cache_enabled and self.cache_service:
            await self.cache_service.set_entity("incidents", str(incident_id), cache_entry)

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
            if self.cache_enabled and self.cache_service:
                await self.cache_service.delete_entity("incidents", str(incident_id))
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
                    if self.cache_enabled and self.cache_service:
                        await self.cache_service.set_entity("incidents", str(incident_id), dict(item))
                    return item
                if self.cache_enabled and self.cache_service:
                    await self.cache_service.delete_entity("incidents", str(incident_id))
                return None
            except Exception:
                logger.debug("Failed to fetch incident by id=%s from DB", incident_id, exc_info=True)

        if self.cache_enabled and self.cache_service:
            cached = await self.cache_service.get_entity("incidents", str(incident_id))
            return dict(cached) if cached else None
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

        if self.cache_enabled and self.cache_service:
            items = await self.cache_service.list_entities("incidents")

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
        if self.cache_enabled and self.cache_service:
            cached = await self.cache_service.get_entity("incidents", str(incident_id))
            if cached:
                old_cache = dict(cached)
                cached["status"] = "resolved"
                cached["resolved_at"] = now
                await self.cache_service.set_entity("incidents", str(incident_id), cached)

        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE incidents SET status = 'resolved', resolved_at = $2 WHERE id = $1",
                incident_id, now
            )
            success = result == "UPDATE 1"

        if not success and old_cache is not None and self.cache_enabled and self.cache_service:
            await self.cache_service.set_entity("incidents", str(incident_id), old_cache)
        return success

    async def hide_incident_from_status_page(self, incident_id: uuid.UUID) -> bool:
        now = utcnow()
        old_cache = None
        if self.cache_enabled and self.cache_service:
            cached = await self.cache_service.get_entity("incidents", str(incident_id))
            if cached:
                old_cache = dict(cached)
                cached["hidden_from_status_page"] = True
                if not cached.get("hidden_from_status_page_at"):
                    cached["hidden_from_status_page_at"] = now
                await self.cache_service.set_entity("incidents", str(incident_id), cached)

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

        if not success and old_cache is not None and self.cache_enabled and self.cache_service:
            await self.cache_service.set_entity("incidents", str(incident_id), old_cache)
        return success



db = Database()
