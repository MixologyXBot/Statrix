# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import uuid
from datetime import datetime
from typing import Any

from redis.asyncio import Redis

from ..utils.time import utcnow
from .base import CacheBackend, SnapshotLoader, coerce_series_score
from .serializer import dumps, loads


ENTITY_KINDS = ("users", "uptime", "server", "heartbeat", "incidents")
INDEX_KINDS = ("user_email", "server_sid", "heartbeat_sid")
SERIES_KINDS = (
    "uptime_checks",
    "server_history",
    "heartbeat_pings",
    "monitor_minutes",
    "maintenance_events",
)


class RedisCacheBackend(CacheBackend):
    backend_name = "redis"

    def __init__(
        self,
        redis_url: str,
        key_prefix: str = "statrix:v1",
        warmup_batch_size: int = 500,
    ) -> None:
        self.redis_url = redis_url
        self.key_prefix = key_prefix.strip() or "statrix:v1"
        self.warmup_batch_size = max(50, int(warmup_batch_size or 500))
        self.client: Redis | None = None
        self.connected = False
        self.last_ping_error: str | None = None

    def _k(self, suffix: str) -> str:
        return f"{self.key_prefix}:{suffix}"

    def _entity_key(self, kind: str) -> str:
        return self._k(f"h:{kind}")

    def _index_key(self, kind: str) -> str:
        return self._k(f"idx:{kind}")

    def _series_zkey(self, series_kind: str, monitor_id: str, monitor_type: str | None = None) -> str:
        if series_kind == "maintenance_events":
            if not monitor_type:
                raise ValueError("monitor_type is required for maintenance_events series")
            return self._k(f"z:{series_kind}:{monitor_type}:{monitor_id}")
        return self._k(f"z:{series_kind}:{monitor_id}")

    def _series_obj_key(self, series_kind: str, monitor_id: str) -> str:
        if series_kind == "monitor_minutes":
            return self._k(f"obj:{series_kind}:{monitor_id}")
        return self._k(f"obj:{series_kind}")

    @staticmethod
    def _coerce_member_id(series_kind: str, item: dict[str, Any]) -> str:
        if series_kind == "monitor_minutes":
            minute = item.get("minute")
            if isinstance(minute, datetime):
                return minute.isoformat()
            return str(minute or "")
        member_id = item.get("id")
        if member_id is None:
            member_id = str(uuid.uuid4())
        return str(member_id)

    @staticmethod
    def _coerce_score(item: dict[str, Any], series_kind: str) -> float:
        return coerce_series_score(item, series_kind)

    async def connect(self) -> None:
        self.client = Redis.from_url(
            self.redis_url,
            encoding="utf-8",
            decode_responses=True,
            health_check_interval=30,
        )
        self.connected = await self.ping()

    async def close(self) -> None:
        if self.client is not None:
            await self.client.aclose()
        self.client = None
        self.connected = False

    async def ping(self) -> bool:
        if self.client is None:
            self.last_ping_error = "Redis client not initialized"
            return False
        try:
            pong = await self.client.ping()
            self.connected = bool(pong)
            self.last_ping_error = None
            return self.connected
        except Exception as exc:
            self.connected = False
            self.last_ping_error = f"{type(exc).__name__}: {exc}"
            return False

    async def write_series_kind(
        self,
        series_kind: str,
        grouped: dict[Any, list[dict[str, Any]]],
    ) -> int:
        if self.client is None:
            raise RuntimeError("Redis cache backend not connected")
        pipe = self.client.pipeline(transaction=False)
        pending = 0
        count = 0

        if series_kind == "maintenance_events":
            for composite_key, rows in (grouped or {}).items():
                if isinstance(composite_key, tuple) and len(composite_key) == 2:
                    monitor_type, monitor_id = composite_key
                else:
                    monitor_type, monitor_id = str(composite_key).split(":", 1)
                for row in rows or []:
                    member_id = self._coerce_member_id(series_kind, row)
                    score = self._coerce_score(row, series_kind)
                    zkey = self._series_zkey(series_kind, str(monitor_id), monitor_type=str(monitor_type))
                    obj_key = self._series_obj_key(series_kind, str(monitor_id))
                    pipe.zadd(zkey, {member_id: score})
                    pipe.hset(obj_key, member_id, dumps(row))
                    pending += 2
                    count += 1
                    if pending >= self.warmup_batch_size:
                        await pipe.execute()
                        pending = 0
        else:
            for monitor_id, rows in (grouped or {}).items():
                monitor_id_s = str(monitor_id)
                zkey = self._series_zkey(series_kind, monitor_id_s)
                obj_key = self._series_obj_key(series_kind, monitor_id_s)
                for row in rows or []:
                    member_id = self._coerce_member_id(series_kind, row)
                    score = self._coerce_score(row, series_kind)
                    pipe.zadd(zkey, {member_id: score})
                    pipe.hset(obj_key, member_id, dumps(row))
                    pending += 2
                    count += 1
                    if pending >= self.warmup_batch_size:
                        await pipe.execute()
                        pending = 0

        if pending > 0:
            await pipe.execute()
        return count

    async def _write_warmup_meta(self, counts: dict[str, int]) -> None:
        if self.client is None:
            return
        meta = dict(counts)
        meta["total_items"] = sum(counts.values())
        pipe = self.client.pipeline(transaction=False)
        pipe.set(self._k("meta:counts"), dumps(meta))
        pipe.set(self._k("meta:loaded_at"), utcnow().isoformat())
        pipe.set(self._k("meta:version"), "1")
        await pipe.execute()

    async def warmup_from_snapshot(self, snapshot: dict[str, Any]) -> None:
        if self.client is None:
            raise RuntimeError("Redis cache backend not connected")

        pipe = self.client.pipeline(transaction=False)
        pending = 0

        # Entities & indexes: build into staging keys, then RENAME atomically.
        # This avoids the empty-hash window that caused 503s during resync.
        staging_renames: list[tuple[str, str]] = []

        empty_finals: list[str] = []

        for kind, items in (snapshot.get("entities") or {}).items():
            final_key = self._entity_key(kind)
            staging_key = final_key + ":staging"
            pipe.delete(staging_key)
            pending += 1
            has_items = False
            for entity_id, payload in (items or {}).items():
                pipe.hset(staging_key, str(entity_id), dumps(payload))
                pending += 1
                has_items = True
                if pending >= self.warmup_batch_size:
                    await pipe.execute()
                    pending = 0
            if has_items:
                staging_renames.append((staging_key, final_key))
            else:
                empty_finals.append(final_key)

        for index_name, index_items in (snapshot.get("indexes") or {}).items():
            final_key = self._index_key(index_name)
            staging_key = final_key + ":staging"
            pipe.delete(staging_key)
            pending += 1
            has_items = False
            for index_key_name, index_value in (index_items or {}).items():
                pipe.hset(staging_key, str(index_key_name), str(index_value))
                pending += 1
                has_items = True
                if pending >= self.warmup_batch_size:
                    await pipe.execute()
                    pending = 0
            if has_items:
                staging_renames.append((staging_key, final_key))
            else:
                empty_finals.append(final_key)

        # Flush remaining staging writes, then RENAME all at once.
        if pending > 0:
            await pipe.execute()
            pending = 0
        for staging_key, final_key in staging_renames:
            pipe.rename(staging_key, final_key)
            pending += 1
        # For empty kinds, just delete the final key (no staging key to rename).
        for final_key in empty_finals:
            pipe.delete(final_key)
            pending += 1
        if pending > 0:
            await pipe.execute()

        # Series: additive write via write_series_kind.
        series_items = snapshot.get("series") or {}
        series_counts: dict[str, int] = {}
        for series_kind, grouped in series_items.items():
            series_counts[series_kind] = await self.write_series_kind(series_kind, grouped)

        counts = {
            "users": len((snapshot.get("entities") or {}).get("users", {})),
            "uptime": len((snapshot.get("entities") or {}).get("uptime", {})),
            "server": len((snapshot.get("entities") or {}).get("server", {})),
            "heartbeat": len((snapshot.get("entities") or {}).get("heartbeat", {})),
            "incidents": len((snapshot.get("entities") or {}).get("incidents", {})),
            **series_counts,
        }
        await self._write_warmup_meta(counts)

    async def get_entity(self, kind: str, entity_id: str) -> dict[str, Any] | None:
        if self.client is None:
            return None
        raw = await self.client.hget(self._entity_key(kind), str(entity_id))
        loaded = loads(raw)
        return dict(loaded) if isinstance(loaded, dict) else None

    async def list_entities(self, kind: str) -> list[dict[str, Any]]:
        if self.client is None:
            return []
        rows = await self.client.hvals(self._entity_key(kind))
        items = []
        for raw in rows:
            loaded = loads(raw)
            if isinstance(loaded, dict):
                items.append(loaded)
        return items

    async def set_entity(self, kind: str, entity_id: str, value: dict[str, Any]) -> None:
        if self.client is None:
            raise RuntimeError("Redis client not connected")
        await self.client.hset(self._entity_key(kind), str(entity_id), dumps(value))

    async def delete_entity(self, kind: str, entity_id: str) -> None:
        if self.client is None:
            raise RuntimeError("Redis client not connected")
        await self.client.hdel(self._entity_key(kind), str(entity_id))

    async def get_index(self, index: str, key: str) -> str | None:
        if self.client is None:
            return None
        raw = await self.client.hget(self._index_key(index), str(key))
        return str(raw) if raw is not None else None

    async def set_index(self, index: str, key: str, value: str) -> None:
        if self.client is None:
            raise RuntimeError("Redis client not connected")
        await self.client.hset(self._index_key(index), str(key), str(value))

    async def delete_index(self, index: str, key: str) -> None:
        if self.client is None:
            raise RuntimeError("Redis client not connected")
        await self.client.hdel(self._index_key(index), str(key))

    async def append_series(
        self,
        series_kind: str,
        monitor_id: str,
        item: dict[str, Any],
        score: float,
        monitor_type: str | None = None,
    ) -> None:
        if self.client is None:
            raise RuntimeError("Redis client not connected")
        monitor_id_s = str(monitor_id)
        member_id = self._coerce_member_id(series_kind, item)
        zkey = self._series_zkey(series_kind, monitor_id_s, monitor_type=monitor_type)
        obj_key = self._series_obj_key(series_kind, monitor_id_s)
        pipe = self.client.pipeline(transaction=False)
        pipe.zadd(zkey, {member_id: float(score)})
        pipe.hset(obj_key, member_id, dumps(item))
        await pipe.execute()

    async def range_series(
        self,
        series_kind: str,
        monitor_id: str,
        start_score: float,
        end_score: float,
        limit: int | None = None,
        monitor_type: str | None = None,
    ) -> list[dict[str, Any]]:
        if self.client is None:
            return []
        monitor_id_s = str(monitor_id)
        zkey = self._series_zkey(series_kind, monitor_id_s, monitor_type=monitor_type)
        obj_key = self._series_obj_key(series_kind, monitor_id_s)
        kwargs: dict[str, Any] = {}
        if limit and limit > 0:
            kwargs["start"] = 0
            kwargs["num"] = limit
        ids = await self.client.zrangebyscore(zkey, min=start_score, max=end_score, **kwargs)
        if not ids:
            return []
        raw_rows = await self.client.hmget(obj_key, ids)
        rows: list[dict[str, Any]] = []
        for raw in raw_rows:
            payload = loads(raw)
            if isinstance(payload, dict):
                rows.append(payload)
        return rows

    async def tail_series(
        self,
        series_kind: str,
        monitor_id: str,
        count: int,
        monitor_type: str | None = None,
    ) -> list[dict[str, Any]]:
        if self.client is None:
            return []
        monitor_id_s = str(monitor_id)
        zkey = self._series_zkey(series_kind, monitor_id_s, monitor_type=monitor_type)
        obj_key = self._series_obj_key(series_kind, monitor_id_s)
        ids = await self.client.zrevrange(zkey, 0, count - 1)
        if not ids:
            return []
        raw_rows = await self.client.hmget(obj_key, ids)
        rows: list[dict[str, Any]] = []
        for raw in raw_rows:
            payload = loads(raw)
            if isinstance(payload, dict):
                rows.append(payload)
        return rows

    async def delete_series_group(
        self,
        series_kind: str,
        monitor_id: str,
        monitor_type: str | None = None,
    ) -> None:
        if self.client is None:
            return
        monitor_id_s = str(monitor_id)
        zkey = self._series_zkey(series_kind, monitor_id_s, monitor_type=monitor_type)
        obj_key = self._series_obj_key(series_kind, monitor_id_s)
        ids = await self.client.zrange(zkey, 0, -1)
        if ids:
            pipe = self.client.pipeline(transaction=False)
            pipe.delete(zkey)
            for member_id in ids:
                pipe.hdel(obj_key, member_id)
            await pipe.execute()
        else:
            await self.client.delete(zkey)

    async def delete_series_range(
        self,
        series_kind: str,
        monitor_id: str,
        max_score: float,
        monitor_type: str | None = None,
    ) -> int:
        if self.client is None:
            return 0
        monitor_id_s = str(monitor_id)
        zkey = self._series_zkey(series_kind, monitor_id_s, monitor_type=monitor_type)
        obj_key = self._series_obj_key(series_kind, monitor_id_s)
        ids = await self.client.zrangebyscore(zkey, min="-inf", max=max_score)
        if not ids:
            return 0
        pipe = self.client.pipeline(transaction=False)
        pipe.zremrangebyscore(zkey, min="-inf", max=max_score)
        for member_id in ids:
            pipe.hdel(obj_key, member_id)
        await pipe.execute()
        return len(ids)

    async def update_series_item(
        self,
        series_kind: str,
        monitor_id: str,
        item: dict[str, Any],
        score: float,
        monitor_type: str | None = None,
    ) -> None:
        if self.client is None:
            raise RuntimeError("Redis client not connected")
        monitor_id_s = str(monitor_id)
        member_id = self._coerce_member_id(series_kind, item)
        zkey = self._series_zkey(series_kind, monitor_id_s, monitor_type=monitor_type)
        obj_key = self._series_obj_key(series_kind, monitor_id_s)
        pipe = self.client.pipeline(transaction=False)
        pipe.zadd(zkey, {member_id: float(score)})
        pipe.hset(obj_key, member_id, dumps(item))
        await pipe.execute()

    async def rebuild_from_db(self, loader_fn: SnapshotLoader) -> None:
        snapshot = await loader_fn()
        await self.warmup_from_snapshot(snapshot)

    async def stats(self) -> dict[str, Any]:
        if self.client is None:
            return {
                "backend": self.backend_name,
                "connected": False,
                "healthy": False,
                "counts": {},
            }
        raw_counts = await self.client.get(self._k("meta:counts"))
        counts = loads(raw_counts) if raw_counts else {}
        loaded_at = await self.client.get(self._k("meta:loaded_at"))
        connected = bool(await self.ping())
        return {
            "backend": self.backend_name,
            "connected": connected,
            "healthy": connected,
            "loaded_at": loaded_at,
            "counts": counts if isinstance(counts, dict) else {},
        }

    async def get_json(self, key: str) -> dict[str, Any] | None:
        if self.client is None:
            return None
        raw = await self.client.get(str(key))
        payload = loads(raw)
        return payload if isinstance(payload, dict) else None

    async def set_json(
        self,
        key: str,
        payload: dict[str, Any],
        ttl_seconds: int | None = None,
    ) -> None:
        if self.client is None:
            raise RuntimeError("Redis client not connected")
        encoded = dumps(payload)
        if ttl_seconds and ttl_seconds > 0:
            await self.client.set(str(key), encoded, ex=int(ttl_seconds))
        else:
            await self.client.set(str(key), encoded)

    async def delete_key(self, key: str) -> None:
        if self.client is None:
            return
        await self.client.delete(str(key))

    async def add_set_member(self, key: str, member: str) -> None:
        if self.client is None:
            raise RuntimeError("Redis client not connected")
        await self.client.sadd(str(key), str(member))

    async def remove_set_member(self, key: str, member: str) -> None:
        if self.client is None:
            return
        await self.client.srem(str(key), str(member))

    async def get_set_members(self, key: str) -> set[str]:
        if self.client is None:
            return set()
        members = await self.client.smembers(str(key))
        return {str(m) for m in members}
