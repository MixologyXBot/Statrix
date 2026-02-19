# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import uuid
from datetime import datetime
from typing import Any

from redis.asyncio import Redis

from ..utils.time import utcnow
from .base import CacheBackend, SnapshotLoader
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
        if series_kind == "uptime_checks":
            ts = item.get("checked_at")
        elif series_kind == "server_history":
            ts = item.get("timestamp")
        elif series_kind == "heartbeat_pings":
            ts = item.get("pinged_at")
        elif series_kind == "monitor_minutes":
            ts = item.get("minute")
        elif series_kind == "maintenance_events":
            ts = item.get("start_at")
        else:
            ts = None
        if isinstance(ts, datetime):
            return ts.timestamp()
        if isinstance(ts, (int, float)):
            return float(ts)
        if isinstance(ts, str):
            try:
                return datetime.fromisoformat(ts).timestamp()
            except Exception:
                return 0.0
        return 0.0

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

    async def _clear_prefix(self) -> None:
        if self.client is None:
            return
        cursor = 0
        pattern = self._k("*")
        while True:
            cursor, keys = await self.client.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                await self.client.delete(*keys)
            if cursor == 0:
                break

    async def warmup_from_snapshot(self, snapshot: dict[str, Any]) -> None:
        if self.client is None:
            raise RuntimeError("Redis cache backend not connected")

        await self._clear_prefix()

        pipe = self.client.pipeline(transaction=False)
        pending = 0

        for kind, items in (snapshot.get("entities") or {}).items():
            entity_key = self._entity_key(kind)
            for entity_id, payload in (items or {}).items():
                pipe.hset(entity_key, str(entity_id), dumps(payload))
                pending += 1
                if pending >= self.warmup_batch_size:
                    await pipe.execute()
                    pending = 0

        for index_name, index_items in (snapshot.get("indexes") or {}).items():
            index_key = self._index_key(index_name)
            for index_key_name, index_value in (index_items or {}).items():
                pipe.hset(index_key, str(index_key_name), str(index_value))
                pending += 1
                if pending >= self.warmup_batch_size:
                    await pipe.execute()
                    pending = 0

        series_items = snapshot.get("series") or {}
        for series_kind, grouped in series_items.items():
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
                        if pending >= self.warmup_batch_size:
                            await pipe.execute()
                            pending = 0
                continue

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
                    if pending >= self.warmup_batch_size:
                        await pipe.execute()
                        pending = 0

        counts = {
            "users": len((snapshot.get("entities") or {}).get("users", {})),
            "uptime": len((snapshot.get("entities") or {}).get("uptime", {})),
            "server": len((snapshot.get("entities") or {}).get("server", {})),
            "heartbeat": len((snapshot.get("entities") or {}).get("heartbeat", {})),
            "incidents": len((snapshot.get("entities") or {}).get("incidents", {})),
            "uptime_checks": sum(len(v) for v in (series_items.get("uptime_checks") or {}).values()),
            "server_history": sum(len(v) for v in (series_items.get("server_history") or {}).values()),
            "heartbeat_pings": sum(len(v) for v in (series_items.get("heartbeat_pings") or {}).values()),
            "maintenance_events": sum(len(v) for v in (series_items.get("maintenance_events") or {}).values()),
            "monitor_minutes": sum(len(v) for v in (series_items.get("monitor_minutes") or {}).values()),
        }
        counts["total_items"] = sum(counts.values())
        meta_counts_key = self._k("meta:counts")
        meta_loaded_key = self._k("meta:loaded_at")
        meta_version_key = self._k("meta:version")
        pipe.set(meta_counts_key, dumps(counts))
        pipe.set(meta_loaded_key, utcnow().isoformat())
        pipe.set(meta_version_key, "1")
        pending += 3

        if pending > 0:
            await pipe.execute()

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
