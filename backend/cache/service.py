# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

import asyncio
import logging
import os
from datetime import datetime
from urllib.parse import urlparse
from typing import Any

from ..config import settings
from ..utils.time import utcnow
from .base import CacheBackend, CacheUnavailableError, SnapshotLoader
from .inmemory_backend import InMemoryCacheBackend
from .redis_backend import RedisCacheBackend

logger = logging.getLogger(__name__)


class CacheService:

    def __init__(self) -> None:
        backend_name = str(getattr(settings, "CACHE_BACKEND", "redis") or "redis").strip().lower()
        self.key_prefix = str(getattr(settings, "CACHE_KEY_PREFIX", "statrix:v1") or "statrix:v1")
        self.fail_fast = bool(getattr(settings, "CACHE_FAIL_FAST", True))
        self.warmup_full = bool(getattr(settings, "CACHE_WARMUP_FULL", True))
        self.cache_backend_name = backend_name
        self.connected = False
        self.healthy = False
        self.last_error: str | None = None
        self.loaded_at: str | None = None
        self._last_ping_ok_at: datetime | None = None
        self._ping_check_interval_seconds = 5

        if backend_name == "redis":
            redis_url = str(getattr(settings, "REDIS_URL", "") or "").strip()
            if not redis_url:
                # Compatibility fallbacks for common Upstash env var names.
                redis_url = str(
                    os.getenv("UPSTASH_REDIS_TLS_URL")
                    or os.getenv("UPSTASH_REDIS_URL")
                    or ""
                ).strip()
            if not redis_url:
                raise RuntimeError("CACHE_BACKEND=redis requires REDIS_URL")
            parsed = urlparse(redis_url)
            if parsed.scheme in {"http", "https"}:
                raise RuntimeError(
                    "REDIS_URL must be Redis protocol (redis:// or rediss://), "
                    "not HTTP REST URL. Use Upstash TLS endpoint."
                )
            if parsed.scheme not in {"redis", "rediss"}:
                raise RuntimeError(
                    f"Unsupported REDIS_URL scheme '{parsed.scheme}'. "
                    "Expected redis:// or rediss://"
                )
            self.backend: CacheBackend = RedisCacheBackend(
                redis_url=redis_url,
                key_prefix=self.key_prefix,
                warmup_batch_size=int(getattr(settings, "CACHE_WARMUP_BATCH_SIZE", 500) or 500),
            )
        else:
            self.backend = InMemoryCacheBackend()

    def _k(self, suffix: str) -> str:
        return f"{self.key_prefix}:{suffix}"

    def _ping_error_detail(self) -> str | None:
        if isinstance(self.backend, RedisCacheBackend):
            return self.backend.last_ping_error
        return None

    async def connect(self) -> None:
        await self.backend.connect()
        self.connected = await self.backend.ping()
        if not self.connected:
            self.healthy = False
            details = None
            if isinstance(self.backend, RedisCacheBackend):
                details = self.backend.last_ping_error
            self.last_error = "Cache ping failed" + (f": {details}" if details else "")
            if self.fail_fast:
                raise RuntimeError(self.last_error)
        else:
            self.healthy = True
            self.last_error = None
            self._last_ping_ok_at = utcnow()

    async def close(self) -> None:
        await self.backend.close()
        self.connected = False
        self.healthy = False

    async def warmup_from_loader(self, loader_fn: SnapshotLoader) -> None:
        try:
            if self.warmup_full:
                await self.backend.rebuild_from_db(loader_fn)
            self.loaded_at = utcnow().isoformat()
            self.healthy = True
            self.last_error = None
            await self.backend.set_json(
                self._k("meta:healthy"),
                {"healthy": True, "updated_at": self.loaded_at},
            )
            await self.backend.set_json(
                self._k("meta:loaded_at"),
                {"loaded_at": self.loaded_at},
            )
            await self.backend.delete_key(self._k("meta:last_error"))
        except Exception as exc:
            await self.mark_unhealthy(str(exc))
            raise

    async def mark_unhealthy(self, error: str) -> None:
        self.healthy = False
        self.last_error = str(error)
        self._last_ping_ok_at = None
        payload = {
            "healthy": False,
            "updated_at": utcnow().isoformat(),
        }
        try:
            await self.backend.set_json(self._k("meta:healthy"), payload)
            await self.backend.set_json(
                self._k("meta:last_error"),
                {"error": self.last_error, "updated_at": payload["updated_at"]},
            )
        except Exception:
            logger.exception("Failed to persist unhealthy cache metadata")

    async def mark_healthy(self) -> None:
        self.connected = True
        self.healthy = True
        self.last_error = None
        now = utcnow().isoformat()
        self.loaded_at = self.loaded_at or now
        self._last_ping_ok_at = utcnow()
        try:
            await self.backend.set_json(self._k("meta:healthy"), {"healthy": True, "updated_at": now})
            await self.backend.delete_key(self._k("meta:last_error"))
        except Exception:
            logger.exception("Failed to persist healthy cache metadata")

    async def ensure_available(self) -> None:
        if not self.fail_fast:
            return

        now = utcnow()
        if (
            self.connected
            and self.healthy
            and self._last_ping_ok_at
            and (now - self._last_ping_ok_at).total_seconds() < self._ping_check_interval_seconds
        ):
            return

        ok = await self.backend.ping()
        if not ok:
            # One quick retry to avoid flapping unhealthy on transient network blips.
            await asyncio.sleep(0.15)
            ok = await self.backend.ping()

        if ok:
            self.connected = True
            self._last_ping_ok_at = utcnow()
            if not self.healthy:
                await self.mark_healthy()
            return

        detail = self._ping_error_detail()
        error = "Cache ping failed" + (f": {detail}" if detail else "")
        self.connected = False
        await self.mark_unhealthy(error)
        raise CacheUnavailableError(error)

    async def stats(self) -> dict[str, Any]:
        backend_stats = await self.backend.stats()
        return {
            "backend": self.cache_backend_name,
            "connected": self.connected,
            "healthy": self.healthy,
            "last_error": self.last_error,
            "loaded_at": self.loaded_at,
            "counts": backend_stats.get("counts", {}),
        }

    def _status_live_key(self, cache_key: str) -> str:
        return self._k(f"status:live:{cache_key}")

    def _status_stale_key(self, cache_key: str) -> str:
        return self._k(f"status:stale:{cache_key}")

    def _status_keys_set(self) -> str:
        return self._k("status:keys")

    async def get_status_live(self, cache_key: str) -> dict[str, Any] | None:
        await self.ensure_available()
        return await self.backend.get_json(self._status_live_key(cache_key))

    async def set_status_live(self, cache_key: str, payload: dict[str, Any], ttl_seconds: int) -> None:
        await self.ensure_available()
        live_key = self._status_live_key(cache_key)
        stale_key = self._status_stale_key(cache_key)
        await self.backend.set_json(live_key, payload, ttl_seconds=max(0, int(ttl_seconds or 0)) or None)
        await self.backend.set_json(stale_key, payload)
        await self.backend.add_set_member(self._status_keys_set(), cache_key)

    async def get_status_stale(self, cache_key: str) -> dict[str, Any] | None:
        await self.ensure_available()
        return await self.backend.get_json(self._status_stale_key(cache_key))

    async def invalidate_status_cache(self) -> None:
        await self.ensure_available()
        set_key = self._status_keys_set()
        keys = await self.backend.get_set_members(set_key)
        for cache_key in keys:
            # Keep stale snapshots so public status can serve a fallback while
            # a fresh payload is being rebuilt.
            await self.backend.delete_key(self._status_live_key(cache_key))
            await self.backend.remove_set_member(set_key, cache_key)

    async def get_prefixed_json(self, suffix: str) -> dict[str, Any] | None:
        await self.ensure_available()
        return await self.backend.get_json(self._k(suffix))

    async def set_prefixed_json(
        self,
        suffix: str,
        payload: dict[str, Any],
        ttl_seconds: int | None = None,
    ) -> None:
        await self.ensure_available()
        await self.backend.set_json(self._k(suffix), payload, ttl_seconds=ttl_seconds)

    async def delete_prefixed_key(self, suffix: str) -> None:
        await self.ensure_available()
        await self.backend.delete_key(self._k(suffix))

    async def add_prefixed_set_member(self, suffix: str, member: str) -> None:
        await self.ensure_available()
        await self.backend.add_set_member(self._k(suffix), str(member))

    async def remove_prefixed_set_member(self, suffix: str, member: str) -> None:
        await self.ensure_available()
        await self.backend.remove_set_member(self._k(suffix), str(member))

    async def get_prefixed_set_members(self, suffix: str) -> set[str]:
        await self.ensure_available()
        return await self.backend.get_set_members(self._k(suffix))

    # ── Staged warmup helpers ─────────────────────────────────────────

    async def write_series_kind(
        self,
        series_kind: str,
        grouped: dict,
    ) -> int:
        await self.ensure_available()
        return await self.backend.write_series_kind(series_kind, grouped)

    async def write_warmup_meta(self, counts: dict[str, int]) -> None:
        if isinstance(self.backend, RedisCacheBackend):
            await self.backend._write_warmup_meta(counts)

    # ── Entity operations ──────────────────────────────────────────────

    async def get_entity(self, kind: str, entity_id: str) -> dict[str, Any] | None:
        await self.ensure_available()
        return await self.backend.get_entity(kind, str(entity_id))

    async def list_entities(self, kind: str) -> list[dict[str, Any]]:
        await self.ensure_available()
        return await self.backend.list_entities(kind)

    async def set_entity(self, kind: str, entity_id: str, value: dict[str, Any]) -> None:
        await self.ensure_available()
        await self.backend.set_entity(kind, str(entity_id), value)

    async def delete_entity(self, kind: str, entity_id: str) -> None:
        await self.ensure_available()
        await self.backend.delete_entity(kind, str(entity_id))

    # ── Index operations ──────────────────────────────────────────────

    async def get_index(self, index: str, key: str) -> str | None:
        await self.ensure_available()
        return await self.backend.get_index(index, str(key))

    async def set_index(self, index: str, key: str, value: str) -> None:
        await self.ensure_available()
        await self.backend.set_index(index, str(key), str(value))

    async def delete_index(self, index: str, key: str) -> None:
        await self.ensure_available()
        await self.backend.delete_index(index, str(key))

    # ── Series operations ─────────────────────────────────────────────

    async def append_series(
        self,
        series_kind: str,
        monitor_id: str,
        item: dict[str, Any],
        score: float,
        monitor_type: str | None = None,
    ) -> None:
        await self.ensure_available()
        await self.backend.append_series(series_kind, str(monitor_id), item, score, monitor_type=monitor_type)

    async def range_series(
        self,
        series_kind: str,
        monitor_id: str,
        start_score: float,
        end_score: float,
        limit: int | None = None,
        monitor_type: str | None = None,
    ) -> list[dict[str, Any]]:
        await self.ensure_available()
        return await self.backend.range_series(series_kind, str(monitor_id), start_score, end_score, limit=limit, monitor_type=monitor_type)

    async def tail_series(
        self,
        series_kind: str,
        monitor_id: str,
        count: int,
        monitor_type: str | None = None,
    ) -> list[dict[str, Any]]:
        await self.ensure_available()
        return await self.backend.tail_series(series_kind, str(monitor_id), count, monitor_type=monitor_type)

    async def delete_series_group(
        self,
        series_kind: str,
        monitor_id: str,
        monitor_type: str | None = None,
    ) -> None:
        await self.ensure_available()
        await self.backend.delete_series_group(series_kind, str(monitor_id), monitor_type=monitor_type)

    async def delete_series_range(
        self,
        series_kind: str,
        monitor_id: str,
        max_score: float,
        monitor_type: str | None = None,
    ) -> int:
        await self.ensure_available()
        return await self.backend.delete_series_range(series_kind, str(monitor_id), max_score, monitor_type=monitor_type)

    async def update_series_item(
        self,
        series_kind: str,
        monitor_id: str,
        item: dict[str, Any],
        score: float,
        monitor_type: str | None = None,
    ) -> None:
        await self.ensure_available()
        await self.backend.update_series_item(series_kind, str(monitor_id), item, score, monitor_type=monitor_type)

    # ── Leader lock ───────────────────────────────────────────────────

    async def try_acquire_leader_lock(self, lock_name: str, owner: str, ttl_seconds: int) -> bool:
        # In-memory backend: allow single process to proceed.
        if not isinstance(self.backend, RedisCacheBackend):
            return True
        await self.ensure_available()
        if self.backend.client is None:
            return False
        lock_key = self._k(f"lock:{lock_name}")
        acquired = await self.backend.client.set(lock_key, owner, nx=True, ex=max(1, int(ttl_seconds or 1)))
        return bool(acquired)

    async def release_leader_lock(self, lock_name: str, owner: str) -> None:
        if not isinstance(self.backend, RedisCacheBackend):
            return
        if self.backend.client is None:
            return
        lock_key = self._k(f"lock:{lock_name}")
        current = await self.backend.client.get(lock_key)
        if current == owner:
            await self.backend.client.delete(lock_key)
