# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

from collections import defaultdict
from typing import Any

from .base import CacheBackend, SnapshotLoader


class InMemoryCacheBackend(CacheBackend):
    backend_name = "inmemory"

    def __init__(self) -> None:
        self.connected = False
        self.entities: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        self.indexes: dict[str, dict[str, str]] = defaultdict(dict)
        self.series: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
        self.meta: dict[str, dict[str, Any]] = {}
        self.sets: dict[str, set[str]] = defaultdict(set)

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    async def ping(self) -> bool:
        return self.connected

    async def warmup_from_snapshot(self, snapshot: dict[str, Any]) -> None:
        self.entities.clear()
        self.indexes.clear()
        self.series.clear()
        self.meta.clear()
        self.sets.clear()

        for kind, items in (snapshot.get("entities") or {}).items():
            self.entities[kind] = {str(k): dict(v) for k, v in (items or {}).items()}

        for idx, items in (snapshot.get("indexes") or {}).items():
            self.indexes[idx] = {str(k): str(v) for k, v in (items or {}).items()}

        for series_kind, groups in (snapshot.get("series") or {}).items():
            self.series[series_kind] = {
                str(k): [dict(row) for row in (rows or [])]
                for k, rows in (groups or {}).items()
            }

    async def get_entity(self, kind: str, entity_id: str) -> dict[str, Any] | None:
        item = self.entities.get(kind, {}).get(str(entity_id))
        return dict(item) if item else None

    async def list_entities(self, kind: str) -> list[dict[str, Any]]:
        return [dict(v) for v in self.entities.get(kind, {}).values()]

    async def set_entity(self, kind: str, entity_id: str, value: dict[str, Any]) -> None:
        self.entities[kind][str(entity_id)] = dict(value)

    async def delete_entity(self, kind: str, entity_id: str) -> None:
        self.entities.get(kind, {}).pop(str(entity_id), None)

    async def get_index(self, index: str, key: str) -> str | None:
        value = self.indexes.get(index, {}).get(str(key))
        return str(value) if value is not None else None

    async def set_index(self, index: str, key: str, value: str) -> None:
        self.indexes[index][str(key)] = str(value)

    async def delete_index(self, index: str, key: str) -> None:
        self.indexes.get(index, {}).pop(str(key), None)

    async def append_series(
        self,
        series_kind: str,
        monitor_id: str,
        item: dict[str, Any],
        score: float,
        monitor_type: str | None = None,
    ) -> None:
        _ = score
        series_key = str(monitor_id)
        if series_kind == "maintenance_events" and monitor_type:
            series_key = f"{monitor_type}:{monitor_id}"
        self.series[series_kind][series_key].append(dict(item))

    async def range_series(
        self,
        series_kind: str,
        monitor_id: str,
        start_score: float,
        end_score: float,
        limit: int | None = None,
        monitor_type: str | None = None,
    ) -> list[dict[str, Any]]:
        _ = (start_score, end_score)
        series_key = str(monitor_id)
        if series_kind == "maintenance_events" and monitor_type:
            series_key = f"{monitor_type}:{monitor_id}"
        rows = [dict(v) for v in self.series.get(series_kind, {}).get(series_key, [])]
        if limit is not None and limit > 0:
            return rows[-limit:]
        return rows

    async def rebuild_from_db(self, loader_fn: SnapshotLoader) -> None:
        snapshot = await loader_fn()
        await self.warmup_from_snapshot(snapshot)

    async def stats(self) -> dict[str, Any]:
        total_entities = sum(len(v) for v in self.entities.values())
        total_indexes = sum(len(v) for v in self.indexes.values())
        total_series = 0
        for groups in self.series.values():
            total_series += sum(len(rows) for rows in groups.values())
        return {
            "backend": self.backend_name,
            "connected": self.connected,
            "healthy": self.connected,
            "counts": {
                "entity_items": total_entities,
                "index_items": total_indexes,
                "series_items": total_series,
                "total_items": total_entities + total_indexes + total_series,
            },
        }

    async def get_json(self, key: str) -> dict[str, Any] | None:
        value = self.meta.get(str(key))
        return dict(value) if value is not None else None

    async def set_json(
        self,
        key: str,
        payload: dict[str, Any],
        ttl_seconds: int | None = None,
    ) -> None:
        _ = ttl_seconds
        self.meta[str(key)] = dict(payload)

    async def delete_key(self, key: str) -> None:
        self.meta.pop(str(key), None)
        self.sets.pop(str(key), None)

    async def add_set_member(self, key: str, member: str) -> None:
        self.sets[str(key)].add(str(member))

    async def remove_set_member(self, key: str, member: str) -> None:
        self.sets[str(key)].discard(str(member))

    async def get_set_members(self, key: str) -> set[str]:
        return set(self.sets.get(str(key), set()))
