# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Awaitable, Callable


SnapshotLoader = Callable[[], Awaitable[dict[str, Any]]]


class CacheUnavailableError(RuntimeError):
    """Raised when cache access is required but unavailable."""


def coerce_series_score(item: dict[str, Any], series_kind: str) -> float:
    """Extract a float timestamp score from a series item based on its kind."""
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
    elif series_kind in (
        "server_history_daily", "uptime_checks_daily",
        "heartbeat_pings_daily", "monitor_minutes_daily",
    ):
        ts = item.get("date")
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


class CacheBackend(ABC):
    """Abstract interface implemented by concrete cache backends."""

    backend_name: str = "unknown"

    @abstractmethod
    async def connect(self) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...

    @abstractmethod
    async def ping(self) -> bool:
        ...

    @abstractmethod
    async def warmup_from_snapshot(self, snapshot: dict[str, Any]) -> None:
        ...

    @abstractmethod
    async def get_entity(self, kind: str, entity_id: str) -> dict[str, Any] | None:
        ...

    @abstractmethod
    async def list_entities(self, kind: str) -> list[dict[str, Any]]:
        ...

    @abstractmethod
    async def set_entity(self, kind: str, entity_id: str, value: dict[str, Any]) -> None:
        ...

    @abstractmethod
    async def delete_entity(self, kind: str, entity_id: str) -> None:
        ...

    @abstractmethod
    async def get_index(self, index: str, key: str) -> str | None:
        ...

    @abstractmethod
    async def set_index(self, index: str, key: str, value: str) -> None:
        ...

    @abstractmethod
    async def delete_index(self, index: str, key: str) -> None:
        ...

    @abstractmethod
    async def append_series(
        self,
        series_kind: str,
        monitor_id: str,
        item: dict[str, Any],
        score: float,
        monitor_type: str | None = None,
    ) -> None:
        ...

    @abstractmethod
    async def range_series(
        self,
        series_kind: str,
        monitor_id: str,
        start_score: float,
        end_score: float,
        limit: int | None = None,
        monitor_type: str | None = None,
    ) -> list[dict[str, Any]]:
        ...

    @abstractmethod
    async def tail_series(
        self,
        series_kind: str,
        monitor_id: str,
        count: int,
        monitor_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return the most recent *count* items (highest score first)."""
        ...

    @abstractmethod
    async def delete_series_group(
        self,
        series_kind: str,
        monitor_id: str,
        monitor_type: str | None = None,
    ) -> None:
        """Delete all series items for a given monitor."""
        ...

    @abstractmethod
    async def delete_series_range(
        self,
        series_kind: str,
        monitor_id: str,
        max_score: float,
        monitor_type: str | None = None,
    ) -> int:
        """Delete series items with score <= max_score. Return count deleted."""
        ...

    @abstractmethod
    async def update_series_item(
        self,
        series_kind: str,
        monitor_id: str,
        item: dict[str, Any],
        score: float,
        monitor_type: str | None = None,
    ) -> None:
        """Insert or update a series item by its member id."""
        ...

    @abstractmethod
    async def write_series_kind(
        self,
        series_kind: str,
        grouped: dict[Any, list[dict[str, Any]]],
    ) -> int:
        """Write a batch of series data for one kind. Returns item count."""
        ...

    @abstractmethod
    async def rebuild_from_db(self, loader_fn: SnapshotLoader) -> None:
        ...

    @abstractmethod
    async def stats(self) -> dict[str, Any]:
        ...

    @abstractmethod
    async def get_json(self, key: str) -> dict[str, Any] | None:
        ...

    @abstractmethod
    async def set_json(
        self,
        key: str,
        payload: dict[str, Any],
        ttl_seconds: int | None = None,
    ) -> None:
        ...

    @abstractmethod
    async def delete_key(self, key: str) -> None:
        ...

    @abstractmethod
    async def add_set_member(self, key: str, member: str) -> None:
        ...

    @abstractmethod
    async def remove_set_member(self, key: str, member: str) -> None:
        ...

    @abstractmethod
    async def get_set_members(self, key: str) -> set[str]:
        ...
