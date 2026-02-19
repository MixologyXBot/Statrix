# This file is a part of Statrix
# Coding : Priyanshu Dey [@HellFireDevil18]

from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable


SnapshotLoader = Callable[[], Awaitable[dict[str, Any]]]


class CacheUnavailableError(RuntimeError):
    """Raised when cache access is required but unavailable."""


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
