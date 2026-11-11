"""Per-host connection limiting and the shared backend pool."""

from __future__ import annotations

import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .backends.base import DownloadBackend
from .backends.registry import BackendRegistry
from .log import get_logger
from .request import RequestConfig
from .util import url_host


class HostLimiter:
    """Cap concurrent requests per host (``--max-per-host``)."""

    def __init__(self, per_host: int = 6) -> None:
        self.per_host = max(0, int(per_host))
        self._semaphores: dict[str, threading.Semaphore] = {}
        self._lock = threading.Lock()
        self.log = get_logger("net")

    def _semaphore(self, host: str) -> threading.Semaphore:
        with self._lock:
            semaphore = self._semaphores.get(host)
            if semaphore is None:
                semaphore = threading.Semaphore(self.per_host)
                self._semaphores[host] = semaphore
            return semaphore

    @contextmanager
    def slot(self, url: str) -> Iterator[None]:
        if self.per_host <= 0:
            yield
            return
        semaphore = self._semaphore(url_host(url) or "-")
        semaphore.acquire()
        try:
            yield
        finally:
            semaphore.release()


class PathLocks:
    """Serialise downloads that target the same output path.

    In a parallel batch two URLs can resolve to the same filename (mirrors,
    ``{filename}`` templates, redirects). Without this guard they would fight
    over one ``.part`` file and corrupt each other.
    """

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    def _lock_for(self, path: Path) -> threading.Lock:
        try:
            key = str(path.resolve())
        except OSError:  # pragma: no cover - exotic filesystems
            key = str(path)
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock

    @contextmanager
    def hold(self, path: Path) -> Iterator[None]:
        lock = self._lock_for(path)
        lock.acquire()
        try:
            yield
        finally:
            lock.release()


class BackendPool:
    """Creates one backend instance per backend name and shares it.

    Sharing matters for pooling: the httpx/httpx_async clients keep their
    connection pools (and one event loop, for the async backend) for the whole
    run instead of per range.
    """

    def __init__(
        self,
        registry: BackendRegistry,
        config: RequestConfig,
        *,
        preferred: str = "auto",
        extra: Mapping[str, Any] | None = None,
        fallback: bool = False,
    ) -> None:
        self.registry = registry
        self.config = config
        self.preferred = preferred
        self.extra = dict(extra or {})
        self.fallback = fallback
        self._backends: dict[str, DownloadBackend] = {}
        self._lock = threading.Lock()
        self.log = get_logger("pool")

    def order(self, preferred: str | None = None) -> list[str]:
        name = preferred or self.preferred
        if self.fallback or name == "auto":
            chain = self.registry.fallback_chain(name)
        else:
            chain = [self.registry.canonical(name)]
        if not chain:
            self.registry.resolve_auto()
            chain = self.registry.fallback_chain(name)
        return chain

    def acquire(self, name: str) -> DownloadBackend:
        canonical = self.registry.canonical(name)
        if canonical == "auto":
            canonical = self.registry.resolve_auto()
        with self._lock:
            backend = self._backends.get(canonical)
            if backend is None:
                backend = self.registry.create(canonical, self.config, self.extra)
                self._backends[canonical] = backend
                self.log.debug("backend created", backend=canonical)
            return backend

    def primary(self) -> DownloadBackend:
        return self.acquire(self.order()[0])

    def close(self) -> None:
        with self._lock:
            backends = list(self._backends.values())
            self._backends.clear()
        for backend in backends:
            try:
                backend.close()
            except Exception as exc:  # pragma: no cover - best effort
                self.log.debug("backend close failed", backend=backend.name, error=str(exc))
