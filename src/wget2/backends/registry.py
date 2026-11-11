"""Backend registry with importlib.metadata plugin discovery."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from importlib import metadata
from typing import Any

from ..errors import BackendUnavailableError, ConfigError
from ..log import get_logger
from ..request import RequestConfig
from .base import DownloadBackend

ENTRY_POINT_GROUP = "wget2.backends"

#: order used by ``--backend auto`` and by backend failover
AUTO_ORDER: tuple[str, ...] = ("pycurl", "httpx", "requests", "httpx_async")

ALIASES: dict[str, str] = {
    "async": "httpx_async",
    "aio": "httpx_async",
    "httpx-async": "httpx_async",
    "curl": "pycurl",
}


def _builtin_backends() -> dict[str, type[DownloadBackend]]:
    from .httpx_async_backend import HttpxAsyncBackend
    from .httpx_backend import HttpxBackend
    from .pycurl_backend import PycurlBackend
    from .requests_backend import RequestsBackend

    return {
        RequestsBackend.name: RequestsBackend,
        PycurlBackend.name: PycurlBackend,
        HttpxBackend.name: HttpxBackend,
        HttpxAsyncBackend.name: HttpxAsyncBackend,
    }


@dataclass(frozen=True)
class BackendInfo:
    name: str
    backend: type[DownloadBackend]
    source: str
    available: bool
    reason: str | None = None

    @property
    def status(self) -> str:
        return "ready" if self.available else f"unavailable ({self.reason})"


class BackendRegistry:
    """Discovers built-in backends and ``wget2.backends`` entry points."""

    def __init__(self) -> None:
        self._backends: dict[str, type[DownloadBackend]] = _builtin_backends()
        self._sources: dict[str, str] = dict.fromkeys(self._backends, "builtin")
        self._plugins_loaded = False
        self.log = get_logger("backends")

    # -- discovery -------------------------------------------------------- #
    def load_plugins(self) -> dict[str, str]:
        """Load third-party backends once. Returns ``name -> distribution``."""
        found: dict[str, str] = {}
        if self._plugins_loaded:
            return found
        self._plugins_loaded = True
        try:
            entry_points = metadata.entry_points(group=ENTRY_POINT_GROUP)
        except Exception as exc:  # pragma: no cover - defensive
            self.log.debug("entry point discovery failed", error=str(exc))
            return found
        for entry_point in entry_points:
            try:
                loaded = entry_point.load()
            except Exception as exc:
                self.log.warning(
                    "plugin backend failed to import",
                    plugin=entry_point.name,
                    error=str(exc),
                )
                continue
            backend_cls = loaded
            if not (isinstance(backend_cls, type) and issubclass(backend_cls, DownloadBackend)):
                factory = getattr(loaded, "BACKEND", None)
                backend_cls = factory
            if not (isinstance(backend_cls, type) and issubclass(backend_cls, DownloadBackend)):
                self.log.warning("plugin is not a DownloadBackend", plugin=entry_point.name)
                continue
            name = getattr(backend_cls, "name", None) or entry_point.name
            dist = getattr(getattr(entry_point, "dist", None), "name", None) or entry_point.value
            self._backends[name] = backend_cls
            self._sources[name] = f"plugin:{dist}"
            found[name] = str(dist)
            self.log.debug("plugin backend registered", backend=name, source=str(dist))
        return found

    # -- access ----------------------------------------------------------- #
    def names(self) -> list[str]:
        return sorted(self._backends)

    def canonical(self, name: str) -> str:
        key = (name or "").strip().lower()
        key = ALIASES.get(key, key)
        if key == "auto":
            return "auto"
        if key not in self._backends:
            self.load_plugins()
        if key not in self._backends:
            raise ConfigError(f"unknown backend '{name}'. Available: {', '.join(self.names())}, auto")
        return key

    def get(self, name: str) -> type[DownloadBackend]:
        canonical = self.canonical(name)
        if canonical == "auto":
            canonical = self.resolve_auto()
        return self._backends[canonical]

    def is_available(self, name: str) -> bool:
        try:
            backend_cls = self._backends.get(self.canonical(name))
        except ConfigError:
            return False
        return bool(backend_cls and backend_cls.available())

    def resolve_auto(self) -> str:
        """First available backend in :data:`AUTO_ORDER`."""
        self.load_plugins()
        for candidate in AUTO_ORDER:
            backend_cls = self._backends.get(candidate)
            if backend_cls is not None and backend_cls.available():
                return candidate
        for name, backend_cls in sorted(self._backends.items()):
            if backend_cls.available():
                return name
        raise BackendUnavailableError("no usable HTTP backend found; install one of: pycurl, httpx, requests")

    def fallback_chain(self, preferred: str) -> list[str]:
        """Candidate backends to try, best first (for ``--backend auto``)."""
        chain: list[str] = []
        if preferred != "auto":
            chain.append(self.canonical(preferred))
        for candidate in AUTO_ORDER:
            backend_cls = self._backends.get(candidate)
            if backend_cls is not None and backend_cls.available() and candidate not in chain:
                chain.append(candidate)
        return chain

    def create(
        self,
        name: str,
        config: RequestConfig,
        extra: Mapping[str, Any] | None = None,
    ) -> DownloadBackend:
        backend_cls = self.get(name)
        if not backend_cls.available():
            raise BackendUnavailableError(
                f"backend '{name}' is unavailable: {backend_cls.availability_reason()}"
            )
        return backend_cls(config, dict(extra or {}))

    def describe(self) -> list[BackendInfo]:
        self.load_plugins()
        return [
            BackendInfo(
                name=name,
                backend=backend_cls,
                source=self._sources.get(name, "unknown"),
                available=backend_cls.available(),
                reason=backend_cls.availability_reason(),
            )
            for name, backend_cls in sorted(self._backends.items())
        ]


REGISTRY = BackendRegistry()


def parse_backend_args(values: list[str] | None) -> dict[str, Any]:
    """Parse repeatable ``--backend-args key=value`` options."""
    out: dict[str, Any] = {}
    for raw in values or ():
        for item in str(raw).split(","):
            item = item.strip()
            if not item:
                continue
            key, sep, value = item.partition("=")
            if not sep:
                raise ConfigError(f"invalid --backend-args item (expected key=value): {item!r}")
            out[key.strip()] = _coerce(value.strip())
    return out


def _coerce(value: str) -> Any:
    lowered = value.lower()
    if lowered in {"true", "yes", "on"}:
        return True
    if lowered in {"false", "no", "off"}:
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value
