"""Backend contract shared by every HTTP implementation."""

from __future__ import annotations

import importlib.util
import re
import threading
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from ..errors import HTTPStatusError, RangeNotSupportedError
from ..log import get_logger
from ..request import RequestConfig
from ..util import content_disposition_filename, parse_http_date, redact_url, url_basename

Sink = Callable[[bytes], object]

_CONTENT_RANGE_RE = re.compile(r"bytes\s+(\d+)-(\d+)/(\d+|\*)", re.IGNORECASE)


class StopStream(Exception):
    """Raised from a sink to abort an in-flight response early.

    Backends must catch it and return the :class:`ResponseInfo` as usual.
    Used by :meth:`DownloadBackend.probe` to avoid downloading whole bodies
    just to discover a length.
    """


def normalize_headers(items: Iterable[tuple[str, str]]) -> dict[str, str]:
    """Lower-case header names, joining duplicates with a newline."""
    out: dict[str, str] = {}
    for name, value in items:
        key = name.strip().lower()
        if not key:
            continue
        if key in out:
            out[key] = f"{out[key]}\n{value}"
        else:
            out[key] = value
    return out


@dataclass(frozen=True)
class ResponseInfo:
    status: int
    url: str
    headers: Mapping[str, str] = field(default_factory=dict)

    def header(self, name: str, default: str | None = None) -> str | None:
        return self.headers.get(name.lower(), default)

    def int_header(self, name: str) -> int | None:
        value = self.header(name)
        if value is None:
            return None
        try:
            return int(value.split(",")[0].strip())
        except ValueError:
            return None


@dataclass(frozen=True)
class ProbeResult:
    url: str
    final_url: str
    status: int
    size: int | None = None
    etag: str | None = None
    last_modified: str | None = None
    accept_ranges: bool = False
    suggested_filename: str | None = None
    content_type: str | None = None
    headers: Mapping[str, str] = field(default_factory=dict)

    def header(self, name: str, default: str | None = None) -> str | None:
        return self.headers.get(name.lower(), default)

    @property
    def last_modified_ts(self) -> float | None:
        return parse_http_date(self.last_modified)


def parse_content_range(value: str | None) -> tuple[int, int, int | None] | None:
    if not value:
        return None
    match = _CONTENT_RANGE_RE.search(value)
    if not match:
        return None
    start, end, total = match.groups()
    return int(start), int(end), (int(total) if total != "*" else None)


class DownloadBackend(ABC):
    """Abstract backend.

    Backends implement exactly one primitive: :meth:`_request`, which performs
    a single HTTP request while streaming the body into ``sink``. Everything
    else (probing, range headers, HTTP error mapping, capability checks) is
    implemented here so backends stay swappable.
    """

    name: str = "base"
    aliases: tuple[str, ...] = ()
    #: importable module names required for this backend
    requires: tuple[str, ...] = ()
    #: True when the backend drives an asyncio event loop internally
    supports_async: bool = False
    #: backend-specific CLI defaults (see --backend-args)
    extra: dict[str, Any]

    def __init__(self, config: RequestConfig | None = None, extra: Mapping[str, Any] | None = None) -> None:
        self.config = config or RequestConfig()
        self.extra = dict(extra or {})
        self.log = get_logger(f"backend.{self.name}")
        self._lock = threading.Lock()
        self._opened = False
        self._open()

    # -- capability ------------------------------------------------------- #
    @classmethod
    def available(cls) -> bool:
        return all(importlib.util.find_spec(module) is not None for module in cls.requires)

    @classmethod
    def availability_reason(cls) -> str | None:
        for module in cls.requires:
            if importlib.util.find_spec(module) is None:
                return f"missing optional dependency '{module}'"
        return None

    # -- lifecycle -------------------------------------------------------- #
    def _open(self) -> None:  # noqa: B027 - optional hook, not abstract
        """Hook for lazy client creation (called from ``__init__``)."""

    def close(self) -> None:
        self._opened = False

    def __enter__(self) -> DownloadBackend:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # -- primitive -------------------------------------------------------- #
    @abstractmethod
    def _request(
        self,
        url: str,
        method: str,
        headers: list[tuple[str, str]],
        sink: Sink | None,
        *,
        allow_status: frozenset[int] = frozenset(),
    ) -> ResponseInfo:
        """Perform one request; raise :class:`HTTPStatusError` for 4xx/5xx."""

    # -- helpers ---------------------------------------------------------- #
    def _fail_status(self, info: ResponseInfo) -> None:
        message = f"HTTP {info.status} for {redact_url(info.url)}"
        if info.status == 404:
            message = f"not found (404): {redact_url(info.url)}"
        raise HTTPStatusError(info.status, message, url=info.url, headers=dict(info.headers))

    def request(
        self,
        url: str,
        *,
        method: str | None = None,
        headers: Iterable[tuple[str, str]] = (),
        sink: Sink | None = None,
        allow_status: frozenset[int] = frozenset(),
    ) -> ResponseInfo:
        http_method = (method or self.config.method or "GET").upper()
        merged = self.config.build_headers(url, headers)
        return self._request(url, http_method, merged, sink, allow_status=allow_status)

    def stream_range(self, url: str, start: int | None, end: int | None, sink: Sink) -> ResponseInfo:
        """GET a byte range (or the whole body when ``start`` is ``None``).

        ``--method``/``--data`` are honoured here so form-style POST downloads
        work; plain downloads always use GET.
        """
        extra: list[tuple[str, str]] = []
        if start is not None:
            if end is None:
                extra.append(("Range", f"bytes={start}-"))
            else:
                extra.append(("Range", f"bytes={start}-{end}"))
        configured = (self.config.method or "GET").upper()
        method = configured if (configured != "GET" or self.config.data is not None) else "GET"
        info = self.request(url, method=method, headers=extra, sink=sink)
        if info.status == 200 and start not in (None, 0):
            raise RangeNotSupportedError(f"server ignored Range header for {redact_url(url)}", url=url)
        return info

    def probe(self, url: str) -> ProbeResult:
        """HEAD the URL, falling back to a 1-range GET when HEAD is unusable."""
        info = self.request(url, method="HEAD", allow_status=frozenset({403, 405, 501}))
        size = info.int_header("content-length")
        accept_ranges = (info.header("accept-ranges") or "").lower() == "bytes"
        content_encoding = (info.header("content-encoding") or "identity").lower()
        if content_encoding not in {"", "identity"}:
            # a compressed transfer would make byte offsets meaningless
            accept_ranges = False
        final_url = info.url

        head_unusable = size is None or info.status in {403, 405, 501}
        if head_unusable:
            info, size, accept_ranges = self._probe_with_get(url)
            final_url = info.url
        elif not accept_ranges and (size or 0) > 0:
            # HEAD worked but the server did not advertise range support:
            # verify with a cheap one-byte request instead of trusting the
            # absence of the header.
            range_info = self.request(url, method="GET", headers=[("Range", "bytes=0-0")], sink=_discard_sink)
            if range_info.status == 206:
                accept_ranges = True
                parsed = parse_content_range(range_info.header("content-range"))
                if parsed and parsed[2] is not None:
                    size = parsed[2]

        suggested = content_disposition_filename(info.header("content-disposition"))
        if not suggested:
            candidate = url_basename(final_url)
            suggested = candidate or None
        return ProbeResult(
            url=url,
            final_url=final_url,
            status=info.status,
            size=size,
            etag=info.header("etag"),
            last_modified=info.header("last-modified"),
            accept_ranges=accept_ranges,
            suggested_filename=suggested,
            content_type=info.header("content-type"),
            headers=dict(info.headers),
        )

    def _probe_with_get(self, url: str) -> tuple[ResponseInfo, int | None, bool]:
        counted = {"bytes": 0}

        def sink(chunk: bytes) -> None:
            counted["bytes"] += len(chunk)
            raise StopStream

        info = self.request(url, method="GET", headers=[("Range", "bytes=0-0")], sink=sink)
        size = None
        accept_ranges = False
        if info.status == 206:
            accept_ranges = True
            parsed = parse_content_range(info.header("content-range"))
            if parsed:
                size = parsed[2]
        if size is None:
            size = info.int_header("content-length")
            if info.status == 200 and counted["bytes"]:
                # the whole body was streamed; content-length is the size
                accept_ranges = False
        return info, size, accept_ranges


def _discard_sink(chunk: bytes) -> None:  # pragma: no cover - trivial
    raise StopStream
