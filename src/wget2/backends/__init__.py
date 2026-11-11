"""Backend package.

Kept import-compatible with the previous release, where ``BACKENDS`` was a
plain dict of ``name -> class``.
"""

from __future__ import annotations

from .base import (
    DownloadBackend,
    ProbeResult,
    ResponseInfo,
    Sink,
    StopStream,
    normalize_headers,
    parse_content_range,
)
from .httpx_async_backend import HttpxAsyncBackend
from .httpx_backend import HttpxBackend
from .pycurl_backend import PycurlBackend
from .registry import (
    ALIASES,
    AUTO_ORDER,
    ENTRY_POINT_GROUP,
    REGISTRY,
    BackendInfo,
    BackendRegistry,
    parse_backend_args,
)
from .requests_backend import RequestsBackend

BACKENDS: dict[str, type[DownloadBackend]] = {
    "requests": RequestsBackend,
    "pycurl": PycurlBackend,
    "httpx": HttpxBackend,
    "httpx_async": HttpxAsyncBackend,
}

DEFAULT_BACKEND = "pycurl"

__all__ = [
    "ALIASES",
    "AUTO_ORDER",
    "BACKENDS",
    "DEFAULT_BACKEND",
    "ENTRY_POINT_GROUP",
    "REGISTRY",
    "BackendInfo",
    "BackendRegistry",
    "DownloadBackend",
    "HttpxAsyncBackend",
    "HttpxBackend",
    "ProbeResult",
    "PycurlBackend",
    "RequestsBackend",
    "ResponseInfo",
    "Sink",
    "StopStream",
    "normalize_headers",
    "parse_backend_args",
    "parse_content_range",
]
