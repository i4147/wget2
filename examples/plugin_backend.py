# Example: a third-party wget2 backend plugin
#
# Package layout:
#
#   mywget2backend/
#       __init__.py
#       backend.py        <- this file
#   pyproject.toml
#
# pyproject.toml:
#
#   [project]
#   name = "mywget2backend"
#   dependencies = ["wget2", "httpx"]
#
#   [project.entry-points."wget2.backends"]
#   mybackend = "mywget2backend.backend:MyBackend"
#
# After `pip install .`, `wget2 --list-backends` shows it and
# `wget2 -b mybackend URL` uses it. No core changes required.

from __future__ import annotations

from typing import Any

import httpx

from wget2.backends.base import DownloadBackend, ResponseInfo, Sink, StopStream, normalize_headers
from wget2.errors import RequestTimeout, TransportError
from wget2.util import redact_url


class MyBackend(DownloadBackend):
    """A minimal backend that only implements the single request primitive."""

    name = "mybackend"
    requires = ("httpx",)

    def _open(self) -> None:
        self._client: Any = None

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
        super().close()

    def _request(
        self,
        url: str,
        method: str,
        headers: list[tuple[str, str]],
        sink: Sink | None,
        *,
        allow_status: frozenset[int] = frozenset(),
    ) -> ResponseInfo:
        if self._client is None:
            self._client = httpx.Client(
                follow_redirects=True,
                timeout=httpx.Timeout(connect=self.config.connect_timeout, read=self.config.timeout),
                verify=not self.config.insecure,
                http2=bool(self.config.http2),
                proxy=self.config.proxy,
            )
        try:
            with self._client.stream(method, url, headers=list(headers)) as response:
                info = ResponseInfo(
                    response.status_code,
                    str(response.url),
                    normalize_headers(response.headers.multi_items()),
                )
                if info.status >= 400 and info.status not in allow_status:
                    self._fail_status(info)
                if sink is not None and method.upper() != "HEAD":
                    try:
                        for chunk in response.iter_raw(256 * 1024):
                            if chunk:
                                sink(chunk)
                    except StopStream:
                        pass
                return info
        except httpx.TimeoutException as exc:
            raise RequestTimeout(f"timeout: {redact_url(url)}", url=url) from exc
        except httpx.HTTPError as exc:
            raise TransportError(f"{exc.__class__.__name__}: {exc}", url=url) from exc
