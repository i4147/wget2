"""httpx async backend driven by a *single* long-lived event loop.

The previous implementation called ``asyncio.run()`` once per range, which
created and tore down an event loop (and a client, and its connection pool)
for every chunk. Here one daemon thread owns one loop and one
:class:`httpx.AsyncClient` for the whole process, so all ranges share the
connection pool and HTTP/2 multiplexing.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, Coroutine, TypeVar

from ..errors import BackendUnavailableError, RequestTimeout, TransportError
from ..request import RequestConfig
from ..util import redact_url
from .base import DownloadBackend, ResponseInfo, Sink, StopStream, normalize_headers

T = TypeVar("T")


class HttpxAsyncBackend(DownloadBackend):
    name = "httpx_async"
    requires = ("httpx",)
    supports_async = True

    def _open(self) -> None:
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover - guarded by requires
            raise BackendUnavailableError(
                "backend 'httpx_async' requires the 'httpx' package (pip install 'wget2[httpx]')"
            ) from exc
        self._httpx = httpx
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._client: Any = None

    # -- event loop ------------------------------------------------------- #
    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._loop is None:
                loop = asyncio.new_event_loop()
                thread = threading.Thread(
                    target=self._run_loop, args=(loop,), name="wget2-asyncio", daemon=True
                )
                thread.start()
                self._loop = loop
                self._thread = thread
                self.log.debug("event loop started", thread=thread.name)
        return self._loop

    @staticmethod
    def _run_loop(loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        finally:  # pragma: no cover - shutdown path
            loop.close()

    def _submit(self, coro: Coroutine[Any, Any, T]) -> T:
        loop = self._ensure_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result()

    async def _client_coro(self) -> Any:
        if self._client is None:
            httpx = self._httpx
            config = self.config
            timeout = httpx.Timeout(
                connect=max(0.0, config.connect_timeout) or None,
                read=max(0.0, config.timeout) or None,
                write=max(0.0, config.timeout) or None,
                pool=max(0.0, config.connect_timeout) or None,
            )
            verify: Any = True
            if config.insecure:
                verify = False
            elif config.ca_cert:
                verify = config.ca_cert
            cert: Any = None
            if config.client_cert:
                cert = (config.client_cert, config.client_key) if config.client_key else config.client_cert
            kwargs: dict[str, Any] = {
                "timeout": timeout,
                "follow_redirects": True,
                "max_redirects": max(1, config.max_redirects),
                "verify": verify,
                "http2": bool(config.http2),
                "trust_env": not config.no_proxy and not config.proxy,
                "limits": httpx.Limits(
                    max_connections=int(self.extra.get("max_connections", 64)),
                    max_keepalive_connections=int(self.extra.get("max_keepalive", 32)),
                ),
            }
            if cert is not None:
                kwargs["cert"] = cert
            if config.proxy and not config.no_proxy:
                kwargs["proxy"] = config.proxy
            try:
                self._client = httpx.AsyncClient(**kwargs)
            except ImportError:
                self.log.warning(
                    "http/2 support unavailable (install 'httpx[http2]'); falling back to HTTP/1.1"
                )
                kwargs["http2"] = False
                self._client = httpx.AsyncClient(**kwargs)
        return self._client

    def close(self) -> None:
        loop = self._loop
        if loop is not None and not loop.is_closed():
            try:
                if self._client is not None:
                    self._submit(self._client.aclose())
            except Exception:  # pragma: no cover - best effort
                pass
            self._client = None
            loop.call_soon_threadsafe(loop.stop)
            if self._thread is not None:
                self._thread.join(timeout=5.0)
            self._loop = None
            self._thread = None
        super().close()

    # -- requests --------------------------------------------------------- #
    def _request(
        self,
        url: str,
        method: str,
        headers: list[tuple[str, str]],
        sink: Sink | None,
        *,
        allow_status: frozenset[int] = frozenset(),
    ) -> ResponseInfo:
        return self._submit(self._request_async(url, method, headers, sink, allow_status))

    async def _request_async(
        self,
        url: str,
        method: str,
        headers: list[tuple[str, str]],
        sink: Sink | None,
        allow_status: frozenset[int],
    ) -> ResponseInfo:
        httpx = self._httpx
        config = self.config
        client = await self._client_coro()
        try:
            async with client.stream(
                method.upper(), url, headers=list(headers), content=config.data
            ) as response:
                info = ResponseInfo(
                    response.status_code,
                    str(response.url),
                    normalize_headers(response.headers.multi_items()),
                )
                if config.cookie_store is not None:
                    config.cookie_store.update_from_headers(info.url, info.header("set-cookie"))
                if info.status >= 400 and info.status not in allow_status:
                    self._fail_status(info)
                if sink is not None and method.upper() != "HEAD":
                    try:
                        async for chunk in response.aiter_raw(1024 * 256):
                            if chunk:
                                sink(chunk)
                    except StopStream:
                        pass
                return info
        except httpx.TimeoutException as exc:
            raise RequestTimeout(f"timeout: {redact_url(url)}", url=url, retryable=True) from exc
        except httpx.ConnectError as exc:
            raise TransportError(f"connect failed: {exc}", url=url, retryable=True) from exc
        except httpx.HTTPError as exc:
            raise TransportError(
                f"{exc.__class__.__name__}: {exc} ({redact_url(url)})",
                url=url,
                retryable=True,
            ) from exc


def create(config: RequestConfig, extra: dict[str, Any] | None = None) -> HttpxAsyncBackend:
    return HttpxAsyncBackend(config, extra)
