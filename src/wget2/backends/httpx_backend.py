"""httpx sync backend: HTTP/2, connection pooling, one shared client."""

from __future__ import annotations

from typing import Any

from ..errors import BackendUnavailableError, RequestTimeout, TransportError
from ..request import RequestConfig
from ..util import redact_url
from .base import DownloadBackend, ResponseInfo, Sink, StopStream, normalize_headers


class HttpxBackend(DownloadBackend):
    name = "httpx"
    requires = ("httpx",)

    def _open(self) -> None:
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover - guarded by requires
            raise BackendUnavailableError(
                "backend 'httpx' requires the 'httpx' package (pip install 'wget2[httpx]')"
            ) from exc
        self._httpx = httpx
        self._client: Any = None

    # ------------------------------------------------------------------ #
    def _make_client(self) -> Any:
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
        }
        if cert is not None:
            kwargs["cert"] = cert
        if config.proxy and not config.no_proxy:
            kwargs["proxy"] = config.proxy
        try:
            return httpx.Client(**kwargs)
        except ImportError:
            # http2 requested but the optional h2 package is missing
            self.log.warning("http/2 support unavailable (install 'httpx[http2]'); falling back to HTTP/1.1")
            kwargs["http2"] = False
            return httpx.Client(**kwargs)

    def client(self) -> Any:
        if self._client is None:
            self._client = self._make_client()
        return self._client

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:  # pragma: no cover - defensive
                pass
            self._client = None
        super().close()

    # ------------------------------------------------------------------ #
    def _request(
        self,
        url: str,
        method: str,
        headers: list[tuple[str, str]],
        sink: Sink | None,
        *,
        allow_status: frozenset[int] = frozenset(),
    ) -> ResponseInfo:
        httpx = self._httpx
        config = self.config
        try:
            with self.client().stream(method.upper(), url, headers=list(headers), content=config.data) as response:
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
                        for chunk in response.iter_raw(1024 * 256):
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
                f"{exc.__class__.__name__}: {exc} ({redact_url(url)})", url=url, retryable=True
            ) from exc


def create(config: RequestConfig, extra: dict[str, Any] | None = None) -> HttpxBackend:
    return HttpxBackend(config, extra)
