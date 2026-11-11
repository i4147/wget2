"""requests backend (sync, HTTP/1.1, thread-local sessions)."""

from __future__ import annotations

import threading
from typing import Any

from ..errors import BackendUnavailableError, RequestTimeout, TransportError
from ..request import RequestConfig
from ..util import redact_url
from .base import DownloadBackend, ResponseInfo, Sink, StopStream, normalize_headers


class RequestsBackend(DownloadBackend):
    name = "requests"
    requires = ("requests",)

    def _open(self) -> None:
        try:
            import requests
        except ImportError as exc:  # pragma: no cover - guarded by requires
            raise BackendUnavailableError(
                "backend 'requests' requires the 'requests' package (pip install 'wget2[requests]')"
            ) from exc
        self._requests = requests
        self._local = threading.local()

    def close(self) -> None:
        session = getattr(self._local, "session", None)
        if session is not None:
            session.close()
            del self._local.session
        super().close()

    def _session(self) -> Any:
        session = getattr(self._local, "session", None)
        if session is None:
            session = self._requests.Session()
            session.max_redirects = max(1, self.config.max_redirects)
            self._local.session = session
        return session

    def _request(
        self,
        url: str,
        method: str,
        headers: list[tuple[str, str]],
        sink: Sink | None,
        *,
        allow_status: frozenset[int] = frozenset(),
    ) -> ResponseInfo:
        requests = self._requests
        config = self.config
        session = self._session()
        proxies: dict[str, str | None] | None = None
        if config.no_proxy or config.should_bypass_proxy(url):
            proxies = {"http": None, "https": None}
        elif config.proxy:
            proxies = {"http": config.proxy, "https": config.proxy}
        verify: Any = True
        if config.insecure:
            verify = False
        elif config.ca_cert:
            verify = config.ca_cert
        cert: Any = None
        if config.client_cert:
            cert = (config.client_cert, config.client_key) if config.client_key else config.client_cert
        timeout = (
            max(0.0, config.connect_timeout) or None,
            max(0.0, config.timeout) or None,
        )
        try:
            response = session.request(
                method.upper(),
                url,
                headers=dict(headers),
                data=config.data,
                stream=True,
                allow_redirects=config.max_redirects > 0,
                timeout=timeout,
                verify=verify,
                cert=cert,
                proxies=proxies,
            )
        except requests.exceptions.Timeout as exc:
            raise RequestTimeout(f"timeout: {redact_url(url)}", url=url, retryable=True) from exc
        except requests.exceptions.SSLError as exc:
            raise TransportError(f"TLS error: {exc} ({redact_url(url)})", url=url, retryable=False) from exc
        except requests.exceptions.RequestException as exc:
            raise TransportError(f"{exc.__class__.__name__}: {exc}", url=url, retryable=True) from exc

        with response:
            info = ResponseInfo(
                response.status_code,
                response.url or url,
                normalize_headers(response.headers.items()),
            )
            if config.cookie_store is not None:
                config.cookie_store.update_from_headers(info.url, info.header("set-cookie"))
            if info.status >= 400 and info.status not in allow_status:
                self._fail_status(info)
            if sink is not None and method.upper() != "HEAD":
                try:
                    for chunk in response.iter_content(1024 * 256):
                        if chunk:
                            sink(chunk)
                except StopStream:
                    pass
                except requests.exceptions.RequestException as exc:
                    raise TransportError(
                        f"transfer interrupted: {exc.__class__.__name__}: {exc}",
                        url=url,
                        retryable=True,
                    ) from exc
            return info


def create(config: RequestConfig, extra: dict[str, Any] | None = None) -> RequestsBackend:
    return RequestsBackend(config, extra)
