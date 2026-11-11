"""pycurl backend (default): HTTP/2, strict TLS control, stall timeouts."""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote, urlparse

from ..errors import BackendUnavailableError, RequestTimeout, TransportError
from ..request import RequestConfig
from ..util import redact_url
from .base import DownloadBackend, ResponseInfo, Sink, StopStream, normalize_headers

#: libcurl error codes worth a retry
_RETRYABLE_CURL_CODES = frozenset({6, 7, 16, 18, 28, 35, 52, 55, 56, 92})


def _discard(_data: bytes) -> int:
    return len(_data)


class PycurlBackend(DownloadBackend):
    name = "pycurl"
    requires = ("pycurl",)

    def _open(self) -> None:
        try:
            import pycurl
        except ImportError as exc:  # pragma: no cover - guarded by requires
            raise BackendUnavailableError(
                "backend 'pycurl' requires the 'pycurl' package (pip install 'wget2[pycurl]')"
            ) from exc
        self._pycurl = pycurl

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
        pycurl = self._pycurl
        config = self.config
        collected: list[tuple[str, str]] = []
        state: dict[str, Any] = {"status": 0, "stop": False, "error": None}
        curl = pycurl.Curl()
        try:
            self._configure(curl, url, method, headers, sink, state, collected)
            try:
                curl.perform()
            except pycurl.error as exc:
                if state.get("error") is not None:
                    # sink failure: surface the original cause, not the abort
                    raise state["error"] from None
                if not state["stop"]:
                    raise self._map_error(exc, url) from exc
            status = int(curl.getinfo(pycurl.RESPONSE_CODE))
            final_url = str(curl.getinfo(pycurl.EFFECTIVE_URL) or url)
            info = ResponseInfo(status, final_url, normalize_headers(collected))
            if config.cookie_store is not None:
                config.cookie_store.update_from_headers(final_url, info.header("set-cookie"))
            if status >= 400 and status not in allow_status:
                self._fail_status(info)
            return info
        finally:
            curl.close()

    # ------------------------------------------------------------------ #
    def _configure(
        self,
        curl: Any,
        url: str,
        method: str,
        headers: list[tuple[str, str]],
        sink: Sink | None,
        state: dict[str, Any],
        collected: list[tuple[str, str]],
    ) -> None:
        pycurl = self._pycurl
        config = self.config

        def on_header(line: bytes) -> int:
            text = line.decode("iso-8859-1").rstrip("\r\n")
            if not text:
                return len(line)
            if text.startswith("HTTP/"):
                parts = text.split()
                if len(parts) >= 2 and parts[1].isdigit():
                    code = int(parts[1])
                    state["status"] = code
                    if code >= 200:  # drop 1xx / CONNECT noise
                        collected.clear()
                return len(line)
            if ":" in text:
                name, value = text.split(":", 1)
                collected.append((name.strip(), value.strip()))
            return len(line)

        def on_write(data: bytes) -> int:
            if state["status"] >= 400:
                return len(data)  # never let error pages reach the sink
            if sink is not None:
                try:
                    sink(data)
                except StopStream:
                    state["stop"] = True
                    return 0  # aborts the transfer
                except BaseException as exc:  # noqa: BLE001 - re-raised by the caller
                    # libcurl cannot carry Python exceptions across the C boundary:
                    # abort the transfer, then re-raise from Python below.
                    state["error"] = exc
                    return 0
            return len(data)

        curl.setopt(pycurl.URL, url)
        curl.setopt(pycurl.NOSIGNAL, 1)
        curl.setopt(pycurl.FOLLOWLOCATION, 1 if config.max_redirects else 0)
        curl.setopt(pycurl.MAXREDIRS, max(0, config.max_redirects))
        curl.setopt(pycurl.PROTOCOLS, pycurl.PROTO_HTTP | pycurl.PROTO_HTTPS)
        if hasattr(pycurl, "REDIR_PROTOCOLS"):
            curl.setopt(pycurl.REDIR_PROTOCOLS, pycurl.PROTO_HTTP | pycurl.PROTO_HTTPS)
        curl.setopt(pycurl.HTTPHEADER, [f"{name}: {value}" for name, value in headers])
        curl.setopt(pycurl.HEADERFUNCTION, on_header)
        curl.setopt(pycurl.ACCEPT_ENCODING, "")  # no transparent decompression
        if config.http2:
            curl.setopt(pycurl.HTTP_VERSION, pycurl.CURL_HTTP_VERSION_2TLS)

        # timeouts: connect timeout plus a stalled-transfer watchdog, so big
        # downloads are not killed by a total-transfer deadline
        curl.setopt(pycurl.CONNECTTIMEOUT_MS, int(max(0.0, config.connect_timeout) * 1000))
        if config.timeout and config.timeout > 0:
            curl.setopt(pycurl.LOW_SPEED_LIMIT, 1)
            curl.setopt(pycurl.LOW_SPEED_TIME, int(config.timeout))
        else:
            curl.setopt(pycurl.TIMEOUT, 0)

        # TLS
        if config.insecure:
            curl.setopt(pycurl.SSL_VERIFYPEER, 0)
            curl.setopt(pycurl.SSL_VERIFYHOST, 0)
        else:
            curl.setopt(pycurl.SSL_VERIFYPEER, 1)
            curl.setopt(pycurl.SSL_VERIFYHOST, 2)
        if config.ca_cert:
            curl.setopt(pycurl.CAINFO, config.ca_cert)
        if config.client_cert:
            curl.setopt(pycurl.SSLCERT, config.client_cert)
        if config.client_key:
            curl.setopt(pycurl.SSLKEY, config.client_key)

        # method / body
        if config.data is not None:
            curl.setopt(pycurl.POSTFIELDS, config.data)
            curl.setopt(pycurl.POSTFIELDSIZE, len(config.data))
        if method.upper() == "HEAD":
            curl.setopt(pycurl.NOBODY, 1)
            curl.setopt(pycurl.WRITEFUNCTION, _discard)
        else:
            curl.setopt(pycurl.WRITEFUNCTION, on_write)
            if method.upper() != "GET" or config.data is not None:
                curl.setopt(pycurl.CUSTOMREQUEST, method.upper())

        # proxy
        proxy = config.proxy_for(url)
        if proxy and not config.should_bypass_proxy(url):
            parsed = urlparse(proxy)
            curl.setopt(pycurl.PROXY, proxy)
            if parsed.username:
                curl.setopt(
                    pycurl.PROXYUSERPWD,
                    f"{unquote(parsed.username)}:{unquote(parsed.password or '')}",
                )
        elif config.no_proxy or config.should_bypass_proxy(url):
            curl.setopt(pycurl.PROXY, "")

    # ------------------------------------------------------------------ #
    def _map_error(self, exc: Any, url: str) -> Exception:
        code = exc.args[0] if exc.args else -1
        message = exc.args[1] if len(exc.args) > 1 else str(exc)
        detail = f"{message} ({redact_url(url)})"
        if code == 28:
            return RequestTimeout(f"timeout: {detail}", url=url, retryable=True)
        return TransportError(f"curl error {code}: {detail}", url=url, retryable=code in _RETRYABLE_CURL_CODES)


def create(config: RequestConfig, extra: dict[str, Any] | None = None) -> PycurlBackend:
    return PycurlBackend(config, extra)
