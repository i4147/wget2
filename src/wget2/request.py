"""Backend-independent request description: headers, auth, TLS, proxy."""

from __future__ import annotations

import functools
import netrc as netrc_module
import os
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import unquote, urlparse

from . import __version__
from .cookies import CookieStore
from .errors import ConfigError
from .log import get_logger
from .util import redact, url_host

DEFAULT_USER_AGENT = f"wget2/{__version__}"


@dataclass(frozen=True)
class RequestConfig:
    """Everything a backend needs to build an HTTP request."""

    headers: tuple[tuple[str, str], ...] = ()
    user_agent: str = DEFAULT_USER_AGENT
    referer: str | None = None
    user: str | None = None
    password: str | None = None
    bearer_token: str | None = None
    use_netrc: bool = False
    netrc_file: str | None = None
    cookies: tuple[tuple[str, str], ...] = ()
    proxy: str | None = None
    no_proxy: bool = False
    insecure: bool = False
    ca_cert: str | None = None
    client_cert: str | None = None
    client_key: str | None = None
    timeout: float = 30.0
    connect_timeout: float = 10.0
    max_redirects: int = 10
    method: str = "GET"
    data: bytes | None = None
    http2: bool = True
    accept_encoding: str = "identity"
    cookie_store: CookieStore | None = field(default=None, compare=False, repr=False)

    # ------------------------------------------------------------------ #
    def with_overrides(self, **kwargs: Any) -> RequestConfig:
        return replace(self, **kwargs)

    def netrc_credentials(self, url: str) -> tuple[str, str] | None:
        if not self.use_netrc:
            return None
        return _lookup_netrc(url_host(url), self._netrc_path())

    def _netrc_path(self) -> str | None:
        return self.netrc_file or None

    def proxy_for(self, url: str) -> str | None:
        """Explicit proxy for ``url``; ``None`` lets the library use its env."""
        if self.no_proxy:
            return None
        if self.proxy:
            return self.proxy
        return None

    def should_bypass_proxy(self, url: str) -> bool:
        if self.no_proxy:
            return True
        host = url_host(url)
        pattern = os.environ.get("NO_PROXY") or os.environ.get("no_proxy")
        if not pattern or not host:
            return False
        for item in pattern.split(","):
            item = item.strip().lstrip(".")
            if not item:
                continue
            if item in {"*", host} or host.endswith("." + item):
                return True
        return False

    def auth_for(self, url: str) -> tuple[str, str] | None:
        if self.user is not None:
            return self.user, self.password or ""
        credentials = self.netrc_credentials(url)
        if credentials:
            return credentials
        parsed = urlparse(url)
        if parsed.username:
            return unquote(parsed.username), unquote(parsed.password or "")
        return None

    def build_headers(self, url: str, extra: Iterable[tuple[str, str]] = ()) -> list[tuple[str, str]]:
        """Assemble the final header list for ``url`` (ordered, deduplicated)."""
        ordered: dict[str, str] = {}

        def put(name: str, value: str) -> None:
            key = name.lower()
            ordered.pop(key, None)
            ordered[key] = value
            ordered[f"__name__{key}"] = name

        put("User-Agent", self.user_agent)
        put("Accept", "*/*")
        if self.accept_encoding:
            put("Accept-Encoding", self.accept_encoding)
        if self.referer:
            put("Referer", self.referer)
        if self.bearer_token:
            put("Authorization", f"Bearer {self.bearer_token}")
        elif (auth := self.auth_for(url)) is not None:
            import base64

            user, password = auth
            token = base64.b64encode(f"{user}:{password}".encode()).decode()
            put("Authorization", f"Basic {token}")

        cookie_header = None
        if self.cookie_store is not None:
            cookie_header = self.cookie_store.header_for(url)
        if cookie_header is None and self.cookies:
            cookie_header = "; ".join(f"{k}={v}" for k, v in self.cookies)
        if cookie_header:
            put("Cookie", cookie_header)

        for name, value in self.headers:
            put(name, value)
        for name, value in extra:
            put(name, value)

        result: list[tuple[str, str]] = []
        for key in list(ordered):
            if key.startswith("__name__"):
                continue
            result.append((ordered[f"__name__{key}"], ordered[key]))
        return result

    def describe(self) -> dict[str, Any]:
        """Safe-for-logs summary (no secrets)."""
        return {
            "user_agent": self.user_agent,
            "headers": [f"{k}" for k, _ in self.headers],
            "auth": "basic" if (self.user or self.use_netrc) else ("bearer" if self.bearer_token else None),
            "proxy": redact(self.proxy) if self.proxy else ("bypassed" if self.no_proxy else "environment"),
            "tls_verify": not self.insecure,
            "timeout": self.timeout,
            "connect_timeout": self.connect_timeout,
            "http2": self.http2,
        }


@functools.lru_cache(maxsize=64)
def _lookup_netrc(host: str, netrc_file: str | None) -> tuple[str, str] | None:
    path = netrc_file or str(Path.home() / ".netrc")
    try:
        parsed = netrc_module.netrc(path)
    except FileNotFoundError:
        get_logger("auth").debug("no netrc file", path=path)
        return None
    except (netrc_module.NetrcParseError, OSError) as exc:
        get_logger("auth").warning("cannot parse netrc file", path=path, error=str(exc))
        return None
    auth = parsed.authenticators(host)
    if not auth:
        return None
    login, _account, password = auth
    return login, password or ""


def parse_header(value: str) -> tuple[str, str]:
    """Parse ``-H 'Name: value'``."""
    name, sep, val = value.partition(":")
    if not sep or not name.strip():
        raise ConfigError(f"invalid header (expected 'Name: value'): {value!r}")
    return name.strip(), val.strip()


def parse_headers(values: Iterable[str] | None) -> tuple[tuple[str, str], ...]:
    return tuple(parse_header(v) for v in (values or ()) if v and v.strip())


def parse_cookies(value: str | None) -> tuple[tuple[str, str], ...]:
    out: list[tuple[str, str]] = []
    if not value:
        return ()
    for part in value.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        name, _, val = part.partition("=")
        out.append((name.strip(), val.strip()))
    return tuple(out)


def read_data_argument(value: str | None, *, binary: bool, from_file: str | None = None) -> bytes | None:
    """Resolve ``--data``/``--data-binary`` values (``@file`` supported)."""
    if value is None:
        return None
    text = value
    if value.startswith("@"):
        path = Path(value[1:])
        try:
            return path.read_bytes()
        except OSError as exc:
            raise ConfigError(f"cannot read request body {path}: {exc}") from exc
    if from_file:
        try:
            return Path(from_file).read_bytes()
        except OSError as exc:  # pragma: no cover - defensive
            raise ConfigError(f"cannot read request body {from_file}: {exc}") from exc
    if binary:
        return text.encode()
    # --data behaves like a form body: newlines are stripped
    return text.replace("\n", "").replace("\r", "").encode()
