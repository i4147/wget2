"""Recursive discovery (``--recursive``/``--mirror``) and accept/reject filters."""

from __future__ import annotations

import fnmatch
import re
from collections import deque
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.parse import urldefrag, urljoin, urlparse, urlunparse

from .backends.base import DownloadBackend, StopStream
from .downloader import DownloadContext
from .errors import Wget2Error
from .log import get_logger
from .util import filename_from_url, redact_url, url_host

MAX_PAGE_BYTES = 8 * 1024 * 1024


class _LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag not in {"a", "link", "img", "script", "iframe", "source"}:
            return
        for name, value in attrs:
            if name in {"href", "src"} and value:
                self.links.append(value)


def extract_links(html: str, base_url: str) -> list[str]:
    parser = _LinkExtractor()
    try:
        parser.feed(html)
    except Exception:  # pragma: no cover - malformed HTML
        pass
    links: list[str] = []
    for raw in parser.links:
        raw = raw.strip()
        if not raw or raw.startswith(("javascript:", "mailto:", "data:", "#", "tel:")):
            continue
        absolute = urldefrag(urljoin(base_url, raw))[0]
        if absolute:
            links.append(absolute)
    return links


def _split_patterns(values: Iterable[str] | None) -> list[str]:
    out: list[str] = []
    for value in values or ():
        for item in str(value).split(","):
            item = item.strip()
            if item:
                out.append(item)
    return out


@dataclass
class FilterOptions:
    accept: tuple[str, ...] = ()
    reject: tuple[str, ...] = ()
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()

    _accept: list[str] = field(default_factory=list, init=False, repr=False)
    _reject: list[str] = field(default_factory=list, init=False, repr=False)
    _include: list[re.Pattern[str]] = field(default_factory=list, init=False, repr=False)
    _exclude: list[re.Pattern[str]] = field(default_factory=list, init=False, repr=False)

    def __post_init__(self) -> None:
        self._accept = [p.lower() for p in _split_patterns(self.accept)]
        self._reject = [p.lower() for p in _split_patterns(self.reject)]
        self._include = [re.compile(p) for p in _split_patterns(self.include)]
        self._exclude = [re.compile(p) for p in _split_patterns(self.exclude)]

    @property
    def active(self) -> bool:
        return bool(self._accept or self._reject or self._include or self._exclude)

    def allows(self, url: str) -> bool:
        if self._include and not any(pattern.search(url) for pattern in self._include):
            return False
        if self._exclude and any(pattern.search(url) for pattern in self._exclude):
            return False
        name = Path(urlparse(url).path).name.lower()
        if self._accept and not any(_glob_match(pattern, name, url) for pattern in self._accept):
            return False
        return not (self._reject and any(_glob_match(pattern, name, url) for pattern in self._reject))

    def allows_file(self, filename: str) -> bool:
        name = filename.lower()
        if self._accept and not any(_glob_match(pattern, name, name) for pattern in self._accept):
            return False
        return not (self._reject and any(_glob_match(pattern, name, name) for pattern in self._reject))


def _glob_match(pattern: str, name: str, url: str) -> bool:
    if any(char in pattern for char in "*?["):
        return fnmatch.fnmatch(name, pattern) or fnmatch.fnmatch(url.lower(), pattern)
    return name.endswith(pattern) or name == pattern


@dataclass
class CrawlOptions:
    level: int = 5
    mirror: bool = False
    recursive: bool = False
    same_host: bool = True
    max_urls: int = 5000
    filters: FilterOptions = field(default_factory=FilterOptions)


class Crawler:
    """Breadth-first, same-host discovery of links (like ``wget -r``)."""

    def __init__(self, context: DownloadContext, backend: DownloadBackend, options: CrawlOptions) -> None:
        self.ctx = context
        self.backend = backend
        self.options = options
        self.log = get_logger("crawler")

    def discover(self, seeds: Iterable[str]) -> list[str]:
        seeds = [self._normalize(seed) for seed in seeds]
        queue: deque[tuple[str, int]] = deque((seed, 0) for seed in reversed(seeds))
        seen: dict[str, int] = {}
        hosts = {url_host(seed) for seed in seeds}
        while queue:
            url, depth = queue.popleft()
            if self.ctx.interrupt.is_set():
                break
            if url in seen:
                continue
            if len(seen) >= self.options.max_urls:
                self.log.warning("crawl limit reached", limit=self.options.max_urls)
                break
            seen[url] = depth
            if depth >= max(0, self.options.level):
                continue
            if not self.options.filters.allows(url):
                continue
            try:
                page = self._fetch_page(url)
            except Wget2Error as exc:
                self.log.debug("crawl fetch failed", url=redact_url(url), error=str(exc))
                continue
            if page is None:
                continue
            base_url, html = page
            for link in extract_links(html, base_url):
                parsed = urlparse(link)
                if parsed.scheme not in {"http", "https"}:
                    continue
                if self.options.same_host:
                    host = parsed.hostname or ""
                    if host not in hosts:
                        continue
                canonical = self._normalize(link)
                if canonical not in seen:
                    queue.append((canonical, depth + 1))
        ordered = [
            url
            for url in seen
            if self.options.filters.allows(url) and self.options.filters.allows_file(filename_from_url(url))
        ]
        self.log.info("crawl complete", urls=len(ordered), seeds=len(seeds))
        return ordered

    # ------------------------------------------------------------------ #
    def _fetch_page(self, url: str) -> tuple[str, str] | None:
        """Fetch a page body (bounded) and decode it; None if not HTML."""
        chunks: list[bytes] = []
        counter = {"n": 0}

        def sink(chunk: bytes) -> None:
            counter["n"] += len(chunk)
            if counter["n"] > MAX_PAGE_BYTES:
                raise StopStream
            chunks.append(chunk)

        with self.ctx.host_limiter.slot(url):
            info = self.backend.request(
                url,
                method="GET",
                headers=[("Accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8")],
                sink=sink,
            )
        content_type = (info.header("content-type") or "").lower()
        if content_type and "html" not in content_type:
            self.log.debug("not an HTML page", url=redact_url(url), content_type=content_type)
            return None
        body = b"".join(chunks)
        encoding = "utf-8"
        match = re.search(r"charset=([\w-]+)", content_type)
        if match:
            encoding = match.group(1)
        try:
            html = body.decode(encoding, errors="replace")
        except LookupError:  # pragma: no cover - bogus charset
            html = body.decode("utf-8", errors="replace")
        return info.url, html

    def _normalize(self, url: str) -> str:
        parsed = urlparse(url)
        path = parsed.path or "/"
        return urlunparse(parsed._replace(fragment="", path=path))
