"""Aggregated, immutable runtime settings (CLI + config file + env)."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from .checksum import ChecksumSpec
from .output import OutputOptions
from .ratelimit import RateLimiter
from .request import RequestConfig
from .retry import DEFAULT_RETRY_STATUS, RetryPolicy

#: files below this size always use a single stream
MULTI_RANGE_MIN = 1024 * 1024
#: files above this size are written straight to disk instead of buffered
CHUNK_THRESHOLD = 10 * 1024 * 1024
#: how many segments per connection the range planner creates
SEGMENTS_PER_CONNECTION = 4
#: upper bound on concurrent connections for a single file
MAX_CONNECTIONS_PER_FILE = 32


@dataclass(frozen=True)
class Settings:
    # -- backend / connections ------------------------------------------- #
    backend: str = "pycurl"
    backend_auto_fallback: bool = False
    backend_extra: dict[str, Any] = field(default_factory=dict)
    connections: int = 8
    max_per_host: int = 6
    jobs: int = 1
    max_active: int = 1

    # -- reliability ------------------------------------------------------ #
    retry: int = 5
    retry_delay: float = 1.0
    retry_backoff: float = 2.0
    retry_max_delay: float = 60.0
    retry_jitter: float = 0.25
    retry_http: frozenset[int] = DEFAULT_RETRY_STATUS
    timeout: float = 30.0
    connect_timeout: float = 10.0
    max_redirects: int = 10

    # -- resume / atomicity ----------------------------------------------- #
    continue_: bool = False
    allow_unvalidated_resume: bool = False
    keep_partial: bool = True
    remove_on_error: bool = False
    check_disk: bool = True

    # -- integrity -------------------------------------------------------- #
    checksums: tuple[ChecksumSpec, ...] = ()
    checksum_table: dict[str, list[ChecksumSpec]] = field(default_factory=dict)

    # -- http ------------------------------------------------------------- #
    request: RequestConfig = field(default_factory=RequestConfig)

    # -- output ----------------------------------------------------------- #
    output: OutputOptions = field(default_factory=OutputOptions)
    manifest: bool = False
    progress_mode: str = "rich"
    json_report: bool = False
    log_level: str = "info"
    log_file: Path | None = None
    json_logs: bool = False
    quiet: bool = False
    verbose: bool = False

    # -- resources -------------------------------------------------------- #
    max_filesize: int | None = None
    rate_limit: float | None = None
    rate_limit_per_connection: float | None = None
    buffer_threshold: int = CHUNK_THRESHOLD

    # -- crawl / modes ---------------------------------------------------- #
    recursive: bool = False
    mirror: bool = False
    level: int = 5
    spider: bool = False
    dry_run: bool = False
    accept: tuple[str, ...] = ()
    reject: tuple[str, ...] = ()
    include: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()
    base_url: str | None = None
    allow_unsafe_schemes: bool = False
    #: alternate URLs tried in order when the primary URL fails
    mirrors: tuple[str, ...] = ()

    # -- derived ---------------------------------------------------------- #
    def retry_policy(self, *, attempts: int | None = None) -> RetryPolicy:
        return RetryPolicy(
            max_retries=self.retry if attempts is None else attempts,
            delay=self.retry_delay,
            backoff=self.retry_backoff,
            max_delay=self.retry_max_delay,
            jitter=self.retry_jitter,
            retry_status=self.retry_http,
        )

    def effective_connections(self, size: int | None) -> int:
        """Adaptive connection count for one file."""
        configured = max(1, min(self.connections, MAX_CONNECTIONS_PER_FILE))
        if not size:
            return 1
        if size < MULTI_RANGE_MIN:
            return 1
        useful = max(1, size // (256 * 1024))
        return max(1, min(configured, useful))

    def global_limiter(self) -> RateLimiter | None:
        return RateLimiter(self.rate_limit) if self.rate_limit else None

    def connection_limiter(self) -> RateLimiter | None:
        return RateLimiter(self.rate_limit_per_connection) if self.rate_limit_per_connection else None

    def with_overrides(self, **kwargs: Any) -> Settings:
        return replace(self, **kwargs)
