"""Exception hierarchy and exit codes.

Every error class carries an ``exit_code`` so the CLI can report a stable,
documented status for each failure class::

    0    success (includes skipped / up-to-date files)
    1    unexpected internal error
    2    usage, configuration or policy refusal
    3    network / transport failure (after retries)
    4    HTTP status error (after retries)
    5    integrity failure (checksum or content-length mismatch)
    6    filesystem failure (permissions, disk space, IO)
    7    partial success in a batch (at least one URL failed)
    9    another instance holds the lock file
    130  interrupted (SIGINT)
"""

from __future__ import annotations

from typing import Any, Final

EXIT_OK: Final[int] = 0
EXIT_ERROR: Final[int] = 1
EXIT_USAGE: Final[int] = 2
EXIT_NETWORK: Final[int] = 3
EXIT_HTTP: Final[int] = 4
EXIT_INTEGRITY: Final[int] = 5
EXIT_FILESYSTEM: Final[int] = 6
EXIT_PARTIAL: Final[int] = 7
EXIT_LOCKED: Final[int] = 9
EXIT_INTERRUPTED: Final[int] = 130

EXIT_CODE_DOC: Final[dict[int, str]] = {
    EXIT_OK: "success (including skipped and up-to-date files)",
    EXIT_ERROR: "unexpected internal error",
    EXIT_USAGE: "usage, configuration or policy refusal",
    EXIT_NETWORK: "network/transport failure after retries",
    EXIT_HTTP: "HTTP status error after retries",
    EXIT_INTEGRITY: "integrity failure (checksum or length mismatch)",
    EXIT_FILESYSTEM: "filesystem failure (permission, disk space, I/O)",
    EXIT_PARTIAL: "batch finished with failures",
    EXIT_LOCKED: "another instance is running",
    EXIT_INTERRUPTED: "interrupted",
}


class Wget2Error(Exception):
    """Base class for all expected failures."""

    exit_code: int = EXIT_ERROR
    kind: str = "error"
    retryable: bool = False

    def __init__(
        self,
        message: str,
        *,
        url: str | None = None,
        status_code: int | None = None,
        retryable: bool | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.url = url
        self.status_code = status_code
        if retryable is not None:
            self.retryable = retryable
        self.details = details or {}

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.message


class UsageError(Wget2Error):
    exit_code = EXIT_USAGE
    kind = "usage"


class ConfigError(UsageError):
    kind = "config"


class SchemeError(UsageError):
    """URL scheme is not in the allowlist."""

    kind = "scheme"


class PolicyError(UsageError):
    """A safety/policy rule refused the operation (size cap, unsafe name, ...)."""

    kind = "policy"


class BackendUnavailableError(UsageError):
    kind = "backend"


class LockError(Wget2Error):
    exit_code = EXIT_LOCKED
    kind = "lock"


class NetworkError(Wget2Error):
    exit_code = EXIT_NETWORK
    kind = "network"
    retryable = True


class TransportError(NetworkError):
    """Connection reset, DNS failure, TLS failure, ..."""

    kind = "transport"


class RequestTimeout(NetworkError):
    kind = "timeout"


class RangeNotSupportedError(NetworkError):
    """Server ignored the Range header (answered 200 to a ranged request)."""

    kind = "range"
    retryable = False


class HTTPStatusError(Wget2Error):
    exit_code = EXIT_HTTP
    kind = "http"

    #: statuses that are worth retrying by default
    RETRYABLE_STATUS: Final[frozenset[int]] = frozenset({408, 425, 429, 500, 502, 503, 504})

    def __init__(
        self,
        status_code: int,
        message: str,
        *,
        url: str | None = None,
        headers: dict[str, str] | None = None,
        retryable: bool | None = None,
    ) -> None:
        if retryable is None:
            retryable = status_code in self.RETRYABLE_STATUS
        super().__init__(
            message,
            url=url,
            status_code=status_code,
            retryable=retryable,
            details={"headers": headers or {}},
        )
        self.headers = headers or {}


class IntegrityError(Wget2Error):
    exit_code = EXIT_INTEGRITY
    kind = "integrity"


class ChecksumError(IntegrityError):
    kind = "checksum"


class ContentLengthError(IntegrityError):
    kind = "length"


class FilesystemError(Wget2Error):
    exit_code = EXIT_FILESYSTEM
    kind = "filesystem"


class DiskSpaceError(FilesystemError):
    kind = "disk-space"


class Interrupted(Wget2Error):
    exit_code = EXIT_INTERRUPTED
    kind = "interrupted"


def exit_code_for(exc: BaseException) -> int:
    """Map any exception to a documented exit code."""
    if isinstance(exc, Wget2Error):
        return exc.exit_code
    if isinstance(exc, (KeyboardInterrupt, Interrupted)):
        return EXIT_INTERRUPTED
    if isinstance(exc, PermissionError):
        return EXIT_FILESYSTEM
    return EXIT_ERROR
