"""Small, dependency-free helpers: sizes, filenames, URLs, atomic IO, redaction."""

from __future__ import annotations

import email.utils
import errno
import os
import re
import shutil
import stat
import unicodedata
from pathlib import Path
from urllib.parse import unquote, urlparse, urlunparse

from .errors import DiskSpaceError, FilesystemError, SchemeError, UsageError

# --------------------------------------------------------------------------- #
# schemes
# --------------------------------------------------------------------------- #
ALLOWED_SCHEMES = frozenset({"http", "https"})
#: schemes that are refused unless --allow-unsafe-schemes is given
DEFAULT_DENIED_SCHEMES = frozenset({"file", "ftp", "ftps", "data", "gopher"})
#: schemes a backend can actually serve when explicitly enabled
OPT_IN_SCHEMES = frozenset({"file", "ftp", "ftps"})

_SIZE_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([kmgtp]?)(i?b?)\s*$", re.IGNORECASE)
_UNITS = {"": 0, "k": 1, "m": 2, "g": 3, "t": 4, "p": 5}


def parse_size(value: str | int | float | None) -> int | None:
    """Parse ``"10M"``/``"512"``/``"1.5GiB"`` into bytes. ``None`` passes through."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text or text.lower() in {"0", "none", "off", "unlimited", "-1"}:
        return None
    match = _SIZE_RE.match(text)
    if not match:
        raise UsageError(f"invalid size value: {value!r}")
    number, unit, _ = match.groups()
    multiplier = 1024 ** _UNITS[unit.lower()]
    return int(float(number) * multiplier)


def human_size(num: float | None) -> str:
    if num is None:
        return "?"
    negative = num < 0
    num = float(abs(num))
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if num < 1024 or unit == "TiB":
            if unit == "B":
                return f"{'-' if negative else ''}{int(num)} B"
            return f"{'-' if negative else ''}{num:.1f} {unit}"
        num /= 1024
    return f"{num:.1f} TiB"  # pragma: no cover


def human_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m{int(sec):02d}s"
    hours, minutes = divmod(int(minutes), 60)
    return f"{hours}h{minutes:02d}m"


def parse_int_list(value: str | None) -> frozenset[int]:
    """Parse ``"429,500-503"`` into a set of ints."""
    if not value:
        return frozenset()
    out: set[int] = set()
    for part in re.split(r"[,\s]+", value.strip()):
        if not part:
            continue
        if "-" in part:
            lo, _, hi = part.partition("-")
            try:
                out.update(range(int(lo), int(hi) + 1))
            except ValueError as exc:
                raise UsageError(f"invalid status range: {part!r}") from exc
        else:
            try:
                out.add(int(part))
            except ValueError as exc:
                raise UsageError(f"invalid status code: {part!r}") from exc
    return frozenset(out)


# --------------------------------------------------------------------------- #
# URLs
# --------------------------------------------------------------------------- #
def validate_url(url: str, *, allow_extra_schemes: bool = False) -> str:
    """Validate the scheme/host of ``url`` and return it stripped."""
    url = (url or "").strip()
    if not url:
        raise UsageError("empty URL")
    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if not scheme:
        raise UsageError(f"URL has no scheme (try https://...): {url}")
    if scheme in ALLOWED_SCHEMES:
        pass
    elif scheme in DEFAULT_DENIED_SCHEMES:
        if not allow_extra_schemes:
            raise SchemeError(f"scheme '{scheme}' is disabled; pass --allow-unsafe-schemes to enable it")
        if scheme not in OPT_IN_SCHEMES:
            raise SchemeError(f"unsupported URL scheme: {scheme}")
    else:
        raise SchemeError(f"unsupported URL scheme: {scheme}")
    if scheme in {"http", "https", "ftp", "ftps"} and not parsed.netloc:
        raise UsageError(f"URL has no host: {url}")
    return url


def url_host(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except ValueError:  # pragma: no cover
        return ""


def url_without_credentials(url: str) -> str:
    try:
        parsed = urlparse(url)
    except ValueError:  # pragma: no cover
        return url
    if not parsed.username and not parsed.password:
        return url
    netloc = parsed.hostname or ""
    if parsed.port:
        netloc = f"{netloc}:{parsed.port}"
    return urlunparse(parsed._replace(netloc=netloc))


def url_basename(url: str) -> str:
    """Best-effort filename from a URL path."""
    try:
        path = urlparse(url).path
    except ValueError:  # pragma: no cover
        return ""
    return unquote(path.rsplit("/", 1)[-1])


# --------------------------------------------------------------------------- #
# filenames
# --------------------------------------------------------------------------- #
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f-\x9f]")
_UNSAFE_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_WINDOWS_RESERVED = frozenset(
    {"con", "prn", "aux", "nul"} | {f"com{i}" for i in range(1, 10)} | {f"lpt{i}" for i in range(1, 10)}
)
MAX_FILENAME = 180


def sanitize_filename(name: str, *, fallback: str = "download", max_length: int = MAX_FILENAME) -> str:
    """Make a URL-derived string safe to use as a single path component.

    Protects against path traversal (``../``, absolute paths), control
    characters, Windows reserved names, NUL bytes and absurd lengths.
    """
    if not name:
        return fallback
    name = unquote(str(name))
    # kill any directory component, whatever the separator
    name = name.replace("\\", "/").rsplit("/", 1)[-1]
    name = _CONTROL_RE.sub("", name)
    name = unicodedata.normalize("NFC", name)
    name = _UNSAFE_CHARS_RE.sub("_", name)
    name = name.strip().strip(".").strip()
    if not name or name in {".", ".."}:
        return fallback
    stem, dot, suffix = name.rpartition(".")
    if dot and stem and stem.lower() in _WINDOWS_RESERVED or not dot and name.lower() in _WINDOWS_RESERVED:
        name = f"_{name}"
    if len(name) > max_length:
        stem, dot, suffix = name.rpartition(".")
        if dot and len(suffix) <= 12:
            keep = max_length - len(suffix) - 1
            name = f"{stem[:keep]}.{suffix}"
        else:
            name = name[:max_length]
    return name or fallback


def filename_from_url(url: str, *, fallback: str = "download") -> str:
    """Derive a sanitized filename from the URL, handling trailing slashes."""
    base = url_basename(url)
    if not base:
        host = url_host(url) or "download"
        base = f"{sanitize_filename(host)}{'-index'}"
        if urlparse(url).path.endswith("/"):
            return sanitize_filename(f"{host}-index.html", fallback=fallback)
        return sanitize_filename(base, fallback=fallback)
    return sanitize_filename(base, fallback=fallback)


def content_disposition_filename(header: str | None) -> str | None:
    """Extract a sanitized filename from a Content-Disposition header."""
    if not header:
        return None
    match = re.search(r"filename\*\s*=\s*([^;]+)", header, re.IGNORECASE)
    if match:
        raw = match.group(1).strip().strip('"')
        if "''" in raw:
            raw = raw.split("''", 1)[1]
        try:
            return sanitize_filename(unquote(raw))
        except Exception:  # pragma: no cover - defensive
            return None
    match = re.search(r'filename\s*=\s*"([^"]*)"', header, re.IGNORECASE)
    if not match:
        match = re.search(r"filename\s*=\s*([^;]+)", header, re.IGNORECASE)
    if match:
        raw = match.group(1).strip().strip('"')
        if raw:
            return sanitize_filename(unquote(raw))
    return None


# --------------------------------------------------------------------------- #
# redaction
# --------------------------------------------------------------------------- #
_CREDENTIAL_URL_RE = re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://)([^/\s:@]+)(?::([^/\s@]*))?@")
_AUTH_HEADER_RE = re.compile(r"(?i)\b(authorization|proxy-authorization)\b(\s*[:=]\s*)([^\s,;]+(?:\s+[^\s,;]+)?)")
_BEARER_RE = re.compile(r"(?i)\b(bearer|basic|token)(\s+)([A-Za-z0-9\-._~+/=]{4,})")
_SECRET_KV_RE = re.compile(
    r"(?i)\b(password|passwd|pwd|token|secret|api[_-]?key|access[_-]?key)(\s*[=:]\s*)(\"?[^\"\s,&}]+)"
)
_REDACTED = "***"


def redact(text: object) -> str:
    """Strip credentials from arbitrary text before it reaches a log sink."""
    out = str(text)
    out = _CREDENTIAL_URL_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}:{_REDACTED}@", out)
    out = _AUTH_HEADER_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}{_REDACTED}", out)
    out = _BEARER_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}{_REDACTED}", out)
    # final pass: key=value style secrets
    return _SECRET_KV_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}{_REDACTED}", out)


def redact_url(url: str) -> str:
    return redact(url_without_credentials(url))


# --------------------------------------------------------------------------- #
# filesystem
# --------------------------------------------------------------------------- #
def fsync_file(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError as exc:  # pragma: no cover - platform dependent
        raise FilesystemError(f"cannot open {path} for fsync: {exc}") from exc
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def fsync_dir(path: Path) -> None:
    """Best-effort fsync of a directory so the rename is durable."""
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:  # pragma: no cover - some filesystems refuse
        pass
    finally:
        os.close(fd)


def atomic_finalize(tmp_path: Path, final_path: Path) -> None:
    """fsync the temporary file, then atomically rename it into place."""
    try:
        fsync_file(tmp_path)
    except FilesystemError:
        pass
    final_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        tmp_path.replace(final_path)
    except OSError as exc:
        raise FilesystemError(f"cannot move {tmp_path} -> {final_path}: {exc}") from exc
    fsync_dir(final_path.parent)


def preallocate(path: Path, size: int) -> None:
    """Reserve ``size`` bytes on disk (sparse-safe)."""
    try:
        with path.open("wb") as handle:
            if size > 0:
                if hasattr(os, "posix_fallocate"):
                    try:
                        os.posix_fallocate(handle.fileno(), 0, size)
                    except OSError:
                        handle.truncate(size)
                else:  # pragma: no cover - non-posix
                    handle.truncate(size)
    except OSError as exc:
        raise FilesystemError(f"cannot allocate {human_size(size)} for {path}: {exc}") from exc


def free_space(directory: Path) -> int:
    try:
        target = directory
        while not target.exists() and target.parent != target:
            target = target.parent
        return shutil.disk_usage(target).free
    except OSError:  # pragma: no cover - defensive
        return 2**62


def ensure_free_space(directory: Path, needed: int, *, factor: float = 1.0) -> None:
    required = int(needed * factor)
    available = free_space(directory)
    if available < required:
        raise DiskSpaceError(
            f"not enough disk space in {directory}: need {human_size(required)}, have {human_size(available)}"
        )


def safe_unlink(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except OSError as exc:  # pragma: no cover - defensive
        if exc.errno not in {errno.ENOENT, errno.EACCES}:
            raise


def is_regular_file(path: Path) -> bool:
    try:
        return stat.S_ISREG(path.stat().st_mode)
    except OSError:
        return False


def unique_path(path: Path, *, limit: int = 10_000) -> Path:
    """Return ``path``, or ``name.1.ext``/``name.2.ext``/... if it exists."""
    if not path.exists():
        return path
    stem, suffix = path.stem, path.suffix
    for index in range(1, limit):
        candidate = path.with_name(f"{stem}.{index}{suffix}")
        if not candidate.exists():
            return candidate
    raise FilesystemError(f"cannot find a free filename for {path}")  # pragma: no cover


# --------------------------------------------------------------------------- #
# HTTP dates
# --------------------------------------------------------------------------- #
def parse_http_date(value: str | None) -> float | None:
    if not value:
        return None
    try:
        parsed = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    try:
        return parsed.timestamp()
    except (OverflowError, OSError):  # pragma: no cover
        return None


def format_http_date(timestamp: float) -> str:
    return email.utils.formatdate(timestamp, usegmt=True)


def apply_mtime(path: Path, timestamp: float | None) -> None:
    if timestamp is None:
        return
    try:
        os.utime(path, (timestamp, timestamp))
    except OSError:  # pragma: no cover - non-fatal
        pass
