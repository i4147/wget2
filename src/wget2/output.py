"""Output path resolution, existing-file policy and manifest sidecars."""

from __future__ import annotations

import json
import re
import string
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import __version__
from .backends.base import ProbeResult
from .errors import FilesystemError, UsageError
from .log import get_logger
from .util import (
    content_disposition_filename,
    filename_from_url,
    parse_http_date,
    sanitize_filename,
    unique_path,
    url_host,
)

MANIFEST_SUFFIX = ".wget2.json"

TEMPLATE_FIELDS = frozenset(
    {"url", "host", "path", "name", "filename", "stem", "ext", "index", "size", "date"}
)


@dataclass(frozen=True)
class OutputOptions:
    output_dir: Path = Path()
    output: Path | None = None
    template: str | None = None
    no_clobber: bool = False
    overwrite: bool = True
    timestamping: bool = False
    use_content_disposition: bool = True
    unique: bool = False
    manifest: bool = False

    def validate(self, url_count: int) -> None:
        if self.output is not None and url_count > 1 and not self.template:
            raise UsageError(
                "--output/-O can only be used with a single URL; use --output-template for batches"
            )
        if self.no_clobber and self.overwrite and self.timestamping is False:
            # both flags explicitly set is contradictory; --no-clobber wins
            get_logger("output").debug("both --no-clobber and --overwrite set; --no-clobber wins")


@dataclass(frozen=True)
class Resolution:
    path: Path
    skip_reason: str | None = None
    detail: str | None = None

    @property
    def skipped(self) -> bool:
        return self.skip_reason is not None


class _SafeDict(dict[str, Any]):
    def __missing__(self, key: str) -> str:  # pragma: no cover - exercised via templates
        get_logger("output").warning("unknown template field", field=key)
        return ""


def render_template(template: str, values: dict[str, Any]) -> str:
    """Format an ``--output-template`` string, ignoring unknown fields."""
    try:
        return string.Formatter().vformat(template, (), _SafeDict(values))
    except (ValueError, IndexError) as exc:
        raise UsageError(f"invalid --output-template {template!r}: {exc}") from exc


def validate_template(template: str) -> None:
    for _, field_name, _, _ in string.Formatter().parse(template):
        if not field_name:
            continue
        base = re.split(r"[.\[]", field_name, maxsplit=1)[0]
        if base and base not in TEMPLATE_FIELDS:
            raise UsageError(
                f"unknown field '{base}' in --output-template "
                f"(available: {', '.join(sorted(TEMPLATE_FIELDS))})"
            )


def template_values(url: str, filename: str, index: int, size: int | None) -> dict[str, Any]:
    stem, dot, ext = filename.rpartition(".")
    if not dot:
        stem, ext = filename, ""
    from urllib.parse import urlparse

    parsed = urlparse(url)
    return {
        "url": url,
        "host": url_host(url) or "download",
        "path": parsed.path.lstrip("/") or filename,
        "name": stem,
        "filename": filename,
        "stem": stem,
        "ext": ext,
        "index": index,
        "size": size if size is not None else "",
        "date": time.strftime("%Y-%m-%d"),
    }


def resolve_output_path(
    options: OutputOptions,
    probe: ProbeResult,
    url: str,
    *,
    index: int = 0,
    local_size: int | None = None,
) -> Resolution:
    """Decide where a URL will be written (or why it is skipped)."""
    filename = filename_from_url(url)
    if options.use_content_disposition:
        from_header = content_disposition_filename(probe.header("content-disposition"))
        if from_header:
            filename = from_header
    elif probe.suggested_filename:
        filename = sanitize_filename(probe.suggested_filename)

    if options.template:
        rendered = render_template(options.template, template_values(url, filename, index, probe.size))
        # a template may produce sub-directories: allow them, but keep them
        # inside the output directory (no absolute paths, no '..')
        rendered = rendered.replace("\\", "/").lstrip("/")
        parts = [sanitize_filename(part) for part in rendered.split("/") if part not in {"", "."}]
        if any(part == ".." for part in rendered.split("/")):
            raise UsageError(f"--output-template must not contain '..': {options.template!r}")
        if not parts:
            raise UsageError(f"--output-template produced an empty path: {options.template!r}")
        candidate = options.output_dir.joinpath(*parts)
    elif options.output is not None:
        candidate = options.output
        if not candidate.is_absolute():
            candidate = options.output_dir / candidate
    else:
        candidate = options.output_dir / filename

    if options.no_clobber and candidate.exists():
        return Resolution(candidate, "skipped", "exists (--no-clobber)")

    if options.timestamping and candidate.exists():
        remote_ts = parse_http_date(probe.last_modified)
        try:
            local_ts = candidate.stat().st_mtime
        except OSError:  # pragma: no cover - race
            local_ts = 0.0
        if remote_ts is not None and local_ts + 1 >= remote_ts:
            return Resolution(candidate, "up-to-date", "local copy is newer (--timestamping)")

    if options.unique and candidate.exists():
        candidate = unique_path(candidate)

    return Resolution(candidate)


def write_manifest(
    final_path: Path,
    *,
    url: str,
    final_url: str | None = None,
    etag: str | None = None,
    last_modified: str | None = None,
    size: int | None = None,
    digests: dict[str, str] | None = None,
    backend: str | None = None,
    extra: dict[str, Any] | None = None,
) -> Path:
    """Write ``<file>.wget2.json`` describing the download."""
    payload: dict[str, Any] = {
        "manifest_version": 1,
        "tool": f"wget2 {__version__}",
        "url": url,
        "final_url": final_url or url,
        "filename": final_path.name,
        "path": str(final_path),
        "size": size if size is not None else (final_path.stat().st_size if final_path.exists() else None),
        "etag": etag,
        "last_modified": last_modified,
        "checksums": digests or {},
        "backend": backend,
        "downloaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mtime": final_path.stat().st_mtime if final_path.exists() else None,
    }
    if extra:
        payload.update(extra)
    manifest_path = final_path.with_name(final_path.name + MANIFEST_SUFFIX)
    try:
        manifest_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    except OSError as exc:
        raise FilesystemError(f"cannot write manifest {manifest_path}: {exc}") from exc
    return manifest_path


def read_manifest(final_path: Path) -> dict[str, Any] | None:
    manifest_path = final_path.with_name(final_path.name + MANIFEST_SUFFIX)
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
