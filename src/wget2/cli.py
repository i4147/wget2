"""Typer command line interface for wget2."""

from __future__ import annotations

import csv
import json
import signal
import sys
from pathlib import Path
from typing import Annotated, Any, Callable, Optional
from urllib.parse import urljoin, urlparse

import typer

from . import __version__
from .backends.registry import REGISTRY, parse_backend_args
from .batch import BatchRunner, render_summary
from .checksum import load_checksum_file, parse_checksums
from .config import prescan_config_paths, resolve_defaults
from .cookies import CookieStore
from .crawler import Crawler, CrawlOptions, FilterOptions
from .downloader import DownloadContext
from .errors import (
    EXIT_CODE_DOC,
    EXIT_ERROR,
    EXIT_OK,
    EXIT_USAGE,
    ConfigError,
    UsageError,
    Wget2Error,
)
from .lock import FileLock
from .log import get_logger, setup_logging
from .output import OutputOptions, validate_template
from .progress import make_reporter
from .ratelimit import parse_rate
from .request import RequestConfig, parse_cookies, parse_headers
from .settings import Settings
from .util import (
    free_space,
    human_size,
    parse_size,
    redact,
    validate_url,
)

EPILOG = """\n\nExit codes:\n""" + "\n".join(f"  {code:<4} {text}" for code, text in sorted(EXIT_CODE_DOC.items()))


# --------------------------------------------------------------------------- #
# input collection
# --------------------------------------------------------------------------- #
def read_urls_from_stdin() -> list[str]:
    return [line.strip() for line in sys.stdin.read().splitlines() if line.strip() and not line.startswith("#")]


def read_urls_from_file(path: Path, input_format: str = "auto") -> list[str]:
    """Read URLs from a text, JSON or CSV file (``-f -`` reads stdin)."""
    if str(path) == "-":
        text = sys.stdin.read()
        suffix = "text"
    else:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise UsageError(f"cannot read URL list {path}: {exc}") from exc
        suffix = path.suffix.lower().lstrip(".")

    fmt = (
        input_format
        if input_format != "auto"
        else ("json" if suffix == "json" else "csv" if suffix == "csv" else "text")
    )
    if fmt == "json":
        try:
            data = json.loads(text or "[]")
        except ValueError as exc:
            raise UsageError(f"invalid JSON in {path}: {exc}") from exc
        if isinstance(data, dict):
            data = data.get("urls") or data.get("files") or []
        urls: list[str] = []
        for item in data:
            if isinstance(item, str):
                urls.append(item)
            elif isinstance(item, dict):
                candidate = item.get("url") or item.get("href")
                if candidate:
                    urls.append(str(candidate))
        return urls
    if fmt == "csv":
        urls = []
        reader = csv.DictReader(text.splitlines())
        for row in reader:
            candidate = row.get("url") or row.get("URL") or row.get("href")
            if not candidate and row:
                candidate = next(iter(row.values()), None)
            if candidate:
                urls.append(str(candidate).strip())
        return [u for u in urls if u]
    return [line.strip() for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")]


def resolve_urls(raw_urls: list[str], base_url: str | None, allow_unsafe: bool) -> list[str]:
    """Normalize, resolve against ``--base-url``, validate and de-duplicate."""
    resolved: list[str] = []
    seen: set[str] = set()
    for raw in raw_urls:
        candidate = raw.strip()
        if not candidate:
            continue
        if base_url and not urlparse(candidate).scheme:
            candidate = urljoin(base_url if base_url.endswith("/") else base_url + "/", candidate)
        validated = validate_url(candidate, allow_extra_schemes=allow_unsafe)
        if validated in seen:
            continue
        seen.add(validated)
        resolved.append(validated)
    return resolved


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _echo(message: str = "", *, err: bool = False) -> None:
    typer.echo(message, err=err)


def _list_backends(json_output: bool) -> None:
    infos = REGISTRY.describe()
    if json_output:
        payload = [
            {
                "name": info.name,
                "source": info.source,
                "available": info.available,
                "reason": info.reason,
            }
            for info in infos
        ]
        _echo(json.dumps({"backends": payload, "default": Settings.backend}, indent=2))
        return
    _echo("Available backends:")
    for info in infos:
        marker = "✔" if info.available else "✖"
        detail = "" if info.available else f"  ({info.reason})"
        _echo(f"  {marker} {info.name:<12} {info.source}{detail}")
    _echo(f"\nauto -> {REGISTRY.resolve_auto()}")


def _self_check(output_dir: Path, backend: str) -> int:
    _echo(f"wget2 {__version__}")
    _echo(f"python {sys.version.split()[0]} on {sys.platform}")
    failures = 0
    infos = REGISTRY.describe()
    for info in infos:
        marker = "✔" if info.available else "✖"
        _echo(f"  {marker} backend {info.name:<12} {info.reason or 'ready'}")
        if not info.available and info.name != "auto":
            continue
    if not any(info.available for info in infos):
        failures += 1
    try:
        requested = REGISTRY.canonical(backend)
        chosen = requested if requested != "auto" else REGISTRY.resolve_auto()
        _echo(f"  ✔ selected backend {chosen}")
    except Wget2Error as exc:
        _echo(f"  ✖ backend selection failed: {exc}")
        failures += 1
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        probe_file = output_dir / ".wget2-check"
        probe_file.write_text("ok", encoding="utf-8")
        probe_file.unlink()
        _echo(f"  ✔ output directory writable: {output_dir}")
    except OSError as exc:
        _echo(f"  ✖ output directory not writable: {exc}")
        failures += 1
    _echo(f"  · free space in {output_dir}: {human_size(free_space(output_dir))}")
    _echo("OK" if not failures else f"{failures} problem(s) found")
    return EXIT_OK if not failures else EXIT_ERROR


def install_signal_handlers(interrupt: Any) -> Callable[[], None]:
    """Route SIGINT/SIGTERM to the cooperative interrupt flag.

    The first signal finishes the current chunk and cleans up; a second one
    raises :class:`KeyboardInterrupt` for an immediate exit.
    """

    def handler(signum: int, frame: Any) -> None:
        if interrupt.is_set():
            raise KeyboardInterrupt
        interrupt.set()
        _echo(
            "\ninterrupt received: stopping safely (press Ctrl+C again to force quit)",
            err=True,
        )

    previous: list[tuple[int, Any]] = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            previous.append((sig, signal.getsignal(sig)))
            signal.signal(sig, handler)
        except (ValueError, OSError):  # pragma: no cover - non-main thread
            pass

    def restore() -> None:
        for sig, old in previous:
            try:
                signal.signal(sig, old)
            except (ValueError, OSError, TypeError):  # pragma: no cover
                pass

    return restore


# --------------------------------------------------------------------------- #
# application factory
# --------------------------------------------------------------------------- #
def build_app(defaults: dict[str, Any]) -> typer.Typer:
    cfg = defaults

    def _d(key: str, default: Any) -> Any:
        value = cfg.get(key, default)
        return default if value is None else value

    def _raw(key: str) -> Optional[str]:
        """Config values are typed; CLI size/rate options want strings."""
        value = cfg.get(key)
        if value is None or value == "":
            return None
        return str(value)

    app = typer.Typer(
        name="wget2",
        help=(
            "Multi-connection downloader with pluggable HTTP backends.\n\n"
            "Reliability: resume (-C), retries with backoff, atomic finalize, checksums, "
            "structured logging, and per-host/global concurrency limits.\n\n"
            "Exit codes are documented in --exit-codes and in the README."
        ),
        add_completion=False,
        rich_markup_mode="rich",
        epilog=EPILOG,
        context_settings={"help_option_names": ["-h", "--help"], "allow_extra_args": True},
    )

    @app.command()
    def main(  # noqa: PLR0913 - CLI surface
        urls: Annotated[
            Optional[list[str]],
            typer.Argument(help="URLs to download (omit when using -f)."),
        ] = None,
        # ---------------- backend ---------------- #
        backend: Annotated[
            str,
            typer.Option(
                "--backend",
                "-b",
                envvar="WGET2_BACKEND",
                help="pycurl (default), httpx, requests, httpx_async or auto.",
            ),
        ] = _d("backend", "pycurl"),
        connections: Annotated[
            int,
            typer.Option("--connections", "-c", envvar="WGET2_CONNECTIONS", help="Parallel connections per URL."),
        ] = int(_d("connections", 8)),
        max_per_host: Annotated[
            int,
            typer.Option(
                "--max-per-host",
                envvar="WGET2_MAX_PER_HOST",
                help="Max concurrent requests per host (0 = unlimited).",
            ),
        ] = int(_d("max_per_host", 6)),
        backend_args: Annotated[
            Optional[list[str]],
            typer.Option("--backend-args", help="Extra backend options as key=value (repeatable)."),
        ] = None,
        backend_auto_fallback: Annotated[
            bool,
            typer.Option(
                "--backend-auto-fallback/--no-backend-auto-fallback",
                help="Retry a file with the next available backend if the first one fails.",
            ),
        ] = bool(_d("backend_auto_fallback", False)),
        list_backends: Annotated[
            bool, typer.Option("--list-backends", help="List available backends and exit.")
        ] = False,
        # ---------------- reliability ---------------- #
        retry: Annotated[
            int, typer.Option("--retry", envvar="WGET2_RETRY", help="Retries per request (0 disables).")
        ] = int(_d("retry", 5)),
        retry_delay: Annotated[float, typer.Option("--retry-delay", help="Initial backoff delay in seconds.")] = float(
            _d("retry_delay", 1.0)
        ),
        retry_backoff: Annotated[
            float, typer.Option("--retry-backoff", help="Exponential backoff multiplier.")
        ] = float(_d("retry_backoff", 2.0)),
        retry_max_delay: Annotated[
            float, typer.Option("--retry-max-delay", help="Upper bound for a backoff delay.")
        ] = float(_d("retry_max_delay", 60.0)),
        retry_jitter: Annotated[
            float, typer.Option("--retry-jitter", help="Random jitter fraction applied to delays (0-1).")
        ] = float(_d("retry_jitter", 0.25)),
        retry_http: Annotated[
            str, typer.Option("--retry-http", help="HTTP statuses to retry, e.g. '429,500-504'.")
        ] = str(_d("retry_http", "408,425,429,500,502,503,504")),
        timeout: Annotated[float, typer.Option("--timeout", help="Read/stall timeout in seconds.")] = float(
            _d("timeout", 30.0)
        ),
        connect_timeout: Annotated[
            float, typer.Option("--connect-timeout", help="Connection timeout in seconds.")
        ] = float(_d("connect_timeout", 10.0)),
        max_redirects: Annotated[int, typer.Option("--max-redirects", help="Maximum redirects to follow.")] = int(
            _d("max_redirects", 10)
        ),
        continue_: Annotated[bool, typer.Option("--continue", "-C", help="Resume from .part files when valid.")] = bool(
            _d("continue_", False)
        ),
        allow_unvalidated_resume: Annotated[
            bool, typer.Option("--allow-unvalidated-resume", help="Resume even without ETag/Last-Modified.")
        ] = bool(_d("allow_unvalidated_resume", False)),
        keep_partial: Annotated[
            bool, typer.Option("--keep-partial/--no-keep-partial", help="Keep .part files after a failure.")
        ] = bool(_d("keep_partial", True)),
        remove_on_error: Annotated[
            bool, typer.Option("--remove-on-error", help="Delete partial files when a download fails.")
        ] = bool(_d("remove_on_error", False)),
        check_disk: Annotated[
            bool, typer.Option("--check-disk/--no-check-disk", help="Pre-check free disk space.")
        ] = bool(_d("check_disk", True)),
        # ---------------- integrity ---------------- #
        checksum: Annotated[
            Optional[list[str]],
            typer.Option("--checksum", help="Expected checksum, e.g. sha256:<hex> (repeatable)."),
        ] = None,
        checksum_file: Annotated[
            Optional[Path],
            typer.Option("--checksum-file", exists=True, dir_okay=False, readable=True, help="sha256sum-style file."),
        ] = None,
        # ---------------- auth / headers ---------------- #
        user: Annotated[Optional[str], typer.Option("--user", help="HTTP basic auth user.")] = _d("user", None),
        password: Annotated[Optional[str], typer.Option("--password", help="HTTP basic auth password.")] = _d(
            "password", None
        ),
        bearer_token: Annotated[
            Optional[str],
            typer.Option("--bearer-token", envvar="WGET2_BEARER_TOKEN", help="Authorization: Bearer token."),
        ] = _d("bearer_token", None),
        netrc: Annotated[bool, typer.Option("--netrc", help="Use credentials from ~/.netrc.")] = bool(
            _d("netrc", False)
        ),
        netrc_file: Annotated[
            Optional[Path], typer.Option("--netrc-file", dir_okay=False, help="Alternate netrc file.")
        ] = _d("netrc_file", None),
        header: Annotated[
            Optional[list[str]],
            typer.Option("--header", "-H", help="Extra request header 'Name: value' (repeatable)."),
        ] = cfg.get("header"),
        user_agent: Annotated[Optional[str], typer.Option("--user-agent", "-U", help="User-Agent header.")] = _d(
            "user_agent", None
        ),
        referer: Annotated[Optional[str], typer.Option("--referer", help="Referer header.")] = _d("referer", None),
        cookie: Annotated[Optional[str], typer.Option("--cookie", help="Cookie header value ('a=1; b=2').")] = _d(
            "cookie", None
        ),
        cookie_jar: Annotated[
            Optional[Path],
            typer.Option("--cookie-jar", dir_okay=False, help="Load/save cookies (Netscape format)."),
        ] = _d("cookie_jar", None),
        method: Annotated[str, typer.Option("--method", "-X", help="HTTP method.")] = _d("method", "GET"),
        data: Annotated[Optional[str], typer.Option("--data", "-d", help="Request body (@file supported).")] = _d(
            "data", None
        ),
        data_binary: Annotated[Optional[str], typer.Option("--data-binary", help="Request body sent verbatim.")] = _d(
            "data_binary", None
        ),
        # ---------------- transport ---------------- #
        proxy: Annotated[Optional[str], typer.Option("--proxy", envvar="WGET2_PROXY", help="Proxy URL.")] = _d(
            "proxy", None
        ),
        no_proxy: Annotated[bool, typer.Option("--no-proxy", help="Ignore HTTP(S)_PROXY and --proxy.")] = bool(
            _d("no_proxy", False)
        ),
        insecure: Annotated[bool, typer.Option("--insecure", "-k", help="Skip TLS verification.")] = bool(
            _d("insecure", False)
        ),
        ca_cert: Annotated[Optional[Path], typer.Option("--ca-cert", dir_okay=False, help="CA bundle path.")] = _d(
            "ca_cert", None
        ),
        client_cert: Annotated[
            Optional[Path], typer.Option("--client-cert", dir_okay=False, help="Client certificate (PEM).")
        ] = _d("client_cert", None),
        client_key: Annotated[
            Optional[Path], typer.Option("--client-key", dir_okay=False, help="Client private key (PEM).")
        ] = _d("client_key", None),
        http2: Annotated[bool, typer.Option("--http2/--no-http2", help="Enable HTTP/2 when supported.")] = bool(
            _d("http2", True)
        ),
        # ---------------- output ---------------- #
        output: Annotated[
            Optional[Path],
            typer.Option("--output", "-O", dir_okay=False, help="Output filename (single URL)."),
        ] = _d("output", None),
        output_template: Annotated[
            Optional[str],
            typer.Option("--output-template", help="Per-URL path template, e.g. '{host}/{filename}'."),
        ] = _d("output_template", None),
        output_dir: Annotated[Path, typer.Option("--output-dir", "-P", "-o", help="Directory for downloads.")] = Path(
            _d("output_dir", ".")
        ),
        no_clobber: Annotated[bool, typer.Option("--no-clobber", "-n", help="Never overwrite existing files.")] = bool(
            _d("no_clobber", False)
        ),
        overwrite: Annotated[
            bool, typer.Option("--overwrite/--no-overwrite", help="Overwrite existing files (default).")
        ] = bool(_d("overwrite", True)),
        timestamping: Annotated[
            bool, typer.Option("--timestamping", "-N", help="Skip files whose local copy is newer.")
        ] = bool(_d("timestamping", False)),
        content_disposition: Annotated[
            bool,
            typer.Option(
                "--content-disposition/--no-content-disposition", help="Honour Content-Disposition filenames."
            ),
        ] = bool(_d("content_disposition", True)),
        unique: Annotated[bool, typer.Option("--unique", help="Never clobber: add .1, .2 suffixes instead.")] = bool(
            _d("unique", False)
        ),
        manifest: Annotated[
            bool, typer.Option("--manifest/--no-manifest", help="Write a <file>.wget2.json sidecar.")
        ] = bool(_d("manifest", False)),
        # ---------------- logging / UX ---------------- #
        quiet: Annotated[bool, typer.Option("--quiet", "-q", help="Only report errors.")] = bool(_d("quiet", False)),
        verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Debug-level logging.")] = bool(
            _d("verbose", False)
        ),
        progress: Annotated[str, typer.Option("--progress", help="rich | plain | none.")] = _d("progress", "rich"),
        no_progress: Annotated[bool, typer.Option("--no-progress", help="Same as --progress=none.")] = bool(
            _d("no_progress", False)
        ),
        json_report: Annotated[bool, typer.Option("--json", help="Emit a JSON report on stdout.")] = bool(
            _d("json", False)
        ),
        log_file: Annotated[
            Optional[Path], typer.Option("--log-file", dir_okay=False, help="Write logs to a file.")
        ] = _d("log_file", None),
        log_level: Annotated[str, typer.Option("--log-level", help="critical|error|warning|info|debug.")] = _d(
            "log_level", "info"
        ),
        log_json: Annotated[bool, typer.Option("--log-json", help="Structured JSON logs.")] = bool(
            _d("log_json", False)
        ),
        config: Annotated[
            Optional[list[Path]], typer.Option("--config", help="TOML/YAML config file (repeatable).")
        ] = None,
        # ---------------- batch ---------------- #
        file: Annotated[
            Optional[Path], typer.Option("--file", "-f", help="Read URLs from FILE ('-' for stdin).")
        ] = None,
        input_format: Annotated[str, typer.Option("--input-format", help="auto | text | json | csv.")] = _d(
            "input_format", "auto"
        ),
        base_url: Annotated[
            Optional[str], typer.Option("--base-url", help="Resolve relative URLs against this base.")
        ] = _d("base_url", None),
        mirror_list: Annotated[
            Optional[list[str]],
            typer.Option(
                "--mirror-list",
                help="Fallback URL(s) tried in order when a download fails (repeatable/comma separated).",
            ),
        ] = cfg.get("mirror_list"),
        jobs: Annotated[int, typer.Option("--jobs", "-j", help="URLs to download in parallel.")] = int(_d("jobs", 1)),
        max_active: Annotated[
            Optional[int], typer.Option("--max-active", help="Hard cap on concurrent file downloads.")
        ] = _d("max_active", None),
        # ---------------- crawling ---------------- #
        recursive: Annotated[bool, typer.Option("--recursive", "-r", help="Follow links (same host).")] = bool(
            _d("recursive", False)
        ),
        mirror: Annotated[
            bool, typer.Option("--mirror", "-m", help="Recursive + timestamping + keep structure.")
        ] = bool(_d("mirror", False)),
        level: Annotated[int, typer.Option("--level", "-l", help="Maximum recursion depth.")] = int(_d("level", 5)),
        spider: Annotated[bool, typer.Option("--spider", help="Check URLs only; download nothing.")] = bool(
            _d("spider", False)
        ),
        dry_run: Annotated[bool, typer.Option("--dry-run", help="Probe (HEAD) and show what would happen.")] = bool(
            _d("dry_run", False)
        ),
        accept: Annotated[
            Optional[list[str]], typer.Option("--accept", "-A", help="Accept only these name patterns.")
        ] = cfg.get("accept"),
        reject: Annotated[
            Optional[list[str]], typer.Option("--reject", "-R", help="Reject these name patterns.")
        ] = cfg.get("reject"),
        include: Annotated[
            Optional[list[str]], typer.Option("--include", "-I", help="Include URLs matching these regexes.")
        ] = cfg.get("include"),
        exclude: Annotated[
            Optional[list[str]], typer.Option("--exclude", "-E", help="Exclude URLs matching these regexes.")
        ] = cfg.get("exclude"),
        # ---------------- resources ---------------- #
        rate_limit: Annotated[
            Optional[str], typer.Option("--rate-limit", help="Global speed cap, e.g. '5M' bytes/s.")
        ] = _raw("rate_limit"),
        rate_limit_per_connection: Annotated[
            Optional[str], typer.Option("--rate-limit-per-connection", help="Per-connection speed cap.")
        ] = _raw("rate_limit_per_connection"),
        max_filesize: Annotated[
            Optional[str], typer.Option("--max-filesize", help="Refuse files larger than this, e.g. '2G'.")
        ] = _raw("max_filesize"),
        buffer_threshold: Annotated[
            Optional[str], typer.Option("--buffer-threshold", help="Files up to this size use buffered mode.")
        ] = _raw("buffer_threshold"),
        # ---------------- ops ---------------- #
        lock_file: Annotated[
            Optional[Path],
            typer.Option("--lock-file", dir_okay=False, help="Lock file path (default: <output-dir>/.wget2.lock)."),
        ] = _d("lock_file", None),
        no_lock: Annotated[bool, typer.Option("--no-lock", help="Disable the single-instance guard.")] = bool(
            _d("no_lock", False)
        ),
        allow_unsafe_schemes: Annotated[
            bool, typer.Option("--allow-unsafe-schemes", help="Permit file:// and ftp:// URLs.")
        ] = bool(_d("allow_unsafe_schemes", False)),
        check: Annotated[bool, typer.Option("--check", help="Run environment self-checks and exit.")] = False,
        exit_codes: Annotated[bool, typer.Option("--exit-codes", help="Print the exit-code table and exit.")] = False,
        version: Annotated[
            Optional[bool],
            typer.Option("--version", "-V", is_eager=True, callback=_version_callback, help="Show version and exit."),
        ] = None,
    ) -> None:
        exit_code = run(locals())
        raise typer.Exit(code=exit_code)

    return app


def _version_callback(value: bool) -> bool:
    if value:
        typer.echo(f"wget2 {__version__}")
        raise typer.Exit(code=EXIT_OK)
    return value


# --------------------------------------------------------------------------- #
# main orchestration
# --------------------------------------------------------------------------- #
def run(options: dict[str, Any]) -> int:
    """Turn parsed CLI options into Settings and execute the run."""
    quiet = bool(options.get("quiet"))
    verbose = bool(options.get("verbose"))
    json_report = bool(options.get("json_report"))
    log_level = str(options.get("log_level") or "info")
    log_file = options.get("log_file")
    progress_mode = str(options.get("progress") or "rich").lower()
    if options.get("no_progress"):
        progress_mode = "none"
    if progress_mode not in {"rich", "plain", "none"}:
        typer.echo(f"error: unknown --progress mode '{progress_mode}'", err=True)
        return EXIT_USAGE
    if quiet and progress_mode == "rich":
        progress_mode = "none"
    setup_logging(
        level=log_level,
        json_logs=bool(options.get("log_json")),
        log_file=Path(log_file) if log_file else None,
        quiet=quiet,
        verbose=verbose,
        force=True,
    )
    log = get_logger("cli")

    output_dir = Path(options.get("output_dir") or ".")
    backend = str(options.get("backend") or "pycurl")

    if options.get("exit_codes"):
        typer.echo(EPILOG.strip())
        return EXIT_OK
    if options.get("list_backends"):
        _list_backends(json_report)
        return EXIT_OK
    if options.get("check"):
        return _self_check(output_dir, backend)

    try:
        # ---------------- validate backend ----------------
        try:
            canonical = REGISTRY.canonical(backend)
            if canonical == "auto":
                resolved_backend = REGISTRY.resolve_auto()
            elif not REGISTRY.is_available(canonical):
                # the default backend is pycurl, an optional dependency: fall back
                # instead of failing when the extra is not installed.
                reason = REGISTRY.get(canonical).availability_reason()
                resolved_backend = REGISTRY.resolve_auto()
                get_logger("cli").warning(
                    "requested backend unavailable, using fallback",
                    backend=canonical,
                    reason=reason,
                    fallback=resolved_backend,
                )
            else:
                resolved_backend = canonical
        except Wget2Error as exc:
            typer.echo(f"error: {exc}", err=True)
            return exc.exit_code if isinstance(exc, Wget2Error) else EXIT_USAGE

        # ---------------- URL collection ----------------
        raw_urls: list[str] = list(options.get("urls") or [])
        file_path = options.get("file")
        if file_path is not None:
            raw_urls.extend(read_urls_from_file(Path(file_path), str(options.get("input_format") or "auto")))
        if not raw_urls:
            typer.echo("error: provide a URL or use -f FILE", err=True)
            return EXIT_USAGE
        urls = resolve_urls(
            raw_urls,
            str(options["base_url"]) if options.get("base_url") else None,
            bool(options.get("allow_unsafe_schemes")),
        )
        if not urls:
            typer.echo("error: no valid URLs to download", err=True)
            return EXIT_USAGE

        # ---------------- settings ----------------
        template = options.get("output_template")
        if template:
            validate_template(str(template))
        mirror = bool(options.get("mirror"))
        recursive = bool(options.get("recursive")) or mirror
        output_options = OutputOptions(
            output_dir=output_dir,
            output=Path(options["output"]) if options.get("output") else None,
            template=str(template) if template else ("{host}/{path}" if mirror and not template else None),
            no_clobber=bool(options.get("no_clobber")),
            overwrite=bool(options.get("overwrite", True)),
            timestamping=bool(options.get("timestamping")) or mirror,
            use_content_disposition=bool(options.get("content_disposition", True)),
            unique=bool(options.get("unique")),
            manifest=bool(options.get("manifest")),
        )
        output_options.validate(len(urls))

        cookie_store = CookieStore()
        if options.get("cookie"):
            cookie_store.load_inline(str(options["cookie"]))
        cookie_jar = Path(options["cookie_jar"]) if options.get("cookie_jar") else None
        if cookie_jar is not None:
            cookie_store.load_file(cookie_jar)

        request_config = RequestConfig(
            headers=parse_headers(options.get("header")),
            user_agent=str(options.get("user_agent") or RequestConfig().user_agent),
            referer=str(options["referer"]) if options.get("referer") else None,
            user=str(options["user"]) if options.get("user") is not None else None,
            password=str(options["password"]) if options.get("password") is not None else None,
            bearer_token=str(options["bearer_token"]) if options.get("bearer_token") else None,
            use_netrc=bool(options.get("netrc")),
            netrc_file=str(options["netrc_file"]) if options.get("netrc_file") else None,
            cookies=parse_cookies(str(options["cookie"]) if options.get("cookie") else None),
            proxy=str(options["proxy"]) if options.get("proxy") else None,
            no_proxy=bool(options.get("no_proxy")),
            insecure=bool(options.get("insecure")),
            ca_cert=str(options["ca_cert"]) if options.get("ca_cert") else None,
            client_cert=str(options["client_cert"]) if options.get("client_cert") else None,
            client_key=str(options["client_key"]) if options.get("client_key") else None,
            timeout=float(options.get("timeout") or 30.0),
            connect_timeout=float(options.get("connect_timeout") or 10.0),
            max_redirects=int(options.get("max_redirects") or 10),
            method=str(options.get("method") or "GET").upper(),
            data=_request_body(options),
            http2=bool(options.get("http2", True)),
            cookie_store=cookie_store,
        )

        checksum_specs = parse_checksums(options.get("checksum") or [])
        checksum_table = load_checksum_file(Path(options["checksum_file"])) if options.get("checksum_file") else {}

        jobs = max(1, int(options.get("jobs") or 1))
        max_active = options.get("max_active")
        max_active_value = max(1, int(max_active)) if max_active else jobs
        max_active_value = min(max_active_value, jobs)

        retry_http = options.get("retry_http")
        settings = Settings(
            backend=resolved_backend,
            backend_auto_fallback=bool(options.get("backend_auto_fallback")) or canonical == "auto",
            backend_extra=parse_backend_args(options.get("backend_args")),
            connections=max(1, int(options.get("connections") or 1)),
            max_per_host=int(options.get("max_per_host") or 0),
            jobs=jobs,
            max_active=max_active_value,
            retry=max(0, int(options.get("retry") or 0)),
            retry_delay=float(options.get("retry_delay") or 0.0),
            retry_backoff=float(options.get("retry_backoff") or 1.0),
            retry_max_delay=float(options.get("retry_max_delay") or 0.0),
            retry_jitter=float(options.get("retry_jitter") or 0.0),
            retry_http=_retry_statuses(retry_http),
            timeout=float(options.get("timeout") or 0.0),
            connect_timeout=float(options.get("connect_timeout") or 0.0),
            max_redirects=int(options.get("max_redirects") or 0),
            continue_=bool(options.get("continue_")),
            allow_unvalidated_resume=bool(options.get("allow_unvalidated_resume")),
            keep_partial=bool(options.get("keep_partial", True)),
            remove_on_error=bool(options.get("remove_on_error")),
            check_disk=bool(options.get("check_disk", True)),
            checksums=tuple(checksum_specs),
            checksum_table=checksum_table,
            request=request_config,
            output=output_options,
            manifest=bool(options.get("manifest")),
            progress_mode=progress_mode,
            json_report=json_report,
            log_level=log_level,
            log_file=Path(log_file) if log_file else None,
            json_logs=bool(options.get("log_json")),
            quiet=quiet,
            verbose=verbose,
            max_filesize=parse_size(options.get("max_filesize")),
            rate_limit=parse_rate(options.get("rate_limit")),
            rate_limit_per_connection=parse_rate(options.get("rate_limit_per_connection")),
            buffer_threshold=parse_size(options.get("buffer_threshold")) or Settings().buffer_threshold,
            recursive=recursive,
            mirror=mirror,
            level=int(options.get("level") or 5),
            spider=bool(options.get("spider")),
            dry_run=bool(options.get("dry_run")),
            accept=tuple(options.get("accept") or ()),
            reject=tuple(options.get("reject") or ()),
            include=tuple(options.get("include") or ()),
            exclude=tuple(options.get("exclude") or ()),
            base_url=str(options["base_url"]) if options.get("base_url") else None,
            allow_unsafe_schemes=bool(options.get("allow_unsafe_schemes")),
            mirrors=_mirror_urls(options.get("mirror_list"), options.get("base_url")),
        )
    except Wget2Error as exc:
        typer.echo(f"error: {redact(exc)}", err=True)
        return exc.exit_code
    except (ValueError, OSError) as exc:
        typer.echo(f"error: {exc}", err=True)
        return EXIT_USAGE

    lock_path = Path(options["lock_file"]) if options.get("lock_file") else output_dir / ".wget2.lock"
    lock = FileLock(lock_path, enabled=not options.get("no_lock") and not settings.dry_run and not settings.spider)
    try:
        lock.acquire()
    except Wget2Error as exc:
        typer.echo(f"error: {exc}", err=True)
        return exc.exit_code

    reporter = make_reporter(
        settings.progress_mode,
        show_overall=len(urls) > 1,
    )
    context = DownloadContext.build(settings, REGISTRY, reporter)
    restore_signals = install_signal_handlers(context.interrupt)
    exit_code = EXIT_OK
    summary = None
    try:
        if recursive:
            filters = FilterOptions(
                accept=settings.accept,
                reject=settings.reject,
                include=settings.include,
                exclude=settings.exclude,
            )
            crawler = Crawler(
                context,
                context.pool.acquire(resolved_backend),
                CrawlOptions(
                    level=settings.level,
                    mirror=mirror,
                    recursive=True,
                    filters=filters,
                ),
            )
            log.info("crawling", seeds=len(urls), level=settings.level, mirror=mirror)
            discovered = crawler.discover(urls)
            log.info("crawl finished", urls=len(discovered))
            urls = discovered or urls
        summary = BatchRunner(context).run(urls)
        exit_code = summary.exit_code
    except Wget2Error as exc:
        typer.echo(f"error: {redact(exc)}", err=True)
        exit_code = exc.exit_code
    except KeyboardInterrupt:  # pragma: no cover - hard interrupt
        typer.echo("interrupted", err=True)
        exit_code = 130
    finally:
        context.close()
        reporter.close()
        restore_signals()
        if cookie_jar is not None:
            try:
                cookie_store.save(cookie_jar)
            except Wget2Error as exc:  # pragma: no cover - best effort
                log.warning("cannot save cookie jar", error=str(exc))
        lock.release()

    if summary is not None:
        if json_report:
            typer.echo(json.dumps(summary.to_dict(), indent=2))
            if summary.failed and not quiet:
                typer.echo(render_summary(summary, quiet=False), err=True)
        elif not quiet:
            typer.echo(render_summary(summary, quiet=False))
    return exit_code


def _mirror_urls(values: Any, base_url: Any) -> tuple[str, ...]:
    """Parse repeatable/comma separated --mirror-list values."""
    out: list[str] = []
    for value in values or ():
        for item in str(value).split(","):
            item = item.strip()
            if not item:
                continue
            if base_url and not urlparse(item).scheme:
                item = urljoin(str(base_url), item)
            out.append(item)
    return tuple(dict.fromkeys(out))


def _retry_statuses(value: Any):
    from .retry import parse_retry_status

    try:
        return parse_retry_status(value)
    except Wget2Error as exc:
        raise ConfigError(f"invalid --retry-http value: {exc}") from exc


def _request_body(options: dict[str, Any]) -> bytes | None:
    from .request import read_data_argument

    if options.get("data_binary"):
        return read_data_argument(str(options["data_binary"]), binary=True)
    if options.get("data") is not None:
        return read_data_argument(str(options["data"]), binary=False)
    return None


# --------------------------------------------------------------------------- #
# entry points
# --------------------------------------------------------------------------- #
def cli(argv: list[str] | None = None) -> None:
    """Console-script entry point (``wget2``)."""
    config_paths = prescan_config_paths(argv)
    defaults = resolve_defaults(config_paths)
    app = build_app(defaults)
    app(args=argv, prog_name="wget2")


def main() -> None:  # pragma: no cover - thin wrapper
    cli()


if __name__ == "__main__":  # pragma: no cover
    cli()
