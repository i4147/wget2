You are extending a Python multi-connection downloader (`wget2`) that currently has:
- Pluggable backends: requests, pycurl, httpx, httpx_async (hardcoded dict)
- Typer CLI, Rich progress, chunked-to-disk vs buffered modes, range planning

Refactor it into a production-ready tool. Requirements:

RELIABILITY
- --retry N, --retry-delay, exponential backoff, --retry-http codes
- --timeout, --connect-timeout, --max-redirects
- Resume via --continue/-C with .part files; validate ETag/Last-Modified
- Detect missing Range support; fall back to single stream
- Atomic finalize (.tmp → rename, fsync); --keep-partial, --remove-on-error
- --backend auto fallback; graceful SIGINT cleanup

INTEGRITY
- --checksum sha256:..., --checksum-file; content-length mismatch handling
- TLS: -k/--insecure, --ca-cert, --client-cert

HTTP
- Auth: --user/--password, --bearer-token, --netrc
- --header/-H (repeatable), --user-agent, --referer
- --cookie, --cookie-jar
- --method, --data, --data-binary
- --proxy, --no-proxy, honor HTTP(S)_PROXY
- --mirror list with failover

OUTPUT
- --output/-O, --output-template, --output-dir/-P, Content-Disposition
- --no-clobber, --overwrite, --timestamping
- --quiet/-q, --verbose/-v, --no-progress, --progress=rich|plain|none
- --json, --log-file, --log-level
- Manifest sidecar (URL, ETag, checksum, timestamp)

BATCH
- --jobs/-j N for parallel URLs; --max-active; per-host connection limit
- stdin via -f -, --input-format json|csv, --base-url
- --recursive/-r, --mirror, --level, --spider
- --accept/--reject, --include/--exclude

PERFORMANCE
- Single event loop for httpx_async (remove per-range asyncio.run)
- HTTP/2 + connection pooling; adaptive connection count

RATE / RESOURCE
- --rate-limit (per-connection and global), --max-filesize, disk-space precheck

EXTENSIBILITY
- Plugin discovery via importlib.metadata entry points
- --list-backends, --backend-args

OPS / UX
- --config FILE (TOML/YAML) + env overrides
- --dry-run (HEAD only), --version, --check
- Batch summary report; aggregate progress for multi-URL
- Lock file / single-instance guard; documented exit codes per error class

SECURITY
- Sanitize URL-derived filenames (path traversal, control chars)
- Scheme allowlist; refuse file:///ftp:// unless enabled
- Redact credentials in logs

PACKAGING
- pyproject.toml with `wget2` entry point; optional extras per backend
- Unit + integration tests (local HTTP server); mypy, ruff, coverage

Prioritize: resume → retries/timeouts → atomic cleanup → checksums → auth/headers/proxy → rate limit → existing-file policy → structured logging → global concurrency.
Preserve existing behavior and public CLI where possible; keep backends swappable.