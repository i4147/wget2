# wget2

A production-grade multi-connection HTTP/HTTPS downloader for the command line, with
pluggable backends, resumable transfers, retries, checksums, structured logging and
batch scheduling.

```
$ wget2 -c 8 -C --retry 5 --checksum sha256:9f2c... -P downloads https://example.com/big.iso
```

* **Swappable HTTP engines** — pycurl (default), httpx, requests, httpx_async, or
  `--backend auto`; third-party backends load through entry points.
* **Resume anything** — `.part` files with byte-coverage tracking, ETag/Last-Modified
  validation, and atomic finalize (`fsync` + rename).
* **Reliability first** — exponential backoff with jitter, retry-status filtering,
  connect/stall timeouts, redirect caps, range-support detection with single-stream
  fallback, graceful SIGINT cleanup.
* **Integrity** — SHA-256/512/MD5/BLAKE2 checksums from the CLI or a `sha256sum` file,
  content-length verification, and a JSON manifest sidecar.
* **Batch native** — `-j` files in parallel, per-host connection limits, `stdin`/JSON/CSV
  input, recursive/mirror crawling, and an aggregate progress display.
* **Ops friendly** — TOML/YAML config, `WGET2_*` env overrides, JSON reports,
  documented exit codes, redacted credentials, and a single-instance lock.

---

## Install

```bash
pip install wget2                # typer + rich only (needs one backend extra)
pip install 'wget2[pycurl]'      # default backend
pip install 'wget2[httpx]'       # HTTP/2 via httpx (sync + async backends)
pip install 'wget2[requests]'
pip install 'wget2[all]'         # everything, plus YAML config support
```

From a checkout:

```bash
pip install -e '.[dev]'
wget2 --check                    # environment self-test
```

Python 3.10+.

---

## Quick start

```bash
# one file, 8 connections, resume if a .part file exists
wget2 -c 8 -C https://example.com/large.iso -P ~/Downloads

# authenticated download to a chosen filename, verified by checksum
wget2 -O report.pdf --bearer-token "$TOKEN" \
      --checksum sha256:$SHA https://api.example.com/report

# batch: 4 files at once, 4 connections each, global rate cap, JSON report
wget2 -f urls.txt -j 4 -c 4 --rate-limit 10M --json > report.json

# mirror a small site (recursive, same host, only HTML)
wget2 -r -l 3 -A '*.html' --mirror https://docs.example.com/

# primary host down? transparently fail over to mirrors, same output name
wget2 https://primary.example.com/pkg.tar.gz \
      --mirror-list https://eu.example.com/pkg.tar.gz,https://us.example.com/pkg.tar.gz
```

Note that `--mirror-list` (fallback URLs) and `-m/--mirror` (wget-style recursive
mirroring) are different things; `--backend-auto-fallback` does the same for *engines*.

---

## Feature map

### Backends

| Flag | Meaning |
| --- | --- |
| `-b, --backend` | `pycurl` (default), `httpx`, `requests`, `httpx_async`, `auto`, or a plugin name |
| `--backend-auto-fallback` | Retry a file with the next available backend if the first one fails |
| `--backend-args key=value` | Backend-specific options (repeatable) |
| `--http2/--no-http2` | Enable HTTP/2 where supported (default on) |
| `--list-backends` | Show every backend, its source and availability |

If the selected backend's optional dependency is missing (e.g. `pycurl` is not
installed), wget2 warns and transparently falls back to the next available backend
instead of failing.

### Reliability

| Flag | Meaning |
| --- | --- |
| `--retry N` | Retries per request (default 5) |
| `--retry-delay`, `--retry-backoff`, `--retry-max-delay`, `--retry-jitter` | Backoff shape |
| `--retry-http CODES` | Status codes to retry, e.g. `429,500-504` |
| `--timeout`, `--connect-timeout`, `--max-redirects` | Stall timeout, connect timeout, redirect cap |
| `-C, --continue` | Resume from `.part` files when validators match |
| `--allow-unvalidated-resume` | Resume even without ETag/Last-Modified |
| `--keep-partial/--no-keep-partial`, `--remove-on-error` | Partial-file policy |
| `--check-disk/--no-check-disk` | Pre-flight free-space check |

Behavior notes:

* **Resume.** The `.part` file is pre-allocated to the final size and written with
  positional writes, so any byte range can be filled independently. `<file>.part.json`
  records byte coverage plus the ETag/Last-Modified/size seen at start. On `-C` the
  validators are compared: a changed ETag, Last-Modified, size, or URL restarts the
  download; otherwise only the missing ranges are fetched — even if `--connections`
  changed since the interruption.
* **Range detection.** wget2 trusts `Accept-Ranges: bytes` and *verifies* it with a
  one-byte request when the header is missing. If the server answers `200` to a ranged
  request, the transfer falls back to a single stream automatically.
* **Atomic finalize.** The body is written to `<file>.part`, `fsync`ed, checksum-verified
  and only then renamed to the final name (with a directory `fsync`), so a partial file
  never appears under the real name.
* **SIGINT.** The first Ctrl+C finishes the current chunk, flushes state and leaves a
  resumable `.part`; a second Ctrl+C exits immediately. Exit code is `130`.

### Integrity

| Flag | Meaning |
| --- | --- |
| `--checksum ALGO:HEX` | Expected digest (repeatable: `sha256:...`, `md5:...`) |
| `--checksum-file FILE` | `sha256sum`-style file, matched by filename |
| `--manifest/--no-manifest` | Write `<file>.wget2.json` (URL, ETag, size, digests, timestamp) |

A content-length/coverage mismatch, a short read, or a checksum mismatch is an
*integrity* failure (exit `5`) and never produces a finalized file.

### HTTP

| Flag | Meaning |
| --- | --- |
| `--user`, `--password`, `--bearer-token`, `--netrc`, `--netrc-file` | Authentication |
| `-H, --header` | Extra request header (repeatable) |
| `-U, --user-agent`, `--referer` | Common headers |
| `--cookie`, `--cookie-jar` | Inline cookies; Netscape-format jar load/save |
| `-X, --method`, `-d, --data`, `--data-binary` | Method and body (`@file` supported) |
| `--proxy`, `--no-proxy` | Proxy override; `HTTP(S)_PROXY` / `NO_PROXY` are honored otherwise |
| `-k, --insecure`, `--ca-cert`, `--client-cert`, `--client-key` | TLS controls |
| `--mirror-list URL` | Alternate mirror URLs tried in order when a download fails (repeatable / comma separated) |

### Output

| Flag | Meaning |
| --- | --- |
| `-O, --output` | Exact filename (single URL only) |
| `--output-template` | `{host}/{filename}`, `{path}`, `{name}`, `{stem}`, `{ext}`, `{index}`, `{size}`, `{date}`, `{url}` |
| `-P, --output-dir` (`-o` kept for compatibility) | Destination directory |
| `--content-disposition/--no-content-disposition` | Use the server-provided filename (default on) |
| `-n, --no-clobber`, `--overwrite`, `-N, --timestamping`, `--unique` | Existing-file policy |
| `-q, --quiet`, `-v, --verbose`, `--progress=rich\|plain\|none`, `--no-progress` | Verbosity |
| `--json` | Machine-readable report on stdout |
| `--log-file`, `--log-level`, `--log-json` | Log destinations and format |

### Batch, crawling and concurrency

| Flag | Meaning |
| --- | --- |
| `-f, --file` | URL list (`-` reads stdin) |
| `--input-format auto\|text\|json\|csv` | Input parsing |
| `--base-url URL` | Resolve relative URLs |
| `-j, --jobs N`, `--max-active N`, `--max-per-host N` | Global file concurrency, hard cap, per-host request cap |
| `-r, --recursive`, `-m, --mirror`, `-l, --level` | Breadth-first same-host crawling |
| `-A/--accept`, `-R/--reject`, `-I/--include`, `-E/--exclude` | Name globs and URL regexes |
| `--spider`, `--dry-run` | Probe only (HEAD), download nothing |

### Resources

| Flag | Meaning |
| --- | --- |
| `--rate-limit 5M` | Aggregate bytes/second |
| `--rate-limit-per-connection 1M` | Per-connection bytes/second |
| `--max-filesize 2G` | Refuse larger resources before downloading |
| `--buffer-threshold` | Files below this size are assembled in memory (default 10 MiB) |

### Operations

| Flag | Meaning |
| --- | --- |
| `--config FILE` | TOML/YAML defaults (repeatable); also `WGET2_CONFIG` |
| `WGET2_*` env vars | Override any config key (`WGET2_BACKEND`, `WGET2_RETRY`, …) |
| `--lock-file`, `--no-lock` | Single-instance guard (default `<output-dir>/.wget2.lock`) |
| `--check` | Self-test: backends, writability, free space |
| `--version`, `--help`, `--exit-codes` | Meta |

Precedence: **command line > environment > config file > built-in default**.

---

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success (including skipped and up-to-date files) |
| `1` | Unexpected internal error |
| `2` | Usage, configuration or policy refusal (bad flag, unknown backend, size cap, unsafe scheme) |
| `3` | Network/transport failure after retries (DNS, connect, reset, timeout) |
| `4` | HTTP status error after retries (404, 500, exhausted 5xx retries) |
| `5` | Integrity failure (checksum or length mismatch, short body) |
| `6` | Filesystem failure (permission, disk space, I/O) |
| `7` | Batch finished with failures (partial success) |
| `9` | Another instance holds the lock file |
| `130` | Interrupted |

`wget2 --exit-codes` prints the same table.

---

## Configuration file

```toml
[download]
backend = "httpx"
connections = 8
jobs = 4
retry = 5
continue = true

[output]
dir = "downloads"
template = "{host}/{filename}"
manifest = true

[auth]
netrc = true

[resources]
rate_limit = "10M"
```

See [`examples/wget2.toml`](examples/wget2.toml) for every key. Sections are only
namespaces; keys are matched case-insensitively with `-` or `_`. Unknown keys produce a
warning (so typos do not silently do nothing). YAML works too (`pip install wget2[yaml]`).

```bash
wget2 --config ~/.config/wget2/config.toml https://example.com/f.bin
WGET2_CONNECTIONS=16 WGET2_BACKEND=httpx wget2 https://example.com/f.bin   # env wins
```

---

## Writing a backend plugin

Backends implement exactly one primitive — perform a request and stream the body into a
sink — and inherit everything else (probing, range headers, retries, error mapping):

```python
from wget2.backends.base import DownloadBackend, ResponseInfo, Sink, normalize_headers


class MyBackend(DownloadBackend):
    name = "mybackend"
    requires = ("myclient",)  # optional dependency check

    def _request(
        self, url, method, headers, sink, *, allow_status=frozenset()
    ): ...  # stream into sink(chunk), return ResponseInfo
```

Register it:

```toml
[project.entry-points."wget2.backends"]
mybackend = "my_package.backend:MyBackend"
```

Then `wget2 --list-backends` shows it and `wget2 -b mybackend URL` uses it. A full,
runnable example lives in [`examples/plugin_backend.py`](examples/plugin_backend.py).

---

## Architecture

```
wget2/
├── cli.py           Typer app, option surface, orchestration
├── config.py        TOML/YAML + WGET2_* env merging
├── settings.py      immutable Settings (validated, derived knobs)
├── request.py       backend-agnostic request description (auth/TLS/proxy/headers)
├── backends/
│   ├── base.py      DownloadBackend ABC + probe/range/error-mapping in one place
│   ├── registry.py  discovery (built-in + entry points), auto/fallback chains
│   ├── pycurl_backend.py / httpx_backend.py / requests_backend.py
│   └── httpx_async_backend.py   single long-lived event loop, shared client
├── downloader.py    probe → plan → resume → ranges → verify → atomic finalize
├── state.py         range planning, byte-coverage algebra, .part state
├── net.py           per-host limits, shared backend pool
├── batch.py         global concurrency + summary report
├── progress.py      rich / plain / none reporters with batch aggregation
├── ratelimit.py     token bucket (global and per connection)
├── retry.py         backoff with jitter and status filtering
├── checksum.py      digest parsing, checksum files, verification
├── output.py        path resolution, existing-file policy, manifests
├── crawler.py       BFS discovery, accept/reject filters
├── cookies.py       cookie jar (Netscape compatible)
├── lock.py          advisory single-instance lock
├── log.py           structured, redacting logging
└── errors.py        exception hierarchy ↔ exit codes
```

Design notes:

* **One primitive per backend.** Adding an engine means implementing `_request`; probing,
  range headers, status handling and `StopStream` semantics are shared, so behavior stays
  consistent across engines.
* **Threads + positional writes.** Range workers are threads that `pwrite` into one
  pre-allocated file at absolute offsets. No part-file stitching, no pickling of shared
  counters, no multiprocessing pool (the previous implementation used
  `multiprocessing.Pool` with a `Value`; that is gone).
* **One event loop for `httpx_async`.** The async backend owns a single daemon thread and
  one `AsyncClient`, so all ranges share a connection pool and HTTP/2 multiplexing —
  instead of an `asyncio.run()` (new loop, new client) per range.
* **Work-stealing segments.** Files are split into ~4 segments per connection; workers
  pull from a shared queue, so a slow range does not stall the others, and the segment
  count adapts to file size and `--connections`.
* **Coverage as an interval set.** Resume correctness never depends on the original plan:
  completed byte intervals are merged, and only the gaps are requested.
* **Logs never leak secrets.** A redaction filter scrubs URL credentials, `Authorization`
  headers, bearer tokens and `password=`-style fields from every log record.

---

## Testing and quality gates

```bash
pytest                     # 317 unit + integration tests (local HTTP server)
pytest --cov=wget2         # coverage (≈84% of statements/branches)
ruff check wget2 tests     # lint
mypy wget2                 # types (clean)
```

The integration suite drives the real CLI against a threaded local HTTP server that can
serve ranges, refuse ranges, truncate bodies, fail transiently, require Basic/Bearer
auth, redirect, set cookies, serve a tiny HTML site for crawling, and throttle — so the
resume/retry/integrity paths are exercised end to end for every installed backend.

---

## Migrating from the previous version

Everything that used to work still works, with the same short flags
(`-c`, `-b`, `-f`, `-o`, `-j` is new for batch). Notable differences:

| Before | Now |
| --- | --- |
| `-b httpx_async` spawned a loop per range | one loop per process, shared client |
| `multiprocessing.Pool` per file | threads + `pwrite` into one preallocated `.part` |
| `--output-dir/-o` | `--output-dir/-P` (and `-o`), `-O` is now the filename |
| silent overwrite | `--overwrite` (default), `--no-clobber`, `--timestamping`, `--unique` |
| errors → exit 1 | typed exit codes (see table) |
| `.part{s}` files per range | one `<file>.part` + `<file>.part.json` state |

## License

MIT — see [LICENSE](LICENSE).
