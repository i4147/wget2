"""Batch scheduling: global concurrency, host limits and the summary report."""

from __future__ import annotations

import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Iterable

from .downloader import DownloadContext, FileDownloader, FileResult
from .errors import EXIT_INTERRUPTED, EXIT_OK, EXIT_PARTIAL
from .log import get_logger
from .util import human_duration, human_size

STATUS_SYMBOLS = {
    "downloaded": "✔",
    "skipped": "→",
    "up-to-date": "=",
    "spidered": "◎",
    "dry-run": "·",
    "failed": "✖",
}


@dataclass
class BatchSummary:
    results: list[FileResult] = field(default_factory=list)
    duration: float = 0.0
    started_at: float = field(default_factory=time.time)
    interrupted: bool = False

    # -- aggregates ------------------------------------------------------- #
    @property
    def downloaded(self) -> list[FileResult]:
        return [r for r in self.results if r.status == "downloaded"]

    @property
    def skipped(self) -> list[FileResult]:
        return [r for r in self.results if r.skipped]

    @property
    def failed(self) -> list[FileResult]:
        return [r for r in self.results if r.status == "failed"]

    @property
    def total_bytes(self) -> int:
        return sum(r.size or 0 for r in self.downloaded)

    @property
    def exit_code(self) -> int:
        if self.interrupted:
            return EXIT_INTERRUPTED
        if not self.failed:
            return EXIT_OK
        if len(self.failed) == len(self.results):
            return self.failed[0].exit_code or EXIT_PARTIAL
        return EXIT_PARTIAL

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary": {
                "total": len(self.results),
                "downloaded": len(self.downloaded),
                "skipped": len(self.skipped),
                "failed": len(self.failed),
                "bytes": self.total_bytes,
                "duration": round(self.duration, 3),
                "interrupted": self.interrupted,
                "exit_code": self.exit_code,
            },
            "files": [result.to_dict() for result in self.results],
        }

    # -- rendering -------------------------------------------------------- #
    def lines(self) -> list[str]:
        lines = []
        for result in self.results:
            symbol = STATUS_SYMBOLS.get(result.status, "?")
            name = result.path.name if result.path else result.url
            suffix = ""
            if result.status == "failed" and result.error:
                suffix = f"  [{result.error_kind}] {result.error}"
            elif result.detail:
                suffix = f"  ({result.detail})"
            lines.append(f"{symbol} {name:<40} {human_size(result.size):>10}{suffix}")
        lines.append(
            f"— {len(self.downloaded)} downloaded, {len(self.skipped)} skipped, {len(self.failed)} failed, "
            f"{human_size(self.total_bytes)} in {human_duration(self.duration)}"
        )
        return lines


class BatchRunner:
    """Runs many URLs with bounded global concurrency."""

    def __init__(self, context: DownloadContext) -> None:
        self.ctx = context
        self.settings = context.settings
        self.log = get_logger("batch")

    def run(self, urls: Iterable[str]) -> BatchSummary:
        url_list = list(urls)
        summary = BatchSummary()
        started = time.monotonic()
        if not url_list:
            summary.duration = 0.0
            return summary
        self.ctx.progress.start(total_files=len(url_list))
        workers = max(1, min(self.settings.max_active, len(url_list)))
        self.log.info(
            "batch started",
            urls=len(url_list),
            workers=workers,
            per_file_connections=self.settings.connections,
        )
        if workers == 1:
            for index, url in enumerate(url_list):
                summary.results.append(FileDownloader(self.ctx).run(url, index))
                if self.ctx.interrupt.is_set():
                    summary.interrupted = True
                    break
        else:
            results: dict[int, FileResult] = {}
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="wget2-file") as pool:
                futures: dict[Future[FileResult], int] = {}
                for index, url in enumerate(url_list):
                    if self.ctx.interrupt.is_set():
                        break
                    future = pool.submit(FileDownloader(self.ctx).run, url, index)
                    futures[future] = index
                for future, index in futures.items():
                    try:
                        results[index] = future.result()
                    except Exception as exc:  # pragma: no cover - defensive
                        self.log.error("worker crashed", error=str(exc), url=url_list[index])
                        results[index] = FileResult(url=url_list[index], index=index, status="failed", error=str(exc))
            summary.results = [results[key] for key in sorted(results)]
        if self.ctx.interrupt.is_set():
            summary.interrupted = True
        summary.duration = time.monotonic() - started
        self.log.info(
            "batch finished",
            downloaded=len(summary.downloaded),
            skipped=len(summary.skipped),
            failed=len(summary.failed),
            exit_code=summary.exit_code,
        )
        return summary


def render_summary(summary: BatchSummary, *, quiet: bool = False) -> str:
    """Plain-text summary block (used by --quiet-less CLI output)."""
    lines = summary.lines()
    if quiet:
        return lines[-1]
    return "\n".join(lines)
