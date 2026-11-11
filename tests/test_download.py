"""End-to-end download behaviour through the CLI."""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path

import pytest

from wget2.backends.registry import REGISTRY
from wget2.state import PartState

pytestmark = pytest.mark.network

BACKENDS = [name for name in ("pycurl", "httpx", "requests", "httpx_async") if REGISTRY.get(name).available()]


def invoke(run_cli, *args: str, config: dict | None = None):
    base = ("--progress", "none")
    return run_cli(*base, *args, config=config)


def part_paths(outdir: Path, name: str) -> tuple[Path, Path]:
    return outdir / f"{name}.part", outdir / f"{name}.part.json"


class TestBasicDownloads:
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_single_backend(self, run_cli, server, outdir, payload, backend) -> None:
        url = server.add("file.bin", payload)
        result = invoke(run_cli, url, "-P", str(outdir), "-b", backend)
        assert result.exit_code == 0, result.output
        target = outdir / "file.bin"
        assert target.read_bytes() == payload
        assert target.stat().st_size == len(payload)

    def test_multi_connection(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        result = invoke(run_cli, url, "-P", str(outdir), "-c", "4")
        assert result.exit_code == 0, result.output
        assert (outdir / "file.bin").read_bytes() == payload
        ranges = [r for r in server.ranges if r[1] is not None]
        assert len(ranges) >= 2, "expected several ranged requests"
        assert len({r[1] for r in ranges}) >= 2, "ranges should start at different offsets"

    def test_no_part_files_left(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        invoke(run_cli, url, "-P", str(outdir))
        assert sorted(p.name for p in outdir.iterdir()) == ["file.bin"]

    def test_range_unsupported_single_stream(self, run_cli, server, outdir, payload) -> None:
        url = server.add("plain.bin", payload, allow_ranges=False)
        result = invoke(run_cli, url, "-P", str(outdir), "-c", "4")
        assert result.exit_code == 0, result.output
        assert (outdir / "plain.bin").read_bytes() == payload
        ranged = [r for r in server.ranges if r[1] is not None]
        assert ranged == [], "server has no range support; no ranged requests expected"

    def test_filename_from_url(self, run_cli, server, outdir) -> None:
        url = server.add("deep/nested/thing.tar.gz", b"data")
        invoke(run_cli, url, "-P", str(outdir))
        assert (outdir / "thing.tar.gz").exists()

    def test_content_disposition_name(self, run_cli, server, outdir) -> None:
        url = server.add("weird", b"data", content_disposition='attachment; filename="friendly.txt"')
        invoke(run_cli, url, "-P", str(outdir))
        assert (outdir / "friendly.txt").read_bytes() == b"data"

    def test_redirect(self, run_cli, server, outdir, payload) -> None:
        server.add("target.bin", payload)
        result = invoke(run_cli, server.url("redirect/target.bin"), "-P", str(outdir))
        assert result.exit_code == 0, result.output
        assert (outdir / "target.bin").read_bytes() == payload


class TestResume:
    def _seed(self, outdir: Path, url: str, payload: bytes, size: int, etag: str, written: int) -> Path:
        part = outdir / (url.rsplit("/", 1)[-1] + ".part")
        with part.open("wb") as handle:
            handle.write(payload[:written])
            handle.truncate(size)
        state = PartState(
            part_path=part,
            url=url,
            size=size,
            etag=etag,
            last_modified="Wed, 21 Oct 2015 07:28:00 GMT",
            plan=[(0, size)],
        )
        state.mark(0, written)
        state.save()
        return part

    def test_resume_continues_from_offset(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        etag = server.files["file.bin"].etag
        half = len(payload) // 2
        self._seed(outdir, url, payload, len(payload), etag, half)

        result = invoke(run_cli, url, "-P", str(outdir), "-C")
        assert result.exit_code == 0, result.output
        assert (outdir / "file.bin").read_bytes() == payload
        starts = [r[1] for r in server.ranges if r[1] is not None]
        assert starts and min(starts) >= half, f"should resume at {half}, got {sorted(set(starts))}"

    def test_resume_with_multiple_connections(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        etag = server.files["file.bin"].etag
        quarter = len(payload) // 4
        self._seed(outdir, url, payload, len(payload), etag, quarter)
        result = invoke(run_cli, url, "-P", str(outdir), "-C", "-c", "4")
        assert result.exit_code == 0, result.output
        assert (outdir / "file.bin").read_bytes() == payload
        requested = sum((r[2] - r[1] + 1) for r in server.ranges if r[1] is not None)
        assert requested < len(payload), "resume must not re-download finished bytes"

    def test_resume_restarts_on_etag_change(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        half = len(payload) // 2
        self._seed(outdir, url, payload, len(payload), '"stale-etag"', half)
        result = invoke(run_cli, url, "-P", str(outdir), "-C")
        assert result.exit_code == 0, result.output
        assert (outdir / "file.bin").read_bytes() == payload
        starts = [r[1] for r in server.ranges if r[1] is not None]
        assert 0 in starts, "everything must be re-downloaded after a validator change"

    def test_without_continue_restarts(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        etag = server.files["file.bin"].etag
        self._seed(outdir, url, payload, len(payload), etag, len(payload) // 2)
        result = invoke(run_cli, url, "-P", str(outdir))
        assert result.exit_code == 0, result.output
        assert (outdir / "file.bin").read_bytes() == payload

    def test_complete_part_is_finalized(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        etag = server.files["file.bin"].etag
        self._seed(outdir, url, payload, len(payload), etag, len(payload))
        result = invoke(run_cli, url, "-P", str(outdir), "-C", "--json")
        assert result.exit_code == 0, result.stdout
        report = json.loads(result.stdout)
        assert report["files"][0]["status"] == "downloaded"
        assert report["files"][0]["resumed"] is True
        assert report["files"][0]["bytes_written"] == 0
        assert (outdir / "file.bin").read_bytes() == payload

    def test_state_removed_after_success(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        invoke(run_cli, url, "-P", str(outdir))
        part, state = part_paths(outdir, "file.bin")
        assert not part.exists()
        assert not state.exists()


class TestIntegrity:
    def test_checksum_match(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        digest = hashlib.sha256(payload).hexdigest()
        result = invoke(run_cli, url, "-P", str(outdir), "--checksum", f"sha256:{digest}")
        assert result.exit_code == 0, result.output
        assert (outdir / "file.bin").exists()

    def test_checksum_mismatch(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        result = invoke(run_cli, url, "-P", str(outdir), "--checksum", "sha256:" + "0" * 64)
        assert result.exit_code == 5, result.output
        assert not (outdir / "file.bin").exists(), "a corrupt file must never be finalized"

    def test_checksum_file(self, run_cli, server, outdir, payload, tmp_path) -> None:
        url = server.add("file.bin", payload)
        sums = tmp_path / "SHA256SUMS"
        sums.write_text(f"{hashlib.sha256(payload).hexdigest()}  file.bin\n")
        result = invoke(run_cli, url, "-P", str(outdir), "--checksum-file", str(sums))
        assert result.exit_code == 0, result.output

    def test_truncated_body_fails(self, run_cli, server, outdir, payload) -> None:
        url = server.add("cut.bin", payload, allow_ranges=False, truncate_at=1024)
        result = invoke(run_cli, url, "-P", str(outdir), "--retry", "0")
        assert result.exit_code in (3, 5), result.output
        assert not (outdir / "cut.bin").exists()

    def test_truncated_removes_part_on_request(self, run_cli, server, outdir, payload) -> None:
        url = server.add("cut.bin", payload, allow_ranges=False, truncate_at=1024)
        result = invoke(run_cli, url, "-P", str(outdir), "--retry", "0", "--remove-on-error")
        assert result.exit_code in (3, 5)
        leftovers = [p for p in outdir.iterdir() if p.name != ".wget2.lock"]
        assert leftovers == []

    def test_keeps_part_by_default(self, run_cli, server, outdir, payload) -> None:
        url = server.add("cut.bin", payload, allow_ranges=False, truncate_at=1024)
        invoke(run_cli, url, "-P", str(outdir), "--retry", "0")
        assert (outdir / "cut.bin.part").exists()

    def test_manifest_sidecar(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        digest = hashlib.sha256(payload).hexdigest()
        result = invoke(run_cli, url, "-P", str(outdir), "--manifest", "--checksum", f"sha256:{digest}")
        assert result.exit_code == 0, result.output
        manifest = json.loads((outdir / "file.bin.wget2.json").read_text())
        assert manifest["url"] == url
        assert manifest["size"] == len(payload)
        assert manifest["checksums"]["sha256"] == digest
        assert manifest["etag"]
        assert manifest["backend"]


class TestRetries:
    def test_retries_transient_errors(self, run_cli, server, outdir, payload) -> None:
        url = server.add("flaky.bin", payload, fail_first=2)
        result = invoke(run_cli, url, "-P", str(outdir), "--retry", "3", "--retry-delay", "0")
        assert result.exit_code == 0, result.output
        assert (outdir / "flaky.bin").read_bytes() == payload

    def test_gives_up_after_retries(self, run_cli, server, outdir, payload) -> None:
        url = server.add("flaky.bin", payload, fail_first=10)
        result = invoke(run_cli, url, "-P", str(outdir), "--retry", "2", "--retry-delay", "0")
        assert result.exit_code == 4, result.output
        assert "503" in result.output or "503" in result.stderr

    def test_404_is_not_retried(self, run_cli, server, outdir) -> None:
        result = invoke(run_cli, server.url("missing.bin"), "-P", str(outdir), "--retry", "3")
        assert result.exit_code == 4
        assert server.hits["missing.bin"] == 1

    def test_custom_retry_status(self, run_cli, server, outdir, payload) -> None:
        url = server.add("f.bin", payload, fail_first=1, fail_status=418)
        result = invoke(
            run_cli,
            url,
            "-P",
            str(outdir),
            "--retry",
            "2",
            "--retry-delay",
            "0",
            "--retry-http",
            "418",
        )
        assert result.exit_code == 0, result.output


class TestAuthAndHeaders:
    def test_basic_auth_required(self, run_cli, server, outdir, payload) -> None:
        server.add("secret.bin", payload)
        server.auth["secret.bin"] = ("alice", "s3cret")
        result = invoke(run_cli, server.url("auth/secret.bin"), "-P", str(outdir), "--retry", "0")
        assert result.exit_code == 4

    def test_basic_auth_supplied(self, run_cli, server, outdir, payload) -> None:
        server.add("secret.bin", payload)
        server.auth["secret.bin"] = ("alice", "s3cret")
        result = invoke(
            run_cli,
            server.url("auth/secret.bin"),
            "-P",
            str(outdir),
            "--user",
            "alice",
            "--password",
            "s3cret",
        )
        assert result.exit_code == 0, result.output
        assert (outdir / "secret.bin").read_bytes() == payload

    def test_bearer(self, run_cli, server, outdir, payload) -> None:
        server.add("t.bin", payload)
        server.bearer["t.bin"] = "tok-abc"
        result = invoke(run_cli, server.url("bearer/t.bin"), "-P", str(outdir), "--bearer-token", "tok-abc")
        assert result.exit_code == 0, result.output

    def test_headers_and_user_agent(self, run_cli, server, outdir) -> None:
        result = invoke(
            run_cli,
            server.url("echo"),
            "-P",
            str(outdir),
            "-O",
            "echo.json",
            "-H",
            "X-Custom: hello",
            "-H",
            "X-Second: two",
            "--user-agent",
            "wget2-test/9",
            "--referer",
            "https://example.com/page",
        )
        assert result.exit_code == 0, result.output
        echoed = json.loads((outdir / "echo.json").read_text())["headers"]
        lowered = {key.lower(): value for key, value in echoed.items()}
        assert lowered["x-custom"] == "hello"
        assert lowered["x-second"] == "two"
        assert lowered["user-agent"] == "wget2-test/9"
        assert lowered["referer"] == "https://example.com/page"

    def test_cookie_header(self, run_cli, server, outdir) -> None:
        result = invoke(
            run_cli,
            server.url("cookie/echo"),
            "-P",
            str(outdir),
            "-O",
            "c.txt",
            "--cookie",
            "a=1; b=2",
        )
        assert result.exit_code == 0, result.output
        body = (outdir / "c.txt").read_bytes().decode()
        assert "a=1" in body and "b=2" in body

    def test_cookie_jar_written(self, run_cli, server, outdir, tmp_path) -> None:
        jar = tmp_path / "cookies.txt"
        result = invoke(
            run_cli,
            server.url("echo"),
            "-P",
            str(outdir),
            "-O",
            "e.json",
            "--cookie",
            "x=9",
            "--cookie-jar",
            str(jar),
        )
        assert result.exit_code == 0, result.output
        assert jar.exists()
        assert "x" in jar.read_text()

    def test_post_data(self, run_cli, server, outdir) -> None:
        result = invoke(
            run_cli,
            server.url("echo"),
            "-P",
            str(outdir),
            "-O",
            "post.json",
            "--method",
            "POST",
            "--data",
            "hello=world",
        )
        assert result.exit_code == 0, result.output
        assert "hello=world" in (outdir / "post.json").read_text()


class TestExistingFiles:
    def test_no_clobber_skips(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        (outdir / "file.bin").write_bytes(b"keep me")
        result = invoke(run_cli, url, "-P", str(outdir), "--no-clobber")
        assert result.exit_code == 0, result.output
        assert (outdir / "file.bin").read_bytes() == b"keep me"

    def test_overwrite_by_default(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        (outdir / "file.bin").write_bytes(b"old")
        result = invoke(run_cli, url, "-P", str(outdir))
        assert result.exit_code == 0
        assert (outdir / "file.bin").read_bytes() == payload

    def test_timestamping_skips_newer(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        target = outdir / "file.bin"
        target.write_bytes(b"newer")
        future = time.time() + 86400
        os.utime(target, (future, future))
        result = invoke(run_cli, url, "-P", str(outdir), "-N")
        assert result.exit_code == 0, result.output
        assert target.read_bytes() == b"newer"

    def test_timestamping_downloads_older(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        target = outdir / "file.bin"
        target.write_bytes(b"older")
        old = time.time() - 40 * 365 * 86400
        os.utime(target, (old, old))
        result = invoke(run_cli, url, "-P", str(outdir), "-N")
        assert result.exit_code == 0, result.output
        assert target.read_bytes() == payload

    def test_unique_names(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        (outdir / "file.bin").write_bytes(b"first")
        result = invoke(run_cli, url, "-P", str(outdir), "--unique")
        assert result.exit_code == 0, result.output
        assert (outdir / "file.bin").read_bytes() == b"first"
        assert (outdir / "file.1.bin").read_bytes() == payload


class TestOutputPaths:
    def test_output_file(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        result = invoke(run_cli, url, "-P", str(outdir), "-O", "renamed.dat")
        assert result.exit_code == 0, result.output
        assert (outdir / "renamed.dat").read_bytes() == payload

    def test_output_template(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        result = invoke(run_cli, url, "-P", str(outdir), "--output-template", "{host}/{filename}")
        assert result.exit_code == 0, result.output
        assert (outdir / "127.0.0.1" / "file.bin").read_bytes() == payload

    def test_output_with_two_urls_is_rejected(self, run_cli, server, outdir) -> None:
        one = server.add("a.bin", b"a")
        two = server.add("b.bin", b"b")
        result = invoke(run_cli, one, two, "-P", str(outdir), "-O", "x.bin")
        assert result.exit_code == 2

    def test_max_filesize(self, run_cli, server, outdir, payload) -> None:
        url = server.add("big.bin", payload)
        result = invoke(run_cli, url, "-P", str(outdir), "--max-filesize", "1K")
        assert result.exit_code == 2
        assert not (outdir / "big.bin").exists()

    def test_empty_resource_refused(self, run_cli, server, outdir) -> None:
        result = invoke(run_cli, server.url("empty"), "-P", str(outdir))
        assert result.exit_code == 2


class TestModes:
    def test_dry_run_writes_nothing(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        result = invoke(run_cli, url, "-P", str(outdir), "--dry-run")
        assert result.exit_code == 0, result.output
        assert list(outdir.iterdir()) == []

    def test_spider_writes_nothing(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        result = invoke(run_cli, url, "-P", str(outdir), "--spider")
        assert result.exit_code == 0, result.output
        assert list(outdir.iterdir()) == []
        assert server.hits["file.bin"] >= 1


class TestBatch:
    def test_parallel_batch(self, run_cli, server, outdir) -> None:
        urls = []
        for index in range(4):
            size = 300_000 + index * 1000
            urls.append(server.add(f"file{index}.bin", bytes([index]) * size))
        result = invoke(run_cli, *urls, "-P", str(outdir), "-j", "4", "-c", "2")
        assert result.exit_code == 0, result.output
        for index in range(4):
            data = (outdir / f"file{index}.bin").read_bytes()
            assert data == bytes([index]) * len(data)
        assert "4 downloaded" in result.output

    def test_partial_failure_exit_code(self, run_cli, server, outdir) -> None:
        good = server.add("good.bin", b"x" * 1000)
        result = invoke(run_cli, good, server.url("nope.bin"), "-P", str(outdir), "-j", "2")
        assert result.exit_code == 7, result.output
        assert (outdir / "good.bin").exists()

    def test_input_file(self, run_cli, server, outdir, tmp_path) -> None:
        one = server.add("a.bin", b"a")
        two = server.add("b.bin", b"b")
        listing = tmp_path / "urls.txt"
        listing.write_text(f"# comment\n{one}\n\n{two}\n")
        result = invoke(run_cli, "-f", str(listing), "-P", str(outdir))
        assert result.exit_code == 0, result.output
        assert (outdir / "a.bin").exists() and (outdir / "b.bin").exists()

    def test_json_input(self, run_cli, server, outdir, tmp_path) -> None:
        url = server.add("a.bin", b"a")
        listing = tmp_path / "urls.json"
        listing.write_text(json.dumps([{"url": url}, {"url": server.url("echo")}]))
        result = invoke(run_cli, "-f", str(listing), "-P", str(outdir), "--input-format", "json")
        assert result.exit_code == 0, result.output

    def test_csv_input(self, run_cli, server, outdir, tmp_path) -> None:
        url = server.add("a.bin", b"a")
        listing = tmp_path / "urls.csv"
        listing.write_text(f"url,note\n{url},first\n")
        result = invoke(run_cli, "-f", str(listing), "-P", str(outdir), "--input-format", "csv")
        assert result.exit_code == 0, result.output

    def test_json_report(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        result = invoke(run_cli, url, "-P", str(outdir), "--json")
        assert result.exit_code == 0, result.stdout
        report = json.loads(result.stdout)
        assert report["summary"]["downloaded"] == 1
        assert report["files"][0]["status"] == "downloaded"
        assert report["files"][0]["size"] == len(payload)


class TestRecursive:
    def test_recursive_crawl(self, run_cli, server, outdir) -> None:
        result = invoke(
            run_cli,
            server.url("site/index.html"),
            "-P",
            str(outdir),
            "-r",
            "-l",
            "2",
            "-A",
            "*.html",
        )
        assert result.exit_code == 0, result.output
        downloaded = {p.name for p in outdir.iterdir()}
        assert {"index.html", "page1.html", "page2.html", "page3.html"} <= downloaded
        assert "skip.png" not in downloaded

    def test_spider_recursive(self, run_cli, server, outdir) -> None:
        result = invoke(run_cli, server.url("site/index.html"), "-P", str(outdir), "-r", "-l", "1", "--spider")
        assert result.exit_code == 0, result.output
        assert list(outdir.iterdir()) == []


class TestResources:
    @pytest.mark.slow
    def test_rate_limit(self, run_cli, server, outdir) -> None:
        payload = b"x" * (400 * 1024)
        url = server.add("rate.bin", payload)
        started = time.monotonic()
        result = invoke(run_cli, url, "-P", str(outdir), "--rate-limit", "200K", "-c", "1")
        elapsed = time.monotonic() - started
        assert result.exit_code == 0, result.output
        assert elapsed >= 1.0, f"rate limiting did not slow the transfer ({elapsed:.2f}s)"

    def test_lock_prevents_second_instance(self, run_cli, server, outdir, payload) -> None:
        from wget2.lock import FileLock

        url = server.add("file.bin", payload)
        lock = FileLock(outdir / ".wget2.lock")
        lock.acquire()
        try:
            result = invoke(run_cli, url, "-P", str(outdir))
        finally:
            lock.release()
        assert result.exit_code == 9
        assert not (outdir / "file.bin").exists()

    def test_no_lock_flag(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        result = invoke(run_cli, url, "-P", str(outdir), "--no-lock")
        assert result.exit_code == 0, result.output
        assert (outdir / "file.bin").exists()


class TestMirrorFailover:
    def test_uses_mirror_when_primary_fails(self, run_cli, server, outdir, payload) -> None:
        primary = server.url("gone.bin")  # 404
        mirror = server.add("copy.bin", payload)
        result = invoke(
            run_cli,
            primary,
            "-P",
            str(outdir),
            "--retry",
            "0",
            "--mirror-list",
            mirror,
            "--json",
        )
        assert result.exit_code == 0, result.stdout
        report = json.loads(result.stdout)
        assert report["files"][0]["status"] == "downloaded"
        # the filename still comes from the requested URL
        assert (outdir / "gone.bin").read_bytes() == payload

    def test_reports_failure_when_all_mirrors_fail(self, run_cli, server, outdir) -> None:
        result = invoke(
            run_cli,
            server.url("a.bin"),
            "-P",
            str(outdir),
            "--retry",
            "0",
            "--mirror-list",
            server.url("b.bin"),
            "--progress",
            "none",
        )
        assert result.exit_code == 4

    def test_mirror_list_from_config(self, run_cli, server, outdir, payload) -> None:
        mirror = server.add("copy.bin", payload)
        result = invoke(
            run_cli,
            server.url("missing.bin"),
            "-P",
            str(outdir),
            "--retry",
            "0",
            "--progress",
            "none",
            config={"mirror_list": [mirror]},
        )
        assert result.exit_code == 0, result.output
        assert (outdir / "missing.bin").read_bytes() == payload


class TestPathCollisions:
    """Two URLs resolving to the same filename must not corrupt each other."""

    def test_parallel_same_filename(self, run_cli, server, outdir) -> None:
        first = b"a" * 400_000
        second = b"b" * 400_000
        one = server.add("same.bin", first)
        two = server.add("norange/same.bin", second)
        result = invoke(run_cli, one, two, "-P", str(outdir), "-j", "2", "-c", "4", "--json")
        assert result.exit_code == 0, result.stdout
        report = json.loads(result.stdout)
        assert report["summary"]["failed"] == 0
        data = (outdir / "same.bin").read_bytes()
        assert data in {first, second}
        leftovers = [p.name for p in outdir.iterdir() if p.name.endswith((".part", ".json"))]
        assert leftovers == []

    def test_parallel_same_filename_no_clobber(self, run_cli, server, outdir) -> None:
        one = server.add("dup.bin", b"a" * 300_000)
        two = server.add("norange/dup.bin", b"b" * 300_000)
        result = invoke(run_cli, one, two, "-P", str(outdir), "-j", "2", "--no-clobber", "--json")
        assert result.exit_code == 0, result.stdout
        report = json.loads(result.stdout)
        statuses = sorted(item["status"] for item in report["files"])
        assert statuses == ["downloaded", "skipped"]


class TestConcurrencyLimits:
    def test_max_active_serialises(self, run_cli, server, outdir) -> None:
        urls = [server.add(f"m{i}.bin", bytes([i]) * 200_000) for i in range(3)]
        result = invoke(run_cli, *urls, "-P", str(outdir), "-j", "3", "--max-active", "1")
        assert result.exit_code == 0, result.output
        assert all((outdir / f"m{i}.bin").exists() for i in range(3))

    def test_per_connection_rate_limit(self, run_cli, server, outdir) -> None:
        # token bucket starts full (burst = 1s of budget): 600 KiB at 200 KiB/s
        # is ~1s of burst plus ~2s of throttling
        url = server.add("p.bin", b"x" * (600 * 1024))
        started = time.monotonic()
        result = invoke(
            run_cli,
            url,
            "-P",
            str(outdir),
            "-c",
            "1",
            "--rate-limit-per-connection",
            "200K",
            "--progress",
            "none",
        )
        elapsed = time.monotonic() - started
        assert result.exit_code == 0, result.output
        assert (outdir / "p.bin").stat().st_size == 600 * 1024
        assert elapsed >= 1.0, f"per-connection limit not applied ({elapsed:.2f}s)"

    def test_per_host_limit(self, run_cli, server, outdir) -> None:
        urls = [server.add(f"h{i}.bin", b"y" * 100_000) for i in range(3)]
        result = invoke(run_cli, *urls, "-P", str(outdir), "-j", "3", "--max-per-host", "1")
        assert result.exit_code == 0, result.output
        assert all((outdir / f"h{i}.bin").exists() for i in range(3))
