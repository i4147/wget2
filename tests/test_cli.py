"""CLI surface: flags, exit codes, configuration and reporting."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from wget2 import __version__
from wget2.backends.registry import REGISTRY

pytestmark = pytest.mark.network


def invoke(run_cli, *args: str, config: dict | None = None):
    return run_cli(*args, config=config)


class TestBasics:
    def test_version(self, run_cli) -> None:
        result = invoke(run_cli, "--version")
        assert result.exit_code == 0
        assert __version__ in result.output

    def test_help_mentions_key_options(self, run_cli) -> None:
        result = invoke(run_cli, "--help")
        assert result.exit_code == 0
        assert "--continue" in result.output
        assert "--exit-codes" in result.output

    def test_exit_codes_table(self, run_cli) -> None:
        result = invoke(run_cli, "--exit-codes")
        assert result.exit_code == 0
        assert "integrity failure" in result.output
        assert "130" in result.output

    def test_no_arguments(self, run_cli) -> None:
        result = invoke(run_cli)
        assert result.exit_code == 2
        assert "provide a URL" in result.output

    def test_list_backends(self, run_cli) -> None:
        result = invoke(run_cli, "--list-backends")
        assert result.exit_code == 0
        for name in REGISTRY.names():
            assert name in result.output

    def test_list_backends_json(self, run_cli) -> None:
        result = invoke(run_cli, "--list-backends", "--json")
        payload = json.loads(result.stdout)
        names = {item["name"] for item in payload["backends"]}
        assert {"pycurl", "httpx", "requests", "httpx_async"} <= names

    def test_check(self, run_cli, outdir) -> None:
        result = invoke(run_cli, "--check", "-P", str(outdir))
        assert result.exit_code == 0, result.output
        assert "output directory writable" in result.output

    def test_unknown_backend(self, run_cli, server) -> None:
        url = server.add("a.bin", b"a")
        result = invoke(run_cli, url, "-b", "does-not-exist", "-P", str(Path("/tmp")))
        assert result.exit_code == 2
        assert "unknown backend" in result.output

    def test_invalid_progress_mode(self, run_cli, server) -> None:
        url = server.add("a.bin", b"a")
        result = invoke(run_cli, url, "--progress", "fancy")
        assert result.exit_code == 2

    def test_unsafe_scheme_refused(self, run_cli) -> None:
        result = invoke(run_cli, "file:///etc/passwd")
        assert result.exit_code == 2
        assert "disabled" in result.output

    def test_invalid_checksum(self, run_cli, server, outdir) -> None:
        url = server.add("a.bin", b"a")
        result = invoke(run_cli, url, "-P", str(outdir), "--checksum", "sha256:zzz")
        assert result.exit_code == 2


class TestConfigDefaults:
    def test_backend_from_config(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        result = invoke(
            run_cli,
            url,
            "-P",
            str(outdir),
            "--progress",
            "none",
            config={"backend": "httpx", "connections": 2},
        )
        assert result.exit_code == 0, result.output
        assert (outdir / "file.bin").read_bytes() == payload

    def test_config_file_via_subprocess(self, tmp_path: Path, server, payload) -> None:
        """The real entry point must honour --config before parsing."""
        url = server.add("file.bin", payload)
        cfg = tmp_path / "wget2.toml"
        outdir = tmp_path / "out"
        cfg.write_text(
            '[download]\nbackend = "requests"\nconnections = 2\n'
            '[output]\ndir = "' + str(outdir) + '"\nmanifest = true\n'
        )
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
        completed = subprocess.run(
            [sys.executable, "-m", "wget2", "--config", str(cfg), url, "--progress", "none"],
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr
        assert (outdir / "file.bin").read_bytes() == payload
        manifest = json.loads((outdir / "file.bin.wget2.json").read_text())
        assert manifest["backend"] == "requests"

    def test_env_override(self, run_cli, server, outdir, payload, monkeypatch) -> None:
        url = server.add("file.bin", payload)
        monkeypatch.setenv("WGET2_BACKEND", "httpx")
        result = invoke(run_cli, url, "-P", str(outdir), "--progress", "none", "--json")
        assert result.exit_code == 0, result.stdout
        assert json.loads(result.stdout)["files"][0]["backend"] == "httpx"


class TestInputHandling:
    def test_missing_input_file(self, run_cli, outdir) -> None:
        result = invoke(run_cli, "-f", "/nonexistent/list.txt", "-P", str(outdir))
        assert result.exit_code == 2

    def test_duplicates_are_removed(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        result = invoke(run_cli, url, url, "-P", str(outdir), "--progress", "none", "--json")
        report = json.loads(result.stdout)
        assert report["summary"]["total"] == 1

    def test_base_url(self, run_cli, server, outdir) -> None:
        server.add("rel.bin", b"relative")
        base = server.url("")[:-1]
        result = invoke(run_cli, "rel.bin", "--base-url", base, "-P", str(outdir), "--progress", "none")
        assert result.exit_code == 0, result.output
        assert (outdir / "rel.bin").read_bytes() == b"relative"

    def test_invalid_template_field(self, run_cli, server, outdir) -> None:
        url = server.add("a.bin", b"a")
        result = invoke(run_cli, url, "-P", str(outdir), "--output-template", "{bogus}")
        assert result.exit_code == 2
        assert "bogus" in result.output


class TestReporting:
    def test_quiet_suppresses_summary(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        result = invoke(run_cli, url, "-P", str(outdir), "-q")
        assert result.exit_code == 0
        assert "downloaded" not in result.stdout
        assert (outdir / "file.bin").exists()

    def test_log_file_written(self, run_cli, server, outdir, payload, tmp_path) -> None:
        url = server.add("file.bin", payload)
        log_path = tmp_path / "wget2.log"
        result = invoke(
            run_cli,
            url,
            "-P",
            str(outdir),
            "--log-file",
            str(log_path),
            "--log-level",
            "debug",
            "--progress",
            "none",
        )
        assert result.exit_code == 0, result.output
        text = log_path.read_text()
        assert "download complete" in text

    def test_json_logs(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        result = invoke(run_cli, url, "-P", str(outdir), "--log-json", "--progress", "none")
        lines = [line for line in result.stderr.splitlines() if line.strip().startswith("{")]
        assert lines
        payloads = [json.loads(line) for line in lines]
        assert any(entry.get("event") == "download complete" for entry in payloads)

    def test_credentials_are_redacted(self, run_cli, server, outdir, payload, tmp_path) -> None:
        url = server.add("file.bin", payload)
        log_path = tmp_path / "redacted.log"
        result = invoke(
            run_cli,
            url,
            "-P",
            str(outdir),
            "--user",
            "alice",
            "--password",
            "topsecret",
            "--log-file",
            str(log_path),
            "--log-level",
            "debug",
            "--progress",
            "none",
        )
        assert result.exit_code == 0, result.output
        assert "topsecret" not in log_path.read_text()


class TestExitCodes:
    def test_success(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        assert invoke(run_cli, url, "-P", str(outdir), "--progress", "none").exit_code == 0

    def test_http_error(self, run_cli, server, outdir) -> None:
        result = invoke(run_cli, server.url("nope"), "-P", str(outdir), "--progress", "none")
        assert result.exit_code == 4

    def test_network_error(self, run_cli, outdir) -> None:
        result = invoke(
            run_cli,
            "http://127.0.0.1:1/x.bin",
            "-P",
            str(outdir),
            "--retry",
            "0",
            "--connect-timeout",
            "1",
            "--progress",
            "none",
        )
        assert result.exit_code == 3

    def test_integrity_error(self, run_cli, server, outdir, payload) -> None:
        url = server.add("file.bin", payload)
        result = invoke(
            run_cli,
            url,
            "-P",
            str(outdir),
            "--checksum",
            "sha256:" + "1" * 64,
            "--progress",
            "none",
        )
        assert result.exit_code == 5


class TestBackendAvailability:
    def test_unavailable_default_falls_back(self, run_cli, server, outdir, payload, monkeypatch) -> None:
        """The default backend is an optional dependency: fall back, don't fail."""
        from wget2.backends.pycurl_backend import PycurlBackend

        url = server.add("file.bin", payload)
        monkeypatch.setattr(PycurlBackend, "available", classmethod(lambda cls: False))
        monkeypatch.setattr(
            PycurlBackend, "availability_reason", classmethod(lambda cls: "missing optional dependency")
        )
        result = invoke(run_cli, url, "-P", str(outdir), "-b", "pycurl", "--progress", "none", "--json")
        assert result.exit_code == 0, result.stdout
        report = json.loads(result.stdout)
        assert report["files"][0]["backend"] != "pycurl"
        assert (outdir / "file.bin").read_bytes() == payload
