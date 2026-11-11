from __future__ import annotations

import time
from pathlib import Path

import pytest

from wget2.backends.base import ProbeResult
from wget2.errors import UsageError
from wget2.output import (
    MANIFEST_SUFFIX,
    OutputOptions,
    read_manifest,
    render_template,
    resolve_output_path,
    validate_template,
    write_manifest,
)


def probe(**kwargs) -> ProbeResult:
    defaults = {
        "url": "https://example.com/dir/file.bin",
        "final_url": "https://example.com/dir/file.bin",
        "status": 200,
        "size": 1024,
        "etag": '"abc"',
        "last_modified": "Wed, 21 Oct 2015 07:28:00 GMT",
        "accept_ranges": True,
    }
    defaults.update(kwargs)
    return ProbeResult(**defaults)


class TestTemplates:
    def test_basic_fields(self) -> None:
        rendered = render_template(
            "{host}/{filename}",
            {"host": "example.com", "filename": "file.bin", "index": 0},
        )
        assert rendered == "example.com/file.bin"

    def test_unknown_field_is_empty(self) -> None:
        assert render_template("a{missing}b", {}) == "ab"

    def test_validate_known(self) -> None:
        validate_template("{host}/{filename}")

    def test_validate_unknown(self) -> None:
        with pytest.raises(UsageError):
            validate_template("{nope}/x")

    def test_invalid_format(self) -> None:
        with pytest.raises(UsageError):
            render_template("{unclosed", {})


class TestResolve:
    def test_default_name(self, tmp_path: Path) -> None:
        options = OutputOptions(output_dir=tmp_path)
        resolution = resolve_output_path(options, probe(), "https://example.com/a/file.bin")
        assert resolution.path == tmp_path / "file.bin"
        assert not resolution.skipped

    def test_content_disposition_wins(self, tmp_path: Path) -> None:
        options = OutputOptions(output_dir=tmp_path)
        headers = {"content-disposition": 'attachment; filename="other.bin"'}
        resolution = resolve_output_path(options, probe(headers=headers), "https://example.com/a/file.bin")
        assert resolution.path.name == "other.bin"

    def test_content_disposition_disabled(self, tmp_path: Path) -> None:
        options = OutputOptions(output_dir=tmp_path, use_content_disposition=False)
        headers = {"content-disposition": 'attachment; filename="other.bin"'}
        resolution = resolve_output_path(options, probe(headers=headers), "https://example.com/a/file.bin")
        assert resolution.path.name == "file.bin"

    def test_explicit_output(self, tmp_path: Path) -> None:
        options = OutputOptions(output_dir=tmp_path, output=Path("renamed.bin"))
        resolution = resolve_output_path(options, probe(), "https://example.com/a/file.bin")
        assert resolution.path == tmp_path / "renamed.bin"

    def test_absolute_output(self, tmp_path: Path) -> None:
        target = tmp_path / "abs.bin"
        options = OutputOptions(output_dir=tmp_path / "ignored", output=target)
        assert resolve_output_path(options, probe(), "https://x/y").path == target

    def test_template_subdirectories(self, tmp_path: Path) -> None:
        options = OutputOptions(output_dir=tmp_path, template="{host}/{filename}")
        resolution = resolve_output_path(options, probe(), "https://example.com/a/f.bin")
        assert resolution.path == tmp_path / "example.com" / "f.bin"

    def test_template_traversal_rejected(self, tmp_path: Path) -> None:
        options = OutputOptions(output_dir=tmp_path, template="../{filename}")
        with pytest.raises(UsageError):
            resolve_output_path(options, probe(), "https://example.com/a/f.bin")

    def test_no_clobber_skips(self, tmp_path: Path) -> None:
        target = tmp_path / "file.bin"
        target.write_bytes(b"existing")
        options = OutputOptions(output_dir=tmp_path, no_clobber=True)
        resolution = resolve_output_path(options, probe(), "https://example.com/a/file.bin")
        assert resolution.skipped
        assert resolution.skip_reason == "skipped"
        assert target.read_bytes() == b"existing"

    def test_timestamping_skips_newer_local(self, tmp_path: Path) -> None:
        target = tmp_path / "file.bin"
        target.write_bytes(b"existing")
        future = time.time() + 3600
        import os

        os.utime(target, (future, future))
        options = OutputOptions(output_dir=tmp_path, timestamping=True)
        resolution = resolve_output_path(options, probe(), "https://example.com/a/file.bin")
        assert resolution.skipped
        assert resolution.skip_reason == "up-to-date"

    def test_timestamping_downloads_older_local(self, tmp_path: Path) -> None:
        target = tmp_path / "file.bin"
        target.write_bytes(b"existing")
        old = time.time() - 30 * 365 * 86400
        import os

        os.utime(target, (old, old))
        options = OutputOptions(output_dir=tmp_path, timestamping=True)
        resolution = resolve_output_path(options, probe(), "https://example.com/a/file.bin")
        assert not resolution.skipped

    def test_unique(self, tmp_path: Path) -> None:
        (tmp_path / "file.bin").write_bytes(b"x")
        options = OutputOptions(output_dir=tmp_path, unique=True)
        resolution = resolve_output_path(options, probe(), "https://example.com/a/file.bin")
        assert resolution.path.name == "file.1.bin"


class TestValidate:
    def test_output_with_multiple_urls(self) -> None:
        with pytest.raises(UsageError):
            OutputOptions(output=Path("x"), output_dir=Path()).validate(2)

    def test_output_with_single_url(self) -> None:
        OutputOptions(output=Path("x"), output_dir=Path()).validate(1)

    def test_output_template_many(self) -> None:
        OutputOptions(output=Path("x"), template="{filename}").validate(5)


class TestManifest:
    def test_roundtrip(self, tmp_path: Path) -> None:
        target = tmp_path / "file.bin"
        target.write_bytes(b"data")
        path = write_manifest(
            target,
            url="https://example.com/file.bin",
            etag='"abc"',
            last_modified="Wed, 21 Oct 2015 07:28:00 GMT",
            size=4,
            digests={"sha256": "f" * 64},
            backend="pycurl",
        )
        assert path.name == "file.bin" + MANIFEST_SUFFIX
        manifest = read_manifest(target)
        assert manifest is not None
        assert manifest["url"] == "https://example.com/file.bin"
        assert manifest["checksums"]["sha256"] == "f" * 64
        assert manifest["etag"] == '"abc"'

    def test_missing(self, tmp_path: Path) -> None:
        assert read_manifest(tmp_path / "nope.bin") is None
