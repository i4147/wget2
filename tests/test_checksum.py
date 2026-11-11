from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from wget2.checksum import (
    load_checksum_file,
    parse_checksum,
    parse_checksums,
    specs_for,
    verify_file,
)
from wget2.errors import ChecksumError, UsageError


class TestParse:
    def test_sha256_prefix(self) -> None:
        digest = "a" * 64
        spec = parse_checksum(f"sha256:{digest}")
        assert spec.algorithm == "sha256"
        assert spec.expected == digest

    def test_bare_hex_defaults_to_sha256(self) -> None:
        spec = parse_checksum("b" * 64)
        assert spec.algorithm == "sha256"

    def test_equals_separator(self) -> None:
        assert parse_checksum("md5:" + "c" * 32).algorithm == "md5"

    @pytest.mark.parametrize("value", ["", "sha256:xyz", "sha256:abcd", "not-a-checksum"])
    def test_invalid(self, value: str) -> None:
        with pytest.raises(UsageError):
            parse_checksum(value)

    def test_multiple(self) -> None:
        specs = parse_checksums([f"sha256:{'a' * 64}", f"md5:{'b' * 32}"])
        assert [s.algorithm for s in specs] == ["sha256", "md5"]


class TestChecksumFile:
    def test_sha256sum_format(self, tmp_path: Path) -> None:
        digest = hashlib.sha256(b"hello").hexdigest()
        path = tmp_path / "SHA256SUMS"
        path.write_text(f"{digest}  hello.txt\n{'d' * 64} *other.bin\n")
        table = load_checksum_file(path)
        assert set(table) == {"hello.txt", "other.bin"}
        assert table["hello.txt"][0].expected == digest

    def test_bsd_format(self, tmp_path: Path) -> None:
        digest = hashlib.sha256(b"x").hexdigest()
        path = tmp_path / "SUMS"
        path.write_text(f"SHA256 (x.tar.gz) = {digest}\n")
        table = load_checksum_file(path)
        assert table["x.tar.gz"][0].algorithm == "sha256"

    def test_specs_for(self, tmp_path: Path) -> None:
        digest = hashlib.sha256(b"data").hexdigest()
        path = tmp_path / "SUMS"
        path.write_text(f"{digest}  data.bin\n")
        table = load_checksum_file(path)
        assert specs_for("data.bin", table)[0].expected == digest
        assert specs_for("missing.bin", table) == []

    def test_comments_ignored(self, tmp_path: Path) -> None:
        path = tmp_path / "SUMS"
        path.write_text("# comment\n\n" + "a" * 64 + "  f.bin\n")
        assert len(load_checksum_file(path)) == 1


class TestVerify:
    def test_ok(self, tmp_path: Path) -> None:
        target = tmp_path / "f.bin"
        target.write_bytes(b"payload")
        digest = hashlib.sha256(b"payload").hexdigest()
        result = verify_file(target, [parse_checksum(f"sha256:{digest}")])
        assert result["sha256"] == digest

    def test_mismatch(self, tmp_path: Path) -> None:
        target = tmp_path / "f.bin"
        target.write_bytes(b"payload")
        with pytest.raises(ChecksumError) as excinfo:
            verify_file(target, [parse_checksum("sha256:" + "0" * 64)])
        assert "mismatch" in str(excinfo.value)
        assert excinfo.value.exit_code == 5

    def test_multiple_algorithms(self, tmp_path: Path) -> None:
        target = tmp_path / "f.bin"
        target.write_bytes(b"payload")
        specs = [
            parse_checksum(f"sha256:{hashlib.sha256(b'payload').hexdigest()}"),
            parse_checksum(f"md5:{hashlib.md5(b'payload').hexdigest()}"),
        ]
        assert set(verify_file(target, specs)) == {"sha256", "md5"}
