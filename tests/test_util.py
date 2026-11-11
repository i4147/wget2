from __future__ import annotations

from pathlib import Path

import pytest

from wget2.errors import SchemeError, UsageError
from wget2.util import (
    content_disposition_filename,
    filename_from_url,
    human_size,
    parse_int_list,
    parse_size,
    redact,
    sanitize_filename,
    url_without_credentials,
    validate_url,
)


class TestSanitizeFilename:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("../../etc/passwd", "passwd"),
            ("..\\..\\windows\\system32\\cmd.exe", "cmd.exe"),
            ("/absolute/path/file.bin", "file.bin"),
            ("file\x00name.bin", "filename.bin"),
            ("bad\x01\x02name", "badname"),
            ("a<b>c:d|e?f*g.bin", "a_b_c_d_e_f_g.bin"),
            ("..", "download"),
            (".", "download"),
            ("", "download"),
            ("  spaced.bin  ", "spaced.bin"),
            ("CON", "_CON"),
            ("nul.txt", "_nul.txt"),
        ],
    )
    def test_sanitizes(self, raw: str, expected: str) -> None:
        assert sanitize_filename(raw) == expected

    def test_percent_encoded_traversal(self) -> None:
        assert sanitize_filename("%2e%2e%2fetc%2fpasswd") == "passwd"

    def test_long_names_are_truncated(self) -> None:
        name = "a" * 500 + ".bin"
        result = sanitize_filename(name)
        assert len(result) <= 180
        assert result.endswith(".bin")

    def test_fallback(self) -> None:
        assert sanitize_filename("....", fallback="x.bin") == "x.bin"


class TestFilenameFromUrl:
    def test_plain(self) -> None:
        assert filename_from_url("https://example.com/a/b/file.tar.gz") == "file.tar.gz"

    def test_traversal_url(self) -> None:
        assert filename_from_url("https://example.com/..%2f..%2fetc/passwd") == "passwd"

    def test_directory_url(self) -> None:
        assert filename_from_url("https://example.com/dir/") == "example.com-index.html"

    def test_query_only(self) -> None:
        assert filename_from_url("https://example.com/download?id=3") == "download"


class TestSizes:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("10", 10),
            ("10k", 10 * 1024),
            ("1.5M", int(1.5 * 1024 * 1024)),
            ("2GiB", 2 * 1024**3),
            (None, None),
            ("0", None),
            ("unlimited", None),
        ],
    )
    def test_parse_size(self, value, expected) -> None:
        assert parse_size(value) == expected

    def test_invalid_size(self) -> None:
        with pytest.raises(UsageError):
            parse_size("ten megabytes")

    def test_human_size(self) -> None:
        assert human_size(0) == "0 B"
        assert human_size(1536) == "1.5 KiB"
        assert human_size(None) == "?"

    def test_parse_int_list(self) -> None:
        assert parse_int_list("429,500-502") == frozenset({429, 500, 501, 502})
        assert parse_int_list("") == frozenset()
        with pytest.raises(UsageError):
            parse_int_list("abc")


class TestUrls:
    def test_allowed(self) -> None:
        assert validate_url("https://example.com/x") == "https://example.com/x"

    @pytest.mark.parametrize("url", ["file:///etc/passwd", "ftp://example.com/x"])
    def test_denied_schemes(self, url: str) -> None:
        with pytest.raises(SchemeError):
            validate_url(url)
        assert validate_url(url, allow_extra_schemes=True)

    def test_always_unsupported_scheme(self) -> None:
        with pytest.raises(SchemeError):
            validate_url("data:text/plain,hi", allow_extra_schemes=True)

    def test_missing_scheme(self) -> None:
        with pytest.raises(UsageError):
            validate_url("example.com/x")

    def test_no_host(self) -> None:
        with pytest.raises(UsageError):
            validate_url("https:///path")

    def test_credentials_stripped(self) -> None:
        assert url_without_credentials("https://user:pass@example.com/a") == "https://example.com/a"


class TestContentDisposition:
    def test_quoted(self) -> None:
        header = 'attachment; filename="report.pdf"'
        assert content_disposition_filename(header) == "report.pdf"

    def test_rfc5987(self) -> None:
        header = "attachment; filename*=UTF-8''na%C3%AFve%20file.txt"
        assert content_disposition_filename(header) == "naïve file.txt"

    def test_traversal_blocked(self) -> None:
        header = 'attachment; filename="../../evil.sh"'
        assert content_disposition_filename(header) == "evil.sh"

    def test_none(self) -> None:
        assert content_disposition_filename(None) is None


class TestRedaction:
    def test_url_credentials(self) -> None:
        assert "s3cret" not in redact("fetching https://user:s3cret@example.com/x")

    def test_bearer(self) -> None:
        out = redact("Authorization: Bearer abcdef123456")
        assert "abcdef123456" not in out
        assert "Authorization" in out

    def test_kv_secret(self) -> None:
        out = redact("password=hunter2 token: xyz")
        assert "hunter2" not in out
        assert "xyz" not in out


def test_parse_http_date_roundtrip() -> None:
    from wget2.util import format_http_date, parse_http_date

    stamp = 1445412480.0
    assert parse_http_date(format_http_date(stamp)) == stamp


def test_atomic_finalize(tmp_path: Path) -> None:
    from wget2.util import atomic_finalize

    src = tmp_path / "a.part"
    src.write_bytes(b"payload")
    dst = tmp_path / "a.bin"
    atomic_finalize(src, dst)
    assert dst.read_bytes() == b"payload"
    assert not src.exists()


def test_ensure_free_space(tmp_path: Path) -> None:
    from wget2.errors import DiskSpaceError
    from wget2.util import ensure_free_space

    ensure_free_space(tmp_path, 1024)
    with pytest.raises(DiskSpaceError):
        ensure_free_space(tmp_path, 10**18)


def test_unique_path(tmp_path: Path) -> None:
    from wget2.util import unique_path

    target = tmp_path / "file.bin"
    assert unique_path(target) == target
    target.write_text("x")
    assert unique_path(target) == tmp_path / "file.1.bin"


def test_preallocate(tmp_path: Path) -> None:
    from wget2.util import preallocate

    path = tmp_path / "sparse.bin"
    preallocate(path, 4096)
    assert path.stat().st_size == 4096
