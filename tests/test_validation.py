# SPDX-License-Identifier: MIT
import zipfile

import pytest

from snakemake_storage_plugin_cached_http.cache import Cache, RequestIdentity
from snakemake_storage_plugin_cached_http.validation import materialize_validated


@pytest.mark.parametrize("data,kind", [(b"", "csv"), (b"<html>blocked", "csv"), (b"wrong", "xls"), (b"PK truncated", "xlsx"), (b"not json", "json")])
def test_invalid_source_does_not_replace_output(tmp_path, data, kind):
    source, output = tmp_path / "source", tmp_path / "output"
    source.write_bytes(data)
    output.write_bytes(b"previous")
    with pytest.raises((ValueError, zipfile.BadZipFile)):
        materialize_validated(source, output, kind)
    assert output.read_bytes() == b"previous"


def test_json_error_policy_is_explicit(tmp_path):
    source, output = tmp_path / "source", tmp_path / "out.json"
    source.write_bytes(b'{"error":"unavailable"}')
    with pytest.raises(ValueError, match="Source API error"):
        materialize_validated(source, output, reject_api_errors=True)
    materialize_validated(source, output)
    assert output.read_bytes() == source.read_bytes()


def _staged(tmp_path, body, staged_body=None):
    cache = Cache(tmp_path / "cache")
    request = RequestIdentity.create("https://example.org/data.xlsx")
    source = tmp_path / "upstream"
    source.write_bytes(body)
    cache.import_file(request, source)
    staged = tmp_path / "stage" / request.key
    staged.parent.mkdir()
    staged.write_bytes(body if staged_body is None else staged_body)
    return cache, request, staged


def test_rejected_staged_input_is_discarded_from_cache(tmp_path):
    cache, request, staged = _staged(tmp_path, b"<!DOCTYPE html>Maintenance en cours")
    with pytest.raises(ValueError, match="HTML response"):
        materialize_validated(staged, tmp_path / "out.xlsx", cache=tmp_path / "cache")
    assert cache.inspect(request) is None
    assert not (tmp_path / "out.xlsx").exists()


def test_cache_root_defaults_to_provider_environment(tmp_path, monkeypatch):
    cache, request, staged = _staged(tmp_path, b"PK truncated")
    monkeypatch.setenv("SNAKEMAKE_STORAGE_CACHED_HTTP_CACHE", str(tmp_path / "cache"))
    with pytest.raises(zipfile.BadZipFile):
        materialize_validated(staged, tmp_path / "out.xlsx")
    assert cache.inspect(request) is None


def test_refreshed_or_accepted_generation_is_kept(tmp_path):
    cache, request, staged = _staged(tmp_path, b"a,b\n1,2\n", b"<html>old copy")
    with pytest.raises(ValueError):
        materialize_validated(staged, tmp_path / "out.csv", cache=tmp_path / "cache")
    assert cache.inspect(request) is not None
    staged.write_bytes(b"a,b\n1,2\n")
    materialize_validated(staged, tmp_path / "out.csv", cache=tmp_path / "cache")
    assert cache.inspect(request) is not None


def test_unstaged_rejection_creates_no_cache(tmp_path):
    source = tmp_path / ("0" * 64)
    source.write_bytes(b"<html>blocked")
    with pytest.raises(ValueError):
        materialize_validated(source, tmp_path / "out.csv", cache=tmp_path / "cache")
    assert not (tmp_path / "cache").exists()
