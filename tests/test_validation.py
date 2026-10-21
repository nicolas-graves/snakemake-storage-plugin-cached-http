# SPDX-License-Identifier: MIT
import zipfile

import pytest

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
