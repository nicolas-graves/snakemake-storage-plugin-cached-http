# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

"""Basic import tests for the snakemake-storage-plugin-cached-http package."""


def test_import_module():
    """Test that the main module can be imported."""
    import snakemake_storage_plugin_cached_http

    assert snakemake_storage_plugin_cached_http is not None


def test_import_storage_provider():
    """Test that StorageProvider class can be imported."""
    from snakemake_storage_plugin_cached_http import StorageProvider

    assert StorageProvider is not None


def test_import_storage_object():
    """Test that StorageObject class can be imported."""
    from snakemake_storage_plugin_cached_http import StorageObject

    assert StorageObject is not None


def test_storage_provider_has_required_methods():
    """Test that StorageProvider has required interface methods."""
    from snakemake_storage_plugin_cached_http import StorageProvider

    # Check for key methods required by the storage plugin interface
    assert hasattr(StorageProvider, "is_valid_query")
    assert hasattr(StorageProvider, "example_queries")
    assert hasattr(StorageProvider, "get_storage_object_cls")


def test_is_valid_query_zenodo():
    """Test that is_valid_query accepts zenodo.org URLs."""
    from snakemake_storage_plugin_cached_http import StorageProvider

    # Valid Zenodo URL
    result = StorageProvider.is_valid_query(
        "https://zenodo.org/records/3520874/files/natura.tiff"
    )
    assert result.valid is True

    # Valid sandbox Zenodo URL
    result = StorageProvider.is_valid_query(
        "https://sandbox.zenodo.org/records/123/files/test.csv"
    )
    assert result.valid is True


def test_is_valid_query_pypsa():
    """Test that is_valid_query accepts data.pypsa.org URLs."""
    from snakemake_storage_plugin_cached_http import StorageProvider

    result = StorageProvider.is_valid_query(
        "https://data.pypsa.org/workflows/eur/eez/v12_20231025/World_EEZ_v12_20231025_LR.zip"
    )
    assert result.valid is True


def test_is_valid_query_generic_http():
    """Test that is_valid_query accepts generic http/https URLs as fallback."""
    from snakemake_storage_plugin_cached_http import StorageProvider

    # Generic HTTP URL should be accepted as fallback
    result = StorageProvider.is_valid_query("https://example.com/file.txt")
    assert result.valid is True

    # Non-HTTP schemes should be rejected
    result = StorageProvider.is_valid_query("ftp://example.com/file.txt")
    assert result.valid is False

    result = StorageProvider.is_valid_query("s3://bucket/key")
    assert result.valid is False


def test_example_queries():
    """Test that example queries are provided and valid."""
    from snakemake_storage_plugin_cached_http import StorageProvider

    examples = StorageProvider.example_queries()
    assert len(examples) > 0
    assert all(hasattr(ex, "query") for ex in examples)
    assert all(hasattr(ex, "description") for ex in examples)


def test_http_provider_coexists_without_mutation():
    import subprocess
    import sys

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import snakemake_storage_plugin_http as http
before = http.StorageProvider.is_valid_query.__func__
import snakemake_storage_plugin_cached_http
assert http.StorageProvider.is_valid_query.__func__ is before
assert http.StorageProvider.is_valid_query('https://example.org/file').valid
""",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    # The integration dependency is optional outside the coexistence test job.
    if (
        result.returncode
        and "No module named 'snakemake_storage_plugin_http'" in result.stderr
    ):
        __import__("pytest").skip("ordinary HTTP plugin is not installed")
    assert result.returncode == 0, result.stderr
