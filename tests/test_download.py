# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

"""Functional tests for downloading and checksumming from Zenodo and data.pypsa.org."""

import json
import logging
import os
import time
from contextlib import asynccontextmanager
from urllib.parse import urlparse

import httpx
import pytest

from snakemake_storage_plugin_cached_http import (
    FileMetadata,
    StorageObject,
    StorageProvider,
    StorageProviderSettings,
    WrongChecksum,
)
from tests.conftest import assert_no_http_requests

# Test URLs and their metadata paths
TEST_CONFIGS = {
    "zenodo": {
        "url": "https://zenodo.org/records/16810901/files/attributed_ports.json",
        "has_size": True,
        "is_immutable": True,
        "has_checksum": True,
    },
    "pypsa": {
        "url": "https://data.pypsa.org/workflows/eur/attributed_ports/2020-07-10/attributed_ports.json",
        "has_size": False,  # data.pypsa.org manifests don't include size
        "is_immutable": True,
        "has_checksum": True,
    },
    "gcs": {
        "url": "https://storage.googleapis.com/open-tyndp-data-store/cached-http/attributed_ports/archive/2020-07-10/attributed_ports.json",
        "has_size": True,
        "is_immutable": False,  # GCS provides modification timestamps
        "has_checksum": True,
    },
    "http": {
        "url": "https://datacatalogfiles.worldbank.org/ddh-published/0038118/1/DR0046414/attributed_ports.geojson",
        "has_size": True,
        "is_immutable": False,
        "has_checksum": False,  # Generic HTTP has no checksum
    },
}


@pytest.fixture
def test_logger():
    """Provide a logger for testing."""
    return logging.getLogger("test")


@pytest.fixture
def storage_provider(tmp_path, test_logger):
    """Create a StorageProvider instance for testing."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    local_prefix = tmp_path / "local"
    local_prefix.mkdir()

    settings = StorageProviderSettings(
        cache=str(cache_dir),
        public_cache=True,
        discard_response_cookies=True,
        skip_remote_checks=False,
        max_concurrent_downloads=3,
    )

    return StorageProvider(
        local_prefix=local_prefix,
        logger=test_logger,
        settings=settings,
    )


@pytest.fixture(params=list(TEST_CONFIGS))
def test_config(request):
    """Provide test configuration (parametrized for all backends)."""
    return TEST_CONFIGS[request.param]


@pytest.fixture(params=[k for k, v in TEST_CONFIGS.items() if not v["is_immutable"]])
def mutable_test_config(request):
    """Provide test configuration for mutable sources only (those with mtime support)."""
    return TEST_CONFIGS[request.param]


@pytest.fixture
def storage_object(test_config, storage_provider):
    """Create a StorageObject for the test file (parametrized for all backends)."""
    obj = StorageObject(
        query=test_config["url"],
        keep_local=False,
        retrieve=True,
        provider=storage_provider,
    )
    yield obj


@pytest.mark.asyncio
async def test_metadata_fetch(storage_provider, test_config):
    """Test that we can fetch metadata from the API/manifest."""
    metadata = await storage_provider.get_metadata(urlparse(test_config["url"]))

    assert metadata is not None
    if test_config["has_checksum"]:
        assert metadata.checksum is not None
        assert metadata.checksum.startswith("md5:")
    if test_config["has_size"]:
        assert metadata.size > 0


@pytest.mark.asyncio
async def test_storage_object_exists(storage_object):
    """Test that the storage object reports existence correctly."""
    exists = await storage_object.managed_exists()
    assert exists is True


@pytest.mark.asyncio
async def test_storage_object_size(storage_object, test_config):
    """Test that the storage object reports size correctly."""
    size = await storage_object.managed_size()
    if test_config["has_size"]:
        assert size > 0
        # The file is a small JSON file, should be less than 1MB
        assert size < 1_000_000
    else:
        # data.pypsa.org manifests don't include size
        assert size == 0


@pytest.mark.asyncio
async def test_storage_object_mtime(storage_object, test_config):
    """Test that mtime is 0 for immutable URLs, non-zero for mutable sources."""
    mtime = await storage_object.managed_mtime()
    if test_config["is_immutable"]:
        assert mtime == 0
    else:
        assert mtime > 0


@pytest.mark.asyncio
async def test_download_and_checksum(storage_object, tmp_path):
    """Test downloading a file and verifying its checksum."""
    local_path = tmp_path / "test_download" / "attributed_ports.json"
    local_path.parent.mkdir(parents=True, exist_ok=True)

    # Mock the local_path method to return our test path
    storage_object.local_path = lambda: local_path

    # Download the file
    await storage_object.managed_retrieve()

    # Verify file was downloaded
    assert local_path.exists()
    assert local_path.stat().st_size > 0

    # Verify it's valid JSON (raw_decode ignores trailing garbage that the worldbank server appends)
    with open(local_path, encoding="utf-8", errors="replace") as f:
        content = f.read()
    data, _ = json.JSONDecoder().raw_decode(content)
    assert isinstance(data, (dict, list))

    # Verify checksum (should not raise WrongChecksum exception)
    await storage_object.verify_checksum(local_path)


@pytest.mark.asyncio
async def test_cache_functionality(storage_provider, test_config, tmp_path):
    """Test that files are cached after download."""
    url = test_config["url"]

    # First download
    obj1 = StorageObject(
        query=url,
        keep_local=False,
        retrieve=True,
        provider=storage_provider,
    )

    local_path1 = tmp_path / "download1" / "attributed_ports.json"
    local_path1.parent.mkdir(parents=True, exist_ok=True)
    obj1.local_path = lambda: local_path1

    await obj1.managed_retrieve()

    # Verify cache was populated
    assert obj1.provider.cache is not None
    cached_path = obj1.provider.cache.get(url)
    assert cached_path is not None
    assert cached_path.exists()

    # Second download should use cache - verify by checking no HTTP requests are made
    obj2 = StorageObject(
        query=url,
        keep_local=False,
        retrieve=True,
        provider=storage_provider,
    )

    local_path2 = tmp_path / "download2" / "attributed_ports.json"
    local_path2.parent.mkdir(parents=True, exist_ok=True)
    obj2.local_path = lambda: local_path2

    # Verify no HTTP requests are made (cache hit skips download, metadata is cached)
    with assert_no_http_requests(storage_provider):
        await obj2.managed_retrieve()

    # Both files should be identical
    assert local_path1.read_bytes() == local_path2.read_bytes()


@pytest.mark.asyncio
async def test_skip_remote_checks(test_config, tmp_path, test_logger):
    """Test that skip_remote_checks works correctly."""
    local_prefix = tmp_path / "local"
    local_prefix.mkdir()

    # Create provider with skip_remote_checks enabled
    settings = StorageProviderSettings(
        cache="",  # No cache
        skip_remote_checks=True,
        max_concurrent_downloads=3,
    )

    provider_skip = StorageProvider(
        local_prefix=local_prefix,
        logger=test_logger,
        settings=settings,
    )

    obj = StorageObject(
        query=test_config["url"],
        keep_local=False,
        retrieve=True,
        provider=provider_skip,
    )

    # With skip_remote_checks, these should return default values without API calls
    assert await obj.managed_exists() is True
    assert await obj.managed_mtime() == 0
    assert await obj.managed_size() == 0


@pytest.mark.asyncio
async def test_wrong_checksum_detection(storage_object, test_config, tmp_path):
    """Test that corrupted files are detected via checksum."""
    corrupted_path = tmp_path / "corrupted.json"
    corrupted_path.write_text('{"corrupted": "data"}')

    if test_config["has_checksum"]:
        with pytest.raises(WrongChecksum):
            await storage_object.verify_checksum(corrupted_path)
    else:
        await storage_object.verify_checksum(corrupted_path)


@pytest.mark.asyncio
async def test_cache_staleness_for_mutable_sources(
    mutable_test_config, tmp_path, test_logger
):
    """Test that stale cached files are re-downloaded for mutable sources."""
    url = mutable_test_config["url"]

    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    local_prefix = tmp_path / "local"
    local_prefix.mkdir()

    settings = StorageProviderSettings(
        cache=str(cache_dir),
        public_cache=True,
        discard_response_cookies=True,
        skip_remote_checks=False,
        max_concurrent_downloads=3,
    )

    provider = StorageProvider(
        local_prefix=local_prefix,
        logger=test_logger,
        settings=settings,
    )

    # First download to populate cache
    obj1 = StorageObject(
        query=url,
        keep_local=False,
        retrieve=True,
        provider=provider,
    )

    local_path1 = tmp_path / "download1" / "testfile"
    local_path1.parent.mkdir(parents=True, exist_ok=True)
    obj1.local_path = lambda: local_path1

    await obj1.managed_retrieve()

    # Verify cache was populated
    assert provider.cache is not None
    cached_path = provider.cache.get(url)
    assert cached_path is not None
    assert cached_path.exists()

    # Modify the cached file slightly
    original_content = cached_path.read_bytes()
    modified_content = original_content.replace(b"}", b', "stale": true}')
    cached_path.write_bytes(modified_content)

    # Set mtime to 5 years ago
    five_years_ago = time.time() - (5 * 365 * 24 * 60 * 60)
    os.utime(cached_path, (five_years_ago, five_years_ago))

    # Clear metadata cache to force re-fetch
    provider._gcs_metadata_cache.clear()

    # Second download should detect stale cache and re-download
    obj2 = StorageObject(
        query=url,
        keep_local=False,
        retrieve=True,
        provider=provider,
    )

    local_path2 = tmp_path / "download2" / "testfile"
    local_path2.parent.mkdir(parents=True, exist_ok=True)
    obj2.local_path = lambda: local_path2

    await obj2.managed_retrieve()

    # Verify cache was populated
    assert provider.cache is not None
    cached_path = provider.cache.get(url)
    assert cached_path is not None
    assert cached_path.exists()

    # The downloaded file should be the original, not the stale modified version
    downloaded_content = cached_path.read_bytes()
    assert b'"stale": true' not in downloaded_content
    assert downloaded_content == original_content


def make_mock_httpr(content: bytes, fail_at: int | None):
    """
    Factory for a mock httpr context manager simulating a range-capable HTTP server.

    Args:
        content: The full file content to serve.
        fail_at: If set, drop the connection after serving this many bytes on the
                 first (non-Range) request, simulating a mid-transfer interruption.
                 If None, serve the full content without interruption.
    """

    @asynccontextmanager
    async def mock_httpr(method, request_url, headers=None):
        range_header = headers.get("Range") if headers else None
        mock_httpr.received_range_headers.append(range_header)

        response = httpx.Response(200, request=httpx.Request(method, request_url))
        if range_header is None:
            response.status_code = 200
            chunk = content
            drop_at = fail_at
        else:
            response.status_code = 206
            offset = int(range_header.removeprefix("bytes=").removesuffix("-"))
            chunk = content[offset:]
            drop_at = None

        async def aiter_bytes(chunk_size=8192):
            if drop_at is None:
                yield chunk
            else:
                yield chunk[:drop_at]
                raise httpx.ReadError("peer closed connection")

        response.aiter_bytes = aiter_bytes
        response.headers = httpx.Headers(
            {"content-length": str(len(chunk)), "etag": '"v1"'}
        )
        if range_header is not None:
            response.headers["content-range"] = (
                f"bytes {offset}-{len(content) - 1}/{len(content)}"
            )

        yield response

    mock_httpr.received_range_headers = []

    return mock_httpr


@pytest.mark.asyncio
async def test_resume_on_partial_file(storage_provider, tmp_path):
    """Test that downloads resume from partial files using HTTP Range requests."""
    url = TEST_CONFIGS["zenodo"]["url"]
    full_content = b'{"port": "test", "lat": 1.0, "lon": 2.0}'

    fail_at = 10
    mock_httpr = make_mock_httpr(full_content, fail_at=fail_at)
    storage_provider.httpr = mock_httpr
    storage_provider.cache = None

    obj = StorageObject(
        query=url,
        keep_local=False,
        retrieve=True,
        provider=storage_provider,
    )

    local_path = tmp_path / "resume_test" / "file.json"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    obj.local_path = lambda: local_path

    async def mock_get_metadata(url):
        return FileMetadata(checksum=None, size=0, mtime=0)

    obj.provider.get_metadata = mock_get_metadata

    await obj.managed_retrieve()

    assert len(mock_httpr.received_range_headers) == 2
    assert (
        mock_httpr.received_range_headers[0] is None
    )  # first attempt: no Range header
    assert mock_httpr.received_range_headers[1] == f"bytes={fail_at}-"
    assert local_path.read_bytes() == full_content
