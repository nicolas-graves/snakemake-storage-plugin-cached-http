# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

import asyncio
import base64
import hashlib
import json
import tempfile
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from email.utils import parsedate_to_datetime
from logging import Logger
from pathlib import Path
from posixpath import dirname, join, normpath, relpath
from typing import cast
from urllib.parse import ParseResult, quote, urlparse

import httpx
import platformdirs
import yaml
from reretry import retry  # pyright: ignore[reportUnknownVariableType]
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_common.logging import get_logger
from snakemake_interface_common.plugin_registry.plugin import SettingsBase
from snakemake_interface_storage_plugins.common import Operation
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface, Mtime
from snakemake_interface_storage_plugins.storage_object import StorageObjectRead
from snakemake_interface_storage_plugins.storage_provider import (
    ExampleQuery,
    QueryType,
    StorageProviderBase,
    StorageQueryValidationResult,
)
from typing_extensions import override

from .cache import Cache, RequestIdentity
from .retrieval import cached_metadata, fetch

logger = get_logger()


class ReretryLoggerAdapter:
    """Adapter to make Snakemake's logger compatible with reretry's logging expectations."""

    _logger: Logger

    def __init__(self, snakemake_logger: Logger):
        self._logger = snakemake_logger

    def warning(self, msg: str, *args, **kwargs):  # pyright: ignore[reportUnknownParameterType, reportUnusedParameter, reportMissingParameterType]
        """
        Format message manually before passing to Snakemake logger.

        This is necessary because Snakemake's DefaultFormatter has a bug where it
        returns record["msg"] without calling interpolating the args. This causes
        literal "%s" to appear in log output instead of formatted values.
        """
        if args:
            # Pre-format the message with % operator
            msg = msg % args
        self._logger.warning(msg)


# Define settings for the Zenodo storage plugin
# NB: We derive from SettingsBase rather than StorageProviderSettingsBase to remove the
# unsupported max_requests_per_second option
@dataclass
class StorageProviderSettings(SettingsBase):
    public_cache: bool = field(
        default=False,
        metadata={
            "help": "Assert that all configured responses are public; required for persistent caching.",
            "env_var": True,
        },
    )
    discard_response_cookies: bool = field(
        default=False,
        metadata={
            "help": "Explicitly allow public responses that set cookies; cookies are discarded and never sent.",
            "env_var": True,
        },
    )
    headers: dict[str, str] = field(
        default_factory=dict,
        metadata={
            "help": "Anonymous representation headers (Accept, Accept-Language, Accept-Encoding, User-Agent)."
        },
    )
    policy: str = field(
        default="auto",
        metadata={
            "help": "auto, checked, immutable, offline or refresh",
            "env_var": True,
        },
    )
    check_interval: float = field(
        default=3600,
        metadata={
            "help": "Seconds between conditional retrieval checks",
            "env_var": True,
        },
    )

    cache: str = field(
        default_factory=lambda: platformdirs.user_cache_dir(
            "snakemake-pypsa-eur", ensure_exists=False
        ),
        metadata={
            "help": 'Cache directory for downloaded files (default: platform-dependent user cache dir). Set to "" to deactivate caching.',
            "env_var": True,
        },
    )
    skip_remote_checks: bool = field(
        default=False,
        metadata={
            "help": "Whether to skip metadata checking with remote server (default: False, ie. do check).",
            "env_var": True,
        },
    )
    max_concurrent_downloads: int = field(
        default=3,
        metadata={
            "help": "Maximum number of concurrent downloads.",
            "env_var": False,
        },
    )


@dataclass
class FileMetadata:
    """Metadata for a file in a Zenodo, data.pypsa.org, or GCS record."""

    checksum: str | None
    size: int
    mtime: float = 0  # modification time (Unix timestamp), used for GCS
    redirect: str | None = None  # used to indicate data.pypsa.org redirection
    etag: str | None = None
    last_modified: str | None = None
    content_type: str | None = None
    content_disposition: str | None = None
    final_url: str | None = None


class WrongChecksum(Exception):
    observed: str
    expected: str

    def __init__(self, observed: str, expected: str):
        self.observed = observed
        self.expected = expected
        super().__init__(f"Checksum mismatch: expected {expected}, got {observed}")


retry_decorator = retry(
    exceptions=(  # pyright: ignore[reportArgumentType]
        httpx.HTTPError,
        OSError,
        WrongChecksum,
    ),
    tries=5,
    delay=3,
    backoff=2,
    logger=ReretryLoggerAdapter(get_logger()),  # pyright: ignore[reportArgumentType]
)


# Implementation of storage provider
class StorageProvider(StorageProviderBase):
    settings: StorageProviderSettings
    cache: Cache | None

    def __post_init__(self):
        super().__post_init__()

        if self.settings.policy not in {
            "auto",
            "checked",
            "immutable",
            "offline",
            "refresh",
        }:
            raise ValueError("Invalid cached-http policy")
        if (
            self.settings.check_interval < 0
            or self.settings.max_concurrent_downloads < 1
        ):
            raise ValueError(
                "check_interval must be nonnegative and concurrency positive"
            )
        RequestIdentity.create("https://example.org/", self.settings.headers)
        if self.settings.cache and not self.settings.public_cache:
            raise ValueError(
                "Persistent caching requires public_cache=True: assert that the configured responses are public"
            )
        # Set up cache
        self.cache = (
            Cache(cache_dir=Path(self.settings.cache)) if self.settings.cache else None
        )

        # Initialize shared client for bounding connections and pipelining
        self._client: httpx.AsyncClient | None = None
        self._client_refcount: int = 0

        # Cache for record metadata to avoid repeated API calls
        self._zenodo_record_cache: dict[str, dict[str, FileMetadata]] = {}
        self._pypsa_manifest_cache: dict[str, dict[str, FileMetadata]] = {}
        self._gcs_metadata_cache: dict[str, FileMetadata] = {}
        self._inventory_metadata: dict[str, FileMetadata | None] = {}

    @override
    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        return False

    @override
    def rate_limiter_key(self, query: str, operation: Operation) -> str:
        raise NotImplementedError()

    @override
    def default_max_requests_per_second(self) -> float:
        raise NotImplementedError()

    @override
    @classmethod
    def example_queries(cls) -> list[ExampleQuery]:
        """Return an example query with description for this storage provider."""
        return [
            ExampleQuery(
                query="https://zenodo.org/records/17249457/files/ARDECO-SNPTD.2021.table.csv",
                description="A Zenodo file URL",
                type=QueryType.INPUT,
            ),
            ExampleQuery(
                query="https://data.pypsa.org/workflows/eur/eez/v12_20231025/World_EEZ_v12_20231025_LR.zip",
                description="A data pypsa file URL",
                type=QueryType.INPUT,
            ),
            ExampleQuery(
                query="https://storage.googleapis.com/open-tyndp-data-store/CBA_projects.zip",
                description="A Google Cloud Storage file URL",
                type=QueryType.INPUT,
            ),
            ExampleQuery(
                query="https://example.com/data/file.csv",
                description="A generic HTTP/HTTPS file URL",
                type=QueryType.INPUT,
            ),
        ]

    @override
    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Handle all http/https URLs."""
        parsed = urlparse(query)
        if parsed.scheme in ("http", "https") and parsed.netloc:
            return StorageQueryValidationResult(query=query, valid=True)

        return StorageQueryValidationResult(
            query=query,
            valid=False,
            reason="Only http and https URLs are handled by this plugin",
        )

    @override
    @classmethod
    def get_storage_object_cls(cls):
        return StorageObject

    @asynccontextmanager
    async def client(self):
        """
        Reentrant async context manager for httpx.AsyncClient.

        Creates a client on first entry and reuses it for nested calls.
        The client is closed only when all context managers have exited.

        Usage:
            async with provider.client() as client:
                response = await client.get(url)
                ...
        """
        self._client_refcount += 1

        # Create client on first entry
        if self._client is None:
            max_concurrent_downloads = self.settings.max_concurrent_downloads
            limits = httpx.Limits(
                max_keepalive_connections=max_concurrent_downloads,
                max_connections=max_concurrent_downloads,
            )
            timeout = httpx.Timeout(60, pool=None)

            self._client = httpx.AsyncClient(
                follow_redirects=False, limits=limits, timeout=timeout, trust_env=False
            )

        try:
            yield self._client
        finally:
            self._client_refcount -= 1
            if self._client_refcount == 0:
                await self._client.aclose()
                self._client = None

    def _get_rate_limit_wait_time(self, headers: httpx.Headers) -> float | None:
        """
        Calculate wait time based on rate limit headers.

        Returns:
            float | None: Wait time in seconds if rate limited, None otherwise
        """
        remaining = int(headers.get("X-RateLimit-Remaining", 100))
        reset_time = int(headers.get("X-RateLimit-Reset", 0))

        if remaining >= 1:
            return None

        wait_seconds = max(0, reset_time - time.time() + 1)
        return wait_seconds

    @asynccontextmanager
    async def httpr(self, method: str, url: str, headers: dict[str, str] | None = None):
        """
        HTTP request wrapper with rate limiting and exception logging.

        Args:
            method: HTTP method (e.g., "get", "post")
            url: URL to request
            headers: Optional additional HTTP headers

        Yields:
            httpx.Response object
        """
        try:
            async with (
                self.client() as client,
                self._stream_public(client, method, url, headers) as response,
            ):
                wait_time = self._get_rate_limit_wait_time(response.headers)
                if wait_time is not None:
                    logger.info(
                        f"Zenodo rate limit exceeded. Waiting {wait_time:.0f}s until reset..."
                    )
                    await asyncio.sleep(wait_time)
                    raise httpx.HTTPError("Rate limit exceeded, retrying after wait")

                yield response
        except Exception as e:
            logger.warning(f"{type(e).__name__} during public HTTP request")
            raise

    @asynccontextmanager
    async def _stream_public(self, client, method, url, headers):
        # Follow redirects explicitly: inspect every hop before forwarding anything.
        effective = dict(RequestIdentity.create(url, self.settings.headers).headers)
        effective.update(headers or {})
        for _ in range(20):
            RequestIdentity.create(url, self.settings.headers)
            request = client.build_request(method, url, headers=effective)
            request.headers.pop("cookie", None)
            response = await client.send(request, stream=True)
            try:
                from .retrieval import validate_response

                validate_response(
                    response,
                    cacheable=False,
                    allow_cookies=self.settings.discard_response_cookies,
                )
                if response.is_redirect and response.next_request is not None:
                    url = str(response.next_request.url)
                    continue
                yield response
                return
            finally:
                await response.aclose()
                client.cookies.clear()
        raise ValueError("Too many HTTP redirects")

    async def fetch(self, url: str, destination: Path, *, policy: str | None = None):
        """Materialize an anonymous representation; return verified response metadata.

        Destination belongs to the caller. No cache-internal path is exposed.
        Use settings.headers to choose representation, and policy='refresh' for
        explicit validation. This asynchronous API shares storage policy/locking.
        """
        return await fetch(self, url, Path(destination), policy=policy)

    @retry_decorator
    async def get_metadata(self, url: ParseResult) -> FileMetadata | None:
        """
        Retrieve and cache file metadata for a Zenodo record, data.pypsa.org file,
        GCS object, or generic HTTP URL.

        Args:
            url: Parsed URL

        Returns:
            FileMetadata or None if not found
        """
        if self.settings.policy == "offline":
            result = await cached_metadata(self, url.geturl())
            return result
        netloc = url.netloc
        if self.settings.headers or url.query:
            return await self.get_http_metadata(url)
        if netloc in ("zenodo.org", "sandbox.zenodo.org"):
            return await self.get_zenodo_metadata(url)
        elif netloc == "data.pypsa.org":
            return await self.get_pypsa_metadata(url)
        elif netloc == "storage.googleapis.com":
            return await self.get_gcs_metadata(url)

        return await self.get_http_metadata(url)

    @staticmethod
    def is_immutable(url: ParseResult) -> bool:
        # Source compatibility helper; operational policy additionally accounts
        # for representation headers in retrieval.immutable.
        if url.query:
            return False
        return (
            url.netloc in {"zenodo.org", "sandbox.zenodo.org"}
            and url.path.startswith("/records/")
            and "/files/" in url.path
        ) or (url.netloc == "data.pypsa.org" and url.path.startswith("/workflows/"))

    async def get_http_metadata(self, parsed: ParseResult) -> FileMetadata | None:
        """
        Retrieve file metadata for a generic HTTP URL via HEAD request.

        Args:
            url: Parsed URL

        Returns:
            FileMetadata with size and mtime from HTTP headers, or None if not found
        """
        url = parsed.geturl()

        async with self.httpr("head", url) as response:
            # HEAD never downloads a body, including unsupported/inadequate HEAD.
            pass

        if response.status_code == 404:
            return None
        if response.status_code in (403, 405, 501):
            # HEAD not supported; assume file exists with unknown size/mtime
            return FileMetadata(checksum=None, size=0, mtime=0.0)
        response.raise_for_status()

        size = int(response.headers.get("content-length", 0))

        last_modified = response.headers.get("last-modified")
        if last_modified:
            try:
                mtime = cast(datetime, parsedate_to_datetime(last_modified)).timestamp()
            except (TypeError, ValueError, OverflowError):
                logger.debug(
                    f"HTTP last-modified not in RFC2822 format: `{last_modified}`"
                )
                mtime = 0.0
        else:
            mtime = 0.0

        identity = RequestIdentity.create(url, self.settings.headers)
        if self.cache:
            old = await asyncio.to_thread(self.cache.inspect, identity)
            if old:
                etag = response.headers.get("etag")
                if etag and old.get("etag") and etag != old["etag"]:
                    mtime = max(time.time(), old["mtime"] + 1)
                elif not mtime:
                    mtime = old["mtime"]
        return FileMetadata(
            checksum=None,
            size=size,
            mtime=mtime,
            etag=response.headers.get("etag"),
            last_modified=response.headers.get("last-modified"),
            content_type=response.headers.get("content-type"),
            content_disposition=response.headers.get("content-disposition"),
            final_url=str(response.url),
        )

    async def get_zenodo_metadata(self, url: ParseResult) -> FileMetadata | None:
        """
        Retrieve and cache file metadata for a Zenodo record or a data.pypsa.org file.

        Args:
            url: Parsed URL

        Returns:
            Dictionary mapping filename to FileMetadata
        """
        netloc = url.netloc
        path = url.path.strip("/")

        # Zenodo record
        _records, record_id, _files, filename = path.split("/", maxsplit=3)

        if _records != "records" or _files != "files":
            raise WorkflowError(
                f"Invalid Zenodo URL format: {url.geturl()}. "
                f"Expected format: https://zenodo.org/records/{{record_id}}/files/{{filename}}"
            )

        # Check cache first
        if record_id in self._zenodo_record_cache:
            return self._zenodo_record_cache[record_id].get(filename)

        # Fetch from API
        api_url = f"https://{netloc}/api/records/{record_id}"

        async with self.httpr("get", api_url) as response:
            response.raise_for_status()

            # Read the full response body
            content = await response.aread()
            data = json.loads(content)

        # Parse files array and build metadata dict
        metadata: dict[str, FileMetadata] = {}
        files = data.get("files", [])
        for file_info in files:
            fn: str | None = file_info.get("key")
            checksum: str | None = file_info.get("checksum")
            size: int = file_info.get("size", 0)

            if not fn:
                continue

            metadata[fn] = FileMetadata(checksum=checksum, size=size)

        # Store in cache
        self._zenodo_record_cache[record_id] = metadata

        return metadata.get(filename)

    async def get_pypsa_metadata(self, url: ParseResult) -> FileMetadata | None:
        """
        Retrieve and cache file metadata from data.pypsa.org manifest.

        Args:
            url: Parsed URL

        Returns:
            FileMetadata for the requested file, or None if not found
        """
        # Check cache first
        path = url.path.strip("/")

        base_path: str = dirname(path)
        while base_path:
            if base_path in self._pypsa_manifest_cache:
                filename: str = relpath(path, base_path)
                return self._pypsa_manifest_cache[base_path].get(filename)
            base_path = dirname(base_path)

        # Fetch manifest
        base_path = dirname(path)
        while base_path:
            manifest_url = url._replace(path=f"/{base_path}/manifest.yaml").geturl()

            async with self.httpr("get", manifest_url) as response:
                if response.status_code == 200:
                    content = await response.aread()
                    data = yaml.safe_load(content)
                    break

            base_path = dirname(base_path)
        else:
            raise WorkflowError(
                f"Failed to fetch data.pypsa.org manifest for {url.geturl()}"
            )

        # Parse files array and build metadata dict
        metadata: dict[str, FileMetadata] = {}
        files = data.get("files", {})
        for filename, file_info in files.items():
            redirect: str | None = file_info.get("redirect")
            checksum: str | None = file_info.get("checksum")
            size: int = file_info.get("size", 0)

            if redirect is not None:
                redirect = normpath(join(base_path, redirect))

            metadata[filename] = FileMetadata(
                checksum=checksum, size=size, redirect=redirect
            )

        # Store in cache
        self._pypsa_manifest_cache[base_path] = metadata

        filename = relpath(path, base_path)
        return metadata.get(filename)

    async def get_gcs_metadata(self, url: ParseResult) -> FileMetadata | None:
        """
        Retrieve and cache file metadata from Google Cloud Storage.

        Uses the GCS JSON API to fetch object metadata including MD5 hash.
        URL format: https://storage.googleapis.com/{bucket}/{object-path}
        API endpoint: https://storage.googleapis.com/storage/v1/b/{bucket}/o/{encoded-object}

        Args:
            url: Parsed URL

        Returns:
            FileMetadata for the requested file, or None if not found
        """
        # Check cache first
        if url.path in self._gcs_metadata_cache:
            return self._gcs_metadata_cache[url.path]

        # Parse bucket and object path from the URL path
        # Path format: /{bucket}/{object-path}
        parts = url.path.strip("/").split("/", maxsplit=1)
        if len(parts) < 2:
            raise WorkflowError(
                f"Invalid GCS URL format: {url.geturl()}. "
                f"Expected format: https://storage.googleapis.com/{{bucket}}/{{object-path}}"
            )

        bucket, object_path = parts

        # URL-encode the object path for the API request (slashes must be encoded)
        encoded_object = quote(object_path, safe="")

        # GCS JSON API endpoint for object metadata
        api_url = url._replace(
            path=f"/storage/v1/b/{bucket}/o/{encoded_object}"
        ).geturl()

        async with self.httpr("get", api_url) as response:
            if response.status_code == 404:
                return None
            response.raise_for_status()

            content = await response.aread()
            data = json.loads(content)

        # GCS returns MD5 as base64-encoded bytes
        md5_base64: str | None = data.get("md5Hash")
        checksum: str | None = None
        if md5_base64:
            # Convert base64 to hex digest
            md5_bytes = base64.b64decode(md5_base64)
            checksum = f"md5:{md5_bytes.hex()}"

        size: int = int(data.get("size", 0))

        updated: str | None = data.get("updated")
        mtime: float = datetime.fromisoformat(updated).timestamp() if updated else 0

        metadata = FileMetadata(checksum=checksum, size=size, mtime=mtime)

        # Store in cache
        self._gcs_metadata_cache[url.path] = metadata

        return metadata


# Implementation of storage object
class StorageObject(StorageObjectRead):
    provider: StorageProvider  # pyright: ignore[reportIncompatibleVariableOverride]
    url: ParseResult

    def __post_init__(self):
        super().__post_init__()
        self.url = urlparse(str(self.query))

    @override
    def local_suffix(self) -> str:
        """Return the local suffix for this object (used by parent class)."""
        return RequestIdentity.create(
            str(self.query), self.provider.settings.headers
        ).key

    @override
    def get_inventory_parent(self) -> str | None:
        """Return the parent directory of this object."""
        # this is optional and can be left as is
        return None

    async def _metadata(self):
        cached = await cached_metadata(self.provider, str(self.query))
        if cached is not None:
            return cached
        if self.provider.settings.policy == "offline":
            raise FileNotFoundError("Offline cache miss or corrupt representation")
        if self.provider.settings.skip_remote_checks:
            return FileMetadata(checksum=None, size=0)
        key = self.local_suffix()
        if key not in self.provider._inventory_metadata:
            self.provider._inventory_metadata[key] = await self.provider.get_metadata(
                self.url
            )
        return self.provider._inventory_metadata[key]

    @override
    async def managed_exists(self) -> bool:
        return await self._metadata() is not None

    @override
    async def managed_mtime(self) -> float:
        metadata = await self._metadata()
        return metadata.mtime if metadata is not None else 0

    @override
    async def managed_size(self) -> int:
        metadata = await self._metadata()
        return metadata.size if metadata is not None else 0

    @override
    async def inventory(self, cache: IOCacheStorageInterface) -> None:
        key = self.cache_key()
        if key in cache.exists_in_storage:
            return
        metadata = await self._metadata()
        cache.exists_in_storage[key] = metadata is not None
        cache.mtime[key] = Mtime(storage=metadata.mtime if metadata else 0)
        cache.size[key] = metadata.size if metadata else 0

    @override
    def cleanup(self):
        """Nothing to cleanup"""

    @override
    def exists(self) -> bool:
        raise NotImplementedError()

    @override
    def size(self) -> int:
        raise NotImplementedError()

    @override
    def mtime(self) -> float:
        raise NotImplementedError()

    @override
    def retrieve_object(self) -> None:
        raise NotImplementedError()

    async def verify_checksum(self, path: Path) -> None:
        """
        Verify `path` against checksum provided by zenodo metadata.

        Raises:
            WrongChecksum
        """
        # Get cached or fetch record metadata
        metadata = await self.provider.get_metadata(self.url)
        if metadata is None:
            raise WorkflowError(f"No metadata found for {self.query}")

        checksum = metadata.checksum
        if checksum is None:
            return

        digest, checksum_expected = checksum.split(":", maxsplit=1)

        def compute_hash(path: Path = path, digest: str = digest):
            with open(path, "rb") as f:
                return hashlib.file_digest(f, digest).hexdigest().lower()

        # Hash large scientific inputs without blocking the async event loop.
        checksum_observed = await asyncio.to_thread(compute_hash, path, digest)

        if checksum_expected != checksum_observed:
            raise WrongChecksum(observed=checksum_observed, expected=checksum_expected)

    async def managed_retrieve(self):
        # Keep private partials alive through retries even with cache disabled.
        if self.provider.cache is None and self.provider.settings.policy != "offline":
            from .retrieval import _fetch

            with tempfile.TemporaryDirectory(prefix="cached-http-") as directory:
                identity = RequestIdentity.create(
                    str(self.query), self.provider.settings.headers
                )
                await retry_decorator(_fetch)(
                    self.provider,
                    Cache(Path(directory)),
                    identity,
                    self.local_path(),
                    self.provider.settings.policy,
                )
        else:
            await retry_decorator(self.provider.fetch)(
                str(self.query), self.local_path()
            )
