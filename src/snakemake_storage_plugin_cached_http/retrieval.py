# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
# SPDX-License-Identifier: MIT
"""One retrieval path for storage objects and dynamic asynchronous clients."""

import asyncio
import json
import os
import tempfile
import time
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlparse

import httpx
from tqdm_loggable.auto import tqdm

from .cache import (
    REPRESENTATION_HEADERS,
    Cache,
    RequestIdentity,
    verify_publisher,
)


async def io(function, *args, **kwargs):
    """Finish filesystem work before releasing a lock, even on cancellation."""
    operation = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(operation)
            break
        except asyncio.CancelledError:
            cancelled = True
    if cancelled:
        raise asyncio.CancelledError
    return result


def validate_response(response, *, cacheable=True, allow_cookies=False):
    """Fail closed on session-bearing or unsupported varying responses."""
    headers = response.headers
    controls = {
        v.strip().split("=", 1)[0].lower()
        for v in headers.get("cache-control", "").split(",")
    }
    if (
        (headers.get("set-cookie") and not allow_cookies)
        or headers.get("www-authenticate")
        or (cacheable and controls & {"private", "no-store"})
    ):
        raise ValueError("Response is not eligible for the public cache")
    vary = {v.strip().lower() for v in headers.get("vary", "").split(",") if v.strip()}
    if cacheable and vary - REPRESENTATION_HEADERS:
        raise ValueError(
            "Response Vary contains an unsupported representation dimension"
        )
    RequestIdentity.create(str(response.url))


def immutable(provider, url, policy):
    if policy == "immutable":
        return True
    parsed = urlparse(url)
    if policy != "auto" or parsed.query or provider.settings.headers:
        return False
    # Only versioned source-file shapes; a host's landing page isn't immutable.
    return (
        parsed.netloc in {"zenodo.org", "sandbox.zenodo.org"}
        and len(parsed.path.strip("/").split("/")) >= 4
        and parsed.path.startswith("/records/")
        and "/files/" in parsed.path
    ) or (parsed.netloc == "data.pypsa.org" and parsed.path.startswith("/workflows/"))


def reusable(provider, url, receipt, policy):
    return bool(
        receipt
        and policy != "refresh"
        and (
            policy == "offline"
            or immutable(provider, url, policy)
            or time.time() - receipt["checked_at"] < provider.settings.check_interval
        )
    )


async def cached_metadata(provider, url):
    from . import FileMetadata

    if not provider.cache:
        return None
    identity = RequestIdentity.create(url, provider.settings.headers)
    receipt = await io(provider.cache.inspect, identity)
    if not reusable(provider, url, receipt, provider.settings.policy):
        return None
    return FileMetadata(
        checksum=receipt.get("publisher_checksum"),
        size=receipt["size"],
        mtime=receipt["mtime"],
        etag=receipt.get("etag"),
        last_modified=receipt.get("last_modified"),
        content_type=receipt.get("content_type"),
        content_disposition=receipt.get("content_disposition"),
        final_url=receipt.get("final_url"),
    )


async def verify_source_checksum(path, expected):
    try:
        await io(verify_publisher, path, expected)
    except ValueError:
        from . import WrongChecksum
        from .cache import checksum

        algorithm, digest = expected.split(":", 1)
        raise WrongChecksum(await io(checksum, path, algorithm), digest) from None


def response_metadata(response):
    headers = response.headers
    modified = headers.get("last-modified")
    try:
        mtime = parsedate_to_datetime(modified).timestamp() if modified else 0
    except (TypeError, ValueError, OverflowError):
        mtime = 0
    return {
        "etag": headers.get("etag"),
        "last_modified": modified,
        "content_type": headers.get("content-type"),
        "content_disposition": headers.get("content-disposition"),
        "final_url": str(response.url),
        "mtime": mtime,
    }


async def fetch(provider, url, destination, *, policy=None):
    policy = policy or provider.settings.policy
    if policy not in {"auto", "checked", "immutable", "offline", "refresh"}:
        raise ValueError("Invalid cache policy")
    identity = RequestIdentity.create(url, provider.settings.headers)
    if provider.cache is None:
        if policy == "offline":
            raise FileNotFoundError("Offline retrieval needs a verified cache entry")
        # Same implementation; temporary cache is deleted after materialization.
        with tempfile.TemporaryDirectory(prefix="cached-http-") as directory:
            return await _fetch(
                provider, Cache(Path(directory)), identity, destination, policy
            )
    return await _fetch(provider, provider.cache, identity, destination, policy)


async def _fetch(provider, cache, identity, destination, policy):
    async with cache.lock(identity):
        receipt = await io(cache.inspect, identity)
        if reusable(provider, identity.url, receipt, policy):
            await io(cache.materialize, identity, destination)
            return receipt
        if policy == "offline":
            raise FileNotFoundError("Offline cache miss or corrupt representation")

        # Preserve source-specific publisher checksums and redirections. Generic
        # HTTP needs no speculative HEAD before a body/conditional GET.
        parsed = urlparse(identity.url)
        source_metadata = None
        if (
            not provider.settings.headers
            and not parsed.query
            and parsed.netloc
            in {
                "zenodo.org",
                "sandbox.zenodo.org",
                "data.pypsa.org",
                "storage.googleapis.com",
            }
        ):
            source_metadata = await provider.get_metadata(parsed)
            if source_metadata is None:
                raise FileNotFoundError("Source metadata not found")
        target = identity.url
        if source_metadata and source_metadata.redirect:
            target = parsed._replace(path=f"/{source_metadata.redirect}").geturl()
        expected = source_metadata.checksum if source_metadata else None
        directory = cache._get_cache_path(identity)
        partial = directory / "partial"
        progress = directory / "partial.json"
        headers = {}
        if receipt:
            if receipt.get("etag"):
                headers["If-None-Match"] = receipt["etag"]
            elif receipt.get("last_modified"):
                headers["If-Modified-Since"] = receipt["last_modified"]
        offset = 0
        validator = None
        resume_final_url = None
        # Resume only raw identity-encoded bytes bound to a strong validator.
        if not receipt and partial.exists() and progress.exists():
            try:
                record = json.loads(progress.read_text())
                validator = record["validator"]
                resume_final_url = RequestIdentity.create(record["final_url"]).url
                if (
                    not isinstance(validator, str)
                    or not validator
                    or validator.startswith("W/")
                    or record["target"] != target
                ):
                    raise ValueError("Invalid resume validator")
                offset = partial.stat().st_size
                if offset > 0:
                    headers.update({"Range": f"bytes={offset}-", "If-Range": validator})
            except (ValueError, KeyError, TypeError, OSError):
                offset = 0
                validator = None
        try:
            async with provider.httpr("get", target, headers=headers) as response:
                validate_response(
                    response, allow_cookies=provider.settings.discard_response_cookies
                )
                if response.status_code == 304:
                    if receipt is None or not (
                        headers.get("If-None-Match") or headers.get("If-Modified-Since")
                    ):
                        raise ValueError(
                            "Unexpected HTTP 304 without a verified conditional entry"
                        )
                    # Reconcile any newly available publisher checksum even if
                    # HTTP validators say the representation is unchanged.
                    await verify_source_checksum(
                        directory / receipt["sha256"], expected
                    )
                    if expected:
                        receipt = {**receipt, "publisher_checksum": expected}
                    # Never rewrite generation or mtime merely because checked.
                    receipt = await io(cache.checked_locked, identity, receipt)
                    await io(cache.materialize, identity, destination)
                    return receipt
                response.raise_for_status()
                if response.status_code not in (200, 206):
                    raise ValueError("Unexpected HTTP body response")
                encoding = response.headers.get("content-encoding", "identity").lower()
                if response.status_code == 206:
                    content_range = response.headers.get("content-range", "")
                    try:
                        if not content_range.startswith("bytes "):
                            raise ValueError("Missing range unit")
                        span, total = content_range.removeprefix("bytes ").split("/")
                        start, end = map(int, span.split("-"))
                        total = int(total)
                    except (ValueError, TypeError):
                        raise ValueError("Invalid Content-Range") from None
                    if (
                        offset <= 0
                        or str(response.url) != resume_final_url
                        or start != offset
                        or end < start
                        or end != total - 1
                        or encoding != "identity"
                        or response.headers.get("etag", validator) != validator
                    ):
                        raise ValueError("Unsafe partial response")
                    mode = "ab"
                else:
                    offset = 0
                    mode = "wb"
                etag = response.headers.get("etag")
                modified = response.headers.get("last-modified")
                resume_validator = (
                    etag if etag and not etag.startswith("W/") else modified
                )
                # Clear old resume provenance before truncation; never bind old
                # bytes to a new response validator if interrupted between steps.
                if mode == "wb":
                    progress.unlink(missing_ok=True)
                transferred = 0
                with partial.open(mode) as stream:
                    if encoding == "identity" and resume_validator:
                        progress.write_text(
                            json.dumps(
                                {
                                    "validator": resume_validator,
                                    "target": target,
                                    "final_url": str(response.url),
                                }
                            )
                        )
                    else:
                        progress.unlink(missing_ok=True)
                    # HTTPX validates wire framing. Compressed lengths describe
                    # encoded bytes, not the decoded bytes written here.
                    length = response.headers.get("content-length")
                    total_progress = (
                        int(length) + offset
                        if length and encoding == "identity"
                        else None
                    )
                    with tqdm(
                        total=total_progress,
                        initial=offset,
                        unit="B",
                        unit_scale=True,
                        desc="HTTP download",
                        leave=True,
                    ) as progress_bar:
                        async for chunk in response.aiter_bytes():
                            stream.write(chunk)
                            transferred += len(chunk)
                            progress_bar.update(len(chunk))
                    stream.flush()
                    os.fsync(stream.fileno())
                length = response.headers.get("content-length")
                if (
                    encoding == "identity"
                    and length is not None
                    and transferred != int(length)
                ):
                    raise httpx.RemoteProtocolError("Incomplete HTTP body")
                if response.status_code == 206 and partial.stat().st_size != total:
                    raise ValueError("Incomplete ranged response")
                metadata = response_metadata(response)
            await verify_source_checksum(partial, expected)
            if not metadata["mtime"]:
                # Stable across identical bodies and 304/check timestamps.
                from .cache import checksum

                same = receipt and await io(checksum, partial) == receipt["sha256"]
                metadata["mtime"] = receipt["mtime"] if same else time.time()
            receipt = await io(
                cache.publish_locked,
                identity,
                partial,
                metadata=metadata,
                publisher_checksum=expected,
            )
            await io(cache.materialize, identity, destination)
            partial.unlink(missing_ok=True)
            progress.unlink(missing_ok=True)
            return receipt
        except httpx.TransportError:
            # Only validator-bound partials may survive interruption.
            if not progress.exists():
                partial.unlink(missing_ok=True)
            raise
        except BaseException:
            partial.unlink(missing_ok=True)
            progress.unlink(missing_ok=True)
            raise
