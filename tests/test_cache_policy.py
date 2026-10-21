# SPDX-License-Identifier: MIT
"""Deterministic public-cache and transport regressions; no external services."""

import asyncio
import gzip
import hashlib
import json
import logging
import multiprocessing
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from snakemake_storage_plugin_cached_http import (
    StorageObject,
    StorageProvider,
    StorageProviderSettings,
)
from snakemake_storage_plugin_cached_http.cache import Cache, RequestIdentity

URL = "https://example.test/data?geo=FR&geo=DE&blank="


def provider(tmp_path, **settings):
    return StorageProvider(
        local_prefix=tmp_path / "stage",
        logger=logging.getLogger("test"),
        settings=StorageProviderSettings(
            cache=str(tmp_path / "cache"), public_cache=True, **settings
        ),
    )


def transport(provider, handler):
    provider._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), follow_redirects=False
    )
    # Keep this injected client alive across requests; close explicitly in tests.
    provider._client_refcount = 1


def request(url=URL, **headers):
    return RequestIdentity.create(url, headers)


def test_rate_limit_reset_formats(monkeypatch):
    now = datetime(2026, 10, 21, 23, 59, 50, tzinfo=timezone.utc).timestamp()
    monkeypatch.setattr("snakemake_storage_plugin_cached_http.time.time", lambda: now)
    storage = object.__new__(StorageProvider)
    reset = "2026-10-22 00:00:00+00:00"

    assert storage._get_rate_limit_wait_time(httpx.Headers({
        "X-RateLimit-Remaining": "1", "X-RateLimit-Reset": reset,
    })) is None
    for value in (reset, "2026-10-22T00:00:00Z", str(int(now + 10))):
        assert storage._get_rate_limit_wait_time(httpx.Headers({
            "X-RateLimit-Remaining": "0", "X-RateLimit-Reset": value,
        })) == 11
    assert storage._get_rate_limit_wait_time(httpx.Headers({
        "X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "invalid",
    })) == 0


def test_request_identity():
    a = request()
    assert a != request(URL.replace("FR", "GB"))
    assert a != request(URL.replace("geo=FR&geo=DE", "geo=DE&geo=FR"))
    assert a != request(Accept="application/json")
    assert request("https://EXAMPLE.test:443/data#fragment") == request(
        "https://example.test/data"
    )
    assert request(Accept="application/json") == request(accept="application/json")
    assert "/" not in a.key and "geo" not in a.key
    assert request("http://example.test/data") != request("https://example.test/data")


@pytest.mark.parametrize(
    "url,headers",
    [
        ("https://user:password@example.test/", {}),
        ("https://example.test/?token=secret", {}),
        ("https://example.test/?X-Amz-Signature=secret", {}),
        (URL, {"Authorization": "secret"}),
        (URL, {"Cookie": "secret"}),
        (URL, {"X-Unmodelled": "value"}),
    ],
)
def test_reject_credentials(url, headers):
    with pytest.raises(ValueError):
        RequestIdentity.create(url, headers)


def test_public_assertion(tmp_path):
    with pytest.raises(ValueError, match="public_cache"):
        StorageProvider(
            local_prefix=tmp_path,
            logger=logging.getLogger("test"),
            settings=StorageProviderSettings(),
        )


def test_import_inspect_and_integrity(tmp_path):
    cache = Cache(tmp_path / "cache")
    body = tmp_path / "input"
    body.write_bytes(b"public")
    identity = request()
    receipt = cache.import_file(
        identity,
        body,
        publisher_checksum="sha256:" + hashlib.sha256(b"public").hexdigest(),
    )
    assert receipt["checked_at"] == 0
    assert cache.inspect(identity)["size"] == 6
    stage = tmp_path / "stage"
    cache.materialize(identity, stage)
    stage.write_bytes(b"modified")
    assert cache.get(identity).read_bytes() == b"public"
    cache.get(identity).write_bytes(b"broken")
    assert cache.inspect(identity) is None
    with pytest.raises(FileNotFoundError):
        cache.materialize(identity, stage)


@pytest.mark.parametrize(
    "broken", ["not JSON", "{}", '{"schema":1,"sha256":"../../private"}', "[]", "null"]
)
def test_bad_receipt(tmp_path, broken):
    cache = Cache(tmp_path)
    directory = cache._get_cache_path(request())
    directory.mkdir(parents=True)
    (directory / "receipt.json").write_text(broken)
    assert cache.get(request()) is None


def test_failed_publish_preserves_previous(tmp_path, monkeypatch):
    cache = Cache(tmp_path / "cache")
    body = tmp_path / "body"
    body.write_bytes(b"previous")
    old = cache.import_file(request(), body)
    body.write_bytes(b"next")

    def interrupt(*args):
        raise OSError("crash before receipt switch")

    monkeypatch.setattr(cache, "_write_receipt", interrupt)
    with pytest.raises(OSError):
        cache.import_file(request(), body)
    assert cache.get(request()).read_bytes() == b"previous"
    assert cache.inspect(request()) == old
    assert not list(cache._get_cache_path(request()).glob(".snapshot-*"))


@pytest.mark.asyncio
async def test_policy_counts_304_and_refresh(tmp_path):
    p = provider(tmp_path, policy="checked", check_interval=3600)
    calls = []

    def handler(req):
        calls.append(req)
        if req.headers.get("if-none-match") == '"v1"':
            return httpx.Response(304, headers={"etag": '"v1"'})
        return httpx.Response(
            200,
            content=b"one",
            headers={"etag": '"v1"', "content-type": "application/json"},
        )

    transport(p, handler)
    first = await p.fetch(URL, tmp_path / "one")
    second = await p.fetch(URL, tmp_path / "two")
    assert first == second and len(calls) == 1
    refreshed = await p.fetch(URL, tmp_path / "three", policy="refresh")
    assert refreshed["sha256"] == first["sha256"]
    assert refreshed["mtime"] == first["mtime"]
    assert refreshed["checked_at"] >= first["checked_at"] and len(calls) == 2
    assert (tmp_path / "three").read_bytes() == b"one"
    await p._client.aclose()


@pytest.mark.asyncio
async def test_offline_import_inventory_and_corrupt(tmp_path):
    p = provider(tmp_path, policy="offline")
    body = tmp_path / "body"
    body.write_bytes(b"seed")
    p.cache.import_file(request(), body)

    async def forbidden(*args, **kwargs):
        raise AssertionError("offline network request")

    p.httpr = forbidden
    obj = StorageObject(query=URL, keep_local=False, retrieve=True, provider=p)
    assert await obj.managed_exists()
    assert await obj.managed_size() == 4
    assert await obj.managed_mtime() == 0
    await obj.managed_retrieve()
    assert obj.local_path().read_bytes() == b"seed"
    p.cache.get(request()).write_bytes(b"corrupt")
    with pytest.raises(FileNotFoundError):
        await p.fetch(URL, tmp_path / "out")
    with pytest.raises(FileNotFoundError):
        await obj.managed_exists()


@pytest.mark.asyncio
async def test_concurrent_single_download(tmp_path):
    p = provider(tmp_path, policy="immutable")
    calls = []

    async def handler(req):
        calls.append(req)
        await asyncio.sleep(0.05)
        return httpx.Response(200, content=b"one")

    transport(p, handler)
    await asyncio.gather(*(p.fetch(URL, tmp_path / str(i)) for i in range(5)))
    assert len(calls) == 1
    assert all((tmp_path / str(i)).read_bytes() == b"one" for i in range(5))
    await p._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        {"vary": "*"},
        {"vary": "Cookie"},
        {"set-cookie": "session=secret"},
        {"cache-control": "private"},
        {"cache-control": "no-store"},
    ],
)
async def test_reject_private_responses(tmp_path, headers):
    p = provider(tmp_path)
    transport(p, lambda req: httpx.Response(200, content=b"private", headers=headers))
    with pytest.raises(ValueError):
        await p.fetch(URL, tmp_path / "out")
    assert p.cache.get(request()) is None
    assert not (tmp_path / "out").exists()
    await p._client.aclose()


@pytest.mark.asyncio
async def test_redirect_and_credential_redirect(tmp_path):
    p = provider(tmp_path)

    def handler(req):
        if req.url.path == "/data":
            return httpx.Response(302, headers={"location": "/final"})
        return httpx.Response(200, content=b"redirect", headers={"vary": "Accept"})

    transport(p, handler)
    receipt = await p.fetch(URL, tmp_path / "out")
    assert receipt["final_url"] == "https://example.test/final"
    assert receipt["identity"]["url"] == URL
    await p._client.aclose()
    transport(
        p,
        lambda req: httpx.Response(
            302, headers={"location": "https://user:password@example.test/final"}
        ),
    )
    with pytest.raises(ValueError, match="Credential"):
        await p.fetch(URL, tmp_path / "out", policy="refresh")
    assert (tmp_path / "out").read_bytes() == b"redirect"
    await p._client.aclose()


@pytest.mark.asyncio
async def test_no_stale_on_http_failure(tmp_path):
    p = provider(tmp_path)
    transport(p, lambda req: httpx.Response(200, content=b"good"))
    await p.fetch(URL, tmp_path / "out")
    await p._client.aclose()
    transport(p, lambda req: httpx.Response(500))
    with pytest.raises(httpx.HTTPStatusError):
        await p.fetch(URL, tmp_path / "out", policy="refresh")
    assert p.cache.get(request()).read_bytes() == b"good"
    assert (tmp_path / "out").read_bytes() == b"good"
    await p._client.aclose()


@pytest.mark.asyncio
async def test_compressed_length_and_short_body(tmp_path):
    p = provider(tmp_path)
    encoded = gzip.compress(b"decoded" * 500)
    transport(
        p,
        lambda req: httpx.Response(
            200,
            content=encoded,
            headers={"content-encoding": "gzip", "content-length": str(len(encoded))},
        ),
    )
    await p.fetch(URL, tmp_path / "out")
    assert (tmp_path / "out").read_bytes() == b"decoded" * 500
    await p._client.aclose()
    transport(
        p,
        lambda req: httpx.Response(
            200, content=b"short", headers={"content-length": "999"}
        ),
    )
    with pytest.raises(httpx.RemoteProtocolError):
        await p.fetch(URL, tmp_path / "out", policy="refresh")
    assert (tmp_path / "out").read_bytes() == b"decoded" * 500
    await p._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [403, 405, 501])
async def test_head_fallback_never_downloads(tmp_path, status):
    p = provider(tmp_path)
    calls = []

    def handler(req):
        calls.append(req.method)
        return httpx.Response(status)

    transport(p, handler)
    metadata = await p.get_http_metadata(
        __import__("urllib.parse", fromlist=["urlparse"]).urlparse(URL)
    )
    assert metadata.size == 0
    assert calls == ["HEAD"]
    await p._client.aclose()


class Interrupted(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b"first"
        raise httpx.ReadError("interrupted")


@pytest.mark.asyncio
async def test_validator_bound_resume(tmp_path):
    p = provider(tmp_path)
    calls = []

    def handler(req):
        calls.append(req)
        if len(calls) == 1:
            return httpx.Response(
                200,
                stream=Interrupted(),
                headers={"etag": '"v1"', "content-length": "11"},
            )
        assert req.headers["range"] == "bytes=5-"
        assert req.headers["if-range"] == '"v1"'
        return httpx.Response(
            206,
            content=b"second",
            headers={"etag": '"v1"', "content-range": "bytes 5-10/11"},
        )

    transport(p, handler)
    with pytest.raises(httpx.ReadError):
        await p.fetch(URL, tmp_path / "out")
    await p.fetch(URL, tmp_path / "out")
    assert (tmp_path / "out").read_bytes() == b"firstsecond"
    await p._client.aclose()


def _process_import(directory, body, queue):
    cache = Cache(Path(directory))
    with cache.sync_lock(request()):
        if cache.get(request()) is None:
            time.sleep(0.05)
            cache.publish_locked(request(), Path(body))
            queue.put("published")
        else:
            queue.put("hit")


def test_independent_process_lock(tmp_path):
    body = tmp_path / "body"
    body.write_bytes(b"shared")
    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    processes = [
        ctx.Process(
            target=_process_import, args=(str(tmp_path / "cache"), str(body), queue)
        )
        for _ in range(3)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=15)
        assert process.exitcode == 0
    assert sorted(queue.get(timeout=2) for _ in processes) == [
        "hit",
        "hit",
        "published",
    ]


@pytest.mark.asyncio
async def test_due_entry_without_validators_downloads_and_keeps_identical_mtime(
    tmp_path,
):
    p = provider(tmp_path, policy="checked", check_interval=0)
    calls = []

    def handler(req):
        calls.append(req)
        assert "if-none-match" not in req.headers
        return httpx.Response(200, content=b"same")

    transport(p, handler)
    first = await p.fetch(URL, tmp_path / "one")
    second = await p.fetch(URL, tmp_path / "two")
    assert len(calls) == 2
    assert first["mtime"] == second["mtime"]
    await p._client.aclose()


@pytest.mark.asyncio
async def test_last_modified_conditional_get(tmp_path):
    p = provider(tmp_path, policy="checked", check_interval=0)
    modified = "Wed, 01 Jan 2020 00:00:00 GMT"

    def handler(req):
        if req.headers.get("if-modified-since") == modified:
            return httpx.Response(304)
        return httpx.Response(200, content=b"body", headers={"last-modified": modified})

    transport(p, handler)
    first = await p.fetch(URL, tmp_path / "out")
    second = await p.fetch(URL, tmp_path / "out")
    assert first["mtime"] == second["mtime"] == 1577836800
    await p._client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "range_header,etag",
    [("bytes 4-10/11", '"v1"'), ("5-10/11", '"v1"'), ("bytes 5-10/11", '"changed"')],
)
async def test_unsafe_resume_is_rejected(tmp_path, range_header, etag):
    p = provider(tmp_path)
    directory = p.cache._get_cache_path(request())
    directory.mkdir(parents=True)
    (directory / "partial").write_bytes(b"first")
    (directory / "partial.json").write_text(
        json.dumps({"validator": '"v1"', "target": URL, "final_url": URL})
    )
    transport(
        p,
        lambda req: httpx.Response(
            206,
            content=b"second",
            headers={"etag": etag, "content-range": range_header},
        ),
    )
    with pytest.raises(ValueError):
        await p.fetch(URL, tmp_path / "out")
    assert p.cache.get(request()) is None
    assert not (directory / "partial").exists()
    await p._client.aclose()


@pytest.mark.asyncio
async def test_ignored_range_restarts(tmp_path):
    p = provider(tmp_path)
    directory = p.cache._get_cache_path(request())
    directory.mkdir(parents=True)
    (directory / "partial").write_bytes(b"old")
    (directory / "partial.json").write_text(
        json.dumps({"validator": '"old"', "target": URL, "final_url": URL})
    )
    transport(
        p,
        lambda req: httpx.Response(
            200, content=b"replacement", headers={"etag": '"new"'}
        ),
    )
    await p.fetch(URL, tmp_path / "out")
    assert (tmp_path / "out").read_bytes() == b"replacement"
    await p._client.aclose()


@pytest.mark.asyncio
async def test_cookie_opt_in_never_sends_cookies(tmp_path):
    p = provider(tmp_path, discard_response_cookies=True)
    calls = []

    def handler(req):
        calls.append(req)
        assert "cookie" not in req.headers
        if req.url.path == "/data":
            return httpx.Response(
                302, headers={"location": "/final", "set-cookie": "session=secret"}
            )
        return httpx.Response(
            200, content=b"public", headers={"set-cookie": "session=other"}
        )

    transport(p, handler)
    await p.fetch(URL, tmp_path / "out")
    receipt = await p.fetch(URL, tmp_path / "out", policy="refresh")
    assert len(calls) == 4
    assert "session" not in json.dumps(receipt)
    assert not p._client.cookies
    await p._client.aclose()


@pytest.mark.asyncio
async def test_cancelled_publication_holds_lock_until_io_finishes(
    tmp_path, monkeypatch
):
    import threading

    p = provider(tmp_path, policy="immutable")
    transport(p, lambda req: httpx.Response(200, content=b"public"))
    entered = threading.Event()
    finish = threading.Event()
    original = p.cache.publish_locked

    def slow_publish(*args, **kwargs):
        entered.set()
        assert finish.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(p.cache, "publish_locked", slow_publish)
    first = asyncio.create_task(p.fetch(URL, tmp_path / "one"))
    assert await asyncio.to_thread(entered.wait, 5)
    first.cancel()
    second = asyncio.create_task(p.fetch(URL, tmp_path / "two"))
    await asyncio.sleep(0.05)
    first.cancel()  # repeated cancellation must not release an in-flight writer
    await asyncio.sleep(0.05)
    assert not first.done() and not second.done()
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await first
    await second
    assert (tmp_path / "two").read_bytes() == b"public"
    await p._client.aclose()


@pytest.mark.asyncio
async def test_304_checks_new_publisher_checksum(tmp_path):
    from snakemake_storage_plugin_cached_http import FileMetadata, WrongChecksum

    url = "https://storage.googleapis.com/public/data"
    p = provider(tmp_path, policy="checked", check_interval=0)
    calls = []

    async def metadata(parsed):
        expected = hashlib.sha256(b"public" if not calls else b"changed").hexdigest()
        return FileMetadata(checksum="sha256:" + expected, size=6)

    p.get_metadata = metadata

    def handler(req):
        calls.append(req)
        if len(calls) > 1:
            return httpx.Response(304)
        return httpx.Response(200, content=b"public", headers={"etag": '"v1"'})

    transport(p, handler)
    first = await p.fetch(url, tmp_path / "out")
    with pytest.raises(WrongChecksum):
        await p.fetch(url, tmp_path / "out")
    assert p.cache.inspect(RequestIdentity.create(url)) == first
    assert (tmp_path / "out").read_bytes() == b"public"
    await p._client.aclose()
