# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
# SPDX-License-Identifier: MIT
"""Versioned public HTTP identities and verified, crash-safe POSIX cache."""

import asyncio
import fcntl
import hashlib
import json
import math
import os
import shutil
import tempfile
import time
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit, urlunsplit

import httpx

SCHEMA = 1
REPRESENTATION_HEADERS = {"accept", "accept-language", "accept-encoding", "user-agent"}
DEFAULT_HEADERS = {
    "accept": "*/*",
    "accept-encoding": "identity",
    "user-agent": "snakemake-cached-http",
}
SENSITIVE_QUERY = {
    "token",
    "access_token",
    "api_key",
    "apikey",
    "key",
    "signature",
    "sig",
    "password",
    "auth",
    "authorization",
}


@dataclass(frozen=True)
class RequestIdentity:
    """Anonymous GET representation; raw credentials and signed URLs are unsupported."""

    url: str
    headers: tuple[tuple[str, str], ...]

    @classmethod
    def create(cls, url: str, headers: dict[str, str] | None = None):
        parsed = urlsplit(url)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
            raise ValueError("A valid HTTP(S) URL is required")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("Credential-bearing URLs are unsupported")
        for key, _ in parse_qsl(parsed.query, keep_blank_values=True):
            lower = key.lower()
            if lower in SENSITIVE_QUERY or lower.startswith(("x-amz-", "x-goog-")):
                raise ValueError(
                    "Credential-bearing or signed query parameters are unsupported"
                )
        normalized = dict(DEFAULT_HEADERS)
        for name, value in (headers or {}).items():
            name = name.lower().strip()
            if name not in REPRESENTATION_HEADERS:
                raise ValueError(f"Unsupported request header: {name}")
            normalized[name] = value.strip()
        # HTTPX canonicalizes the actual wire URL (case/default ports/escaping).
        canonical = str(
            httpx.URL(
                urlunsplit(
                    (parsed.scheme, parsed.netloc, parsed.path or "/", parsed.query, "")
                )
            )
        )
        return cls(canonical, tuple(sorted(normalized.items())))

    @property
    def document(self):
        return {
            "schema": SCHEMA,
            "method": "GET",
            "url": self.url,
            "headers": dict(self.headers),
        }

    @property
    def key(self):
        return hashlib.sha256(
            json.dumps(self.document, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


def checksum(path: Path, algorithm="sha256"):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, algorithm).hexdigest()


def verify_publisher(path: Path, expected: str | None):
    if expected is not None:
        algorithm, value = expected.split(":", 1)
        if checksum(path, algorithm).lower() != value.lower():
            raise ValueError("Publisher checksum mismatch")


def atomic_copy(source: Path, destination: Path):
    """Copy without hardlinks, publish atomically and sync the directory."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".partial-", dir=destination.parent)
    try:
        with os.fdopen(fd, "wb") as output, source.open("rb") as stream:
            shutil.copyfileobj(stream, output)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)
        sync_directory(destination.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def durable_directory(path):
    """Make new cache directory entries durable as well as their contents."""
    missing = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    path.mkdir(parents=True, exist_ok=True)
    for directory in reversed(missing):
        sync_directory(directory)
        sync_directory(directory.parent)


@dataclass
class Cache:
    """Local/shared POSIX filesystem cache. Never hand writable bodies to consumers.

    Mutation methods ending in ``_locked`` require a held identity lock. Public
    import takes its own lock. Async clients use ``lock`` so contention cannot
    block the event loop; no executor-held lock can leak on cancellation.
    """

    cache_dir: Path

    def __post_init__(self):
        self.cache_dir = Path(self.cache_dir).expanduser() / "v1"
        durable_directory(self.cache_dir)

    def identity(self, request):
        return (
            request
            if isinstance(request, RequestIdentity)
            else RequestIdentity.create(request)
        )

    def _get_cache_path(self, request):
        return self.cache_dir / self.identity(request).key

    @contextmanager
    def sync_lock(self, request):
        directory = self._get_cache_path(request)
        durable_directory(directory)
        with (directory / "lock").open("ab") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)

    @asynccontextmanager
    async def lock(self, request):
        directory = self._get_cache_path(request)
        durable_directory(directory)
        with (directory / "lock").open("ab") as stream:
            while True:
                try:
                    fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    await asyncio.sleep(0.05)
            try:
                yield
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)

    def inspect(self, request):
        """Return a verified receipt, or None on missing/corrupt data; no network."""
        identity = self.identity(request)
        directory = self._get_cache_path(identity)
        try:
            receipt = json.loads((directory / "receipt.json").read_text())
            if (
                type(receipt["schema"]) is not int
                or receipt["schema"] != SCHEMA
                or receipt["identity"] != identity.document
            ):
                return None
            digest = receipt["sha256"]
            if (
                not isinstance(digest, str)
                or len(digest) != 64
                or any(c not in "0123456789abcdef" for c in digest)
            ):
                return None
            body = directory / digest
            if (
                type(receipt["size"]) is not int
                or body.stat().st_size != receipt["size"]
                or checksum(body) != digest
            ):
                return None
            for name in ("retrieved_at", "checked_at", "mtime"):
                if (
                    type(receipt[name]) not in (float, int)
                    or not math.isfinite(receipt[name])
                    or (name != "mtime" and receipt[name] < 0)
                ):
                    return None
            for name in (
                "etag",
                "last_modified",
                "content_type",
                "content_disposition",
                "final_url",
                "publisher_checksum",
            ):
                if receipt.get(name) is not None and not isinstance(receipt[name], str):
                    return None
            verify_publisher(body, receipt.get("publisher_checksum"))
            return receipt
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            return None

    def get(self, request):
        receipt = self.inspect(request)
        return self._get_cache_path(request) / receipt["sha256"] if receipt else None

    def _write_receipt(self, request, receipt):
        directory = self._get_cache_path(request)
        fd, temporary = tempfile.mkstemp(prefix=".receipt-", dir=directory)
        try:
            with os.fdopen(fd, "w") as stream:
                json.dump(receipt, stream, sort_keys=True)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, directory / "receipt.json")
            sync_directory(directory)
        finally:
            Path(temporary).unlink(missing_ok=True)

    def publish_locked(
        self,
        request,
        filepath,
        *,
        metadata=None,
        publisher_checksum=None,
        remotely_checked=True,
    ):
        identity = self.identity(request)
        filepath = Path(filepath)
        verify_publisher(filepath, publisher_checksum)
        directory = self._get_cache_path(identity)
        durable_directory(directory)
        # Copy first: never publish a digest computed before a source mutates.
        fd, snapshot_name = tempfile.mkstemp(prefix=".snapshot-", dir=directory)
        os.close(fd)
        snapshot = Path(snapshot_name)
        try:
            atomic_copy(filepath, snapshot)
            verify_publisher(snapshot, publisher_checksum)
            digest = checksum(snapshot)
            os.replace(snapshot, directory / digest)
            sync_directory(directory)
            now = time.time()
            allowed = {
                k: v
                for k, v in (metadata or {}).items()
                if k
                in {
                    "etag",
                    "last_modified",
                    "content_type",
                    "content_disposition",
                    "final_url",
                    "mtime",
                }
            }
            # final URL must meet the same public request constraints.
            if allowed.get("final_url"):
                allowed["final_url"] = RequestIdentity.create(allowed["final_url"]).url
            receipt = {
                "schema": SCHEMA,
                "identity": identity.document,
                "sha256": digest,
                "size": (directory / digest).stat().st_size,
                "retrieved_at": now,
                "checked_at": now if remotely_checked else 0,
                "mtime": 0,
                "publisher_checksum": publisher_checksum,
                **allowed,
            }
            self._write_receipt(identity, receipt)
            return receipt
        finally:
            snapshot.unlink(missing_ok=True)

    def checked_locked(self, request, receipt):
        updated = {**receipt, "checked_at": time.time()}
        self._write_receipt(request, updated)
        return updated

    def import_file(self, request, filepath, *, metadata=None, publisher_checksum=None):
        """Import public bytes offline; validators are unknown unless supplied.

        The caller asserts these bytes are public and match this exact identity.
        Import is not proof of remote freshness (checked_at stays zero).
        """
        with self.sync_lock(request):
            return self.publish_locked(
                request,
                filepath,
                metadata=metadata,
                publisher_checksum=publisher_checksum,
                remotely_checked=False,
            )

    def put(self, request, filepath):
        """Import an existing public file; prefer import_file for provenance."""
        return self.import_file(request, filepath)

    def materialize(self, request, destination):
        source = self.get(request)
        if source is None:
            raise FileNotFoundError("No verified cached representation")
        atomic_copy(source, Path(destination))
