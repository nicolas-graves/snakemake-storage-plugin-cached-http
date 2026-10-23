# SPDX-License-Identifier: MIT
"""Opt-in file checks for consumers materializing downloaded source documents.

The provider applies no scientific-format policy. Workflows select these checks
explicitly before publishing their own raw outputs.
"""

import json
import os
import zipfile
from pathlib import Path

import platformdirs

from .cache import Cache, RequestIdentity, atomic_copy, checksum


def validate_file(path, format=None, *, reject_api_errors=False):
    path = Path(path)
    kind = format or path.suffix.lstrip(".").lower()
    with path.open("rb") as stream:
        prefix = stream.read(1024)
    if not prefix:
        raise ValueError("Empty source response")
    if prefix.lstrip().lower().startswith((b"<!doctype html", b"<html")):
        raise ValueError("HTML response where source data was expected")
    signatures = {
        "zip": b"PK",
        "xlsx": b"PK",
        "xls": b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1",
        "pdf": b"%PDF",
        "parquet": b"PAR1",
    }
    if kind in signatures and not prefix.startswith(signatures[kind]):
        raise ValueError(f"Invalid {kind} source signature")
    if kind in {"zip", "xlsx"}:
        with zipfile.ZipFile(path) as archive:
            if archive.testzip() is not None:
                raise ValueError(f"Corrupt {kind} source archive")
    if kind == "json":
        document = json.loads(path.read_bytes())
        if reject_api_errors and isinstance(document, dict) and "error" in document:
            raise ValueError(f"Source API error: {document['error']}")


def discard_staged(source, cache=None):
    """Drop the cache generation a staged storage input still is; True if dropped.

    Staged inputs are named by request identity key. Anything else, or a cache
    entry since refreshed to other bytes, is left alone. ``cache`` defaults to
    the provider's default root.
    """
    if cache is None:
        cache = os.environ.get(
            "SNAKEMAKE_STORAGE_CACHED_HTTP_CACHE",
            platformdirs.user_cache_dir("snakemake-pypsa-eur", ensure_exists=False),
        )
    if not cache:
        return False
    source = Path(source)
    # Look before Cache/lock: both create directories.
    receipt = Path(cache).expanduser() / "v1" / source.name / "receipt.json"
    try:
        document = json.loads(receipt.read_text())["identity"]
        identity = RequestIdentity(
            document["url"], tuple(sorted(document["headers"].items()))
        )
        if identity.key != source.name:
            return False
        return Cache(Path(cache)).discard(identity, checksum(source))
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return False


def materialize_validated(
    source, destination, format=None, *, reject_api_errors=False, cache=None
):
    """Validate and atomically copy; leave an existing destination intact on error.

    A rejected storage input is also discarded from the persistent cache it was
    staged from (``cache``, see ``discard_staged``) so it is not reused.
    """
    kind = format or Path(destination).suffix.lstrip(".").lower()
    try:
        validate_file(source, kind, reject_api_errors=reject_api_errors)
    except (ValueError, zipfile.BadZipFile):
        discard_staged(source, cache)
        raise
    atomic_copy(Path(source), Path(destination))
