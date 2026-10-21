# SPDX-License-Identifier: MIT
"""Opt-in file checks for consumers materializing downloaded source documents.

The provider applies no scientific-format policy. Workflows select these checks
explicitly before publishing their own raw outputs.
"""

import json
import zipfile
from pathlib import Path

from .cache import atomic_copy


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


def materialize_validated(source, destination, format=None, *, reject_api_errors=False):
    """Validate and atomically copy; leave an existing destination intact on error."""
    kind = format or Path(destination).suffix.lstrip(".").lower()
    validate_file(source, kind, reject_api_errors=reject_api_errors)
    atomic_copy(Path(source), Path(destination))
