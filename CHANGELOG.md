<!--
SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
SPDX-License-Identifier: CC-BY-4.0
-->

# Changelog

## Unreleased

- `validation.materialize_validated` discards a rejected staged input from the
  persistent cache (only while it is still the current generation), so a body such
  as a maintenance HTML page is downloaded again on the next run instead of being
  reused indefinitely under immutable or not-yet-due checked policy. It takes an
  optional `cache=` root; `Cache.discard(request, digest)` is the underlying API.

## v0.4 — Generic HTTP(S) support and resumable downloads

- Generic HTTP(S) fallback: any `http://` or `https://` URL is now accepted, with size and
  mtime read from `Content-Length` and `Last-Modified` response headers. Servers that do not
  support `HEAD` requests are handled gracefully (size and mtime default to 0). No checksum
  is available for generic URLs. If `snakemake-storage-plugin-http` is installed alongside,
  a `FutureWarning` is emitted and it is patched to refuse all URLs for the session.
  **You should uninstall `snakemake-storage-plugin-http`** after upgrading (shim removed in v0.5).
- Resumable downloads: interrupted transfers are continued from where they left off using
  HTTP `Range` requests (`206 Partial Content`). Servers that do not support range requests
  fall back to a full re-download. Partial files are preserved across retries for
  connection/timeout errors, and discarded on checksum mismatches or other errors.

## v0.3 — Google Cloud Storage support

- Support for `storage.googleapis.com` URLs with checksum verification via the GCS JSON API
  (`md5Hash` field) and mtime from GCS object metadata.

## v0.2.1 — zstd dependency fix

- `zstandard` dependency for decompressing Cloudflare-compressed responses.

## v0.2 — data.pypsa.org support

- Support for `data.pypsa.org` URLs with checksum verification via `manifest.yaml` files
  discovered by searching up the directory tree.
- Redirect support: manifest entries can specify a `redirect` field to point to another path.

## v0.1 — Initial release

- Snakemake storage plugin for Zenodo URLs (`zenodo.org`, `sandbox.zenodo.org`) with:
  - Local filesystem caching via `Cache` class
  - Checksum verification from Zenodo API
  - Adaptive rate limiting using `X-RateLimit-*` headers with exponential backoff retry
  - Concurrent download limiting via semaphore
  - Progress bars with `tqdm-loggable`
