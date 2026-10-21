<!--
SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
SPDX-License-Identifier: CC-BY-4.0
-->

# Snakemake Storage Plugin: Cached HTTP

HTTP inputs with persistent verified caching, conditional retrieval, public-source
checksum handlers, rate limiting and safe resumable transfers. Supports Python
3.11–3.13 and Snakemake's storage interface `>=4.2.1,<5`.

```sh
pip install -e '.[dev]'
pytest -q
```

## Explicit provider registration

```python
storage cached_http:
    provider="cached-http",
    cache="~/.cache/snakemake-pypsa-eur",
    public_cache=True,
    policy="checked",
    check_interval=3600,
    headers={"Accept": "application/json"},

rule download:
    input: storage.cached_http("https://example.org/data?geo=FR")
    output: "data.json"
    shell: "cp {input} {output}"
```

`public_cache=True` asserts that the configured response representations are public
and suitable for persistent reuse. It is required when persistent caching is on.
Absence of credentials alone does not establish that. Authenticated retrieval and
browser fallback are outside this plugin's public-cache contract.

Both `http` and `cached-http` providers may be installed. Use tagged registration
and `storage.cached_http(...)` / `storage.http(...)` to choose explicitly; generic
`storage(url)` can be ambiguous. Importing this plugin never modifies the other
provider.

## Settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `cache` | platform user cache for `snakemake-pypsa-eur` | Persistent cache root; `""` disables persistence |
| `public_cache` | `False` | Explicit public-resource assertion required for persistent caching |
| `policy` | `"auto"` | `auto`, `checked`, `immutable`, `offline`, or `refresh` |
| `check_interval` | `3600` | Seconds before a checked entry needs conditional retrieval |
| `headers` | `{}` | Representation headers; configure in the Snakefile/Python API |
| `discard_response_cookies` | `False` | Explicitly permit public responses that set cookies; discard every cookie |
| `skip_remote_checks` | `False` | Suppress inventory metadata probes; retrieval may still use the network |
| `max_concurrent_downloads` | `3` | Maximum HTTP connections per provider instance |

Scalar settings except concurrency support plugin environment variables, e.g.
`SNAKEMAKE_STORAGE_CACHED_HTTP_CACHE` and
`SNAKEMAKE_STORAGE_CACHED_HTTP_POLICY`. Representation headers are configured in
Python or the Snakefile; they are not a credential channel.

## Freshness and network behavior

| Policy | Verified hit | Miss/corruption |
| --- | --- | --- |
| `auto` | Reuse recognized versioned source URLs indefinitely; otherwise checked policy | Download and verify |
| `checked` | Reuse within interval, then conditional GET using ETag/Last-Modified | Download and verify |
| `immutable` | Reuse without network | Download and verify |
| `offline` | Reuse without any HTTP, including metadata APIs | Fail without network |
| `refresh` | Conditional GET on each retrieval; no stale-on-error fallback | Download and verify |

A 304 keeps the body and storage mtime; only the check time advances. Without
validators, a due retrieval downloads the complete body; identical content keeps
its storage mtime. Failures preserve previous cached bytes and staging outputs,
but are reported rather than silently returning stale data.

Inventory, dry runs and DAG rendering may perform **HEAD or source metadata API**
requests when an entry is missing or due. They never fetch a file body just to
learn size/existence. HEAD 403/405/501 yields unknown size/mtime; retrieval resolves
that uncertainty. A generic server with neither validators nor Last-Modified
cannot reliably announce changes during inventory. Use explicit refresh/forced
retrieval when change detection is required for such a source. `refresh` controls
retrieval; it does not itself force Snakemake to schedule an up-to-date rule.

Known handlers retain publisher checksums: Zenodo API, PyPSA manifests and GCS
metadata. `auto` recognizes Zenodo record-file URLs and PyPSA workflow-file paths;
use explicit `checked` if a publisher changes those resources in place. Headers
or query parameters disable this automatic immutability classification.

Zenodo can set a session cookie even on public files. After establishing that the
selected files are public, opt into `discard_response_cookies=True`. No received
cookie is ever sent on a later request, redirect or retry. Private/no-store body
responses, authentication challenges and unsupported `Vary` dimensions still fail.
Source metadata responses are transient and may have private/no-store controls;
they are never published as cached file bodies.

## Durable cache and import

Schema v1 stores hashed request identities, immutable SHA-256 body generations and
an atomically replaced receipt under `<cache>/v1/<identity>/`. Identity includes
the full ordered query and effective representation headers. XML and JSON or FR
and DE queries cannot collide; secrets and query strings never appear in filenames.
Receipts include public URLs and permitted headers, so the cache directory itself
must have appropriate filesystem access controls.

Every cache hit checks size, SHA-256 and any publisher checksum. Local integrity
hashes do not establish publisher authenticity. Corrupt entries are misses online
and errors offline. Per-identity POSIX process locks deduplicate writers, and
atomic fsynced publication preserves previous generations on interrupted refresh.
Staging uses copies, never writable hardlinks. There is no automatic eviction.

Old URL-shaped cache entries are not read implicitly. Import approved public bytes
using the supported offline API:

```python
from pathlib import Path
from snakemake_storage_plugin_cached_http.cache import Cache, RequestIdentity

cache = Cache(Path("~/.cache/snakemake-pypsa-eur"))
request = RequestIdentity.create(
    "https://example.org/data?geo=FR", {"Accept": "application/json"}
)
receipt = cache.import_file(request, Path("existing.json"))
assert receipt["checked_at"] == 0  # import is not a remote freshness check
print(cache.inspect(request))     # verifies bytes; never makes a request
cache.materialize(request, Path("copy.json"))
```

Import accepts optional `publisher_checksum="sha256:..."` (or a supported hashlib
algorithm) and metadata keys `etag`, `last_modified`, `mtime`, `content_type`,
`content_disposition`, `final_url`. Unknown validators stay unknown. Importing a
legacy downloader's receipt means translating those fields once, explicitly; the
plugin does not maintain that receipt format.

## Dynamic asynchronous client API

Prefer declared storage inputs when requests are known. A client discovering URLs
at runtime may use the same implementation through `StorageProvider.fetch`:

```python
import asyncio
import logging
from pathlib import Path
from snakemake_storage_plugin_cached_http import StorageProvider, StorageProviderSettings

provider = StorageProvider(
    local_prefix=Path(".stage"), logger=logging.getLogger("downloads"),
    settings=StorageProviderSettings(
        cache=".response-cache", public_cache=True, policy="checked",
        headers={"Accept": "application/xml"},
    ),
)
receipt = asyncio.run(provider.fetch("https://example.org/structure?id=1", Path("structure.xml")))
print(receipt["content_type"], receipt["final_url"])
```

The caller owns the destination and its lifetime; internal cache body paths are
not part of this retrieval API. The returned verified receipt includes digest,
size, timestamps, validators and selected response metadata. `policy="refresh"`
overrides policy for one call. `fetch` performs one attempt and reports HTTPX
transport/status errors, validation/checksum errors or offline `FileNotFoundError`.
Storage retrieval wraps this API in the existing five-attempt exponential retry
and rate-limit handling. Synchronous clients can call `asyncio.run` outside an
existing event loop; asynchronous clients must await the call. Scientific parsing
and validation belong to the caller.

See [cache guarantees and operational limits](docs/cache.md) and the
[implementation plan](docs/plans/plan-cached-http-improvements.org).

## License

MIT License
