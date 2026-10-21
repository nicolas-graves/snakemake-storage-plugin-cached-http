<!-- SPDX-License-Identifier: CC-BY-4.0 -->
# Cache guarantees and operational limits

## Request and transport boundary

Only anonymous GET representations are supported. Effective defaults are
`Accept: */*`, `Accept-Encoding: identity`, and `User-Agent: snakemake-cached-http`.
Accept, Accept-Language, Accept-Encoding and User-Agent are the only configurable
headers, and all participate in identity. Header names are case-normalized.
HTTPX normalizes wire URLs (scheme/host case, default ports, escaping); fragments
are ignored. Query pair ordering, duplicates and blank values are preserved.

URLs with userinfo, known credential query keys, or AWS/Google signed-URL keys
are rejected before requests. There is no universal way to recognize arbitrary
secrets disguised as ordinary query values: the public-resource assertion remains
the caller's responsibility. Credentials, cookies, arbitrary request headers and
ambient proxy/netrc configuration are unsupported. HTTPX runs with trust_env=False;
redirects are inspected individually before following. Original requests remain
distinct even when they redirect to the same final URL.

`Vary` is supported only for the four represented header names. Unknown dimensions
and `Vary: *` fail closed for stored bodies. Cache-Control private/no-store and
authentication challenges fail. Set-Cookie fails unless the caller explicitly
opts into discarding response cookies for resources already established public.
Cookies are stripped from every outgoing request regardless. Public-cache trust
cannot be inferred from successful anonymous access alone.

Receipts intentionally contain public URLs and selected representation headers;
filenames contain hashes only. No arbitrary response headers or cookies are
serialized. Do not use this public cache for private data or signed URLs.

## Publication and locking

Cache v1 layout:

```
v1/<identity SHA-256>/
    lock
    <body SHA-256>         immutable completed generation
    receipt.json          atomic current-generation pointer and integrity metadata
    partial               private resumable transfer, never a cache hit
    partial.json          strong ETag/Last-Modified, original and final target for resume
```

A writer holds flock across cache recheck, retrieval, checksum verification and
publication. Async lock contention yields to the event loop. Hashing and atomic
copies run off the event loop; cancellation waits for that filesystem operation
before releasing the lock. Different keys can progress independently.

Publication copies bytes to a private snapshot, checks publisher digest, hashes
that snapshot, syncs it, renames it to a digest generation, syncs the directory,
then syncs and atomically replaces the receipt and syncs the directory again.
This ordering covers process interruption and requests filesystem durability via
fsync; hardware/filesystem implementations must honor those operations for
power-loss durability. Orphans from interruption are ignored. Existing generations
are retained without eviction; garbage collection is not included.

Use a POSIX filesystem with working cross-process flock, atomic same-filesystem
rename and directory fsync. Windows is unsupported. Network/shared filesystems
must actually provide these guarantees. Lock files remain permanently present;
do not remove them while clients are running. No expired-lock guessing is needed:
the OS releases locks on process exit.

Every successful read verifies receipt shape, size, SHA-256 and available publisher
checksum. Bodies must not be modified directly. Cache directories are trusted
local storage, not a boundary against malicious users with write access.
Materialization makes an independent atomic copy so a job cannot mutate cached
bytes by editing a hardlink. No scientific format assumptions are applied.

## Transfer and resume

A failed refresh leaves both previous receipt and staging output intact. No stale
fallback is implicit. Interrupted raw identity-encoded transfers may resume only
with a strong ETag or Last-Modified bound to their original and observed final
redirect target, using Range and If-Range.
A 206 must have a consistent Content-Range and matching validator; an ignored Range
(200) restarts. Changed/unsafe ranges discard the partial. Compressed transfers
restart; Content-Length is compared with decoded bytes only for identity encoding,
and HTTPX checks wire framing. Storage retries retain private partials even when
persistent cache is disabled. Public fetch calls retain them in persistent cache.

## Freshness versus storage timestamps

`checked_at` determines the check interval, never the Snakemake mtime. Import sets
it to zero. 304 retains mtime and body. A 200 with identical bytes retains the prior
mtime when Last-Modified is absent. New content without Last-Modified receives a
stable retrieval timestamp; generic HEAD ETag changes announce a newer mtime during
inventory. Where no useful remote validators exist, inventory cannot discover a
content change without downloading; it deliberately does not download bodies.
An explicit forced retrieval can refresh these sources.

Inventory may use HEAD or source APIs even in a dry run when an entry is absent
or due. `skip_remote_checks` bypasses those probes only. `offline` checks cache
before all transport and source metadata paths and fails clearly on missing or
corrupt data. A per-call policy override is an explicit network-policy change.
`refresh` controls actual retrieval, not Snakemake scheduling. Source metadata
caches live for one provider lifetime; use a new provider/workflow invocation to
observe a new publisher manifest/API version.

## Deployment and migration

Install the plugin alongside Snakemake and on every worker executing Snakemake
storage operations. Install it inside scientific job environments only for clients
that actually use its Python API. Configure the same persistent root on workers
only if that root is really shared and lock-safe; worker-local caches are separate.
No localhost/browser access is involved.

Changing Python/job software environments does not change request identity or
persistent body bytes. Cache location must live outside ephemeral environment and
Snakemake staging directories. Staging cleanup does not remove persistent entries.

Existing installations must explicitly set public_cache, select their desired
policy, and import suitable existing bytes. Cache v1 does not auto-adopt legacy
unverified files. Retain old cache files until import verification and offline
retrieval succeed; removal remains an operator decision.

The deterministic tests cover HTTPX transport, local HTTP, independent processes,
Snakemake parsing/inventory/dry run/DAG/retrieval/cleanup/rerun and both providers
coexisting. CI runs the supported Python matrix. Local verification records the
exact runtime in the plan; it does not establish every interface release in the
declared dependency range.
