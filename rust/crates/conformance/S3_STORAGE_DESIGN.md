# S3 object layout and ETags

This is the next consumer-driven change to the Filesystem S3 view. The pinned
Historical Ceph compatibility probes failed because a
Filesystem file at `/a` excludes a directory at `/a/b`, and because the S3
ETag is derived from the whole workspace generation rather than the object's
bytes. The first failure also covers keys containing empty path components.

## Contract to preserve

- No Filesystem path is silently reinterpreted as protocol metadata.
- An object key is an opaque nonempty UTF-8 string of at most 1,024 bytes.
  Slashes, leading and trailing slashes, and repeated slashes are data.
- `a`, `a/b`, `a/`, `/a`, and `a//b` can coexist. Listing orders their exact
  UTF-8 bytes and applies prefix and delimiter to the decoded key, not the
  storage path. A page and its continuation use one immutable generation.
- Single-part PUTs return the MD5 of exactly the accepted body bytes.
  Multipart completion returns the standard digest of the ordered part MD5s
  and the part count. ETags for an unchanged object remain stable when an
  unrelated object changes. Internal content identities remain BLAKE3.
- A crash cannot publish a body without its key and ETag, or publish key and
  ETag metadata without the body. Copy and overwrite preserve this invariant.

## Proposed SDK shape

Keep one `Workspace::s3()` facade. Put S3 key encoding and content metadata
behind it, so HTTP, Rust, and test consumers do not choose a
layout or inspect private paths. A streaming write should call one public S3
operation with staged content and its digest; the HTTP adapter should only
translate protocol fields and compute the digest while it already reads the
request body. The multipart completion path should use the same publication
operation. Keep the Filesystem transaction as the atomic publication boundary.

The first encoded-slot prototype was rejected before publication: a directory
inside the customer Filesystem tree is visible and writable through ordinary
Filesystem calls and native mounts. It cannot be called private or trusted as
an S3 index. The implementation must either reserve the namespace at the
engine boundary with safe access for the S3 facade, or place the index outside
the customer tree. Measure both approaches before selecting one. Encode the
full UTF-8 key reversibly, with a terminal object slot distinct from child-key
slots; check the encoded size against each volume's configured path limits.
Store the ETag with the body in one atomic publication and verify their
association on read. No encoded S3 slots have been published yet.

The encoded layout replaces the direct-path layout; nothing reads or converts
objects stored under the direct-path layout. The current S3 view exposes
ordinary Filesystem regular files, so the change must state explicitly whether
Filesystem files remain visible as objects. Do not assume that a companion
workspace and the customer workspace can publish atomically without a shared
authority operation.

## Required executable evidence before promotion

1. Promote the three known Ceph probes and add keys with leading, trailing,
   repeated, and Unicode slashes, plus byte-limit boundaries.
2. Test both write orders for `a` and `a/b`; overwrite, copy, delete, list,
   restart, and continuation.
3. Inject interruption before staging, before publication, and after an
   indeterminate publication response; replay one idempotency identity.
4. Verify conditional GET/HEAD/copy with MD5 ETags and unchanged ETags after an
   unrelated write.
5. Compare PUT, GET, HEAD, list, and copy latency, throughput, CPU, memory,
   and allocations with the current direct-path baseline on Windows, WSL,
   and macOS. If the encoded layout adds a lookup or metadata read, measure
   its cost and remove it where possible before setting the regression gate.
6. Verify ordinary Filesystem and native-mount reads, writes, rename, remove,
   subdirectory mounts, and restart cannot corrupt or impersonate internal S3
   state. A layout that fails this check must not be promoted.

Do not claim full S3 compatibility from these cases. The existing selected
suite and all cross-host SDK gates must continue to pass during the change.
