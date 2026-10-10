# acyclic-stream

Hierarchical append-only streams with exact sequence cursors, conditional appends, forks, and coordinated commits. A `StreamClient` wraps any `StreamProvider` without changing provider semantics.

```sh
cargo add acyclic-stream
```

The default `grpc` feature provides the service client; `local` enables the durable local implementation. `MemoryStream` is deterministic but process-local. Use `StreamPath::new` to validate a path, keep the returned cursor when consuming records, and use an idempotency key plus tail precondition when retrying or coordinating writes.

On browser WASM builds, `BrowserStream::open(name, limits)` binds that same Rust state machine to a strict `IndexedDB` command journal. It implements the ordinary `StreamProvider` contract, including atomic multi-path commits and provider-clock deadlines. Native and browser persistence share the command codec and Rust replay. Tabs and workers serialize refresh, admission and publication in one transaction on one named database; this gives no atomicity across databases, filesystem volumes or other providers. Browser storage eviction and origin quotas remain platform limits.

Each handle retains a disposable Rust cache and catches up only new journal commands. Initial recovery and total retention are bounded by `BrowserStreamLimits` (default 65,536 commands and 256 MiB of journal bytes) and `MemoryLimits`. New work fails at capacity while exact retained replay still succeeds. Cancellation or persistence failure invalidates unpublished cached state. Live follow polls durable state every 100 ms while consumed, so killing a publisher before any wakeup cannot hide its committed records. Dropping the cursor ends that polling. The provider does not run or schedule application work. Provider hooks use `BoxProviderFuture`, `BoxProviderStream`, `ProviderTask` and `ProviderPlatform`: native futures remain `Send` and native providers remain `Send + Sync`; WASM hooks retain event-loop ownership.

`MemoryStream` and `LocalStream` retain retry outcomes and immutable commit envelopes indefinitely within their explicit capacity limits. Replay precedes capacity admission; a new identity fails closed when the store is full. An admitted identity cannot execute again or acquire another digest after a clock advance. `LocalStream` preserves these outcomes through journal replay and snapshots. Recovery enforces the same capacity limits as live admission. Hosted services must provide the same no-reexecution guarantee under their own durable account-wide retry authority; local checks do not qualify that service behavior.

The `http` feature provides `http::HttpStream`, implementing the same provider interface over hosted JSON routes. It accepts HTTPS (or loopback HTTP for local tests), a bearer token, a response byte bound, and an optional private CA. Follow validates the starting cursor against the tail once, then lazily polls contiguous pages of at most 256 records. It drains each page under consumer backpressure before fetching again, waits 250 ms after an empty page, and reports a terminal error once. Dropping the stream cancels its pending read or idle wait. Mutations are not automatically retried. Canonical successful Commit and committed retry observations include the full immutable `envelope`, so mutation-only credentials need no follow-up commit read. Published compact-only servers still require `commits/read` access for those operations.

Native servers can use `http_response::encode(route, protobuf_bytes, maximum_json_bytes)` to project generated append, fork, Commit, commit-read, child-page and retry-observation responses through the same Rust code used by WASM. The successful Commit retains its existing `ok`, `commitId`, `tails` and `forks` fields and adds the complete `envelope`. The encoder does not authorize or durably accept mutations.

`http_codec::HTTP_ROUTES` lists exactly the ten hosted StreamService RPC
projections. `http_codec::decode` reverses the SDK request JSON into generated
protobuf without moving provider validation or authorization into the adapter.
`http_response::StreamProjection` expands compressed read/follow and children
batches into canonical JSON elements, checking cross-frame cursor/order bounds;
`Collection` bounds finite JSON arrays. Native front doors can expose these
same elements as JSON, NDJSON or SSE. Follow is streaming-only and emits an
accepted head even when idle; cancellation must propagate to the underlying RPC.
Token issuance remains account-authority-owned, not a fabricated Stream RPC.

Private consumers may supply their own SVID/mTLS channels with
`grpc::Client::from_channels(channels, bearer)`. The caller owns channel
authentication; bearer validation and canonical request bounds remain unchanged.

Replicated providers can use `request::append_digest`, `request::fork_digest`, and `request::commit_digest` for the canonical state-independent validation and retry digest. Commit normalization sorts participants and rejects duplicate paths; providers still own atomic authorization, state checks, admission, durability and deadline decisions.

`preparation::append`, `preparation::fork`, and `preparation::commit` construct canonical outcomes and immutable envelopes from pre-commit existence/tails, an accepted commit ID and commit timestamp. Coordinated preparation requires explicit observations for every participant and fork source; missing observations are invalid. Forks always use the source's pre-commit prefix, including when that commit also appends to the source. The memory provider shares these condition, authority and record construction helpers. Providers must perform retry lookup first and obtain observations, authorize, reserve capacity and publish atomically; preparation itself performs no durable acceptance or retention.

`persistence::encode_observation` / `decode_observation` and `encode_envelope` / `decode_envelope` preserve complete terminal retry outcomes and immutable envelopes using existing generated Stream v1 protobuf messages. Each takes an explicit positive byte bound. Invalid facts, malformed bytes and noncanonical encodings fail closed, including unknown fields. The storage owner supplies its own checksum/container, account and request-digest binding, accepted ID/time durability and atomic publication; these codecs do not establish acceptance or authenticate stored bytes. No optional feature is required.

Shared native storage may use `StreamPath::qualify_storage`, `from_storage` and
`unqualify_storage` with a protected, account-derived namespace hash. The canonical
storage qualifier is forbidden in public paths and preserves the complete public
path byte/segment allowance. Public HTTP/gRPC/permission decoders must continue
to use `StreamPath::new`. `persistence::decode_storage_envelope` and
`decode_storage_observation` read retained native preparations/retry facts using
the same canonical protobuf; public fact decoding rejects qualified paths.
Native providers own account registration, namespace authorization, original
public retry-digest binding and response unqualification.

Use `children_page` to discover large agent or stream hierarchies. Its ordered continuation carries the exact last hierarchy-changing commit ID; a concurrent path creation returns `HierarchyChanged`, so restart the traversal instead of silently missing or duplicating a child. Ancestor paths are materialized lazily when descendants are created; no fork lineage is required for discovery.

`LocalStream` writes one first-party v1 journal/snapshot format. The journal magic is `ACYCLIC-STREAM-LOCAL-V1\0`; the snapshot magic is `ACYCLIC-STREAM-SNAP-V01\0`. Both bind eight little-endian limit words and a journal epoch. A snapshot then holds the canonical `ACYCLIC-STREAM-STATE-V1\0` state and a SHA-256 checksum. A journal frame holds a four-byte little-endian payload length, an eight-byte sampled Unix commit timestamp, the canonical command protobuf and a SHA-256 checksum. There are no expiry fields, format readers or migration branches. Other bytes are incompatible.

Recovery rejects unknown or duplicate protobuf fields, duplicate retained identities, invalid history links and capacity violations. It validates the complete semantic prefix before truncating a torn final frame or restarting a journal covered by the immediately succeeding snapshot epoch. A failed semantic open preserves existing journal/snapshot bytes and releases ownership. Successful recovery synchronizes recovered frames before exposing state. An unfinished snapshot temporary file is inert and is overwritten by the next compaction.

The crash model permits a torn final append or header and a crash between durable snapshot installation and journal restart. A valid checksummed frame after an invalid frame indicates corruption and fails closed. Checksums detect damage; they do not authenticate a hostile storage device or prove arbitrary power-loss behavior.

The [Rust API](https://docs.rs/acyclic-stream/latest/acyclic_stream/) and [v1 protocol](https://github.com/acyclic-labs/sdk/tree/main/rust/crates/stream/proto/stream/v1) are the sources for exact limits and commit behavior.

The native `acyclic.stream.http.follow` span lives with the cursor. Its `rev`, `queued`, and `phase` fields distinguish startup tail validation, read work, idle sleep, and time awaiting the consumer. HTTP call spans include response body consumption; abandoning a pending physical request records `outcome = "err"` and `error.kind = "cancelled"`. A trace-level `follow.poll` child also includes page decoding and validation, and a debug-level `follow.sleep` child attributes the idle wait. Polling and destruction preserve the visible follow's originating subscriber. A terminal error records its stable code; a dropped cursor records `terminal = "dropped"` without claiming successful completion or a particular cancellation cause. No paths, credentials, or record contents are recorded.

`HttpStream::follow` uses bounded polling rather than the front door's streaming Follow route. With instantaneous reads and uniformly timed arrivals, a fixed idle interval `d` costs `1/d` reads per second and adds mean detection delay `d/2`. Any polling schedule guaranteeing a maximum blind interval `d` needs at least `1/d` reads per second in that model. The 250 ms default therefore costs four idle reads per second and adds 125 ms mean detection delay in the ideal model; startup requires one tail and one immediate read. Network/server time, decoding, scheduling, and consumer backpressure add latency, so these figures are not wall-clock guarantees. Increasing the polling delay trades latency for load; it cannot improve both within this polling protocol.
