# acyclic-stream

Hierarchical append-only streams with exact sequence cursors, conditional appends, forks, and coordinated commits. A `StreamClient` wraps any `StreamProvider` without changing provider semantics.

```sh
cargo add acyclic-stream
```

The default `grpc` feature provides the service client; `local` enables the durable local implementation. `MemoryStream` is deterministic but process-local. Use `StreamPath::new` to validate a path, keep the returned cursor when consuming records, and use an idempotency key plus tail precondition when retrying or coordinating writes.

Native Rust gRPC and the Node/Bun N-API companion trust the platform roots through
`rustls-platform-verifier`: Windows certificate-chain APIs, macOS Security.framework,
and Linux native roots selected by `SSL_CERT_FILE` and `SSL_CERT_DIR`. A supplied
PEM CA is **the only trust** for the connection, including pooled and eager clients;
it replaces ambient trust. This does not change HTTP, browser, or direct
TypeScript ConnectRPC transport trust.

Hosted gRPC Read/Follow frames may carry an optional opaque 32-byte delivery token. After validating the complete frame, the client queues a best-effort receipt to that frame's issuing endpoint, independently of record delivery. One worker per logical reader holds at most 16 pending receipts plus one in-flight RPC bounded to 500 ms; queue pressure discards receipts, not records. Dropping or ending the reader cancels its worker and pending receipts. Failures do not alter read results, and no per-frame detached tasks or durable receipt ledger are created.

`grpc::Client::without_delivery_acknowledgements` (also on `StreamClient<grpc::Client>`) opts translating clients out. Explicit `acknowledge_delivery(endpoint_index, token)` uses configured endpoint order and never fails over an endpoint-bound token. Local/memory server adapters return `Unsupported`; they never manufacture hosted tokens. These are transport receipts, not proof of application consumption, billable egress, or Actor checkpoints.

Actual hosted use requires the Data service to implement the canonical `acyclic.stream.v1.StreamService/AcknowledgeDelivery` RPC, token issuance on Read/Follow frames, authenticated caller/issuing-endpoint binding, expiry, and idempotent replay with durable receipt retention. Those service capabilities are external prerequisites, not SDK implementations or deployment claims. Existing timestamps, atomic Commit, indefinite history, and reserved deletion fields are unchanged.

Record values and append/fork mutation payloads use `OwnedBytes`. Ordinary callers can use `OwnedBytes::from_static`, `OwnedBytes::copy_from_slice`, or `OwnedBytes::from(Vec<u8>)`; compare borrowed contents with `as_slice()`. Generated protobuf byte fields remain `bytes::Bytes`; the ordinary wire adapter retains the original guarded owner without copying its data.

Native `OwnedBytes::from_vec_with_guard_in_place` admits sharing controls before
moving the vector; rejection restores the same original guard and leaves the
vector untouched. `try_into_ordinary_vec` reuses only unique, fully visible,
known ordinary backing. Guarded, opaque, sliced, or aliased carriers cannot
escape custody through that operation.

The ordinary HTTP protobuf bridge uses `Bytes::from_owner(OwnedBytes)` without
copying the source payload or detaching its custody. Encoding creates the
ordinary request body before transport; this unbounded control allocation is
not a finite native provider guarantee.

On browser WASM builds, `BrowserStream::open(name, limits)` binds that same Rust state machine to a strict `IndexedDB` command journal. It implements the ordinary `StreamProvider` contract, including atomic multi-path commits and provider-clock deadlines. Native and browser persistence share the command codec and Rust replay. Tabs and workers serialize refresh, admission and publication in one transaction on one named database; this gives no atomicity across databases, filesystem volumes or other providers. Browser storage eviction and origin quotas remain platform limits.

Each handle retains a disposable Rust cache and catches up only new journal commands. Initial recovery and total retention are bounded by `BrowserStreamLimits` (default 65,536 commands and 256 MiB of journal bytes) and `MemoryLimits`. New work fails at capacity while exact retained replay still succeeds. Cancellation or persistence failure invalidates unpublished cached state. Live follow polls durable state every 100 ms while consumed, so killing a publisher before any wakeup cannot hide its committed records. Dropping the cursor ends that polling. The provider does not run or schedule application work. Provider hooks use `BoxProviderFuture`, `BoxProviderStream`, `ProviderTask` and `ProviderPlatform`: native futures remain `Send` and native providers remain `Send + Sync`; WASM hooks retain event-loop ownership.

Browser append preflight borrows the same canonical command wire as native
persistence. Checked encoded length rejects the command bound before encoding
or opening an IndexedDB transaction; encoding allocation failure reports
capacity without accepting a mutation. Browser clock, replay, transaction and
lost-ack semantics are unchanged.

`MemoryStream` and `LocalStream` retain retry outcomes and immutable commit envelopes indefinitely within their explicit capacity limits. Replay precedes capacity admission; a new identity fails closed when the store is full. An admitted identity cannot execute again or acquire another digest after a clock advance. `LocalStream` preserves these outcomes through journal replay and snapshots. Recovery enforces the same capacity limits as live admission. Hosted services must provide the same no-reexecution guarantee under their own durable account-wide retry authority; local checks do not qualify that service behavior.

Native `MemoryStream` and `LocalStream` operations use the existing provider
engine with the original `NativeReadMeter`, including retained-window reads,
conditional commits and provider-clock deadlines. Input copies, accepted worker
frames, raw record buffers and persistent retry payloads retain their actual
original custody through completion and mandatory durability drain. Initialize
native I/O services explicitly before exposing the finite provider.
These limits describe SDK-observable calls and logical known payload/control
storage, not provider-internal physical I/O, delivery acknowledgments or RSS.
Known `Arc` contents are charged until their `Self` is destroyed; opaque
standard-library allocation backing retained by `Weak` after that point is
excluded. Caller input vectors are borrowed or copied with admission, never
adopted as if the SDK had allocated them.

The `http` feature provides `http::HttpStream`, implementing the same provider interface over hosted JSON routes. It accepts HTTPS (or loopback HTTP for local tests), a bearer token, a response byte bound, and an optional private CA. Follow validates the starting cursor against the tail once, then lazily polls contiguous pages of at most 256 records. It drains each page under consumer backpressure before fetching again, waits 250 ms after an empty page, and reports a terminal error once. Dropping the stream cancels its pending read or idle wait. Mutations are not automatically retried. Canonical successful Commit and committed retry observations include the full immutable `envelope`, so mutation-only credentials need no follow-up commit read. Published compact-only servers still require `commits/read` access for those operations.

Hosted HTTP receipt acknowledgement is explicit: `HttpStream::acknowledge_delivery` accepts exactly 32 token bytes and posts the Rust-owned `deliveryToken` projection to `/v1/stream/delivery/acknowledge`. A successful response must be a JSON object. HTTP reads and follow never request or automatically acknowledge delivery metadata. Tokens are issued, scoped, expired and durably deduplicated by the hosted service; acknowledgement does not claim application consumption, billing or cursor advancement. Memory, local and browser providers issue no delivery tokens, and the WASM local dispatch rejects explicit acknowledgement as `Unsupported`.

The canonical `HTTP_RESPONSE_CONTRACT` includes this route as `empty`: the
protobuf response projects to a validated JSON object, and the generated
TypeScript/WASM decoder exposes its successful result as `undefined`.

Native servers can use `http_response::encode(route, protobuf_bytes, maximum_json_bytes)` to project generated append, fork, Commit, commit-read, child-page and retry-observation responses through the same Rust code used by WASM. The successful Commit retains its existing `ok`, `commitId`, `tails` and `forks` fields and adds the complete `envelope`. The encoder does not authorize or durably accept mutations.

Replicated providers can use `request::append_digest`, `request::fork_digest`, and `request::commit_digest` for the canonical state-independent validation and retry digest. Commit normalization sorts participants and rejects duplicate paths; providers still own atomic authorization, state checks, admission, durability and deadline decisions.

`preparation::append`, `preparation::fork`, and `preparation::commit` construct canonical outcomes and immutable envelopes from pre-commit existence/tails, an accepted commit ID and commit timestamp. Coordinated preparation requires explicit observations for every participant and fork source; missing observations are invalid. Forks always use the source's pre-commit prefix, including when that commit also appends to the source. The memory provider shares these condition, authority and record construction helpers. Providers must perform retry lookup first and obtain observations, authorize, reserve capacity and publish atomically; preparation itself performs no durable acceptance or retention.

Native completion preparation accepts only two exact Tail conditions, one Append
and one empty record slot, an immutable retry key, and a declared maximum record
size. Memory creates the actual canonical result, replay and history controls
before the source action and holds numeric resident-capacity reservations that
ordinary and native mutations also honor. No Memory lock spans that action.
Completion fills those existing slots, rechecks both tails and the original
history/path visibility under the canonical CAS, and uses the normal Commit
digest, ID and retry outcome. Changed history with unchanged tails fails with
`PrefixNotRetained`; competing tails produce the canonical retained conflict.
An already retained receipt is checked first and remains replayable with the
exact canonical digest even after its participant paths or history retire.
Completion performs only previously admitted work and records actual SDK
attempts; it does not admit, allocate, stringify a cause, or create source
authority after cancellation. Known SDK Vec/Arc payload controls retain their
original allocation guards; opaque standard-library backing and runtime wait
queues are not claimed as measured Root payload.

The packet also owns one genuine, prebuilt recovery future. Taking it transfers
that existing frame; polling it later observes the current canonical retry table,
not a prefetched result. Its exact single-Append/single-record output and two
conflict slots, original immutable key, and Read credits are funded before the
source action. Foreign families or outcomes outside that closed shape fail with
a static typed mismatch or bound error without allocating or reporting absence.
The unified `OwnedBytes` record and key aliases retain their original payload
custody through the last alias, including an empty sliced view.

`native_commit_guarded` uses the same canonical Commit engine with the caller's
owned original guard and typed error. Memory prepares real controls, releases
its state lock while awaiting that guard, and then immediately tries the CAS
lock. Contention or a changed canonical mutation revision discards the attempt
and reruns the original guard; no permission survives an asynchronous lock wait.
The revision covers publication, retained conflicts, fork/history, residency
and reservation transitions on the existing state identity. Recovery replaces
that identity, and revision exhaustion fails new guarded acceptance closed
without preventing actual cleanup or previously accepted completion.
Guarded publication binds its preallocated record timestamps to the protected
provider clock; cached replay facts retain their original timestamp and digest.
No new namespace authority or work meter is created. The already preaccepted
physical-completion ingress deliberately does not acquire fresh task authority
after its source action.
Guard denial preserves the original typed cause, leaves the full physical
journal unchanged and publishes no retry outcome. The SDK write-call counter
can still advance on entry; it is not evidence of physical journal mutation.

`persistence::encode_observation` / `decode_observation` and `encode_envelope` / `decode_envelope` preserve complete terminal retry outcomes and immutable envelopes using existing generated Stream v1 protobuf messages. Each takes an explicit positive byte bound. Invalid facts, malformed bytes and noncanonical encodings fail closed, including unknown fields. The storage owner supplies its own checksum/container, account and request-digest binding, accepted ID/time durability and atomic publication; these codecs do not establish acceptance or authenticate stored bytes. No optional feature is required.

Use `children_page` to discover large agent or stream hierarchies. Its ordered continuation carries the exact last hierarchy-changing commit ID; a concurrent path creation returns `HierarchyChanged`, so restart the traversal instead of silently missing or duplicating a child. Ancestor paths are materialized lazily when descendants are created; no fork lineage is required for discovery.

`LocalStream` writes one first-party v1 journal/snapshot format. The journal magic is `ACYCLIC-STREAM-LOCAL-V1\0`; the snapshot magic is `ACYCLIC-STREAM-SNAP-V01\0`. Both bind eight little-endian limit words and a journal epoch. A snapshot then holds the canonical `ACYCLIC-STREAM-STATE-V1\0` state and a SHA-256 checksum. A journal frame holds a four-byte little-endian payload length, an eight-byte sampled Unix commit timestamp, the canonical command protobuf and a SHA-256 checksum. There are no expiry fields, format readers or migration branches. Other bytes are incompatible.

Recovery rejects unknown or duplicate protobuf fields, duplicate retained identities, invalid history links and capacity violations. It validates the complete semantic prefix before truncating a torn final frame or restarting a journal covered by the immediately succeeding snapshot epoch. A failed semantic open preserves existing journal/snapshot bytes and releases ownership. Successful recovery synchronizes recovered frames before exposing state. An unfinished snapshot temporary file is inert and is overwritten by the next compaction.

The crash model permits a torn final append or header and a crash between durable snapshot installation and journal restart. A valid checksummed frame after an invalid frame indicates corruption and fails closed. Checksums detect damage; they do not authenticate a hostile storage device or prove arbitrary power-loss behavior.

The [Rust API](https://docs.rs/acyclic-stream/latest/acyclic_stream/) and [v1 protocol](https://github.com/acyclic-labs/sdk/tree/main/rust/crates/stream/proto/stream/v1) are the sources for exact limits and commit behavior.

The native `acyclic.stream.http.follow` span lives with the cursor. Its `rev`, `queued`, and `phase` fields distinguish startup tail validation, read work, idle sleep, and time awaiting the consumer. HTTP call spans include response body consumption; abandoning a pending physical request records `outcome = "err"` and `error.kind = "cancelled"`. A trace-level `follow.poll` child also includes page decoding and validation, and a debug-level `follow.sleep` child attributes the idle wait. Polling and destruction preserve the visible follow's originating subscriber. A terminal error records its stable code; a dropped cursor records `terminal = "dropped"` without claiming successful completion or a particular cancellation cause. No paths, credentials, or record contents are recorded.

The current HTTP contract has no long-poll route. With instantaneous reads and uniformly timed arrivals, a fixed idle interval `d` costs `1/d` reads per second and adds mean detection delay `d/2`. Any polling schedule guaranteeing a maximum blind interval `d` needs at least `1/d` reads per second in that model. The 250 ms default therefore costs four idle reads per second and adds 125 ms mean detection delay in the ideal model; startup requires one tail and one immediate read. Network/server time, decoding, scheduling, and consumer backpressure add latency, so these figures are not wall-clock guarantees. Increasing the delay trades latency for load; it cannot improve both within this protocol.
