# acyclic-stream

Hierarchical append-only streams with exact sequence cursors, conditional appends, forks, and coordinated commits. A `StreamClient` wraps any `StreamProvider` without changing provider semantics.

Remote consumers adopt `grpc::Client` for the provider contract and obtain its
`session_client()` for multiplexed calls and live queries. Reuse those handles
within an authority scope: `session::Client` owns channel credit, checked resume,
revision floors and reconnection. A consumer must not introduce a second follow
loop or connection pool. Forwarding adapters preserve filters, absence policy,
history fences, deadlines and revisions, including across reconnects. Credentials
remain transport-owned and never enter record payloads or persisted cursors.

An application receipt stream is ordinary retained stream history. Retain its
terminal outcomes for at least as long as a caller may replay the corresponding
identity, and publish the receipt in the same Commit as the effects it certifies.
Transport delivery and credit release do not certify application completion.
After a disconnect, cancellation or crash before a terminal receipt is observed,
the outcome is ambiguous: inspect retained facts or retry the exact request with
the same identity. Absence of a receipt, including an empty inspection while an
operation may still be in flight, never proves nonexecution. Retiring receipts
must not make a previously accepted identity executable again.

An actor fork is `CommitMutation::Fork` plus its activation append in one
`CommitRequest`, guarded by destination absence and the required source head.
The fork mutation's `records` append to the inherited root; a separate activation
stream outside the destination subtree may be another `CommitMutation::Append`.
Mutations below the destination remain invalid. The actor runtime consumes the
committed activation; consumers must not publish activation before the fork or
register the child in a second transaction. The receipt certifies both facts or
neither, and exact replay cannot activate a second child.

`FollowRequest::when_absent` defaults to `NotFound`. `Wait` registers under the
same publication lock that captures the cursor, then waits for creation without
rewinding that cursor. Creation through an inherited fork is also publication.
Dropping the follow releases its registration. An existing checked prefix that
disappears or changes fails with `PrefixNotRetained`; authorization refusal stays
`AccessDenied`. Neither event turns into a new creation wait.

LATEST projects opaque record bytes using exactly one indexed header key and an
optional `attention` attribute (`true` or `false`, encoded as ASCII). Attention
records are lossless; ordinary pending values conflate only within the same key.
ALL preserves the original record. `SessionFollow.bootstrap_head` starts a fresh
LATEST subscription with the last record at one captured Head, followed from its
checked tail. It does not reconstruct older keys or historical attention events.
A resumed nonzero cursor replays forward without repeating the head bootstrap.
Harness status publishers use `StatusRecord::append_record`, retaining their
existing JSON encoding and declaring attention on approval, host and terminal
transitions.

`heads(paths, children, limit)` captures named heads, named absence and an optional
child page at one authority revision. The caller supplies the total result budget;
named observations consume one slot each and the remaining slots bound children.
The hierarchy continuation retains its existing revision fence. A refused named
path refuses the entire view; it never appears as absence. Remote adapters carry
this operation intact through the shared session rather than composing head calls.

`ReadRequest.key` seeks an exact opaque header key within
`[from, min(key.before, tail at open))`. Its limit counts matching records;
returned sequences keep their original gaps. Resume a full page at the last
sequence plus one with the same exclusive bound. EOF proves the captured interval
has no further match. The memory index shares inherited fork cuts and is rebuilt
from canonical snapshot facts. Native seek work and retained index nodes remain
on the original work owner's ledger. Session reads use the same credit and
authorization as ordinary finite reads and reject changed key bytes.

Opening a local snapshot written before receipt revisions returns
`UnsupportedPreRevisionStore`, with instructions to re-create the store. It does
not infer a missing revision or alter the persisted snapshot.

`Stream::submit` returns an owned `'static` `Pending` for one canonical Commit.
Submission transfers custody to the executor immediately. Cloned handles share
one result; cancelling or dropping a waiter never withdraws or retries the write.
The handle retains the exact keyed request for recovery of an ambiguous outcome.
Staged publication uses the same `Pending::submit` once its Commit is ready.

`session::Client::deliveries` supplies application-owned ALL/LATEST decisions on
the existing pool. Persist a delivery's proposed v1 checkpoint atomically with
the consumer's effects or discard decision, then call `commit` or `discard`.
Returning transport credit alone never advances this checkpoint. An abandoned
delivery ends the interest without completion; recovery starts at the last durable
consumer checkpoint. An outstanding decision backpressures the next delivery.
Reconnect fences old handles: completing one returns false and the current
generation replays the item. Consumer effects therefore use their retained
identity on replay. TypeScript `StreamSessions.deliveries` uses the same Rust
handle and exposes the same explicit decision methods.

The follow-up measurement plan compares 1, 8 and 64 observations and logical
backlogs of 10^6 through 10^9 records. Report lock acquisitions, retained RAM,
first-output latency and work during cancellation, withheld application completion
and reconnect. Compare coherent heads against separate observations while a root
advances; indexed seek against the same reader with its index disabled; and lazy
pinned history against eager window construction. Correctness controls include
replaced fork prefixes, creation during registration, distinct opaque keys,
attention, malformed JSON, tiny credit, a crash before application commit and
changed key bytes. These are planned measurements, not performance results.

`pages::PageSource::next_page(after, budget)` is the storage seam for pinned
history, with `after` naming the next unread sequence. A source retains one
immutable history and its inherited fork cuts; each nonempty page is contiguous
and bounded by the caller's page budget. `pages::PinnedCursor` retains one page,
loads on demand, and advances only when a record is consumed. Memory reads and
follows use this cursor over immutable batch windows, without building a queue
for the retained backlog. Durable page ports can await their storage event in
the same interface. Replacing a follow pin checks its preceding commit before
exposing records from the new cut.

```sh
cargo add acyclic-stream
```

The default `grpc` feature provides the service client; `local` enables the durable local implementation. `MemoryStream` is deterministic but process-local. Use `StreamPath::new` to validate a path, keep the returned cursor when consuming records, and use an idempotency key plus tail precondition when retrying or coordinating writes.

On browser WASM builds, `BrowserStream::open(name, limits)` binds that same Rust state machine to a strict `IndexedDB` command journal. It implements the ordinary `StreamProvider` contract, including atomic multi-path commits and provider-clock deadlines. Native and browser persistence share the command codec and Rust replay. Tabs and workers serialize refresh, admission and publication in one transaction on one named database; this gives no atomicity across databases, filesystem volumes or other providers. Browser storage eviction and origin quotas remain platform limits.

Persistent providers share `Replicated<J: CommandJournal>` for canonical replay
and conditional publication. The journal port supplies `append_if(expected,
frames)`, `read(from, page)` and an atomically registered `changed(after)` event.
Storage cursors are opaque and strictly increasing within the retained journal;
they are separate from Stream sequence numbers and applied revision indexes.
Catch-up pins its first observed journal head and reads bounded pages through
that cut, so concurrent writers cannot keep a reader chasing the head. Only a
reported stale proposal is replanned, with its original request identity and
deadline; an unknown transport outcome closes that replica. Browser, host,
PostgreSQL and local storage retain their native durability boundaries. Native
local completion and prepared ingress retain their original funded guards.

`StreamProvider::context()` borrows the session's process claims and reports its
publication policy without allocating another owner. Clones preserve claim
identity. Only storage whose every writer shares those claims declares process
exclusion; a remote cache cannot provide it. Deferred durability is explicit:
the session must be able to seal its accepted writes with its durability barrier.
Forwarding adapters preserve these declarations only with the same boundaries.

Single-path `head` and revision-only `wait_applied` are `StreamProviderExt`
projections of `heads`, with one provider request and the same revision floor
and response validation. Providers implement `heads` once; a forwarding adapter
cannot substitute separate head observations for a mixed view. Revision waits
park outside host write serialization, so later writes can satisfy their cut.
`HeadProjection::Watermarks` returns accepted and durable offsets at that same
snapshot without reading the last record or reconstructing inherited origin.
The response identifies its projection; mismatched or incomplete observations
are refused before they reach a consumer.

Each handle retains a disposable Rust cache and catches up only new journal commands. Initial recovery and total retention are bounded by `BrowserStreamLimits` (default 65,536 commands and 256 MiB of journal bytes) and `MemoryLimits`. New work fails at capacity while exact retained replay still succeeds. Cancellation or persistence failure invalidates unpublished cached state. Live follow registers before reading and receives publication notifications over `BroadcastChannel`; subscribers catch up new durable commands on notification. Dropping the cursor releases its subscription. The provider does not run or schedule application work. Provider hooks use `BoxProviderFuture`, `BoxProviderStream`, `ProviderTask` and `ProviderPlatform`: native futures remain `Send` and native providers remain `Send + Sync`; WASM hooks retain event-loop ownership.

`MemoryStream` and `LocalStream` retain successful retry outcomes and immutable commit envelopes indefinitely within their explicit capacity limits. Replay precedes capacity admission; a new identity fails closed when the store is full. An admitted identity cannot execute again or acquire another digest after a clock advance. `LocalStream` preserves these outcomes through journal replay and snapshots. Recovery enforces the same capacity limits as live admission. Hosted services must provide the same no-reexecution guarantee under their own durable account-wide retry authority; local checks do not qualify that service behavior.

The `http` feature provides `http::HttpStream`, implementing the same provider interface over hosted JSON routes. It accepts HTTPS (or loopback HTTP for local tests), a bearer token, a response byte bound, and an optional private CA. Follow uses the canonical multiplexed session, with per-channel credit, checked resume cursors, and one shared connection per route. Browser HTTP clients use its WebSocket transport. Dropping a cursor cancels its logical channel. Mutations are not automatically retried. Canonical successful Commit and committed retry observations include the full immutable `envelope`, so mutation-only credentials need no follow-up commit read. Published compact-only servers still require `commits/read` access for those operations.

Native servers can use `http_response::encode(route, protobuf_bytes, maximum_json_bytes)` to project generated append, fork, Commit, commit-read, child-page and retry-observation responses through the same Rust code used by WASM. The successful Commit retains its existing `ok`, `commitId`, `tails` and `forks` fields and adds the complete `envelope`. The encoder does not authorize or durably accept mutations.

Replicated providers can use `request::append_digest`, `request::fork_digest`, and `request::commit_digest` for the canonical state-independent validation and retry digest. Commit normalization sorts participants and rejects duplicate paths; providers still own atomic authorization, state checks, admission, durability and deadline decisions.

`preparation::append`, `preparation::fork`, and `preparation::commit` construct canonical outcomes and immutable envelopes from pre-commit existence/tails, an accepted commit ID and commit timestamp. Coordinated preparation requires explicit observations for every participant and fork source; missing observations are invalid. Forks always use the source's pre-commit prefix, including when that commit also appends to the source. The memory provider shares these condition, authority and record construction helpers. Providers must perform retry lookup first and obtain observations, authorize, reserve capacity and publish atomically; preparation itself performs no durable acceptance or retention.

`persistence::encode_observation` / `decode_observation` and `encode_envelope` / `decode_envelope` preserve complete terminal retry outcomes and immutable envelopes using existing generated Stream v1 protobuf messages. Each takes an explicit positive byte bound. Invalid facts, malformed bytes and noncanonical encodings fail closed, including unknown fields. The storage owner supplies its own checksum/container, account and request-digest binding, accepted ID/time durability and atomic publication; these codecs do not establish acceptance or authenticate stored bytes. No optional feature is required.

Use `children_page` to discover large agent or stream hierarchies. Its ordered continuation carries the exact last hierarchy-changing commit ID; a concurrent path creation returns `HierarchyChanged`, so restart the traversal instead of silently missing or duplicating a child. Ancestor paths are materialized lazily when descendants are created; no fork lineage is required for discovery.

`LocalStream` writes one first-party v1 journal/snapshot format. The journal magic is `ACYCLIC-STREAM-LOCAL-V1\0`; the snapshot magic is `ACYCLIC-STREAM-SNAP-V01\0`. Both bind eight little-endian limit words and a journal epoch. A snapshot then holds the canonical `ACYCLIC-STREAM-STATE-V1\0` state and a SHA-256 checksum. A journal frame holds a four-byte little-endian payload length, an eight-byte sampled Unix commit timestamp, the canonical command protobuf and a SHA-256 checksum. There are no expiry fields, format readers or migration branches. Other bytes are incompatible.

Recovery rejects unknown or duplicate protobuf fields, duplicate retained identities, invalid history links and capacity violations. It validates the complete semantic prefix before truncating a torn final frame or restarting a journal covered by the immediately succeeding snapshot epoch. A failed semantic open preserves existing journal/snapshot bytes and releases ownership. Successful recovery synchronizes recovered frames before exposing state. An unfinished snapshot temporary file is inert and is overwritten by the next compaction.

The crash model permits a torn final append or header and a crash between durable snapshot installation and journal restart. A valid checksummed frame after an invalid frame indicates corruption and fails closed. Checksums detect damage; they do not authenticate a hostile storage device or prove arbitrary power-loss behavior.

The [Rust API](https://docs.rs/acyclic-stream/latest/acyclic_stream/) and [v1 protocol](https://github.com/acyclic-labs/sdk/tree/main/rust/crates/stream/proto/stream/v1) are the sources for exact limits and commit behavior.

Stream helpers require the caller's `IdempotencyKey` for every mutation. Conditional rejection stores no outcome and writes no journal frame; the same key may be retried with fresh conditions. Successful bindings remain fixed for the authority lifetime. After an uncertain completion, retry the exact request; an empty inspection alone does not prove that an in-flight request failed. `Tail { expected: 0 }` accepts absent and existing-empty paths; `Absent` requires absence.

`ForkRequest` and `CommitMutation::Fork` share the immutable pre-commit namespace subtree. `at_tail` selects the root stream prefix; descendants retain their heads at that same cut. A destination may be nested inside its source. `exclude: Vec<RelativePath>` removes canonical relative segment prefixes, such as `children`, from inheritance. Companion source appends and binding records compose in one Commit. Mutations beneath a fork destination are rejected, and a fork destination must be guarded by `Absent`. An `Append` needs no condition on its own path, so a send guarded only by a recipient's closed marker is one Commit. Appends after publication remain private.

The persistent segment trie uses an AVL sibling index with exact count, byte, depth and path-length aggregates. Fork work is independent of descendant count: O(destination depth × sibling-index height + excluded path/index work). Point updates copy only their index spines; snapshots enumerate the retained namespace. Capacity checks never enumerate the source subtree. Forks and readers retain immutable history through reference ownership.

Run the shared provider suite against a fresh authority prefix with `ACYCLIC_CONFORMANCE_ENDPOINT` and `ACYCLIC_CONFORMANCE_TOKEN` using `cargo run -p acyclic-stream --features http,conformance --example provider-conformance`. It calls `conformance::verify(&backend)`, the same complete contract, key/subtree histories and request-budget suite used for memory, local, HTTP and browser providers. The same `provider-conformance -- --fixture HTTPS_ENDPOINT` runner keeps the local fixture's commit-only credential, authentication and response-size checks. Postgres uses this runner with `--features postgres,conformance --example provider-conformance -- --postgres`; its `--scale` option retains the on-demand subscription/resource vector. `wave1-conformance` accepts `ACYCLIC_CONFORMANCE_NAMESPACE` and selects individual named histories when producing proof receipts.

For G46/G47, enable `conformance` and implement `conformance::Backend` with the
public provider and its request ledger. `Fixture::new(provider)` wraps a concrete
provider in the shared `testing::Counted<P>` before type erasure. Its ledger
measures public method calls; a backend qualifying physical transport traffic
supplies its own dispatch ledger, including calls during cursor pulls. Register
the complete suite with `conformance::suite!(MyBackend::fresh)`, where the async
factory retains disposable storage and returns `Result<impl Backend, E>`.
`conformance::bench(&backend)` checks 256 append/follow/read cycles on 4-KiB
records and reports elapsed time and request classes. Each steady-state operation
requires zero control requests and at most one data request. Use fresh storage
for each invocation. These functions run on demand.

The `conformance` feature exposes `testing::Counted<P>` and `assert_requests!` for downstream request budgets. For example, `assert_requests!(provider, provider.append(request).await, control = 0, data <= 1)` returns the original result and checks only calls issued during that expression. Metadata, list, head, lease and token calls count as control traffic. Retain `provider.ledger()` before erasing a counted provider into a trait object to keep measuring its calls.

These counters measure the instrumented provider boundary. To qualify physical transport requests, put the same ledger at the backend's transport dispatch boundary. Shared clones contribute to one ledger; give concurrent independent workloads separate wrappers.

Single-path `tail` uses the accepted-offset projection of `heads`; it neither
fetches history nor waits for persistence. `watermarks` requests durability
metadata explicitly. A durability receipt or barrier joins persistence when
that guarantee is required. Empty and single-path observation sets use inline
storage; larger sets grow with the requested budget.

`HeadsRequest::durable_through` asks for an irreversible-prefix confirmation in
that same captured view. `PathHead::retained_through` carries this proof
separately from `durable`: retained memory history makes no crash-persistence
claim. Local storage releases the publication read lock before joining a
required device flush. Already confirmed and out-of-range requests add no flush.

The observation consolidation removes repeated source-level work: the wire tail
handler takes one provider view instead of separate revision, offset and final
revision observations. A local receipt takes one publication snapshot instead
of a watermark snapshot followed by another accepted snapshot. Ordinary and
native accepted-tail reads join zero durability barriers. Empty and single-path
request/result collections add zero collection allocations; larger views grow
with their actual cardinality. These are source counts, not measured latency or
throughput results. Physical flush counts depend on the requested guarantee and
concurrent work already in the device buffer.

Local, browser, journal and Postgres adapters borrow the same memory snapshot
implementation directly. This removes their inner boxed provider future. Local
multi-path views also reuse the request's path storage instead of cloning its
collection to associate each captured head with its durability metadata.

All local query families share accepted-publication visibility, including native
reads, retained outcome inspection and immutable-envelope reads. A query adds
zero durability barriers; explicit receipts and session barriers own that cost.
The cancellation control holds the real device flush while ordinary and native
queries observe the accepted record, then checks that its receipt remains
pending until the device completes.

`StreamProvider::write` is the single publication entry for `WriteRequest` and
`WriteOutcome`. Append, Fork and Commit remain distinct canonical families;
their digests, atomic current-tail selection and retained replay are preserved.
The corresponding `StreamProviderExt` methods validate the returned family.
Transparent adapters forward the same future directly: their former one future
allocation per publication becomes zero. Memory and transport producers select the request
family before boxing, so dispatch adds no allocation or physical storage work.
Journal-backed providers return the shared core outcome without unwrapping and
re-wrapping each family. These are source counts, not performance measurements.

Ordinary and native finite memory reads share the immutable history pager and
inline page state, including exact opaque-key seeks and inherited cuts. Native
opening retains only its funded future and cursor frames: the former range-sized
window allocation is removed. History examinations happen at page pulls, outside
the publication lock; record copying still reserves bytes and examination work
before the clone. A late refusal retains its original I/O cause and refunds its
reservations. The eager reader remains a negative control for opening storage
and first-output work. These source-level allocation counts do not claim a
backlog-independent total traversal cost.

Funded native Append and Commit use `native_write` with the same canonical
`WriteRequest` and `WriteOutcome` as portable publication. Their extension
projections add no boxed future and preserve immediate admission of the concrete
producer frame. Both commit entry points use the same generic guard owner;
local callbacks remain owned through detached physical completion. The
allocation control compares native dispatch with its projection and deliberately
boxes a projection to expose the otherwise unadmitted extra frame.
