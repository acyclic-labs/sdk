# Stream topic guide

This guide is the Rust source counterpart for the Stream topic routes. The
public contract is `StreamProvider`; `StreamClient` and `Stream<P>` are
convenience layers over that provider and do not change provider semantics.
The examples below use `MemoryStream`, which is deterministic and
process-local. Provider implementations use the same trait.

## Paths and hierarchy

`StreamPath::new` validates a permanent account-relative path. It rejects an
empty path, leading or trailing slashes, `.` and `..` segments, whitespace,
control characters, backslashes, non-ASCII text, and paths beyond
`MAX_PATH_BYTES` or `MAX_ITEMS` segments. `StreamClient::stream` returns a
typed `Stream<P>` only after this validation. A path names logical stream
state; it does not expose provider placement or storage topology.

`children` lists one fixed-snapshot page of direct children. For large or
changing hierarchies, use `children_page`: its `after` cursor and
`hierarchy_version` must be sent back unchanged. A concurrent path creation
returns `StreamError::HierarchyChanged`; restart from the first page instead of
silently duplicating or skipping children. Ancestors are materialized lazily
when descendants are created, and discovery does not imply fork lineage.

## Append and tail CAS

`AppendRequest` carries one path, a nonempty contiguous batch, an optional
`if_tail` condition, and an optional `IdempotencyKey`. A successful append
returns `AppendOutcome::Committed(AppendReceipt)` with its half-open sequence
range and resulting tail. A stale condition returns `TailConflict` and leaves
the stream unchanged. The sequence is the next record position: the first
record is zero and the receipt's `end` and `tail` are exclusive.

Retry the same logical request with the same key. Providers retain the terminal
`IdempotencyOutcome`; changing the path, records, or condition under that key
returns `StreamError::IdempotencyMismatch`. A new logical mutation requires a
new key. Providers must look up the key before admitting another execution.

```rust
use acyclic_stream::{AppendOutcome, IdempotencyKey, MemoryStream, StreamClient};
use bytes::Bytes;
use std::sync::Arc;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = StreamClient::new(Arc::new(MemoryStream::default()));
let events = client.stream("runs/example/events")?;
let key = IdempotencyKey::new(Bytes::from_static(b"runs-example-v1"))?;
let outcome = events
    .append_batch(vec![Bytes::from_static(b"started")], Some(0), Some(key))
    .await?;
assert!(matches!(outcome, AppendOutcome::Committed(receipt) if receipt.tail == 1));
# Ok(())
# }
```

## Read, replay, and follow

`read` opens a bounded finite `RecordStream` from an inclusive sequence and
limit. The cursor to persist is `record.sequence + 1`; it is exclusive when a
consumer resumes. `Stream::replay` repeatedly reads bounded pages to the tail
and checks every returned sequence. A missing stream replays as empty from zero;
an out-of-order provider response fails as `StreamError::Unavailable`.

`follow` first replays from the requested sequence and then remains live
without a handoff gap. Both APIs are backpressured streams. Dropping a hosted
HTTP follow cancels polling; mutation retries remain the caller's explicit
responsibility.

## Forks and immutable prefixes

`ForkRequest` names an existing source and an absent destination. Omitting
`at_tail` selects the source tail atomically; supplying it selects an exact
retained prefix. `ForkReceipt::forked_at` records the exclusive inherited end
and the destination starts with that immutable prefix. New appends belong to
the destination's independent suffix; a fork does not copy history or create a
physical placement promise.

The source prefix must still be retained. An existing destination returns
`StreamError::AlreadyExists`, and a missing retained prefix returns
`StreamError::PrefixNotRetained`. Fork retries use the same idempotency key and
request arguments.

## Cross-stream coordination

Use `CommitRequest` when several paths must change at one linearization point.
Every participant has an explicit `CommitCondition::Tail` or
`CommitCondition::Absent`, and every mutation is an append or an exact-prefix
fork. `CommitOutcome::Committed` returns one immutable `CommittedEnvelope`;
`Conflict` returns the exact failed conditions and leaves every path unchanged.
The request is bounded by `MAX_ITEMS` and `MAX_COMMAND_BYTES`.

```rust
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey,
    MemoryStream, StreamClient, StreamPath,
};
use bytes::Bytes;
use std::sync::Arc;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = StreamClient::new(Arc::new(MemoryStream::default()));
let key = IdempotencyKey::new(Bytes::from_static(b"create-run-v1"))?;
let result = client.commit(CommitRequest {
    conditions: vec![CommitCondition::Absent {
        path: StreamPath::new("runs/example")?,
    }],
    mutations: vec![CommitMutation::Append {
        path: StreamPath::new("runs/example")?,
        records: vec![Bytes::from_static(b"created")],
    }],
    idempotency_key: key,
}).await?;
assert!(matches!(result, CommitOutcome::Committed(_)));
# Ok(())
# }
```

`commit_digest`, `preparation::commit`, and the persistence codecs support
provider implementations; they do not themselves authorize, durably accept,
or retain a mutation. `commit_before` evaluates a trusted provider clock at
the same linearization point and fails closed with `DeadlineElapsed` or
`Unsupported` when the provider cannot enforce that contract.

## Durability and lifecycle

`MemoryStream` is a bounded process-local provider. On native targets,
`LocalStream` uses the existing V4 journal and
snapshot format. `LocalStream::open` validates headers, configured limits,
checksums, and retained retry outcomes before serving requests; incompatible or
corrupt durable data fails closed. `LocalDurability` controls the local
provider's flush policy, while `deferring_durability` scopes an explicit
deferred flush.

Successful mutations retain immutable commit envelopes and retry outcomes
within the configured capacity. Recovery does not authorize a second execution
of an admitted identity. Capacity exhaustion rejects new identities rather
than evicting a surviving terminal result. The Stream API does not expose a
generic delete or trim operation; providers retain history according to their
declared implementation and limits.

## Access and transports

`StreamProvider` is the authority boundary. `http::HttpStream` accepts HTTPS
(or loopback HTTP for local tests), a bearer token, a response byte bound, and
an optional private CA. The service client uses gRPC. Hosted adapters preserve
the same path, sequence, idempotency, fork, commit, and bounded-stream
semantics.

Access-token operation names are the canonical `TOKEN_OPERATIONS` inventory:
`list`, `read`, `follow`, `append`, `fork`, `create`, and `commit`. A token's
scope is evaluated by the provider for the requested operation and path. The
HTTP conformance example exercises authentication, bounded responses,
commit-only access, deadline failure, and exact idempotent replay against a
provider endpoint.

## Qualification scenarios

Run the crate's maintained checks from the SDK workspace with
`cargo test -p acyclic-stream --all-features --locked`. The executable
`examples/http-conformance.rs` accepts an endpoint and runs the canonical
conformance suite plus authentication, bounds, commit-envelope, and deadline
checks. `examples/token-operations.rs` emits the generated token-operation
inventory used by SDK adapters.

The Rust API, generated `FILE_DESCRIPTOR_SET`, `conformance::verify`, and the
provider's durable qualification evidence are the sources for exact limits.
