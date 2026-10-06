# Stream Rust quickstart

`acyclic-stream` gives an application a typed handle to a permanent,
append-only path. The same [`StreamProvider`] contract is used by the in-memory
provider in tests, the durable local provider, and hosted transports.

```toml
# From the SDK source workspace; pin the package source used by the application.
[dependencies]
acyclic-stream = { path = "rust/crates/stream" }
bytes = "1"
futures = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Append and replay

This executable scenario appends a bounded batch with a tail precondition and
a stable retry identity, then replays the committed records from sequence zero.

```rust
use std::sync::Arc;
use acyclic_stream::{AppendOutcome, IdempotencyKey, MemoryStream, StreamClient};
use bytes::Bytes;
use futures::TryStreamExt;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = StreamClient::new(Arc::new(MemoryStream::default()));
    let events = client.stream("runs/example/events")?;
    let retry = IdempotencyKey::new(Bytes::from_static(b"runs-example-v1"))?;
    let outcome = events.append_batch(
        vec![Bytes::from_static(b"started"), Bytes::from_static(b"finished")],
        Some(0), Some(retry),
    ).await?;
    let AppendOutcome::Committed(receipt) = outcome else {
        return Err("the stream changed before this append".into());
    };
    assert_eq!((receipt.start, receipt.end, receipt.tail), (0, 2, 2));
    let records = events.read(0, 100).await?.try_collect::<Vec<_>>().await?;
    assert_eq!(records.iter().map(|record| record.value.as_ref()).collect::<Vec<_>>(),
        [b"started".as_slice(), b"finished".as_slice()]);
    Ok(())
}
```

Retrying the exact request with the same key returns the retained outcome. A
changed path, records, or tail condition with that key fails with
`StreamError::IdempotencyMismatch`; create a new key for a new logical
mutation. The cursor is exclusive: persist `record.sequence + 1` before
resuming a read.

## Coordinate paths in one commit

Use `commit` when several paths must change at one linearization point. Every
participant gets an explicit condition, and a conflict leaves all paths
unchanged.

```rust
use acyclic_stream::{CommitCondition, CommitMutation, CommitRequest, CommitOutcome,
    IdempotencyKey, MemoryStream, StreamClient, StreamPath};
use bytes::Bytes;
use std::sync::Arc;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = StreamClient::new(Arc::new(MemoryStream::default()));
let result = client.commit(CommitRequest {
    conditions: vec![
        CommitCondition::Absent { path: StreamPath::new("runs/example")? },
        CommitCondition::Absent { path: StreamPath::new("runs/example/audit")? },
    ],
    mutations: vec![
        CommitMutation::Append { path: StreamPath::new("runs/example")?, records: vec![Bytes::from_static(b"created")] },
        CommitMutation::Append { path: StreamPath::new("runs/example/audit")?, records: vec![Bytes::from_static(b"created-by-example")] },
    ],
    idempotency_key: IdempotencyKey::new(Bytes::from_static(b"create-run-v1"))?,
}).await?;
match result {
    CommitOutcome::Committed(envelope) => assert_eq!(envelope.mutations.len(), 2),
    CommitOutcome::Conflict(conflicts) => eprintln!("nothing changed: {conflicts:?}"),
}
# Ok(())
# }
```

`StreamProvider` implementations supply hosted or durable storage while the
append, replay, and commit APIs remain the same across providers.
