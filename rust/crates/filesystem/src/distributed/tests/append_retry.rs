//! Prepared commit races exercise the production retry-slot lookup and provider locks.
use super::*;
use futures::FutureExt as _;
use std::{cell::Cell, future::Future};

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;
loom::thread_local! { static BROKEN: Cell<bool> = Cell::new(false); }

pub(in crate::distributed) fn reuse_operation_key() -> bool {
    BROKEN.with(Cell::get)
}

fn ready<F: Future>(future: F) -> F::Output {
    // No other writer exists during preparation or after its join. Assert an
    // immediate completion without adding executor wakeups to those cuts.
    Box::pin(future)
        .now_or_never()
        .expect("uncontended memory-provider call suspended")
}

fn concurrent<F: Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

fn record(authority: AuthorityId, head: Head, id: u8) -> DurableCommit {
    let proposed = proposed(id);
    let mut record = DurableCommit {
        epoch: head.epoch,
        sequence: head.sequence.checked_next().unwrap(),
        operation_id: proposed.operation_id,
        fingerprint: proposed.fingerprint,
        previous_digest: head.digest,
        digest: Digest::ZERO,
        payload: proposed.payload,
        settled_at_micros: None,
    };
    record.digest = authority_commit_digest(
        authority,
        record.epoch,
        record.sequence,
        record.operation_id,
        record.fingerprint,
        record.previous_digest,
        &record.payload,
    );
    record
}

fn request(authority: AuthorityId, record: &DurableCommit, key: IdempotencyKey) -> CommitRequest {
    let path = stream_path(authority, "/records").unwrap();
    CommitRequest {
        conditions: vec![CommitCondition::Tail {
            path: path.clone(),
            expected: record.sequence.get(),
        }],
        mutations: vec![CommitMutation::Append {
            path,
            records: vec![
                StreamsDurableRecord::encode(record, STREAM_RECORD_LIMIT)
                    .unwrap()
                    .into(),
            ],
        }],
        idempotency_key: key,
    }
}

fn scenario(broken: bool, same_operation: bool) {
    BROKEN.with(|fault| fault.set(broken));
    let stream = Arc::new(MemoryStream::default());
    let store = StreamAuthorityStore::new(Arc::clone(&stream));
    let authority = AuthorityId::from_bytes([71; 16]);
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
    // Preparation is deterministic and precedes the only concurrent mutation.
    // Use the production constructor without unrelated reopen/head reads.
    let mut creation = CommitRequest {
        conditions: work.vec(4).unwrap(),
        mutations: work.vec(4).unwrap(),
        idempotency_key: stream_key(b"fixture", b"authority").unwrap(),
    };
    store
        .creation_request(
            &mut creation,
            authority,
            Epoch::GENESIS,
            None,
            None,
            &mut work,
        )
        .unwrap();
    assert!(matches!(
        ready(stream.commit(creation)).unwrap(),
        CommitOutcome::Committed(_)
    ));
    let head = Head::genesis(Epoch::GENESIS);
    let own = record(authority, head, 73);
    let peer = record(authority, head, if same_operation { 73 } else { 72 });
    let (_, key, existing) =
        ready(store.operation_attempt(authority, own.operation_id, &mut work)).unwrap();
    assert!(existing.is_none());
    let mut own_request = request(authority, &own, key);
    own_request.conditions.extend([
        CommitCondition::Absent {
            path: stream_path(authority, "/retirement").unwrap(),
        },
        CommitCondition::Tail {
            path: stream_path(authority, "/epochs").unwrap(),
            expected: 1,
        },
        CommitCondition::Tail {
            path: stream_path(authority, "/publication-gate").unwrap(),
            expected: 1,
        },
    ]);
    let peer_request = request(
        authority,
        &peer,
        super::super::replay::operation_key(authority, peer.operation_id, 0, &mut work).unwrap(),
    );
    let writer = Arc::clone(&stream);
    let writer = loom::thread::spawn(move || concurrent(writer.commit(peer_request)));
    let first = concurrent(stream.commit(own_request));
    let other = writer.join().unwrap();
    assert!(matches!(
        first,
        Ok(_) | Err(StreamError::IdempotencyMismatch)
    ));
    assert!(matches!(
        other,
        Ok(_) | Err(StreamError::IdempotencyMismatch)
    ));
    // All commit orderings are explored above. Settlement is now sequential;
    // repeat the real slot lookup rather than rebuilding the task/runtime.
    let (_, key, committed) =
        ready(store.operation_attempt(authority, own.operation_id, &mut work)).unwrap();
    if committed.is_none() {
        let rebased = record(
            authority,
            Head {
                epoch: peer.epoch,
                sequence: peer.sequence,
                digest: peer.digest,
            },
            73,
        );
        let result = ready(stream.commit(request(authority, &rebased, key)));
        assert!(
            matches!(result, Ok(CommitOutcome::Committed(_))),
            "rebased authority append did not commit: {result:?}"
        );
    }
    let (_, _, retained) =
        ready(store.operation_attempt(authority, own.operation_id, &mut work)).unwrap();
    assert_eq!(retained.unwrap().fingerprint, own.fingerprint);
    let mut records = ready(stream.read(ReadRequest {
        path: stream_path(authority, "/records").unwrap(),
        from: 1,
        limit: 3,
    }))
    .unwrap();
    let mut count = 0;
    while let Some(record) = ready(records.next()) {
        let record = record.unwrap();
        let durable = work.decode_durable(&record.value).unwrap();
        count += usize::from(durable.operation_id == own.operation_id);
    }
    assert_eq!(count, 1);
    work.spent()
        .verify(crate::performance::BOUNDARY_BUDGET)
        .unwrap();
}

#[test]
fn a_lost_cas_does_not_poison_the_rebased_operation_identity() {
    for same_operation in [false, true] {
        loom::model(move || scenario(false, same_operation));
    }
    let error = std::panic::catch_unwind(|| loom::model(|| scenario(true, false)))
        .expect_err("reusing the failed attempt identity was accepted");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("rebased authority append did not commit"),
        "{message}"
    );
}
