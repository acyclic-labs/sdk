//! Sequence mechanism adapted from #915, without its native-delta storage.
use super::*;
use crate::task_volume::{Channel, Component, FileKey, TaskFile};
use acyclic_stream::{CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey};

fn text(client: &crate::client::TaskFileClient, task: &Component) -> Vec<u8> {
    client
        .views()
        .selections()
        .next()
        .unwrap()
        .1
        .task(task)
        .text()
        .flat_map(|text| {
            text.file
                .segments
                .iter()
                .flat_map(|bytes| bytes.iter().copied())
        })
        .collect()
}
#[tokio::test]
async fn reserved_sequence_retries_and_reopens_across_reverse_sorted_executions() {
    let (f, journal, files, event) = fixture().await.unwrap();
    let task_id = journal.owner.as_ref().unwrap().0.task_id();
    let task = Component::new(task_id.to_string()).unwrap();
    let owner = crate::wire::Authority {
        kind: crate::wire::AggregateKind::Task as i32,
        id: task_id.to_string(),
    };
    // The fault adapter deliberately has no watch implementation. Observe its
    // same underlying provider through the real query-capable MemoryStream.
    let observed = crate::task_volume::TaskVolume::open(
        Arc::new(f.stream.inner.clone()),
        files.inner.path().clone(),
    )
    .await
    .unwrap();
    let mut live =
        crate::client::TaskFileClient::observe_task(&observed, &owner, task_id, 32, 65536)
            .await
            .unwrap();
    let named = files
        .inner
        .next_chunk_key(task.clone(), OP, 0, None)
        .await
        .unwrap();
    let peer = crate::OperationId::from_bytes([255; 16]);
    assert!(OP < peer);
    let key = files
        .inner
        .next_chunk_key(task.clone(), peer, 0, None)
        .await
        .unwrap();
    let prepared = files
        .inner
        .prepare_named_chunk(key, Channel::Text, b"peer", None)
        .await
        .unwrap()
        .unwrap();
    let request = CommitRequest {
        conditions: vec![prepared.condition],
        mutations: vec![CommitMutation::Append {
            path: prepared.path,
            records: vec![prepared.record],
        }],
        idempotency_key: IdempotencyKey::new(b"peer".to_vec()).unwrap(),
    };
    assert!(matches!(
        f.stream.commit(request).await.unwrap(),
        CommitOutcome::Committed(_)
    ));
    live.next_change().await.unwrap();
    let error = files
        .inner
        .prepare_named_chunk(named.clone(), Channel::Text, b"late", None)
        .await;
    assert!(
        matches!(error,Err(Error::TaskFiles(ref error)) if matches!(**error,crate::task_volume::Error::SequenceConflict(_)))
    );
    // Restoring unchecked preparation admits the stale reserved sequence.
    assert!(
        files
            .inner
            .prepare_journal_file(named, Channel::Text, b"late", None)
            .await
            .unwrap()
            .is_some()
    );
    journal
        .append(OP, "event:100", &event, None)
        .unwrap()
        .await
        .unwrap();
    live.next_change().await.unwrap();
    let before = files.inner.head().await.unwrap();
    journal
        .append(OP, "event:100", &event, None)
        .unwrap()
        .await
        .unwrap();
    assert_eq!(
        before,
        files.inner.head().await.unwrap(),
        "retry identity included a new publication sequence"
    );
    let reopened = observed.reopen().await.unwrap();
    let snapshot = reopened.snapshot(&Query::Root, 8).await.unwrap();
    let actual: Vec<_> = snapshot
        .files
        .iter()
        .map(|file| match file.key {
            FileKey::Task {
                file:
                    TaskFile::Chunk {
                        sequence,
                        execution,
                        ..
                    },
                ..
            } => (sequence, execution, file.bytes.to_vec()),
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(
        actual,
        [(0, peer, b"peer".to_vec()), (1, OP, b"working".to_vec())]
    );
    let replay = crate::client::TaskFileClient::observe_task(&reopened, &owner, task_id, 32, 65536)
        .await
        .unwrap();
    assert_eq!(text(&live, &task), b"peerworking");
    assert_eq!(text(&live, &task), text(&replay, &task));
}
