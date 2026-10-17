//! Real prepared task-file transactions and the provider's conflict cache.
use super::*;
use crate::task_volume::{Channel, Component, FileKey, Query, TaskFile, TaskVolume};
use acyclic_stream::{MemoryStream, StreamPath, StreamProvider};

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

/// Executes the real journal preparation/dispatch race in a Loom exploration.
pub fn verify_prepared_journal_identity(finalize: bool) {
    loom::future::block_on(Box::pin(async {
        let provider = Arc::new(MemoryStream::default());
        let volume = Arc::new(
            TaskVolume::open(provider.clone(), StreamPath::new("files").unwrap())
                .await
                .unwrap(),
        );
        let owner_path = StreamPath::new("owner").unwrap();
        provider
            .append(acyclic_stream::AppendRequest {
                path: owner_path.clone(),
                records: vec![b"owner".to_vec().into()],
                if_tail: None,
                idempotency_key: None,
            })
            .await
            .unwrap();
        let peer_file = volume
            .prepare_journal_file(
                FileKey::Task {
                    task: Component::new("t").unwrap(),
                    file: TaskFile::Chunk {
                        turn: 0,
                        execution: OperationId::from_bytes([3; 16]),
                        sequence: 0,
                    },
                },
                Channel::Text,
                b"unrelated",
                None,
            )
            .await
            .unwrap()
            .unwrap();
        // Prepare the peer's immutable record before spawning. Its actual
        // provider append races every poll of preparation and journal dispatch.
        let peer_provider = provider.clone();
        let peer = loom::thread::spawn(move || {
            loom::future::block_on(Box::pin(peer_provider.append(
                acyclic_stream::AppendRequest {
                    path: peer_file.path,
                    records: vec![peer_file.record],
                    if_tail: None,
                    idempotency_key: None,
                },
            )))
            .unwrap();
        });
        let path = StreamPath::new("execution").unwrap();
        let operation = OperationId::from_bytes([7; 16]);
        loop {
            let chunk = volume
                .prepare_journal_file(
                    FileKey::Task {
                        task: Component::new("t").unwrap(),
                        file: TaskFile::Chunk {
                            turn: 0,
                            execution: operation,
                            sequence: 0,
                        },
                    },
                    Channel::Text,
                    b"delta",
                    None,
                )
                .await
                .unwrap()
                .unwrap();
            let (key, held) = super::super::super::digest_key(&[1; 32], &mut None).unwrap();
            let mut work = None;
            let mut request = journal_request::<true>(
                key,
                held,
                acyclic_stream::CommitCondition::Tail {
                    path: owner_path.clone(),
                    expected: 1,
                },
                acyclic_stream::CommitCondition::Absent { path: path.clone() },
                path.clone(),
                b"observation".to_vec().into(),
                &mut work,
            )
            .unwrap();
            add_condition(&mut request, chunk.condition).unwrap();
            add_record(&mut request, chunk.path, chunk.record).unwrap();
            let result = if finalize {
                commit_journal(request, held, &provider, operation).await
            } else {
                commit_journal_impl::<_, false>(request, held, &provider, operation).await
            };
            assert!(
                result.is_ok(),
                "prepared retry reused a changed transaction identity: {result:?}"
            );
            if matches!(
                result.unwrap().0,
                acyclic_stream::CommitOutcome::Committed(_)
            ) {
                break;
            }
        }
        peer.join().unwrap();
        let snapshot = volume.snapshot(&Query::Root, 4).await.unwrap();
        assert_eq!(snapshot.files.len(), 2);
        // The same fenced refresh must install the scoped indexes atomically.
        for (execution, bytes) in [
            (operation, b"delta".as_slice()),
            (OperationId::from_bytes([3; 16]), b"unrelated".as_slice()),
        ] {
            let selected = volume
                .snapshot(
                    &Query::ExecutionChunks {
                        task: Component::new("t").unwrap(),
                        turn: 0,
                        execution,
                    },
                    1,
                )
                .await
                .unwrap();
            assert_eq!(selected.files.len(), 1);
            assert_eq!(selected.files[0].bytes.as_ref(), bytes);
        }
    }));
}
