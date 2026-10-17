//! The actual selection publisher races a source edit or owner replacement.
use super::*;
use crate::task_volume::{Component, FileValue, TaskFile, model_checking};
use acyclic_stream::{
    AppendRequest, CommitCondition, CommitId, CommitMutation, CommitOutcome, CommitRequest,
    IdempotencyKey, MemoryStream, OwnedBytes, StreamPath,
};
use std::sync::Arc;

const _: fn(acyclic_stream::query_sync::SyncMutex<()>) -> loom::sync::Mutex<()> = |lock| lock;

fn scenario(broken: bool, change_owner: bool) {
    loom::future::block_on(Box::pin(async move {
        let provider = Arc::new(MemoryStream::default());
        let source = TaskVolume::open(provider.clone(), StreamPath::new("source").unwrap())
            .await
            .unwrap();
        let results = TaskVolume::open(provider.clone(), StreamPath::new("results").unwrap())
            .await
            .unwrap();
        let lease = StreamPath::new("lease").unwrap();
        provider
            .append(AppendRequest {
                path: lease.clone(),
                records: vec![OwnedBytes::from(b"original".to_vec())],
                if_tail: None,
                idempotency_key: None,
            })
            .await
            .unwrap();
        let session = "00000000-0000-0000-0000-000000000001";
        let sha256: [u8; 32] = Sha256::digest(b"x").into();
        let prefix = format!("workspace/native/{session}/{}", hex::encode(sha256));
        let selected = Selection {
            manifest: InputPath::try_from(format!("{prefix}/manifest.json")).unwrap(),
            head: None,
            sha256,
        };
        let chunk = FileValue::new(
            FileKey::parse(&format!("{prefix}/chunk-0")).unwrap(),
            b"x".to_vec(),
        );
        let manifest = FileValue::new(
            FileKey::Input(selected.manifest.clone()),
            br#"{"chunks":1,"length":1}"#.to_vec(),
        );
        for file in [&chunk, &manifest] {
            source
                .append(source.head().await.unwrap(), file)
                .await
                .unwrap();
        }
        let (_, source_head) = selected.read(&source, session, 1).await.unwrap();
        let generation = results.head().await.unwrap();
        let changed = if change_owner {
            b"replacement".to_vec().into()
        } else {
            FileValue::new(chunk.key.clone(), b"y".to_vec())
                .record_value(None)
                .unwrap()
        };
        let destination = if change_owner {
            lease.clone()
        } else {
            source.path().clone()
        };
        let destination_tail = if change_owner {
            1
        } else {
            source.head().await.unwrap().position
        };
        let competitor = provider.clone();
        let result_path = results.path().clone();
        // A successful edit is proven to precede pinning by its atomic guard
        // on the still-empty result stream. No auxiliary observer mutex.
        let edit = CommitRequest {
            conditions: vec![
                CommitCondition::Head {
                    path: result_path,
                    expected: generation.position,
                    commit_id: CommitId::from_bytes(generation.commit),
                },
                // Every mutation must guard its own destination too.
                CommitCondition::Tail {
                    path: destination.clone(),
                    expected: destination_tail,
                },
            ],
            mutations: vec![CommitMutation::Append {
                path: destination,
                records: vec![changed],
            }],
            idempotency_key: IdempotencyKey::new(b"edit".to_vec()).unwrap(),
        };
        if broken {
            model_checking::set_fault(model_checking::Fault::OmitAuthorityConditions);
        }
        let file = FileValue::new(
            FileKey::Task {
                task: Component::new("task").unwrap(),
                file: TaskFile::Context(Component::new("selection").unwrap()),
            },
            b"pinned".to_vec(),
        );
        let mut publication =
            Box::pin(
                selected.publish(&source, &results, (session, 1, source_head), &file, || {
                    std::future::ready(Ok((
                        CommitCondition::Tail {
                            path: lease.clone(),
                            expected: 1,
                        },
                        None,
                    )))
                }),
            );
        // The editor cannot change result preparation: it writes only the
        // source or lease. Reach the real provider boundary before spawning
        // it. Both atomic commit orders, including an earlier edit, remain
        // exhaustive; unrelated serialization/index-lock interleavings vanish.
        model_checking::pause_next_publication();
        std::future::poll_fn(|cx| {
            assert!(publication.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        let editor = loom::thread::spawn(move || {
            loom::future::block_on(Box::pin(competitor.commit(edit))).unwrap()
        });
        let pinned = publication.await;
        let edited = editor.join().unwrap();
        assert!(
            !(matches!(edited, CommitOutcome::Committed(_)) && pinned.is_ok()),
            "pinned context after its source or owner changed"
        );
        let snapshot = results
            .snapshot(&Query::File(file.key.clone()), 1)
            .await
            .unwrap();
        assert_eq!(!snapshot.files.is_empty(), pinned.is_ok());
    }));
}

#[test]
fn source_and_owner_guards_are_atomic_with_negative_controls() {
    for owner in [false, true] {
        loom::model(move || scenario(false, owner));
        let error = std::panic::catch_unwind(move || loom::model(move || scenario(true, owner)))
            .expect_err("dropping source/owner conditions was accepted");
        let text = error
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| error.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(
            text.contains("pinned context after its source or owner changed"),
            "{text}"
        );
    }
}
