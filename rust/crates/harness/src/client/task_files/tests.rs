use super::*;
use crate::{
    OperationId,
    task_volume::{Channel, Component, FileKey, FileValue, TaskFile, TaskVolume},
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::FutureExt as _;
use proptest::prelude::*;
use std::sync::Arc;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    // Snapshot plus follow equals reopening, independent of byte boundaries,
    // invalid UTF-8, channel and idempotent re-publication.
    #[test]
    fn task_files_snapshot_follow_and_reopen_share_the_same_views(
        chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 1..16), 1..7),
        cut in 0usize..7, reasoning in any::<bool>(),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let volume = TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("file-client").unwrap()).await.unwrap();
            let task = crate::TaskId::from_bytes([1;16]);
            let owner = wire::Authority { kind: wire::AggregateKind::Task as i32, id: task.to_string() };
            let key = format!("AGGREGATE_KIND_TASK:{task}");
            let mut generation = volume.snapshot(&Query::Tasks, 32).await.unwrap().generation;
            let files: Vec<_> = chunks.iter().enumerate().map(|(index, bytes)| FileValue {
                requester: None, publication: None,
                key: FileKey::Task { task: Component::new(task.to_string()).unwrap(), file: TaskFile::Chunk { execution: crate::OperationId::from_bytes([0; 16]),  turn: 0, sequence: index as u64 } },
                channel: Some(if reasoning { Channel::Reasoning } else { Channel::Text }), bytes: bytes.clone().into(),
            }).collect();
            let cut = cut.min(files.len());
            for file in &files[..cut] { generation = volume.append(generation, file).await.unwrap(); }
            let mut live = TaskFileClient::observe(&volume, &owner, 32, 4096).await.unwrap();
            for file in &files[cut..] {
                generation = volume.append(generation, file).await.unwrap();
                tokio::task::unconstrained(live.next_change()).now_or_never().expect("accepted cut must be ready").unwrap();
                prop_assert_eq!(volume.append(generation, file).await.unwrap(), generation);
                prop_assert!(tokio::task::unconstrained(live.next_change()).now_or_never().is_none());
            }
            let replay = TaskFileClient::observe(&volume, &owner, 32, 4096).await.unwrap();
            prop_assert_eq!(&live.views().transcripts, &replay.views().transcripts);
            let bytes: Vec<u8> = chunks.into_iter().flatten().collect();
            let transcript = &live.views().transcripts[&key];
            let text = if reasoning { &transcript.reasoning } else { &transcript.streaming_text };
            let expected = String::from_utf8_lossy(&bytes);
            prop_assert_eq!(text.as_str(), expected.as_ref());
            prop_assert_eq!(transcript.streaming_turn, Some(OperationId::from_bytes([0;16])));
            Ok(())
        })?;
    }
}

async fn foreign_requests_and_resets(identity: [u8; 16]) {
    let mut volumes = Vec::new();
    let mut owners = Vec::new();
    for number in 1..=2 {
        let volume = TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new(format!("volume-{number}")).unwrap(),
        )
        .await
        .unwrap();
        let owner = wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: crate::TaskId::from_bytes(if number == 1 {
                identity
            } else {
                let mut other = identity;
                other[0] ^= 1;
                other
            })
            .to_string(),
        };
        let empty = volume.snapshot(&Query::Tasks, 16).await.unwrap();
        volume
            .append(
                empty.generation,
                &FileValue {
                    requester: None,
                    publication: None,
                    key: FileKey::Task {
                        task: Component::new(&owner.id).unwrap(),
                        file: TaskFile::Chunk {
                            execution: crate::OperationId::from_bytes([0; 16]),
                            turn: 0,
                            sequence: 0,
                        },
                    },
                    channel: Some(Channel::Text),
                    bytes: vec![b'a' + number].into(),
                },
            )
            .await
            .unwrap();
        owners.push(owner);
        volumes.push((volume, empty));
    }
    let mut client = TaskFileClient::observe(&volumes[0].0, &owners[0], 16, 4096)
        .await
        .unwrap();
    client
        .session
        .query_snapshot(
            Query::Tasks,
            volumes[1].0.snapshot(&Query::Tasks, 16).await.unwrap(),
            16,
            4096,
        )
        .unwrap();
    assert_eq!(client.views().transcripts.len(), 2);
    let first = format!("AGGREGATE_KIND_TASK:{}", owners[0].id);
    let second = format!("AGGREGATE_KIND_TASK:{}", owners[1].id);
    let foreign = client.views().transcripts[&second].clone();
    let (volume, empty) = volumes.remove(0);
    let cut = volume.snapshot(&Query::Tasks, 16).await.unwrap();
    // A caller may submit a request naming another task. That editable input
    // cannot take ownership of the other volume's rendered text.
    volume
        .append(
            cut.generation,
            &FileValue::new(
                FileKey::Task {
                    task: Component::new(&owners[1].id).unwrap(),
                    file: TaskFile::Request(Component::new("foreign-request").unwrap()),
                },
                b"request".to_vec(),
            ),
        )
        .await
        .unwrap();
    client.next_change().await.unwrap();
    assert_eq!(client.views().transcripts[&second], foreign);
    let cut = volume.snapshot(&Query::Tasks, 16).await.unwrap();
    volume
        .append(
            cut.generation,
            &FileValue {
                requester: None,
                publication: None,
                key: FileKey::Task {
                    task: Component::new(&owners[0].id).unwrap(),
                    file: TaskFile::Chunk {
                        execution: crate::OperationId::from_bytes([0; 16]),
                        turn: 0,
                        sequence: 1,
                    },
                },
                channel: Some(Channel::Text),
                bytes: b"!".to_vec().into(),
            },
        )
        .await
        .unwrap();
    client.next_change().await.unwrap();
    assert_eq!(client.views().transcripts[&first].streaming_text, "b!");
    assert_eq!(client.views().transcripts[&second], foreign);
    client
        .session
        .query_snapshot(Query::Tasks, empty, 16, 4096)
        .unwrap();
    assert!(!client.views().transcripts.contains_key(&first));
    assert_eq!(client.views().transcripts[&second], foreign);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    // For arbitrary distinct task identities, caller requests and snapshot
    // resets cannot replace another followed volume's projected transcript.
    #[test]
    fn changing_and_resetting_one_volume_preserves_other_task_views(identity in any::<[u8;16]>()) {
        futures::executor::block_on(foreign_requests_and_resets(identity));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    // Command outcomes fold in committed order, independent of request UUID
    // ordering, and snapshot/follow/reopen preserve the same visible text.
    #[test]
    fn completed_command_text_round_trips_through_task_views(
        messages in prop::collection::vec(".{0,48}", 1..5),
    ) {
        futures::executor::block_on(async {
            let volume = TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("completed-commands").unwrap()).await.unwrap();
            let task = crate::TaskId::new();
            let owner = crate::wire::Authority { kind: crate::wire::AggregateKind::Task as i32, id: task.to_string() };
            let key = format!("AGGREGATE_KIND_TASK:{task}");
            let cut = volume.snapshot(&Query::Tasks, 32).await.unwrap();
            volume.append(cut.generation, &FileValue {
                requester: None, publication: None,
                key: FileKey::Task { task: Component::new(task.to_string()).unwrap(), file: TaskFile::Chunk { execution: crate::OperationId::from_bytes([0; 16]),  turn: 0, sequence: 0 } },
                channel: Some(Channel::Reasoning), bytes: b"reasoning".to_vec().into(),
            }).await.unwrap();
            let mut followed = TaskFileClient::observe(&volume, &owner, 32, 16_384).await.unwrap();
            let mut expected = String::new();
            for (index, message) in messages.iter().enumerate() {
                let file = FileValue::new(FileKey::Task {
                    task: Component::new(task.to_string()).unwrap(),
                    file: TaskFile::ExecutionResult(Component::new(OperationId::from_bytes((u128::MAX - index as u128).to_be_bytes()).to_string()).unwrap()),
                }, serde_json::to_vec(&serde_json::json!({"sequence":index + 1,"ok":{"text":message}})).unwrap());
                let cut = volume.snapshot(&Query::Tasks, 32).await.unwrap();
                volume.append(cut.generation, &file).await.unwrap();
                followed.next_change().await.unwrap();
                if !expected.is_empty() { expected.push_str("\n\n"); }
                expected.push_str(message);
                assert_eq!(followed.views().transcripts[&key].streaming_text, expected);
                assert_eq!(followed.views().transcripts[&key].reasoning, "reasoning");
                let reopened = TaskFileClient::observe(&volume, &owner, 32, 16_384).await.unwrap();
                assert_eq!(reopened.views(), followed.views());
            }
        });
    }
}

#[tokio::test]
async fn current_task_tail_ignores_large_request_and_unrelated_task_archives() {
    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("bounded-current-task").unwrap(),
    )
    .await
    .unwrap();
    let task = crate::TaskId::new();
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: task.to_string(),
    };
    let current = Component::new(task.to_string()).unwrap();
    let foreign = Component::new(crate::TaskId::new().to_string()).unwrap();
    let mut generation = volume.snapshot(&Query::Root, 1).await.unwrap().generation;
    for target in [current.clone(), foreign] {
        let file = FileValue::new(
            FileKey::Task {
                task: target,
                file: TaskFile::Request(Component::new("large").unwrap()),
            },
            vec![b'x'; 40_000],
        );
        generation = volume.append(generation, &file).await.unwrap();
    }
    let mut client = TaskFileClient::observe_task(&volume, &owner, task, 16, 128)
        .await
        .unwrap();
    let file = FileValue {
        requester: None,
        publication: None,
        key: FileKey::Task {
            task: current,
            file: TaskFile::Chunk {
                execution: crate::OperationId::from_bytes([0; 16]),
                turn: 0,
                sequence: 0,
            },
        },
        channel: Some(Channel::Text),
        bytes: b"visible".to_vec().into(),
    };
    volume.append(generation, &file).await.unwrap();
    client.next_change().await.unwrap();
    assert_eq!(
        client.views().transcripts[&format!("AGGREGATE_KIND_TASK:{task}")].streaming_text,
        "visible"
    );
    assert_eq!(
        client
            .views()
            .queries
            .values()
            .map(|query| query.files().len())
            .sum::<usize>(),
        1
    );
}

#[cfg(all(feature = "tui", feature = "terminal"))]
mod retention;

async fn checkpoint_model<const BROKEN: bool>() {
    use futures::StreamExt as _;
    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("checkpoint-frame").unwrap(),
    )
    .await
    .unwrap();
    let task = crate::TaskId::from_bytes([34; 16]);
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: task.to_string(),
    };
    let mut client = TaskFileClient::observe(&volume, &owner, 128, 65536)
        .await
        .unwrap();
    let revision = client.presentation_revision();
    let ignored = FileValue::new(FileKey::Goal, b"unrelated".to_vec());
    volume
        .append(volume.head().await.unwrap(), &ignored)
        .await
        .unwrap();
    client.next_change().await.unwrap();
    if BROKEN {
        client.advance_presentation();
    }
    assert_eq!(
        client.presentation_revision(),
        revision,
        "checkpoints must not invalidate presentation"
    );
    assert!(!client.commands_dirty);
    // The alternate File input path consumes another real checkpoint as well.
    volume
        .append(
            volume.head().await.unwrap(),
            &FileValue::new(FileKey::Goal, b"other".to_vec()),
        )
        .await
        .unwrap();
    let change = client.changes.next().await.unwrap().unwrap();
    client
        .apply_update(super::super::ViewUpdate::File(change))
        .unwrap();
    assert_eq!(client.presentation_revision(), revision);
    let other = FileValue::new(
        FileKey::Task {
            task: Component::new(task.to_string()).unwrap(),
            file: TaskFile::Request(Component::new("draft").unwrap()),
        },
        b"draft".to_vec(),
    );
    client
        .apply_update(super::super::ViewUpdate::DocumentFile {
            authority: owner.clone(),
            file: other.clone(),
        })
        .unwrap();
    let cursor = client
        .views()
        .queries
        .values()
        .next()
        .unwrap()
        .cursor()
        .clone();
    client
        .apply_update(super::super::ViewUpdate::DocumentChange {
            authority: owner.clone(),
            change: crate::task_volume::Change {
                file: Some(other),
                files: vec![],
                removed: vec![],
                cursor,
            },
        })
        .unwrap();
    assert_eq!(
        client.presentation_revision(),
        revision,
        "non-document writes must not invalidate presentation"
    );
    client
        .apply_update(super::super::ViewUpdate::DocumentFile {
            authority: owner,
            file: FileValue::new(FileKey::Goal, b"visible goal".to_vec()),
        })
        .unwrap();
    assert!(client.presentation_revision() > revision);
}

#[test]
fn filtered_checkpoints_and_non_document_writes_preserve_the_render_cut() {
    futures::executor::block_on(checkpoint_model::<false>());
}

#[test]
#[should_panic(expected = "checkpoints must not invalidate presentation")]
fn checkpoint_invalidation_negative_control() {
    futures::executor::block_on(checkpoint_model::<true>());
}
