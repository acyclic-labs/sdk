use super::*;
use crate::{
    OperationId,
    task_volume::{Channel, Component, FileKey, FileValue, TaskFile, TaskVolume},
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::FutureExt as _;
use proptest::prelude::*;
use std::sync::Arc;
fn selection(client: &TaskFileClient) -> &super::super::FileQueryView {
    client.views().selection(&client.task_selection).unwrap()
}
fn joined_text(client: &TaskFileClient, task: &str, reasoning: bool) -> String {
    let task = Component::new(task).unwrap();
    let bytes: Vec<_> = selection(client)
        .task(&task)
        .text()
        .filter(|text| {
            text.channel
                == if reasoning {
                    Channel::Reasoning
                } else {
                    Channel::Text
                }
        })
        .flat_map(|text| {
            text.file
                .segments
                .iter()
                .flat_map(|segment| segment.iter().copied())
        })
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}
fn same_files(left: &super::super::FileQueryView, right: &super::super::FileQueryView) {
    assert_eq!(left.files(), right.files());
    assert_eq!(left.cursor(), right.cursor());
}

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
            let mut generation = volume.snapshot(&Query::Tasks, 32).await.unwrap().generation;
            let files: Vec<_> = chunks.iter().enumerate().map(|(index, bytes)| FileValue {
                publication: None,
                key: FileKey::Task { task: Component::new(task.to_string()).unwrap(), file: TaskFile::Chunk { turn: 0, sequence: index as u64 } },
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
            same_files(selection(&live),selection(&replay));
            let bytes: Vec<u8> = chunks.into_iter().flatten().collect();
            let expected = String::from_utf8_lossy(&bytes);
            prop_assert_eq!(joined_text(&live,&owner.id,reasoning), expected.as_ref());
            prop_assert!(selection(&live).task(&Component::new(&owner.id).unwrap()).text().all(|text|text.turn==0));
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
                    publication: None,
                    key: FileKey::Task {
                        task: Component::new(&owner.id).unwrap(),
                        file: TaskFile::Chunk {
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
        .views
        .query_snapshot(
            Query::Tasks,
            volumes[1].0.snapshot(&Query::Tasks, 16).await.unwrap(),
            16,
            4096,
        )
        .unwrap();
    assert_eq!(client.views().selections().count(), 2);
    let foreign_identity = client
        .views()
        .selections()
        .find(|(id, _)| **id != client.task_selection)
        .unwrap()
        .0
        .to_owned();
    let foreign = client.views().selection(&foreign_identity).unwrap().clone();
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
    assert_eq!(
        client.views().selection(&foreign_identity).unwrap(),
        &foreign
    );
    let cut = volume.snapshot(&Query::Tasks, 16).await.unwrap();
    volume
        .append(
            cut.generation,
            &FileValue {
                publication: None,
                key: FileKey::Task {
                    task: Component::new(&owners[0].id).unwrap(),
                    file: TaskFile::Chunk {
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
    assert_eq!(joined_text(&client, &owners[0].id, false), "b!");
    assert_eq!(
        client.views().selection(&foreign_identity).unwrap(),
        &foreign
    );
    client
        .views
        .query_snapshot(Query::Tasks, empty, 16, 4096)
        .unwrap();
    assert!(selection(&client).files().is_empty());
    assert_eq!(
        client.views().selection(&foreign_identity).unwrap(),
        &foreign
    );
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
            let cut = volume.snapshot(&Query::Tasks, 32).await.unwrap();
            volume.append(cut.generation, &FileValue {
                publication: None,
                key: FileKey::Task { task: Component::new(task.to_string()).unwrap(), file: TaskFile::Chunk { turn: 0, sequence: 0 } },
                channel: Some(Channel::Reasoning), bytes: b"reasoning".to_vec().into(),
            }).await.unwrap();
            let mut followed = TaskFileClient::observe(&volume, &owner, 32, 16_384).await.unwrap();
            for (index, message) in messages.iter().enumerate() {
                let file = FileValue::new(FileKey::Task {
                    task: Component::new(task.to_string()).unwrap(),
                    file: TaskFile::ExecutionResult(Component::new(OperationId::from_bytes((u128::MAX - index as u128).to_be_bytes()).to_string()).unwrap()),
                }, serde_json::to_vec(&serde_json::json!({"sequence":index + 1,"ok":{"text":message}})).unwrap());
                let cut = volume.snapshot(&Query::Tasks, 32).await.unwrap();
                volume.append(cut.generation, &file).await.unwrap();
                followed.next_change().await.unwrap();
                let expected=messages[..=index].join("\n\n");
                let output_owner=super::super::OutputOwner::Task(Component::new(&owner.id).unwrap());
                let actual=selection(&followed).outputs(&output_owner,0).map(|(_,row)|row.text.unwrap().into_owned()).collect::<Vec<_>>().join("\n\n");
                assert_eq!(actual,expected);
                assert_eq!(joined_text(&followed,&owner.id,true), "reasoning");
                let reopened = TaskFileClient::observe(&volume, &owner, 32, 16_384).await.unwrap();
                same_files(selection(&reopened),selection(&followed));
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
        publication: None,
        key: FileKey::Task {
            task: current,
            file: TaskFile::Chunk {
                turn: 0,
                sequence: 0,
            },
        },
        channel: Some(Channel::Text),
        bytes: b"visible".to_vec().into(),
    };
    volume.append(generation, &file).await.unwrap();
    client.next_change().await.unwrap();
    assert_eq!(joined_text(&client, &owner.id, false), "visible");
    assert_eq!(
        client
            .views()
            .selections()
            .map(|(_, query)| query.files().len())
            .sum::<usize>(),
        1
    );
}

#[cfg(all(feature = "tui", feature = "terminal"))]
mod retention;

async fn replacement_sequence(tasks: &[u8], retain_old: bool) {
    let volume = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("selected").unwrap(),
    )
    .await
    .unwrap();
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: crate::TaskId::from_bytes([99; 16]).to_string(),
    };
    let mut client = TaskFileClient::observe(&volume, &owner, 16, 4096)
        .await
        .unwrap();
    let goal = volume
        .snapshot(&Query::File(FileKey::Goal), 1)
        .await
        .unwrap();
    let goal_identity = (goal.cursor.volume, goal.cursor.query);
    client
        .views
        .query_snapshot(Query::File(FileKey::Goal), goal, 1, 4096)
        .unwrap();
    for number in tasks {
        let task = crate::TaskId::from_bytes([*number; 16]);
        let before = client
            .views()
            .selection(&client.task_selection)
            .unwrap()
            .clone();
        let old_query = before.query().clone();
        let old_snapshot = volume.snapshot(&old_query, 16).await.unwrap();
        client.follow_task(&volume, task).await.unwrap();
        if retain_old && old_query != TaskFileClient::task_query(task).unwrap() {
            client
                .views
                .query_snapshot(old_query, old_snapshot, 16, 4096)
                .unwrap();
        }
        let selected: Vec<_> = client
            .views()
            .selections()
            .filter(|(_, files)| !matches!(files.query(), Query::File(FileKey::Goal)))
            .collect();
        assert_eq!(
            selected.len(),
            1,
            "retired query must release all its files"
        );
        assert_eq!(
            selected[0].1.query(),
            &TaskFileClient::task_query(task).unwrap()
        );
        assert!(
            client.views().selection(&goal_identity).is_some(),
            "unrelated document survives"
        );
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn selected_task_replacement_releases_only_the_old_query(tasks in prop::collection::vec(0u8..4,1..8)) {
        futures::executor::block_on(replacement_sequence(&tasks,false));
    }
}
#[test]
fn retained_task_selection_negative_control() {
    assert!(
        std::panic::catch_unwind(|| futures::executor::block_on(replacement_sequence(
            &[1, 2],
            true
        )))
        .is_err()
    );
}


async fn replace_ui_sessions(sessions: &[u8], retain: bool) {
    let volume=Arc::new(TaskVolume::open(Arc::new(MemoryStream::default()),StreamPath::new("sessions").unwrap()).await.unwrap());
    let owner=wire::Authority { kind:wire::AggregateKind::Task as i32,id:"root".into() };
    let mut client=TaskFileClient::observe(volume.as_ref(),&owner,128,65_536).await.unwrap();
    let mut previous=None;
    for number in sessions {
        let number=u128::from(*number);
        let commands=crate::client::CommandSession::open(volume.clone(),uuid::Uuid::from_u128(number),65_536,128).await.unwrap();
        let old=if let Some(identity)=previous {
            let query=client.views().selection(&identity).unwrap().query().clone();
            Some((identity,query.clone(),volume.snapshot(&query,128).await.unwrap()))
        } else { None };
        client.follow_commands(&commands).await.unwrap();
        if let Some((identity,query,snapshot))=old {
            if Some(identity)!=client.command_selection {
                if retain { client.views.query_snapshot(query,snapshot,128,65_536).unwrap(); }
                assert!(client.views().selection(&identity).is_none());
            }
        }
        let selection=client.views().selections().filter(|(_,files)|matches!(files.query(),Query::CommandSession { .. })).collect::<Vec<_>>();
        assert_eq!(selection.len(),1);
        assert_eq!(selection[0].1.query(), &Query::CommandSession {session:Component::new(uuid::Uuid::from_u128(number).to_string()).unwrap(),recent:32});
        assert!(client.views().selection(&client.task_selection).is_some());
        previous=Some(*selection[0].0);
    }
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    // Rebinding replaces the whole command selection while the task follow
    // remains intact; reopening the same session never creates a second slot.
    #[test]
    fn replacing_the_ui_session_retires_its_previous_projection(sessions in prop::collection::vec(1u8..5,1..7)) {
        futures::executor::block_on(replace_ui_sessions(&sessions,false));
    }
}
#[test]
fn retained_ui_session_projection_negative_control() {
    futures::executor::block_on(replace_ui_sessions(&[1,2,1],false));
    assert!(std::panic::catch_unwind(||futures::executor::block_on(replace_ui_sessions(&[1,2,1],true))).is_err());
}
