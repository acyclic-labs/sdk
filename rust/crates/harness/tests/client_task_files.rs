//! The task projection must equal the selected files, with no payload copies.
use acyclic_harness::{
    client::{ClientViewLimits, ClientViews, QueryViewLimits, TaskQueryView},
    task_volume::{
        Channel, Component, FileKey, HeadFile, Query, TaskFile, TaskVolume, ToolFile, Write,
    },
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::StreamExt as _;
use proptest::prelude::*;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy)]
enum Fault {
    None,
    Foreign,
    TextIdentity,
}

fn client() -> ClientViews {
    ClientViews::new(
        StreamPath::new("swarm").unwrap(),
        ClientViewLimits {
            agents: 0,
            selected: 0,
            overview_values: 0,
            overview_bytes: 0,
            query: QueryViewLimits {
                files: 0,
                segments: 0,
                bytes: 0,
            },
        },
    )
    .unwrap()
}
fn writes(rows: &[(u8, u8, u8, u8)]) -> BTreeMap<FileKey, Write> {
    let mut files = BTreeMap::new();
    for &(owner, turn, sequence, kind) in rows {
        let task = Component::new(["a", "aa", "z"][usize::from(owner) % 3]).unwrap();
        let turn = if turn == 255 {
            u64::MAX
        } else {
            u64::from(turn)
        };
        let sequence = u64::from(sequence);
        let mut channel = None;
        let file = match kind % 8 {
            5 => TaskFile::Completion(turn),
            6 | 7 => {
                channel = Some(if kind % 8 == 6 {
                    Channel::Text
                } else {
                    Channel::Reasoning
                });
                TaskFile::Chunk {
                    execution: acyclic_harness::OperationId::from_bytes([0; 16]),
                    turn,
                    sequence,
                }
            }
            kind => TaskFile::Tool {
                turn,
                call: Component::new("call").unwrap(),
                file: match kind {
                    0 => ToolFile::Request,
                    1 => ToolFile::Arguments(sequence),
                    2 => ToolFile::Stdout(sequence),
                    3 => ToolFile::Stderr(sequence),
                    _ => ToolFile::Result,
                },
            },
        };
        let mut write = Write::new(
            FileKey::Task { task, file },
            vec![owner, kind, sequence as u8],
        );
        write.channel = channel;
        files.insert(write.key.clone(), write);
    }
    files
}
fn same(actual: Vec<&HeadFile>, expected: Vec<&HeadFile>) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected)
            .all(|(a, e)| std::ptr::eq(*a, e))
}
fn check(view: TaskQueryView<'_>, source: &BTreeMap<FileKey, HeadFile>, fault: Fault) -> bool {
    let expected: Vec<_> = source
        .values()
        .filter(|file| {
            matches!(&file.key,
        FileKey::Task {task,..} if task == view.task)
        })
        .collect();
    let actual = if matches!(fault, Fault::Foreign) {
        source.values().collect()
    } else {
        view.files().collect()
    };
    if !same(actual, expected.clone()) {
        return false;
    }
    let actual_text: Vec<_> = view
        .text()
        .map(|mut text| {
            if matches!(fault, Fault::TextIdentity) {
                text.turn = text.turn.wrapping_add(1);
            }
            text
        })
        .collect();
    if actual_text.iter().any(|text| match &text.file.key {
        FileKey::Task {
            file: TaskFile::Chunk {
                execution, turn, ..
            },
            ..
        } => {
            text.execution != *execution
                || text.turn != *turn
                || Some(text.channel) != text.file.channel
        }
        FileKey::Task {
            file:
                TaskFile::TextHead {
                    execution,
                    turn,
                    channel,
                },
            ..
        } => text.execution != *execution || text.turn != *turn || text.channel != *channel,
        _ => true,
    }) {
        return false;
    }
    if view.completions().any(|(turn, file)| {
        !matches!(&file.key,
        FileKey::Task {file:TaskFile::Completion(expected),..} if *expected==turn)
    }) {
        return false;
    }
    let text: Vec<_> = expected
        .iter()
        .copied()
        .filter(|head| {
            matches!(
                head.key,
                FileKey::Task {
                    file: TaskFile::Chunk { .. } | TaskFile::TextHead { .. },
                    ..
                }
            )
        })
        .collect();
    if !same(actual_text.into_iter().map(|v| v.file).collect(), text) {
        return false;
    }
    let completion: Vec<_> = expected
        .iter()
        .copied()
        .filter(|head| {
            matches!(
                head.key,
                FileKey::Task {
                    file: TaskFile::Completion(_),
                    ..
                }
            )
        })
        .collect();
    if !same(view.completions().map(|(_, v)| v).collect(), completion) {
        return false;
    }
    let mut tools = BTreeMap::<_, Vec<_>>::new();
    for head in expected {
        if let FileKey::Task {
            file: TaskFile::Tool { turn, call, .. },
            ..
        } = &head.key
        {
            tools.entry((*turn, call.as_str())).or_default().push(head);
        }
    }
    let actual: Vec<_> = view.tools().collect();
    if actual.len() != tools.len() {
        return false;
    }
    for tool in actual {
        let Some(expected) = tools.remove(&(tool.turn, tool.call.as_str())) else {
            return false;
        };
        for (kind, actual) in [
            (0, tool.request().into_iter().collect()),
            (1, tool.arguments().collect()),
            (2, tool.stdout().collect()),
            (3, tool.stderr().collect()),
            (4, tool.result().into_iter().collect()),
        ] {
            let expected = expected
                .iter()
                .copied()
                .filter(|head| {
                    matches!(&head.key,
                    FileKey::Task {file:TaskFile::Tool {file,..},..} if match file {
                        ToolFile::Request=>kind==0, ToolFile::Arguments(_)=>kind==1,
                        ToolFile::Stdout(_)=>kind==2, ToolFile::Stderr(_)=>kind==3,
                        ToolFile::Result=>kind==4,
                    })
                })
                .collect();
            if !same(actual, expected) {
                return false;
            }
        }
    }
    tools.is_empty()
}
async fn exercise(rows: &[(u8, u8, u8, u8)], fault: Fault) -> bool {
    let provider = Arc::new(MemoryStream::default());
    let volume = TaskVolume::open(provider.clone(), StreamPath::new("tasks/files").unwrap())
        .await
        .unwrap();
    let mut observation = volume.observe(Query::Root, None, 1024).await.unwrap();
    let mut views = client();
    views
        .query_snapshot(
            Query::Root,
            observation.snapshot.take().unwrap(),
            1024,
            1_048_576,
        )
        .unwrap();
    let task = Component::new("a").unwrap();
    for write in writes(rows).into_values() {
        volume
            .append(volume.head().await.unwrap(), &write)
            .await
            .unwrap();
        views
            .query_change(observation.changes.next().await.unwrap().unwrap())
            .unwrap();
        let (key, files) = views.selections().next().unwrap();
        if !check(views.task(key, &task).unwrap(), files.files(), fault) {
            return false;
        }
    }
    let reopened = TaskVolume::open_existing(provider, volume.path().clone())
        .await
        .unwrap();
    let snapshot = reopened.snapshot(&Query::Root, 1024).await.unwrap();
    let mut restored = client();
    restored
        .query_snapshot(Query::Root, snapshot, 1024, 1_048_576)
        .unwrap();
    views == restored
}
proptest! {
    // Every projected folder equals the independent task/file-key partition;
    // numeric ordering, missing requests and same call IDs on other turns are retained.
    #[test]
    fn task_folders_preserve_the_exact_file_partition(
        rows in proptest::collection::vec((any::<u8>(),any::<u8>(),any::<u8>(),any::<u8>()),0..32),
    ) {
        prop_assert!(futures::executor::block_on(exercise(&rows,Fault::None)));
    }
}
#[test]
fn numeric_chunks_missing_requests_and_full_width_turns_survive_reopen() {
    assert!(futures::executor::block_on(exercise(
        &[
            (0, 255, 10, 2),
            (0, 255, 2, 2),
            (0, 255, 1, 3),
            (0, 255, 0, 4),
            (0, 2, 0, 0),
            (0, 10, 0, 0),
            (0, 10, 0, 5),
            (0, 2, 0, 5),
            (0, 255, 0, 6),
            (0, 255, 1, 7),
            (1, 255, 0, 0),
        ],
        Fault::None
    )));
}
#[test]
fn negative_control_including_another_tasks_files_is_rejected() {
    assert!(!futures::executor::block_on(exercise(
        &[(1, 0, 0, 0), (0, 0, 1, 2)],
        Fault::Foreign
    )));
}

#[test]
fn negative_control_changing_text_identity_is_rejected() {
    assert!(!futures::executor::block_on(exercise(
        &[(0, 255, 0, 6)],
        Fault::TextIdentity
    )));
}
