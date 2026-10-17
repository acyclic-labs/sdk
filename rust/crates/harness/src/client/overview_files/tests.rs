use super::*;
use crate::{
    TaskId,
    client::TaskFileViews,
    task_volume::{
        Component, FileKey, FileValue, TaskFile, TaskState, TaskStatus, TaskVolume, ToolFile,
    },
    test_support::measure_task_volume_work,
};
use acyclic_stream::{MemoryStream, StreamPath};
use acyclic_test_support::differential;
use futures::FutureExt as _;
use std::sync::Arc;

const PUBLICATIONS: u64 = 16;

fn status(task: TaskId, parent: Option<TaskId>) -> FileValue {
    let bytes = TaskStatus {
        task,
        parent,
        state: TaskState::Pending,
        attention: false,
        owner_agent: None,
    }
    .encode()
    .unwrap();
    FileValue::new(
        FileKey::Task {
            task: Component::new(task.to_string()).unwrap(),
            file: TaskFile::Status,
        },
        bytes,
    )
}

/// A transcript publication by the busy root agent; `STATUS` swaps in a
/// status publication by a fresh agent instead.
fn publication<const STATUS: bool>(index: u64) -> FileValue {
    let root = TaskId::from_bytes([1; 16]);
    if STATUS {
        let mut bytes = [0xff; 16];
        bytes[..8].copy_from_slice(&index.to_le_bytes());
        return status(TaskId::from_bytes(bytes), Some(root));
    }
    FileValue::new(
        FileKey::Task {
            task: Component::new(root.to_string()).unwrap(),
            file: TaskFile::Tool {
                turn: 1,
                call: Component::new(format!("call-{index}")).unwrap(),
                file: ToolFile::Result,
            },
        },
        b"{}".to_vec(),
    )
}

/// Publishes `agents` statuses and follows them into the shared views, then
/// publishes `PUBLICATIONS` files. Returns the task-volume work (records and
/// index entries examined) and the recent-window entry operations of
/// following and applying every resulting update, and whether the fold's
/// revision moved.
fn follow<const STATUS: bool>(agents: u8) -> (u64, Vec<(&'static str, u64)>, bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let volume = TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("overview-follow").unwrap(),
        )
        .await
        .unwrap();
        for agent in 1..=agents {
            let parent = (agent > 1).then(|| TaskId::from_bytes([1; 16]));
            let file = status(TaskId::from_bytes([agent; 16]), parent);
            volume
                .append(volume.head().await.unwrap(), &file)
                .await
                .unwrap();
        }
        let mut updates = task_overview_files(&volume, 4096, 1 << 20).await.unwrap();
        let mut views = TaskFileViews::default();
        let Some(Ok(ViewUpdate::QuerySnapshot {
            query,
            snapshot,
            maximum_files,
            maximum_bytes,
        })) = updates.next().await
        else {
            panic!("overview snapshot first");
        };
        assert_eq!(snapshot.files.len(), usize::from(agents));
        views
            .query_snapshot(query, snapshot, maximum_files, maximum_bytes)
            .unwrap();
        let revision = |views: &TaskFileViews| views.queries.values().next().unwrap().revision();
        let before = revision(&views);
        let mut work = 0;
        let mut window = Vec::new();
        for index in 0..PUBLICATIONS {
            volume
                .append(volume.head().await.unwrap(), &publication::<STATUS>(index))
                .await
                .unwrap();
            let (((), examined), operations) = differential::observe::<_, _, (&'static str, u64)>(
                || (),
                |()| {
                    measure_task_volume_work(|| {
                        let Some(Ok(ViewUpdate::File(change))) = updates
                            .next()
                            .now_or_never()
                            .expect("in-memory follow is ready")
                        else {
                            panic!("overview change");
                        };
                        views.query_change(change).unwrap();
                    })
                },
            );
            work += examined;
            window.extend(operations);
        }
        (work, window, revision(&views) != before)
    })
}

/// The checked property: following unrelated publications costs the same
/// with few or many agents, touches no retained status entry, and never
/// moves the revision the tree keys on.
fn assert_follow_is_independent_of_agents<const STATUS: bool>() {
    let (few, few_window, revised) = follow::<STATUS>(4);
    let (many, many_window, _) = follow::<STATUS>(64);
    assert_eq!(
        (few_window, many_window),
        (vec![], vec![]),
        "unrelated publications touched the status window"
    );
    assert_eq!(few, many, "follow work grows with the number of agents");
    assert!(!revised, "unrelated publications moved the revision");
}

#[test]
fn transcript_publications_cost_the_same_with_many_agents() {
    assert_follow_is_independent_of_agents::<false>();
}

#[test]
#[should_panic(expected = "unrelated publications touched the status window")]
fn status_publications_touch_the_window_negative_control() {
    assert_follow_is_independent_of_agents::<true>();
}
