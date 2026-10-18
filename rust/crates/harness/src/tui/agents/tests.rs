use super::*;
use crate::{
    OperationId,
    client::{TaskFileClient, task_overview_files},
    task_volume::{Component, FileValue, TaskVolume},
    wire,
};
use acyclic_stream::{MemoryStream, StreamPath};
use futures::StreamExt as _;
use std::sync::Arc;

async fn append(volume: &TaskVolume<MemoryStream>, key: FileKey, bytes: Vec<u8>) {
    volume
        .append(volume.head().await.unwrap(), &FileValue::new(key, bytes))
        .await
        .unwrap();
}

fn status_key(task: TaskId) -> FileKey {
    FileKey::Task {
        task: Component::new(task.to_string()).unwrap(),
        file: TaskFile::Status,
    }
}

fn status(task: TaskId, parent: Option<TaskId>, state: TaskState, attention: bool) -> Vec<u8> {
    TaskStatus {
        task,
        parent,
        state,
        attention,
        owner_agent: None,
    }
    .encode()
    .unwrap()
}

/// Frames, checkpoints from unrelated publications and selection changes do
/// no tree work; only a status publication rebuilds. `guard` false removes
/// the revision guard, re-adding the redundant per-frame rebuild.
async fn rebuilds_follow_status_publications(guard: bool) {
    let volume = Arc::new(
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("agent-panel").unwrap(),
        )
        .await
        .unwrap(),
    );
    let root = TaskId::from_bytes([1; 16]);
    append(
        &volume,
        status_key(root),
        status(root, None, TaskState::Completed, false),
    )
    .await;
    let owner = wire::Authority {
        kind: wire::AggregateKind::Task as i32,
        id: root.to_string(),
    };
    let mut files = CommandSession::open(
        volume.clone(),
        OperationId::from_bytes([2; 16])
            .to_string()
            .parse()
            .unwrap(),
        65_536,
        64,
    )
    .await
    .unwrap();
    let mut client = TaskFileClient::observe_task(volume.as_ref(), &owner, root, 64, 65_536)
        .await
        .unwrap();
    let mut overviews = task_overview_files(volume.as_ref(), client.views_mut(), 64, 65_536)
        .await
        .unwrap();
    let mut panel = AgentPanel::default();
    for frame in 0..40u8 {
        if !guard {
            panel.cut = None;
        }
        if frame % 10 == 5 {
            // An unrelated publication: a cursor-only checkpoint for this query.
            files
                .write_state(
                    CommandFileRoot::Session,
                    "layout.json",
                    &serde_json::json!({"selected_task": root, "frame": frame}),
                )
                .await
                .unwrap();
            client
                .apply_change(overviews.next().await.unwrap().unwrap())
                .unwrap();
        }
        panel.update(client.views(), &files).unwrap();
    }
    assert_eq!(panel.builds, 1, "tree builds without a status change");
    let child = TaskId::from_bytes([3; 16]);
    append(
        &volume,
        status_key(child),
        status(child, Some(root), TaskState::Pending, true),
    )
    .await;
    client
        .apply_change(overviews.next().await.unwrap().unwrap())
        .unwrap();
    panel.update(client.views(), &files).unwrap();
    panel.update(client.views(), &files).unwrap();
    assert_eq!(panel.builds, 2);
    assert_eq!(
        panel.tree.child(&root.to_string()),
        Some(child.to_string().as_str())
    );
}

#[tokio::test]
async fn only_status_publications_rebuild_the_tree() {
    rebuilds_follow_status_publications(true).await;
}

#[tokio::test]
#[should_panic(expected = "tree builds without a status change")]
async fn rebuilding_without_the_revision_guard_is_detected_negative_control() {
    rebuilds_follow_status_publications(false).await;
}

#[tokio::test]
async fn malformed_and_unknown_status_files_are_rejected() {
    let task = TaskId::from_bytes([1; 16]);
    let other = TaskId::from_bytes([9; 16]);
    for (bytes, expected) in [
        (br#"{"parent":null}"#.to_vec(), "task status"),
        (
            format!(r#"{{"task":"{task}","parent":null,"state":"dreaming","attention":false,"owner_agent":null}}"#).into_bytes(),
            "task status",
        ),
        (
            format!(r#"{{"task":"{task}","parent":"not-a-task","state":"pending","attention":false,"owner_agent":null}}"#).into_bytes(),
            "task status",
        ),
        (status(other, None, TaskState::Pending, false), "under"),
    ] {
        let volume = Arc::new(
            TaskVolume::open(
                Arc::new(MemoryStream::default()),
                StreamPath::new("agent-panel-invalid").unwrap(),
            )
            .await
            .unwrap(),
        );
        append(
            &volume,
            status_key(task),
            bytes,
        )
        .await;
        let root = TaskId::from_bytes([1; 16]);
        let owner = wire::Authority {
            kind: wire::AggregateKind::Task as i32,
            id: root.to_string(),
        };
        let mut client = TaskFileClient::observe_task(volume.as_ref(), &owner, root, 64, 65_536)
            .await
            .unwrap();
        // Only the installed snapshot is read; the follow is never polled.
        let _snapshot_only = task_overview_files(volume.as_ref(), client.views_mut(), 64, 65_536)
            .await
            .unwrap();
        let files = CommandSession::open(
            volume,
            OperationId::from_bytes([2; 16])
                .to_string()
                .parse()
                .unwrap(),
            65_536,
            64,
        )
        .await
        .unwrap();
        let error = AgentPanel::default()
            .update(client.views(), &files)
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}
