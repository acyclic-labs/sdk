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
        br#"{"parent":null,"state":"completed"}"#.to_vec(),
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
    let mut overviews = task_overview_files(volume.as_ref(), 64, 65_536)
        .await
        .unwrap();
    client
        .apply_update(overviews.next().await.unwrap().unwrap())
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
                .apply_update(overviews.next().await.unwrap().unwrap())
                .unwrap();
        }
        panel.update(client.views(), &files).unwrap();
    }
    assert_eq!(panel.builds, 1, "tree builds without a status change");
    let child = TaskId::from_bytes([3; 16]);
    append(
        &volume,
        status_key(child),
        format!(r#"{{"parent":"{root}","state":"pending","attention":true}}"#).into_bytes(),
    )
    .await;
    client
        .apply_update(overviews.next().await.unwrap().unwrap())
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
    for (bytes, expected) in [
        (&br#"{"parent":null}"#[..], "task status"),
        (
            br#"{"parent":null,"state":"dreaming"}"#,
            "unknown task state",
        ),
        (
            br#"{"parent":"not-a-task","state":"pending"}"#,
            "task status",
        ),
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
            status_key(TaskId::from_bytes([1; 16])),
            bytes.to_vec(),
        )
        .await;
        let snapshot = volume.snapshot(&Query::TaskOverviews, 64).await.unwrap();
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
        let mut views = TaskFileViews::default();
        views
            .query_snapshot(Query::TaskOverviews, snapshot, 64, 65_536)
            .unwrap();
        let error = AgentPanel::default().update(&views, &files).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}
