//! The bound file writer and query use the same durable private provider.
use super::*;
use crate::task_volume::{Component, FileKey, FileValue, Query, TaskFile};
use futures::StreamExt as _;

#[tokio::test]
async fn private_request_files_follow_immediately_and_survive_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(super::super::storage)?;
    let provider = scripted([]);
    let harness = LocalHarness::open(LocalHarnessConfig::new(
        directory.path(),
        (model()?, provider.clone()),
    ))
    .await?;
    let files = harness.task_files();
    assert_eq!(files.volume().class(), VolumeClass::AgentPrivate);
    assert_ne!(files.volume(), harness.scratch());
    assert_ne!(files.volume(), harness.journal_volume());
    let task = Component::new("task").map_err(super::super::storage)?;
    let mut initial = files
        .observe(Query::Requests(task.clone()), None, 8)
        .await
        .map_err(super::super::storage)?;
    assert!(!initial.reset);
    let snapshot = initial.snapshot.take().unwrap();
    assert!(snapshot.files.is_empty());
    let request = FileValue::new(
        FileKey::Task {
            task: task.clone(),
            file: TaskFile::Request(Component::new("request").map_err(super::super::storage)?),
        },
        br#"{"input":"hi"}"#.to_vec(),
    );
    let generation = files
        .write(snapshot.generation, &request)
        .await
        .map_err(super::super::storage)?;
    let change = initial
        .changes
        .next()
        .await
        .unwrap()
        .map_err(super::super::storage)?;
    assert_eq!(change.file.as_ref(), Some(&request));
    assert_eq!(change.cursor.position, generation.position);
    assert_eq!(
        files
            .write(generation, &request)
            .await
            .map_err(super::super::storage)?,
        generation
    );
    let status = FileValue::new(
        FileKey::Task {
            task: task.clone(),
            file: TaskFile::Status,
        },
        b"{}".to_vec(),
    );
    assert!(matches!(
        files.write(generation, &status).await,
        Err(crate::task_volume::Error::Unauthorized)
    ));
    let mut altered = request.clone();
    altered.bytes = acyclic_stream::OwnedBytes::from_static(b"different");
    assert!(matches!(
        files.write(generation, &altered).await,
        Err(crate::task_volume::Error::ImmutableConflict(_))
    ));
    let cursor = change.cursor;
    drop(initial);
    drop(harness);

    let harness = LocalHarness::open(LocalHarnessConfig::new(
        directory.path(),
        (model()?, provider),
    ))
    .await?;
    let reopened = harness
        .task_files()
        .observe(Query::Requests(task.clone()), None, 8)
        .await
        .map_err(super::super::storage)?;
    let snapshot = reopened.snapshot.unwrap();
    assert_eq!(snapshot.files, vec![request]);
    assert_eq!(snapshot.generation, generation);
    let resumed = harness
        .task_files()
        .observe(Query::Requests(task), Some(cursor), 8)
        .await
        .map_err(super::super::storage)?;
    assert!(!resumed.reset);
    assert!(resumed.snapshot.is_none());
    assert!(!directory.path().join(".tasks").exists());
    Ok(())
}
