//! The bound file writer and query use the same durable private provider.
use acyclic_harness::task_volume::{Component, FileKey, FileValue, Query, TaskFile};
use acyclic_harness::{
    Error, LocalHarness, Result,
    conversation::VolumeClass,
    model::Model,
    test_support::{ScriptedModelProvider, local_harness_config},
};
use futures::StreamExt as _;
use std::sync::Arc;

#[tokio::test]
async fn explicitly_hermetic_compositions_default_to_private_state() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let model = Model::new(
        "scripted".into(),
        "test".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let config = acyclic_harness::LocalHarnessConfig::new(
        directory.path(),
        (model, Arc::new(ScriptedModelProvider::new([]))),
    )
    .hermetic()
    .without_default_tools();
    #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    let config = config.settings(
        acyclic_harness::native_settings::SettingsConfig::new(directory.path(), directory.path())
            .codex_home(directory.path().join(".codex")),
    );
    let first = LocalHarness::open(config).await?;
    // A second explicit fixture config must not reopen the first composition's per-project
    // user store: its private volume identities must be distinct.
    let model = Model::new(
        "scripted".into(),
        "test".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let config = acyclic_harness::LocalHarnessConfig::new(
        directory.path(),
        (model, Arc::new(ScriptedModelProvider::new([]))),
    )
    .hermetic()
    .without_default_tools();
    #[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
    let config = config.settings(
        acyclic_harness::native_settings::SettingsConfig::new(directory.path(), directory.path())
            .codex_home(directory.path().join(".codex")),
    );
    let second = LocalHarness::open(config).await?;
    assert_ne!(first.task_files().volume(), second.task_files().volume());
    first.close().await?;
    second.close().await
}

#[cfg(any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http"))]
#[tokio::test]
async fn integration_compositions_cannot_discover_the_real_settings_home() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let model = Model::new(
        "scripted".into(),
        "test".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let config = acyclic_harness::LocalHarnessConfig::new(
        directory.path(),
        (model, Arc::new(ScriptedModelProvider::new([]))),
    )
    .hermetic()
    .without_default_tools();
    // This compiles Harness as a dependency, without cfg(test). Bypassing the
    // settings helper while explicitly hermetic must fail before reading the real user's settings.
    assert!(matches!(
        LocalHarness::open(config).await,
        Err(Error::Unsupported(message)) if message == "hermetic compositions must inject settings"
    ));
    Ok(())
}

#[tokio::test]
async fn private_request_files_follow_immediately_and_survive_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let provider = Arc::new(ScriptedModelProvider::new([]));
    let model = Model::new(
        "scripted".into(),
        "test".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let harness = LocalHarness::open(
        local_harness_config(directory.path(), (model.clone(), provider.clone()))?
            .state_directory(state.path()),
    )
    .await?;
    let files = harness.task_files();
    assert_eq!(files.volume().class(), VolumeClass::AgentPrivate);
    assert_ne!(files.volume(), harness.scratch());
    assert_ne!(files.volume(), harness.journal_volume());
    let task = Component::new("task").map_err(storage)?;
    let mut initial = files
        .observe(Query::Requests(task.clone()), None, 8)
        .await
        .map_err(storage)?;
    assert!(!initial.reset);
    let snapshot = initial.snapshot.take().unwrap();
    assert!(snapshot.files.is_empty());
    let request = FileValue::new(
        FileKey::Task {
            task: task.clone(),
            file: TaskFile::Request(Component::new("request").map_err(storage)?),
        },
        br#"{"input":"hi"}"#.to_vec(),
    );
    let generation = files
        .write(snapshot.generation, &request)
        .await
        .map_err(storage)?;
    let change = initial.changes.next().await.unwrap().map_err(storage)?;
    assert_eq!(change.file.as_ref(), Some(&request));
    assert_eq!(change.cursor.position, generation.position);
    assert_eq!(
        files.write(generation, &request).await.map_err(storage)?,
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
        Err(acyclic_harness::task_volume::Error::Unauthorized)
    ));
    let mut altered = request.clone();
    altered.bytes = acyclic_stream::OwnedBytes::from_static(b"different");
    assert!(matches!(
        files.write(generation, &altered).await,
        Err(acyclic_harness::task_volume::Error::ImmutableConflict(_))
    ));
    let cursor = change.cursor;
    drop(initial);
    drop(harness);

    let harness = LocalHarness::open(
        local_harness_config(directory.path(), (model, provider))?.state_directory(state.path()),
    )
    .await?;
    let reopened = harness
        .task_files()
        .observe(Query::Requests(task.clone()), None, 8)
        .await
        .map_err(storage)?;
    let snapshot = reopened.snapshot.unwrap();
    assert_eq!(snapshot.files, vec![request]);
    assert_eq!(snapshot.generation, generation);
    let resumed = harness
        .task_files()
        .observe(Query::Requests(task), Some(cursor), 8)
        .await
        .map_err(storage)?;
    assert!(!resumed.reset);
    assert!(resumed.snapshot.is_none());
    assert!(!directory.path().join(".tasks").exists());
    Ok(())
}

fn storage(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}
