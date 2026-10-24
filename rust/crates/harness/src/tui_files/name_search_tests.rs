use super::*;
use crate::{
    OperationId, TaskId,
    task_volume::{FileKey, FileValue},
};

async fn publish(files: &CompletionFiles, request: OperationId, path: &str) -> crate::Result<()> {
    let binding = files.search.as_ref().unwrap();
    let key = FileKey::parse("search/results.json")?;
    let cut = binding
        .files
        .inner
        .snapshot(&Query::File(key.clone()), 1)
        .await?
        .generation;
    binding
        .files
        .inner
        .append(
            cut,
            &FileValue::new(
                key,
                serde_json::to_vec(
                    &serde_json::json!({"request":request,"query":"rs","paths":[path]}),
                )
                .map_err(|error| crate::Error::Invalid(error.to_string()))?,
            ),
        )
        .await?;
    Ok(())
}

async fn apply_result(files: &mut CompletionFiles, input: &mut PromptInput) -> crate::Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let change = files.read_change().await?;
            let result = change
                .file
                .iter()
                .chain(&change.files)
                .any(|file| file.key.path() == "search/results.json");
            files.apply_change(change, input)?;
            if result {
                return Ok(());
            }
        }
    })
    .await
    .map_err(|error| crate::Error::Storage(error.to_string()))?
}

#[tokio::test]
async fn name_search_request_identity_fences_task_switches_and_cancelled_results()
-> crate::Result<()> {
    let root = tempfile::tempdir().map_err(|error| crate::Error::Storage(error.to_string()))?;
    let state = tempfile::tempdir().map_err(|error| crate::Error::Storage(error.to_string()))?;
    let model = crate::model::Model::new(
        "scripted".into(),
        "completion".into(),
        "1".into(),
        serde_json::Value::Null,
    )?;
    let harness = crate::LocalHarness::open(
        crate::test_support::local_harness_config(
            root.path(),
            (
                model,
                std::sync::Arc::new(crate::test_support::ScriptedModelProvider::new([])),
            ),
        )?
        .state_directory(state.path())
        .without_default_tools(),
    )
    .await?;
    let key = FileKey::parse("search/results.json")?;
    let cut = harness
        .task_files()
        .inner
        .snapshot(&Query::File(key.clone()), 1)
        .await?
        .generation;
    harness
        .task_files()
        .inner
        .append(
            cut,
            &FileValue::new(
                key,
                br#"{"query":"rs","paths":["previous-client.rs"]}"#.to_vec(),
            ),
        )
        .await?;
    let mut input = PromptInput::new(crate::tui::Editor::new("@rs"));
    let mut files =
        CompletionFiles::observe_authorized(harness.task_files(), &mut input, None).await?;
    // Negative control: restoring the snapshot alone exposes the old result.
    assert_eq!(
        input.completion.items().next().unwrap().value,
        "previous-client.rs"
    );
    files.bind_search(
        &mut input,
        std::sync::Arc::clone(harness.task_files()),
        tokio::spawn(std::future::pending()),
    )?;
    assert_eq!(input.completion.items().len(), 0);
    files
        .refresh_query(&mut input, Some(TaskId::from_bytes([1; 16])))
        .await?;
    let first = files
        .search
        .as_ref()
        .unwrap()
        .request
        .as_ref()
        .unwrap()
        .request;
    publish(&files, first, "first.rs").await?;
    apply_result(&mut files, &mut input).await?;
    assert_eq!(input.completion.items().next().unwrap().value, "first.rs");
    // Same query text, different task: clear immediately and reject old results.
    files
        .refresh_query(&mut input, Some(TaskId::from_bytes([2; 16])))
        .await?;
    let second = files
        .search
        .as_ref()
        .unwrap()
        .request
        .as_ref()
        .unwrap()
        .request;
    assert_ne!(first, second);
    assert_eq!(input.completion.items().len(), 0);
    publish(&files, first, "stale.rs").await?;
    apply_result(&mut files, &mut input).await?;
    assert_eq!(input.completion.items().len(), 0);
    // Negative control: a query-text-only fold accepts this stale task result.
    input
        .completion
        .set_file(
            "search/results.json",
            Some(br#"{"query":"rs","paths":["stale.rs"]}"#),
        )
        .map_err(|error| crate::Error::Invalid(error.to_string()))?;
    input.refresh_completions();
    assert_eq!(input.completion.items().next().unwrap().value, "stale.rs");
    publish(&files, second, "second.rs").await?;
    apply_result(&mut files, &mut input).await?;
    assert_eq!(input.completion.items().next().unwrap().value, "second.rs");
    input.editor.set_text("plain", 1);
    files.refresh_query(&mut input, None).await?;
    publish(&files, second, "late.rs").await?;
    apply_result(&mut files, &mut input).await?;
    input.editor.set_text("@rs", 2);
    input.refresh_completions();
    assert_eq!(input.completion.items().len(), 0);
    Ok(())
}
