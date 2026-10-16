use super::*;
use acyclic_harness::{
    Admission, LocalHarness, LocalHarnessConfig, OperationId, TaskId,
    client::consume_local_command,
    model::Model,
    runtime::RuntimeTask,
    test_support::{ScriptedModelProvider, ScriptedTurn},
};
struct NoReads;
impl ContentResidencyVerifier for NoReads {}
fn provider(turns: impl IntoIterator<Item = ScriptedTurn>) -> Arc<ScriptedModelProvider> {
    Arc::new(ScriptedModelProvider::new(turns).with_context_capacity(
        acyclic_harness::context::ModelContextCapacity {
            context_tokens: 32_768,
            output_tokens: 4096,
        },
    ))
}
async fn local(
    directory: &std::path::Path,
    provider: Arc<ScriptedModelProvider>,
) -> Result<LocalHarness> {
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(|e| Error::Storage(e.to_string()))?;
    let fs = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(directory.join("filesystem")))
        .await
        .map_err(|e| Error::Storage(e.to_string()))?;
    let reference = acyclic_harness::resources::ProviderRef::new(
        "local-test".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let volume = acyclic_harness::conversation::VolumeRef::new(
        reference.clone(),
        "project".into(),
        acyclic_harness::conversation::VolumeClass::Project,
        acyclic_harness::conversation::VolumeOwner::Project("test".into()),
        &mut None,
    )?;
    acyclic_harness::filesystem::FilesystemHost::new(fs, &reference, None)?
        .create_volume(&volume)
        .await?;
    let model = Model::new("scripted".into(), "test".into(), "1".into(), Value::Null)?;
    LocalHarness::open(
        LocalHarnessConfig::new(directory.join("filesystem"), (model, provider)).volume(volume),
    )
    .await
}
async fn send(
    local: &LocalHarness,
    client: &mut CommandSession<acyclic_stream::LocalStream>,
    text: &str,
) -> Result<Value> {
    client.submit(text, &NoReads).await?;
    let request = client.next_request().await?;
    Ok(consume_local_command(client, local, &request)
        .await?
        .expect("unclaimed request"))
}
async fn selected(
    local: &LocalHarness,
    task: TaskId,
) -> Result<CommandSession<acyclic_stream::LocalStream>> {
    let mut client = CommandSession::open(
        local.task_files().clone(),
        uuid::Uuid::new_v4(),
        65_536,
        256,
    )
    .await?;
    client
        .write_state(
            CommandFileRoot::Session,
            "layout.json",
            &json!({"selected_task":task}),
        )
        .await?;
    Ok(client)
}
#[tokio::test]
async fn production_pause_stop_resume_commands_gate_real_owner_dispatch_and_reopen() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider([ScriptedTurn::Text("continued".into())]);
    let local = local(directory.path(), provider.clone()).await?;
    let Admission::Accepted(RuntimeTask::Durable { task_id, .. }) =
        local.submit(OperationId::new(), "wait for resume").await?
    else {
        panic!("admission")
    };
    let mut client = selected(&local, task_id).await?;
    for (text, paused) in [
        ("/pause", true),
        ("/pause", false),
        ("/stop", true),
        ("/resume", false),
        ("/stopall", true),
        ("/resumeall", false),
        ("/stop", true),
    ] {
        assert_eq!(send(&local, &mut client, text).await?["paused"], paused);
    }
    let mut running = Box::pin(local.resume(task_id));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), running.as_mut())
            .await
            .is_err(),
        "paused task completed before resume"
    );
    assert!(
        provider.requests().is_empty(),
        "paused owner dispatched a model"
    );
    // Recreate the UI consumer. The active actor remains parked on its file follow.
    drop(client);
    let mut client = selected(&local, task_id).await?;
    assert_eq!(send(&local, &mut client, "/resume").await?["paused"], false);
    assert_eq!(running.await?.text, "continued");
    Ok(())
}
#[tokio::test]
async fn production_compact_and_export_requests_have_durable_effects() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider([
        ScriptedTurn::Text("remember the invariant".into()),
        ScriptedTurn::Text("continued".into()),
    ]);
    let local = local(directory.path(), provider.clone()).await?;
    let Admission::Accepted(RuntimeTask::Durable { task_id, .. }) =
        local.submit(OperationId::new(), "original prompt").await?
    else {
        panic!("admission")
    };
    let mut client = selected(&local, task_id).await?;
    let compact = send(&local, &mut client, "/compact keep decisions").await?;
    assert_eq!(compact["summary"], "remember the invariant");
    let requests = provider.requests();
    assert!(requests[0].messages.iter().any(|message| matches!(&message.content, acyclic_harness::model::ModelContent::Text(text) if text.contains("original prompt") && text.contains("keep decisions"))), "compaction omitted the actual admitted prompt");
    let queued = send(&local, &mut client, "continue").await?;
    let task = TaskId::parse(queued["task"].as_str().unwrap())?;
    assert_eq!(local.resume(task).await?.text, "continued");
    assert!(provider.requests().last().unwrap().messages.iter().any(|message| matches!(&message.content, acyclic_harness::model::ModelContent::Text(text) if text.contains("remember the invariant"))));
    let export = send(&local, &mut client, "/export").await?;
    let snapshot = local
        .task_files()
        .snapshot(
            &Query::File(FileKey::parse(export["export"].as_str().unwrap())?),
            1,
        )
        .await?;
    let diagnostic: Value = serde_json::from_slice(&snapshot.files[0].bytes).unwrap();
    assert!(
        diagnostic["files"]
            .as_array()
            .unwrap()
            .iter()
            .any(|file| file["path"].as_str().unwrap().contains("/requests/"))
    );
    Ok(())
}

#[cfg(all(feature = "native-execution", unix))]
#[tokio::test]
async fn child_shell_writes_and_model_bash_share_only_the_child_fork() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join(".git")).unwrap();
    std::fs::write(directory.path().join("marker"), "parent").unwrap();
    let provider = provider([
        acyclic_harness::test_support::ScriptedTurn::Events(vec![
            acyclic_harness::test_support::ScriptedEvent::Event(
                acyclic_harness::model::ModelEvent::ToolCall {
                    call_id: "child-read".into(),
                    name: "bash".into(),
                    arguments: json!({"command":"cat marker"}),
                },
            ),
            acyclic_harness::test_support::ScriptedEvent::Event(
                acyclic_harness::model::ModelEvent::Completed {
                    metadata: json!({}),
                },
            ),
        ]),
        ScriptedTurn::Text("done".into()),
    ]);
    let model = Model::new("scripted".into(), "test".into(), "1".into(), Value::Null)?;
    let local = LocalHarness::open(LocalHarnessConfig::new(
        directory.path(),
        (model, provider.clone()),
    ))
    .await?;
    let Admission::Accepted(RuntimeTask::Durable {
        task_id: parent, ..
    }) = local.submit(OperationId::new(), "parent").await?
    else {
        panic!("parent")
    };
    let mut client = selected(&local, parent).await?;
    let child = send(&local, &mut client, "/delegate child").await?;
    let task = TaskId::parse(child["task"].as_str().unwrap())?;
    client
        .write_state(
            CommandFileRoot::Session,
            "layout.json",
            &json!({"selected_task":task}),
        )
        .await?;
    let shell = send(
        &local,
        &mut client,
        "/shell printf child > marker; cat marker",
    )
    .await?;
    assert_eq!(shell["stdout"], "child");
    assert_eq!(
        std::fs::read_to_string(directory.path().join("marker")).unwrap(),
        "parent"
    );
    assert!(
        provider.requests().is_empty(),
        "shell went through the model"
    );
    assert_eq!(local.resume(task).await?.text, "done");
    let messages = provider.requests();
    assert!(
        serde_json::to_string(&messages.last().unwrap().messages)
            .unwrap()
            .contains("child")
    );
    assert_eq!(
        std::fs::read_to_string(directory.path().join("marker")).unwrap(),
        "parent"
    );
    Ok(())
}
