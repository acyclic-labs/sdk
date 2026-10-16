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
            &Query::File(FileKey::parse(export["export"].as_str().unwrap()).map_err(volume_error)?),
            1,
        )
        .await
        .map_err(volume_error)?;
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
    let output = messages
        .last()
        .unwrap()
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            acyclic_harness::model::ModelContentPart::ToolResult {
                call_id,
                name,
                content: acyclic_harness::model::ToolResultContent::Json { value },
            } if call_id == "child-read" && name == "bash" => Some(value),
            _ => None,
        })
        .expect("model did not receive its child bash result");
    assert_eq!(output["stdout"], "child");
    assert_eq!(output["success"], true);
    assert_eq!(
        std::fs::read_to_string(directory.path().join("marker")).unwrap(),
        "parent"
    );
    Ok(())
}

#[tokio::test]
async fn followed_model_selection_reaches_the_provider_for_new_and_child_turns() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider([
        ScriptedTurn::Text("first".into()),
        ScriptedTurn::Text("child".into()),
    ]);
    let local = local(directory.path(), provider.clone()).await?;
    let mut client = CommandSession::open(
        local.task_files().clone(),
        uuid::Uuid::new_v4(),
        65_536,
        256,
    )
    .await?;
    client.submit("/model other", &NoReads).await?;
    let root = send(&local, &mut client, "root prompt").await?;
    let root = TaskId::parse(root["task"].as_str().unwrap())?;
    assert_eq!(local.resume(root).await?.text, "first");
    assert_eq!(provider.requests()[0].model.name, "other");
    let child = send(&local, &mut client, "child prompt").await?;
    let child = TaskId::parse(child["task"].as_str().unwrap())?;
    // The task is already admitted; dispatch must read the followed selection.
    client.submit("/model child-model", &NoReads).await?;
    assert_eq!(local.resume(child).await?.text, "child");
    assert_eq!(provider.requests()[1].model.name, "child-model");
    Ok(())
}

#[tokio::test]
async fn confirmed_delete_keeps_its_target_and_clear_cancels_conversations() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let provider = provider([]);
    let local = local(directory.path(), provider.clone()).await?;
    let mut client = CommandSession::open(
        local.task_files().clone(),
        uuid::Uuid::new_v4(),
        65_536,
        256,
    )
    .await?;
    let root = send(&local, &mut client, "root prompt").await?;
    let root = TaskId::parse(root["task"].as_str().unwrap())?;
    client.submit("/delete", &NoReads).await?;
    assert!(client.take_request().is_none());
    client.submit("no", &NoReads).await?;
    assert!(client.take_request().is_none());
    assert!(
        !client
            .state(CommandFileRoot::Task, "settings.json")?
            .unwrap()["deleted"]
            .as_bool()
            .unwrap_or(false)
    );
    client.submit("/delete", &NoReads).await?;
    let child = send(&local, &mut client, "child prompt").await?;
    let child = TaskId::parse(child["task"].as_str().unwrap())?;
    client.submit("yes", &NoReads).await?;
    let request = client.next_request().await?;
    assert!(
        matches!(&request.key, FileKey::Task { task, .. } if task.as_str() == root.to_string())
    );
    consume_local_command(&mut client, &local, &request).await?;
    assert!(local.resume(root).await.is_err());
    assert!(
        consume_local_command(&mut client, &local, &request)
            .await?
            .is_none()
    );
    assert!(provider.requests().is_empty());
    client.submit("/clearall", &NoReads).await?;
    assert!(client.take_request().is_none());
    client.submit("yes", &NoReads).await?;
    let request = client.next_request().await?;
    consume_local_command(&mut client, &local, &request).await?;
    assert!(local.resume(child).await.is_err());
    assert!(client.selected_task()?.is_none());
    assert!(
        client.output_page(0)?.is_none(),
        "clear left the transcript visible"
    );
    Ok(())
}

#[tokio::test]
async fn search_and_fetch_use_the_registered_web_executor_without_model_dispatch() -> Result<()> {
    use acyclic_harness::tool::{
        JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolResult,
    };
    struct Web;
    impl ToolExecutor for Web {
        fn execute<'a>(
            &'a self,
            invocation: ToolInvocation,
            context: Option<acyclic_harness::runtime::ToolContext>,
        ) -> acyclic_stream::BoxProviderFuture<'a, Result<ToolResult>> {
            Box::pin(async move {
                assert_eq!(invocation.name, "web_search");
                assert!(context.is_some(), "web command bypassed task executor");
                Ok(ToolResult {
                    value: invocation.arguments,
                })
            })
        }
        fn reconcile<'a>(
            &'a self,
            _: ToolInvocation,
            _: Option<acyclic_harness::runtime::ToolContext>,
        ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<ToolResult>>> {
            Box::pin(async { Ok(None) })
        }
    }
    let schema = json!({"type":"object"});
    let web = Tool {
        hook_names: &[],
        definition: ToolDefinition {
            name: "web_search".into(),
            revision: "1".into(),
            description: "Registered web executor".into(),
            input_schema: schema.clone(),
            output_schema: schema.clone(),
            projection_schema: acyclic_harness::tool::json_projection_schema(schema),
        },
        executor: Arc::new(Web),
        projection: Arc::new(JsonToolProjection),
    };
    let directory = tempfile::tempdir().unwrap();
    let provider = provider([]);
    let model = Model::new("scripted".into(), "test".into(), "1".into(), Value::Null)?;
    let local = LocalHarness::open(
        LocalHarnessConfig::new(directory.path(), (model, provider.clone())).tool(web),
    )
    .await?;
    let mut client = CommandSession::open(
        local.task_files().clone(),
        uuid::Uuid::new_v4(),
        65_536,
        256,
    )
    .await?;
    assert_eq!(
        send(&local, &mut client, "/search needle").await?,
        json!({"query":"needle"})
    );
    assert_eq!(
        send(&local, &mut client, "/fetch https://example.test/page").await?,
        json!({"url":"https://example.test/page"})
    );
    assert!(provider.requests().is_empty());
    Ok(())
}
