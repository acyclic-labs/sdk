use super::*;
use acyclic_harness::{
    Admission, LocalHarness, OperationId, TaskId,
    client::consume_local_command,
    model::Model,
    runtime::RuntimeTask,
    test_support::{ScriptedModelProvider, ScriptedTurn},
};
// Workspace lineage is a conversation-turn invariant. /delegate now starts a
// file-driven agent, with a different result and independently driven lifecycle.
struct NoReads;
impl ContentResidencyVerifier for NoReads {}

#[cfg(all(feature = "native-execution", target_os = "linux"))]
#[tokio::test]
async fn descendant_named_volume_survives_branch_switch_and_routes_bash_cwd() -> Result<()> {
    use acyclic_harness::model::{ModelContentPart, ModelEvent, ToolCall, ToolResultContent};
    use acyclic_harness::test_support::ScriptedEvent;
    let directory = crate::client_local::Directory::new().unwrap();
    let calls = [
        (
            "bash",
            json!({"command":"printf private > .volumes/scratch/child-private"}),
        ),
        (
            "bash",
            json!({"command":"cat child-private", "cwd":".volumes/scratch"}),
        ),
        ("fs", json!({"command":"git --volume scratch status"})),
        ("fs", json!({"command":"git switch -c selected-child"})),
        (
            "bash",
            json!({"command":"cat child-private; test ! -f ../../child-private", "cwd":".volumes/scratch"}),
        ),
    ];
    let provider = provider(
        calls
            .into_iter()
            .enumerate()
            .map(|(index, (name, arguments))| {
                ScriptedTurn::Events(vec![
                    ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                        format!("scratch-{index}"),
                        name.into(),
                        arguments,
                    ))),
                    ScriptedEvent::Event(ModelEvent::Completed {
                        metadata: json!({}),
                    }),
                ])
            })
            .chain([ScriptedTurn::Text("scratch retained".into())]),
    );
    let local = local(directory.path(), provider.clone()).await?;
    let Admission::Accepted(RuntimeTask::Durable {
        task_id: parent, ..
    }) = local.submit(OperationId::new(), "parent").await?
    else {
        panic!("parent admission")
    };
    let mut client = selected(&local, parent).await?;
    let child = send(&local, &mut client, "child prompt").await?;
    let child = TaskId::parse(child["task"].as_str().unwrap())?;
    assert_eq!(local.resume(child).await?.text, "scratch retained");
    let requests = provider.requests();
    let output = |id: &str| {
        requests
            .last()
            .unwrap()
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .find_map(|part| match part {
                ModelContentPart::ToolResult {
                    call_id,
                    content: ToolResultContent::Json { value },
                    ..
                } if call_id == id => Some(value),
                _ => None,
            })
            .expect("model did not receive the tool result")
    };
    for id in ["scratch-0", "scratch-1", "scratch-4"] {
        assert_eq!(output(id)["success"], true, "{id}: {}", output(id));
    }
    assert_eq!(output("scratch-1")["stdout"], "private");
    assert_eq!(output("scratch-4")["stdout"], "private");
    for id in ["scratch-2", "scratch-3"] {
        assert!(output(id).get("result").is_some(), "{id}: {}", output(id));
    }
    assert!(
        !local
            .workspace_path()
            .unwrap()
            .join("child-private")
            .exists()
    );
    Ok(())
}

#[cfg(all(feature = "native-execution", unix))]
#[tokio::test]
async fn child_restore_is_visible_to_the_next_model_bash_without_a_branch_switch() -> Result<()> {
    use acyclic_harness::model::{ModelContentPart, ModelEvent, ToolCall, ToolResultContent};
    use acyclic_harness::test_support::ScriptedEvent;
    let directory = crate::client_local::Directory::new().unwrap();
    let provider = provider(
        [
            ("bash", "printf original > marker"),
            ("fs", "git add marker"),
            ("fs", "git commit -m baseline"),
            ("bash", "printf changed > marker"),
            ("fs", "git restore --source HEAD -- marker"),
            ("bash", "cat marker"),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (name, command))| {
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                    format!("restore-{index}"),
                    name.into(),
                    json!({"command":command}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ])
        })
        .chain([ScriptedTurn::Text("restored".into())]),
    );
    let local = local(directory.path(), provider.clone()).await?;
    let Admission::Accepted(RuntimeTask::Durable {
        task_id: parent, ..
    }) = local.submit(OperationId::new(), "parent").await?
    else {
        panic!("parent admission")
    };
    let mut client = selected(&local, parent).await?;
    let child = send(&local, &mut client, "child prompt").await?;
    let child = TaskId::parse(child["task"].as_str().unwrap())?;
    assert_eq!(
        local
            .resume(child)
            .await
            .unwrap_or_else(|error| panic!(
                "child restore failed after {} model requests: {error:?}",
                provider.requests().len()
            ))
            .text,
        "restored"
    );
    let requests = provider.requests();
    let output = |id: &str| {
        requests
            .last()
            .unwrap()
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .find_map(|part| match part {
                ModelContentPart::ToolResult {
                    call_id,
                    content: ToolResultContent::Json { value },
                    ..
                } if call_id == id => Some(value),
                _ => None,
            })
            .expect("model did not receive a successful tool result")
    };
    assert!(
        output("restore-4").get("result").is_some(),
        "restore was not executed"
    );
    assert_eq!(
        output("restore-5")["stdout"],
        "original",
        "child mount retained pre-restore contents"
    );
    assert!(!local.workspace_path().unwrap().join("marker").exists());
    Ok(())
}
fn provider(turns: impl IntoIterator<Item = ScriptedTurn>) -> Arc<ScriptedModelProvider> {
    Arc::new(ScriptedModelProvider::new(turns).with_context_capacity(
        acyclic_harness::context::ModelContextCapacity {
            context_tokens: 32_768,
            output_tokens: Some(4096),
        },
    ))
}
async fn local(
    directory: &std::path::Path,
    provider: Arc<ScriptedModelProvider>,
) -> Result<LocalHarness> {
    local_with_limits(directory, provider, Default::default()).await
}
async fn local_with_limits(
    directory: &std::path::Path,
    provider: Arc<ScriptedModelProvider>,
    limits: acyclic_harness::conversation::Limits,
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
        crate::client_local::config(directory.join("filesystem"), (model, provider))?
            .volume(volume)
            .limits(limits),
    )
    .await
}
async fn send(
    local: &LocalHarness,
    client: &mut CommandSession<dyn acyclic_stream::StreamProvider>,
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
) -> Result<CommandSession<dyn acyclic_stream::StreamProvider>> {
    let mut client = CommandSession::open_local(&local, uuid::Uuid::new_v4(), 65_536, 256).await?;
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
    let directory = crate::client_local::Directory::new().unwrap();
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
    let (output, resumed) = tokio::join!(running, async {
        let mut client = selected(&local, task_id).await?;
        send(&local, &mut client, "/resume").await
    });
    assert_eq!(resumed?["paused"], false);
    assert_eq!(output?.text, "continued");
    Ok(())
}
#[tokio::test]
async fn production_compact_and_export_requests_have_durable_effects() -> Result<()> {
    let directory = crate::client_local::Directory::new().unwrap();
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
    for required in ["original prompt", "keep decisions"] {
        assert!(requests[0].messages.iter().any(|message| matches!(&message.content, acyclic_harness::model::ModelContent::Text(text) if text.contains(required))), "compaction omitted {required}");
    }
    assert!(
        requests[0].tools.is_empty(),
        "manual compaction exposed coding tools"
    );
    assert!(requests[0].max_output_tokens.is_some());
    let queued = send(&local, &mut client, "continue").await?;
    let task = TaskId::parse(queued["task"].as_str().unwrap())?;
    assert_eq!(local.resume(task).await?.text, "continued");
    assert!(provider.requests().last().unwrap().messages.iter().any(|message| matches!(&message.content, acyclic_harness::model::ModelContent::Text(text) if text.contains("remember the invariant"))));
    client.refresh_files().await?;
    let export = send(&local, &mut client, "/export").await?;
    let snapshot = local
        .task_files()
        .observe(
            Query::File(FileKey::parse(export["export"].as_str().unwrap()).map_err(volume_error)?),
            None,
            1,
        )
        .await
        .map_err(volume_error)?
        .snapshot
        .expect("initial snapshot");
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
    let directory = crate::client_local::Directory::new().unwrap();
    std::fs::create_dir(directory.path().join(".git")).unwrap();
    std::fs::write(directory.path().join("marker"), "parent").unwrap();
    let provider = provider([
        acyclic_harness::test_support::ScriptedTurn::Events(vec![
            acyclic_harness::test_support::ScriptedEvent::Event(
                acyclic_harness::model::ModelEvent::ToolCall(
                    acyclic_harness::model::ToolCall::new(
                        "child-read".into(),
                        "bash".into(),
                        json!({"command":"cat marker"}),
                    ),
                ),
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
    let local = LocalHarness::open(crate::client_local::config(
        directory.path(),
        (model, provider.clone()),
    )?)
    .await?;
    let Admission::Accepted(RuntimeTask::Durable {
        task_id: parent, ..
    }) = local.submit(OperationId::new(), "parent").await?
    else {
        panic!("parent")
    };
    let mut client = selected(&local, parent).await?;
    let child = send(&local, &mut client, "child prompt").await?;
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
    let directory = crate::client_local::Directory::new().unwrap();
    let provider = provider([
        ScriptedTurn::Text("first".into()),
        ScriptedTurn::Text("child".into()),
    ]);
    let local = local(directory.path(), provider.clone()).await?;
    let mut client = CommandSession::open_local(&local, uuid::Uuid::new_v4(), 65_536, 256).await?;
    client.submit("/model other", &NoReads).await?;
    let root = send(&local, &mut client, "root prompt").await?;
    let root = TaskId::parse(root["task"].as_str().unwrap())?;
    let child = send(&local, &mut client, "child prompt").await?;
    let child = TaskId::parse(child["task"].as_str().unwrap())?;
    assert_eq!(local.resume(root).await?.text, "first");
    assert_eq!(provider.requests()[0].model.name, "other");
    // The task is already admitted; dispatch must read the followed selection.
    client.refresh_files().await?;
    client.submit("/model child-model", &NoReads).await?;
    assert_eq!(local.resume(child).await?.text, "child");
    assert_eq!(provider.requests()[1].model.name, "child-model");
    Ok(())
}

#[tokio::test]
async fn confirmed_delete_keeps_its_target_and_clear_cancels_conversations() -> Result<()> {
    let directory = crate::client_local::Directory::new().unwrap();
    let provider = provider([]);
    let local = local(directory.path(), provider.clone()).await?;
    let mut client = CommandSession::open_local(&local, uuid::Uuid::new_v4(), 65_536, 256).await?;
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
                context.as_ref().unwrap().task().owned_task_binding(None)?;
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
    let directory = crate::client_local::Directory::new().unwrap();
    let provider = provider([]);
    let model = Model::new("scripted".into(), "test".into(), "1".into(), Value::Null)?;
    let local = LocalHarness::open(
        crate::client_local::config(directory.path(), (model, provider.clone()))?.tool(web),
    )
    .await?;
    let mut client = CommandSession::open_local(&local, uuid::Uuid::new_v4(), 65_536, 256).await?;
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

#[tokio::test]
async fn clear_all_pages_an_archive_larger_than_its_enumeration_bound() -> Result<()> {
    let directory = crate::client_local::Directory::new().unwrap();
    let local = local_with_limits(
        directory.path(),
        provider([]),
        acyclic_harness::conversation::Limits {
            context_messages: 8,
            ..Default::default()
        },
    )
    .await?;
    let mut client = CommandSession::open_local(&local, uuid::Uuid::new_v4(), 65_536, 128).await?;
    let root = send(&local, &mut client, "root").await?;
    let root = TaskId::parse(root["task"].as_str().unwrap())?;
    let child = send(&local, &mut client, "child").await?;
    let child = TaskId::parse(child["task"].as_str().unwrap())?;
    assert!(local.tools().get("bash", None, None)?.is_some());
    // Rejected tool calls retain owner input records but are not conversations.
    // These force multiple admission-directory pages without launching a shell.
    for _ in 0..12 {
        assert!(
            local
                .dispatch_command(
                    OperationId::new(),
                    None,
                    CommandResolution::Tool {
                        tool: acyclic_harness::client::CommandTool::Bash,
                        input: json!({"command":42}),
                    }
                )
                .await
                .is_err()
        );
    }
    for index in 0..25 {
        let key = FileKey::parse(&format!(".ui/archive-{index:04}/settings.json"))
            .map_err(volume_error)?;
        let cut = local
            .task_files()
            .observe(Query::File(key.clone()), None, 1)
            .await
            .map_err(volume_error)?
            .snapshot
            .expect("initial snapshot")
            .generation;
        local
            .task_files()
            .write(cut, &FileValue::new(key, b"{}".to_vec()))
            .await
            .map_err(volume_error)?;
    }
    assert!(matches!(
        client.submit("/clearall", &NoReads).await,
        Err(Error::Conflict(_))
    ));
    client.submit("/clearall", &NoReads).await?;
    client.submit("yes", &NoReads).await?;
    let request = client.next_request().await?;
    let result = consume_local_command(&mut client, &local, &request)
        .await?
        .unwrap();
    assert_eq!(result["deleted_count"], 2);
    assert!(local.resume(root).await.is_err() && local.resume(child).await.is_err());
    assert!(client.selected_task()?.is_none());
    Ok(())
}

async fn clear_direct_admissions(completed: usize, page: usize) -> Result<()> {
    let directory = crate::client_local::Directory::new().unwrap();
    let provider = provider((0..completed).map(|_| ScriptedTurn::Text("done".into())));
    let local = local_with_limits(
        directory.path(),
        provider.clone(),
        acyclic_harness::conversation::Limits {
            context_messages: page,
            ..Default::default()
        },
    )
    .await?;
    let mut tasks = Vec::new();
    for index in 0..=completed {
        let Admission::Accepted(RuntimeTask::Durable { task_id, .. }) =
            local.submit(OperationId::new(), "direct admission").await?
        else {
            panic!("direct admission")
        };
        tasks.push(task_id);
        if index < completed {
            assert_eq!(local.resume(task_id).await?.text, "done");
        }
        let key =
            FileKey::parse(&format!(".ui/task-{task_id}/settings.json")).map_err(volume_error)?;
        assert!(
            local
                .task_files()
                .observe(Query::File(key), None, 1)
                .await
                .map_err(volume_error)?
                .snapshot
                .unwrap()
                .files
                .is_empty(),
            "direct admissions must exercise conversations without UI settings"
        );
    }
    let mut client = CommandSession::open_local(&local, uuid::Uuid::new_v4(), 65_536, 128).await?;
    client.submit("/clearall", &NoReads).await?;
    let result = send(&local, &mut client, "yes").await?;
    assert_eq!(
        result["deleted_count"],
        completed + 1,
        "clear skipped an owner admission without UI settings"
    );
    for task in &tasks {
        let key =
            FileKey::parse(&format!(".ui/task-{task}/settings.json")).map_err(volume_error)?;
        let snapshot = local
            .task_files()
            .observe(Query::File(key), None, 1)
            .await
            .map_err(volume_error)?
            .snapshot
            .unwrap();
        let settings: Value = serde_json::from_slice(&snapshot.files[0].bytes).unwrap();
        assert_eq!(settings["deleted"], true);
    }
    assert!(matches!(
        local.resume(*tasks.last().unwrap()).await,
        Err(Error::InteractionRejected(
            acyclic_harness::InteractionRejection::Cancelled
        ))
    ));
    assert_eq!(
        provider.requests().len(),
        completed,
        "clear dispatched a pending conversation"
    );
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]
    // Every direct admission, settled or pending, is cleared without requiring
    // optional UI settings, independently of the bounded enumeration page size.
    #[test]
    fn clear_all_covers_direct_admissions_without_ui_settings(completed in 0usize..3, page in 8usize..12) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(clear_direct_admissions(completed, page)).unwrap();
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(8))]
    #[test]
    fn root_exports_never_embed_earlier_exports(repetitions in 2usize..5) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(async {
                let directory = crate::client_local::Directory::new().unwrap();
                let local = local(directory.path(), provider([])).await?;
                let mut client = CommandSession::open_local(&local, uuid::Uuid::new_v4(), 65_536, 128).await?;
                for _ in 0..repetitions {
                    let result = send(&local, &mut client, "/export").await?;
                    let key = FileKey::parse(result["export"].as_str().unwrap()).map_err(volume_error)?;
                    let snapshot = local.task_files().observe(Query::File(key), None, 1).await
                        .map_err(volume_error)?.snapshot.unwrap();
                    let exported: Value = serde_json::from_slice(&snapshot.files[0].bytes).unwrap();
                    assert!(exported["files"].as_array().unwrap().iter().all(|file|
                        !file["path"].as_str().unwrap().starts_with(".ui/exports/")),
                        "root export embedded previous diagnostic bytes");
                    assert!(snapshot.files[0].bytes.len() <= 65_536);
                }
                Ok::<(), Error>(())
            }).unwrap();
    }
}

#[tokio::test]
async fn command_observers_share_the_authorized_task_volume_without_exposing_host_writes()
-> Result<()> {
    use acyclic_harness::task_volume::{Component, TaskFile};
    let directory = crate::client_local::Directory::new().unwrap();
    let local = local(directory.path(), provider([])).await?;
    let session = uuid::Uuid::new_v4();
    let mut client = CommandSession::open_local(&local, session, 65_536, 128).await?;
    client.submit("/shortcuts", &NoReads).await?;
    let observed = local
        .task_files()
        .observe(
            Query::UiDirectory(Component::new(format!("session-{session}")).map_err(volume_error)?),
            None,
            8,
        )
        .await
        .map_err(volume_error)?;
    let snapshot = observed.snapshot.expect("authorized snapshot");
    assert!(snapshot.files.iter().any(
        |file| serde_json::from_slice::<Value>(&file.bytes).unwrap()["open_view"] == "shortcuts"
    ));
    let status = FileValue::new(
        FileKey::Task {
            task: Component::new("other").map_err(volume_error)?,
            file: TaskFile::Status,
        },
        b"{}".to_vec(),
    );
    assert!(matches!(
        local.task_files().write(snapshot.generation, &status).await,
        Err(acyclic_harness::task_volume::Error::Unauthorized)
    ));
    let reopened = client.reopen().await?;
    assert_eq!(
        reopened.state(CommandFileRoot::Session, "layout.json")?,
        client.state(CommandFileRoot::Session, "layout.json")?
    );
    Ok(())
}

#[tokio::test]
async fn settled_owner_tasks_publish_attention_for_live_clients() -> Result<()> {
    let directory = crate::client_local::Directory::new().unwrap();
    let local = local(
        directory.path(),
        provider([ScriptedTurn::Text("finished".into())]),
    )
    .await?;
    let task = match local.submit(OperationId::new(), "finish").await? {
        Admission::Accepted(RuntimeTask::Durable { task_id, .. }) => task_id,
        _ => panic!("durable admission"),
    };
    local.resume(task).await?;
    let mut client = CommandSession::open_local(&local, uuid::Uuid::new_v4(), 65_536, 256).await?;
    client.submit("/attention", &NoReads).await?;
    assert_eq!(client.selected_task()?, Some(task));
    assert!(
        client
            .state(CommandFileRoot::Config, "attention.json")?
            .unwrap()["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|id| *id == json!(task))
    );
    client.submit("/delete", &NoReads).await?;
    client.submit("yes", &NoReads).await?;
    let request = client.next_request().await?;
    consume_local_command(&mut client, &local, &request).await?;
    assert!(
        client
            .state(CommandFileRoot::Config, "attention.json")?
            .unwrap()["tasks"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    Ok(())
}

#[cfg(all(feature = "native-execution", unix))]
#[tokio::test]
async fn descendants_inherit_selected_branches_and_reopen_their_original_forks() -> Result<()> {
    use acyclic_harness::model::{ModelEvent, ToolCall};
    use acyclic_harness::test_support::ScriptedEvent;
    let directory = crate::client_local::Directory::new().unwrap();
    std::fs::create_dir(directory.path().join(".git")).unwrap();
    std::fs::write(directory.path().join("marker"), "original").unwrap();
    let call = |id: &str, name: &str, command: &str| {
        ScriptedTurn::Events(vec![
            ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                id.into(),
                name.into(),
                json!({"command":command}),
            ))),
            ScriptedEvent::Event(ModelEvent::Completed {
                metadata: json!({}),
            }),
        ])
    };
    let provider = provider([
        call("parent-switch", "fs", "git switch -c parent-branch"),
        call("parent-write", "bash", "printf parent-branch > marker"),
        ScriptedTurn::Text("parent ready".into()),
        call("child-switch", "fs", "git switch -c child-branch"),
        call("child-write", "bash", "printf child-branch > marker"),
        ScriptedTurn::Text("child ready".into()),
        call("later-switch", "fs", "git switch -c later-branch"),
        call("later-write", "bash", "printf later-branch > marker"),
        ScriptedTurn::Text("later ready".into()),
    ]);
    let model = Model::new("scripted".into(), "test".into(), "1".into(), Value::Null)?;
    let local = LocalHarness::open(crate::client_local::config(
        directory.path(),
        (model.clone(), provider.clone()),
    )?)
    .await
    .unwrap();
    let Admission::Accepted(RuntimeTask::Durable {
        task_id: parent, ..
    }) = local.submit(OperationId::new(), "parent").await.unwrap()
    else {
        panic!("parent admission")
    };
    let mut client = selected(&local, parent).await.unwrap();
    let child = send(&local, &mut client, "child prompt").await.unwrap();
    let child = TaskId::parse(child["task"].as_str().unwrap())?;
    assert_eq!(local.resume(parent).await.unwrap().text, "parent ready");
    client.refresh_files().await.unwrap();
    let inherited = send(&local, &mut client, "/shell cat marker")
        .await
        .unwrap();
    assert_eq!(
        inherited["stdout"], "parent-branch",
        "child forked the original root view"
    );
    let grandchild = send(&local, &mut client, "grandchild").await.unwrap();
    let grandchild = TaskId::parse(grandchild["task"].as_str().unwrap())?;
    assert_eq!(local.resume(child).await.unwrap().text, "child ready");
    client.refresh_files().await.unwrap();
    let inherited = send(&local, &mut client, "/shell cat marker; printf '\\n'; pwd")
        .await
        .unwrap();
    let previous_mount = inherited["stdout"]
        .as_str()
        .unwrap()
        .strip_prefix("child-branch\n")
        .expect("grandchild forked the original child view")
        .to_owned();
    assert_eq!(
        std::fs::read_to_string(directory.path().join("marker")).unwrap(),
        "parent-branch"
    );
    assert_eq!(
        local.run("switch the root again").await.unwrap().text,
        "later ready"
    );
    assert_eq!(
        std::fs::read_to_string(directory.path().join("marker")).unwrap(),
        "later-branch"
    );
    drop(client);
    drop(local);
    let local = LocalHarness::open(crate::client_local::config(
        directory.path(),
        (model, provider),
    )?)
    .await
    .unwrap();
    let mut client = selected(&local, grandchild).await.unwrap();
    let inherited = send(&local, &mut client, "/shell cat marker; printf '\\n'; pwd")
        .await
        .unwrap();
    let reopened_mount = inherited["stdout"]
        .as_str()
        .unwrap()
        .strip_prefix("child-branch\n")
        .expect("reopening rebound an existing task to its parent's later branch");
    assert_ne!(
        reopened_mount, previous_mount,
        "a retiring mount must never share the replacement mount's namespace"
    );
    Ok(())
}
