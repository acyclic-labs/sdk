use super::*;
use crate::{
    model::{ModelContentPart, ToolCall},
    native_settings::SettingsConfig,
};

// Test-only leaf lock; record calls synchronously and never hold it across IO.
struct RecordingTool(Arc<Mutex<Vec<Value>>>);

impl ToolExecutor for RecordingTool {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(invocation.arguments.clone());
            Ok(ToolResult {
                value: invocation.arguments,
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { panic!("completed custom tool was reconciled") })
    }
}

fn guarded(name: &str, aliases: &'static [&'static str], calls: &Arc<Mutex<Vec<Value>>>) -> Tool {
    let schema = json!({"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false});
    Tool {
        hook_names: aliases,
        definition: ToolDefinition {
            name: name.into(),
            revision: "1".into(),
            description: "record admitted integer".into(),
            input_schema: schema.clone(),
            output_schema: schema.clone(),
            projection_schema: crate::tool::json_projection_schema(schema),
        },
        executor: Arc::new(RecordingTool(calls.clone())),
        projection: Arc::new(JsonToolProjection),
    }
}

fn calls(names: &[&str]) -> ScriptedTurn {
    let mut events: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let mut call =
                ToolCall::new(format!("call-{index}"), (*name).into(), json!({"value":7}));
            call.metadata = json!({"opaque_vendor_signature":"preserve-me"});
            ScriptedEvent::Event(ModelEvent::ToolCall(call))
        })
        .collect();
    events.push(ScriptedEvent::Event(ModelEvent::Completed {
        metadata: Value::Null,
    }));
    ScriptedTurn::Events(events)
}

fn configured(
    root: &Path,
    provider: Arc<ScriptedModelProvider>,
    tools: Vec<Tool>,
) -> Result<LocalHarnessConfig> {
    let workspace = root.join("workspace");
    let home = root.join("home");
    Ok(checkout_config(&workspace, (model()?, provider))?
        .state_directory(root.join("state"))
        .settings(SettingsConfig::new(&workspace, &home).codex_home(home.join(".codex")))
        .without_default_tools()
        .tools(tools))
}

fn fixture(root: &Path) -> Result<std::path::PathBuf> {
    std::fs::create_dir(root.join("workspace")).map_err(super::super::storage)?;
    std::fs::create_dir_all(root.join("home/.claude")).map_err(super::super::storage)?;
    let script = root.join("guard.sh");
    std::fs::write(&script, "set -eu\ncat > \"$1-input.json\"\nprintf '%s\\n' \"$1\" >> hook-runs\nprintf '%s' \"$2\"\n")
        .map_err(super::super::storage)?;
    Ok(script)
}

fn rule(script: &Path, name: &str, output: Value) -> Value {
    json!({"type":"command","command":"sh","args":[script, name, output.to_string()],"onFailure":"block"})
}

#[tokio::test]
async fn guards_use_aliases_revalidate_replacements_and_retain_post_tool_feedback() -> Result<()> {
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let script = fixture(root.path())?;
    let settings = json!({"hooks":{
        "PreToolUse":[
            {"matcher":"Bash","hooks":[rule(&script,"deny",json!({"hookSpecificOutput":{"permissionDecision":"deny","permissionDecisionReason":"guard denied"}}))]},
            {"matcher":"Edit","hooks":[rule(&script,"replace",json!({"hookSpecificOutput":{"updatedInput":{"value":42},"additionalContext":"pre-feedback"}}))]},
            {"matcher":"Read","hooks":[{"type":"command","command":"missing-report-guard","args":[]}]},
            {"matcher":"Write","hooks":[rule(&script,"invalid",json!({"hookSpecificOutput":{"updatedInput":{"value":false}}}))]}
        ],
        "PostToolUse":[{"matcher":"Edit","hooks":[rule(&script,"post",json!({"decision":"block","reason":"Fix lint errors"}))]}]
    }});
    std::fs::write(
        root.path().join("home/.claude/settings.json"),
        settings.to_string(),
    )
    .map_err(super::super::storage)?;
    let executed = Arc::new(Mutex::new(Vec::new()));
    let provider = scripted([
        calls(&["blocked", "changed", "reported"]),
        ScriptedTurn::Text("finished".into()),
        calls(&["invalid"]),
        ScriptedTurn::Text("invalid replacement rejected".into()),
    ]);
    let tools = vec![
        guarded("blocked", &["Bash"], &executed),
        guarded("changed", &["Edit"], &executed),
        guarded("reported", &["Read"], &executed),
        guarded("invalid", &["Write"], &executed),
    ];
    let harness = LocalHarness::open(configured(root.path(), provider.clone(), tools)?).await?;
    let task = super::context::admitted(harness.submit(OperationId::new(), "guard tools").await?)?;
    let output = harness.resume(task).await?;
    assert_eq!(output.text, "finished");
    assert_eq!(harness.resume(task).await?, output);
    assert_eq!(
        *executed.lock().unwrap(),
        vec![json!({"value":42}), json!({"value":7})]
    );
    let requests = provider.requests();
    let followup = serde_json::to_string(&requests[1]).map_err(super::super::storage)?;
    for feedback in [
        "guard denied",
        "pre-feedback",
        "Fix lint errors",
        "preserve-me",
    ] {
        assert!(
            followup.contains(feedback),
            "missing {feedback}: {followup}"
        );
    }
    let post: Value = serde_json::from_slice(
        &std::fs::read(root.path().join("workspace/post-input.json"))
            .map_err(super::super::storage)?,
    )
    .map_err(super::super::storage)?;
    assert_eq!(post["tool_input"], json!({"value":42}));
    assert_eq!(post["tool_response"], json!({"value":42}));
    assert_eq!(post["tool_use_id"], "call-1");
    let tool_reply = requests[1]
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .find(
            |part| matches!(part, ModelContentPart::ToolResult { name, .. } if name == "changed"),
        );
    assert!(tool_reply.is_some());
    assert_eq!(
        std::fs::read_to_string(root.path().join("workspace/hook-runs"))
            .map_err(super::super::storage)?,
        "deny\nreplace\npost\n"
    );
    let invalid = super::context::admitted(
        harness
            .submit(OperationId::new(), "invalid replacement")
            .await?,
    )?;
    assert_eq!(
        harness.resume(invalid).await?.text,
        "invalid replacement rejected"
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 4);
    let rejection = serde_json::to_string(&requests[3]).map_err(super::super::storage)?;
    assert!(
        rejection.contains("invalid"),
        "missing model-visible rejection: {rejection}"
    );
    assert_eq!(
        executed.lock().unwrap().len(),
        2,
        "invalid replacement executed"
    );
    harness.close().await?;
    Ok(())
}

#[tokio::test]
async fn reopening_after_tool_hooks_reuses_both_phases_and_the_effective_invocation() -> Result<()>
{
    use futures::StreamExt;
    let root = tempfile::tempdir().map_err(super::super::storage)?;
    let script = fixture(root.path())?;
    let settings = json!({"hooks":{
        "PreToolUse":[{"matcher":"Edit","hooks":[rule(&script,"pre",json!({"hookSpecificOutput":{"updatedInput":{"value":42}}}))]}],
        "PostToolUse":[{"matcher":"Edit","hooks":[rule(&script,"post",json!({"decision":"block","reason":"retained-post-feedback"}))]}]
    }});
    let path = root.path().join("home/.claude/settings.json");
    std::fs::write(&path, settings.to_string()).map_err(super::super::storage)?;
    let executed = Arc::new(Mutex::new(Vec::new()));
    let (signal, mut received) = futures::channel::mpsc::unbounded();
    let provider = Arc::new(
        ScriptedModelProvider::new([
            calls(&["changed"]),
            ScriptedTurn::Events(vec![ScriptedEvent::Pending {}]),
        ])
        .with_context_capacity(capacity())
        .with_dispatch_observer(move |_| {
            signal.unbounded_send(()).unwrap();
        }),
    );
    let config = |provider| {
        configured(
            root.path(),
            provider,
            vec![guarded("changed", &["Edit"], &executed)],
        )
    };
    let harness = LocalHarness::open(config(provider.clone())?).await?;
    let task = super::context::admitted(
        harness
            .submit(OperationId::new(), "recover guarded call")
            .await?,
    )?;
    let mut pending = Box::pin(harness.resume(task));
    tokio::select! {
        _ = async { assert_eq!(received.next().await, Some(())); assert_eq!(received.next().await, Some(())); } => {},
        result = &mut pending => panic!("did not reach the post-tool provider cut: {result:?}"),
    }
    drop(pending);
    let backend = harness.workspace().unwrap().store.clone();
    drop(harness);
    std::fs::remove_file(&path).map_err(super::super::storage)?;
    std::fs::remove_file(&script).map_err(super::super::storage)?;
    let recovered = Arc::new(
        ScriptedModelProvider::new([])
            .with_context_capacity(capacity())
            .with_reconciliations([ScriptedReconciliation {
                orphan: true,
                observed: vec![],
                continuation: Some(vec![
                    ModelEvent::Content {
                        delta: "recovered".into(),
                    },
                    ModelEvent::Completed {
                        metadata: Value::Null,
                    },
                ]),
            }]),
    );
    let harness = LocalHarness::open(config(recovered.clone())?).await?;
    drop(backend);
    assert_eq!(harness.resume(task).await?.text, "recovered");
    assert_eq!(*executed.lock().unwrap(), vec![json!({"value":42})]);
    assert!(recovered.requests().is_empty());
    assert_eq!(recovered.reconciliations().len(), 1);
    assert_eq!(
        std::fs::read_to_string(root.path().join("workspace/hook-runs"))
            .map_err(super::super::storage)?,
        "pre\npost\n"
    );
    let requests = provider.requests();
    let body = serde_json::to_string(&requests[1]).map_err(super::super::storage)?;
    assert!(body.contains("retained-post-feedback"));
    Ok(())
}
