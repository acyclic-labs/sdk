//! End to end per exposed family: the model sees its family's declarations, a
//! native call runs the canonical capability under the capability's grant,
//! and the result returns under the native call byte for byte.
use super::*;
use crate::model::NativeToolProfile;

/// The canonical capability: records the call it was handed and echoes it.
struct Canonical(Mutex<Vec<ToolInvocation>>);

impl crate::tool::ToolExecutor for Canonical {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _context: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        let value = json!({"ran":invocation.name,"arguments":invocation.arguments});
        self.0.lock().unwrap().push(invocation);
        Box::pin(async move { Ok(ToolResult { value }) })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _context: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

/// The stock capabilities under their catalog input contracts, run by `canonical`.
fn capabilities(canonical: &Arc<Canonical>) -> Result<ToolRegistry> {
    let mut tools = ToolRegistry::default();
    for spec in crate::tools::catalog() {
        if !["read", "write", "edit", "shell"].contains(&spec.name) {
            continue;
        }
        let mut definition = spec.definition()?;
        definition.output_schema = json!({});
        definition.projection_schema = crate::tool::json_projection_schema(json!({}));
        tools.register(
            (
                crate::tool::Tool {
                    hook_names: &[],
                    definition,
                    executor: canonical.clone(),
                    projection: Arc::new(Projection),
                },
                None,
            ),
            &mut None,
            None,
        )?;
    }
    Ok(tools)
}

fn native_call(call_id: &str, name: &str, arguments: Value) -> ModelEvent {
    ModelEvent::ToolCall(crate::model::ToolCall::new(
        call_id.into(),
        name.into(),
        arguments,
    ))
}

async fn run(
    profile: NativeToolProfile,
    calls: Vec<ModelEvent>,
) -> Result<(Vec<String>, Vec<ToolInvocation>, String)> {
    let canonical = Arc::new(Canonical(Mutex::new(Vec::new())));
    let mut tools = capabilities(&canonical)?;
    crate::tools::native::bind(&mut tools, profile)?;
    let model = Arc::new(
        ScriptedModelProvider::new([tool_turn(calls), done_turn()])
            .with_token_counter(fixture_token_count),
    );
    let executor = uncompacted_executor(
        Model::new("fixture".into(), "exact".into(), "1".into())?,
        model.clone(),
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(
        RuntimeScope::new(
            Capabilities::new(
                ["read", "write", "edit", "shell"].map(crate::contract::capability::tool_call),
            ),
            Limits::default(),
        )?,
        allowed()?,
    )?;
    let output = executor
        .execute(
            TurnInput {
                operation_id: OperationId::new(),
                input: ModelContent::Text("user".into()),
                selected_context: None,
            },
            &Journal::default(),
        )
        .await?;
    assert_eq!(output.text, "done");
    let requests = model.requests();
    let offered = requests[0]
        .tools
        .iter()
        .map(|tool| tool.name.clone())
        .collect();
    let results: Vec<_> = requests[1]
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter(|part| matches!(part, ModelContentPart::ToolResult { .. }))
        .collect();
    let results =
        serde_json::to_string(&results).map_err(|error| Error::Invalid(error.to_string()))?;
    let ran = std::mem::take(&mut *canonical.0.lock().unwrap());
    Ok((offered, ran, results))
}

fn ran(invocations: &[ToolInvocation]) -> Vec<(String, String, Value)> {
    invocations
        .iter()
        .map(|call| {
            (
                call.call_id.clone(),
                call.name.clone(),
                call.arguments.clone(),
            )
        })
        .collect()
}

#[tokio::test]
async fn anthropic_native_calls_run_their_capabilities() -> Result<()> {
    let (offered, invocations, results) = run(
        NativeToolProfile::Anthropic,
        vec![
            native_call(
                "toolu_1",
                "Read",
                json!({"file_path":"src/lib.rs","offset":3,"limit":2}),
            ),
            native_call(
                "toolu_2",
                "NotebookEdit",
                json!({"notebook_path":"n.ipynb","new_source":"x","cell_type":"code","edit_mode":"insert"}),
            ),
            native_call("toolu_3", "Bash", json!({"command":"ls","timeout":5000})),
            native_call(
                "toolu_4",
                "Bash",
                json!({"command":"sleep 9","run_in_background":true}),
            ),
            native_call("toolu_5", "read", json!({"path":"src/lib.rs"})),
        ],
    )
    .await?;
    // Grants name capabilities; each is offered as its family's declarations.
    assert_eq!(offered, ["Edit", "NotebookEdit", "Read", "Bash", "Write"]);
    let inserted_id = invocations[1].operation_id.to_string();
    assert_eq!(
        ran(&invocations),
        [
            (
                "toolu_1".into(),
                "read".into(),
                json!({"path":"src/lib.rs","selection":{"kind":"lines","offset":2,"limit":2}}),
            ),
            (
                "toolu_2".into(),
                "edit".into(),
                json!({"path":"n.ipynb","notebook":{"kind":"insert","cell":{"kind":"index","index":0},"id":inserted_id,"cell_type":"code","source":"x"}}),
            ),
            (
                "toolu_3".into(),
                "shell".into(),
                json!({"command":"ls","timeout_ms":5000}),
            ),
        ]
    );
    assert_eq!(
        results,
        // The operation UUID is runtime-owned, while the surrounding wire
        // result and the model's call ID remain fixed by the scripted call.
        include_str!("native_tools/anthropic.json")
            .trim_end()
            .replace("\"id\":\"toolu_2\"", &format!("\"id\":\"{inserted_id}\""))
    );
    Ok(())
}

#[tokio::test]
async fn google_native_calls_run_their_capabilities() -> Result<()> {
    let (offered, invocations, results) = run(
        NativeToolProfile::Google,
        vec![
            native_call(
                "call_1",
                "read_file",
                json!({"file_path":"src/lib.rs","start_line":3,"end_line":4}),
            ),
            native_call(
                "call_2",
                "replace",
                json!({"file_path":"a.txt","instruction":"fix","old_string":"a","new_string":"b","allow_multiple":true}),
            ),
            native_call(
                "call_3",
                "run_shell_command",
                json!({"command":"ls","dir_path":"src"}),
            ),
        ],
    )
    .await?;
    assert_eq!(
        offered,
        ["replace", "read_file", "run_shell_command", "write_file"]
    );
    assert_eq!(
        ran(&invocations),
        [
            (
                "call_1".into(),
                "read".into(),
                json!({"path":"src/lib.rs","selection":{"kind":"lines","offset":2,"limit":2}}),
            ),
            (
                "call_2".into(),
                "edit".into(),
                json!({"path":"a.txt","old_text":"a","new_text":"b","replace_all":true}),
            ),
            (
                "call_3".into(),
                "shell".into(),
                json!({"command":"ls","cwd":"src"}),
            ),
        ]
    );
    assert_eq!(results, include_str!("native_tools/google.json").trim_end());
    Ok(())
}

#[tokio::test]
async fn openai_stays_on_canonical_tools_until_its_engine_lands() -> Result<()> {
    let (offered, invocations, _) = run(
        NativeToolProfile::Openai,
        vec![native_call("call_1", "read", json!({"path":"src/lib.rs"}))],
    )
    .await?;
    assert_eq!(offered, ["edit", "read", "shell", "write"]);
    assert_eq!(
        ran(&invocations),
        [("call_1".into(), "read".into(), json!({"path":"src/lib.rs"}))]
    );
    Ok(())
}

#[test]
fn native_declarations_run_the_revisions_a_manifest_pins() -> Result<()> {
    let canonical = Arc::new(Canonical(Mutex::new(Vec::new())));
    let mut tools = capabilities(&canonical)?;
    let manifest = tools.definitions(None)?;
    let pinned = manifest
        .iter()
        .find(|tool| tool.name == "read")
        .map(|tool| tool.revision.clone())
        .ok_or_else(|| Error::NotFound("read".into()))?;
    // A later revision of the capability is registered after the manifest.
    let mut later = tools
        .get("read", None, None)?
        .ok_or_else(|| Error::NotFound("read".into()))?
        .clone();
    later.definition.revision = "later".into();
    tools.register((later, None), &mut None, None)?;
    let native = crate::tools::native::NativeTools::new(NativeToolProfile::Anthropic)?;
    let grants = Capabilities::new(
        ["read", "write", "edit", "shell"].map(crate::contract::capability::tool_call),
    );
    let offered = |tools: &ToolRegistry| -> Result<Vec<String>> {
        Ok(tools
            .definitions_for_grants(&grants, None)?
            .into_iter()
            .map(|tool| tool.name)
            .collect())
    };
    let runs = |tools: &ToolRegistry| -> Result<Option<String>> {
        Ok(tools.get("Read", None, None)?.and_then(|tool| {
            tool.executor
                .capability()
                .map(|capability| capability.definition.revision.clone())
        }))
    };
    let mut whole = tools.clone();
    whole.pin_model_definitions(&manifest, None)?;
    native.bind(&mut whole)?;
    assert_eq!(
        offered(&whole)?,
        ["Edit", "NotebookEdit", "Read", "Bash", "Write"]
    );
    assert_eq!(runs(&whole)?, Some(pinned));
    // A manifest naming one capability offers only its declarations.
    let mut read = tools.clone();
    read.pin_model_definitions(
        &manifest
            .iter()
            .filter(|tool| tool.name == "read")
            .cloned()
            .collect::<Vec<_>>(),
        None,
    )?;
    native.bind(&mut read)?;
    assert_eq!(offered(&read)?, ["Read"]);
    // A bound registry is never pinned again.
    assert!(matches!(
        whole.pin_model_definitions(&manifest, None),
        Err(Error::Conflict(_))
    ));
    Ok(())
}
