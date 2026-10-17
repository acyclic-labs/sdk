use super::*;

// Returns the file version named in its arguments, or fails when asked to,
// and reports results the way file tools do: the version under `file`.
struct Observer;

impl crate::tool::ToolExecutor for Observer {
    fn observed_file(&self, result: &ToolResult) -> Option<FileRef> {
        serde_json::from_value(result.value.get("file")?.clone()).ok()
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        _context: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            if invocation.arguments["fail"] == json!(true) {
                return Err(Error::Invalid("offset is past the end of the file".into()));
            }
            Ok(ToolResult {
                value: json!({"file": invocation.arguments["file"]}),
            })
        })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _context: Option<crate::runtime::ToolContext>,
    ) -> BoxFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

fn version(path: &str, bytes: &[u8]) -> Result<FileRef> {
    let volume = crate::conversation::VolumeRef::new(
        crate::resources::ProviderRef::new(
            "observed".into(),
            "filesystem".into(),
            "1".into(),
            &mut None,
        )?,
        "project".into(),
        VolumeClass::Project,
        crate::conversation::VolumeOwner::Project("observed".into()),
        &mut None,
    )?;
    FileRef::new(
        volume,
        path.into(),
        "generation".into(),
        crate::conversation::FileDescriptor::from_bytes(bytes, "text/plain", &mut None)?,
        path.replace('/', "-"),
        &mut None,
    )
}

async fn run(calls: &[(&str, &FileRef, bool)]) -> Result<(StockExecutor, Journal, OperationId)> {
    let events = calls
        .iter()
        .map(|(call, file, fail)| echo_call(call, json!({"file": file, "fail": fail})));
    let model = ScriptedModelProvider::new([tool_turn(events), done_turn()])
        .with_token_counter(fixture_token_count);
    let tools = echo_tools(
        Arc::new(Observer),
        json!({"type":"object"}),
        json!({"type":"object"}),
        crate::tool::json_projection_schema(json!({"type":"object"})),
    )?;
    let executor = uncompacted_executor(
        Model::new("example".into(), "model".into(), "1".into(), Value::Null)?,
        Arc::new(model),
        ContextPipeline::default(),
        tools,
    )
    .with_tool_authority(
        RuntimeScope::new(
            Capabilities::new(["tool:call:example.echo"]),
            Limits::default(),
        )?,
        None,
    )?;
    let journal = Journal::default();
    let operation = OperationId::from_bytes([7; 16]);
    let input = TurnInput {
        operation_id: operation,
        input: ModelContent::Text("edit".into()),
        selected_context: None,
        max_steps: 4,
    };
    // The turn's own outcome is irrelevant: only its journal matters here.
    let _ = executor.execute(input, &journal).await;
    Ok((executor, journal, operation))
}

fn reopen(journal: &Journal) -> Journal {
    Journal(
        Mutex::new(journal.0.lock().unwrap().clone()),
        Mutex::new(journal.1.lock().unwrap().clone()),
        Mutex::new(journal.2.lock().unwrap().clone()),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
        AtomicUsize::new(0),
    )
}

// What the model observed is derived from the durable journal alone: a fresh
// executor over a reopened journal (a restart) derives the same versions, and
// a failed call never advances one.
#[tokio::test]
async fn observed_versions_survive_restart_and_ignore_failed_calls() -> Result<()> {
    let (a1, a2, b1) = (
        version("plans/a.md", b"v1")?,
        version("plans/a.md", b"v2")?,
        version("plans/b.md", b"v1")?,
    );
    let (executor, journal, operation) = run(&[
        ("read-a", &a1, false),
        ("read-b", &b1, false),
        ("past-eof", &a2, true),
    ])
    .await?;
    let restarted = reopen(&journal);
    let observed = StockExecution::ordinary(&executor)
        .observed_files(&restarted, operation)
        .await?;
    assert_eq!(observed, [a1.clone(), b1.clone()]);
    // Negative control: the same call succeeding does advance the version.
    let (executor, journal, operation) = run(&[
        ("read-a", &a1, false),
        ("read-b", &b1, false),
        ("reread", &a2, false),
    ])
    .await?;
    let observed = StockExecution::ordinary(&executor)
        .observed_files(&reopen(&journal), operation)
        .await?;
    assert_eq!(observed, [b1, a2]);
    // Negative control: losing the journal loses the observation, so nothing
    // but the journal backs it.
    let observed = StockExecution::ordinary(&executor)
        .observed_files(&Journal::default(), operation)
        .await?;
    assert!(observed.is_empty());
    Ok(())
}
