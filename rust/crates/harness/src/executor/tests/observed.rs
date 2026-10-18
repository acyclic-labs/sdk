use super::*;
use crate::executor::observed::ObservedFold;

// Returns the file version named in its arguments, or fails when asked to,
// and reports results the way file tools do: the version under `file`.
struct Observer;

impl crate::tool::ToolExecutor for Observer {
    fn reports_observed_files(&self) -> bool {
        true
    }

    fn uses_observed_files(&self) -> bool {
        true
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
    let scope = RuntimeScope::new(
        Capabilities::new([
            "task:spawn:context-probe@1",
            "model:generate",
            "tool:call:example.echo",
        ]),
        Limits::default(),
    )?;
    let context = capture_admitted_context(scope.clone()).await?;
    let operation = OperationId::from_bytes([7; 16]);
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
    .with_tool_authority(scope, None)?
    .with_task_context(&context, operation)?;
    let journal = Journal::default();
    let input = TurnInput {
        operation_id: operation,
        input: ModelContent::Text("edit".into()),
        selected_context: None,
        max_steps: 4,
    };
    let outcome = executor.execute(input, &journal).await;
    if calls.iter().any(|(_, _, fail)| *fail) {
        assert!(outcome.is_err(), "the requested failed read must fail");
    } else {
        assert_eq!(outcome?.text, "done");
    }
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
        .observed_files(&restarted, operation, &mut ObservedFold::default())
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
        .observed_files(&reopen(&journal), operation, &mut ObservedFold::default())
        .await?;
    assert_eq!(observed, [b1, a2]);
    // Negative control: losing the journal loses the observation, so nothing
    // but the journal backs it.
    let observed = StockExecution::ordinary(&executor)
        .observed_files(&Journal::default(), operation, &mut ObservedFold::default())
        .await?;
    assert!(observed.is_empty());
    Ok(())
}

async fn turn_loads(edits: usize) -> Result<usize> {
    let a = version("plans/a.md", b"v1")?;
    let ids: Vec<String> = (0..=edits).map(|index| format!("call-{index}")).collect();
    let calls: Vec<(&str, &FileRef, bool)> =
        ids.iter().map(|id| (id.as_str(), &a, false)).collect();
    let (_, journal, _) = run(&calls).await?;
    Ok(journal.4.load(Ordering::SeqCst))
}

// Every call that checks observed versions derives them, yet a turn's journal
// payload loads grow linearly with its calls: each invocation and result is
// loaded once, not once per later call.
#[tokio::test]
async fn deriving_observations_is_linear_in_the_turn() -> Result<()> {
    let (none, fifty, hundred) = (
        turn_loads(0).await?,
        turn_loads(50).await?,
        turn_loads(100).await?,
    );
    assert!(fifty > none);
    assert_eq!(
        hundred - fifty,
        fifty - none,
        "loads {none} {fifty} {hundred}"
    );
    Ok(())
}

// A session continues its fold after the records it already folded; a new
// session (a restart) folds the journal once from its start.
#[tokio::test]
async fn a_session_folds_each_record_once() -> Result<()> {
    let (a1, b1) = (version("plans/a.md", b"v1")?, version("plans/b.md", b"v1")?);
    let (executor, journal, operation) =
        run(&[("read-a", &a1, false), ("read-b", &b1, false)]).await?;
    let journal = reopen(&journal);
    let session = StockExecution::ordinary(&executor);
    let mut observed = ObservedFold::default();
    let before = journal.4.load(Ordering::SeqCst);
    assert_eq!(
        session
            .observed_files(&journal, operation, &mut observed)
            .await?,
        [a1.clone(), b1.clone()]
    );
    // One invocation and one result per completed call.
    let folded = journal.4.load(Ordering::SeqCst);
    assert_eq!(folded - before, 4);
    for _ in 0..10 {
        assert_eq!(
            session
                .observed_files(&journal, operation, &mut observed)
                .await?,
            [a1.clone(), b1.clone()]
        );
    }
    assert_eq!(journal.4.load(Ordering::SeqCst), folded);
    // Negative control: a fresh session per derivation reloads everything,
    // which is exactly the quadratic cost the fold removes.
    for _ in 0..10 {
        StockExecution::ordinary(&executor)
            .observed_files(&journal, operation, &mut ObservedFold::default())
            .await?;
    }
    assert_eq!(journal.4.load(Ordering::SeqCst), folded + 40);
    Ok(())
}
