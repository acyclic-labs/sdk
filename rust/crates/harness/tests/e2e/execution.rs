use super::{
    TestResult,
    expectations::Expectations,
    matchers,
    scenario::{self, Scenario},
};
use acyclic_harness::{
    AgentId, OperationId,
    context::{
        CompactionPolicy, Context, ContextPipeline, ContextPlacement, ContextStage,
        ModelTokenCount, SourceStage,
    },
    executor::{ExecutionEvent, Executor, StockExecutor, TurnInput},
    filesystem::MemoryHarnessStorage,
    model::{Model, ModelContent},
    test_support::ScriptedModelProvider,
    tool::ToolRegistry,
};
use acyclic_stream::{MemoryStream, StreamProvider};
use serde_json::Value;
use std::sync::Arc;

pub(super) async fn run(case: Scenario) -> TestResult {
    run_on(case, Arc::new(MemoryStream::default()))
        .await
        .map(drop)
}

/// A checked scenario's journal, kept for cross-provider comparison.
pub(super) struct Outcome<P> {
    storage: MemoryHarnessStorage<P>,
    operation: OperationId,
}

impl<P: StreamProvider> Outcome<P> {
    /// Event kinds and loaded model events: identical scenario data must
    /// replay the same observations on every Stream provider.
    pub(super) async fn trace(&self) -> TestResult<Vec<Value>> {
        let journal = self.storage.journal();
        Ok(replay(journal.as_ref(), self.operation)
            .await?
            .into_iter()
            .map(|event| match event["kind"].as_str() {
                Some("model") => serde_json::json!({"kind": "model", "event": event["event"]}),
                _ => serde_json::json!({"kind": event["kind"]}),
            })
            .collect())
    }
}

/// Runs one executor scenario with its journal on `stream`.
pub(super) async fn run_on<P: StreamProvider>(
    mut case: Scenario,
    stream: Arc<P>,
) -> TestResult<Outcome<P>> {
    let limits = case.execution_limits()?;
    let storage = workspace_on(&case.seed, stream).await?;
    case.seed.clear();
    let (executor, provider, source) = configure(&mut case, limits)?;
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text(case.take_instruction()?),
        selected_context: None,
        max_steps: case.max_steps.unwrap_or(16),
    };
    let journal = storage.journal();
    let operation = input.operation_id;
    let executor = if let Some(discovery) = &case.discovery {
        discovery
            .stage(&storage)
            .await
            .and_then(|stage| executor.with_context_stage(Arc::new(stage)))
    } else {
        Ok(executor)
    };
    match executor {
        Err(error) => {
            check_result::<()>(&case.expect, Err(error))?;
        }
        Ok(executor) => {
            if let Some(source) = source {
                run_summary(
                    &executor,
                    provider.as_ref(),
                    journal.as_ref(),
                    input,
                    source,
                    &case,
                )
                .await?;
            } else {
                run_turn(&executor, provider.as_ref(), journal.as_ref(), input, &case).await?;
            }
        }
    }
    check_provider(&case.expect, &provider, journal.as_ref()).await?;
    check_events(&case.expect, journal.as_ref(), operation).await?;
    check_files(&case.expect.files, &storage).await?;
    Ok(Outcome { storage, operation })
}

pub(super) async fn workspace(
    seed: &std::collections::BTreeMap<String, String>,
) -> TestResult<MemoryHarnessStorage> {
    workspace_on(seed, Arc::new(MemoryStream::default())).await
}

async fn workspace_on<P: StreamProvider>(
    seed: &std::collections::BTreeMap<String, String>,
    stream: Arc<P>,
) -> TestResult<MemoryHarnessStorage<P>> {
    let storage = MemoryHarnessStorage::with_stream(AgentId::new(), 1_048_576, stream).await?;
    publish(&storage, seed).await?;
    Ok(storage)
}

pub(super) async fn publish<P: StreamProvider>(
    storage: &MemoryHarnessStorage<P>,
    files: &std::collections::BTreeMap<String, String>,
) -> acyclic_harness::Result<()> {
    for (path, contents) in files {
        storage
            .stage(
                OperationId::new(),
                path,
                contents.as_bytes(),
                "text/plain",
                path.rsplit('/').next().unwrap_or_default(),
            )
            .await?;
    }
    Ok(())
}

fn configure(
    case: &mut Scenario,
    limits: acyclic_harness::conversation::Limits,
) -> acyclic_harness::Result<(StockExecutor, Arc<ScriptedModelProvider>, Option<Context>)> {
    let mut provider = ScriptedModelProvider::new(std::mem::take(&mut case.turns))
        .with_reconciliations(std::mem::take(&mut case.recoveries));
    if let Some(capacity) = case.capacity {
        provider = provider.with_context_capacity(capacity);
    }
    if let Some(tokens) = case.message_tokens {
        provider = provider.with_token_counter(move |request| {
            Ok(ModelTokenCount::from_upper_bounds(
                request.request_digest(),
                0,
                vec![tokens; request.request().messages.len()],
            ))
        });
    }
    let provider = Arc::new(provider);
    let mut summary_source = None;
    let context = if case.kind == scenario::Kind::Summary {
        summary_source = Some(Context {
            messages: std::mem::take(&mut case.context),
            ..Context::default()
        });
        ContextPipeline::default()
    } else if case.context.is_empty() {
        ContextPipeline::default()
    } else {
        ContextPipeline::new([Arc::new(SourceStage::new(
            "history",
            "1",
            Arc::new(Context {
                messages: std::mem::take(&mut case.context),
                ..Context::default()
            }),
            ContextPlacement::Prepend,
        )) as Arc<dyn ContextStage>])
    };
    let mut executor = StockExecutor::new(
        Model::new(
            "scripted".into(),
            "scenario".into(),
            "1".into(),
            Value::Null,
        )?,
        provider.clone(),
        context,
        ToolRegistry::default(),
    )
    .with_compaction_policy(case.compaction.take().unwrap_or(CompactionPolicy::Disabled))
    .with_limits(limits);
    if let Some(maximum) = case.max_output_tokens {
        executor = executor.with_max_output_tokens(maximum)?;
    }
    Ok((executor, provider, summary_source))
}

async fn run_summary(
    executor: &StockExecutor,
    provider: &ScriptedModelProvider,
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    input: TurnInput,
    source: Context,
    case: &Scenario,
) -> TestResult {
    let operation = input.operation_id;
    let replay_input = case.replay.then(|| input.clone());
    let replay_source = case.replay.then(|| source.clone());
    for expected_error in &case.resume {
        let attempt = (*executor).clone();
        check_interruption(
            expected_error,
            attempt
                .summarize(journal, operation, source.clone(), input.input.clone())
                .await,
        )?;
    }
    let result = executor
        .summarize(journal, operation, source, input.input)
        .await;
    if let Some(actual) = check_result(&case.expect, result)? {
        let text = journal.load(&actual.output, None)?.await?;
        check_text(&case.expect, text.as_ref())?;
        if let Some(input) = replay_input {
            let counts = (provider.requests().len(), provider.reconciliations().len());
            let replay = executor
                .summarize(
                    journal,
                    operation,
                    replay_source.ok_or("missing replay source")?,
                    input.input,
                )
                .await?;
            if replay != actual
                || counts != (provider.requests().len(), provider.reconciliations().len())
            {
                return Err(
                    "summary replay changed the result or invoked the provider again".into(),
                );
            }
        }
    }
    Ok(())
}

async fn run_turn(
    executor: &StockExecutor,
    provider: &ScriptedModelProvider,
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    input: TurnInput,
    case: &Scenario,
) -> TestResult {
    let replay_input = case.replay.then(|| input.clone());
    for expected_error in &case.resume {
        let attempt = (*executor).clone();
        check_interruption(
            expected_error,
            attempt.execute(input.clone(), journal).await,
        )?;
    }
    let result = executor.execute(input, journal).await;
    if let Some(actual) = check_result(&case.expect, result)? {
        check_text(&case.expect, actual.text.as_bytes())?;
        if let Some(input) = replay_input {
            let requests = provider.requests().len();
            let reconciliations = provider.reconciliations().len();
            let replay = executor.execute(input, journal).await?;
            if replay != actual
                || provider.requests().len() != requests
                || provider.reconciliations().len() != reconciliations
            {
                return Err("replay changed the result or dispatched the model again".into());
            }
        }
    }
    Ok(())
}

async fn check_provider(
    expected: &Expectations,
    provider: &ScriptedModelProvider,
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
) -> TestResult {
    if provider.remaining() != 0 {
        return Err(format!("{} unused scripted turns", provider.remaining()).into());
    }
    let recoveries = provider.recoveries();
    if !recoveries.is_empty() {
        return Err(format!("{} unused reconciliation replies", recoveries.len()).into());
    }
    let requests = provider.requests();
    let reconciliations = provider.reconciliations();
    for (label, expected, actual) in [
        ("requests", expected.request_count, requests.len()),
        (
            "reconciliations",
            expected.reconciliation_count,
            reconciliations.len(),
        ),
    ] {
        if expected.is_some_and(|expected| expected != actual) {
            return Err(format!("{label}: expected {expected:?}, got {actual}").into());
        }
    }
    if expected.requests.is_some() || !expected.not_requests.is_empty() {
        let requests = requests
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(expected) = &expected.requests {
            matchers::ordered(expected, &requests, "requests")?;
        }
        for forbidden in &expected.not_requests {
            if requests
                .iter()
                .any(|request| matchers::subset(forbidden, request))
            {
                return Err(format!("forbidden request: {forbidden}").into());
            }
        }
    }
    if let Some(expected) = &expected.request_files {
        if expected.len() != requests.len() {
            return Err("request_files must match the number of observed requests".into());
        }
        for (index, (expected, request)) in expected.iter().zip(&requests).enumerate() {
            let mut files = request
                .messages
                .iter()
                .flat_map(|message| message.content.file_refs());
            for (file_index, expected) in expected.iter().enumerate() {
                let file = files
                    .next()
                    .ok_or_else(|| format!("request_files[{index}][{file_index}]: missing file"))?;
                let actual = journal.load(file, None)?.await?;
                if actual.as_ref() != expected.as_bytes() {
                    return Err(format!(
                        "request_files[{index}][{file_index}]: expected {expected:?}, got {:?}",
                        String::from_utf8_lossy(&actual)
                    )
                    .into());
                }
            }
            if files.next().is_some() {
                return Err(format!("request_files[{index}]: unexpected file").into());
            }
        }
    }
    Ok(())
}

async fn check_events(
    expected: &Expectations,
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    operation: OperationId,
) -> TestResult {
    if expected.events.is_some()
        || expected.model_events.is_some()
        || !expected.not_events.is_empty()
    {
        let events = replay(journal, operation).await?;
        if let Some(expected) = &expected.events {
            matchers::ordered(expected, &events, "events")?;
        }
        if !expected.matches_model_events(&events) {
            return Err(format!(
                "model events differ: expected {:?}, got {events:?}",
                expected.model_events
            )
            .into());
        }
        for forbidden in &expected.not_events {
            if events
                .iter()
                .any(|event| matchers::subset(forbidden, event))
            {
                return Err(format!("forbidden event: {forbidden}").into());
            }
        }
    }
    Ok(())
}

/// Every journal observation of `operation`, with model events loaded.
async fn replay(
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    operation: OperationId,
) -> TestResult<Vec<Value>> {
    let mut events = Vec::new();
    let mut after = 0;
    loop {
        let records = journal.replay(operation, after, 64, None)?.await?;
        if records.is_empty() {
            return Ok(events);
        }
        for record in records {
            after = record.sequence;
            let mut value = serde_json::to_value(&record.event)?;
            if let ExecutionEvent::Model { event, .. } = &record.event {
                *value.get_mut("event").ok_or("missing model event")? =
                    serde_json::from_slice(&journal.load(event, None)?.await?)?;
            }
            events.push(value);
        }
    }
}

pub(super) async fn check_files<P: StreamProvider>(
    expected: &std::collections::BTreeMap<String, Option<String>>,
    storage: &MemoryHarnessStorage<P>,
) -> TestResult {
    for (path, expected) in expected {
        let actual = storage
            .read_delegated_path(storage.owner_scope(), "", path, None)
            .await;
        match (expected, actual) {
            (Some(expected), Ok((_, actual))) if actual.as_ref() == expected.as_bytes() => {}
            (None, Err(acyclic_harness::Error::NotFound(_))) => {}
            (expected, actual) => {
                return Err(format!("file {path}: expected {expected:?}, got {actual:?}").into());
            }
        }
    }
    Ok(())
}

fn check_interruption<T: std::fmt::Debug>(
    expected: &str,
    result: acyclic_harness::Result<T>,
) -> TestResult {
    match result {
        Err(error) if error.to_string().contains(expected) => Ok(()),
        actual => Err(format!("expected interrupted attempt {expected:?}, got {actual:?}").into()),
    }
}

fn check_result<T: serde::Serialize + std::fmt::Debug>(
    expected: &Expectations,
    result: acyclic_harness::Result<T>,
) -> TestResult<Option<T>> {
    match (&expected.error, result) {
        (None, Ok(actual)) => {
            if let Some(expected) = &expected.result
                && !matchers::subset(expected, &serde_json::to_value(&actual)?)
            {
                return Err(format!("result: expected {expected}, got {actual:?}").into());
            }
            Ok(Some(actual))
        }
        (Some(expected), Err(actual)) if actual.to_string().contains(expected) => Ok(None),
        (_, actual) => Err(format!("unexpected result: {actual:?}").into()),
    }
}

fn check_text(expected: &Expectations, actual: &[u8]) -> TestResult {
    if expected.text.as_deref().map(str::as_bytes) != Some(actual) {
        return Err(format!(
            "text: expected {:?}, got {:?}",
            expected.text,
            String::from_utf8_lossy(actual)
        )
        .into());
    }
    Ok(())
}
