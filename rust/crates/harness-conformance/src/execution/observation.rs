//! Raw executor observations. This path does not receive scenario expectations.
use super::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// Executor output and journal facts, without a host-side pass/fail decision.
pub struct Observation {
    /// Serialized successful result or the executor's diagnostic.
    pub result: Result<Value, String>,
    /// Loaded final text for successful execution.
    pub text: Option<String>,
    /// Outcome of each requested interrupted attempt.
    pub interruptions: Vec<Result<(), String>>,
    /// A replay mismatch or unexpected provider dispatch.
    pub replay_error: Option<String>,
    /// Complete model requests in dispatch order.
    pub requests: Vec<Value>,
    /// Loaded request attachments in message order.
    pub request_files: Vec<Vec<String>>,
    /// Model reconciliation call count.
    pub reconciliations: usize,
    /// Unconsumed scripted turns.
    pub unused_turns: usize,
    /// Unconsumed scripted reconciliation replies.
    pub unused_recoveries: usize,
    /// Complete execution events with model payloads loaded.
    pub events: Vec<Value>,
    /// Requested fs path observations, preserving absence and read failure.
    pub files: BTreeMap<String, Result<Option<String>, String>>,
}

/// Input is a resolved corpus recipe with `expect` removed, plus paths to inspect.
pub async fn collect(recipe: Value, paths: Vec<String>) -> TestResult<Observation> {
    let storage = workspace(&BTreeMap::new()).await?;
    collect_in(recipe, paths, storage).await
}

/// Uses the host's actual fs and Streams providers for the executor journal,
/// content and context discovery, rather than hiding memory providers in compute.
pub async fn collect_on<P, A, O>(
    recipe: Value,
    paths: Vec<String>,
    filesystem: Fs<A, O>,
    stream: Arc<P>,
) -> TestResult<Observation>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let storage =
        MemoryHarnessStorage::from_providers(AgentId::new(), 1_048_576, filesystem, stream).await?;
    collect_in(recipe, paths, storage).await
}

async fn collect_in<P, A, O>(
    mut recipe: Value,
    paths: Vec<String>,
    storage: MemoryHarnessStorage<P, A, O>,
) -> TestResult<Observation>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    let object = recipe.as_object_mut().ok_or("scenario must be an object")?;
    if object.contains_key("expect") {
        return Err("host fixture must not contain expectations".into());
    }
    object.remove("driver");
    // The parser validates the fixture schema; this placeholder never serves as
    // an oracle and is not used to choose execution behaviour.
    object.insert("expect".into(), serde_json::json!({"text":""}));
    let mut case = Scenario::from_value(recipe)?;
    let limits = case.execution_limits()?;
    publish(&storage, &case.seed).await?;
    let (executor, provider, source) = configure(&mut case, limits)?;
    let input = TurnInput {
        operation_id: OperationId::new(),
        input: ModelContent::Text(case.take_instruction()?),
        selected_context: None,
        max_steps: case.max_steps.unwrap_or(16),
    };
    let operation = input.operation_id;
    let journal = storage.journal();
    let configured = if let Some(discovery) = &case.discovery {
        discovery
            .stage(&storage)
            .await
            .and_then(|stage| executor.with_context_stage(Arc::new(stage)))
    } else {
        Ok(executor)
    };
    let execution = match configured {
        Err(error) => Execution {
            result: Err(error.to_string()),
            text: None,
            interruptions: vec![],
            replay_error: None,
        },
        Ok(executor) => {
            run_attempts(&executor, &provider, journal.as_ref(), input, source, &case).await
        }
    };
    let requests = provider.requests();
    let mut request_files = Vec::new();
    for request in &requests {
        let mut contents = Vec::new();
        for file in request
            .messages
            .iter()
            .flat_map(|message| message.content.file_refs())
        {
            contents.push(String::from_utf8(
                journal.load(file, None)?.await?.to_vec(),
            )?);
        }
        request_files.push(contents);
    }
    let mut files = BTreeMap::new();
    for path in paths {
        let content = match storage
            .read_delegated_path(storage.owner_scope(), "", &path, None)
            .await
        {
            Ok((_, bytes)) => String::from_utf8(bytes.to_vec())
                .map(Some)
                .map_err(|e| e.to_string()),
            Err(acyclic_harness::Error::NotFound(_)) => Ok(None),
            Err(error) => Err(error.to_string()),
        };
        files.insert(path, content);
    }
    Ok(Observation {
        result: execution.result,
        text: execution.text,
        interruptions: execution.interruptions,
        replay_error: execution.replay_error,
        requests: requests
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<_, _>>()?,
        request_files,
        reconciliations: provider.reconciliations().len(),
        unused_turns: provider.remaining(),
        unused_recoveries: provider.recoveries().len(),
        events: replay(journal.as_ref(), operation).await?,
        files,
    })
}

struct Execution {
    result: Result<Value, String>,
    text: Option<String>,
    interruptions: Vec<Result<(), String>>,
    replay_error: Option<String>,
}

async fn run_attempts(
    executor: &StockExecutor,
    provider: &ScriptedModelProvider,
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    input: TurnInput,
    source: Option<Context>,
    case: &Scenario,
) -> Execution {
    let mut execution = Execution {
        result: Err("not executed".into()),
        text: None,
        interruptions: vec![],
        replay_error: None,
    };
    for _ in &case.resume {
        execution.interruptions.push(
            attempt(executor, journal, input.clone(), source.clone())
                .await
                .map(|_| ())
                .map_err(|e| e.to_string()),
        );
    }
    execution.result = match attempt(executor, journal, input.clone(), source.clone()).await {
        Err(error) => Err(error.to_string()),
        Ok((value, text)) => {
            if case.replay {
                let counts = (provider.requests().len(), provider.reconciliations().len());
                match attempt(executor, journal, input, source).await {
                    Ok((replayed, output))
                        if replayed == value
                            && output == text
                            && counts
                                == (
                                    provider.requests().len(),
                                    provider.reconciliations().len(),
                                ) => {}
                    actual => {
                        execution.replay_error = Some(format!(
                            "replay changed result or dispatched provider: {actual:?}"
                        ));
                    }
                }
            }
            execution.text = Some(text);
            Ok(value)
        }
    };
    execution
}

async fn attempt(
    executor: &StockExecutor,
    journal: &dyn acyclic_harness::executor::ExecutionJournal,
    input: TurnInput,
    source: Option<Context>,
) -> acyclic_harness::Result<(Value, String)> {
    let (value, text) = if let Some(source) = source {
        let result = executor
            .summarize(journal, input.operation_id, source, input.input)
            .await?;
        let text = String::from_utf8(journal.load(&result.output, None)?.await?.to_vec())
            .map_err(|e| acyclic_harness::Error::Invalid(e.to_string()))?;
        (serde_json::to_value(result), text)
    } else {
        let result = executor.execute(input, journal).await?;
        (serde_json::to_value(&result), result.text)
    };
    Ok((
        value.map_err(|e| acyclic_harness::Error::Invalid(e.to_string()))?,
        text,
    ))
}

/// Applies the existing corpus oracles to observations obtained from any host.
pub(crate) fn check(case: &Scenario, actual: &Observation) -> TestResult {
    let expected = &case.expect;
    check_outcome(case, actual)?;
    check_script(expected, actual)?;
    check_transcript(expected, actual)?;
    for (path, expected) in &expected.files {
        if actual.files.get(path) != Some(&Ok(expected.clone())) {
            return Err(format!(
                "file {path}: expected {expected:?}, got {:?}",
                actual.files.get(path)
            )
            .into());
        }
    }
    Ok(())
}

fn check_outcome(case: &Scenario, actual: &Observation) -> TestResult {
    let expected = &case.expect;
    if actual.interruptions.len() != case.resume.len() {
        return Err("interruption observation count differs".into());
    }
    for (expected, actual) in case.resume.iter().zip(&actual.interruptions) {
        if !matches!(actual, Err(error) if error.contains(expected)) {
            return Err(
                format!("expected interrupted attempt {expected:?}, got {actual:?}").into(),
            );
        }
    }
    match (&expected.error, &actual.result) {
        (Some(expected), Err(error)) if error.contains(expected) => {}
        (None, Ok(value)) => {
            if let Some(expected) = &expected.result
                && !matchers::subset(expected, value)
            {
                return Err(format!("result: expected {expected}, got {value}").into());
            }
            if expected.text != actual.text {
                return Err(
                    format!("text: expected {:?}, got {:?}", expected.text, actual.text).into(),
                );
            }
        }
        _ => return Err(format!("unexpected result: {:?}", actual.result).into()),
    }
    if let Some(error) = &actual.replay_error {
        return Err(error.clone().into());
    }
    Ok(())
}

fn check_script(expected: &Expectations, actual: &Observation) -> TestResult {
    if actual.unused_turns != 0 || actual.unused_recoveries != 0 {
        return Err(format!(
            "unused scripted input: {} turns, {} recoveries",
            actual.unused_turns, actual.unused_recoveries
        )
        .into());
    }
    for (label, expected, actual) in [
        ("requests", expected.request_count, actual.requests.len()),
        (
            "reconciliations",
            expected.reconciliation_count,
            actual.reconciliations,
        ),
    ] {
        if expected.is_some_and(|expected| expected != actual) {
            return Err(format!("{label}: expected {expected:?}, got {actual}").into());
        }
    }
    Ok(())
}

fn check_transcript(expected: &Expectations, actual: &Observation) -> TestResult {
    for (label, expected, forbidden, observed) in [
        (
            "requests",
            &expected.requests,
            &expected.not_requests,
            &actual.requests,
        ),
        (
            "events",
            &expected.events,
            &expected.not_events,
            &actual.events,
        ),
    ] {
        if let Some(expected) = expected {
            matchers::ordered(expected, observed, label)?;
        }
        for forbidden in forbidden {
            if observed
                .iter()
                .any(|value| matchers::subset(forbidden, value))
            {
                return Err(format!("forbidden {label}: {forbidden}").into());
            }
        }
    }
    if !expected.matches_model_events(&actual.events) {
        return Err("model events differ".into());
    }
    if let Some(expected) = &expected.request_files
        && expected != &actual.request_files
    {
        return Err(format!(
            "request_files: expected {expected:?}, got {:?}",
            actual.request_files
        )
        .into());
    }
    Ok(())
}
