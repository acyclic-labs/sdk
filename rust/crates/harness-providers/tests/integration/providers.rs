use super::{
    TestResult, local_workspace, matchers,
    provider_case::ProviderCase,
    provider_http::{Recorded, resolve},
};
use acyclic_harness::{
    LocalHarness, OperationId,
    context::CompactionPolicy,
    conversation::Limits,
    model::{
        ModelContent, ModelDispatch, ModelEvent, ModelMessage, ModelProvider as _, ModelRequest,
        ModelRole, PreparedModelRequest,
    },
    test_support::{PromptOutcome, local_harness_config, prompt_settled},
};
use acyclic_harness_providers::{HttpRequest, HttpTransport as _};
use futures::TryStreamExt as _;
use serde_json::Value;
use std::sync::Arc;

pub(super) fn check_events(expected: &[Value], events: &[ModelEvent]) -> TestResult {
    if expected.len() != events.len() {
        return Err(format!(
            "provider events differ: expected {} got {}",
            expected.len(),
            events.len()
        )
        .into());
    }
    for (expected, actual) in expected.iter().zip(events) {
        let actual = serde_json::to_value(actual)?;
        let retained: &[&str] = match at(&actual, "/kind").as_str() {
            Some("reasoning_block") => &["/block/continuity", "/block/origin/credential_scope"],
            Some("text_part") => &["/native"],
            _ => &[],
        };
        if !matchers::subset(expected, &actual)
            || retained
                .iter()
                .any(|pointer| at(expected, pointer) != at(&actual, pointer))
        {
            return Err(
                format!("provider events differ: expected {expected}, got {actual}").into(),
            );
        }
    }
    Ok(())
}

/// The value at `pointer`, or null where it is absent, as JSON indexing reads it.
fn at<'a>(value: &'a Value, pointer: &str) -> &'a Value {
    value.pointer(pointer).unwrap_or(&Value::Null)
}

/// Responses that complete a model step; refused or stalled ones fail or are redelivered.
fn completed_steps(case: &ProviderCase) -> usize {
    (0..case.responses.len())
        .filter(|&index| case.completes(index))
        .count()
}

pub(super) async fn parsed(case: &ProviderCase, transport: Arc<Recorded>) -> TestResult {
    let provider = resolve(case, transport.clone())?;
    for (step, expected) in case.expect.events.iter().enumerate() {
        let request = PreparedModelRequest::prepare(
            ModelRequest {
                summary: false,
                compose_tools: false,
                model: provider.model().clone(),
                messages: vec![ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text(case.prompt.clone()),
                }],
                tools: case
                    .tools
                    .iter()
                    .map(|name| echo_tool(name).definition)
                    .collect(),
                sampling: Default::default(),
                max_output_tokens: Some(64),
            },
            Limits::default(),
            None,
        )?;
        let dispatch = ModelDispatch {
            operation_id: OperationId::from_bytes([73; 16]),
            step: u32::try_from(step)?,
            request_digest: request.request_digest(),
        };
        let events: Vec<_> = provider.generate(&request, dispatch).try_collect().await?;
        let mut ordinal = 0;
        for event in &events {
            if let ModelEvent::ToolCall(call) = event {
                if call.call_id != dispatch.tool_call_id(ordinal) {
                    return Err("provider lost canonical call identity".into());
                }
                ordinal += 1;
            }
        }
        check_events(expected, &events)?;
        // Fault the consumer of real parsed events, keeping case expectations fixed.
        for inserted in [
            ModelEvent::Content {
                delta: String::new(),
            },
            ModelEvent::Reasoning {
                delta: String::new(),
            },
        ] {
            let mut broken = events.clone();
            broken.insert(0, inserted);
            if check_events(expected, &broken).is_ok() {
                return Err("provider oracle accepted an inserted delta".into());
            }
        }
        for (index, event) in events.iter().enumerate() {
            if let ModelEvent::TextPart { .. } = event {
                let mut broken = events.clone();
                if let ModelEvent::TextPart { text, .. } = &mut broken[index] {
                    text.push('!');
                }
                if check_events(expected, &broken).is_ok() {
                    return Err("provider oracle accepted corrupted retained text".into());
                }
            }
            if let ModelEvent::ReasoningBlock { .. } = event {
                for scope in [false, true] {
                    let mut broken = events.clone();
                    if let Some(ModelEvent::ReasoningBlock { block }) = broken.get_mut(index) {
                        if scope {
                            block.origin.credential_scope =
                                block.origin.credential_scope.is_none().then_some([73; 32]);
                        } else {
                            block.continuity.push(255);
                        }
                    }
                    if check_events(expected, &broken).is_ok() {
                        return Err(
                            "provider oracle accepted corrupted reasoning ownership or bytes"
                                .into(),
                        );
                    }
                }
            }
        }
        let mut broken = events.clone();
        for event in &mut broken {
            if let ModelEvent::ToolCall(call) = event {
                call.arguments
                    .as_object_mut()
                    .ok_or("tool arguments")?
                    .insert("injected".into(), true.into());
            }
        }
        if broken != events && check_events(expected, &broken).is_ok() {
            return Err("provider oracle accepted inserted arguments".into());
        }
    }
    let (sent, remaining) = transport.take_requests();
    if sent.len() != case.responses.len() || remaining != 0 {
        return Err("provider parsing left an unused fixture".into());
    }
    // Replay one actual production request after the recording ends. A refused
    // extra exchange must remain observable to the exact request-count oracle.
    let request = sent.into_iter().next().ok_or("empty HTTP recording")?;
    if transport
        .send(HttpRequest {
            url: request.url,
            headers: Default::default(),
            body: request.body,
        })
        .await
        .is_ok()
    {
        return Err("HTTP recording accepted an unscripted exchange".into());
    }
    let (excess, remaining) = transport.take_requests();
    if excess.len() != 1 || remaining != 0 {
        return Err("HTTP recording hid an exhausted exchange".into());
    }
    Ok(())
}

/// A registered tool returning its arguments, named like an installed MCP tool.
pub(super) fn echo_tool(name: &str) -> acyclic_harness::tool::Tool {
    use acyclic_harness::tool::{
        JsonToolProjection, Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolResult,
    };
    struct Echo;
    impl ToolExecutor for Echo {
        fn execute<'a>(
            &'a self,
            invocation: ToolInvocation,
            _: Option<acyclic_harness::runtime::ToolContext>,
        ) -> acyclic_stream::BoxProviderFuture<'a, acyclic_harness::Result<ToolResult>> {
            Box::pin(async move {
                Ok(ToolResult {
                    value: invocation.arguments,
                })
            })
        }
        fn reconcile<'a>(
            &'a self,
            _: ToolInvocation,
            _: Option<acyclic_harness::runtime::ToolContext>,
        ) -> acyclic_stream::BoxProviderFuture<'a, acyclic_harness::Result<Option<ToolResult>>>
        {
            Box::pin(async { Ok(None) })
        }
    }
    let schema = serde_json::json!({"type":"object"});
    Tool {
        hook_names: &[],
        definition: ToolDefinition {
            name: name.into(),
            revision: "1".into(),
            description: "Echo".into(),
            input_schema: schema.clone(),
            output_schema: schema.clone(),
            projection_schema: acyclic_harness::tool::json_projection_schema(schema),
        },
        executor: Arc::new(Echo),
        projection: Arc::new(JsonToolProjection),
    }
}

pub(super) async fn run(case: ProviderCase) -> TestResult {
    case.validate()?;
    let transport = Recorded::new(&case);
    // The parse oracle replays completed exchanges one per step; refusals and stalls are
    // task-level.
    let parse =
        case.unreachable == 0 && (0..case.responses.len()).all(|index| case.completes(index));
    if parse {
        parsed(&case, transport.clone()).await?;
    }
    if parse {
        transport.restart()?;
    }
    task(&case, &transport).await
}

async fn task(case: &ProviderCase, transport: &Arc<Recorded>) -> TestResult {
    let temporary = local_workspace::temporary()?;
    let directory = temporary.path().join("filesystem");
    std::fs::create_dir(&directory)?;
    let (host, volume, workspace, reader, _authority) =
        local_workspace::seed(&directory, &case.seed)
            .await
            .map_err(|error| format!("provider scenario seed: {error}"))?;
    let provider = Arc::new(resolve(case, transport.clone())?);
    let config = local_harness_config(&directory, (provider.model().clone(), provider))
        .map_err(|error| format!("provider scenario configuration: {error}"))?
        .state_directory(temporary.path().join("state"))
        .volume(volume.clone())
        .compaction(CompactionPolicy::Disabled)
        .goal_continuations(None)
        .tools(case.tools.iter().map(String::as_str).map(echo_tool));
    let harness = Arc::new(
        LocalHarness::open(config)
            .await
            .map_err(|error| format!("provider scenario open: {error}"))?,
    );
    match (
        case.expect.outcome,
        prompt_settled(&harness, "provider", &case.prompt)
            .await
            .map_err(|error| format!("provider scenario prompt: {error}"))?,
    ) {
        (
            super::expected_outcome::ExpectedOutcome::Succeeded,
            PromptOutcome::Settled(_, output),
        ) if output.text == case.expect.text && output.steps as usize == completed_steps(case) => {}
        (super::expected_outcome::ExpectedOutcome::Failed, PromptOutcome::Failed(_)) => {}
        (_, output) => return Err(format!("provider task output differs: {output:?}").into()),
    }
    check_requests(case, transport)?;
    drop(harness);
    drop(reader);
    drop(host);
    let cold = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&directory))
        .await
        .map_err(|error| format!("provider scenario reopen: {error}"))?;
    let cold = acyclic_harness::filesystem::FilesystemHost::new(cold, volume.provider(), None)?;
    for (path, expected) in &case.expect.files {
        let actual = cold
            .read(&workspace, None, &format!("/{path}"), 65_536, None)
            .await;
        match (expected, actual) {
            (Some(expected), Ok(bytes)) if bytes.as_ref() == expected.as_bytes() => {}
            (None, Err(error)) if error.code() == acyclic_harness::wire::ErrorCode::NotFound => {}
            (_, actual) => {
                return Err(format!("provider persisted file differs: {path}: {actual:?}").into());
            }
        }
    }
    Ok(())
}

/// The recorded HTTP script against the case, faulting each asserted request field.
fn check_requests(case: &ProviderCase, transport: &Recorded) -> TestResult {
    let (requests, remaining) = transport.take_requests();
    if requests.len() != case.responses.len() || remaining != 0 {
        return Err("provider task did not consume the exact HTTP script".into());
    }
    // Each refused connection was redelivered under the bounded policy until the script ran out.
    if transport.refused() != usize::from(case.unreachable) {
        return Err(format!(
            "provider task made {} refused attempts",
            transport.refused()
        )
        .into());
    }
    for (request, expected) in requests.iter().zip(&case.expect.requests) {
        let body: Value = serde_json::from_slice(&request.body)?;
        if request.url != format!("https://recorded.invalid/v1{}", case.expect.endpoint)
            || !matchers::subset(&serde_json::json!({"$contains":case.prompt}), &body)
        {
            return Err("provider task request lost its endpoint or prompt".into());
        }
        check_request(expected, &body)?;
        // Fault the recorded production request, leaving fixture assertions intact.
        // Every declared call/result payload and identity must reject loss/change.
        let mut asserted = Vec::new();
        for (pointer, value) in expected {
            assertion_paths(value, at(&body, pointer), pointer.clone(), &mut asserted)?;
        }
        for pointer in &asserted {
            let mut dropped = body.clone();
            let (parent, key) = pointer
                .rsplit_once('/')
                .ok_or("request assertion pointer")?;
            match dropped.pointer_mut(parent) {
                Some(Value::Object(object)) => {
                    object.remove(key);
                }
                Some(Value::Array(array)) => {
                    array.remove(key.parse::<usize>()?);
                }
                _ => return Err("request assertion parent".into()),
            }
            if check_request(expected, &dropped).is_ok() {
                return Err(format!("request oracle accepted removal at {pointer}").into());
            }
            for corruption in [Value::Null, Value::String("CORRUPTED".into())] {
                let mut broken = body.clone();
                *broken
                    .pointer_mut(pointer)
                    .ok_or("request assertion pointer")? = corruption;
                if check_request(expected, &broken).is_ok() {
                    return Err(format!("request oracle accepted corruption at {pointer}").into());
                }
            }
        }
    }
    Ok(())
}

/// Retain field-level fault injection when notices move ordered history entries.
fn assertion_paths(
    expected: &Value,
    actual: &Value,
    pointer: String,
    paths: &mut Vec<String>,
) -> TestResult {
    paths.push(pointer.clone());
    match (expected, actual) {
        (Value::Object(expected), Value::Object(actual)) if !expected.contains_key("$contains") => {
            for (key, value) in expected {
                let escaped = key.replace('~', "~0").replace('/', "~1");
                assertion_paths(
                    value,
                    actual.get(key).ok_or("request assertion field")?,
                    format!("{pointer}/{escaped}"),
                    paths,
                )?;
            }
        }
        (Value::Array(expected), Value::Array(actual)) => {
            let mut remaining = actual.iter().enumerate();
            for value in expected {
                let (index, actual) = remaining
                    .find(|(_, actual)| matchers::subset(value, actual))
                    .ok_or("request assertion observation")?;
                assertion_paths(value, actual, format!("{pointer}/{index}"), paths)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn check_request(
    expected: &std::collections::BTreeMap<String, Value>,
    body: &Value,
) -> TestResult {
    for (pointer, value) in expected {
        if !body
            .pointer(pointer)
            .is_some_and(|actual| matchers::subset(value, actual))
        {
            return Err(format!(
                "provider request differs at {pointer}: expected {value}, got {body}"
            )
            .into());
        }
    }
    Ok(())
}
