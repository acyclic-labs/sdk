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
    test_support::{local_harness_config, local_task_stream},
};
use acyclic_stream::{StreamPath, StreamProvider as _};
use futures::{StreamExt as _, TryStreamExt as _};
use prost::Message as _;
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
        let reasoning = actual["kind"] == "reasoning_block";
        if !matchers::subset(expected, &actual)
            || (reasoning
                && (expected["block"]["continuity"] != actual["block"]["continuity"]
                    || expected["block"]["origin"]["credential_scope"]
                        != actual["block"]["origin"]["credential_scope"]))
        {
            return Err(
                format!("provider events differ: expected {expected}, got {actual}").into(),
            );
        }
    }
    Ok(())
}

async fn parsed(case: &ProviderCase, transport: Arc<Recorded>) -> TestResult {
    let provider = resolve(case, transport.clone())?;
    for (step, expected) in case.expect.events.iter().enumerate() {
        let request = PreparedModelRequest::prepare(
            ModelRequest {
                model: provider.model().clone(),
                messages: vec![ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text(case.prompt.clone()),
                }],
                tools: vec![],
                max_output_tokens: Some(64),
            },
            Limits::default(),
            None,
        )?;
        let dispatch = ModelDispatch {
            operation_id: OperationId::from_bytes([73; 16]),
            step: step as u32,
            request_digest: request.request_digest(),
        };
        let events: Vec<_> = provider.generate(request, dispatch).try_collect().await?;
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
        for index in 0..events.len() {
            if let ModelEvent::ReasoningBlock { .. } = &events[index] {
                for scope in [false, true] {
                    let mut broken = events.clone();
                    let ModelEvent::ReasoningBlock { block } = &mut broken[index] else {
                        unreachable!()
                    };
                    if scope {
                        block.origin.credential_scope = None;
                    } else {
                        block.continuity.push(255);
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
    Ok(())
}

pub(super) async fn run(case: ProviderCase) -> TestResult {
    case.validate()?;
    let transport = Recorded::new(&case);
    parsed(&case, transport.clone()).await?;
    transport.restart()?;
    let temporary = local_workspace::temporary()?;
    let directory = temporary.path().join("filesystem");
    std::fs::create_dir(&directory)?;
    let (host, volume, workspace, reader) = local_workspace::seed(&directory, &case.seed).await?;
    let provider = Arc::new(resolve(&case, transport.clone())?);
    let config = local_harness_config(&directory, (provider.model().clone(), provider))?
        .state_directory(temporary.path().join("state"))
        .volume(volume.clone())
        .compaction(CompactionPolicy::Disabled)
        .goal_continuations(None);
    let harness = LocalHarness::open(config).await?;
    let (operation, task_id, status) =
        super::local_admission::submit(&harness, &case.prompt, reader.as_ref()).await?;
    let stream = local_task_stream(&harness);
    let path = StreamPath::new("harness/v3/coordinator/events")?;
    let mut frames = stream.follow(path.clone(), 0).await?;
    let output = harness.resume(task_id).await?;
    super::local_admission::settled(&harness, task_id, status, &case.expect.statuses).await?;
    if output.text != case.expect.text || output.steps as usize != case.responses.len() {
        return Err(format!("provider task output differs: {output:?}").into());
    }
    let cut = stream.tail(path).await?;
    let mut wire = Vec::new();
    super::stall::waiting("provider task wire replay");
    for sequence in 0..cut {
        let record = frames.next().await.ok_or("provider task wire ended")??;
        if record.sequence != sequence {
            return Err("provider task wire has a gap".into());
        }
        let envelope =
            acyclic_harness::wire::SchedulerEventEnvelope::decode(record.value.as_ref())?;
        wire.push(serde_json::from_slice(&envelope.canonical_event_json)?);
    }
    super::local_execution::check_frames(
        &case.expect.frames,
        &wire,
        operation,
        super::local_case::ExpectedOutcome::Succeeded,
    )?;
    // Keep other child completions: only the root publication is dropped.
    let root = serde_json::to_value(operation)?;
    let terminal = wire
        .iter()
        .position(|event| event["kind"] == "completed" && event["operation_id"] == root)
        .ok_or("missing terminal")?;
    wire.remove(terminal);
    if super::local_execution::check_frames(
        &case.expect.frames,
        &wire,
        operation,
        super::local_case::ExpectedOutcome::Succeeded,
    )
    .is_ok()
    {
        return Err("wire oracle accepted a dropped root completion".into());
    }
    let (requests, remaining) = transport.take_requests();
    if requests.len() != case.responses.len() || remaining != 0 {
        return Err("provider task did not consume the exact HTTP script".into());
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
        for pointer in expected.keys() {
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
    drop(frames);
    drop(stream);
    drop(harness);
    drop(reader);
    drop(host);
    let cold = acyclic_fs::Fs::local(acyclic_fs::LocalOptions::new(&directory)).await?;
    let cold = acyclic_harness::filesystem::FilesystemHost::new(cold, volume.provider(), None)?;
    for (path, expected) in &case.expect.files {
        let actual = cold
            .read(&workspace, None, &format!("/{path}"), 65_536, None)
            .await;
        match (expected, actual) {
            (Some(expected), Ok(bytes)) if bytes.as_ref() == expected.as_bytes() => {}
            (None, Err(acyclic_harness::Error::NotFound(_))) => {}
            (_, actual) => {
                return Err(format!("provider persisted file differs: {path}: {actual:?}").into());
            }
        }
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
