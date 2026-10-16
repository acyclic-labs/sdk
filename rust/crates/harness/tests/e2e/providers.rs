use super::{
    TestResult, diff_workspace, matchers,
    provider_case::ProviderCase,
    provider_http::{Recorded, resolve},
};
use acyclic_harness::{
    Admission, LocalHarness, OperationId, Outcome,
    context::CompactionPolicy,
    conversation::Limits,
    model::{
        ModelContent, ModelDispatch, ModelEvent, ModelMessage, ModelProvider as _, ModelRequest,
        ModelRole, PreparedModelRequest,
    },
    runtime::RuntimeTask,
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
        if !matchers::subset(expected, &actual) {
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

fn wire_success(expected: &[Value], wire: &[Value], operation: OperationId) -> TestResult {
    let operation = serde_json::to_value(operation)?;
    let terminal: Vec<_> = wire
        .iter()
        .filter(|event| event["kind"] == "completed" && event["operation_id"] == operation)
        .collect();
    if terminal.len() != 1 {
        return Err("provider task lacks exactly one root completion frame".into());
    }
    let event: acyclic_harness::scheduler::SchedulerEvent =
        serde_json::from_value(terminal[0].clone())?;
    if !matches!(
        event,
        acyclic_harness::scheduler::SchedulerEvent::Completed {
            outcome: Outcome::Succeeded(_),
            ..
        }
    ) {
        return Err("provider task wire did not succeed".into());
    }
    matchers::ordered(expected, wire, "provider task wire")?;
    Ok(())
}

pub(super) async fn run(case: ProviderCase) -> TestResult {
    case.validate()?;
    let transport = Recorded::new(&case);
    parsed(&case, transport.clone()).await?;
    transport.restart()?;
    let temporary = diff_workspace::temporary()?;
    let directory = temporary.path().join("filesystem");
    std::fs::create_dir(&directory)?;
    let (host, volume, workspace, reader) = diff_workspace::seed(&directory, &case.seed).await?;
    let provider = Arc::new(resolve(&case, transport.clone())?);
    let mut config = local_harness_config(&directory, (provider.model().clone(), provider))?
        .state_directory(temporary.path().join("state"))
        .volume(volume.clone())
        .compaction(CompactionPolicy::Disabled);
    config.goal_continuations = None;
    let harness = LocalHarness::open(config).await?;
    let operation = OperationId::new();
    let Admission::Accepted(RuntimeTask::Durable { task_id, .. }) =
        harness.submit(operation, &case.prompt).await?
    else {
        return Err("provider task was not admitted".into());
    };
    let stream = local_task_stream(&harness);
    let path = StreamPath::new("harness/v3/coordinator/events")?;
    let mut frames = stream.follow(path.clone(), 0).await?;
    let output = harness.resume(task_id).await?;
    if output.text != case.expect.text || output.steps as usize != case.responses.len() {
        return Err(format!("provider task output differs: {output:?}").into());
    }
    let cut = stream.tail(path).await?;
    let mut wire = Vec::new();
    for sequence in 0..cut {
        let record = frames.next().await.ok_or("provider task wire ended")??;
        if record.sequence != sequence {
            return Err("provider task wire has a gap".into());
        }
        let envelope =
            acyclic_harness::wire::SchedulerEventEnvelope::decode(record.value.as_ref())?;
        wire.push(serde_json::from_slice(&envelope.canonical_event_json)?);
    }
    wire_success(&case.expect.frames, &wire, operation)?;
    // Keep other child completions: only the root publication is dropped.
    let root = serde_json::to_value(operation)?;
    let broken: Vec<_> = wire
        .iter()
        .filter(|event| !(event["kind"] == "completed" && event["operation_id"] == root))
        .cloned()
        .collect();
    if wire_success(&case.expect.frames, &broken, operation).is_ok() {
        return Err("wire oracle accepted a dropped root completion".into());
    }
    let (requests, remaining) = transport.take_requests();
    if requests.len() != case.responses.len() || remaining != 0 {
        return Err("provider task did not consume the exact HTTP script".into());
    }
    for request in requests {
        if request.url != format!("https://recorded.invalid/v1{}", case.expect.endpoint)
            || !matchers::subset(
                &serde_json::json!({"$contains":case.prompt}),
                &serde_json::from_slice(&request.body)?,
            )
        {
            return Err("provider task request lost its endpoint or prompt".into());
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
