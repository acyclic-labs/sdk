//! Replay from a recording through the provider seam.
use super::{ObservedConversation, volume, volume_config};
use crate::{
    Error, Result,
    model::{
        Divergence, DivergenceSink, ModelDelivery, ModelDispatch, ModelEvent, ModelProvider,
        ModelRequest, PreparedModelRequest, ReasoningBlock, ReasoningKind, ReasoningOrigin,
        ReasoningOutcome, RecordedModel, Recording, ToolCall,
    },
    runtime::ToolContext,
    test_support::{ScriptedEvent, ScriptedTurn},
    tool::{Tool, ToolExecutor, ToolInvocation, ToolResult},
};
use acyclic_stream::BoxProviderFuture;
use std::sync::{Arc, Mutex};

fn agent(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

/// Fails every call with its message. A replay registers one that says the
/// tool ran, so any replayed call reaching its executor changes the run.
struct Fails(&'static str);
impl ToolExecutor for Fails {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async { Err(Error::Invalid(self.0.into())) })
    }
    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { Ok(None) })
    }
}

#[derive(Default)]
struct Divergences(Mutex<Vec<Divergence>>);
impl DivergenceSink for Divergences {
    fn diverged(&self, divergence: Divergence) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(divergence);
    }
}
impl Divergences {
    fn taken(&self) -> Vec<Divergence> {
        std::mem::take(
            &mut self
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

/// Runs `inputs` on a fresh harness and returns the run's recording.
async fn run(provider: Arc<dyn ModelProvider>, tool: Tool, inputs: &[&str]) -> Result<Recording> {
    // Identical fixture inputs include identities, not just prompt text. Drive
    // the original scheduler and entropy boundary; never normalize requests.
    let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::seeded(71);
    schedule.set_process_provider(|_, _, _| {
        Err(acyclic_native_runtime::ProcessCaptureFailure {
            error: std::io::Error::other("logical replay attempted a native command"),
            status: None,
            stdout: Vec::new(),
            stderr: Vec::new(),
            cleanup_completed: true,
        })
    });
    let mut running = Box::pin(schedule.scope(async {
        let directory = tempfile::tempdir().map_err(agent)?;
        let project = volume(directory.path()).await?;
        let harness = Arc::new(
            super::open_scripted(
                volume_config(directory.path(), project, provider)?
                    .without_user_settings()
                    .without_default_tools()
                    .compaction(crate::context::CompactionPolicy::Disabled)
                    .tool(tool),
            )
            .await?,
        );
        let mut client = ObservedConversation::open(&harness).await?;
        for input in inputs {
            client.send(input, input).await?;
        }
        client.conversation.recording().await
    }));
    let result = loop {
        tokio::select! {
            biased;
            event = events.recv() => event.expect("replay boundary producer").release(),
            result = &mut running => break result,
        }
    };
    drop(running);
    loop {
        tokio::select! {
            biased;
            event = events.recv() => event.expect("replay drain producer").release(),
            () = schedule.drained() => break,
        }
    }
    assert!(
        schedule
            .trace()
            .iter()
            .all(|event| !event.starts_with("process:")),
        "logical replay requested unrelated native process capture"
    );
    result
}

/// A source run whose first turn reasons with native bytes and calls a tool.
async fn source(tool: Tool) -> Result<(Recording, Vec<ModelRequest>)> {
    let reasoning = ReasoningBlock {
        kind: ReasoningKind::Signature,
        text: Some("plan".into()),
        origin: ReasoningOrigin {
            provider: "scripted".into(),
            model: "test".into(),
            credential_scope: Some([7; 32]),
        },
        continuity: br#"{"thought":"plan","signature":"s="}"#.to_vec(),
        outcome: ReasoningOutcome::PreservedReadable,
    };
    let provider = super::scripted([
        ScriptedTurn::Events(
            [
                ModelEvent::ReasoningBlock { block: reasoning },
                ModelEvent::ToolCall(ToolCall::new(
                    "echo-1".into(),
                    "example.echo".into(),
                    serde_json::json!({"text": "x"}),
                )),
                ModelEvent::Completed {
                    metadata: serde_json::json!({}),
                },
            ]
            .map(ScriptedEvent::Event)
            .into(),
        ),
        ScriptedTurn::Text("answer 0".into()),
        ScriptedTurn::Text("answer 1".into()),
    ]);
    let recording = run(provider.clone(), tool, &["first", "second"]).await?;
    assert_eq!(recording.len(), 3);
    let requests = provider.requests();
    let document = requests[0]
        .messages
        .iter()
        .flat_map(|message| message.content.parts())
        .filter_map(|part| match part {
            crate::model::ModelContentPart::Text { text, .. } => {
                serde_json::from_str::<serde_json::Value>(text).ok()
            }
            _ => None,
        })
        .find(|document| document.get("workspaces").is_some())
        .expect("original workspace announcement");
    assert!(
        document["workspaces"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| {
                entry["paths"]
                    .as_object()
                    .unwrap()
                    .values()
                    .all(serde_json::Value::is_null)
            }),
        "logical bindings advertised an OS path"
    );
    Ok((recording, requests))
}

/// Compare answers while retaining each call's original dispatch attribution.
fn same_answers(left: &Recording, right: &Recording) {
    assert_eq!(left.len(), right.len());
    let right = right.calls().collect::<std::collections::HashMap<_, _>>();
    for (key, calls) in left.calls() {
        let expected = right[key];
        assert_eq!(calls.len(), expected.len());
        for (call, original) in calls.iter().zip(expected) {
            assert_eq!(call.events, original.events);
            assert_eq!(call.tools, original.tools);
            assert!(call.source.is_some());
            assert!(original.source.is_some());
        }
    }
}

fn failing(message: &'static str) -> Tool {
    let mut tool = super::tool("example.echo");
    tool.executor = Arc::new(Fails(message));
    tool
}

fn refused() -> Tool {
    failing("replayed tool ran")
}

#[test]
fn a_run_replays_its_source_until_input_changes() -> Result<()> {
    acyclic_native_runtime::testing::seeded_runtime(71)
        .map_err(agent)?
        .block_on(async {
            tokio::time::pause();
            replays_until_input_changes().await
        })
}

async fn replays_until_input_changes() -> Result<()> {
    let (source, requests) = source(super::tool("example.echo")).await?;
    let expected = Arc::new(source);
    let live = super::scripted([]);
    let divergences = Arc::new(Divergences::default());
    let replay = Arc::new(RecordedModel::new(
        live.clone(),
        Arc::clone(&expected),
        divergences.clone(),
    ));
    let replayed = run(replay.clone(), refused(), &["first", "second"]).await?;
    assert_eq!(
        divergences.taken(),
        [],
        "source requests: {requests:#?}; unmatched requests: {:#?}",
        live.requests()
    );
    assert!(live.requests().is_empty());
    assert_eq!(replay.unused(), 0);
    // Same keys, verbatim events (native reasoning included) and the
    // recorded tool result, journaled again by the replayed run.
    same_answers(&replayed, expected.as_ref());
    // Observe the original provider seam using a request captured from the
    // actual source execution above, without constructing provider fixtures.
    let request = PreparedModelRequest::prepare(requests[0].clone(), Default::default(), None)?;
    let key = crate::model::replay_key(request.bytes())?;
    let original = expected
        .calls()
        .find(|(candidate, _)| **candidate == key)
        .unwrap()
        .1[0]
        .source;
    let dispatch = ModelDispatch {
        operation_id: crate::OperationId::new(),
        step: 0,
        request_digest: request.request_digest(),
    };
    let trial = || RecordedModel::new(live.clone(), Arc::clone(&expected), divergences.clone());
    let adapter = trial();
    let stream = adapter.generate(&request, dispatch);
    assert_eq!(
        stream.delivery(),
        ModelDelivery::Recorded { source: original }
    );
    assert_ne!(original.unwrap().operation_id, dispatch.operation_id);
    use futures::TryStreamExt as _;
    let events: Vec<_> = stream.try_collect().await?;
    assert!(!events.is_empty());
    #[cfg(not(target_arch = "wasm32"))]
    {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &cancellation,
        )
        .map_err(|(cause, _)| cause)?;
        let adapter = trial();
        let stream = adapter
            .generate_with_native_work(&request, dispatch, &work)?
            .await?;
        assert_eq!(
            stream.delivery(),
            ModelDelivery::Recorded { source: original }
        );
        let paid: Vec<_> = stream.try_collect().await?;
        assert_eq!(
            paid.iter().map(|(event, _)| event).collect::<Vec<_>>(),
            events.iter().collect::<Vec<_>>()
        );
    }
    // A producer without an annotation stays unknown even after a replay miss.
    let unknown = RecordedModel::new(
        super::scripted([]),
        Arc::new(Recording::default()),
        divergences.clone(),
    );
    let stream = unknown.generate(&request, dispatch);
    assert_eq!(stream.delivery(), ModelDelivery::Unknown);
    // The old event-only stream cannot distinguish these two delivery paths.
    assert_ne!(
        stream.delivery(),
        ModelDelivery::Recorded { source: original }
    );
    // An in-memory source may know its events without an original
    // dispatch. Preserve that absence rather than attributing the new caller.
    let mut unattributed = Recording::default();
    let mut call = expected
        .calls()
        .find(|(candidate, _)| **candidate == key)
        .unwrap()
        .1[0]
        .clone();
    call.source = None;
    unattributed.insert(key, call);
    let inner = Arc::new(RecordedModel::new(
        live.clone(),
        Arc::new(unattributed),
        divergences.clone(),
    ));
    let outer = RecordedModel::new(inner, Arc::new(Recording::default()), divergences.clone());
    let stream = outer.generate(&request, dispatch);
    assert_eq!(stream.delivery(), ModelDelivery::Recorded { source: None });
    assert_eq!(stream.try_collect::<Vec<_>>().await?, events);
    // The same immutable native source goes live only at changed input.
    let live = super::scripted([ScriptedTurn::Text("live".into())]);
    let divergences = Arc::new(Divergences::default());
    let replay = Arc::new(RecordedModel::new(
        live.clone(),
        Arc::clone(&expected),
        divergences.clone(),
    ));
    let replayed = run(replay.clone(), refused(), &["first", "changed"]).await?;
    let [divergence] = divergences.taken()[..] else {
        return Err(Error::Invalid("expected exactly one divergence".into()));
    };
    assert_eq!(divergence.dispatch.step, 0);
    assert_eq!(live.requests().len(), 1);
    assert!(replay.diverged());
    // Only the unmatched second answer stays unused.
    assert_eq!(replay.unused(), 1);
    assert_eq!(replayed.len(), 3);
    Ok(())
}

#[test]
fn a_failed_tool_call_replays_its_recorded_failure() -> Result<()> {
    acyclic_native_runtime::testing::seeded_runtime(71)
        .map_err(agent)?
        .block_on(async {
            tokio::time::pause();
            replays_failed_tool_call().await
        })
}

async fn replays_failed_tool_call() -> Result<()> {
    let (source, requests) = source(failing("source failure")).await?;
    let expected = Arc::new(source);
    let live = super::scripted([]);
    let divergences = Arc::new(Divergences::default());
    let replay = Arc::new(RecordedModel::new(
        live.clone(),
        Arc::clone(&expected),
        divergences.clone(),
    ));
    let replayed = run(replay, refused(), &["first", "second"]).await?;
    assert_eq!(
        divergences.taken(),
        [],
        "source requests: {requests:#?}; unmatched requests: {:#?}",
        live.requests()
    );
    assert!(live.requests().is_empty());
    same_answers(&replayed, &expected);
    Ok(())
}
