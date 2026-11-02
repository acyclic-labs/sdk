//! Replay from a recording through the provider seam.
use super::super::LocalAgents;
use super::{volume, volume_config};
use crate::{
    Error, LocalHarness, Result,
    agent::AgentPath,
    local::agent_conversation::Conversation,
    model::{
        Divergence, DivergenceSink, ModelEvent, ModelProvider, ReasoningBlock, ReasoningKind,
        ReasoningOrigin, ReasoningOutcome, RecordedModel, Recording, ToolCall,
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

/// A replayed tool call must never reach its executor.
struct Refuse;
impl ToolExecutor for Refuse {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async { Err(Error::Invalid("replayed tool ran".into())) })
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
        std::mem::take(&mut self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner))
    }
}

/// Runs `inputs` on a fresh harness and returns the run's recording.
async fn run(provider: Arc<dyn ModelProvider>, tool: Tool, inputs: &[&str]) -> Result<Recording> {
    let directory = tempfile::tempdir().map_err(agent)?;
    let project = volume(directory.path()).await?;
    let harness = Arc::new(
        LocalHarness::open(
            volume_config(directory.path(), project, provider)?
                .compaction(crate::context::CompactionPolicy::Disabled)
                .tool(tool),
        )
        .await?,
    );
    let (agents, root) = harness.agents().await?;
    let (agents, root): (Arc<LocalAgents>, AgentPath) = (Arc::clone(agents), root.clone());
    let client = Conversation::open(harness.connect().await?)?;
    for input in inputs {
        client.send(input, input).await?;
        super::agents::settled(&agents, &root).await?;
    }
    client.recording().await
}

/// A source run whose first turn reasons with native bytes and calls a tool.
async fn source() -> Result<Recording> {
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
    let recording = run(provider, super::tool("example.echo"), &["first", "second"]).await?;
    assert_eq!(recording.len(), 3);
    Ok(recording)
}

fn refused() -> Tool {
    let mut tool = super::tool("example.echo");
    tool.executor = Arc::new(Refuse);
    tool
}

#[tokio::test]
async fn a_fresh_run_replays_its_source_without_the_model_or_its_tools() -> Result<()> {
    let expected = source().await?;
    let live = super::scripted([]);
    let divergences = Arc::new(Divergences::default());
    let replay = Arc::new(RecordedModel::new(
        live.clone(),
        source().await?,
        divergences.clone(),
    ));
    let replayed = run(replay.clone(), refused(), &["first", "second"]).await?;
    assert_eq!(divergences.taken(), []);
    assert!(live.requests().is_empty());
    assert_eq!(replay.unused(), 0);
    // Same keys, verbatim events (native reasoning included) and the
    // recorded tool result, journaled again by the replayed run.
    assert_eq!(replayed, expected);
    Ok(())
}

#[tokio::test]
async fn a_changed_input_goes_live_at_its_first_step_and_stays_live() -> Result<()> {
    let live = super::scripted([ScriptedTurn::Text("live".into())]);
    let divergences = Arc::new(Divergences::default());
    let replay = Arc::new(RecordedModel::new(
        live.clone(),
        source().await?,
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
