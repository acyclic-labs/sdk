//! A definitive invocation refusal preserves the owning actor and its children.
use super::*;
use crate::model::{ModelContent, ModelContentPart, ModelEvent, ToolCall, ToolResultContent};
use crate::test_support::ScriptedEvent;
use acyclic_harness_client::{AgentCommand, AgentReply};
use std::sync::atomic::{AtomicUsize, Ordering};

struct CancelledInvocation(AtomicUsize);
impl ToolExecutor for CancelledInvocation {
    fn execute<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Err(Error::InteractionRejected(
                crate::InteractionRejection::Cancelled,
            ))
        })
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
        _: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async { panic!("definitive refusal must not enter reconciliation") })
    }
}

#[tokio::test]
async fn cancelled_invocation_keeps_parent_and_live_child_accounted() -> Result<()> {
    let directory = tempfile::tempdir().map_err(crate::local::storage)?;
    let project = volume(directory.path()).await?;
    let (entered, active) = tokio::sync::oneshot::channel();
    let (release, held) = tokio::sync::oneshot::channel();
    let gate = std::sync::Mutex::new(Some((entered, held)));
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Text("child completed".into()),
            ScriptedTurn::Events(vec![
                ScriptedEvent::Event(ModelEvent::ToolCall(ToolCall::new(
                    "cancelled-call".into(),
                    "example.cancel".into(),
                    json!({}),
                ))),
                ScriptedEvent::Event(ModelEvent::Completed {
                    metadata: json!({}),
                }),
            ]),
            ScriptedTurn::Text("parent recovered".into()),
            ScriptedTurn::Text("parent remains available".into()),
        ])
        .with_context_capacity(capacity())
        .with_dispatch_observer(move |_| {
            let gate = gate.lock().unwrap().take();
            Box::pin(async move {
                if let Some((entered, held)) = gate {
                    let _ = entered.send(());
                    held.await.map_err(crate::local::storage)?;
                }
                Ok(())
            })
        }),
    );
    let cancelled = Arc::new(CancelledInvocation(AtomicUsize::new(0)));
    let mut invocation = tool("example.cancel");
    invocation.executor = cancelled.clone();
    let harness = Arc::new(
        open_scripted(
            volume_config(directory.path(), project, provider.clone())?
                .without_default_tools()
                .tool(invocation)
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    );
    let connection = harness.connect().await?;
    let (agents, root) = harness.agents().await?;
    let (agents, root) = (Arc::clone(agents), root.clone());
    let AgentReply::Spawned { agent: child, .. } = connection
        .execute(&AgentCommand::Spawn {
            parent: root.clone(),
            id: crate::agent::IntentId::try_from("worker").map_err(crate::local::storage)?,
            brief: ModelContent::Text("continue independent work".into()),
            role: "worker".into(),
        })
        .await?
    else {
        return Err(Error::Conflict("child was not spawned".into()));
    };
    active.await.map_err(crate::local::storage)?;
    let mut child_observer = ObservedConversation::observe(
        Conversation::open(connection.select(child.clone()).await?),
        None,
    )
    .await?;
    let mut parent =
        ObservedConversation::observe(Conversation::open(connection.clone()), None).await?;
    let recovered = parent
        .send("parent-input", "attempt this invocation")
        .await?;
    assert_eq!(recovered.outcome, crate::agent::TurnOutcome::Completed);
    assert_eq!(cancelled.0.load(Ordering::SeqCst), 1);
    let requests = provider.requests();
    assert_eq!(
        requests.len(),
        3,
        "parent must recover while its child is active"
    );
    assert!(requests[2].messages.iter().flat_map(|message| message.content.parts()).any(|part| {
        matches!(part, ModelContentPart::ToolResult { call_id, content: ToolResultContent::Error { message }, .. }
            if call_id == "cancelled-call" && message.contains("interaction cancelled"))
    }), "next model request must contain the original definitive failure");
    let state = agents.state(&root).await.map_err(crate::local::storage)?;
    assert!(!matches!(state.phase, crate::agent::Phase::Done { .. }));
    assert!(
        state
            .open_children
            .contains(&child)
            .await
            .map_err(crate::local::storage)?,
        "the active child must remain in the parent's original child index"
    );
    assert!(
        release.send(()).is_ok(),
        "parent refusal cancelled its child request"
    );
    loop {
        match child_observer.observer.next().await? {
            Event::Turn { entry, .. } => {
                assert!(matches!(entry.input, crate::agent::TurnInput::Brief { .. }));
                assert_eq!(entry.outcome, crate::agent::TurnOutcome::Completed);
                break;
            }
            Event::Status(status) if matches!(status.phase, crate::agent::Phase::Done { .. }) => {
                return Err(Error::Conflict(format!(
                    "child ended before its reply: {status:?}"
                )));
            }
            _ => {}
        }
    }
    let resumed = parent
        .send("later-parent-input", "continue coordinating")
        .await?;
    assert_eq!(resumed.outcome, crate::agent::TurnOutcome::Completed);
    assert_eq!(provider.request_count(), 4);
    drop(child_observer);
    drop(parent);
    drop(connection);
    drop(agents);
    Arc::try_unwrap(harness)
        .map_err(|_| Error::Conflict("test still owns its harness".into()))?
        .disconnect()
        .await
}
