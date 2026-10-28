//! Compaction, rewind and clear select the root's later model context as
//! checkpoints in its own conversation stream, durable across a reopen.

use super::{ScriptedModelProvider, ScriptedTurn, scripted, volume, volume_config};
use crate::{
    Error, LocalHarness, Result,
    agent::{ContextAction, ConversationEntry},
    local::{
        Event,
        agent_conversation::{Conversation, checkpoint},
    },
};
use std::{path::Path, sync::Arc};

fn failed(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

async fn open(directory: &Path, provider: Arc<ScriptedModelProvider>) -> Result<Arc<LocalHarness>> {
    let project = volume(directory).await?;
    Ok(Arc::new(
        LocalHarness::open(
            volume_config(directory, project, provider)?
                .compaction(crate::context::CompactionPolicy::Disabled),
        )
        .await?,
    ))
}

/// One conversation and its observation, which reports each settled entry.
struct Client {
    conversation: Arc<Conversation>,
    observer: crate::local::Observer,
}

impl Client {
    async fn open(harness: &Arc<LocalHarness>) -> Result<Self> {
        // The root's runtime runs once the harness hosts its agents.
        harness.agents().await?;
        let conversation = Conversation::open(harness.connect().await?)?;
        let observer = conversation.observe(conversation.settled().await?).await?;
        Ok(Self {
            conversation,
            observer,
        })
    }

    /// The entry settled at inbox sequence `inbox`, as the observer reports
    /// it; a failed or finished agent, or a stalled one, fails the wait.
    async fn settled(&mut self, inbox: u64) -> Result<ConversationEntry> {
        let mut seen = Vec::new();
        let wait = async {
            loop {
                match self.observer.next().await? {
                    Event::Turn(entry) if entry.inbox == inbox => return Ok(entry),
                    Event::Failed(error) => return Err(Error::Conflict(error)),
                    Event::Status(status) => {
                        if matches!(status.phase, crate::agent::Phase::Done { .. }) {
                            return Err(Error::Conflict(format!("agent finished: {status:?}")));
                        }
                        seen.push(format!("{status:?}"));
                    }
                    Event::Turn(entry) => seen.push(format!("turn at {}", entry.inbox)),
                    Event::Output => seen.push("output".into()),
                }
            }
        };
        let settled = tokio::time::timeout(std::time::Duration::from_secs(20), wait).await;
        match settled {
            Ok(result) => result,
            Err(_) => {
                let (settled, _) = (self.conversation.settled().await?, ());
                Err(Error::Conflict(format!(
                    "no entry settled at inbox {inbox}; {settled} settled; events {seen:?}"
                )))
            }
        }
    }

    async fn send(&mut self, id: &str, prompt: &str) -> Result<ConversationEntry> {
        let inbox = self.conversation.send(id, prompt).await?;
        self.settled(inbox).await
    }
}

/// The `index`th model request, serialized.
fn request(provider: &ScriptedModelProvider, index: usize) -> Result<String> {
    let request = provider
        .request(index)
        .ok_or_else(|| Error::Conflict(format!("no model request {index}")))?;
    serde_json::to_string(&request).map_err(failed)
}

#[tokio::test]
async fn checkpoints_select_later_context_and_survive_a_reopen() -> Result<()> {
    let directory = tempfile::tempdir().map_err(failed)?;
    let provider = scripted(
        [
            "FIRST_REPLY",
            "CHECKPOINT_SUMMARY",
            "DISCARDED_REPLY",
            "KEPT_REPLY",
        ]
        .map(|text| ScriptedTurn::Text(text.into())),
    );
    let harness = open(directory.path(), provider.clone()).await?;
    let mut client = Client::open(&harness).await?;
    client.send("first", "remember FIRST_FACT").await?;

    // Compaction is one model request over the whole current context.
    let inbox = client
        .conversation
        .compact("compact", Some("KEEP_GUIDANCE"))
        .await?;
    let entry = client.settled(inbox).await?;
    assert!(matches!(
        checkpoint(&entry),
        Some(ContextAction::Compact {
            instructions: Some(_)
        })
    ));
    let (prompt, summary) = client.conversation.turn(&entry).await?;
    assert_eq!((prompt, summary.as_str()), (None, "CHECKPOINT_SUMMARY"));
    let compaction = request(&provider, 1)?;
    for needle in [
        "Summarize this conversation",
        "KEEP_GUIDANCE",
        "FIRST_FACT",
        "FIRST_REPLY",
    ] {
        assert!(compaction.contains(needle), "{needle}: {compaction}");
    }

    // A later turn starts from the summary alone.
    client.send("discard", "DISCARD_PROMPT").await?;
    let after = request(&provider, 2)?;
    assert!(after.contains("CHECKPOINT_SUMMARY"), "{after}");
    assert!(!after.contains("FIRST_FACT") && !after.contains("FIRST_REPLY"));
    assert!(!after.contains("Summarize this conversation"), "{after}");

    // A rewind runs no model request and restores the checkpoint.
    let inbox = client.conversation.rewind("rewind").await?;
    let entry = client.settled(inbox).await?;
    assert_eq!(checkpoint(&entry), Some(&ContextAction::Rewind));
    assert_eq!(provider.request_count(), 3);
    assert_eq!(
        client.conversation.turn(&entry).await?.1,
        "CHECKPOINT_SUMMARY"
    );
    client.send("kept", "KEPT_PROMPT").await?;
    let rewound = request(&provider, 3)?;
    assert!(rewound.contains("CHECKPOINT_SUMMARY"), "{rewound}");
    assert!(!rewound.contains("DISCARD_PROMPT") && !rewound.contains("DISCARDED_REPLY"));
    drop(client);
    drop(harness);

    // The rewind is durable: the reopened conversation keeps its selection.
    let reopened =
        scripted(["REOPENED_REPLY", "FRESH_REPLY"].map(|text| ScriptedTurn::Text(text.into())));
    let harness = open(directory.path(), reopened.clone()).await?;
    let mut client = Client::open(&harness).await?;
    client.send("reopened", "REOPENED_PROMPT").await?;
    let resumed = request(&reopened, 0)?;
    for needle in ["CHECKPOINT_SUMMARY", "KEPT_PROMPT", "KEPT_REPLY"] {
        assert!(resumed.contains(needle), "{needle}: {resumed}");
    }
    assert!(!resumed.contains("DISCARD_PROMPT") && !resumed.contains("FIRST_FACT"));

    // Clearing retires the conversation: the next turn has no history.
    let inbox = client.conversation.clear_all("clear").await?;
    let entry = client.settled(inbox).await?;
    assert_eq!(checkpoint(&entry), Some(&ContextAction::Clear));
    assert_eq!(reopened.request_count(), 1);
    client.send("fresh", "FRESH_PROMPT").await?;
    let fresh = request(&reopened, 1)?;
    assert!(fresh.contains("FRESH_PROMPT"), "{fresh}");
    for needle in ["CHECKPOINT_SUMMARY", "KEPT_REPLY", "REOPENED_REPLY"] {
        assert!(!fresh.contains(needle), "{needle}: {fresh}");
    }
    Ok(())
}

#[tokio::test]
async fn a_cancelled_turn_settles_durably_without_a_model_request() -> Result<()> {
    let directory = tempfile::tempdir().map_err(failed)?;
    let dispatched = Arc::new(tokio::sync::Notify::new());
    let signal = Arc::clone(&dispatched);
    let provider = Arc::new(
        ScriptedModelProvider::new([
            ScriptedTurn::Events(vec![crate::test_support::ScriptedEvent::Pending {}]),
            ScriptedTurn::Text("AFTER_REPLY".into()),
        ])
        .with_context_capacity(super::capacity())
        .with_dispatch_observer(move |_| signal.notify_one()),
    );
    let harness = open(directory.path(), provider.clone()).await?;
    let mut client = Client::open(&harness).await?;
    let inbox = client.conversation.send("held", "CANCELLED_PROMPT").await?;
    // The turn is cancelled while its model request streams.
    tokio::time::timeout(std::time::Duration::from_secs(20), dispatched.notified())
        .await
        .map_err(|_| Error::Conflict("the held turn never reached the model".into()))?;
    client.conversation.cancel().await?;
    let entry = client.settled(inbox).await?;
    assert_eq!(entry.outcome, crate::agent::TurnOutcome::Cancelled);
    assert_eq!(checkpoint(&entry), None);
    let (prompt, _) = client.conversation.turn(&entry).await?;
    assert_eq!(prompt.as_deref(), Some("CANCELLED_PROMPT"));
    assert_eq!(provider.request_count(), 1);
    // The cancelled prompt stays in the later context.
    client.send("after", "AFTER_PROMPT").await?;
    let after = request(&provider, 1)?;
    assert!(after.contains("CANCELLED_PROMPT"), "{after}");
    Ok(())
}
