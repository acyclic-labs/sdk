//! The GraphCoder conversation: the local harness's root agent, which fans
//! out through its spawn, send and archive tools. Prompts, cancels and
//! approval decisions are inbox commits; observation follows the agent's
//! status stream and reads its settled conversation entries.
use acyclic_harness::{
    Error, LocalHarness, Result,
    agent::{
        AgentPath, AgentRecord as _, Control, ConversationEntry, Delivered, InboxRecord, IntentId,
        Phase, Sender, StatusRecord, TurnInput,
    },
    approvals::ApprovalResponder,
    conversation::{ContentResidencyVerifier, FileRef},
    executor::TurnOutput,
};
use acyclic_stream::{FollowFilter, StreamError, StreamProvider as _};
use futures::{StreamExt as _, TryStreamExt as _, stream::LocalBoxStream};
use std::sync::Arc;

/// Bytes read back from one staged prompt or turn output.
const MAXIMUM_TEXT: u64 = 16 * 1024 * 1024;
const USER: &str = "local";

fn failed(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

fn intent(id: &str) -> Result<IntentId> {
    IntentId::try_from(id).map_err(failed)
}

/// What a follower observes, in commit order.
pub enum Event {
    /// The agent's phase after a transition, with its consumed inbox cursor.
    Status(StatusRecord),
    /// One settled turn: its message's inbox sequence, if a message started it.
    Turn {
        inbox: Option<u64>,
        entry: ConversationEntry,
    },
}

pub struct Conversation {
    local: Arc<LocalHarness>,
    agent: AgentPath,
    reader: Arc<dyn ContentResidencyVerifier>,
}

impl Conversation {
    /// The root agent of `local`, created on first use and reopened after.
    pub async fn open(local: Arc<LocalHarness>) -> Result<Arc<Self>> {
        let agent = local.agents().await?.1.clone();
        let reader = local.client_reader()?;
        Ok(Arc::new(Self {
            local,
            agent,
            reader,
        }))
    }

    /// Send `prompt` as message `id`; returns its inbox sequence. The same id
    /// and prompt replay exactly.
    pub async fn send(&self, id: &str, prompt: &str) -> Result<u64> {
        match self
            .local
            .prompt(&intent(id)?, prompt)
            .await
            .map_err(failed)?
        {
            Delivered::Committed { inbox } => {
                tracing::debug!(id, inbox, "message committed");
                Ok(inbox)
            }
            Delivered::Closed => Err(Error::Conflict("conversation is archived".into())),
        }
    }

    /// Stop the running turn; the conversation stays open.
    pub async fn cancel(&self) -> Result<()> {
        let (agents, _) = self.local.agents().await?;
        let by = Sender::User {
            principal: USER.into(),
        };
        let id = intent(&acyclic_harness::OperationId::new().to_string())?;
        agents
            .send(
                &self.agent,
                &by,
                &InboxRecord::Cancel(Control { by: by.clone(), id }),
            )
            .await
            .map_err(failed)?;
        Ok(())
    }

    /// Answer the agent's pending approval request as the local user.
    pub async fn decide(&self, request: &IntentId, approved: bool) -> Result<()> {
        self.local
            .decide(
                &self.agent,
                request,
                ApprovalResponder::Human(USER),
                approved,
                None,
            )
            .await
            .map_err(failed)?;
        Ok(())
    }

    /// The text of a staged prompt or turn output.
    pub async fn text(&self, file: &FileRef) -> Result<String> {
        let bytes = self.reader.read(file, MAXIMUM_TEXT, None, None)?.await?;
        String::from_utf8(bytes.to_vec()).map_err(failed)
    }

    /// The prompt and answer text of a settled turn.
    pub async fn turn(&self, entry: &ConversationEntry) -> Result<(Option<String>, String)> {
        let prompt = match &entry.input {
            TurnInput::Message { body, .. } => Some(self.text(body).await?),
            _ => None,
        };
        let output = self.text(&entry.output).await?;
        let output: TurnOutput = serde_json::from_str(&output).map_err(failed)?;
        Ok((prompt, output.text))
    }

    /// Number of settled turns.
    pub async fn settled(&self) -> Result<u64> {
        let (agents, _) = self.local.agents().await?;
        let conversation = self.agent.conversation().map_err(failed)?;
        match agents.provider().tail(conversation).await {
            Ok(tail) => Ok(tail),
            Err(StreamError::NotFound) => Ok(0),
            Err(error) => Err(failed(error)),
        }
    }

    /// Every settled turn from conversation sequence `from`, and the agent's
    /// phase from its latest status on. A status records only a phase change;
    /// a settled turn is its conversation entry.
    pub async fn events(
        self: &Arc<Self>,
        from: u64,
    ) -> Result<LocalBoxStream<'static, Result<Event>>> {
        let provider = Arc::clone(self.local.agents().await?.0.provider());
        let status = self.agent.status().map_err(failed)?;
        let latest = provider.tail(status.clone()).await.map_err(failed)?;
        let statuses = provider
            .follow(status, latest.saturating_sub(1), FollowFilter::default())
            .await
            .map_err(failed)?
            .map_err(failed)
            .and_then(|record| async move {
                let status = StatusRecord::decode(&record.value).map_err(failed)?;
                tracing::debug!(phase = ?status.phase, cursor = status.cursor, "agent status");
                Ok(Event::Status(status))
            });
        let path = self.agent.conversation().map_err(failed)?;
        // The conversation stream starts with the agent's first settled turn.
        let turns = futures::stream::try_unfold(
            (provider, path, from, None),
            |(provider, path, next, mut follow)| async move {
                loop {
                    let Some(records) = follow.as_mut() else {
                        match provider
                            .follow(path.clone(), next, FollowFilter::default())
                            .await
                        {
                            Ok(records) => follow = Some(records),
                            Err(StreamError::NotFound) => {
                                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                            }
                            Err(error) => return Err(failed(error)),
                        }
                        continue;
                    };
                    let record = records
                        .try_next()
                        .await
                        .map_err(failed)?
                        .ok_or_else(|| Error::Conflict("conversation follow ended".into()))?;
                    let entry = ConversationEntry::decode(&record.value).map_err(failed)?;
                    let inbox = match &entry.input {
                        TurnInput::Message { inbox, .. } => Some(*inbox),
                        _ => None,
                    };
                    let state = (provider, path, record.sequence + 1, follow);
                    return Ok(Some((Event::Turn { inbox, entry }, state)));
                }
            },
        );
        Ok(futures::stream::select(statuses.boxed_local(), turns.boxed_local()).boxed_local())
    }
}

/// Whether `status` ends the wait for a turn without its output: the agent
/// finished.
pub fn ended(status: &StatusRecord) -> Option<String> {
    match &status.phase {
        Phase::Done { outcome } => Some(format!("conversation ended: {outcome:?}")),
        _ => None,
    }
}
