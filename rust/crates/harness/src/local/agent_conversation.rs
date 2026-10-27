//! A typed client for one agent tree's root conversation, which fans out
//! through its spawn, send and archive tools. Prompts, cancels and approval
//! decisions are commands on the tree's [`super::Connection`], in process or
//! remote alike; observation is one [`super::Observer`] of its status, its
//! settled conversation entries and its tool output. Frontends render its
//! events; it holds no frontend state.
use super::Connection;
use crate::{
    Error, Result,
    agent::{
        AgentPath, ConversationEntry, Delivered, IntentId, Phase, Reply, StatusRecord, TurnInput,
    },
    conversation::FileRef,
    executor::TurnOutput,
    interaction::{ApprovalBinding, Interaction},
    model::ModelContent,
};
use acyclic_harness_client::{AgentCommand, AgentPageRequest, AgentReply, ReadTarget};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Bytes read back from one staged prompt or turn output.
const MAXIMUM_TEXT: u64 = 16 * 1024 * 1024;

fn failed(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

fn intent(id: &str) -> Result<IntentId> {
    IntentId::try_from(id).map_err(failed)
}

fn unexpected() -> Error {
    Error::Conflict("the endpoint answered another command".into())
}

/// One agent tree's root conversation, driven by its connection's principal.
pub struct Conversation {
    connection: Connection,
    /// The binding each decision was sent with: a retried decision, its
    /// reply lost after the agent resumed, is sent again exactly, though the
    /// request is no longer pending.
    decided: Mutex<HashMap<IntentId, ApprovalBinding>>,
}

impl Conversation {
    /// The conversation of `connection`'s root.
    ///
    /// # Errors
    /// Never today; kept fallible for the endpoint handshake.
    pub fn open(connection: Connection) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            connection,
            decided: Mutex::new(HashMap::new()),
        }))
    }

    fn agent(&self) -> &AgentPath {
        self.connection.root()
    }

    async fn delivered(&self, command: &AgentCommand) -> Result<Delivered> {
        match self.connection.execute(command).await? {
            AgentReply::Delivered { delivery } => Ok(delivery),
            _ => Err(unexpected()),
        }
    }

    /// Send `prompt` as message `id`; returns its inbox sequence. The same id
    /// and prompt replay exactly.
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn send(&self, id: &str, prompt: &str) -> Result<u64> {
        let command = AgentCommand::Prompt {
            agent: self.agent().clone(),
            id: intent(id)?,
            body: ModelContent::Text(prompt.to_owned()),
        };
        match self.delivered(&command).await? {
            Delivered::Committed { inbox } => {
                tracing::debug!(id, inbox, "message committed");
                Ok(inbox)
            }
            Delivered::Closed => Err(Error::Conflict("conversation is archived".into())),
        }
    }

    /// Stop the running turn; the conversation stays open.
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn cancel(&self) -> Result<()> {
        let command = AgentCommand::Cancel {
            agent: self.agent().clone(),
            id: intent(&crate::OperationId::new().to_string())?,
        };
        self.delivered(&command).await.map(drop)
    }

    /// Answer the agent's pending approval request `request`, bound to the
    /// exact action it shows, as this connection's principal.
    ///
    /// # Errors
    /// When `request` is not pending, or session, host or decoding failures.
    pub async fn decide(&self, request: &IntentId, approved: bool) -> Result<()> {
        let sent = self.decided().get(request).cloned();
        let binding = match sent {
            Some(binding) => binding,
            None => self.pending_binding(request).await?,
        };
        self.decided().insert(request.clone(), binding.clone());
        self.delivered(&AgentCommand::Decide {
            agent: self.agent().clone(),
            id: request.clone(),
            binding,
            approved,
            reason: None,
        })
        .await
        .map(drop)
    }

    fn decided(&self) -> std::sync::MutexGuard<'_, HashMap<IntentId, ApprovalBinding>> {
        self.decided
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The binding of `request`, which `agent` waits on.
    async fn pending_binding(&self, request: &IntentId) -> Result<ApprovalBinding> {
        let AgentReply::Approvals { items, .. } = self
            .connection
            .execute(&AgentCommand::Approvals {
                agent: self.agent().clone(),
                page: AgentPageRequest::default(),
            })
            .await?
        else {
            return Err(unexpected());
        };
        items
            .into_iter()
            .find(|pending| &pending.id == request)
            .and_then(|pending| match pending.request {
                Interaction::Approval {
                    operation_id,
                    action_digest,
                    ..
                } => Some(ApprovalBinding {
                    operation_id,
                    action_digest,
                }),
                _ => None,
            })
            .ok_or_else(|| Error::Conflict("no such approval is pending".into()))
    }

    /// Reads all of `target` on `agent`, at most `maximum` bytes.
    pub(super) async fn read(
        connection: &Connection,
        agent: &AgentPath,
        target: ReadTarget,
        maximum: u64,
    ) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        loop {
            let reply = connection
                .execute(&AgentCommand::Read {
                    agent: agent.clone(),
                    target: target.clone(),
                    offset: bytes.len() as u64,
                })
                .await?;
            let (range, length) = reply.content().map_err(failed)?;
            if length > maximum {
                return Err(Error::Invalid("read file exceeds its limit".into()));
            }
            let done = range.is_empty();
            bytes.extend_from_slice(&range);
            if done || bytes.len() as u64 >= length {
                return Ok(bytes);
            }
        }
    }

    /// The text of a staged prompt or turn output.
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn text(&self, file: &FileRef) -> Result<String> {
        let bytes = Self::read(
            &self.connection,
            self.agent(),
            ReadTarget::File { file: file.clone() },
            MAXIMUM_TEXT,
        )
        .await?;
        String::from_utf8(bytes).map_err(failed)
    }

    /// The prompt and answer text of a settled turn.
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn turn(&self, entry: &ConversationEntry) -> Result<(Option<String>, String)> {
        let prompt = match &entry.input {
            TurnInput::Message { body, .. } => Some(self.text(body).await?),
            _ => None,
        };
        let output = self.text(&entry.output).await?;
        let output: TurnOutput = serde_json::from_str(&output).map_err(failed)?;
        Ok((prompt, output.text))
    }

    async fn settlement(&self) -> Result<(u64, u64, Phase)> {
        match self
            .connection
            .execute(&AgentCommand::Settlement {
                agent: self.agent().clone(),
            })
            .await?
        {
            AgentReply::Settlement {
                settled,
                statuses,
                phase,
            } => Ok((settled, statuses, phase)),
            _ => Err(unexpected()),
        }
    }

    /// The committed reply to message `id`, which [`Self::send`] committed at
    /// inbox sequence `inbox`: its settled entry, pending, or dropped.
    ///
    /// # Errors
    /// Session, host or decoding failures, and an entry at `inbox` that
    /// settled another request.
    pub async fn reply(&self, id: &str, inbox: u64) -> Result<Reply> {
        match self
            .connection
            .execute(&AgentCommand::Reply {
                agent: self.agent().clone(),
                id: intent(id)?,
                inbox,
            })
            .await?
        {
            AgentReply::Reply { reply } => Ok(reply),
            _ => Err(unexpected()),
        }
    }

    /// Why a dropped prompt got no turn: the agent finished, or a cancel
    /// cleared it.
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn dropped(&self) -> Result<String> {
        let (_, _, phase) = self.settlement().await?;
        Ok(finished(&phase).unwrap_or_else(|| "the prompt was cancelled".into()))
    }

    /// Number of settled turns.
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn settled(&self) -> Result<u64> {
        Ok(self.settlement().await?.0)
    }

    /// Every settled turn from conversation sequence `from`, the agent's
    /// phase from its latest status on, and its newest tool call's output. A
    /// status records only a phase change; a settled turn is its conversation
    /// entry.
    ///
    /// # Errors
    /// When the follows are refused.
    pub async fn observe(&self, from: u64) -> Result<super::Observer> {
        let (_, statuses, _) = self.settlement().await?;
        super::Observer::open(&self.connection, from, statuses.saturating_sub(1))
    }
}

/// Whether `status` ends the wait for a turn without its output: the agent
/// finished.
#[must_use]
pub fn ended(status: &StatusRecord) -> Option<String> {
    finished(&status.phase)
}

fn finished(phase: &Phase) -> Option<String> {
    match phase {
        Phase::Done { outcome } => Some(format!("conversation ended: {outcome:?}")),
        _ => None,
    }
}
