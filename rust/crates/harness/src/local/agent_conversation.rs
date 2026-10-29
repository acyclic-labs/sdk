//! A typed client for one agent tree's root conversation, which fans out
//! through its spawn, send and archive tools. Prompts, cancels and approval
//! decisions are commands on the tree's [`super::Connection`], in process or
//! remote alike; observation is one [`super::Observer`] of its status, its
//! settled conversation entries and its tool output. Frontends render its
//! events; it holds no frontend state.
//!
//! [`Conversation::compact`], [`Conversation::rewind`] and
//! [`Conversation::clear_all`] select the model context of later turns. Each
//! settles like a turn, as one conversation entry, a checkpoint, which the
//! observer reports as [`super::Event::Turn`] whose input [`checkpoint`] names:
//! later turns see that checkpoint's summary, then the turns after it.
use super::Connection;
use crate::{
    Error, Result,
    agent::{
        AgentPath, ContextAction, ConversationEntry, Delivered, IntentId, Phase, Reply,
        StatusRecord, TurnInput,
    },
    conversation::FileRef,
    executor::TurnOutput,
    interaction::{ApprovalBinding, Interaction},
    model::ModelContent,
};
use acyclic_harness_client::{
    AgentCommand, AgentControlFailure, AgentExchangeError, AgentPageRequest, AgentReply, ReadTarget,
};
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

/// Where a sent message stands. Only a refusal is settled: an unknown
/// outcome may have committed, so it is resent exactly, never reported.
#[derive(Debug, PartialEq, Eq)]
pub enum Sent {
    /// Committed at this inbox sequence.
    Accepted(u64),
    /// Definitively not committed, for this reason.
    Refused(String),
    /// Neither known to have committed nor refused: resending the same id
    /// and prompt answers it, and never commits it twice.
    Unknown,
}

/// A prompt's exchange as [`Sent`]: only the host's decoded refusal, or a
/// closed conversation, is a refusal; any other failure after the request
/// left may have committed it.
fn sent(exchange: std::result::Result<AgentReply, AgentExchangeError>) -> Result<Sent> {
    match exchange {
        Ok(AgentReply::Delivered {
            delivery: Delivered::Committed { inbox },
        }) => Ok(Sent::Accepted(inbox)),
        Ok(AgentReply::Delivered {
            delivery: Delivered::Closed,
        }) => Ok(Sent::Refused("conversation is archived".into())),
        Ok(_) => Err(unexpected()),
        Err(AgentExchangeError::Control(refused @ AgentControlFailure::Refused { .. })) => {
            Ok(Sent::Refused(refused.to_string()))
        }
        Err(_) => Ok(Sent::Unknown),
    }
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

    /// The root agent's id in operation form: the task its failures name.
    #[must_use]
    pub fn task(&self) -> crate::OperationId {
        crate::OperationId::from_bytes(self.agent().id().to_bytes())
    }

    async fn delivered(&self, command: &AgentCommand) -> Result<Delivered> {
        match self.connection.execute(command).await? {
            AgentReply::Delivered { delivery } => Ok(delivery),
            _ => Err(unexpected()),
        }
    }

    /// The inbox sequence `command` committed at, which its settled entry
    /// carries.
    async fn committed(&self, command: &AgentCommand) -> Result<u64> {
        match self.delivered(command).await? {
            Delivered::Committed { inbox } => Ok(inbox),
            Delivered::Closed => Err(Error::Conflict("conversation is archived".into())),
        }
    }

    /// Compact the conversation as request `id`: one model request
    /// summarizes the current context, guided by `instructions`, and later
    /// turns start from that summary. Returns its inbox sequence; the
    /// checkpoint settles as the entry at it. The same id replays exactly.
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn compact(&self, id: &str, instructions: Option<&str>) -> Result<u64> {
        self.committed(&AgentCommand::Compact {
            agent: self.agent().clone(),
            id: intent(id)?,
            instructions: instructions.map(|text| ModelContent::Text(text.to_owned())),
        })
        .await
    }

    /// Rewind the conversation as request `id` to its latest checkpoint:
    /// every turn settled after it leaves later turns' context. Returns its
    /// inbox sequence, like [`Self::compact`].
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn rewind(&self, id: &str) -> Result<u64> {
        self.committed(&AgentCommand::Rewind {
            agent: self.agent().clone(),
            id: intent(id)?,
        })
        .await
    }

    /// Retire the conversation as request `id`: later turns, and a reopened
    /// transcript, start after it with no history. Returns its inbox
    /// sequence, like [`Self::compact`].
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn clear_all(&self, id: &str) -> Result<u64> {
        self.committed(&AgentCommand::ClearAll {
            agent: self.agent().clone(),
            id: intent(id)?,
        })
        .await
    }

    /// Send `prompt` as message `id`: accepted at its inbox sequence,
    /// refused, or of unknown outcome. The same id and prompt replay exactly.
    ///
    /// # Errors
    /// When the prompt cannot be prepared, or the endpoint answers another
    /// command.
    pub async fn deliver(&self, id: &str, prompt: &str) -> Result<Sent> {
        let command = AgentCommand::Prompt {
            agent: self.agent().clone(),
            id: intent(id)?,
            body: ModelContent::Text(prompt.to_owned()),
        };
        let sent = sent(self.connection.exchange(&command).await?)?;
        tracing::debug!(id, ?sent, "message sent");
        Ok(sent)
    }

    /// [`Self::deliver`] for a caller that treats every outcome but
    /// acceptance as a failure; returns its inbox sequence.
    ///
    /// # Errors
    /// A refusal with its reason, an unknown outcome, or as [`Self::deliver`].
    pub async fn send(&self, id: &str, prompt: &str) -> Result<u64> {
        match self.deliver(id, prompt).await? {
            Sent::Accepted(inbox) => Ok(inbox),
            Sent::Refused(reason) => Err(Error::Storage(reason)),
            Sent::Unknown => Err(Error::Storage(
                "the message's commit outcome is unknown; resend it".into(),
            )),
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

    /// The approval `request` that `agent` waits on, if it is still pending.
    async fn pending_approval(&self, request: &IntentId) -> Result<Option<Interaction>> {
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
        Ok(items
            .into_iter()
            .find(|pending| &pending.id == request)
            .map(|pending| pending.request)
            .filter(|request| matches!(request, Interaction::Approval { .. })))
    }

    /// The explanation approval `request` shows its approver, or `None` once
    /// it is no longer pending.
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn approval_prompt(&self, request: &IntentId) -> Result<Option<String>> {
        Ok(match self.pending_approval(request).await? {
            Some(Interaction::Approval { prompt, .. }) => Some(prompt),
            _ => None,
        })
    }

    /// The binding of `request`, which `agent` waits on.
    async fn pending_binding(&self, request: &IntentId) -> Result<ApprovalBinding> {
        match self.pending_approval(request).await? {
            Some(Interaction::Approval {
                operation_id,
                action_digest,
                ..
            }) => Ok(ApprovalBinding {
                operation_id,
                action_digest,
            }),
            _ => Err(Error::Conflict("no such approval is pending".into())),
        }
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
            let (range, length) =
                Self::read_at(connection, agent, target.clone(), bytes.len() as u64).await?;
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

    /// One bounded range of `target` on `agent` from `offset`, and the
    /// target's whole length.
    pub(super) async fn read_at(
        connection: &Connection,
        agent: &AgentPath,
        target: ReadTarget,
        offset: u64,
    ) -> Result<(Vec<u8>, u64)> {
        let reply = connection
            .execute(&AgentCommand::Read {
                agent: agent.clone(),
                target,
                offset,
            })
            .await?;
        reply.content().map_err(failed)
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

/// The selection a settled `entry` made when it is a checkpoint (a
/// compaction, rewind or clear); `None` for a turn.
#[must_use]
pub const fn checkpoint(entry: &ConversationEntry) -> Option<&ContextAction> {
    entry.checkpoint()
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

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_stream::{CommitRequest, IdempotencyKey, StreamError};

    #[test]
    fn only_a_host_refusal_settles_a_send() -> Result<()> {
        let refused = AgentExchangeError::Control(AgentControlFailure::Refused {
            code: "agent_turn_error".into(),
            reason: "Unknown command: /gone".into(),
        });
        assert_eq!(
            sent(Err(refused))?,
            Sent::Refused("Unknown command: /gone".into())
        );
        assert_eq!(
            sent(Ok(AgentReply::Delivered {
                delivery: Delivered::Committed { inbox: 4 },
            }))?,
            Sent::Accepted(4)
        );
        // Negative controls: an unknown commit outcome, and a session that
        // failed after the request left, may each have committed it.
        let request = CommitRequest {
            deadline_unix_millis: None,
            conditions: Vec::new(),
            mutations: Vec::new(),
            idempotency_key: IdempotencyKey::new(b"message".to_vec()).map_err(failed)?,
        };
        let unknown = AgentControlFailure::Unknown(Box::new(request));
        assert_eq!(
            sent(Err(AgentExchangeError::Control(unknown)))?,
            Sent::Unknown
        );
        assert_eq!(
            sent(Err(AgentExchangeError::Session(StreamError::Unavailable)))?,
            Sent::Unknown
        );
        Ok(())
    }
}
