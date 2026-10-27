//! A typed client for the local harness's root agent conversation, which fans
//! out through its spawn, send and archive tools. Prompts, cancels and
//! approval decisions are inbox commits; observation is one [`super::Observer`]
//! of its status, its settled conversation entries and its tool output. Frontends render
//! its events; it holds no frontend state.
use crate::{
    Error, LocalHarness, Result,
    agent::{
        AgentPath, Control, ConversationEntry, Delivered, InboxRecord, IntentId, Phase, Reply,
        Sender, StatusRecord, TurnInput,
    },
    approvals::{ApprovalPolicy, ApprovalResponder, Approver},
    conversation::{ContentResidencyVerifier, FileRef},
    executor::TurnOutput,
};
use acyclic_stream::{StreamError, StreamProvider as _};
use std::sync::Arc;

/// Bytes read back from one staged prompt or turn output.
const MAXIMUM_TEXT: u64 = 16 * 1024 * 1024;
/// The local user's principal when the agent's policy names no human.
const USER: &str = "local";

fn failed(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

fn intent(id: &str) -> Result<IntentId> {
    IntentId::try_from(id).map_err(failed)
}

/// One local agent's conversation, observed and driven as its local user.
pub struct Conversation {
    local: Arc<LocalHarness>,
    agent: AgentPath,
    reader: Arc<dyn ContentResidencyVerifier>,
    /// The human principals of the agent's policy: the terminal user is each.
    humans: Vec<String>,
}

impl Conversation {
    /// The root agent of `local`, created on first use and reopened after.
    ///
    /// # Errors
    /// Harness, storage or decoding failures.
    pub async fn open(local: Arc<LocalHarness>) -> Result<Arc<Self>> {
        let (agents, agent) = local.agents().await?;
        let agent = agent.clone();
        let reader = local.client_reader()?;
        let mut humans = Vec::new();
        if let Some(policy) = agents.state(&agent).await.map_err(failed)?.policy {
            let bytes = reader.read(&policy, MAXIMUM_TEXT, None, None)?.await?;
            let policy: ApprovalPolicy = serde_json::from_slice(&bytes).map_err(failed)?;
            for approver in policy.approvers {
                if let Approver::Human { principal } = approver
                    && !humans.contains(&principal)
                {
                    humans.push(principal);
                }
            }
        }
        if humans.is_empty() {
            humans.push(USER.into());
        }
        Ok(Arc::new(Self {
            local,
            agent,
            reader,
            humans,
        }))
    }

    /// Send `prompt` as message `id`; returns its inbox sequence. The same id
    /// and prompt replay exactly.
    ///
    /// # Errors
    /// Harness, storage or decoding failures.
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
    ///
    /// # Errors
    /// Harness, storage or decoding failures.
    pub async fn cancel(&self) -> Result<()> {
        let (agents, _) = self.local.agents().await?;
        let by = Sender::User {
            principal: USER.into(),
        };
        let id = intent(&crate::OperationId::new().to_string())?;
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

    /// Answer the agent's pending approval request as the local user, who
    /// holds every human stage of the policy: the active stage accepts one.
    ///
    /// # Errors
    /// Harness, storage or decoding failures.
    pub async fn decide(&self, request: &IntentId, approved: bool) -> Result<()> {
        let mut refused = None;
        for human in &self.humans {
            match self
                .local
                .decide(
                    &self.agent,
                    request,
                    ApprovalResponder::Human(human),
                    approved,
                    None,
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(crate::agent::Error::Turn(error))
                    if matches!(*error, Error::Unauthorized(_)) =>
                {
                    refused = Some(error);
                }
                Err(error) => return Err(failed(error)),
            }
        }
        Err(refused.map_or_else(
            || Error::Conflict("no human approver".into()),
            |error| *error,
        ))
    }

    /// The text of a staged prompt or turn output.
    ///
    /// # Errors
    /// Harness, storage or decoding failures.
    pub async fn text(&self, file: &FileRef) -> Result<String> {
        let bytes = self.reader.read(file, MAXIMUM_TEXT, None, None)?.await?;
        String::from_utf8(bytes.to_vec()).map_err(failed)
    }

    /// The prompt and answer text of a settled turn.
    ///
    /// # Errors
    /// Harness, storage or decoding failures.
    pub async fn turn(&self, entry: &ConversationEntry) -> Result<(Option<String>, String)> {
        let prompt = match &entry.input {
            TurnInput::Message { body, .. } => Some(self.text(body).await?),
            _ => None,
        };
        let output = self.text(&entry.output).await?;
        let output: TurnOutput = serde_json::from_str(&output).map_err(failed)?;
        Ok((prompt, output.text))
    }

    /// The committed reply to message `id`, which [`Self::send`] committed at
    /// inbox sequence `inbox`: its settled entry, pending, or dropped.
    ///
    /// # Errors
    /// Harness, storage or decoding failures, and an entry at `inbox` that
    /// settled another request.
    pub async fn reply(&self, id: &str, inbox: u64) -> Result<Reply> {
        let (agents, _) = self.local.agents().await?;
        agents
            .reply(&self.agent, &intent(id)?, inbox)
            .await
            .map_err(|error| match error {
                crate::agent::Error::Conflict(reason) => Error::Conflict(reason.into()),
                error => failed(error),
            })
    }

    /// Number of settled turns.
    ///
    /// # Errors
    /// Harness, storage or decoding failures.
    pub async fn settled(&self) -> Result<u64> {
        let (agents, _) = self.local.agents().await?;
        let conversation = self.agent.conversation().map_err(failed)?;
        match agents.provider().tail(conversation).await {
            Ok(tail) => Ok(tail),
            Err(StreamError::NotFound) => Ok(0),
            Err(error) => Err(failed(error)),
        }
    }

    /// Every settled turn from conversation sequence `from`, the agent's
    /// phase from its latest status on, and its newest tool call's output. A
    /// status records only a phase change; a settled turn is its conversation
    /// entry.
    ///
    /// # Errors
    /// See [`LocalHarness::observe`].
    pub async fn observe(&self, from: u64) -> Result<super::Observer> {
        self.local.observe(&self.agent, from).await
    }
}

/// Whether `status` ends the wait for a turn without its output: the agent
/// finished.
#[must_use]
pub fn ended(status: &StatusRecord) -> Option<String> {
    match &status.phase {
        Phase::Done { outcome } => Some(format!("conversation ended: {outcome:?}")),
        _ => None,
    }
}
