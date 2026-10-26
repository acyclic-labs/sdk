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
    approvals::{ApprovalPolicy, ApprovalResponder, Approver},
    conversation::{ContentResidencyVerifier, FileRef},
    executor::TurnOutput,
};
use acyclic_stream::{FollowFilter, StreamError, StreamProvider as _};
use futures::{StreamExt as _, TryStreamExt as _, stream::LocalBoxStream};
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

/// What a follower observes, in commit order.
pub enum Event {
    /// The agent's phase after a transition, with its consumed inbox cursor.
    Status(StatusRecord),
    /// One settled turn: its message's inbox sequence, if a message started it.
    Turn {
        inbox: Option<u64>,
        entry: ConversationEntry,
    },
    /// The runtime failed the agent's delivery; it stays queued for a retry
    /// that may fail the same way.
    Failed(String),
}

pub struct Conversation {
    local: Arc<LocalHarness>,
    agent: AgentPath,
    reader: Arc<dyn ContentResidencyVerifier>,
    /// The human principals of the agent's policy: the terminal user is each.
    humans: Vec<String>,
}

impl Conversation {
    /// The root agent of `local`, created on first use and reopened after.
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

    /// Answer the agent's pending approval request as the local user, who
    /// holds every human stage of the policy: the active stage accepts one.
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
                Err(acyclic_harness::agent::Error::Turn(error))
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

    /// Whether message `inbox`, which a status cursor already passed, was
    /// dropped without a turn (cancelled): it is neither queued, unread nor in
    /// a running turn. Returns the conversation tail that holds every turn
    /// settled by then, so an entry for it below that tail still answers it.
    pub async fn dropped(&self, inbox: u64) -> Result<Option<u64>> {
        let (agents, _) = self.local.agents().await?;
        let state = agents.state(&self.agent).await.map_err(failed)?;
        let queued = state.queued.iter().any(
            |input| matches!(input, TurnInput::Message { inbox: queued, .. } if *queued == inbox),
        );
        let unread = state
            .overflow
            .is_some_and(|first| inbox >= first && !state.taken.contains(&inbox));
        Ok((!queued && !unread && state.turn.is_none()).then_some(state.conversation_tail))
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
        let (agents, _) = self.local.agents().await?;
        let provider = Arc::clone(agents.provider());
        let actor = agents.actor(&self.agent).map_err(failed)?;
        let receiver = self
            .local
            .agent_failures()
            .ok_or_else(|| Error::Conflict("agents are not hosted".into()))?;
        // Failures of this agent's deliveries, from now on.
        let failures = futures::stream::unfold(receiver, move |mut receiver| {
            let actor = actor.clone();
            async move {
                loop {
                    match receiver.recv().await {
                        Ok(failure) if failure.actor == actor => {
                            return Some((Ok(Event::Failed(failure.error)), receiver));
                        }
                        Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                    }
                }
            }
        });
        // Register, then check: the status follow is open before the first
        // conversation follow, and the step that creates the conversation
        // stream also appends a status, so a missing stream is retried only
        // after the agent's next status.
        let status = self.agent.status().map_err(failed)?;
        let latest = provider.tail(status.clone()).await.map_err(failed)?;
        let statuses = provider
            .follow(status, latest.saturating_sub(1), FollowFilter::default())
            .await
            .map_err(failed)?;
        let path = self.agent.conversation().map_err(failed)?;
        let observed = futures::stream::try_unfold(
            (provider, path, statuses, from, None, true),
            |(provider, path, mut statuses, next, mut turns, mut retry)| async move {
                if turns.is_none() && retry {
                    retry = false;
                    match provider
                        .follow(path.clone(), next, FollowFilter::default())
                        .await
                    {
                        Ok(records) => turns = Some(records),
                        Err(StreamError::NotFound) => {}
                        Err(error) => return Err(failed(error)),
                    }
                }
                let ended = || Error::Conflict("agent follow ended".into());
                let event = match turns.as_mut() {
                    Some(records) => tokio::select! {
                        record = statuses.try_next() => Err(record.map_err(failed)?.ok_or_else(ended)?),
                        record = records.try_next() => Ok(record.map_err(failed)?.ok_or_else(ended)?),
                    },
                    None => Err(statuses.try_next().await.map_err(failed)?.ok_or_else(ended)?),
                };
                let (event, next) = match event {
                    Err(record) => {
                        let status = StatusRecord::decode(&record.value).map_err(failed)?;
                        tracing::debug!(phase = ?status.phase, cursor = status.cursor, "agent status");
                        retry = true;
                        (Event::Status(status), next)
                    }
                    Ok(record) => {
                        let entry = ConversationEntry::decode(&record.value).map_err(failed)?;
                        let inbox = match &entry.input {
                            TurnInput::Message { inbox, .. } => Some(*inbox),
                            _ => None,
                        };
                        (Event::Turn { inbox, entry }, record.sequence + 1)
                    }
                };
                Ok(Some((event, (provider, path, statuses, next, turns, retry))))
            },
        );
        Ok(futures::stream::select(observed.boxed_local(), failures.boxed_local()).boxed_local())
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
