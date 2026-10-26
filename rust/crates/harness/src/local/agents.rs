//! The local host of the agent model: every agent of this harness on one A4
//! runtime over the filesystem's volume-authority stream, so a spawn creates
//! its child's volumes in the spawn's own commit, and its turns on the stock
//! executor with this harness's model, tools, context and compaction.

use super::{LocalHarness, LocalStream, storage};
use crate::{
    Error, OperationId, Result,
    agent::{
        AgentHost, AgentPath, AgentState, Delivered, ExecutorDriver, ForkAdopted, InboxRecord,
        IntentId, Message, Sender, VolumeBinding,
    },
    conversation::{ContentAccess, ContentPublisher, VolumeRef},
    executor::{Executor, StockExecutor},
    filesystem::{
        FilesystemSpawnVolumes, FilesystemTurnHost, GOAL_VOLUME, PRIVATE_VOLUME, TurnAdmission,
        TurnAuthority, TurnExecutors, TurnLimits,
    },
    model::ModelContent,
};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use acyclic_stream::BoxProviderFuture;
use std::sync::{Arc, Weak};

/// Turns of local agents.
pub type LocalTurns = FilesystemTurnHost<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;
/// Every agent of one local harness.
pub type LocalAgents = AgentHost<LocalStream>;

mod decisions;

/// Settled turns of history in each agent turn's model context.
const HISTORY_TURNS: u32 = 64;
/// Backstop on one agent delivery, which awaits a whole turn. A turn is
/// bounded by its model steps and each model and tool call's own deadline;
/// this only reclaims a delivery whose provider never answers.
const TURN_BACKSTOP_MS: u64 = 6 * 60 * 60 * 1000;

/// The hosted agents, their root, and the runtime task that ends with them.
pub(super) struct Hosted {
    agents: Arc<LocalAgents>,
    turns: Arc<LocalTurns>,
    root: AgentPath,
    failures: tokio::sync::broadcast::Sender<AgentFailure>,
    run: tokio::task::JoinHandle<()>,
}

/// One failed agent delivery. It stays queued and is retried; a waiting
/// client reports it rather than waiting for an output that may never come.
#[derive(Clone, Debug)]
pub struct AgentFailure {
    /// The actor whose delivery failed; see [`AgentHost::actor`].
    pub actor: ActorReference,
    /// The failure, as the runtime reported it.
    pub error: String,
}

impl Drop for Hosted {
    fn drop(&mut self) {
        self.run.abort();
    }
}

/// Builds each agent turn's executor from the harness that hosts it.
struct Executors(Weak<LocalHarness>);

impl TurnExecutors for Executors {
    fn executor<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, Result<Arc<dyn Executor>>> {
        Box::pin(async move {
            let local = self
                .0
                .upgrade()
                .ok_or_else(|| Error::Unsupported("local harness is closed".into()))?;
            let writer = authority
                .content
                .writer
                .as_ref()
                .ok_or_else(|| Error::Invalid("agent turn has no private writer".into()))?;
            let prompt = match input {
                ModelContent::Text(text) => Some(text.as_str()),
                ModelContent::Part(_) | ModelContent::Parts(_) => None,
            };
            let manifest = match &local.context_manifest {
                Some(manifest) => manifest.clone(),
                None => {
                    local
                        .stage_context_manifest_with(turn, prompt, &**writer)?
                        .await?
                }
            };
            let context = local
                .harness
                .agent_context(&authority.grants, authority.content.clone())?;
            let reader = authority.content.reader.clone();
            let (model, provider) = local.agent()?;
            let provider = provider
                .bind_content(reader.clone(), local.limits.file_bytes)
                .unwrap_or_else(|| provider.clone());
            // The agent tools act for this turn: their effects commit with it.
            let mut tools = local.tools().clone();
            if let Some(effects) = &authority.effects {
                crate::tools::bind_agent_tools(
                    &mut tools,
                    agent,
                    Arc::clone(effects),
                    Arc::clone(writer),
                    local.limits.file_bytes,
                    local.max_children,
                )?;
            }
            let executor = StockExecutor::new(
                model.clone(),
                provider,
                crate::context::ContextPipeline::default(),
                tools,
            )
            .with_limits(local.limits)
            .with_compaction_policy(local.compaction.clone())
            .with_context_manifest(manifest, reader)
            .await?
            .with_tool_authority(context.scope().clone(), authority.approval.clone())?
            .with_task_context(&context, turn)?;
            // The agent's first turn is the one before its first conversation entry.
            #[cfg(any(test, feature = "hooks"))]
            let executor = executor
                .with_agent_hooks(&context, turn, input, agent.conversation_tail == 0)
                .await?;
            Ok(Arc::new(executor) as Arc<dyn Executor>)
        })
    }
}

impl LocalHarness {
    /// Every agent of this harness on one A4 runtime, and its root agent.
    ///
    /// The first call creates the root agent (replayed exactly on reopen) and
    /// starts the runtime; it stops when this harness drops. Controls are
    /// [`AgentHost`] commits; observation is the agents' status and
    /// conversation streams.
    pub async fn agents(self: &Arc<Self>) -> Result<(&Arc<LocalAgents>, &AgentPath)> {
        let hosted = self
            .agents
            .get_or_try_init(|| async { self.host_agents().await })
            .await?;
        Ok((&hosted.agents, &hosted.root))
    }

    /// Send `prompt` to the root agent as message `id`: one staged body file
    /// on its private volume, then one inbox commit. Retrying the same `id`
    /// with the same prompt replays exactly.
    /// An uncertain inbox commit returns `agent::Error::Unknown` with its exact
    /// request; retain it and settle it through `agent::submit`.
    pub async fn prompt(
        self: &Arc<Self>,
        id: &IntentId,
        prompt: &str,
    ) -> std::result::Result<Delivered, crate::agent::Error> {
        let preparation = |error| crate::agent::Error::Turn(Box::new(error));
        let (agents, root) = self.agents().await.map_err(preparation)?;
        let mut hasher = blake3::Hasher::new_derive_key("acyclic.local.agent.prompt.v1");
        hasher.update(root.as_str().as_bytes());
        hasher.update(&[0]);
        hasher.update(id.as_str().as_bytes());
        let mut bytes = [0; 16];
        hasher.finalize_xof().fill(&mut bytes);
        let operation = OperationId::from_bytes(bytes);
        let body = self
            .input_publisher_with_limit(self.limits.file_bytes)
            .map_err(preparation)?
            .stage_once(
                operation,
                &format!("inputs/{operation}.txt"),
                prompt.as_bytes(),
                "text/plain",
                "prompt",
                ContentAccess::default(),
            )
            .map_err(preparation)?
            .await
            .map_err(preparation)?
            .0;
        let from = Sender::User {
            principal: "local".into(),
        };
        let message = InboxRecord::Message(Message {
            from: from.clone(),
            id: id.clone(),
            body: Box::new(body),
        });
        agents.send(root, &from, &message).await
    }

    /// The configured approval policy, staged once on the root's private
    /// volume: the root agent's tools run under it.
    async fn root_policy(
        &self,
        root: &crate::agent::AgentId,
    ) -> Result<crate::conversation::FileRef> {
        let policy = crate::contract::canonical_json_bytes(&self.approval_policy)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let mut hasher = blake3::Hasher::new_derive_key("acyclic.local.agent.policy.v1");
        hasher.update(root.as_str().as_bytes());
        hasher.update(&[0]);
        hasher.update(&policy);
        let mut bytes = [0; 16];
        hasher.finalize_xof().fill(&mut bytes);
        let operation = OperationId::from_bytes(bytes);
        Ok(self
            .input_publisher_with_limit(self.limits.file_bytes)?
            .stage_once(
                operation,
                &format!("inputs/{operation}.json"),
                &policy,
                "application/json",
                "policy",
                ContentAccess::default(),
            )?
            .await?
            .0)
    }

    async fn host_agents(self: &Arc<Self>) -> Result<Hosted> {
        let authority = self.root_authority.clone().ok_or_else(|| {
            Error::Unsupported("an embedded harness has no agent signing custody".into())
        })?;
        let id = crate::agent::AgentId::from_bytes(self.root.into_bytes());
        let stream = self
            .agent_stream
            .clone()
            .ok_or_else(|| Error::Unsupported("an embedded harness has no agent stream".into()))?;
        let limits = TurnLimits {
            maximum_bytes: self.limits.file_bytes,
            max_steps: u32::try_from(self.limits.model_steps)
                .map_err(|_| Error::Invalid("model step limit exceeds u32".into()))?,
            history_turns: HISTORY_TURNS,
        };
        let turns = Arc::new(
            FilesystemTurnHost::new(
                stream.clone(),
                self.files.clone(),
                authority.clone(),
                TurnAdmission::LocalRoot,
                Arc::new(Executors(Arc::downgrade(self))),
                limits,
            )
            .with_approvers(self.approval_models.clone())
            .with_goals(self.goal_continuations)
            .with_prompt(self.prompt.as_deref()),
        );
        let within =
            ActorReference::new(b"local", b"agents", id.as_str().as_bytes()).map_err(storage)?;
        let agents = Arc::new(
            AgentHost::with_spawn_volumes(
                stream,
                ExecutorDriver::new(Arc::clone(&turns)),
                within,
                ActorLimits::new(TURN_BACKSTOP_MS, 1 << 20, 1 << 16).map_err(storage)?,
                Arc::new(SystemClock),
                Arc::new(FilesystemSpawnVolumes::new(
                    self.files.clone(),
                    authority,
                    TurnAdmission::LocalRoot,
                    self.limits.file_bytes,
                )),
            )
            .map_err(storage)?,
        );
        // A reopened root keeps the policy it was created with: the agent
        // model has no policy-update record, and replaying a creation with
        // another policy would refuse the reopen.
        let existing = AgentPath::root(&id);
        let root = match agents.state(&existing).await {
            Ok(state) => {
                let configured = self.root_policy(&id).await?;
                if state.policy.as_ref() != Some(&configured) {
                    tracing::warn!(
                        "the configured approval policy differs from the one this workspace's                          root agent was created with; the agent keeps its original policy"
                    );
                }
                agents.host(&existing).map_err(storage)?;
                existing
            }
            Err(crate::agent::Error::NoSuchAgent) => self.create_root(&agents, id).await?,
            Err(error) => return Err(storage(error)),
        };
        let (failures, _) = tokio::sync::broadcast::channel(64);
        let report = failures.clone();
        let runner = Arc::clone(&agents);
        let run = tokio::spawn(async move {
            let Err(error) = runner
                .run(|actor, error| {
                    tracing::warn!(?actor, %error, "agent turn failed");
                    let _ = report.send(AgentFailure {
                        actor: actor.clone(),
                        error: error.to_string(),
                    });
                })
                .await;
            tracing::error!(%error, "agent runtime stopped");
        });
        Ok(Hosted {
            agents,
            turns,
            root,
            failures,
            run,
        })
    }

    /// Create the root agent over this harness's volumes and policy.
    async fn create_root(
        &self,
        agents: &LocalAgents,
        id: crate::agent::AgentId,
    ) -> Result<AgentPath> {
        let bind = |name: &str, volume: &VolumeRef| VolumeBinding {
            name: name.to_owned(),
            volume: volume.clone(),
        };
        let policy = self.root_policy(&id).await?;
        agents
            .create(
                &IntentId::try_from("root").map_err(storage)?,
                ForkAdopted {
                    agent: id,
                    parent: None,
                    forked_at: 0,
                    conversation_tail: 0,
                    volumes: vec![
                        bind(PRIVATE_VOLUME, &self.journal),
                        bind("project", &self.project),
                        bind(GOAL_VOLUME, &self.scratch),
                    ],
                    brief: None,
                    policy: Some(policy),
                },
            )
            .await
            .map_err(storage)
    }

    /// Failed agent deliveries from now on, once the agents are hosted.
    pub fn agent_failures(&self) -> Option<tokio::sync::broadcast::Receiver<AgentFailure>> {
        self.agents.get().map(|hosted| hosted.failures.subscribe())
    }
}
