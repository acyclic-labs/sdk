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
        FilesystemSpawnVolumes, FilesystemTurnHost, PRIVATE_VOLUME, TurnAuthority, TurnExecutors,
        TurnLimits,
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
pub type LocalAgents = AgentHost<LocalStream, ExecutorDriver<Arc<LocalTurns>>>;

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
    run: tokio::task::JoinHandle<()>,
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
            let context = local.runtime.harness().agent_context(
                turn,
                &authority.grants,
                authority.content.clone(),
            )?;
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

    async fn host_agents(self: &Arc<Self>) -> Result<Hosted> {
        let authority = self.root_authority.clone().ok_or_else(|| {
            Error::Unsupported("an embedded harness has no agent signing custody".into())
        })?;
        let id = crate::agent::AgentId::from_bytes(self.root.into_bytes());
        let stream = self.agent_stream.clone().ok_or_else(|| {
            Error::Unsupported("an embedded harness has no agent stream".into())
        })?;
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
                Arc::new(Executors(Arc::downgrade(self))),
                limits,
            )
            .with_approvers(self.approval_models.clone()),
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
                    self.limits.file_bytes,
                )),
            )
            .map_err(storage)?,
        );
        let bind = |name: &str, volume: &VolumeRef| VolumeBinding {
            name: name.to_owned(),
            volume: volume.clone(),
        };
        let root = agents
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
                        bind("scratch", &self.scratch),
                    ],
                    brief: None,
                    policy: None,
                },
            )
            .await
            .map_err(storage)?;
        let runner = Arc::clone(&agents);
        let run = tokio::spawn(async move {
            let Err(error) = runner
                .run(|actor, error| tracing::warn!(?actor, %error, "agent turn failed"))
                .await;
            tracing::error!(%error, "agent runtime stopped");
        });
        Ok(Hosted {
            agents,
            turns,
            root,
            run,
        })
    }
}
