//! The local host of the agent model: every agent of this harness on one A4
//! runtime over the filesystem's volume-authority stream, so a spawn creates
//! its child's volumes in the spawn's own commit, and its turns on the stock
//! executor with this harness's model, tools, context and compaction.

use super::{LocalHarness, LocalStream, storage};
use crate::{
    Error, OperationId, Result,
    agent::{AgentHost, AgentPath, AgentState, IntentId, VolumeBinding},
    conversation::{ContentAccess, ContentPublisher, VolumeRef},
    executor::{Executor, StockExecutor},
    filesystem::{
        FilesystemTurnHost, GOAL_VOLUME, PRIVATE_VOLUME, TurnAuthority, TurnContext, TurnExecutors,
        TurnLimits,
    },
    model::ModelContent,
};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::SystemClock};
use acyclic_auth::{authority::PolicyDecision, contract::Principal};
use acyclic_fs::{LocalAuthorityBackend, LocalObjectBackend};
use acyclic_harness_client::AgentCommand;
use acyclic_stream::{BoxProviderFuture, StreamError};
use std::sync::{Arc, Weak};

/// Turns of local agents.
pub type LocalTurns = FilesystemTurnHost<LocalStream, LocalAuthorityBackend, LocalObjectBackend>;
/// Every agent of one local harness.
pub type LocalAgents = AgentHost<LocalStream>;

/// Settled turns of history in each agent turn's model context.
const HISTORY_TURNS: u32 = 64;
/// Backstop on one agent delivery, which awaits a whole turn. A turn is
/// bounded by its model steps and each model and tool call's own deadline;
/// this only reclaims a delivery whose provider never answers.
const TURN_BACKSTOP_MS: u64 = 6 * 60 * 60 * 1000;

/// The served agent tree: the same [`crate::hosting::Server`] a remote host
/// serves, and the runtime task that ends with it.
pub(super) struct Hosted {
    server: Arc<LocalServer>,
    failures: tokio::sync::broadcast::Sender<AgentFailure>,
    run: tokio::task::JoinHandle<()>,
}

/// The local harness's agent tree, served as every host serves one.
pub type LocalServer = crate::hosting::Server<LocalStream, LocalObjectBackend>;

impl Hosted {
    pub(super) fn agents(&self) -> &Arc<LocalAgents> {
        self.server.bound().agents()
    }

    pub(super) fn root(&self) -> &AgentPath {
        self.server.root()
    }

    pub(super) const fn server(&self) -> &Arc<LocalServer> {
        &self.server
    }

    pub(super) const fn failures(&self) -> &tokio::sync::broadcast::Sender<AgentFailure> {
        &self.failures
    }
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

/// The local user owns the whole tree: its policy admits every command its
/// own session credential carries, and its prompts attach the checkout files
/// they mention.
struct LocalUser(Weak<LocalHarness>);

impl crate::hosting::Platform for LocalUser {
    fn policy<'a>(
        &'a self,
        _: &'a Principal,
        _: &'a AgentCommand,
    ) -> BoxProviderFuture<'a, std::result::Result<PolicyDecision, StreamError>> {
        Box::pin(async { Ok(PolicyDecision::Allow) })
    }

    fn attachments<'a>(
        &'a self,
        key: &'a [u8],
        text: &'a str,
    ) -> BoxProviderFuture<'a, Result<Option<crate::conversation::FileRef>>> {
        Box::pin(async move {
            self.0
                .upgrade()
                .ok_or_else(|| Error::Unsupported("local harness is closed".into()))?
                .prompt_attachments(key, text)
                .await
        })
    }
}

/// What the local harness adds to each stock turn: its context manifest,
/// the turn's task context and tool authority, and its hooks.
struct LocalContext(Weak<LocalHarness>);

impl TurnContext for LocalContext {
    fn prepare<'a>(
        &'a self,
        (executor, _): (StockExecutor, crate::conversation::Limits),
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, Result<StockExecutor>> {
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
            let executor = executor
                .with_context_manifest(manifest, authority.content.reader.clone())
                .await?
                .with_tool_authority(context.scope().clone(), authority.approval.clone())?
                .with_task_context(&context, turn)?;
            // SessionStart runs once per agent, keyed by the agent itself, so
            // a forked child (which inherits its parent's tail) runs its own.
            #[cfg(any(test, feature = "hooks"))]
            let executor = executor
                .with_agent_hooks(&context, turn, input, session_operation(&agent.path))
                .await?;
            #[cfg(not(any(test, feature = "hooks")))]
            let _ = agent;
            Ok(executor)
        })
    }
}

/// A native host's agents: it owns their loop and tool execution, so none
/// of their turns runs here.
struct NativeHost;

impl TurnExecutors for NativeHost {
    fn executor<'a>(
        &'a self,
        _: &'a AgentState,
        _: OperationId,
        _: &'a ModelContent,
        _: &'a TurnAuthority,
    ) -> BoxProviderFuture<'a, Result<Arc<dyn Executor>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "native hosts own their agent loop and tool execution".into(),
            ))
        })
    }
}

impl LocalHarness {
    /// Every agent turn's executor: the stock composition over this
    /// harness's model, tools, limits and compaction, in its context.
    fn executors(self: &Arc<Self>) -> Arc<dyn TurnExecutors> {
        match &self.agent {
            Some(model) => Arc::new(self.preset.executors(
                model.clone(),
                self.tools().clone(),
                (self.limits, self.compaction.clone()),
                Arc::new(LocalContext(Arc::downgrade(self))),
            )),
            None => Arc::new(NativeHost),
        }
    }
}

impl LocalHarness {
    /// Guest sources use the same public enrollment producer as hosted trees.
    #[cfg(all(target_os = "linux", feature = "native-execution"))]
    pub(super) async fn guest_session(
        self: &Arc<Self>,
        enrollment: crate::filesystem::hosting::LocalGuestEnrollment,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::native_mount::GuestSessionAdmission> {
        use crate::filesystem::hosting::{Bound, Placement, Turns};
        let issuer = self.root_authority.clone().ok_or_else(|| {
            Error::Unsupported("local guest enrollment requires signing custody".into())
        })?;
        let bound = Bound::local_root(
            issuer,
            Placement {
                filesystem: self.volume_filesystem.clone(),
                provider: self.project.provider().clone(),
                within: ActorReference::new(b"local", b"native", b"guests").map_err(storage)?,
            },
            Turns {
                executors: self.executors(),
                limits: TurnLimits {
                    maximum_bytes: self.limits.file_bytes,
                    max_steps: u32::try_from(self.limits.model_steps)
                        .map_err(|_| Error::Invalid("model step limit exceeds u32".into()))?,
                    history_turns: HISTORY_TURNS,
                },
                actors: ActorLimits::new(TURN_BACKSTOP_MS, 1 << 20, 1 << 16).map_err(storage)?,
                clock: Arc::new(SystemClock),
                approval_policy: self.preset.approval_policy.clone(),
            },
        )
        .map_err(storage)?;
        bound.guest_session(enrollment, work).await
    }

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
        Ok((hosted.agents(), hosted.root()))
    }

    /// The configured approval policy, staged once on the root's private
    /// volume: the root agent's tools run under it.
    async fn root_policy(
        &self,
        root: &crate::agent::AgentId,
    ) -> Result<crate::conversation::FileRef> {
        let policy = crate::contract::canonical_json_bytes(&self.preset.approval_policy)
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
        use crate::hosting::{Bound, RootRequest, Serving, Turns};
        let authority = self.root_authority.clone().ok_or_else(|| {
            Error::Unsupported("an embedded harness has no agent signing custody".into())
        })?;
        let id = crate::agent::AgentId::from_bytes(self.root.into_bytes());
        let within =
            ActorReference::new(b"local", b"agents", id.as_str().as_bytes()).map_err(storage)?;
        let turns = Turns {
            executors: self.executors(),
            limits: TurnLimits {
                maximum_bytes: self.limits.file_bytes,
                max_steps: u32::try_from(self.limits.model_steps)
                    .map_err(|_| Error::Invalid("model step limit exceeds u32".into()))?,
                history_turns: HISTORY_TURNS,
            },
            actors: ActorLimits::new(TURN_BACKSTOP_MS, 1 << 20, 1 << 16).map_err(storage)?,
            clock: Arc::new(SystemClock),
            approval_policy: self.preset.approval_policy.clone(),
        };
        let configure = |turns: LocalTurns| {
            turns
                .with_approvers(self.approval_models.clone())
                .with_goals(self.goal_continuations)
                .with_prompt(self.preset.prompt.as_deref())
        };
        let bound = if let Some(verifier) = &self.served {
            let caller = verifier.caller(
                &super::Custody {
                    issuer: &authority,
                    clock: &self.clock,
                },
                &self.signed,
            )?;
            Bound::admitted_host(
                authority,
                caller,
                Arc::clone(&self.files),
                within,
                turns,
                configure,
            )
        } else {
            Bound::local_host(authority, Arc::clone(&self.files), within, turns, configure)
        }
        .map_err(storage)?;
        let platform: Arc<dyn crate::hosting::Platform> = Arc::new(LocalUser(Arc::downgrade(self)));
        // A reopened root keeps the policy it was created with: the agent
        // model has no policy-update record, and replaying a creation with
        // another policy would refuse the reopen.
        let existing = AgentPath::root(&id);
        let configured = self.root_policy(&id).await?;
        let server = match bound.agents().state(&existing).await {
            Ok(state) => {
                if state.policy.as_ref() != Some(&configured) {
                    tracing::warn!(
                        "the configured approval policy differs from the one this workspace's \
                         root agent was created with; the agent keeps its original policy"
                    );
                }
                // This runtime runs the reopened root; reopening only restores
                // its placement.
                bound.agents().host(&existing).map_err(storage)?;
                crate::hosting::reopen(bound, platform, existing)
                    .await
                    .map_err(storage)?
            }
            Err(crate::agent::Error::NoSuchAgent) => {
                let bind = |name: &str, volume: &VolumeRef| VolumeBinding {
                    name: name.to_owned(),
                    volume: volume.clone(),
                };
                let request = RootRequest {
                    agent: id,
                    fresh: Vec::new(),
                    retained: vec![
                        bind(PRIVATE_VOLUME, &self.journal),
                        bind("project", &self.project),
                        bind(GOAL_VOLUME, &self.scratch),
                    ],
                    brief: None,
                    policy: Some(configured),
                };
                let intent = IntentId::try_from("root").map_err(storage)?;
                match crate::hosting::serve(bound, platform, &intent, request)
                    .await
                    .map_err(storage)?
                {
                    Serving::Served(server) => server,
                    Serving::Unknown(_) => {
                        return Err(Error::Storage(
                            "the root agent's creation is uncertain; reopen to settle it".into(),
                        ));
                    }
                }
            }
            Err(error) => return Err(storage(error)),
        };
        let (failures, _) = tokio::sync::broadcast::channel(64);
        let report = failures.clone();
        let runner = Arc::clone(&server);
        let run = tokio::spawn(async move {
            let error = runner
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
            server,
            failures,
            run,
        })
    }

    /// Failed agent deliveries from now on, once the agents are hosted.
    pub fn agent_failures(&self) -> Option<tokio::sync::broadcast::Receiver<AgentFailure>> {
        self.agents.get().map(|hosted| hosted.failures.subscribe())
    }
}

/// The operation keying `agent`'s one `SessionStart`.
#[cfg(any(test, feature = "hooks"))]
fn session_operation(agent: &crate::agent::AgentPath) -> OperationId {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.agent.session-start/v1");
    hasher.update(agent.as_str().as_bytes());
    let mut id = [0; 16];
    hasher.finalize_xof().fill(&mut id);
    OperationId::from_bytes(id)
}
