//! The local host of the agent model: every agent of this harness on one A4
//! runtime over the filesystem's volume-authority stream, so a spawn creates
//! its child's volumes in the spawn's own commit, and its turns on the stock
//! executor with this harness's model, tools, context and compaction.

use super::{LocalHarness, LocalStream, storage};
use crate::{
    Error, OperationId, Result,
    agent::{
        AgentHost, AgentPath, AgentState, IntentId, WorkspaceEntry, WorkspaceRole, WorkspaceTable,
    },
    conversation::{ContentAccess, ContentPublisher},
    executor::{Executor, StockExecutor},
    filesystem::{
        FilesystemTurnHost, HOME_VOLUME, PRIVATE_VOLUME, Tasks, TurnAuthority, TurnContext,
        TurnExecutors, TurnLimits,
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
const HISTORY_TURNS: u32 = 64;

/// The served agent tree: the same [`crate::hosting::Server`] a remote host
/// serves, and the runtime task that ends with it.
pub(super) struct Hosted {
    server: Arc<LocalServer>,
    pub(super) runtime: crate::hosting::Runtime,
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
}

/// The local user owns the whole tree: its policy admits every command its
/// own session credential carries, and its prompts attach the checkout files
/// they mention.
struct LocalUser {
    sessions: bool,
    harness: Weak<LocalHarness>,
    /// The root's retained diff projection, which its continuation pages
    /// read without projecting again.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    diffs: crate::hosting::DiffPages,
}

impl LocalUser {
    fn harness(&self) -> Result<Arc<LocalHarness>> {
        self.harness
            .upgrade()
            .ok_or_else(|| Error::Unsupported("local harness is closed".into()))
    }
}

impl crate::hosting::Platform for LocalUser {
    fn compaction_disabled(&self) -> bool {
        self.harness().is_ok_and(|local| {
            matches!(
                local.compaction.as_ref(),
                Some(crate::context::CompactionPolicy::Disabled)
            )
        })
    }

    fn sessions(&self) -> bool {
        self.sessions
    }

    fn session_source(&self) -> Option<AgentPath> {
        let local = self.harness().ok()?;
        Some(AgentPath::root(&crate::agent::AgentId::from_bytes(
            local.root.into_bytes(),
        )))
    }

    fn session_root(
        &self,
        agent: crate::agent::AgentId,
    ) -> BoxProviderFuture<'_, Result<crate::hosting::RootRequest>> {
        Box::pin(async move {
            let local = self.harness()?;
            let policy = local.root_policy(&agent).await?;
            Ok(crate::hosting::RootRequest {
                peer: true,
                agent,
                fresh: Vec::new(),
                retained: Vec::new(),
                brief: None,
                policy: Some(policy),
            })
        })
    }

    #[cfg(any(test, not(target_arch = "wasm32")))]
    fn merge_session<'a>(
        &'a self,
        agent: &'a AgentPath,
        operation: OperationId,
    ) -> BoxProviderFuture<'a, Result<Option<String>>> {
        Box::pin(async move { self.harness()?.merge_session(agent, operation).await })
    }

    fn policy<'a>(
        &'a self,
        _: &'a Principal,
        _: &'a AgentCommand,
    ) -> BoxProviderFuture<'a, std::result::Result<PolicyDecision, StreamError>> {
        Box::pin(async { Ok(PolicyDecision::Allow) })
    }

    fn prompt_command<'a>(
        &'a self,
        text: &'a str,
    ) -> BoxProviderFuture<'a, Result<Option<String>>> {
        Box::pin(async move { self.harness()?.resolve_prompt_command(text).await })
    }

    fn attachments<'a>(
        &'a self,
        key: &'a [u8],
        text: Option<&'a str>,
    ) -> BoxProviderFuture<'a, Result<Option<crate::conversation::FileRef>>> {
        Box::pin(async move { self.harness()?.prompt_attachments(key, text).await })
    }

    /// The root's checkout diff, read under the tree owner's own signed
    /// authority, the one every client of this tree is minted under: the
    /// server rechecked this session's credential and policy for this page,
    /// and the shared producer checks the owner's file authority before
    /// disclosing any path or content, retained continuation pages included.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    fn diff<'a>(
        &'a self,
        _: &'a acyclic_stream::session::CallAuthorization,
        agent: &'a AgentPath,
        page: &'a acyclic_harness_client::AgentPageRequest,
    ) -> BoxProviderFuture<'a, Result<Vec<crate::context::DiffFilePage>>> {
        Box::pin(async move {
            let local = self.harness()?;
            if local.agents.get().map(Hosted::root) == Some(agent) {
                return local.project_diff(&self.diffs, page).await;
            }
            let state = local
                .agents()
                .await?
                .0
                .state(agent)
                .await
                .map_err(storage)?;
            let project = WorkspaceTable::primary_in(&state.volumes)
                .filter(|binding| binding.role == WorkspaceRole::User)
                .ok_or_else(|| Error::Invalid("selected agent has no project binding".into()))?;
            let operation = crate::filesystem::spawned_operation(agent, project.id);
            let base = local
                .files
                .creation_generation(project.volume()?, operation)
                .await?
                .ok_or_else(|| {
                    Error::Unsupported("selected project has no fork creation receipt".into())
                })?;
            let reference = crate::filesystem::workspace_ref(
                project.volume()?.provider().clone(),
                &project.volume()?.storage_name(&mut None)?,
            )?;
            let current = local.files.resolve(&reference, None).await?.generation;
            let reader = local
                .agents
                .get()
                .ok_or_else(|| Error::Unsupported("local agents are not hosted".into()))?
                .server
                .bound()
                .content_reader(agent)
                .await
                .map_err(storage)?;
            self.diffs
                .page(&*reader, project.volume()?, (&base, &current), page)
                .await
        })
    }

    /// The local user is the tree's one human: it answers as the default
    /// human approver, the one a tool hook's escalation asks.
    fn approver(&self, _: &Principal) -> String {
        crate::approvals::HUMAN_APPROVER.to_owned()
    }
}

/// What the local harness adds to each stock turn: its context manifest,
/// the turn's task context and tool authority, and its hooks.
struct LocalContext(Weak<LocalHarness>);

impl TurnContext for LocalContext {
    fn settle<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
    ) -> BoxProviderFuture<'a, Result<()>> {
        Box::pin(async move {
            let local = self
                .0
                .upgrade()
                .ok_or_else(|| Error::Unsupported("local harness is closed".into()))?;
            #[cfg(any(test, not(target_arch = "wasm32")))]
            local.merge_completed_workspace(agent, turn).await?;
            #[cfg(not(any(test, not(target_arch = "wasm32"))))]
            let _ = (local, agent, turn);
            Ok(())
        })
    }

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
            let bindings = local
                .turn_bindings(
                    agent,
                    &authority.issuer,
                    &authority.scope,
                    &authority.content,
                )
                .await?;
            let epoch = local
                .context_epoch(agent, &bindings.context, &**writer)?
                .await?;
            let executor = executor
                .with_context_epoch(&epoch, authority.content.reader.clone())
                .await?;
            // Before the turn's first request: the step-0 boundary appends
            // what changed since the epoch froze.
            let through = local
                .announce_changes(
                    agent,
                    &epoch,
                    &bindings.context,
                    &**writer,
                    authority.content.reader.as_ref(),
                )
                .await?;
            if let Some(through) = through {
                authority
                    .journal
                    .model_boundary()
                    .ok_or_else(|| Error::Invalid("agent turn has no model boundary".into()))?
                    .include_prepared_sources(turn, through)?;
            }
            #[cfg(any(test, not(target_arch = "wasm32")))]
            let hooks = local.turn_hooks(turn, &**writer).await?;
            let (executor, context) =
                Tasks::bind(&bindings.harness, executor, agent, turn, authority)?;
            let executor = executor.with_live_reply(crate::executor::LiveReply::new(
                Arc::clone(writer),
                format!("{}/{turn}", crate::filesystem::TOOL_OUTPUT),
            ));
            // SessionStart runs once per agent, keyed by the agent itself, so
            // a forked child (which inherits its parent's tail) runs its own.
            #[cfg(any(test, not(target_arch = "wasm32")))]
            let executor = executor
                .with_agent_hooks(
                    &context,
                    turn,
                    input,
                    session_operation(&agent.path),
                    hooks.as_ref(),
                )
                .await?;
            #[cfg(not(any(test, not(target_arch = "wasm32"))))]
            let _ = (context, input);
            Ok(executor)
        })
    }

    /// Every agent of the tree runs on its one home target, the checkout
    /// (a child on its fork view of it).
    fn roster(&self, _: &AgentState) -> Option<Arc<crate::agent::Roster>> {
        let local = self.0.upgrade()?;
        local.harness.targets().map(|targets| targets.roster())
    }

    fn workspaces<'a>(
        &'a self,
        agent: &'a AgentState,
        seen: &'a std::collections::BTreeMap<crate::agent::EntryId, [u8; 32]>,
        content: &'a crate::runtime::ContentBindings,
        issuer: &'a crate::core::RuntimeAuthority,
        scope: &'a crate::core::Scope,
    ) -> BoxProviderFuture<'a, Result<Option<crate::agent::WorkspaceUpdate>>> {
        Box::pin(async move {
            let local = self
                .0
                .upgrade()
                .ok_or_else(|| Error::Unsupported("local harness is closed".into()))?;
            // Use the same admitted placement as turn preparation, including
            // the selected child's own project and auxiliary projections.
            let bindings = local.turn_bindings(agent, issuer, scope, content).await?;
            let roster = bindings.harness.targets().map(|targets| targets.roster());
            let exposures = agent
                .volumes
                .iter()
                .filter(|entry| entry.role != WorkspaceRole::Internal)
                .map(|entry| {
                    #[cfg(any(test, not(target_arch = "wasm32")))]
                    let path = bindings
                        .context
                        .workspace
                        .as_ref()
                        .and_then(|workspace| {
                            if !entry.root.is_empty()
                                || entry.mode != crate::agent::WorkspaceMode::ReadWrite
                            {
                                return None;
                            }
                            let volume = entry.as_volume()?;
                            let (relative, mount) = workspace
                                .mounts
                                .mounts()
                                .find(|(_, mount)| &mount.view.volume == volume)?;
                            match &mount.native_root {
                                crate::workspace::NativeRoot::Logical => None,
                                crate::workspace::NativeRoot::Composed => {
                                    Some(workspace.mounts.root.join(relative))
                                }
                                crate::workspace::NativeRoot::Path(path) => Some(path.clone()),
                            }
                        })
                        .map(|path| path.to_string_lossy().into_owned());
                    #[cfg(not(any(test, not(target_arch = "wasm32"))))]
                    let path: Option<String> = None;
                    crate::agent::WorkspaceExposure {
                        entry: entry.id,
                        paths: roster
                            .iter()
                            .flat_map(|roster| roster.targets())
                            .map(|target| {
                                (
                                    target.name.clone(),
                                    if target.name == HOME_TARGET {
                                        path.clone()
                                    } else {
                                        None
                                    },
                                )
                            })
                            .collect(),
                    }
                })
                .collect();
            let limits = crate::context::ContextDiscoveryLimits::default();
            let limits = crate::context::ContextDiscoveryLimits {
                instruction_bytes: limits
                    .instruction_bytes
                    .into_iter()
                    .chain(local.limits.file_bytes)
                    .min(),
                ..limits
            };
            crate::agent::WorkspaceUpdate::discover(
                agent.volumes.clone(),
                roster.as_deref(),
                exposures,
                seen,
                content.reader.as_ref(),
                &Default::default(),
                &limits,
            )
            .await
            .map(Some)
        })
    }
}

struct TurnBindings<'a> {
    harness: Arc<crate::runtime::AgentHarness>,
    context: super::context::Binding<'a>,
}

impl LocalHarness {
    /// Publish a child's fork before its terminal turn becomes observable.
    /// The shared workspace command engine owns capture, merge, conflict
    /// projection and physical/native reconciliation under the parent's lock.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    async fn merge_completed_workspace(&self, agent: &AgentState, turn: OperationId) -> Result<()> {
        let (Some(parent), Some(root)) = (&agent.parent, &self.workspace) else {
            return Ok(());
        };
        let hosted = self
            .agents
            .get()
            .ok_or_else(|| Error::Unsupported("agent hosting is closed".into()))?;
        let parent = hosted.agents().state(parent).await.map_err(storage)?;
        let project = |state: &AgentState| {
            WorkspaceTable::primary_in(&state.volumes)
                .filter(|binding| binding.role == WorkspaceRole::User)
                .and_then(|binding| binding.as_volume())
                .cloned()
                .ok_or_else(|| Error::Invalid("agent has no project volume".into()))
        };
        let child_project = project(agent)?;
        let parent_project = project(&parent)?;
        if child_project == parent_project {
            return Ok(());
        }
        let issuer = self
            .root_authority
            .as_ref()
            .ok_or_else(|| Error::Unsupported("agent signing custody is closed".into()))?;
        let authority = crate::filesystem::agent_authority(
            &issuer.clone().into(),
            &crate::filesystem::TurnAdmission::LocalRoot,
            &parent.path,
            &parent.volumes,
            &[],
        )
        .await?;
        // The base checkout belongs to the task; a bound child checkout
        // belongs to its conversation. Keep each workspace's original
        // audience and the parent's exact volume grants and identity.
        let (signed, verifier) = if parent_project == self.project {
            (
                issuer.bootstrap_scope(
                    "child-completion",
                    &authority.grants,
                    Some(crate::AgentId::from_bytes(parent.path.id_bytes())),
                    &mut None,
                )?,
                issuer.verifier(&mut None)?,
            )
        } else {
            (authority.scope, authority.verifier)
        };
        let owner = super::workspace::owner::AgentOwner(parent.path.clone());
        let parent_workspace = self
            .project_workspace(
                root,
                &parent.path,
                &parent_project,
                &parent.volumes,
                &signed,
                &verifier,
            )
            .await?;
        let child = root.spawned_project_view(&child_project).await?;
        let branch = format!("agents/{}", agent.path.id());
        acyclic_fs::GitCompatRepository::new(
            parent_workspace.view().workspace().id(),
            parent_workspace.store.clone(),
        )
        .register_branch_workspace(branch.clone(), child.workspace().id(), None, false)
        .await
        .map_err(storage)?;
        let (capture_options, capture_policy) =
            super::checkout_tools::capture::configuration(&parent_workspace)?;
        let commands = super::workspace::git::Commands {
            workspace: &parent_workspace,
            signed: &signed,
            verifier: &verifier,
            clock: &*self.clock,
            capture_options: &capture_options,
            capture_policy: &capture_policy,
        };
        let scope = crate::runtime::RuntimeScope::new(authority.grants, Default::default())?;
        let source = child
            .workspace()
            .head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map_err(storage)?
            .value;
        let mut state = crate::sync::owned::lock_owned(super::workspace::StateArc::clone(
            &parent_workspace.state,
        ))
        .await;
        parent_workspace
            .defer_publication(
                turn,
                branch,
                acyclic_fs::GitGenerationRef {
                    workspace_id: child.workspace().id(),
                    generation: source.id(),
                },
            )
            .await?;
        commands
            .drain_publications(
                &super::workspace::git::CommandContext {
                    owner: &owner,
                    operation: turn,
                    scope: &scope,
                    request: None,
                },
                &mut state,
            )
            .await
    }
    /// Bind a committed project using the same adapter for turns and human
    /// filesystem actions. Only an action needing native access binds a view.
    #[cfg(any(test, not(target_arch = "wasm32")))]
    pub(super) async fn project_workspace(
        &self,
        workspace: &Arc<super::workspace::LocalWorkspace>,
        agent: &AgentPath,
        project: &crate::conversation::VolumeRef,
        bindings: &[WorkspaceEntry],
        scope: &crate::core::Scope,
        verifier: &crate::core::AuthorityVerifier,
    ) -> Result<Arc<super::workspace::LocalWorkspace>> {
        if project == &self.project
            && self.session_bindings().iter().all(|expected| {
                bindings.iter().any(|binding| {
                    binding.name == expected.name && binding.source == expected.source
                })
            })
        {
            return Ok(Arc::clone(workspace));
        }
        let view = workspace.spawned_project_view(project).await?;
        let owner = super::workspace::owner::AgentOwner(agent.clone());
        let authority = crate::filesystem::NativeMountAuthority::admitted(
            verifier.clone(),
            scope.copy(&mut None)?,
        );
        let workspace = workspace
            .bound_task_view(view, project, bindings, &owner, &authority)
            .await?;
        Ok(workspace)
    }

    /// Retained session volumes are the same for model and native-host roots;
    /// each root binds its own private journal separately.
    pub(super) fn session_bindings(&self) -> [WorkspaceEntry; 3] {
        [
            (crate::agent::WorkspaceRole::User, "project", &self.project),
            (
                crate::agent::WorkspaceRole::Home,
                HOME_VOLUME,
                &self.scratch,
            ),
            (
                crate::agent::WorkspaceRole::Shared,
                super::SHARED_VOLUME,
                &self.shared,
            ),
        ]
        .map(|(role, name, volume)| {
            WorkspaceEntry::initial_volume(role, name.to_owned(), volume.clone())
        })
    }

    /// An agent's default files and commands select the same project. The
    /// root reuses its admitted binding; a child opens its retained fork.
    // Keep turn preparation independent of the child workspace's future layout.
    fn turn_bindings<'a>(
        &'a self,
        agent: &'a AgentState,
        issuer: &'a crate::core::RuntimeAuthority,
        scope: &'a crate::core::Scope,
        content: &'a crate::runtime::ContentBindings,
    ) -> BoxProviderFuture<'a, Result<TurnBindings<'a>>> {
        Box::pin(async move {
            let project = WorkspaceTable::primary_in(&agent.volumes)
                .and_then(|binding| binding.as_volume())
                .ok_or_else(|| Error::Invalid("agent has no project volume".into()))?;
            let scratch = agent
                .volumes
                .iter()
                .find(|binding| binding.role == WorkspaceRole::Home)
                .and_then(|binding| binding.as_volume())
                .ok_or_else(|| Error::Invalid("agent has no home volume".into()))?;
            let verifier = issuer.verifier(&mut None)?;
            #[cfg(any(test, not(target_arch = "wasm32")))]
            let journal = agent
                .volumes
                .iter()
                .find(|binding| binding.role == WorkspaceRole::Private)
                .and_then(|binding| binding.as_volume())
                .ok_or_else(|| Error::Invalid("agent has no private volume".into()))?;
            let context = super::context::Binding {
                project,
                scratch,
                #[cfg(any(test, not(target_arch = "wasm32")))]
                journal,
                reader: content.reader.as_ref(),
                #[cfg(any(test, not(target_arch = "wasm32")))]
                verifier: verifier.clone(),
                #[cfg(any(test, not(target_arch = "wasm32")))]
                scope,
                #[cfg(any(test, not(target_arch = "wasm32")))]
                workspace: self.workspace.clone(),
            };
            if project == &self.project
                && scratch == &self.scratch
                && agent.volumes.iter().any(|binding| {
                    binding.role == WorkspaceRole::Shared
                        && binding.as_volume() == Some(&self.shared)
                })
            {
                return Ok(TurnBindings {
                    harness: Arc::clone(&self.harness),
                    context,
                });
            }
            #[cfg(any(test, not(target_arch = "wasm32")))]
            if super::open::uses_native_execution(&self.tools)?
                && let Some(workspace) = &self.workspace
            {
                let workspace = self
                    .project_workspace(
                        workspace,
                        &agent.path,
                        project,
                        &agent.volumes,
                        scope,
                        &verifier,
                    )
                    .await?;
                let (tools, writer) = workspace
                    .executor(
                        (project, &self.files, self.limits.file_bytes),
                        Arc::clone(&self.clock),
                        scope,
                        &verifier,
                        None,
                    )
                    .await?;
                let context = super::context::Binding {
                    workspace: Some(Arc::clone(&workspace)),
                    ..context
                };
                return Ok(TurnBindings {
                    context,
                    harness: self.harness.with_workspace(
                        Some(home(tools)?),
                        crate::runtime::ContentBindings {
                            volume: project.clone(),
                            reader: Arc::clone(&content.reader),
                            writer: Some(writer),
                        },
                    )?,
                });
            }
            #[cfg(any(test, not(target_arch = "wasm32")))]
            let context = if let Some(workspace) = &self.workspace {
                let view = workspace.spawned_project_view(project).await?;
                let authority = crate::filesystem::NativeMountAuthority::admitted(
                    verifier.clone(),
                    scope.copy(&mut None)?,
                );
                super::context::Binding {
                    workspace: Some(workspace.context_task_view(view, project, &authority)?),
                    ..context
                }
            } else {
                context
            };
            let writer = crate::filesystem::FilesystemContentPublisher::new(
                Arc::clone(&self.files),
                project,
                &verifier,
                scope,
                self.limits.file_bytes,
                None,
            )?;
            Ok(TurnBindings {
                context,
                harness: self.harness.with_workspace(
                    None,
                    crate::runtime::ContentBindings {
                        volume: project.clone(),
                        reader: Arc::clone(&content.reader),
                        writer: Some(writer),
                    },
                )?,
            })
        })
    }
}

/// The tree's one execution target: this host's checkout, run by `tools`
/// and guarded by its one human. Every agent's fork view runs on it.
pub(super) fn home(
    tools: Arc<dyn crate::tool::ToolExecutor>,
) -> Result<Arc<dyn crate::runtime::TargetResolver>> {
    let record = crate::runtime::InProcessTargets::local_record(
        HOME_TARGET,
        crate::approvals::HUMAN_APPROVER,
    );
    Ok(Arc::new(crate::runtime::InProcessTargets::home(
        record, tools,
    )?))
}

/// The name of a local tree's home target.
const HOME_TARGET: &str = "local";

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
            Some(model) => {
                let executors = crate::filesystem::StockExecutors::new(
                    model.clone(),
                    self.tools().clone(),
                    (self.limits, self.compaction.clone()),
                    Arc::new(LocalContext(Arc::downgrade(self))),
                )
                .with_agent_tools(&self.agent_tools)
                .with_question_plugin(self.question_plugin)
                .with_templates();
                let executors = match &self.models {
                    Some(models) => executors.with_models(models.clone()),
                    None => executors,
                };
                Arc::new(match self.sampling {
                    Some(sampling) => executors.with_sampling(sampling),
                    None => executors,
                })
            }
            None => Arc::new(NativeHost),
        }
    }
}

impl LocalHarness {
    /// Guest sources use the same public enrollment producer as hosted trees.
    #[cfg(all(target_os = "linux", not(target_arch = "wasm32")))]
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
                    history_turns: HISTORY_TURNS,
                },
                actors: ActorLimits::default(),
                clock: Arc::new(SystemClock),
                approval_policy: self.preset.approval_policy.clone(),
            },
        )
        .map_err(storage)?;
        bound
            .guest_session(
                enrollment.volume,
                enrollment.posix,
                enrollment.writable,
                None,
                work,
            )
            .await
    }

    /// Every agent of this harness on one A4 runtime, and its root agent.
    ///
    /// The first call creates the root agent (replayed exactly on reopen) and
    /// starts the runtime; it stops when this harness drops. Controls are
    /// [`AgentHost`] commits; observation is the agents' status and
    /// conversation streams.
    /// A retained root must already bind this session's shared volume. A
    /// session created without that admission is refused before hosting;
    /// start a new session rather than changing its saved authority.
    pub async fn agents(self: &Arc<Self>) -> Result<(&Arc<LocalAgents>, &AgentPath)> {
        let hosted = self
            .agents
            .get_or_try_init(|| async { self.host_agents().await })
            .await?;
        Ok((hosted.agents(), hosted.root()))
    }

    pub(super) fn require_shared_binding(&self, state: &AgentState) -> Result<()> {
        if state.volumes.iter().any(|binding| {
            binding.role == WorkspaceRole::Shared && binding.as_volume() == Some(&self.shared)
        }) {
            return Ok(());
        }
        Err(Error::Unsupported(
            "session has no admitted shared volume; start a new session to use this harness".into(),
        ))
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

    // Hosting is initialized once by the original cell. Erase its future here
    // so every connection holds a pointer instead of the entire startup chain.
    fn host_agents(self: &Arc<Self>) -> BoxProviderFuture<'_, Result<Hosted>> {
        Box::pin(self.initialize_agents())
    }

    async fn initialize_agents(self: &Arc<Self>) -> Result<Hosted> {
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
                history_turns: HISTORY_TURNS,
            },
            actors: ActorLimits::default(),
            clock: Arc::new(SystemClock),
            approval_policy: self.preset.approval_policy.clone(),
        };
        let configure = |turns: LocalTurns| {
            let turns = match &self.topology {
                Some(topology) => turns.with_topology(topology.clone()),
                None => turns,
            };
            turns
                .with_approvers(self.approval_models.clone())
                .with_goals(self.goal_continuations)
        };
        // A checkout's spawned children fork its retained lazy cut, so each
        // child's project is a related fork the parent can diff and merge.
        // Only native execution has checkouts; a configured volume has none.
        #[cfg(any(test, not(target_arch = "wasm32")))]
        let cuts = self.workspace().map(|workspace| {
            Arc::new(crate::local::workspace::LocalVolumeResources(Arc::clone(
                workspace,
            ))) as Arc<dyn crate::filesystem::VolumeResources>
        });
        #[cfg(not(any(test, not(target_arch = "wasm32"))))]
        let cuts: Option<Arc<dyn crate::filesystem::VolumeResources>> = None;
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
                cuts,
                configure,
            )
        } else {
            Bound::local_host(
                authority,
                Arc::clone(&self.files),
                within,
                turns,
                cuts,
                configure,
            )
        }
        .map_err(storage)?;
        let platform: Arc<dyn crate::hosting::Platform> = Arc::new(LocalUser {
            sessions: bound.admit_sessions().is_ok(),
            harness: Arc::downgrade(self),
            #[cfg(any(test, not(target_arch = "wasm32")))]
            diffs: crate::hosting::DiffPages::default(),
        });
        // A reopened root keeps the policy it was created with: the agent
        // model has no policy-update record, and replaying a creation with
        // another policy would refuse the reopen.
        let existing = AgentPath::root(&id);
        let configured = self.root_policy(&id).await?;
        let server = match bound.agents().state(&existing).await {
            Ok(state) => {
                if state
                    .profile
                    .as_ref()
                    .and_then(|profile| profile.topology.as_ref())
                    != self.topology.as_ref()
                {
                    return Err(Error::Invalid("this session has another topology; start with a fresh --host-state-directory".into()));
                }
                self.require_shared_binding(&state)?;
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
                let request = RootRequest {
                    peer: true,
                    agent: id,
                    fresh: Vec::new(),
                    retained: std::iter::once(WorkspaceEntry::initial_volume(
                        crate::agent::WorkspaceRole::Private,
                        PRIVATE_VOLUME.to_owned(),
                        self.journal.clone(),
                    ))
                    .chain(self.session_bindings())
                    .collect(),
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
        let runtime = crate::hosting::Runtime::start(Arc::clone(&server));
        Ok(Hosted { server, runtime })
    }
}

/// The operation keying `agent`'s one `SessionStart`.
#[cfg(any(test, not(target_arch = "wasm32")))]
fn session_operation(agent: &crate::agent::AgentPath) -> OperationId {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.agent.session-start/v1");
    hasher.update(agent.as_str().as_bytes());
    let mut id = [0; 16];
    hasher.finalize_xof().fill(&mut id);
    OperationId::from_bytes(id)
}
