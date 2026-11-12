//! Agent turns on the filesystem: each agent's journal is a conversation-scoped
//! authority over the agent's journal volume; its executor comes from the host's
//! [`TurnExecutors`] for that agent and authority, per turn.
//!
//! The model context of a turn is the shared instruction, then the agent's
//! own conversation stream from its latest checkpoint: that checkpoint's
//! summary, the turns settled after it as user/assistant messages, then the
//! turn's input. The host binds exactly that selection into the turn's journal,
//! so the executor can dispatch no other history.

use super::{
    FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
    FilesystemHost, FilesystemJournalBinding,
    agent_effects::FilesystemTurnEffects,
    agent_goal::FilesystemTurnGoal,
    agent_internal::{AgentInternalFiles, AgentTurnOutputs},
};
use crate::approvals::{AgentApprovalPolicy, AgentApprovals, ApprovalPolicy};
use crate::{
    Capabilities, Error, OperationId, Result,
    agent::{
        AgentPath, AgentRecord, AgentState, ConversationEntry, OpenTurn, TurnHost, WorkspaceEntry,
    },
    conversation::{
        ContentResidencyVerifier, FileRef, ModelContextSelection, VolumeClass, VolumeOperation,
        VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityVerifier, RuntimeAuthority, Scope},
    executor::{Executor, TurnOutput},
    model::{ModelContent, ModelMessage, ModelRole},
    projection::SelectedModelContext,
    runtime::ContentBindings,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{
    BoxProviderFuture as BoxFuture, ReadRequest, StreamProvider, StreamProviderExt as _,
};
use futures::TryStreamExt;
use std::sync::Arc;

pub use crate::conversation::{PRIVATE_VOLUME, private_volume};

#[cfg(test)]
#[path = "agent_turns/admission_tests.rs"]
mod admission_tests;

/// The authority of one agent turn: the agent's conversation issuer, its scope
/// over every bound volume, and the agent's content bound to that scope.
pub struct TurnAuthority {
    /// Original template admission shared with the hosting entrance.
    pub templates: Option<Arc<dyn crate::agent::SpawnTemplates>>,
    /// Signing custody; it must outlive the turn's journal.
    pub issuer: RuntimeAuthority,
    /// Read and write over the agent's bound volumes.
    pub scope: Scope,
    /// The capabilities `scope` grants, for the executor's tool authority.
    pub grants: Capabilities,
    /// Reader of files under `scope` and writer of the private volume, for
    /// model content, tool inputs and the turn's own output.
    pub content: ContentBindings,
    /// Approval gate of the agent's resolved policy (see
    /// [`FilesystemTurnHost::new`]): every turn has one.
    pub approval: Arc<dyn crate::runtime::ToolPolicy>,
    /// Where the turn's agent tools record their effects.
    pub effects: Option<Arc<dyn crate::agent::TurnEffects>>,
    /// The same finalized journal returned in `OpenTurn`, retained by tools.
    pub journal: Arc<dyn crate::executor::ExecutionJournal>,
    /// What the turn's `wait` observes of the agent's children.
    pub children: Option<Arc<dyn crate::agent::TurnChildren>>,
    /// Commands bound to the original filesystem placement, if supplied.
    pub workspace_tools: Option<Arc<dyn crate::runtime::FilesystemExecutor>>,
}

/// The conversation authority of one agent over its bound volumes.
pub struct AgentAuthority<'v> {
    /// `issuer`'s custody under the agent's conversation audience.
    pub issuer: RuntimeAuthority,
    /// Read and write over every bound volume.
    pub scope: Scope,
    /// The capabilities `scope` grants.
    pub grants: Capabilities,
    /// Verifier of grants under `scope`.
    pub verifier: AuthorityVerifier,
    /// The agent's own private volume binding.
    pub private: &'v WorkspaceEntry,
}

/// What every agent scope of a host derives from.
#[derive(Clone, Debug)]
pub enum TurnAdmission {
    /// The trusted local runtime root: each agent's scope is a root grant over
    /// its bound volumes, under the agent's conversation audience.
    LocalRoot,
    /// A workload scope admitted under `audience`. Each agent's scope is
    /// delegated from it, keeping its audience, agent and lifetime, and grants
    /// only what it admits: a read-only admission cannot write. A delegated host
    /// requires journal read/write and the signed `role:harness-host` claim;
    /// guest scopes always remove that claim.
    Admitted {
        /// The audience the admitted scope was issued for.
        audience: Authority,
        /// The admitted scope.
        scope: Scope,
    },
}

impl TurnAdmission {
    /// Checks identity, audience, lifetime and attenuation before storage IO.
    /// File authority is checked separately against the committed bindings.
    pub(super) fn verify(
        &self,
        issuer: &RuntimeAuthority,
        path: &AgentPath,
    ) -> Result<RuntimeAuthority> {
        match self {
            Self::LocalRoot => issuer.with_audience(Authority {
                kind: AggregateKind::Conversation,
                id: path.id().into(),
            }),
            Self::Admitted { audience, scope } => {
                if scope
                    .agent()
                    .is_some_and(|agent| agent.into_bytes() != path.id_bytes())
                {
                    return Err(Error::Unauthorized(
                        "admitted scope belongs to another agent".into(),
                    ));
                }
                let issuer = issuer.with_audience(audience.clone())?;
                issuer.verifier(&mut None)?.verify(scope, &mut None)?;
                Ok(issuer)
            }
        }
    }
}

/// An agent's admission, verified before any of its IO: its private volume
/// is its own, and under [`TurnAdmission::Admitted`] the admitted scope is
/// authentic, live, bound to this audience and agent, and grants the private
/// volume's write. Attestation is deferred until execution needs a signed scope. Every
/// read of a turn and its [`AgentAuthority`] require one.
pub(super) struct AdmittedAgent<'v> {
    issuer: RuntimeAuthority,
    owner: crate::AgentId,
    volumes: &'v [WorkspaceEntry],
    private: &'v WorkspaceEntry,
    journal: VolumeRef,
    /// Original immutable admission; attestation occurs only when needed.
    parent: Option<&'v Scope>,
}

impl<'v> AdmittedAgent<'v> {
    /// Verifies the admission of the agent at `path` over `volumes`, signed
    /// with `issuer`'s custody. It performs no IO.
    pub(super) fn verify(
        issuer: &RuntimeAuthority,
        admission: &'v TurnAdmission,
        path: &AgentPath,
        volumes: &'v [WorkspaceEntry],
    ) -> Result<Self> {
        let owner = crate::AgentId::from_bytes(path.id_bytes());
        let private = volumes
            .iter()
            .find(|binding| binding.role == crate::agent::WorkspaceRole::Private)
            .ok_or_else(|| Error::Invalid("agent has no private volume".into()))?;
        if private.volume()?.class() != VolumeClass::AgentPrivate
            || *private.volume()?.owner() != VolumeOwner::Agent(owner)
        {
            return Err(Error::Unauthorized(
                "private volume belongs to another agent".into(),
            ));
        }
        let journal =
            crate::conversation::journal_volume(private.volume()?.provider().clone(), &path.id())?;
        if volumes
            .iter()
            .any(|binding| binding.as_volume() == Some(&journal))
        {
            return Err(Error::Unauthorized(
                "the agent journal is not a workspace".into(),
            ));
        }
        let issuer = admission.verify(issuer, path)?;
        let parent = match admission {
            TurnAdmission::LocalRoot => None,
            TurnAdmission::Admitted { scope, .. } => Some(scope),
        };
        if let Some(scope) = parent {
            let verifier = issuer.verifier(&mut None)?;
            crate::conversation::ContentGrant::authorize_agent_volume(
                &verifier,
                scope,
                owner,
                private.volume()?,
                VolumeOperation::Write,
                &mut None,
            )?;
            require_structured_access(
                scope,
                owner,
                private.volume()?,
                &[
                    acyclic_fs::access::Action::Create,
                    acyclic_fs::access::Action::Write,
                ],
            )?;
        }
        if issuer.native().is_none() {
            let scope = parent
                .or_else(|| issuer.caller().map(|(_, scope)| scope))
                .ok_or_else(|| Error::Unauthorized("admission has no host custody".into()))?;
            if !scope
                .capabilities()
                .contains(crate::contract::capability::HARNESS_HOST)
            {
                return Err(Error::Unauthorized(
                    "admission has no host journal custody".into(),
                ));
            }
            for operation in [VolumeOperation::Read, VolumeOperation::Write] {
                crate::conversation::ContentGrant::authorize_agent_volume(
                    &issuer.verifier(&mut None)?,
                    scope,
                    owner,
                    &journal,
                    operation,
                    &mut None,
                )?;
            }
            require_structured_access(
                scope,
                owner,
                &journal,
                &[
                    acyclic_fs::access::Action::Read,
                    acyclic_fs::access::Action::Create,
                    acyclic_fs::access::Action::Write,
                ],
            )?;
        }
        Ok(Self {
            issuer,
            owner,
            volumes,
            private,
            journal,
            parent,
        })
    }

    /// The agent's authority over its bound volumes. Under a local root, each
    /// `external` file on a volume the agent does not bind (a message or
    /// result from another agent) is granted alone, by its exact version; an
    /// admitted agent reads what its admission grants.
    pub(super) async fn authority(self, external: &[FileRef]) -> Result<AgentAuthority<'v>> {
        let (grants, scope) = match self.parent {
            Some(parent) => {
                let grants = Capabilities::new(
                    parent
                        .capabilities()
                        .iter()
                        .filter(|label| *label != crate::contract::capability::HARNESS_HOST)
                        .map(str::to_owned),
                );
                let scope = self
                    .issuer
                    .issue(Some(parent), "agent", &grants, Some(self.owner))
                    .await?;
                (scope.capabilities().clone(), scope)
            }
            None => {
                let grants = Capabilities::new(root_grants(self.volumes, external)?);
                let scope = self
                    .issuer
                    .issue(None, "agent", &grants, Some(self.owner))
                    .await?;
                (grants, scope)
            }
        };
        let verifier = self.issuer.verifier(&mut None)?;
        Ok(AgentAuthority {
            issuer: self.issuer,
            scope,
            grants,
            verifier,
            private: self.private,
        })
    }

    /// Host authority over this agent's journal and retained conversation files.
    async fn journal_authority<'a>(
        &self,
        retained: impl IntoIterator<Item = &'a FileRef>,
    ) -> Result<(VolumeRef, AgentAuthority<'v>)> {
        let content = Capabilities::new(
            retained
                .into_iter()
                .filter(|file| file.volume() != &self.journal)
                .map(|file| file.read_capability(&mut None))
                .collect::<Result<Vec<_>>>()?,
        );
        let scope = self.control_scope(&content).await?;
        Ok((
            self.journal.clone(),
            AgentAuthority {
                issuer: self.issuer.clone(),
                verifier: self.issuer.verifier(&mut None)?,
                grants: scope.capabilities().clone(),
                scope,
                private: self.private,
            },
        ))
    }

    async fn control_scope(&self, content: &Capabilities) -> Result<Scope> {
        self.authorize_host_volume(&self.journal)?;
        let parent = if self.issuer.native().is_some() {
            None
        } else {
            self.parent
                .or_else(|| self.issuer.caller().map(|(_, scope)| scope))
        };
        if let Some(parent) = parent
            && parent.agent() == Some(self.owner)
        {
            return Ok(parent.clone());
        }
        let grants = match parent {
            Some(parent) => parent.capabilities().clone(),
            None => journal_grants(&self.journal, content)?,
        };
        self.issuer
            .issue(parent, "host-journal", &grants, Some(self.owner))
            .await
    }

    /// Pure admission for a proposed volume. Local enrollment already owns
    /// its root; an admitted caller must grant the exact persisted owner.
    pub(super) fn authorize_volume(&self, volume: &crate::conversation::VolumeRef) -> Result<()> {
        if let Some(scope) = self.parent {
            crate::conversation::ContentGrant::authorize_agent_volume(
                &self.issuer.verifier(&mut None)?,
                scope,
                self.owner,
                volume,
                VolumeOperation::Write,
                &mut None,
            )?;
            require_structured_access(
                scope,
                self.owner,
                volume,
                &[
                    acyclic_fs::access::Action::Create,
                    acyclic_fs::access::Action::Write,
                ],
            )?;
        } else {
            volume.require_writer(Some(self.owner), &None)?;
        }
        Ok(())
    }

    /// Host-owned storage is outside the workload's tool grant. Native custody
    /// checks the persisted owner; a delegated host retains its original ceiling.
    pub(super) fn authorize_host_volume(&self, volume: &VolumeRef) -> Result<()> {
        if self.issuer.native().is_some() {
            volume.require_writer(Some(self.owner), &None)
        } else {
            self.authorize_volume(volume)
        }
    }
}

/// One host journal grant set shared by local, portable and memory admission.
pub(super) fn journal_grants(journal: &VolumeRef, content: &Capabilities) -> Result<Capabilities> {
    Ok(Capabilities::new(content.iter().map(str::to_owned).chain(
        [
            crate::contract::capability::HARNESS_HOST.to_owned(),
            journal.capability(VolumeOperation::Read, &mut None)?,
            journal.capability(VolumeOperation::Write, &mut None)?,
        ],
    )))
}

/// The filesystem's decision over `scope`'s structured grant, if it has one:
/// it creates and writes every path of `volume`. It performs no IO; the
/// writer still applies the volume's persisted policy.
pub(super) fn require_structured_access(
    scope: &Scope,
    owner: crate::AgentId,
    volume: &crate::conversation::VolumeRef,
    actions: &[acyclic_fs::access::Action],
) -> Result<()> {
    use acyclic_fs::access::{
        Capability, Context, INITIAL_POLICY, PathTrie, Policy, Principal, PrincipalKind, Resource,
    };
    let Some(label) = crate::core::filesystem_scope_label(scope.capabilities(), &mut None)? else {
        return Ok(());
    };
    let denied =
        || Error::Unauthorized("admission does not grant the complete volume access".into());
    let capability = Capability::from_scope_label(label)
        .ok()
        .flatten()
        .ok_or_else(denied)?;
    let policy: Policy = serde_json::from_slice(INITIAL_POLICY)
        .map_err(|_| Error::FilesystemAccessConfiguration("initial policy is invalid"))?;
    let principal = Principal {
        kind: PrincipalKind::Agent,
        id: owner.to_string(),
        lineage: Vec::new(),
        groups: Default::default(),
        roles: Default::default(),
        tags: Default::default(),
        attributes: Default::default(),
    };
    let now = super::access::access_now()?;
    let view = PathTrie::compile(
        &policy,
        &principal,
        Resource {
            config: acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral),
            lineage: &[],
            volume: volume.id(),
            generation: 0,
            head: 0,
            path: "",
        },
        Context { now, approvals: 0 },
        &capability,
    )
    .map_err(|_| denied())?;
    if view.allows_every_path(actions, now) {
        Ok(())
    } else {
        Err(denied())
    }
}

/// A local root agent's grants: routing, tools, the visible entries' read/write
/// ceilings, its provider's seed layer and retained exact input versions.
/// Raw writers cannot widen a rooted entry; scoped publication belongs to the
/// table's view composition.
fn root_grants(volumes: &[WorkspaceEntry], external: &[FileRef]) -> Result<Vec<String>> {
    let mut grants = Vec::with_capacity(3 * volumes.len() + 2);
    let seed = volumes
        .iter()
        .find_map(WorkspaceEntry::as_volume)
        .map(|volume| crate::seed::volume(volume.provider()))
        .transpose()?;
    if let Some(seed) = &seed {
        grants.push(seed.capability(VolumeOperation::Read, &mut None)?);
    }
    grants.push(crate::contract::capability::INTERACTION_ROUTE.to_owned());
    // Every agent may spawn, message and archive within its own subtree.
    grants.extend(crate::tools::AGENT_TOOLS.map(crate::contract::capability::tool_call));
    for binding in volumes
        .iter()
        .filter(|binding| binding.role != crate::agent::WorkspaceRole::Internal)
    {
        let Some(volume) = binding.as_volume() else {
            continue; // A lazy subtree view has no whole-volume authority.
        };
        if !binding.root.is_empty() {
            grants.push(volume.directory_read_capability(&binding.root, &mut None)?);
        } else {
            grants.push(volume.capability(VolumeOperation::Read, &mut None)?);
            if binding.mode == crate::agent::WorkspaceMode::ReadWrite {
                for operation in [VolumeOperation::Write, VolumeOperation::Delete] {
                    grants.push(volume.capability(operation, &mut None)?);
                }
            }
        }
    }
    for file in external {
        // A retained exact file can lie outside the current root on a still
        // bound volume. Its immutable conversation selection grants it alone.
        if seed.as_ref() != Some(file.volume())
            && !volumes.iter().any(|binding| {
                binding.role != crate::agent::WorkspaceRole::Internal
                    && binding.as_volume() == Some(file.volume())
                    && binding.root.is_empty()
            })
        {
            // Whole-volume reads already cover retained versions. Adding
            // redundant exact grants on reopen would change an unchanged
            // execution's authority binding merely because it retained input.
            grants.push(file.read_capability(&mut None)?);
            // An attachment manifest's files are copies in its own directory,
            // so one grant reaches them however many it lists.
            if file.descriptor().media_type() == crate::conversation::ATTACHMENT_MANIFEST_MEDIA_TYPE
                && let Some((directory, _)) = file.path().rsplit_once('/')
            {
                grants.push(
                    file.volume()
                        .directory_read_capability(directory, &mut None)?,
                );
            }
        }
    }
    Ok(grants)
}

/// The authority of the agent at `path` over `volumes`, signed with
/// `issuer`'s custody under `admission`: [`AdmittedAgent::verify`], then its
/// authority over `external` files.
pub async fn agent_authority<'v>(
    issuer: &RuntimeAuthority,
    admission: &'v TurnAdmission,
    path: &AgentPath,
    volumes: &'v [WorkspaceEntry],
    external: &[FileRef],
) -> Result<AgentAuthority<'v>> {
    AdmittedAgent::verify(issuer, admission, path, volumes)?
        .authority(external)
        .await
}

/// The host's authority over this agent's implicit journal and exact retained
/// conversation files. The original admission is verified before storage IO.
/// This scope carries host custody and must never enter task or tool bindings;
/// use [`agent_authority`] for the workload's independent workspace scope.
pub async fn agent_journal_authority<'v>(
    issuer: &RuntimeAuthority,
    admission: &'v TurnAdmission,
    path: &AgentPath,
    volumes: &'v [WorkspaceEntry],
    retained: &[FileRef],
) -> Result<(VolumeRef, AgentAuthority<'v>)> {
    AdmittedAgent::verify(issuer, admission, path, volumes)?
        .journal_authority(retained)
        .await
}

/// Builds the executor of one agent turn: its tools, context, policy, limits
/// and compaction, under the turn's [`TurnAuthority`]. It may stage files (a
/// context manifest) through the authority's private writer.
pub trait TurnExecutors: acyclic_stream::ProviderPlatform {
    /// Default question plugin choice for a newly created root session.
    fn question_plugin(&self) -> bool {
        false
    }

    /// Root model selected by this composition, pinned at root creation.
    fn model_selection(&self) -> Option<String> {
        None
    }

    /// Publishes each bound workspace before the original turn becomes terminal.
    fn settle<'a>(
        &'a self,
        _agent: &'a AgentState,
        _turn: OperationId,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
    /// The executor's admitted content bounds. Selection chunks announcements
    /// and pending events under these same bounds before request admission.
    fn content_limits(&self) -> crate::conversation::Limits {
        Default::default()
    }

    /// Executor for `agent`'s turn `turn` on `input`.
    fn executor<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxFuture<'a, Result<Arc<dyn Executor>>>;

    /// Admitted workspace exposures and instructions not yet present in this
    /// selected conversation. `seen` binds each slot to its latest discovered
    /// source identity, as returned by `WorkspaceInstructions::source_for`.
    /// Hosts without workspace exposure return none.
    fn workspaces<'a>(
        &'a self,
        agent: &'a AgentState,
        seen: &'a std::collections::BTreeMap<crate::agent::EntryId, [u8; 32]>,
        content: &'a ContentBindings,
        issuer: &'a RuntimeAuthority,
        scope: &'a Scope,
    ) -> BoxFuture<'a, Result<Option<crate::agent::WorkspaceUpdate>>> {
        let _ = (agent, seen, content, issuer, scope);
        Box::pin(async { Ok(None) })
    }

    /// Whether these turns use filesystem templates at agent creation.
    fn templates(&self) -> bool {
        false
    }

    /// The execution targets `agent`'s `shell` reaches, announced to its
    /// model whenever they change; `None` binds no targets.
    fn roster(&self, agent: &AgentState) -> Option<Arc<crate::agent::Roster>> {
        let _ = agent;
        None
    }
}

/// Bounds of one agent turn.
#[derive(Clone, Copy, Debug)]
pub struct TurnLimits {
    /// Journal bytes per turn.
    pub maximum_bytes: Option<u64>,
    /// Page size of the settled history in the model context. The window
    /// starts at a multiple of this many conversation entries and holds at
    /// least this many and fewer than twice as many, so it advances one whole
    /// page at a time: the prompt prefix, and with it the provider's prompt
    /// cache, is rebuilt once per page rather than on every turn. Page cuts
    /// are absolute sequences, so forks that share history share the cut.
    pub history_turns: u32,
}

/// [`TurnHost`] binding every agent to its executor and a journal on its
/// private volume, under a conversation authority named by the agent's id.
pub struct FilesystemTurnHost<P: ?Sized, A: 'static, O: 'static> {
    stream: Arc<P>,
    within: acyclic_actors::addressing::ActorReference,
    host: Arc<FilesystemHost<A, O>>,
    issuer: RuntimeAuthority,
    admission: TurnAdmission,
    executors: Arc<dyn TurnExecutors>,
    workspace_tools: Option<WorkspaceToolsFactory>,
    limits: TurnLimits,
    approvers: crate::approvals::ApproverModels,
    goals: Option<crate::goal::GoalContinuationPolicy>,
    policy: crate::approvals::ApprovalPolicy,
    templates: Arc<dyn crate::agent::SpawnTemplates>,
    #[cfg(feature = "test-support")]
    binding_fault: Option<Arc<dyn Fn(&AgentState) -> Result<()> + Send + Sync>>,
}

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::filesystem) type WorkspaceToolsFactory = Arc<
    dyn Fn(&AgentState, &TurnAuthority) -> Result<Arc<dyn crate::runtime::FilesystemExecutor>>
        + Send
        + Sync,
>;
#[cfg(target_arch = "wasm32")]
pub(in crate::filesystem) type WorkspaceToolsFactory =
    Arc<dyn Fn(&AgentState, &TurnAuthority) -> Result<Arc<dyn crate::runtime::FilesystemExecutor>>>;

/// A turn's bindings before its approval gate is resolved.
struct Bindings<A: 'static, O: 'static> {
    issuer: RuntimeAuthority,
    scope: Scope,
    grants: Capabilities,
    content: ContentBindings,
    journal: JournalStorage<A, O>,
}

/// Host custody is separate from the authority exposed to the model and tools.
struct JournalStorage<A: 'static, O: 'static> {
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
    files: Arc<AgentInternalFiles<A, O>>,
    reader: Arc<FilesystemContentVerifier<A, O>>,
}

impl<P, O> FilesystemTurnHost<P, acyclic_fs::StreamAuthorityStore<P>, O>
where
    P: acyclic_fs::distributed::AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Binds filesystem commands to each turn's original placement and authority.
    #[must_use]
    pub fn with_filesystem_tools(mut self) -> Self {
        let host = Arc::clone(&self.host);
        self.workspace_tools = Some(Arc::new(move |agent, authority| {
            Ok(Arc::new(super::FilesystemTools::new(
                Arc::clone(&host),
                agent,
                authority,
            )?))
        }));
        self
    }
}

impl<P, A, O> FilesystemTurnHost<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Host signing each agent's authority with `issuer`'s custody, derived
    /// from `admission`. Every turn runs under one approval policy, resolved
    /// once: the agent's own policy file, chosen explicitly when it was
    /// adopted, or else `policy`, the host's preset. No policy is ever
    /// absent, so accepting every action is only ever an explicit choice.
    /// A malformed pinned policy denies actions. `within` is the original actor
    /// account and namespace used to observe child settlement.
    #[must_use]
    pub fn new(
        stream: Arc<P>,
        within: acyclic_actors::addressing::ActorReference,
        host: Arc<FilesystemHost<A, O>>,
        issuer: impl Into<RuntimeAuthority>,
        admission: TurnAdmission,
        executors: Arc<dyn TurnExecutors>,
        limits: TurnLimits,
        policy: crate::approvals::ApprovalPolicy,
    ) -> Self {
        let issuer = issuer.into();
        let templates = Arc::new(super::Templates::new(
            Arc::clone(&host),
            issuer.clone(),
            admission.clone(),
        ));
        Self {
            stream,
            within,
            host,
            issuer,
            templates,
            admission,
            executors,
            workspace_tools: None,
            limits,
            approvers: Default::default(),
            goals: Some(Default::default()),
            policy,
            #[cfg(feature = "test-support")]
            binding_fault: None,
        }
    }

    /// Injects a deterministic failure before a turn binds or reads content.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    #[must_use]
    pub fn with_binding_fault(
        mut self,
        fault: impl Fn(&AgentState) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        self.binding_fault = Some(Arc::new(fault));
        self
    }

    /// The same reader both model and host child creation use.
    pub fn with_topology(mut self, topology: crate::topology::Topology) -> Self {
        self.templates = Arc::new(crate::topology::Templates {
            inner: self.templates,
            topology,
        });
        self
    }

    /// Start-time default for new roots; each root retains its own selection.
    pub fn question_plugin(&self) -> bool {
        self.executors.question_plugin()
    }

    /// The same reader both model and host child creation use.
    pub fn templates(&self) -> Option<Arc<dyn crate::agent::SpawnTemplates>> {
        self.executors
            .templates()
            .then(|| Arc::clone(&self.templates))
    }

    /// Continues each agent's active goal within `goals`; `None` runs no goals.
    #[must_use]
    pub const fn with_goals(mut self, goals: Option<crate::goal::GoalContinuationPolicy>) -> Self {
        self.goals = goals;
        self
    }

    /// Binds the independent models that `llm` approval stages name.
    #[must_use]
    pub fn with_approvers(mut self, approvers: crate::approvals::ApproverModels) -> Self {
        self.approvers = approvers;
        self
    }

    /// `agent`'s approval policy, the parent answering its parent stages, and
    /// the approval files on its private volume.
    async fn approvals(
        &self,
        agent: &AgentState,
        reader: &dyn ContentResidencyVerifier,
    ) -> Result<(AgentApprovalPolicy, ApprovalPolicy)> {
        let limits = crate::conversation::Limits {
            file_bytes: self.limits.maximum_bytes,
            ..crate::conversation::Limits::default()
        };
        let parent = agent.parent.as_ref().map(crate::agent::AgentPath::id);
        let bound = match &agent.policy {
            Some(file) => {
                let bytes = reader
                    .read(file, file.descriptor().byte_length(), None, None)?
                    .await?;
                AgentApprovalPolicy::from_file(
                    agent.path.id(),
                    &bytes,
                    parent,
                    self.approvers.clone(),
                    limits,
                )?
            }
            None => AgentApprovalPolicy::new(
                agent.path.id(),
                self.policy.clone(),
                parent,
                self.approvers.clone(),
                limits,
            )?,
        };
        let policy = bound.policy().clone();
        Ok((bound, policy))
    }

    /// `responder` decides approval `id` of `agent`. On success the caller
    /// appends `InboxRecord::Approval { id }` to the agent's inbox to resume it.
    pub async fn decide(
        &self,
        agent: &AgentState,
        id: &str,
        responder: crate::approvals::ApprovalResponder<'_>,
        verdict: crate::interaction::ApprovalVerdict,
        reason: Option<String>,
    ) -> Result<()> {
        let (_, bindings, files) = self.approval_journal(agent).await?;
        let (policy, _) = self
            .approvals(agent, bindings.journal.reader.as_ref())
            .await?;
        crate::approvals::decide(&*files, &policy, id, responder, verdict, reason).await
    }

    /// Decides approval `request` of `agent` as `responder`, then wakes the
    /// waiting turn with one `InboxRecord::Approval` from that responder.
    ///
    /// The embedding authenticates `responder` (its human connection or the
    /// running parent); only human and parent roles enter here. Retrying
    /// identical arguments repairs a crash between decision and wake; an
    /// uncertain wake is `agent::Error::Unknown` with its exact request.
    pub async fn respond(
        &self,
        agents: &crate::agent::AgentHost<P>,
        agent: &AgentPath,
        request: &crate::agent::IntentId,
        responder: crate::approvals::ApprovalResponder<'_>,
        verdict: crate::interaction::ApprovalVerdict,
        reason: Option<String>,
    ) -> std::result::Result<crate::agent::Delivered, crate::agent::Error> {
        use crate::agent::{Approval, Error as AgentError, InboxRecord, Sender};
        use crate::approvals::ApprovalResponder;
        let state = agents.state(agent).await?;
        let from = match responder {
            ApprovalResponder::Human(principal) => Sender::User {
                principal: principal.to_owned(),
            },
            ApprovalResponder::Parent(parent) => Sender::Agent {
                path: state
                    .parent
                    .clone()
                    .filter(|path| &path.id() == parent)
                    .ok_or(AgentError::Record("approval responder is not the parent"))?,
            },
            _ => return Err(AgentError::Record("external approval role is required")),
        };
        self.decide(&state, request.as_str(), responder, verdict, reason)
            .await
            .map_err(|error| AgentError::Turn(Box::new(error)))?;
        let wake = InboxRecord::Approval(Approval {
            id: request.clone(),
        });
        agents.send(agent, &from, &wake).await
    }

    /// Approval `id` of `agent` as `principal` already decided it, read from
    /// its request file whether or not the turn still waits on it: an
    /// identical decision retried after the agent resumed replays.
    pub async fn decided_approval(
        &self,
        agent: &AgentState,
        id: &crate::agent::IntentId,
        principal: &str,
    ) -> Result<Option<crate::interaction::Interaction>> {
        let (_, bindings, files) = self.approval_journal(agent).await?;
        let (policy, _) = self
            .approvals(agent, bindings.journal.reader.as_ref())
            .await?;
        let Some(bytes) =
            crate::approvals::decided_request(&*files, &policy, id.as_str(), principal).await?
        else {
            return Ok(None);
        };
        crate::contract::json_from_slice(&bytes)
            .map(Some)
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    /// The journal `agent`'s approvals are read and decided through: its
    /// inherited approval policy, staged on an ancestor's private volume, is
    /// granted by its exact version as its turns grant it.
    async fn approval_journal(
        &self,
        agent: &AgentState,
    ) -> Result<(
        FilesystemExecutionJournal<P, A, O>,
        Bindings<A, O>,
        Arc<AgentInternalFiles<A, O>>,
    )> {
        self.journal(self.admit(agent)?, agent.policy.as_slice(), &[], None, None)
            .await
    }

    /// Opens journal queries through this agent's original journal grant.
    pub(super) async fn query_journal(
        &self,
        agent: &AgentState,
    ) -> Result<FilesystemExecutionJournal<P, A, O>> {
        let (journal, _, _) = self
            .journal(self.admit(agent)?, &[], &[], None, None)
            .await?;
        Ok(journal)
    }

    pub(super) async fn journal_authority<'a>(
        &'a self,
        agent: &'a AgentState,
    ) -> Result<(VolumeRef, AgentAuthority<'a>)> {
        self.admit(agent)?.journal_authority(&[]).await
    }

    /// Read current project roots through the agent's signed grant and persisted
    /// filesystem policy. A write selection additionally requires whole-volume
    /// publication authority; the returned generation is the policy's own cut.
    pub async fn project_heads(
        &self,
        agent: &AgentState,
        operation: VolumeOperation,
    ) -> Result<Vec<crate::agent::HistoryVolume>> {
        self.project_selection(agent, operation, None).await
    }

    async fn project_selection(
        &self,
        agent: &AgentState,
        operation: VolumeOperation,
        saved: Option<&[crate::agent::SavedVolume]>,
    ) -> Result<Vec<crate::agent::HistoryVolume>> {
        use acyclic_fs::access::{Action, ActionSet};
        let authority = self.admit(agent)?.authority(&[]).await?;
        let cancel = acyclic_fs::CancellationToken::new();
        let io = super::PreparationIo::for_unmeasured(&cancel)?;
        let label = crate::core::filesystem_scope_label(authority.scope.capabilities(), &mut None)?;
        let payload = super::access::capability_payload(label, operation, &io)?;
        let actions = match operation {
            VolumeOperation::Read => &[Action::List, Action::Read][..],
            VolumeOperation::Write => ActionSet::DestinationWrite.actions(),
            VolumeOperation::Delete => ActionSet::DestinationDelete.actions(),
        };
        let mut heads = Vec::new();
        for binding in &agent.volumes {
            if binding
                .as_volume()
                .is_none_or(|volume| volume.class() != VolumeClass::Project)
            {
                continue;
            }
            crate::conversation::ContentGrant::authorize_volume(
                &authority.verifier,
                &authority.scope,
                binding.volume()?,
                operation,
                &mut None,
            )?;
            let workspace = self
                .host
                .filesystem
                .open_workspace(
                    binding.volume()?.storage_name(&mut None)?,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &cancel,
                )
                .await
                .map_err(|failure| Error::Storage(failure.error.to_string()))?
                .value;
            let view =
                super::access::access_view(&workspace, &authority.scope, None, payload, None, &io)
                    .await?;
            if !view
                .0
                .allows_every_path(actions, super::access::access_now()?)
            {
                return Err(Error::Unauthorized(
                    "project selection requires authority over the whole volume".into(),
                ));
            }
            if let Some(saved) = saved {
                let source = saved
                    .iter()
                    .find(|source| source.binding.id == binding.id)
                    .ok_or_else(|| Error::Unauthorized("saved project binding differs".into()))?;
                let retained = io.sdk(
                    self.host
                        .filesystem
                        .open_workspace(&source.retained_workspace, &io, &cancel)
                        .await,
                )?;
                let mut checkout = io.sdk(
                    retained
                        .checkout(
                            acyclic_fs::model::GenerationSelector::Exact(
                                source.retained_generation,
                            ),
                            acyclic_fs::model::CheckoutMode::read_only_pinned(),
                            &io,
                            &cancel,
                        )
                        .await,
                )?;
                let revision = io.sdk(checkout.access_policy_revision(&io, &cancel).await)?;
                // Root metadata is part of a complete generation restore.
                // Content publication authority cannot roll back policy. The
                // current decision already carries its authenticated revision;
                // compare the immutable source without another head read.
                if !view.0.matches_policy_revision(view.2, revision) {
                    return Err(Error::Unauthorized(
                        "file history selection cannot replace the current access policy".into(),
                    ));
                }
            }
            heads.push(crate::agent::HistoryVolume {
                binding: binding.clone(),
                generation: view.2,
            });
        }
        Ok(heads)
    }

    /// `agent`'s verified admission; it performs no IO.
    fn admit<'v>(&'v self, agent: &'v AgentState) -> Result<AdmittedAgent<'v>> {
        AdmittedAgent::verify(&self.issuer, &self.admission, &agent.path, &agent.volumes)
    }

    /// The journal and authority of `agent`'s turns. Each `external` input
    /// file on a volume the agent does not bind (a message or result from
    /// another agent, current or in history) is granted alone, by its exact
    /// version.
    async fn journal(
        &self,
        admitted: AdmittedAgent<'_>,
        external: &[FileRef],
        retained: &[FileRef],
        context: Option<&mut TurnContext<'_>>,
        stop: Option<acyclic_actors::guest::CancellationToken>,
    ) -> Result<(
        FilesystemExecutionJournal<P, A, O>,
        Bindings<A, O>,
        Arc<AgentInternalFiles<A, O>>,
    )> {
        let local_root = admitted.parent.is_none();
        let storage = crate::core::until_turn_stopped(
            stop.clone(),
            self.journal_storage(&admitted, external.iter().chain(retained)),
        )
        .await?;
        let AgentAuthority {
            issuer,
            mut scope,
            grants: mut capabilities,
            verifier,
            private,
        } = crate::core::until_turn_stopped(stop.clone(), admitted.authority(external)).await?;
        // The turn's reader and writer take no content bound of their own:
        // each caller states its bound, and harness-owned records (manifests,
        // settled outputs, policies) are read at their pinned length, so a
        // configured file bound limits admitted content, never the turn.
        let mut reader = FilesystemContentVerifier::new(
            Arc::clone(&self.host),
            &verifier,
            &scope,
            crate::conversation::MAX_EXACT_JS_INTEGER,
            None,
        )?;
        // Open only the committed input envelopes under their original grant.
        // Their validated ordered contents become the selection before deriving
        // the exact-file closure, including files on an ancestor's private volume.
        for file in external {
            crate::conversation::ContentGrant::verify_read(&verifier, &scope, file)?;
        }
        if let Some(context) = context {
            // Decode selected host-owned envelopes before deriving the
            // workload-visible content closure from their validated contents.
            context.expand_inputs(storage.reader.as_ref()).await?;
            let mut inherited = Vec::new();
            for file in context.files() {
                if let Err(error) =
                    crate::conversation::ContentGrant::verify_read(&verifier, &scope, file)
                {
                    if !local_root {
                        return Err(error);
                    }
                    inherited.push(file.read_capability(&mut None)?);
                }
            }
            if !inherited.is_empty() {
                // Local history delegates only these immutable files. An admitted
                // workload retains its original ceiling and cannot mint this grant.
                inherited.extend(capabilities.iter().map(str::to_owned));
                capabilities = Capabilities::new(inherited);
                scope = crate::core::until_turn_stopped(
                    stop.clone(),
                    issuer.issue(None, "agent", &capabilities, scope.agent()),
                )
                .await?;
                reader = FilesystemContentVerifier::new(
                    Arc::clone(&self.host),
                    &verifier,
                    &scope,
                    crate::conversation::MAX_EXACT_JS_INTEGER,
                    None,
                )?;
            }
        }
        let writer = FilesystemContentPublisher::new(
            Arc::clone(&self.host),
            private.volume()?,
            &verifier,
            &scope,
            crate::conversation::MAX_EXACT_JS_INTEGER,
            None,
        )?;
        let journal = FilesystemExecutionJournal::new(
            Arc::clone(&self.stream),
            Arc::clone(&self.host),
            FilesystemJournalBinding {
                volume: &storage.volume,
                verifier: &storage.verifier,
                scope: &storage.scope,
                io: None,
            },
        )?
        .with_input_verifier(reader.clone());
        let internal = self.internal_files(
            &storage.volume,
            private.volume()?,
            &verifier,
            &storage.scope,
            &scope,
        )?;
        Ok((
            journal,
            Bindings {
                issuer,
                scope,
                grants: capabilities,
                content: ContentBindings {
                    volume: private.volume()?.clone(),
                    reader,
                    writer: Some(writer),
                },
                journal: storage,
            },
            internal,
        ))
    }

    fn internal_files(
        &self,
        volume: &VolumeRef,
        public_volume: &VolumeRef,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        public_scope: &Scope,
    ) -> Result<Arc<AgentInternalFiles<A, O>>> {
        Ok(Arc::new(AgentInternalFiles::new(
            Arc::clone(&self.host),
            volume.clone(),
            crate::conversation::ContentGrant::verify(
                verifier,
                scope,
                volume,
                VolumeOperation::Write,
                &mut None,
            )?,
            public_volume.clone(),
            crate::conversation::ContentGrant::verify(
                verifier,
                public_scope,
                public_volume,
                VolumeOperation::Write,
                &mut None,
            )?,
        )))
    }

    /// Host-only storage for execution payloads, effects and announcements.
    /// Retained documents are selected by the conversation, independently of
    /// current workspace roots.
    /// This reader and its journal write grant never enter the tool bindings.
    async fn journal_storage<'a>(
        &self,
        admitted: &AdmittedAgent<'_>,
        retained: impl IntoIterator<Item = &'a FileRef>,
    ) -> Result<JournalStorage<A, O>> {
        let (volume, authority) = admitted.journal_authority(retained).await?;
        let AgentAuthority {
            scope, verifier, ..
        } = authority;
        let files = self.internal_files(&volume, &volume, &verifier, &scope, &scope)?;
        let reader = FilesystemContentVerifier::new(
            Arc::clone(&self.host),
            &verifier,
            &scope,
            crate::conversation::MAX_EXACT_JS_INTEGER,
            None,
        )?;
        Ok(JournalStorage {
            volume,
            verifier,
            scope,
            files,
            reader,
        })
    }

    /// The agent's settled turns from the current history page cut, with their
    /// sequences, oldest first.
    async fn history(
        &self,
        _: &AdmittedAgent<'_>,
        agent: &AgentState,
    ) -> Result<Vec<(u64, ConversationEntry)>> {
        if agent.conversation_tail == 0 {
            return Ok(Vec::new());
        }
        let cut = super::history::read(
            self.stream.as_ref(),
            &agent.path,
            agent.conversation_tail - 1,
        )
        .await?
        .context;
        let page = u64::from(self.limits.history_turns);
        let from = cut.tail.saturating_sub(page) / page.max(1) * page.max(1);
        let mut entries = Vec::new();
        if let Some(checkpoint) = cut.checkpoint.filter(|at| *at < from) {
            entries
                .extend((checkpoint..).zip(self.context_entries(&cut.path, checkpoint, 1).await?));
        }
        entries.extend(
            (from..).zip(
                self.context_entries(&cut.path, from, cut.tail - from)
                    .await?,
            ),
        );
        Ok(entries)
    }

    /// `count` conversation entries of `agent` from sequence `from`.
    async fn context_entries(
        &self,
        path: &acyclic_stream::StreamPath,
        mut from: u64,
        count: u64,
    ) -> Result<Vec<ConversationEntry>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let end = from
            .checked_add(count)
            .ok_or_else(|| Error::Invalid("conversation range overflows".into()))?;
        let mut entries = Vec::new();
        while from < end {
            let limit = (end - from).min(acyclic_stream::MAX_ITEMS as u64) as u32;
            let through = from + u64::from(limit);
            let records = self
                .stream
                .read(ReadRequest {
                    key: None,
                    path: path.clone(),
                    from,
                    limit,
                    at_least: Default::default(),
                })
                .await?;
            let mut records = acyclic_stream::pages::ExactRange::new(records, from..through)?;
            while let Some(record) = records.try_next().await? {
                entries.push(ConversationEntry::decode(&record.value).map_err(Error::from)?);
            }
            from = through;
        }
        Ok(entries)
    }
}

/// Stable message identity of one side of a settled or current turn.
fn message_id(turn: OperationId, role: ModelRole) -> uuid::Uuid {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.agent.message.v1");
    hasher.update(&turn.into_bytes());
    hasher.update(&[u8::from(role == ModelRole::Assistant)]);
    let mut bytes = [0; 16];
    hasher.finalize_xof().fill(&mut bytes);
    uuid::Uuid::from_bytes(bytes)
}

/// Stable message identity of a settled turn's later transcript message; the
/// first keeps the reply's own identity.
fn reply_part_id(turn: OperationId, part: usize) -> uuid::Uuid {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.agent.reply-part.v1");
    hasher.update(&turn.into_bytes());
    hasher.update(&(part as u64).to_le_bytes());
    let mut bytes = [0; 16];
    hasher.finalize_xof().fill(&mut bytes);
    uuid::Uuid::from_bytes(bytes)
}

/// Stable message identity of the announcement delta leading the turn input.
fn announcement_id(turn: OperationId) -> uuid::Uuid {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.agent.roster.v1");
    hasher.update(&turn.into_bytes());
    let mut bytes = [0; 16];
    hasher.finalize_xof().fill(&mut bytes);
    uuid::Uuid::from_bytes(bytes)
}

fn announcement_part_id(turn: OperationId, part: usize) -> uuid::Uuid {
    if part == 0 {
        return announcement_id(turn);
    }
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.agent.announcement-part.v1");
    hasher.update(&turn.into_bytes());
    hasher.update(&(part as u64).to_le_bytes());
    let mut bytes = [0; 16];
    hasher.finalize_xof().fill(&mut bytes);
    uuid::Uuid::from_bytes(bytes)
}

fn notice_id(turn: OperationId, block: usize) -> uuid::Uuid {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.agent.notices.v1");
    hasher.update(&turn.into_bytes());
    hasher.update(&(block as u64).to_le_bytes());
    let mut bytes = [0; 16];
    hasher.finalize_xof().fill(&mut bytes);
    uuid::Uuid::from_bytes(bytes)
}

/// Read only the frozen original interval. A quiet event delivered during a
/// paused turn is beyond this cut and cannot change an admitted request.
async fn selected_notices<P: StreamProvider + ?Sized>(
    stream: &P,
    agent: &AgentPath,
    selected: Option<&crate::agent::NoticeSelection>,
    input: ModelContent,
    maximum: Option<u64>,
) -> Result<(Vec<ModelContent>, Option<ModelContent>)> {
    use crate::model::ModelContentPart;
    let mut pending = NoticeBlocks::new(maximum);
    if let Some(selected) = selected {
        selected.validate(agent).map_err(Error::from)?;
        let mut from = selected.range.start;
        while from < selected.range.end {
            let limit = (selected.range.end - from).min(acyclic_stream::MAX_ITEMS as u64) as u32;
            let records = stream
                .read(ReadRequest {
                    key: None,
                    path: selected.source.inbox().map_err(Error::from)?,
                    from,
                    limit,
                    at_least: Default::default(),
                })
                .await?;
            let end = from + u64::from(limit);
            let mut records = acyclic_stream::pages::ExactRange::new(records, from..end)?;
            while let Some(record) = records.try_next().await? {
                if let crate::agent::InboxRecord::Notice {
                    notice: crate::agent::NoticePayload::Quiet(notice),
                    ..
                } = crate::agent::InboxRecord::decode(&record.value).map_err(Error::from)?
                {
                    pending.push(notice)?;
                }
            }
            from = end;
        }
    }
    let input = match input {
        ModelContent::Part(ModelContentPart::Notices { notices }) => {
            for notice in notices {
                pending.push(notice)?;
            }
            None
        }
        ModelContent::Parts(mut parts)
            if matches!(parts.first(), Some(ModelContentPart::Notices { .. })) =>
        {
            // Move the original attachment vector; do not clone its files or
            // derive authority from references mentioned in notice JSON.
            parts.rotate_left(1);
            let Some(ModelContentPart::Notices { notices }) = parts.pop() else {
                return Err(Error::Invalid("notice input lost its event block".into()));
            };
            for notice in notices {
                pending.push(notice)?;
            }
            match parts.len() {
                0 => None,
                1 => parts.pop().map(ModelContent::Part),
                _ => Some(ModelContent::Parts(parts)),
            }
        }
        other => Some(other),
    };
    Ok((pending.finish(), input))
}

/// Partition before coalescing: individually admitted notices cannot grow one
/// unbounded coalesced paths array. Each block retains the original event order;
/// coalescing is restricted to that unsent block and can only reduce its size.
pub(super) struct NoticeBlocks {
    pending: acyclic_model_render::PendingNotices,
    blocks: Vec<ModelContent>,
    maximum: Option<u64>,
    envelope: u64,
    bytes: u64,
    count: usize,
}

impl NoticeBlocks {
    pub(super) fn new(maximum: Option<u64>) -> Self {
        Self {
            pending: Default::default(),
            blocks: Vec::new(),
            maximum,
            envelope: 0,
            bytes: 0,
            count: 0,
        }
    }

    pub(super) fn push(&mut self, notice: crate::conversation::wire::Notice) -> Result<()> {
        if let Some(maximum) = self.maximum {
            if self.envelope == 0 {
                self.envelope = crate::executor::json::json_byte_length(
                    &crate::model::ModelContentPart::Notices {
                        notices: Vec::new(),
                    },
                    Some(maximum),
                    &mut None,
                )?;
                self.bytes = self.envelope;
            }
            // Count the original event once, without a temporary JSON buffer.
            let bytes = crate::executor::json::json_byte_length_at_depth(
                &notice,
                Some(maximum - self.envelope),
                256,
                &mut None,
            )?;
            let separator = u64::from(self.count != 0);
            if bytes + separator > maximum - self.bytes {
                self.flush();
            }
            self.bytes += bytes + u64::from(self.count != 0);
        }
        self.pending
            .push(notice)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        self.count += 1;
        Ok(())
    }

    fn flush(&mut self) {
        if self.count != 0 {
            let notices = std::mem::take(&mut self.pending).finish();
            self.blocks.push(ModelContent::Part(
                crate::model::ModelContentPart::Notices { notices },
            ));
            self.bytes = self.envelope;
            self.count = 0;
        }
    }

    pub(super) fn finish(mut self) -> Vec<ModelContent> {
        self.flush();
        self.blocks
    }
}

/// Stable, domain-separated identity of a directive.
fn directive_id(domain: &str, text: &str) -> uuid::Uuid {
    let mut hasher = blake3::Hasher::new_derive_key(domain);
    hasher.update(text.as_bytes());
    let mut bytes = [0; 16];
    hasher.finalize_xof().fill(&mut bytes);
    uuid::Uuid::from_bytes(bytes)
}

/// A turn's context before its settled outputs are read: the system prompt,
/// each settled turn (its model input and staged output), the current input,
/// and the approval policy it runs under. It is the one place that names what
/// a turn can reach, so the turn's read grant ([`Self::files`]) and its model
/// messages ([`Self::select`]) cannot disagree: a source added here is granted
/// before either a child's result or a follow-up can be observed.
struct TurnContext<'a> {
    /// The latest checkpoint: its turn and its staged summary.
    checkpoint: Option<(OperationId, FileRef)>,
    /// Each settled turn, its original input and output, and its decoded transcript.
    settled: Vec<SettledTurn>,
    /// Where a spawned agent's own conversation starts: after the summary
    /// and this many settled turns inherited from its parent, or before the
    /// summary when its own checkpoint replaced them. Its role directive sits
    /// exactly there, so its context extends the parent's at the fork.
    fork: Option<usize>,
    /// Each inherited settled turn that opens an ancestor's own turns, and
    /// that ancestor: the ancestor's directive sits before it, exactly where
    /// the ancestor's own requests placed it at its fork point.
    ancestors: Vec<(usize, AgentPath)>,
    turn: OperationId,
    input: ModelContent,
    policy: Option<&'a FileRef>,
    notices: Option<crate::agent::NoticeSelection>,
}

struct SettledTurn {
    turn: OperationId,
    announcements: Vec<crate::agent::AnnouncedDocument>,
    input: ModelContent,
    output: FileRef,
    steering: Option<FileRef>,
    steered: Vec<ModelMessage>,
    transcript: Vec<ModelMessage>,
    notices: Option<crate::agent::NoticeSelection>,
}

impl<'a> TurnContext<'a> {
    /// The context of `agent`'s `turn` over its settled `history`: from its
    /// latest checkpoint on, which replaces every entry before it.
    fn new(
        agent: &'a AgentState,
        mut history: Vec<(u64, ConversationEntry)>,
        turn: OperationId,
        input: &'a ModelContent,
    ) -> Result<Self> {
        let inherited = |sequence: u64| sequence < agent.conversation_base;
        let mut fork = Some(0);
        let checkpoint = match history
            .iter()
            .rposition(|(_, entry)| entry.checkpoint().is_some())
        {
            Some(at) => {
                let after = history.split_off(at + 1);
                let (sequence, entry) = std::mem::replace(&mut history, after)
                    .pop()
                    .ok_or_else(|| Error::Invalid("checkpoint entry is absent".into()))?;
                if !inherited(sequence) {
                    fork = None;
                }
                Some((
                    crate::agent::turn_id(agent, &entry.input).map_err(Error::from)?,
                    entry.output,
                ))
            }
            None => None,
        };
        // A cancelled checkpoint selects nothing and is no turn.
        // Every turn stages its output on its own agent's private volume, so
        // the owner of an inherited output names the ancestor that ran it.
        let lineage: Vec<_> = std::iter::successors(agent.path.parent(), AgentPath::parent)
            .map(|path| {
                let owner = VolumeOwner::Agent(crate::AgentId::from_bytes(path.id_bytes()));
                (owner, path)
            })
            .collect();
        let mut ancestors = Vec::new();
        let mut previous = None;
        let settled: Vec<_> = history
            .into_iter()
            .filter(|(_, entry)| entry.input.context().is_none())
            .enumerate()
            .map(|(at, (sequence, entry))| {
                if let Some(fork) = fork.as_mut().filter(|_| inherited(sequence)) {
                    *fork = at + 1;
                }
                if inherited(sequence) {
                    let owner = entry.output.volume().owner();
                    if previous != Some(owner.clone()) {
                        if let Some((_, path)) = lineage.iter().find(|(id, _)| id == owner) {
                            ancestors.push((at, path.clone()));
                        }
                        previous = Some(owner.clone());
                    }
                }
                Ok(SettledTurn {
                    turn: crate::agent::turn_id(agent, &entry.input).map_err(Error::from)?,
                    announcements: entry.announcements,
                    input: crate::agent::model_input_at(entry.inbox, &entry.input)
                        .map_err(Error::from)?,
                    output: entry.output,
                    steering: entry.steering.map(|steering| steering.file),
                    steered: Vec::new(),
                    transcript: Vec::new(),
                    notices: entry.notices,
                })
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            checkpoint,
            settled,
            fork,
            ancestors,
            turn,
            input: input.clone(),
            // A spawned agent runs under the approval policy it inherited,
            // staged on an ancestor's private volume.
            policy: agent.policy.as_ref(),
            notices: crate::agent::turn_notices(agent).map_err(Error::from)?,
        })
    }

    /// Decode each selected input and settled transcript once, retaining
    /// their ordered exact references for both grant derivation and the
    /// eventual model selection.
    async fn expand_inputs(&mut self, reader: &dyn ContentResidencyVerifier) -> Result<()> {
        for SettledTurn {
            input,
            output,
            transcript,
            steered,
            ..
        } in &mut self.settled
        {
            let original = std::mem::replace(input, ModelContent::Text(String::new()));
            *input = crate::agent::expand_attachments(original, reader).await?;
            *transcript = staged_transcript(reader, output, std::mem::take(steered)).await?;
        }
        let original = std::mem::replace(&mut self.input, ModelContent::Text(String::new()));
        self.input = crate::agent::expand_attachments(original, reader).await?;
        Ok(())
    }

    /// Only the latest selected delta of this kind can suppress a new one.
    /// Compare semantic identities: a reroot can retain the displayed paths.
    fn contains_announcement(
        &self,
        kind: crate::agent::AnnouncementKind,
        digest: [u8; 32],
    ) -> bool {
        self.settled
            .iter()
            .rev()
            .flat_map(|turn| turn.announcements.iter().rev())
            .find(|document| document.kind == kind)
            .is_some_and(|document| document.digest == digest)
    }

    async fn instruction_entries(
        &self,
        reader: &dyn ContentResidencyVerifier,
    ) -> Result<std::collections::BTreeMap<crate::agent::EntryId, [u8; 32]>> {
        let mut seen = std::collections::BTreeMap::new();
        for selection in self
            .settled
            .iter()
            .flat_map(|turn| &turn.announcements)
            .flat_map(|document| &document.instructions)
        {
            for entry in selection.entries(reader).await? {
                seen.insert(entry.entry, entry.source);
            }
        }
        Ok(seen)
    }

    /// Files supplied to the turn's model and tool content bindings, each by
    /// its exact version. Announcement documents use the host-only reader.
    /// This includes the files
    /// each message names, each settled output and the files its transcript
    /// names (an inherited turn's are staged on an ancestor's private volume,
    /// a child's result on the child's), and the approval policy.
    fn files(&self) -> impl Iterator<Item = &FileRef> {
        self.checkpoint
            .iter()
            .map(|(_, summary)| summary)
            .chain(self.settled.iter().flat_map(|settled| {
                settled
                    .input
                    .file_refs()
                    .chain(std::iter::once(&settled.output))
                    .chain(
                        settled
                            .transcript
                            .iter()
                            .flat_map(|message| message.content.file_refs()),
                    )
                    .chain(settled.steering.iter())
                    .chain(
                        settled
                            .steered
                            .iter()
                            .flat_map(|message| message.content.file_refs()),
                    )
            }))
            .chain(self.input.file_refs())
            .chain(self.policy)
    }

    /// The selection: the checkpoint's summary as one assistant message
    /// (none when it is empty), then each settled turn's input and its staged
    /// transcript, read through `reader`, which holds [`Self::files`]. Shared
    /// instructions open every selection; a spawned agent's role directive
    /// sits at its fork point. Target rosters remain separate user messages
    /// before each turn input and retain their original publication identity.
    /// Every earlier selection is a prefix of this one, so provider prompt
    /// caches hold across turns and forks.
    async fn select<P: StreamProvider + ?Sized>(
        self,
        stream: &P,
        agent: &AgentState,
        reader: &dyn ContentResidencyVerifier,
        announcement_reader: &dyn ContentResidencyVerifier,
        input: &ModelContent,
        announcements: &[crate::agent::AnnouncedDocument],
        content_limits: crate::conversation::Limits,
    ) -> Result<SelectedModelContext> {
        let render_bytes = content_limits.render_bytes;
        let Self {
            checkpoint,
            settled,
            fork,
            ancestors,
            turn,
            notices: current_notices,
            ..
        } = self;
        let summary = match checkpoint {
            Some((checkpoint, output)) => Some((checkpoint, staged_text(reader, &output).await?))
                .filter(|(_, summary)| !summary.is_empty()),
            None => None,
        };
        let capacity = 2 * settled.len()
            + 2
            + usize::from(summary.is_some())
            + usize::from(agent.parent.is_some())
            + usize::from(!announcements.is_empty());
        let mut messages = Vec::with_capacity(capacity);
        let mut message_ids = Vec::with_capacity(messages.capacity());
        messages.push(ModelMessage {
            role: ModelRole::System,
            content: ModelContent::Text(crate::seed::AGENT_PROMPT.to_owned()),
        });
        message_ids.push(directive_id(
            "acyclic.harness.agent.prompt.v1",
            crate::seed::AGENT_PROMPT,
        ));
        // A spawned agent's role at its fork point. Only the agent's path
        // and its parent's decide the text, so a descendant re-renders an
        // ancestor's directive byte for byte.
        let place = |messages: &mut Vec<ModelMessage>,
                     message_ids: &mut Vec<uuid::Uuid>,
                     child: &AgentPath| {
            let Some(parent) = child.parent() else {
                return;
            };
            let directive = format!(
                "You are child agent {child}, working for parent {parent} in your own fork. Your task is the brief appended to this fork. Your edits stay in this fork until your parent merges your branch. Your final message is your report to your parent. Use send with agent {parent} to send updates or questions."
            );
            message_ids.push(directive_id(
                "acyclic.harness.agent.child-role.v1",
                &directive,
            ));
            messages.push(ModelMessage {
                role: ModelRole::User,
                content: ModelContent::Text(directive),
            });
        };
        let directive = |messages: &mut Vec<ModelMessage>, message_ids: &mut Vec<uuid::Uuid>| {
            if agent.parent.is_some() {
                place(messages, message_ids, &agent.path);
            }
        };
        if fork.is_none() {
            directive(&mut messages, &mut message_ids);
        }
        let forked = settled.len();
        if let Some((checkpoint, summary)) = summary {
            messages.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text(summary),
            });
            message_ids.push(message_id(checkpoint, ModelRole::Assistant));
        }
        for (
            at,
            SettledTurn {
                turn: settled,
                announcements: announced,
                input: content,
                transcript,
                notices,
                ..
            },
        ) in settled.into_iter().enumerate()
        {
            if fork == Some(at) {
                directive(&mut messages, &mut message_ids);
            }
            if let Some((_, ancestor)) = ancestors.iter().find(|(opened, _)| *opened == at) {
                place(&mut messages, &mut message_ids, ancestor);
            }
            if !announced.is_empty() {
                for (part, content) in crate::agent::announcement_messages(
                    &announced,
                    announcement_reader,
                    content_limits,
                )
                .await?
                .into_iter()
                .enumerate()
                {
                    messages.push(ModelMessage {
                        role: ModelRole::User,
                        content,
                    });
                    message_ids.push(announcement_part_id(settled, part));
                }
            }
            let (notices, content) =
                selected_notices(stream, &agent.path, notices.as_ref(), content, render_bytes)
                    .await?;
            for (block, notices) in notices.into_iter().enumerate() {
                messages.push(ModelMessage {
                    role: ModelRole::User,
                    content: notices,
                });
                message_ids.push(notice_id(settled, block));
            }
            if let Some(content) = content {
                messages.push(ModelMessage {
                    role: ModelRole::User,
                    content,
                });
                message_ids.push(message_id(settled, ModelRole::User));
            }
            if transcript.is_empty() {
                return Err(Error::Invalid("settled transcript is not decoded".into()));
            }
            for (part, message) in transcript.into_iter().enumerate() {
                messages.push(message);
                message_ids.push(match part {
                    0 => message_id(settled, ModelRole::Assistant),
                    part => reply_part_id(settled, part),
                });
            }
        }
        if fork == Some(forked) {
            directive(&mut messages, &mut message_ids);
        }
        if !announcements.is_empty() {
            for (part, content) in crate::agent::announcement_messages(
                announcements,
                announcement_reader,
                content_limits,
            )
            .await?
            .into_iter()
            .enumerate()
            {
                messages.push(ModelMessage {
                    role: ModelRole::User,
                    content,
                });
                message_ids.push(announcement_part_id(turn, part));
            }
        }
        let (notices, input) = selected_notices(
            stream,
            &agent.path,
            current_notices.as_ref(),
            input.clone(),
            render_bytes,
        )
        .await?;
        for (block, notices) in notices.into_iter().enumerate() {
            messages.push(ModelMessage {
                role: ModelRole::User,
                content: notices,
            });
            message_ids.push(notice_id(turn, block));
        }
        if let Some(input) = input {
            messages.push(ModelMessage {
                role: ModelRole::User,
                content: input,
            });
            message_ids.push(message_id(turn, ModelRole::User));
        }
        Ok(SelectedModelContext {
            selection: ModelContextSelection {
                conversation_revision: agent.conversation_tail + 1,
                message_ids,
                checkpoint: None,
            },
            messages,
        })
    }
}

/// The assistant text of a staged turn output, read through `reader` at its
/// pinned length.
async fn staged_text(reader: &dyn ContentResidencyVerifier, output: &FileRef) -> Result<String> {
    let bytes = reader
        .read(output, output.descriptor().byte_length(), None, None)?
        .await?;
    serde_json::from_slice::<TurnOutput>(&bytes)
        .map(|output| output.text)
        .map_err(|_| Error::Invalid("staged turn output is malformed".into()))
}

/// A settled turn's transcript after its input as the next turn sees it:
/// its surviving history, retired steering and final reply, verbatim, or its
/// separately admitted steering and text when the output kept none. A complete
/// transcript already contains steering in request order, so its fallback must
/// never be prepended a second time. Replaying the transcript rather than a
/// summary of it preserves the exact admitted steering across turns and forks.
async fn staged_transcript(
    reader: &dyn ContentResidencyVerifier,
    output: &FileRef,
    mut fallback: Vec<ModelMessage>,
) -> Result<Vec<ModelMessage>> {
    let bytes = reader
        .read(output, output.descriptor().byte_length(), None, None)?
        .await?;
    let output = serde_json::from_slice::<TurnOutput>(&bytes)
        .map_err(|_| Error::Invalid("staged turn output is malformed".into()))?;
    Ok(if output.transcript.is_empty() {
        fallback.push(ModelMessage {
            role: ModelRole::Assistant,
            content: ModelContent::Text(output.text),
        });
        fallback
    } else {
        output.transcript
    })
}

impl<P, A, O> TurnHost for FilesystemTurnHost<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn settle<'a>(&'a self, agent: &'a AgentState, turn: OperationId) -> BoxFuture<'a, Result<()>> {
        self.executors.settle(agent, turn)
    }
    fn open_control<'a>(
        &'a self,
        agent: &'a AgentState,
        action: &'a crate::agent::ContextAction,
        stop: acyclic_actors::guest::CancellationToken,
    ) -> BoxFuture<'a, Result<crate::agent::OpenControl>> {
        Box::pin(async move {
            let admitted = self.admit(agent)?;
            let checkpoint = if matches!(action, crate::agent::ContextAction::Rewind)
                && agent.conversation_tail != 0
            {
                let cut = super::history::read(
                    self.stream.as_ref(),
                    &agent.path,
                    agent.conversation_tail - 1,
                )
                .await?
                .context;
                match cut.checkpoint {
                    Some(at) => {
                        let mut records = self
                            .stream
                            .read(ReadRequest {
                                key: None,
                                path: cut.path,
                                from: at,
                                limit: 1,
                                at_least: Default::default(),
                            })
                            .await?;
                        let record = records.try_next().await?.ok_or_else(|| {
                            Error::Invalid("selected checkpoint is absent".into())
                        })?;
                        let entry =
                            ConversationEntry::decode(&record.value).map_err(Error::from)?;
                        if record.sequence != at || entry.checkpoint().is_none() {
                            return Err(Error::Invalid(
                                "selected checkpoint differs from its indexed cut".into(),
                            ));
                        }
                        Some(entry.output)
                    }
                    None => None,
                }
            } else {
                None
            };
            let storage =
                crate::core::until_turn_stopped(Some(stop), self.journal_storage(&admitted, &[]))
                    .await?;
            Ok(crate::agent::OpenControl {
                outputs: storage.files,
                checkpoint,
                custody: Some(admitted.issuer),
            })
        })
    }

    fn selection<'a>(
        &'a self,
        agent: &'a AgentState,
        action: &'a crate::agent::ContextAction,
    ) -> BoxFuture<'a, Result<Option<String>>> {
        Box::pin(async move {
            self.admit(agent)?;
            let crate::agent::ContextAction::Restore { at, mode, expected } = action else {
                return Ok(None);
            };
            if *at >= agent.conversation_tail {
                return Ok(Some("restore cut is not a prior settled turn".into()));
            }
            let saved = match super::history::read(self.stream.as_ref(), &agent.path, *at).await {
                Ok(saved) => saved,
                Err(Error::Invalid(reason)) => return Ok(Some(reason)),
                Err(error) => return Err(error),
            };
            if !mode.files() {
                return Ok((!expected.is_empty())
                    .then(|| "context-only restore carries project heads".into()));
            }
            let projects = match self
                .project_selection(agent, VolumeOperation::Write, Some(&saved.volumes))
                .await
            {
                Ok(projects) => projects,
                Err(Error::Unauthorized(reason)) => return Ok(Some(reason)),
                Err(error) => return Err(error),
            };
            if projects.len() != expected.len() || projects.len() != saved.volumes.len() {
                return Ok(Some("restore must cover every project binding".into()));
            }
            for head in projects {
                if expected.iter().filter(|value| **value == head).count() != 1
                    || saved
                        .volumes
                        .iter()
                        .filter(|value| value.binding.id == head.binding.id)
                        .count()
                        != 1
                {
                    return Ok(Some("restore expected project heads changed".into()));
                }
            }
            Ok(None)
        })
    }

    fn open<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        inbox: u64,
        cursor: u64,
        input: &'a ModelContent,
        stop: acyclic_actors::guest::CancellationToken,
    ) -> BoxFuture<'a, Result<OpenTurn>> {
        Box::pin(crate::core::until_turn_stopped(
            Some(stop.clone()),
            async move {
                // The admission is verified before the turn's first read.
                #[cfg(feature = "test-support")]
                if let Some(fault) = &self.binding_fault {
                    fault(agent)?;
                }
                let admitted = self.admit(agent)?;
                // The model context holds every settled turn of the agent's
                // conversation, inherited ones included: a spawned child is a
                // clone of its parent at the fork cut.
                let history = self.history(&admitted, agent).await?;
                // The turn's grant is derived from its context, before the turn
                // can observe anything: every file the context can reach.
                let mut context = TurnContext::new(agent, history, turn, input)?;
                // An open turn pins its original delta, including instructions
                // from sources no longer bound after a remove or re-root. Admit
                // those exact files before constructing its reader.
                let pinned_announcements =
                    crate::agent::turn_announcements(agent).map_err(Error::from)?;
                // Steering transcripts pin references admitted after the original
                // turn opened. Read their manifests under exact grants, then include
                // those references in this turn's ordinary authority derivation.
                let transcripts: Vec<FileRef> = context
                    .settled
                    .iter()
                    .filter_map(|settled| settled.steering.clone())
                    .collect();
                if !transcripts.is_empty() {
                    let authority = crate::core::until_turn_stopped(
                        Some(stop.clone()),
                        self.admit(agent)?.authority(&transcripts),
                    )
                    .await?;
                    let reader = FilesystemContentVerifier::new(
                        Arc::clone(&self.host),
                        &authority.verifier,
                        &authority.scope,
                        crate::conversation::MAX_EXACT_JS_INTEGER,
                        None,
                    )?;
                    for settled in &mut context.settled {
                        let Some(file) = &settled.steering else {
                            continue;
                        };
                        let bytes = reader
                            .read(file, file.descriptor().byte_length(), None, None)?
                            .await?;
                        settled.steered = crate::contract::json_from_slice(&bytes)
                            .map_err(|error| Error::Invalid(error.to_string()))?;
                    }
                }
                let retained: Vec<FileRef> = context
                    .settled
                    .iter()
                    .flat_map(|settled| &settled.announcements)
                    .flat_map(crate::agent::AnnouncedDocument::files)
                    .chain(
                        pinned_announcements
                            .iter()
                            .flatten()
                            .flat_map(crate::agent::AnnouncedDocument::files),
                    )
                    .cloned()
                    .collect();
                let external: Vec<FileRef> = context
                    .files()
                    .chain(
                        agent
                            .profile
                            .as_ref()
                            .and_then(|creation| creation.template.as_ref()),
                    )
                    .chain(
                        agent
                            .profile
                            .as_ref()
                            .and_then(|creation| creation.continuation.as_ref())
                            .and_then(|continuation| continuation.override_item.as_ref()),
                    )
                    .cloned()
                    .collect();
                let (journal, bindings, files) = self
                    .journal(
                        admitted,
                        &external,
                        &retained,
                        Some(&mut context),
                        Some(stop.clone()),
                    )
                    .await?;
                let input =
                    std::mem::replace(&mut context.input, ModelContent::Text(String::new()));
                let input = &input;
                // Compare with the documents actually retained by this history
                // selection. Losing their turn or compacting starts a new base;
                // retained messages are immutable. Pin the complete delta once
                // so a resumed turn sends the request it originally opened.
                use crate::agent::{
                    AnnouncedDocument, AnnouncementDocument as _, AnnouncementKind,
                };
                let announcements = match pinned_announcements {
                    Some(kept) => kept,
                    None => {
                        let roster = self.executors.roster(agent).filter(|roster| {
                            !context.contains_announcement(
                                AnnouncementKind::Targets,
                                roster.canonical().1,
                            )
                        });
                        let seen = context
                            .instruction_entries(bindings.journal.reader.as_ref())
                            .await?;
                        let update = self
                            .executors
                            .workspaces(
                                agent,
                                &seen,
                                &bindings.content,
                                &bindings.issuer,
                                &bindings.scope,
                            )
                            .await?
                            .filter(|update| {
                                !context.contains_announcement(
                                    AnnouncementKind::Workspaces,
                                    update.document.canonical().1,
                                ) || !update.instructions.is_empty()
                            });
                        let mut changed = Vec::new();
                        if roster.is_some() || update.is_some() {
                            let announcement_files = &bindings.journal.files;
                            if let Some(roster) = roster {
                                let (bytes, digest) = roster.canonical();
                                changed.push(AnnouncedDocument {
                                    kind: AnnouncementKind::Targets,
                                    digest,
                                    file: announcement_files
                                        .stage_announcement(
                                            AnnouncementKind::Targets,
                                            digest,
                                            bytes,
                                        )
                                        .await?,
                                    instructions: None,
                                });
                            }
                            if let Some(update) = update {
                                let (bytes, digest) = update.document.canonical();
                                changed.push(AnnouncedDocument {
                                    kind: AnnouncementKind::Workspaces,
                                    digest,
                                    file: announcement_files
                                        .stage_announcement(
                                            AnnouncementKind::Workspaces,
                                            digest,
                                            bytes,
                                        )
                                        .await?,
                                    instructions: announcement_files
                                        .stage_instructions(
                                            update.instructions,
                                            bindings.content.reader.as_ref(),
                                        )
                                        .await?,
                                });
                            }
                        }
                        changed
                    }
                };
                let segment = crate::agent::turn_segment(agent).map_err(Error::from)?;
                let effects = Arc::new(FilesystemTurnEffects::new(
                    Arc::clone(&bindings.journal.files),
                    Arc::clone(&self.stream),
                    self.within.clone(),
                    agent.path.clone(),
                    (turn, segment),
                    self.limits.maximum_bytes,
                )) as Arc<dyn crate::agent::TurnEffects>;
                let children = Arc::new(FilesystemTurnChildren {
                    files: Arc::clone(&files),
                    stream: Arc::clone(&self.stream),
                    within: self.within.clone(),
                    host: Arc::clone(&self.host),
                    issuer: self.issuer.clone(),
                    admission: self.admission.clone(),
                    agent: agent.path.clone(),
                    volumes: agent.volumes.clone(),
                    maximum_bytes: self.limits.maximum_bytes,
                }) as Arc<dyn crate::agent::TurnChildren>;
                let goal = match self.goals {
                    Some(policy) => FilesystemTurnGoal::bind(
                        &self.host,
                        agent,
                        &bindings.issuer.verifier(&mut None)?,
                        &bindings.scope,
                        self.limits.maximum_bytes,
                        policy,
                    )?
                    .map(|goal| Arc::new(goal) as Arc<dyn crate::agent::TurnGoal>),
                    None => None,
                };
                let (policy, resolved) = self
                    .approvals(agent, bindings.journal.reader.as_ref())
                    .await?;
                let outputs = Arc::new(AgentTurnOutputs {
                    ui: Arc::clone(&files),
                    journal: Arc::clone(&bindings.journal.files),
                });
                let boundary = super::agent_steering::FilesystemBoundary::new(
                    Arc::clone(&self.stream),
                    Arc::clone(&self.host),
                    self.issuer.clone(),
                    self.admission.clone(),
                    agent.clone(),
                    turn,
                    inbox,
                    Arc::clone(&files),
                    Arc::clone(bindings.content.writer.as_ref().ok_or_else(|| {
                        Error::Invalid("agent turn has no private writer".into())
                    })?),
                    self.executors.content_limits().render_bytes,
                );
                // Each call dispatches only once the effects of the turn's earlier
                // calls committed, so it reaches what they created, and a `wait`
                // only once its children settled.
                let approvals = Arc::new(
                    AgentApprovals::new(files, Arc::new(policy)).yielding(Arc::clone(&effects)),
                );
                let journal = journal
                    .with_model_boundary(Arc::new(boundary))
                    .with_stop(stop)
                    .with_agent_approvals(Arc::clone(&approvals));
                // Only this agent's selected turns have a journal on its private
                // volume; an inherited ancestor turn is not a local predecessor.
                let preceding_turn = context
                    .settled
                    .get(context.fork.unwrap_or(0)..)
                    .and_then(|turns| turns.last())
                    .map(|turn| turn.turn);
                let journal = journal.with_preceding_turn(preceding_turn).await?;
                let selected = context
                    .select(
                        &*self.stream,
                        agent,
                        &*bindings.content.reader,
                        bindings.journal.reader.as_ref(),
                        input,
                        &announcements,
                        self.executors.content_limits(),
                    )
                    .await?;
                // The original selection validator requires the current input to
                // equal its final message. A waking Notice carries the whole frozen
                // event block after coalescing, not just the event that woke it.
                let input = selected
                    .messages
                    .last()
                    .ok_or_else(|| Error::Invalid("selected turn has no current input".into()))?
                    .content
                    .clone();
                let journal = journal.with_bound_selection(selected.clone());
                let journal: Arc<dyn crate::executor::ExecutionJournal> = Arc::new(journal);
                let mut authority = TurnAuthority {
                    templates: self.templates(),
                    issuer: bindings.issuer,
                    scope: bindings.scope,
                    grants: bindings.grants,
                    content: bindings.content,
                    approval: Arc::new(resolved.bind()?),
                    effects: Some(Arc::clone(&effects)),
                    journal: Arc::clone(&journal),
                    children: Some(children),
                    workspace_tools: None,
                };
                if let Some(factory) = &self.workspace_tools {
                    authority.workspace_tools = Some(factory(agent, &authority)?);
                }
                Ok(OpenTurn {
                    executor: self
                        .executors
                        .executor(agent, turn, &input, &authority)
                        .await?,
                    journal,
                    custody: Some(authority.issuer),
                    selected: Some(selected),
                    approvals: Some(approvals),
                    effects: Some(effects),
                    goal,
                    outputs,
                    input: Some(input),
                    announcements,
                })
            },
        ))
    }
}

/// What one agent turn's `wait` observes: each child's status and, once it
/// settled, its last answer, read under a grant of exactly that output.
struct FilesystemTurnChildren<P: ?Sized, A: 'static, O: 'static> {
    files: Arc<AgentInternalFiles<A, O>>,
    stream: Arc<P>,
    within: acyclic_actors::addressing::ActorReference,
    host: Arc<FilesystemHost<A, O>>,
    issuer: RuntimeAuthority,
    admission: TurnAdmission,
    agent: AgentPath,
    volumes: Vec<WorkspaceEntry>,
    maximum_bytes: Option<u64>,
}

impl<P, A, O> crate::agent::TurnChildren for FilesystemTurnChildren<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn agent(&self) -> &AgentPath {
        &self.agent
    }

    fn sent_reply<'a>(
        &'a self,
        id: &'a crate::agent::IntentId,
    ) -> BoxFuture<'a, Result<Option<crate::agent::SentReply>>> {
        Box::pin(async move {
            crate::agent::sent_reply(&self.stream, &self.within, &self.agent, id)
                .await
                .map_err(Error::from)
        })
    }

    fn peer_admission<'a>(
        &'a self,
        grants: &'a crate::Capabilities,
        recipient: &'a AgentPath,
    ) -> BoxFuture<'a, Result<Option<crate::agent::PeerAdmission>>> {
        Box::pin(async move {
            crate::agent::session_peer_admission(&*self.stream, grants, &self.agent, recipient)
                .await
                .map_err(Error::from)
        })
    }

    fn closed(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let path = self.agent.inbox().map_err(Error::from)?;
            // Archive's close marker and inbox delivery commit together. Watch
            // the delivery, so active work stops before resource retirement.
            let tail = self.stream.tail(path.clone()).await?;
            match self
                .stream
                .tail(self.agent.closed().map_err(Error::from)?)
                .await
            {
                Ok(_) => return Ok(()),
                Err(error) if error.category() == acyclic_stream::StreamError::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            let mut records = acyclic_stream::records_only(
                self.stream
                    .follow(acyclic_stream::FollowRequest::new(path, tail))
                    .await?,
            );
            while let Some(record) = records.try_next().await? {
                let record =
                    crate::agent::InboxRecord::decode(&record.value).map_err(Error::from)?;
                if matches!(record, crate::agent::InboxRecord::Archive(_)) {
                    return Ok(());
                }
            }
            Err(acyclic_stream::StreamError::Unavailable.into())
        })
    }

    fn select<'a>(
        &'a self,
        operation: OperationId,
        references: &'a [String],
    ) -> BoxFuture<'a, Result<Vec<(String, AgentPath)>>> {
        Box::pin(async move {
            use super::agent_internal::InternalFileCreation;
            let open = |bytes: &[u8]| -> Result<Vec<(String, AgentPath)>> {
                let selected: Vec<(Option<String>, [u8; 16])> = crate::agent::records::open(bytes)
                    .map_err(|_| Error::Storage("invalid wait selection".into()))?;
                selected
                    .into_iter()
                    .map(|(reference, id)| {
                        let child = self
                            .agent
                            .child(&crate::agent::AgentId::from_bytes(id))
                            .map_err(Error::from)?;
                        Ok((reference.unwrap_or_else(|| child.as_str().into()), child))
                    })
                    .collect()
            };
            let path = format!("waits/v1/{operation}.msgpack");
            let vacant = match self.files.prepare_create(&path, self.maximum_bytes).await? {
                InternalFileCreation::Existing(bytes) => {
                    return open(&bytes);
                }
                InternalFileCreation::Vacant(file) => file,
            };
            let selected = crate::agent::select_children(self, references).await?;
            // All selections are direct children. Derive their ancestry and any
            // path reference from the owner instead of repeating it per child.
            let compact: Vec<_> = selected
                .iter()
                .map(|(reference, child)| {
                    (
                        (reference != child.as_str()).then_some(reference),
                        child.id_bytes(),
                    )
                })
                .collect();
            let bytes = crate::agent::records::seal(&compact)?;
            match vacant.create(&bytes).await? {
                None => Ok(selected),
                Some(bytes) => open(&bytes),
            }
        })
    }

    fn resolve<'a>(
        &'a self,
        parent: &'a AgentPath,
        name: &'a str,
    ) -> BoxFuture<'a, Result<crate::agent::SpawnResolution>> {
        Box::pin(async move {
            crate::agent::resolve_spawn_name(&self.stream, &self.within, parent, name)
                .await
                .map_err(Error::from)
        })
    }

    fn settled<'a>(
        &'a self,
        child: &'a AgentPath,
    ) -> BoxFuture<'a, Result<crate::agent::Settlement>> {
        Box::pin(async move {
            match crate::agent::settled(&self.stream, &self.within, child).await {
                Ok(settled) => Ok(settled),
                Err(crate::agent::Error::NoSuchAgent) => {
                    Err(Error::NotFound(child.as_str().to_owned()))
                }
                Err(error) => Err(Error::from(error)),
            }
        })
    }

    fn diagnostic<'a>(
        &'a self,
        agent: &'a AgentPath,
        digest: [u8; 32],
        from: u64,
        maximum_bytes: u64,
        io: Option<&'a crate::conversation::ContentIo>,
    ) -> BoxFuture<
        'a,
        Result<(
            crate::agent::Diagnostic,
            crate::agent::DiagnosticRead,
            Option<crate::conversation::ContentHold<'a>>,
        )>,
    > {
        Box::pin(async move {
            if agent != &self.agent && !agent.is_child_of(&self.agent) {
                return Err(Error::Unauthorized(
                    "diagnostic query is outside the original agent relationship".into(),
                ));
            }
            // This observation proves that the complete immutable prefix was
            // published; staged/unpublished bytes are never a readable result.
            let selected = self.settled(agent).await?;
            let crate::agent::Phase::Done {
                outcome: crate::agent::AgentOutcome::FailedDiagnostic { diagnostic },
            } = selected.phase
            else {
                return Err(Error::NotFound("published diagnostic".into()));
            };
            if diagnostic.agent != *agent || diagnostic.digest != digest {
                return Err(Error::NotFound(
                    "diagnostic identity differs from the published result".into(),
                ));
            }
            let mut range = diagnostic.range(from, maximum_bytes);
            let capacity = range.capacity();
            let mut hold = io
                .map(|io| io.reserve_decode(capacity as u64))
                .transpose()?;
            if let Some(io) = io {
                io.allocation_attempt(capacity as u64)?;
            }
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(capacity)
                .map_err(|_| Error::Storage("diagnostic page allocation failed".into()))?;
            if let (Some(io), Some(hold)) = (io, hold.as_mut()) {
                if bytes.capacity() > capacity {
                    hold.absorb(io.reserve_decode((bytes.capacity() - capacity) as u64)?)?;
                }
            }
            if range.records() != 0 {
                let request = range.request().map_err(Error::from)?;
                let mut observer = io.map(crate::filesystem::PreparationIo::history_io);
                let mut work = observer
                    .as_mut()
                    .map(|work| work as &mut dyn crate::store::HistoryIo);
                let mut records = crate::store::operations::read_records(
                    &self.stream,
                    &request.path,
                    request.from,
                    request.limit,
                    &mut work,
                )
                .await??;
                while !range.complete() {
                    let record = records
                        .next(&mut work)
                        .await?
                        .ok_or_else(|| Error::Storage("diagnostic prefix is incomplete".into()))?;
                    let slice = range.take_record(&record).map_err(Error::from)?;
                    if let Some(io) = io {
                        io.charged(acyclic_fs::WorkCounters {
                            bytes_copied: slice.len() as u64,
                            ..Default::default()
                        })?;
                    }
                    bytes.extend_from_slice(slice);
                }
            }
            let page = range.finish(bytes).map_err(Error::from)?;
            Ok((diagnostic, page, hold))
        })
    }

    fn answer<'a>(
        &'a self,
        child: &'a AgentPath,
        sequence: u64,
    ) -> BoxFuture<'a, Result<(String, crate::agent::TurnOutcome)>> {
        Box::pin(async move {
            let entry = crate::agent::answer(&*self.stream, child, sequence)
                .await
                .map_err(Error::from)?;
            let outcome = entry.outcome;
            let external = [entry.output];
            let authority = agent_authority(
                &self.issuer,
                &self.admission,
                &self.agent,
                &self.volumes,
                &external,
            )
            .await?;
            let reader = FilesystemContentVerifier::new(
                Arc::clone(&self.host),
                &authority.verifier,
                &authority.scope,
                self.maximum_bytes,
                None,
            )?;
            let bytes = reader
                .read(
                    &external[0],
                    external[0].descriptor().byte_length(),
                    None,
                    None,
                )?
                .await?;
            let output: TurnOutput = serde_json::from_slice(&bytes)
                .map_err(|_| Error::Invalid("staged turn output is malformed".into()))?;
            Ok((output.text, outcome))
        })
    }
}
