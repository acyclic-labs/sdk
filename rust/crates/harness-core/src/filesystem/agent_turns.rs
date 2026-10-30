//! Agent turns on the filesystem: each agent's journal is a conversation-scoped
//! authority over the agent's private volume; its executor comes from the host's
//! [`TurnExecutors`] for that agent and authority, per turn.
//!
//! The model context of a turn is the host's system prompt, then the agent's
//! own conversation stream from its latest checkpoint: that checkpoint's
//! summary, the turns settled after it as user/assistant messages, then the
//! turn's input. The host binds exactly that selection into the turn's journal,
//! so the executor can dispatch no other history.

use super::{
    FilesystemContentPublisher, FilesystemContentVerifier, FilesystemExecutionJournal,
    FilesystemHost, FilesystemJournalBinding, agent_effects::FilesystemTurnEffects,
    agent_goal::FilesystemTurnGoal, agent_internal::AgentInternalFiles,
};
use crate::approvals::{AgentApprovalPolicy, AgentApprovals, ApprovalPolicy};
use crate::{
    Capabilities, Error, OperationId, Result,
    agent::{
        AgentPath, AgentRecord, AgentState, ConversationEntry, OpenTurn, TurnHost, VolumeBinding,
    },
    conversation::{
        ContentResidencyVerifier, FileRef, ModelContextSelection, VolumeClass, VolumeOperation,
        VolumeOwner, VolumeRef,
    },
    core::{AggregateKind, Authority, AuthorityVerifier, RuntimeAuthority, Scope},
    executor::{Executor, TurnOutput},
    model::{ModelContent, ModelMessage, ModelRole},
    projection::SelectedModelContext,
    resources::ProviderRef,
    runtime::ContentBindings,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{BoxProviderFuture as BoxFuture, ReadRequest, StreamError, StreamProvider};
use futures::TryStreamExt;
use std::sync::Arc;

/// Name of the volume binding that holds an agent's journal and staged turn output.
pub const PRIVATE_VOLUME: &str = "private";

/// The private volume of `agent`, which every portable host creates and binds
/// as [`PRIVATE_VOLUME`]. A platform selecting an Actor-hosted agent's volume
/// for its Machine passes the manifest's volume provider and
/// [`AgentId::hosted_root`](crate::agent::AgentId::hosted_root) of the actor id.
pub fn private_volume(provider: ProviderRef, agent: &crate::agent::AgentId) -> Result<VolumeRef> {
    VolumeRef::new(
        provider,
        PRIVATE_VOLUME.to_owned(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(crate::AgentId::from_bytes(agent.to_bytes())),
        &mut None,
    )
}

#[cfg(test)]
#[path = "agent_turns/admission_tests.rs"]
mod admission_tests;

/// The authority of one agent turn: the agent's conversation issuer, its scope
/// over every bound volume, and the agent's content bound to that scope.
pub struct TurnAuthority {
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
    pub private: &'v VolumeBinding,
}

/// What every agent scope of a host derives from.
#[derive(Clone, Debug)]
pub enum TurnAdmission {
    /// The trusted local runtime root: each agent's scope is a root grant over
    /// its bound volumes, under the agent's conversation audience.
    LocalRoot,
    /// A workload scope admitted under `audience`. Each agent's scope is
    /// delegated from it, keeping its audience, agent and lifetime, and grants
    /// only what it admits: a read-only admission cannot write.
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
        let owner = crate::AgentId::from_bytes(path.id().to_bytes());
        match self {
            Self::LocalRoot => issuer.with_audience(Authority {
                kind: AggregateKind::Conversation,
                id: path.id().as_str().to_owned(),
            }),
            Self::Admitted { audience, scope } => {
                if scope.agent().is_some_and(|agent| agent != owner) {
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
    volumes: &'v [VolumeBinding],
    private: &'v VolumeBinding,
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
        volumes: &'v [VolumeBinding],
    ) -> Result<Self> {
        let owner = crate::AgentId::from_bytes(path.id().to_bytes());
        let private = volumes
            .iter()
            .find(|binding| binding.name == PRIVATE_VOLUME)
            .ok_or_else(|| Error::Invalid("agent has no private volume".into()))?;
        if private.volume.class() != VolumeClass::AgentPrivate
            || *private.volume.owner() != VolumeOwner::Agent(owner)
        {
            return Err(Error::Unauthorized(
                "private volume belongs to another agent".into(),
            ));
        }
        let issuer = admission.verify(issuer, path)?;
        let parent = match admission {
            TurnAdmission::LocalRoot => None,
            TurnAdmission::Admitted { scope, .. } => Some(scope),
        };
        if let Some(scope) = parent {
            // File authority always names the committed private binding.
            crate::conversation::ContentGrant::authorize_agent_volume(
                &issuer.verifier(&mut None)?,
                scope,
                owner,
                &private.volume,
                VolumeOperation::Write,
                u64::MAX,
                &mut None,
            )?;
            require_structured_write(scope, owner, &private.volume)?;
        }
        Ok(Self {
            issuer,
            owner,
            volumes,
            private,
            parent,
        })
    }

    /// The agent's authority over its bound volumes. Under a local root, each
    /// `external` file on a volume the agent does not bind (a message or
    /// result from another agent) is granted alone, by its exact version; an
    /// admitted agent reads what its admission grants.
    pub(super) async fn authority(self, external: &[FileRef]) -> Result<AgentAuthority<'v>> {
        let Self {
            issuer,
            owner,
            volumes,
            private,
            parent,
        } = self;
        let (grants, scope) = match parent {
            Some(parent) => {
                let scope = issuer
                    .issue(Some(parent), "agent", parent.capabilities(), Some(owner))
                    .await?;
                (scope.capabilities().clone(), scope)
            }
            None => {
                let grants = Capabilities::new(root_grants(volumes, external)?);
                let scope = issuer.issue(None, "agent", &grants, Some(owner)).await?;
                (grants, scope)
            }
        };
        let verifier = issuer.verifier(&mut None)?;
        Ok(AgentAuthority {
            issuer,
            scope,
            grants,
            verifier,
            private,
        })
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
                u64::MAX,
                &mut None,
            )?;
            require_structured_write(scope, self.owner, volume)?;
        } else {
            volume.require_writer(Some(self.owner), &None)?;
        }
        Ok(())
    }
}

/// The filesystem's decision over `scope`'s structured grant, if it has one:
/// it creates and writes every path of `volume`. It performs no IO; the
/// writer still applies the volume's persisted policy.
pub(super) fn require_structured_write(
    scope: &Scope,
    owner: crate::AgentId,
    volume: &crate::conversation::VolumeRef,
) -> Result<()> {
    use acyclic_fs::access::{
        Action, Capability, Context, INITIAL_POLICY, PathTrie, Policy, Principal, PrincipalKind,
        Resource,
    };
    let Some(label) = crate::core::filesystem_scope_label(scope.capabilities(), &mut None)? else {
        return Ok(());
    };
    let denied = || Error::Unauthorized("admission does not grant writing the volume".into());
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
    if view.allows_every_path(&[Action::Create, Action::Write], now) {
        Ok(())
    } else {
        Err(denied())
    }
}

/// A local root agent's grants: routing, its agent tools, read and write of
/// every bound volume, and each `external` file by its exact version.
fn root_grants(volumes: &[VolumeBinding], external: &[FileRef]) -> Result<Vec<String>> {
    let mut grants = Vec::with_capacity(2 * volumes.len() + 1);
    grants.push(crate::contract::capability::INTERACTION_ROUTE.to_owned());
    // Every agent may spawn, message and archive within its own subtree.
    grants.extend(crate::tools::AGENT_TOOLS.map(crate::contract::capability::tool_call));
    for binding in volumes {
        grants.push(
            binding
                .volume
                .capability(VolumeOperation::Read, &mut None)?,
        );
        grants.push(
            binding
                .volume
                .capability(VolumeOperation::Write, &mut None)?,
        );
    }
    for file in external {
        if !volumes
            .iter()
            .any(|binding| binding.volume == *file.volume())
        {
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
    volumes: &'v [VolumeBinding],
    external: &[FileRef],
) -> Result<AgentAuthority<'v>> {
    AdmittedAgent::verify(issuer, admission, path, volumes)?
        .authority(external)
        .await
}

/// Builds the executor of one agent turn: its tools, context, policy, limits
/// and compaction, under the turn's [`TurnAuthority`]. It may stage files (a
/// context manifest) through the authority's private writer.
pub trait TurnExecutors: acyclic_stream::ProviderPlatform {
    /// Executor for `agent`'s turn `turn` on `input`.
    fn executor<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        authority: &'a TurnAuthority,
    ) -> BoxFuture<'a, Result<Arc<dyn Executor>>>;
}

/// Bounds of one agent turn.
#[derive(Clone, Copy, Debug)]
pub struct TurnLimits {
    /// Journal bytes per turn.
    pub maximum_bytes: u64,
    /// Model and tool steps per turn.
    pub max_steps: u32,
    /// Settled turns of history in the model context.
    pub history_turns: u32,
}

/// [`TurnHost`] binding every agent to its executor and a journal on its
/// private volume, under a conversation authority named by the agent's id.
pub struct FilesystemTurnHost<P: ?Sized, A: 'static, O: 'static> {
    stream: Arc<P>,
    host: Arc<FilesystemHost<A, O>>,
    issuer: RuntimeAuthority,
    admission: TurnAdmission,
    executors: Arc<dyn TurnExecutors>,
    limits: TurnLimits,
    approvers: crate::approvals::ApproverModels,
    goals: Option<crate::goal::GoalContinuationPolicy>,
    prompt: Option<Arc<str>>,
    policy: crate::approvals::ApprovalPolicy,
}

/// A turn's bindings before its approval gate is resolved.
struct Bindings {
    issuer: RuntimeAuthority,
    scope: Scope,
    grants: Capabilities,
    content: ContentBindings,
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
    #[must_use]
    pub fn new(
        stream: Arc<P>,
        host: Arc<FilesystemHost<A, O>>,
        issuer: impl Into<RuntimeAuthority>,
        admission: TurnAdmission,
        executors: Arc<dyn TurnExecutors>,
        limits: TurnLimits,
        policy: crate::approvals::ApprovalPolicy,
    ) -> Self {
        Self {
            stream,
            host,
            issuer: issuer.into(),
            admission,
            executors,
            limits,
            approvers: Default::default(),
            goals: Some(Default::default()),
            prompt: None,
            policy,
        }
    }

    /// Opens every turn's model context with `prompt` as its system message;
    /// `None` sends none.
    #[must_use]
    pub fn with_prompt(mut self, prompt: Option<&str>) -> Self {
        self.prompt = prompt.map(Arc::from);
        self
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
        content: &ContentBindings,
    ) -> Result<(AgentApprovalPolicy, ApprovalPolicy)> {
        let policy = match &agent.policy {
            Some(file) => {
                let bytes = content
                    .reader
                    .read(file, file.descriptor().byte_length(), None, None)?
                    .await?;
                crate::contract::json_from_slice::<ApprovalPolicy>(&bytes)
                    .map_err(|error| Error::Invalid(error.to_string()))?
            }
            None => self.policy.clone(),
        };
        let limits = crate::conversation::Limits {
            file_bytes: self.limits.maximum_bytes,
            ..crate::conversation::Limits::default()
        };
        let bound = AgentApprovalPolicy::new(
            policy.clone(),
            agent.parent.as_ref().map(crate::agent::AgentPath::id),
            self.approvers.clone(),
            limits,
        )?;
        Ok((bound, policy))
    }

    /// `responder` decides approval `id` of `agent`. On success the caller
    /// appends `InboxRecord::Approval { id }` to the agent's inbox to resume it.
    pub async fn decide(
        &self,
        agent: &AgentState,
        id: &str,
        responder: crate::approvals::ApprovalResponder<'_>,
        approved: bool,
        reason: Option<String>,
    ) -> Result<()> {
        let (_, bindings, files) = self.approval_journal(agent).await?;
        let (policy, _) = self.approvals(agent, &bindings.content).await?;
        crate::approvals::decide(&*files, &policy, id, responder, approved, reason).await
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
        approved: bool,
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
        self.decide(&state, request.as_str(), responder, approved, reason)
            .await
            .map_err(|error| AgentError::Turn(Box::new(error)))?;
        let wake = InboxRecord::Approval(Approval {
            id: request.clone(),
        });
        agents.send(agent, &from, &wake).await
    }

    /// The approval `agent`'s turn waits on, read from its request file.
    pub async fn pending_approval(
        &self,
        agent: &AgentState,
    ) -> Result<Option<(crate::agent::IntentId, crate::interaction::Interaction)>> {
        let crate::agent::Phase::WaitingApproval { id } = &agent.phase else {
            return Ok(None);
        };
        let (_, bindings, files) = self.approval_journal(agent).await?;
        let (policy, _) = self.approvals(agent, &bindings.content).await?;
        let bytes = policy.request(&*files, id.as_str()).await?;
        let request = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(Some((id.clone(), request)))
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
        let (policy, _) = self.approvals(agent, &bindings.content).await?;
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
        Bindings,
        Arc<AgentInternalFiles<A, O>>,
    )> {
        self.journal(self.admit(agent)?, agent.policy.as_slice())
            .await
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
    ) -> Result<(
        FilesystemExecutionJournal<P, A, O>,
        Bindings,
        Arc<AgentInternalFiles<A, O>>,
    )> {
        let AgentAuthority {
            issuer,
            scope,
            grants: capabilities,
            verifier,
            private,
        } = admitted.authority(external).await?;
        // The turn's reader and writer take no content bound of their own:
        // each caller states its bound, and harness-owned records (manifests,
        // settled outputs, policies) are read at their pinned length, so a
        // configured file bound limits admitted content, never the turn.
        let reader = FilesystemContentVerifier::new(
            Arc::clone(&self.host),
            &verifier,
            &scope,
            crate::conversation::MAX_EXACT_JS_INTEGER,
            None,
        )?;
        let writer = FilesystemContentPublisher::new(
            Arc::clone(&self.host),
            &private.volume,
            &verifier,
            &scope,
            crate::conversation::MAX_EXACT_JS_INTEGER,
            None,
        )?;
        let journal = FilesystemExecutionJournal::new(
            Arc::clone(&self.stream),
            Arc::clone(&self.host),
            FilesystemJournalBinding {
                volume: &private.volume,
                verifier: &verifier,
                scope: &scope,
                io: None,
            },
        )?
        .with_input_verifier(reader.clone());
        let internal = AgentInternalFiles::new(
            Arc::clone(&self.host),
            private.volume.clone(),
            crate::conversation::ContentGrant::verify(
                &verifier,
                &scope,
                &private.volume,
                VolumeOperation::Write,
                u64::MAX,
                &mut None,
            )?,
        );
        Ok((
            journal,
            Bindings {
                issuer,
                scope,
                grants: capabilities,
                content: ContentBindings {
                    reader,
                    writer: Some(writer),
                },
            },
            Arc::new(internal),
        ))
    }

    /// The agent's last settled turns, oldest first.
    async fn history(
        &self,
        _: &AdmittedAgent<'_>,
        agent: &AgentState,
    ) -> Result<Vec<ConversationEntry>> {
        let tail = agent.conversation_tail;
        // Nothing before the latest checkpoint is context: read from it, or
        // the bounded window when it is further back, plus that one entry
        // read by its indexed sequence.
        let window = tail.saturating_sub(u64::from(self.limits.history_turns));
        let from = agent.checkpoint.map_or(window, |at| at.max(window));
        let mut entries = match agent.checkpoint.filter(|at| *at < from) {
            Some(at) => self.entries(agent, at, 1).await?,
            None => Vec::new(),
        };
        entries.extend(self.entries(agent, from, tail - from).await?);
        Ok(entries)
    }

    /// `count` conversation entries of `agent` from sequence `from`.
    async fn entries(
        &self,
        agent: &AgentState,
        from: u64,
        count: u64,
    ) -> Result<Vec<ConversationEntry>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let path = agent.path.conversation().map_err(agent_error)?;
        let records = match self
            .stream
            .read(ReadRequest {
                key: None,
                path,
                from,
                limit: u32::try_from(count).unwrap_or(u32::MAX),
                at_least: Default::default(),
            })
            .await
        {
            Ok(records) => records.try_collect::<Vec<_>>().await?,
            Err(StreamError::NotFound) => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        records
            .iter()
            .map(|record| ConversationEntry::decode(&record.value).map_err(agent_error))
            .collect()
    }
}

fn agent_error(error: crate::agent::Error) -> Error {
    Error::Invalid(error.to_string())
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

/// Stable message identity of the system `prompt`.
fn prompt_id(prompt: &str) -> uuid::Uuid {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.agent.prompt.v1");
    hasher.update(prompt.as_bytes());
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
    prompt: Option<&'a str>,
    /// The latest checkpoint: its turn and its staged summary.
    checkpoint: Option<(OperationId, FileRef)>,
    settled: Vec<(OperationId, ModelContent, FileRef)>,
    turn: OperationId,
    input: &'a ModelContent,
    policy: Option<&'a FileRef>,
}

impl<'a> TurnContext<'a> {
    /// The context of `agent`'s `turn` over its settled `history`: from its
    /// latest checkpoint on, which replaces every entry before it.
    fn new(
        agent: &'a AgentState,
        prompt: Option<&'a str>,
        mut history: Vec<ConversationEntry>,
        turn: OperationId,
        input: &'a ModelContent,
    ) -> Result<Self> {
        let checkpoint = match history
            .iter()
            .rposition(|entry| entry.checkpoint().is_some())
        {
            Some(at) => {
                let after = history.split_off(at + 1);
                let entry = std::mem::replace(&mut history, after)
                    .pop()
                    .ok_or_else(|| Error::Invalid("checkpoint entry is absent".into()))?;
                Some((
                    crate::agent::turn_id(agent, &entry.input).map_err(agent_error)?,
                    entry.output,
                ))
            }
            None => None,
        };
        // A cancelled checkpoint selects nothing and is no turn.
        let settled = history
            .into_iter()
            .filter(|entry| entry.input.context().is_none())
            .map(|entry| {
                Ok((
                    crate::agent::turn_id(agent, &entry.input).map_err(agent_error)?,
                    crate::agent::model_input(&entry.input).map_err(agent_error)?,
                    entry.output,
                ))
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            prompt,
            checkpoint,
            settled,
            turn,
            input,
            // A spawned agent runs under the approval policy it inherited,
            // staged on an ancestor's private volume.
            policy: agent.policy.as_ref(),
        })
    }

    /// Every file the turn can reach, each by its exact version: the files
    /// each message names, each settled output (an inherited turn's is staged
    /// on an ancestor's private volume, a child's result on the child's), and
    /// the approval policy.
    fn files(&self) -> impl Iterator<Item = &FileRef> {
        self.checkpoint
            .iter()
            .map(|(_, summary)| summary)
            .chain(
                self.settled.iter().flat_map(|(_, input, output)| {
                    input.file_refs().chain(std::iter::once(output))
                }),
            )
            .chain(self.input.file_refs())
            .chain(self.policy)
    }

    /// The selection: the checkpoint's summary as one assistant message
    /// (none when it is empty), then one user and one assistant message per
    /// settled turn, assistant text read from its staged output through
    /// `reader`, which holds [`Self::files`]. Without a prompt or history
    /// there is none.
    async fn select(
        self,
        agent: &AgentState,
        reader: &dyn ContentResidencyVerifier,
        input: &ModelContent,
    ) -> Result<Option<SelectedModelContext>> {
        let Self {
            prompt,
            checkpoint,
            settled,
            turn,
            ..
        } = self;
        let summary = match checkpoint {
            Some((checkpoint, output)) => Some((checkpoint, staged_text(reader, &output).await?))
                .filter(|(_, summary)| !summary.is_empty()),
            None => None,
        };
        if settled.is_empty() && summary.is_none() && prompt.is_none() {
            return Ok(None);
        }
        let mut messages = Vec::with_capacity(2 * settled.len() + 2);
        let mut message_ids = Vec::with_capacity(messages.capacity());
        if let Some(prompt) = prompt {
            messages.push(ModelMessage {
                role: ModelRole::System,
                content: ModelContent::Text(prompt.to_owned()),
            });
            message_ids.push(prompt_id(prompt));
        }
        if let Some((checkpoint, summary)) = summary {
            messages.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text(summary),
            });
            message_ids.push(message_id(checkpoint, ModelRole::Assistant));
        }
        for (settled, content, output) in settled {
            let text = staged_text(reader, &output).await?;
            messages.push(ModelMessage {
                role: ModelRole::User,
                content: crate::agent::expand_attachments(content, reader).await?,
            });
            message_ids.push(message_id(settled, ModelRole::User));
            messages.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text(text),
            });
            message_ids.push(message_id(settled, ModelRole::Assistant));
        }
        messages.push(ModelMessage {
            role: ModelRole::User,
            content: input.clone(),
        });
        message_ids.push(message_id(turn, ModelRole::User));
        Ok(Some(SelectedModelContext {
            selection: ModelContextSelection {
                conversation_revision: agent.conversation_tail + 1,
                message_ids,
                checkpoint: None,
            },
            messages,
        }))
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

impl<P, A, O> TurnHost for FilesystemTurnHost<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn open<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        stop: acyclic_actors::guest::CancellationToken,
    ) -> BoxFuture<'a, Result<OpenTurn>> {
        Box::pin(async move {
            // The admission is verified before the turn's first read.
            let admitted = self.admit(agent)?;
            // The model context holds every settled turn of the agent's
            // conversation, inherited ones included: a spawned child is a
            // clone of its parent at the fork cut.
            let history = self.history(&admitted, agent).await?;
            // The turn's grant is derived from its context, before the turn
            // can observe anything: every file the context can reach.
            let context = TurnContext::new(agent, self.prompt.as_deref(), history, turn, input)?;
            let checkpoint = context
                .checkpoint
                .as_ref()
                .map(|(_, output)| output.clone());
            let external: Vec<FileRef> = context.files().cloned().collect();
            let (journal, bindings, files) = crate::core::until_turn_stopped(
                Some(stop.clone()),
                self.journal(admitted, &external),
            )
            .await?;
            // Every file the context reaches is readable under the turn's
            // grant, by the one shared read check, before the turn runs: a
            // missing grant fails here whichever input arrived first, not on
            // whichever path happens to read the file.
            let verifier = bindings.issuer.verifier(&mut None)?;
            for file in &external {
                crate::conversation::ContentGrant::verify_read(&verifier, &bindings.scope, file)?;
            }
            // The executor runs the input with its attachments listed, read
            // under the grant above at the manifest's pinned length.
            let input =
                &crate::agent::expand_attachments(input.clone(), &*bindings.content.reader).await?;
            let segment = crate::agent::turn_segment(agent).map_err(agent_error)?;
            let effects = Arc::new(FilesystemTurnEffects::new(
                Arc::clone(&files),
                Arc::clone(&self.stream),
                agent.path.clone(),
                (turn, segment),
                self.limits.maximum_bytes,
            )) as Arc<dyn crate::agent::TurnEffects>;
            let children = Arc::new(FilesystemTurnChildren {
                stream: Arc::clone(&self.stream),
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
            let (policy, resolved) = self.approvals(agent, &bindings.content).await?;
            let outputs = Arc::clone(&files);
            // Each call dispatches only once the effects of the turn's earlier
            // calls committed, so it reaches what they created, and a `wait`
            // only once its children settled.
            let approvals = Arc::new(
                AgentApprovals::new(files, Arc::new(policy))
                    .yielding(Arc::clone(&effects))
                    .awaiting(Arc::clone(&children)),
            );
            let journal = journal
                .with_stop(stop)
                .with_agent_approvals(Arc::clone(&approvals));
            let selected = context
                .select(agent, &*bindings.content.reader, input)
                .await?;
            let journal = match &selected {
                Some(selected) => journal.with_bound_selection(selected.clone()),
                None => journal,
            };
            let journal: Arc<dyn crate::executor::ExecutionJournal> = Arc::new(journal);
            let authority = TurnAuthority {
                issuer: bindings.issuer,
                scope: bindings.scope,
                grants: bindings.grants,
                content: bindings.content,
                approval: Arc::new(resolved.bind()?),
                effects: Some(Arc::clone(&effects)),
                journal: Arc::clone(&journal),
                children: Some(children),
            };
            Ok(OpenTurn {
                executor: self
                    .executors
                    .executor(agent, turn, input, &authority)
                    .await?,
                journal,
                max_steps: self.limits.max_steps,
                custody: Some(authority.issuer),
                selected,
                checkpoint,
                approvals: Some(approvals),
                effects: Some(effects),
                goal,
                outputs,
                input: Some(input.clone()),
            })
        })
    }
}

/// What one agent turn's `wait` observes: each child's status and, once it
/// settled, its last answer, read under a grant of exactly that output.
struct FilesystemTurnChildren<P: ?Sized, A: 'static, O: 'static> {
    stream: Arc<P>,
    host: Arc<FilesystemHost<A, O>>,
    issuer: RuntimeAuthority,
    admission: TurnAdmission,
    agent: AgentPath,
    volumes: Vec<VolumeBinding>,
    maximum_bytes: u64,
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

    fn settled<'a>(
        &'a self,
        child: &'a AgentPath,
    ) -> BoxFuture<'a, Result<Option<crate::agent::Phase>>> {
        Box::pin(async move {
            match crate::agent::settled(&*self.stream, child).await {
                Ok(settled) => Ok(settled),
                Err(crate::agent::Error::NoSuchAgent) => {
                    Err(Error::NotFound(child.as_str().to_owned()))
                }
                Err(error) => Err(agent_error(error)),
            }
        })
    }

    fn answer<'a>(&'a self, child: &'a AgentPath) -> BoxFuture<'a, Result<Option<String>>> {
        Box::pin(async move {
            let Some(entry) = crate::agent::answer(&*self.stream, child)
                .await
                .map_err(agent_error)?
            else {
                return Ok(None);
            };
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
                .read(&external[0], self.maximum_bytes, None, None)?
                .await?;
            let output: TurnOutput = serde_json::from_slice(&bytes)
                .map_err(|_| Error::Invalid("staged turn output is malformed".into()))?;
            Ok(Some(output.text))
        })
    }
}
