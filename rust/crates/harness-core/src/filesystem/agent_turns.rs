//! Agent turns on the filesystem: each agent's journal is a conversation-scoped
//! authority over the agent's private volume; its executor comes from the host's
//! [`TurnExecutors`] for that agent and authority, per turn.
//!
//! The model context of a turn is the host's system prompt, then the agent's
//! own conversation stream: the last settled turns as user/assistant messages,
//! then the turn's input. The host binds exactly that selection into the turn's journal,
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
        VolumeOwner,
    },
    core::{AggregateKind, Authority, AuthorityRoot, AuthorityVerifier, Scope},
    executor::{Executor, TurnOutput},
    model::{ModelContent, ModelMessage, ModelRole},
    projection::SelectedModelContext,
    runtime::ContentBindings,
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::{BoxProviderFuture as BoxFuture, ReadRequest, StreamError, StreamProvider};
use futures::TryStreamExt;
use std::sync::Arc;

/// Name of the volume binding that holds an agent's journal and staged turn output.
pub const PRIVATE_VOLUME: &str = "private";

/// The authority of one agent turn: the agent's conversation issuer, its scope
/// over every bound volume, and the agent's content bound to that scope.
pub struct TurnAuthority {
    /// Signing custody; it must outlive the turn's journal.
    pub issuer: AuthorityRoot,
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
}

/// The conversation authority of one agent over its bound volumes.
pub struct AgentAuthority<'v> {
    /// `issuer`'s custody under the agent's conversation audience.
    pub issuer: AuthorityRoot,
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
    pub(super) fn derive(
        &self,
        issuer: &AuthorityRoot,
        path: &AgentPath,
    ) -> Result<(AuthorityRoot, Option<Scope>)> {
        let owner = crate::AgentId::from_bytes(path.id().to_bytes());
        match self {
            Self::LocalRoot => Ok((
                issuer.with_audience(Authority {
                    kind: AggregateKind::Conversation,
                    id: path.id().as_str().to_owned(),
                }),
                None,
            )),
            Self::Admitted { audience, scope } => {
                if scope.agent().is_some_and(|agent| agent != owner) {
                    return Err(Error::Unauthorized(
                        "admitted scope belongs to another agent".into(),
                    ));
                }
                let issuer = issuer.with_audience(audience.clone());
                let delegated = issuer.issue_derived(
                    scope,
                    "agent",
                    scope.capabilities(),
                    Some(owner),
                    &mut None,
                )?;
                Ok((issuer, Some(delegated)))
            }
        }
    }
}

/// An agent's admission, verified before any of its IO: its private volume
/// is its own, and under [`TurnAdmission::Admitted`] the admitted scope is
/// authentic, live, bound to this audience and agent, and grants the private
/// volume's write, and the agent's scope is already delegated from it. Every
/// read of a turn and its [`AgentAuthority`] require one.
pub(super) struct AdmittedAgent<'v> {
    issuer: AuthorityRoot,
    owner: crate::AgentId,
    volumes: &'v [VolumeBinding],
    private: &'v VolumeBinding,
    /// The agent's scope delegated from its admission, if admitted.
    delegated: Option<Scope>,
}

impl<'v> AdmittedAgent<'v> {
    /// Verifies the admission of the agent at `path` over `volumes`, signed
    /// with `issuer`'s custody. It performs no IO.
    pub(super) fn verify(
        issuer: &AuthorityRoot,
        admission: &TurnAdmission,
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
        let (issuer, delegated) = admission.derive(issuer, path)?;
        if let Some(scope) = &delegated {
            // File authority always names the committed private binding.
            crate::conversation::ContentGrant::verify(
                &issuer.verifier(&mut None)?,
                scope,
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
            delegated,
        })
    }

    /// The agent's authority over its bound volumes. Under a local root, each
    /// `external` file on a volume the agent does not bind (a message or
    /// result from another agent) is granted alone, by its exact version; an
    /// admitted agent reads what its admission grants.
    pub(super) fn authority(self, external: &[FileRef]) -> Result<AgentAuthority<'v>> {
        let Self {
            issuer,
            owner,
            volumes,
            private,
            delegated,
        } = self;
        let (grants, scope) = match delegated {
            Some(scope) => (scope.capabilities().clone(), scope),
            None => {
                let grants = Capabilities::new(root_grants(volumes, external)?);
                let scope = issuer.bootstrap_scope("agent", &grants, Some(owner), &mut None)?;
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
        }
    }
    Ok(grants)
}

/// The authority of the agent at `path` over `volumes`, signed with
/// `issuer`'s custody under `admission`: [`AdmittedAgent::verify`], then its
/// authority over `external` files.
pub fn agent_authority<'v>(
    issuer: &AuthorityRoot,
    admission: &TurnAdmission,
    path: &AgentPath,
    volumes: &'v [VolumeBinding],
    external: &[FileRef],
) -> Result<AgentAuthority<'v>> {
    AdmittedAgent::verify(issuer, admission, path, volumes)?.authority(external)
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
    issuer: AuthorityRoot,
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
    issuer: AuthorityRoot,
    scope: Scope,
    grants: Capabilities,
    content: ContentBindings,
}

impl<P, A, O> FilesystemTurnHost<P, A, O>
where
    P: StreamProvider + ?Sized + 'static,
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
        issuer: AuthorityRoot,
        admission: TurnAdmission,
        executors: Arc<dyn TurnExecutors>,
        limits: TurnLimits,
        policy: crate::approvals::ApprovalPolicy,
    ) -> Self {
        Self {
            stream,
            host,
            issuer,
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
                    .read(file, self.limits.maximum_bytes, None, None)?
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
        let (_, bindings, files) = self.approval_journal(agent)?;
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
        let (_, bindings, files) = self.approval_journal(agent)?;
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
        let (_, bindings, files) = self.approval_journal(agent)?;
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
    fn approval_journal(
        &self,
        agent: &AgentState,
    ) -> Result<(
        FilesystemExecutionJournal<P, A, O>,
        Bindings,
        Arc<AgentInternalFiles<A, O>>,
    )> {
        self.journal(self.admit(agent)?, agent.policy.as_slice())
    }

    /// `agent`'s verified admission; it performs no IO.
    fn admit<'v>(&'v self, agent: &'v AgentState) -> Result<AdmittedAgent<'v>> {
        AdmittedAgent::verify(&self.issuer, &self.admission, &agent.path, &agent.volumes)
    }

    /// The journal and authority of `agent`'s turns. Each `external` input
    /// file on a volume the agent does not bind (a message or result from
    /// another agent, current or in history) is granted alone, by its exact
    /// version.
    fn journal(
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
        } = admitted.authority(external)?;
        let reader = FilesystemContentVerifier::new(
            Arc::clone(&self.host),
            &verifier,
            &scope,
            self.limits.maximum_bytes,
            None,
        )?;
        let writer = FilesystemContentPublisher::new(
            Arc::clone(&self.host),
            &private.volume,
            &verifier,
            &scope,
            self.limits.maximum_bytes,
            None,
        )?;
        let journal = FilesystemExecutionJournal::new(
            Arc::clone(&self.stream),
            Arc::clone(&self.host),
            FilesystemJournalBinding {
                volume: &private.volume,
                verifier: &verifier,
                scope: &scope,
                maximum_bytes: self.limits.maximum_bytes,
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
        let from = tail.saturating_sub(u64::from(self.limits.history_turns));
        if from == tail {
            return Ok(Vec::new());
        }
        let path = agent.path.conversation().map_err(agent_error)?;
        let records = match self
            .stream
            .read(ReadRequest {
                path,
                from,
                limit: u32::try_from(tail - from).unwrap_or(u32::MAX),
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
    settled: Vec<(OperationId, ModelContent, FileRef)>,
    turn: OperationId,
    input: &'a ModelContent,
    policy: Option<&'a FileRef>,
}

impl<'a> TurnContext<'a> {
    /// The context of `agent`'s `turn` over its settled `history`.
    fn new(
        agent: &'a AgentState,
        prompt: Option<&'a str>,
        history: Vec<ConversationEntry>,
        turn: OperationId,
        input: &'a ModelContent,
    ) -> Result<Self> {
        let settled = history
            .into_iter()
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
        self.settled
            .iter()
            .flat_map(|(_, input, output)| input.file_refs().chain(std::iter::once(output)))
            .chain(self.input.file_refs())
            .chain(self.policy)
    }

    /// The selection: one user and one assistant message per settled turn,
    /// assistant text read from its staged output through `reader`, which
    /// holds [`Self::files`]. Without a prompt or history there is none.
    async fn select(
        self,
        agent: &AgentState,
        reader: &dyn ContentResidencyVerifier,
        maximum_bytes: u64,
    ) -> Result<Option<SelectedModelContext>> {
        let Self {
            prompt,
            settled,
            turn,
            input,
            ..
        } = self;
        if settled.is_empty() && prompt.is_none() {
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
        for (settled, content, output) in settled {
            let bytes = reader.read(&output, maximum_bytes, None, None)?.await?;
            let output: TurnOutput = serde_json::from_slice(&bytes)
                .map_err(|_| Error::Invalid("staged turn output is malformed".into()))?;
            messages.push(ModelMessage {
                role: ModelRole::User,
                content,
            });
            message_ids.push(message_id(settled, ModelRole::User));
            messages.push(ModelMessage {
                role: ModelRole::Assistant,
                content: ModelContent::Text(output.text),
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

impl<P, A, O> TurnHost for FilesystemTurnHost<P, A, O>
where
    P: StreamProvider + ?Sized + 'static,
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
            let external: Vec<FileRef> = context.files().cloned().collect();
            let (journal, bindings, files) = self.journal(admitted, &external)?;
            // Every file the context reaches is readable under the turn's
            // grant, by the one shared read check, before the turn runs: a
            // missing grant fails here whichever input arrived first, not on
            // whichever path happens to read the file.
            let verifier = bindings.issuer.verifier(&mut None)?;
            for file in &external {
                crate::conversation::ContentGrant::verify_read(&verifier, &bindings.scope, file)?;
            }
            let effects = Some(Arc::new(FilesystemTurnEffects::new(
                Arc::clone(&files),
                Arc::clone(&self.stream),
                agent.path.clone(),
                turn,
                self.limits.maximum_bytes,
            )) as Arc<dyn crate::agent::TurnEffects>);
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
            let authority = TurnAuthority {
                issuer: bindings.issuer,
                scope: bindings.scope,
                grants: bindings.grants,
                content: bindings.content,
                approval: Arc::new(resolved.bind()?),
                effects: effects.clone(),
            };
            let outputs = Arc::clone(&files);
            let approvals = Arc::new(AgentApprovals::new(files, Arc::new(policy)));
            let journal = journal
                .with_stop(stop)
                .with_agent_approvals(Arc::clone(&approvals));
            let selected = context
                .select(agent, &*authority.content.reader, self.limits.maximum_bytes)
                .await?;
            let journal = match &selected {
                Some(selected) => journal.with_bound_selection(selected.clone()),
                None => journal,
            };
            Ok(OpenTurn {
                executor: self
                    .executors
                    .executor(agent, turn, input, &authority)
                    .await?,
                journal: Arc::new(journal),
                max_steps: self.limits.max_steps,
                custody: Some(authority.issuer),
                selected,
                approvals: Some(approvals),
                effects,
                goal,
                outputs,
            })
        })
    }
}
