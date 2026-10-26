//! Agent turns on the filesystem: each agent's journal is a conversation-scoped
//! authority over the agent's private volume; its executor comes from the host's
//! [`TurnExecutors`] for that agent and authority, per turn.
//!
//! The model context of a turn is the agent's own conversation stream: the last
//! settled turns (inherited ones included) as user/assistant messages, then the
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
        AgentPath, AgentRecord, AgentState, ConversationEntry, OpenTurn, TurnHost, TurnStop,
        VolumeBinding,
    },
    conversation::{FileRef, ModelContextSelection, VolumeClass, VolumeOperation, VolumeOwner},
    core::{AggregateKind, Authority, AuthorityRoot, AuthorityVerifier, SchemaRegistry, Scope},
    executor::{ExecutionJournal, Executor, TurnOutput},
    model::{ModelContent, ModelContentPart, ModelMessage, ModelRole},
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
    /// Approval gate of the agent's policy file; without one every tool runs.
    pub approval: Option<Arc<dyn crate::runtime::ToolPolicy>>,
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
        let (issuer, delegated) = match admission {
            TurnAdmission::LocalRoot => (
                issuer.with_audience(Authority {
                    kind: AggregateKind::Conversation,
                    id: path.id().as_str().to_owned(),
                }),
                None,
            ),
            TurnAdmission::Admitted { audience, scope } => {
                if scope.agent().is_some_and(|agent| agent != owner) {
                    return Err(Error::Unauthorized(
                        "admitted scope belongs to another agent".into(),
                    ));
                }
                let issuer = issuer.with_audience(audience.clone());
                // The agent holds exactly its admission's grants, so it can
                // never widen them, and whatever volume, directory or file the
                // admission grants stays readable.
                let delegated = issuer.issue_derived(
                    scope,
                    "agent",
                    scope.capabilities(),
                    Some(owner),
                    &mut None,
                )?;
                // The same check the turn's private writer makes: its exact or
                // structured grants admit writing the private volume.
                crate::conversation::ContentGrant::verify(
                    &issuer.verifier(&mut None)?,
                    &delegated,
                    &private.volume,
                    VolumeOperation::Write,
                    u64::MAX,
                    &mut None,
                )?;
                (issuer, Some(delegated))
            }
        };
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
pub struct FilesystemTurnHost<P, A: 'static, O: 'static> {
    stream: Arc<P>,
    host: Arc<FilesystemHost<A, O>>,
    issuer: AuthorityRoot,
    admission: TurnAdmission,
    executors: Arc<dyn TurnExecutors>,
    schemas: SchemaRegistry,
    limits: TurnLimits,
    approvers: crate::approvals::ApproverModels,
    goals: Option<crate::goal::GoalContinuationPolicy>,
}

impl<P, A, O> FilesystemTurnHost<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Host signing each agent's authority with `issuer`'s custody, derived
    /// from `admission`.
    #[must_use]
    pub fn new(
        stream: Arc<P>,
        host: Arc<FilesystemHost<A, O>>,
        issuer: AuthorityRoot,
        admission: TurnAdmission,
        executors: Arc<dyn TurnExecutors>,
        limits: TurnLimits,
    ) -> Self {
        Self {
            stream,
            host,
            issuer,
            admission,
            executors,
            schemas: SchemaRegistry::new(),
            limits,
            approvers: Default::default(),
            goals: Some(Default::default()),
        }
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
    ) -> Result<(AgentApprovalPolicy, Option<ApprovalPolicy>)> {
        let policy = match &agent.policy {
            Some(file) => {
                let bytes = content
                    .reader
                    .read(file, self.limits.maximum_bytes, None, None)?
                    .await?;
                Some(
                    crate::contract::json_from_slice::<ApprovalPolicy>(&bytes)
                        .map_err(|error| Error::Invalid(error.to_string()))?,
                )
            }
            None => None,
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
        let (_, authority, files) = self.journal(self.admit(agent)?, &[])?;
        let (policy, _) = self.approvals(agent, &authority.content).await?;
        crate::approvals::decide(&*files, &policy, id, responder, approved, reason).await
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
        TurnAuthority,
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
            &self.schemas,
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
            TurnAuthority {
                issuer,
                scope,
                grants: capabilities,
                content: ContentBindings {
                    reader,
                    writer: Some(writer),
                },
                approval: None,
                effects: None,
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

/// The files a turn input's model content names.
fn input_files(content: &ModelContent) -> Vec<FileRef> {
    let file = |part: &ModelContentPart| match part {
        ModelContentPart::File { file, .. } => Some(file.clone()),
        _ => None,
    };
    match content {
        ModelContent::Part(part) => file(part).into_iter().collect(),
        ModelContent::Parts(parts) => parts.iter().filter_map(file).collect(),
        ModelContent::Text(_) => Vec::new(),
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

/// The selection of `history` plus the current `input`: one user and one
/// assistant message per settled turn, assistant text read from its staged output.
async fn select(
    agent: &AgentState,
    journal: &dyn ExecutionJournal,
    history: Vec<ConversationEntry>,
    turn: OperationId,
    input: &ModelContent,
) -> Result<Option<SelectedModelContext>> {
    if history.is_empty() {
        return Ok(None);
    }
    let mut messages = Vec::with_capacity(2 * history.len() + 1);
    let mut message_ids = Vec::with_capacity(messages.capacity());
    for entry in history {
        let settled = crate::agent::turn_id(agent, &entry.input).map_err(agent_error)?;
        let bytes = journal.load(&entry.output, None)?.await?;
        let output: TurnOutput = serde_json::from_slice(&bytes)
            .map_err(|_| Error::Invalid("staged turn output is malformed".into()))?;
        messages.push(ModelMessage {
            role: ModelRole::User,
            content: crate::agent::model_input(&entry.input).map_err(agent_error)?,
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

impl<P, A, O> TurnHost for FilesystemTurnHost<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn open<'a>(
        &'a self,
        agent: &'a AgentState,
        turn: OperationId,
        input: &'a ModelContent,
        stop: Arc<TurnStop>,
    ) -> BoxFuture<'a, Result<OpenTurn>> {
        Box::pin(async move {
            // The admission is verified before the turn's first read.
            let admitted = self.admit(agent)?;
            // An inherited turn staged its output on an ancestor's private
            // volume, which this agent's grants do not reach; the model
            // context holds this agent's own settled turns.
            let own = agent
                .volumes
                .iter()
                .find(|binding| binding.name == PRIVATE_VOLUME)
                .map(|binding| &binding.volume);
            let history: Vec<ConversationEntry> = self
                .history(&admitted, agent)
                .await?
                .into_iter()
                .filter(|entry| Some(entry.output.volume()) == own)
                .collect();
            // Every input the model context names, retained history included,
            // stays readable by its exact version.
            let mut external = input_files(input);
            for entry in &history {
                let content = crate::agent::model_input(&entry.input).map_err(agent_error)?;
                external.extend(input_files(&content));
            }
            let (journal, mut authority, files) = self.journal(admitted, &external)?;
            authority.effects = Some(Arc::new(FilesystemTurnEffects::new(
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
                    &authority.issuer.verifier(&mut None)?,
                    &authority.scope,
                    self.limits.maximum_bytes,
                    policy,
                )?
                .map(|goal| Arc::new(goal) as Arc<dyn crate::agent::TurnGoal>),
                None => None,
            };
            let (policy, configured) = self.approvals(agent, &authority.content).await?;
            authority.approval = configured
                .map(|policy| {
                    policy
                        .bind()
                        .map(|gate| Arc::new(gate) as Arc<dyn crate::runtime::ToolPolicy>)
                })
                .transpose()?;
            let approvals = Arc::new(AgentApprovals::new(files, Arc::new(policy)));
            let effects = authority.effects.clone();
            let journal = journal
                .with_stop(stop)
                .with_agent_approvals(Arc::clone(&approvals));
            let selected = select(agent, &journal, history, turn, input).await?;
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
            })
        })
    }
}
