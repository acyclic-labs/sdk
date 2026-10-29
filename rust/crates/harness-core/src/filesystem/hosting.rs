//! The admitted root of one hosted agent tree: the association of a host's
//! signing custody, the admission its platform verified for the caller, and
//! the one placement its agents and volumes are both built on, made once and
//! used for every root the caller creates. The same [`Bound`] serves local, self-hosted and managed
//! hosting; only how the platform verifies its caller differs.
//!
//! A root's fresh volumes and its adoption are one commit:
//! [`Bound::prepare_root`] verifies every authority the root needs before any
//! IO, then prepares that commit, and [`Bound::submit_root`] submits it. The
//! [`Prepared`] commit owns its fresh volumes' publication holds until it
//! settles, so no unpublished object it names is collected before then.

use super::{
    FilesystemContentPublisher, FilesystemHost, FilesystemSpawnVolumes, FilesystemTurnHost,
    TurnAdmission, TurnExecutors, TurnLimits, agent_authority,
};
use crate::{
    OperationId,
    agent::{
        AgentHandler, AgentHost, AgentId, AgentPath, Delivered, Error, ExecutorDriver, ForkAdopted,
        IntentId, Sender, Spawn, VolumeBinding,
    },
    approvals::ApprovalResponder,
    conversation::{ContentAccess, ContentPublisher, FileRef},
    core::{Authority, AuthorityRoot, RuntimeAuthority, Scope},
    interaction::{ApprovalBinding, Interaction},
    resources::ProviderRef,
};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::Clock};
use acyclic_fs::{
    AsyncObjectStore, CancellationToken, CreateSpec, CreationOperation, FilesystemCommit,
    FilesystemCommitOutcome, Fs, StreamAuthorityStore, WorkBudget, WorkspaceName,
    distributed::AuthorityProvider,
};
use std::sync::Arc;

type Host<P, O> = FilesystemHost<StreamAuthorityStore<P>, O>;
/// The turn host of a binding's agents.
pub type Turner<P, O> = FilesystemTurnHost<P, StreamAuthorityStore<P>, O>;

#[cfg(all(target_os = "linux", feature = "native-execution"))]
mod guest;
#[cfg(all(target_os = "linux", feature = "native-execution"))]
pub use guest::{GuestAuthority, GuestMappingSource, LocalGuestEnrollment};

/// The caller a host's platform verified for one request, before any of the
/// request's IO: the audience its scope was issued for, and the scope itself.
/// Only the platform constructs one, from its own verified credential; no
/// request field, guest output or account string is a caller.
#[derive(Clone, Debug)]
pub struct VerifiedCaller {
    /// The audience the caller's scope was issued for.
    pub audience: Authority,
    /// The caller's original verified scope.
    pub scope: Scope,
}

/// One root agent a caller creates: the volumes created with it and the
/// existing volumes it binds, which stay references and are never written by
/// the creation.
#[derive(Clone, Debug)]
pub struct RootRequest {
    /// The root agent.
    pub agent: AgentId,
    /// Volumes created, empty, in the root's own commit; the agent's private
    /// volume is one of them.
    pub fresh: Vec<VolumeBinding>,
    /// Existing volumes the root binds as they are.
    pub retained: Vec<VolumeBinding>,
    /// Task brief, already staged, if any.
    pub brief: Option<FileRef>,
    /// Approval policy file, if any.
    pub policy: Option<FileRef>,
}

/// A prepared root: the one sealed commit creating its fresh volumes, its
/// inbox with `ForkAdopted`, its actor state and its activation, together
/// with the holds keeping the fresh volumes' objects until it settles. Only
/// [`Bound::submit_root`] submits it, consuming it; an uncertain submission
/// hands it back to resubmit exactly.
pub struct Prepared {
    child: AgentPath,
    commit: FilesystemCommit,
}

impl Prepared {
    /// The root agent's path.
    #[must_use]
    pub const fn child(&self) -> &AgentPath {
        &self.child
    }

    /// The sealed creation's exact bytes.
    #[must_use]
    pub fn request(&self) -> &acyclic_stream::CommitRequest {
        self.commit.request()
    }
}

impl std::fmt::Debug for Prepared {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Prepared")
            .field("child", &self.child)
            .finish_non_exhaustive()
    }
}

/// How one root submission ended.
#[derive(Debug)]
pub enum RootSubmission {
    /// The root exists and is hosted.
    Created(AgentPath),
    /// The result is unknown: resubmit exactly this commit, which still holds
    /// its fresh volumes' objects.
    Unknown(Prepared),
}

/// The one admitted stream instance and its placement. Agents, their actor
/// state and their volumes' authority all live on `stream`, so a root's
/// volumes and adoption, and every spawn, commit together.
pub struct Placement<P: ?Sized + 'static, O: 'static> {
    /// The canonical filesystem; its authority provider also hosts the agents.
    pub filesystem: Fs<StreamAuthorityStore<P>, O>,
    /// The filesystem provider every volume of this placement names.
    pub provider: ProviderRef,
    /// Account and namespace of the agents' actors. The binding's runtime
    /// claims only these, so each tree served on a shared stream needs its
    /// own namespace, and one binding serves all of its roots' trees.
    pub within: ActorReference,
}

/// How hosted agents run their turns.
pub struct Turns {
    /// Each turn's executor.
    pub executors: Arc<dyn TurnExecutors>,
    /// Bounds of every turn.
    pub limits: TurnLimits,
    /// Bounds of every agent actor.
    pub actors: ActorLimits,
    /// Alarm clock.
    pub clock: Arc<dyn Clock>,
    /// The approval policy of every agent with no policy file of its own:
    /// the host's preset ([`crate::preset::Preset::approval_policy`]).
    pub approval_policy: crate::approvals::ApprovalPolicy,
}

/// A host's custody, a caller's admission and the one placement they act on,
/// bound once. Every root, turn and spawn of this binding uses this placement.
pub struct Bound<P: AuthorityProvider + ?Sized + 'static, O: 'static> {
    issuer: RuntimeAuthority,
    admission: TurnAdmission,
    within: ActorReference,
    maximum_bytes: u64,
    agents: Arc<AgentHost<P>>,
    turns: Arc<Turner<P, O>>,
    volumes: Arc<Host<P, O>>,
    clock: Arc<dyn Clock>,
    handler: Arc<dyn acyclic_actors::guest::Handler>,
    spawns: Arc<FilesystemSpawnVolumes<P, O>>,
}

impl<P: AuthorityProvider + ?Sized + 'static, O> Clone for Bound<P, O> {
    fn clone(&self) -> Self {
        Self {
            issuer: self.issuer.clone(),
            admission: self.admission.clone(),
            within: self.within.clone(),
            maximum_bytes: self.maximum_bytes,
            agents: self.agents.clone(),
            turns: self.turns.clone(),
            volumes: self.volumes.clone(),
            clock: self.clock.clone(),
            handler: self.handler.clone(),
            spawns: self.spawns.clone(),
        }
    }
}

impl<P: AuthorityProvider + ?Sized + 'static, O> Bound<P, O> {
    /// The host's canonical filesystem facade, for content observation under
    /// the same admission as its turns.
    #[must_use]
    pub const fn volumes(&self) -> &Arc<Host<P, O>> {
        &self.volumes
    }
}

impl<P, O> Bound<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Binds `caller`, as the host's platform verified it, to the host's
    /// signing custody `issuer` over `placement`.
    ///
    /// # Errors
    /// When the placement's provider or limits are invalid.
    pub fn admitted(
        issuer: AuthorityRoot,
        caller: VerifiedCaller,
        placement: Placement<P, O>,
        turns: Turns,
    ) -> Result<Self, Error> {
        Self::new(
            issuer.into(),
            TurnAdmission::Admitted {
                audience: caller.audience,
                scope: caller.scope,
            },
            placement,
            turns,
        )
    }

    /// Bind the platform's opaque admitted invocation once. The turn host and
    /// selected-volume factory retain this same owner; no request supplies a
    /// replacement caller, audience or signing root.
    pub fn platform_admitted(
        authority: RuntimeAuthority,
        placement: Placement<P, O>,
        turns: Turns,
    ) -> Result<Self, Error> {
        authority.verifier(&mut None).map_err(turn)?;
        let (audience, scope) = authority.caller().ok_or_else(|| {
            turn(crate::Error::Unauthorized(
                "platform admission is required".into(),
            ))
        })?;
        let admission = TurnAdmission::Admitted {
            audience: audience.clone(),
            scope: scope.clone(),
        };
        Self::new(authority, admission, placement, turns)
    }

    /// [`Self::admitted`] over the host's own composed `volumes`, as
    /// [`Self::local_host`] is to [`Self::local_root`]: a self-hosted
    /// endpoint serves its harness's agents to its verified caller exactly as
    /// a managed host does. `configure` sets the host's turn options.
    ///
    /// # Errors
    /// As [`Self::admitted`].
    pub fn admitted_host(
        issuer: AuthorityRoot,
        caller: VerifiedCaller,
        volumes: Arc<FilesystemHost<StreamAuthorityStore<P>, O>>,
        within: ActorReference,
        turns: Turns,
        cuts: Option<Arc<dyn super::ForkCuts>>,
        configure: impl FnOnce(Turner<P, O>) -> Turner<P, O>,
    ) -> Result<Self, Error> {
        Self::on(
            issuer.into(),
            TurnAdmission::Admitted {
                audience: caller.audience,
                scope: caller.scope,
            },
            volumes,
            within,
            (turns, cuts),
            configure,
        )
    }

    /// The explicit self-host enrollment: the host's own custody is the root
    /// of every agent it creates. Never selected by a request.
    ///
    /// # Errors
    /// As [`Self::admitted`].
    pub fn local_root(
        issuer: AuthorityRoot,
        placement: Placement<P, O>,
        turns: Turns,
    ) -> Result<Self, Error> {
        Self::new(issuer.into(), TurnAdmission::LocalRoot, placement, turns)
    }

    /// The self-host enrollment over the host's own composed `volumes`, as
    /// [`Self::local_root`]: the local harness, whose volumes carry its native
    /// state, serves through the same binding. `configure` sets the host's
    /// turn options (approval models, goals, system prompt).
    ///
    /// # Errors
    /// As [`Self::admitted`].
    pub fn local_host(
        issuer: AuthorityRoot,
        volumes: Arc<FilesystemHost<StreamAuthorityStore<P>, O>>,
        within: ActorReference,
        turns: Turns,
        cuts: Option<Arc<dyn super::ForkCuts>>,
        configure: impl FnOnce(Turner<P, O>) -> Turner<P, O>,
    ) -> Result<Self, Error> {
        Self::on(
            issuer.into(),
            TurnAdmission::LocalRoot,
            volumes,
            within,
            (turns, cuts),
            configure,
        )
    }

    fn new(
        issuer: RuntimeAuthority,
        admission: TurnAdmission,
        placement: Placement<P, O>,
        turns: Turns,
    ) -> Result<Self, Error> {
        let Placement {
            filesystem,
            provider,
            within,
        } = placement;
        let volumes = FilesystemHost::new(filesystem, &provider, None).map_err(turn)?;
        Self::on(
            issuer,
            admission,
            volumes,
            within,
            (turns, None),
            |turner| turner,
        )
    }

    fn on(
        issuer: RuntimeAuthority,
        admission: TurnAdmission,
        volumes: Arc<Host<P, O>>,
        within: ActorReference,
        (turns, cuts): (Turns, Option<Arc<dyn super::ForkCuts>>),
        configure: impl FnOnce(Turner<P, O>) -> Turner<P, O>,
    ) -> Result<Self, Error> {
        let stream = volumes.filesystem.stream_provider();
        let maximum_bytes = turns.limits.maximum_bytes;
        // The preset's policy binds before any agent runs under it.
        turns.approval_policy.bind().map_err(turn)?;
        let turner = Arc::new(configure(FilesystemTurnHost::new(
            Arc::clone(&stream),
            Arc::clone(&volumes),
            issuer.clone(),
            admission.clone(),
            turns.executors,
            turns.limits,
            turns.approval_policy,
        )));
        let driver = ExecutorDriver::new(Arc::clone(&turner));
        // A host keeping state beside its volumes (a lazy checkout) chooses
        // each spawn's fork cut, so a child's project is a related fork.
        let spawns = FilesystemSpawnVolumes::new(Arc::clone(&volumes));
        let spawns = Arc::new(match cuts {
            Some(cuts) => spawns.with_fork_cuts(cuts),
            None => spawns,
        });
        let handler = Arc::new(
            AgentHandler::new(crate::agent::actor_store(&stream)?, driver, turns.actors)
                .map_err(|_| Error::Record("agent handler manifest"))?
                .with_spawn_volumes(spawns.clone()),
        );
        let agents = Arc::new(AgentHost::hosting(
            stream,
            within.clone(),
            handler.clone(),
            turns.clock.clone(),
            Some(spawns.clone()),
        )?);
        Ok(Self {
            issuer,
            admission,
            within,
            maximum_bytes,
            agents,
            turns: turner,
            volumes,
            clock: turns.clock,
            handler,
            spawns,
        })
    }

    /// Runs deliveries through the host-injected guest boundary. Admission,
    /// turn composition, root publication and spawn participants remain those
    /// of this binding; the guest owns only delivery execution.
    pub fn hosting(
        mut self,
        handler: Arc<dyn acyclic_actors::guest::Handler>,
    ) -> Result<Self, Error> {
        // The guest composes the same spawns as this binding's own handler.
        self.agents = Arc::new(AgentHost::hosting(
            self.agents.provider().clone(),
            self.within.clone(),
            handler.clone(),
            self.clock.clone(),
            Some(self.spawns.clone()),
        )?);
        self.handler = handler;
        Ok(self)
    }

    /// The handler composed from this binding's preset and exact admission.
    /// A guest boundary may wrap it before passing it to [`Self::hosting`].
    #[must_use]
    pub fn handler(&self) -> Arc<dyn acyclic_actors::guest::Handler> {
        self.handler.clone()
    }

    /// The admission every agent of this binding derives its scope from, for
    /// its turns ([`super::FilesystemTurnHost`]) and spawns
    /// ([`super::FilesystemSpawnVolumes`]).
    #[must_use]
    pub const fn admission(&self) -> &TurnAdmission {
        &self.admission
    }

    /// The signing root every agent credential of this binding's tree is
    /// issued under.
    #[must_use]
    pub const fn issuer(&self) -> &RuntimeAuthority {
        &self.issuer
    }

    /// The agents this binding creates and hosts.
    #[must_use]
    pub const fn agents(&self) -> &Arc<AgentHost<P>> {
        &self.agents
    }

    /// The turns of this binding's agents: their approvals and decisions.
    #[must_use]
    pub const fn turns(&self) -> &Arc<Turner<P, O>> {
        &self.turns
    }

    /// The one credential a client of `root` holds: it invokes `calls`, the
    /// actor resource of the server of this binding's tree, and observes
    /// `root`'s conversation, status and tool output; with the session
    /// authority admitting it.
    ///
    /// # Errors
    /// When `root` is absent or its scope does not cover its observation.
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    pub async fn client_credential(
        &self,
        root: &AgentPath,
        calls: &str,
        clock: Arc<dyn acyclic_stream::UnixMillisClock>,
    ) -> Result<super::AgentObservation, Error> {
        self.admit_path(root)?;
        let state = self.agents.state(root).await?;
        let authority = agent_authority(
            &self.issuer,
            &self.admission,
            &state.path,
            &state.volumes,
            &[],
        )
        .await
        .map_err(turn)?;
        self.volumes
            .agent_observation(
                &authority,
                &state.path,
                &authority.private.volume,
                clock,
                Some(calls),
            )
            .await
            .map_err(turn)
    }

    /// The `fs:/` resource of `agent`'s tool output, whose follow announces
    /// each new chunk.
    ///
    /// # Errors
    /// When `agent` is absent or its private volume cannot be opened.
    pub async fn tool_resource(&self, agent: &AgentPath) -> Result<String, Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        let private = state
            .volumes
            .iter()
            .find(|binding| binding.name == super::PRIVATE_VOLUME)
            .ok_or(Error::Record("agent has no private volume"))?;
        self.volumes
            .tool_resource(&private.volume)
            .await
            .map_err(turn)
    }

    /// The change follows of this binding's volumes, for the session that
    /// serves its tree.
    ///
    /// # Errors
    /// As [`super::FilesystemHost::change_provider`].
    pub fn changes(
        &self,
    ) -> Result<acyclic_fs::session::ChangeProvider<P, StreamAuthorityStore<P>, O>, Error> {
        self.volumes
            .change_provider(self.agents.provider().clone())
            .map_err(turn)
    }

    /// Reads at most `maximum` bytes of `file` from `offset`, through
    /// `agent`'s own authorized reader: only a file on one of `agent`'s
    /// volumes, within its scope, or the exact brief its adoption named, is
    /// readable. Returns the bytes and the file's length.
    ///
    /// # Errors
    /// When `agent` is absent, the file is outside its scope, or `offset` is
    /// past the file's end.
    pub async fn read_file(
        &self,
        agent: &AgentPath,
        file: &FileRef,
        offset: u64,
        maximum: u64,
    ) -> Result<(Vec<u8>, u64), Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        let authority = agent_authority(
            &self.issuer,
            &self.admission,
            &state.path,
            &state.volumes,
            state.brief_grant(file),
        )
        .await
        .map_err(turn)?;
        let reader = super::FilesystemContentVerifier::new(
            Arc::clone(&self.volumes),
            &authority.verifier,
            &authority.scope,
            self.maximum_bytes,
            None,
        )
        .map_err(turn)?;
        let length = file.descriptor().byte_length();
        let remaining = length
            .checked_sub(offset)
            .ok_or(Error::Record("read offset is past the file's end"))?;
        let range = acyclic_fs::ByteRange {
            offset,
            length: remaining.min(maximum),
        };
        let bytes = crate::conversation::ContentResidencyVerifier::read_range(
            &*reader, file, range, None, None,
        )
        .map_err(turn)?
        .await
        .map_err(turn)?;
        Ok((bytes.to_vec(), length))
    }

    /// Reads at most `maximum` bytes from `offset` of `path` below `agent`'s
    /// tool output, under `agent`'s own grant, loading only those bytes.
    /// Returns the bytes and the file's length.
    ///
    /// # Errors
    /// As [`Self::read_file`].
    pub async fn read_tool_output(
        &self,
        agent: &AgentPath,
        path: &str,
        offset: u64,
        maximum: u64,
    ) -> Result<(Vec<u8>, u64), Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        let authority = agent_authority(
            &self.issuer,
            &self.admission,
            &state.path,
            &state.volumes,
            &[],
        )
        .await
        .map_err(turn)?;
        let grant = super::ContentGrant::verify_directory_read(
            &authority.verifier,
            &authority.scope,
            &authority.private.volume,
            super::TOOL_OUTPUT,
            &mut None,
        )
        .map_err(turn)?;
        let (bytes, length) = self
            .volumes
            .read_private_range(
                &authority.private.volume,
                &grant,
                path,
                offset,
                maximum,
                None,
            )
            .await
            .map_err(turn)?;
        Ok((bytes.to_vec(), length))
    }

    /// The approval `agent` waits on, with the exact action it shows. An
    /// agent waits on at most one.
    ///
    /// # Errors
    /// When `agent` is absent or its request file is unreadable.
    pub async fn pending_approval(
        &self,
        agent: &AgentPath,
    ) -> Result<Option<(IntentId, Interaction)>, Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        self.turns.pending_approval(&state).await.map_err(turn)
    }

    /// `principal` decides approval `id` of `agent`, bound to the exact action
    /// it was shown: the pending approval, or, for an identical retry after
    /// the agent resumed, the request `principal` already decided. Then the
    /// agent is woken with the decision.
    ///
    /// # Errors
    /// When `binding` names no such pending or already-decided action, or
    /// the decision or its delivery fails.
    pub async fn decide(
        &self,
        agent: &AgentPath,
        id: &IntentId,
        binding: &ApprovalBinding,
        principal: &str,
        approved: bool,
        reason: Option<String>,
    ) -> Result<Delivered, Error> {
        let binds = |shown: Option<Interaction>| {
            matches!(
                shown,
                Some(Interaction::Approval { operation_id, action_digest, .. })
                    if operation_id == binding.operation_id
                        && action_digest == binding.action_digest
            )
        };
        let pending = self
            .pending_approval(agent)
            .await?
            .and_then(|(pending, shown)| (&pending == id).then_some(shown));
        let shown = match pending {
            Some(shown) => binds(Some(shown)),
            None => {
                self.admit_path(agent)?;
                let state = self.agents.state(agent).await?;
                binds(
                    self.turns
                        .decided_approval(&state, id, principal)
                        .await
                        .map_err(turn)?,
                )
            }
        };
        if !shown {
            return Err(Error::Record("approval is not pending with this binding"));
        }
        self.turns
            .respond(
                &self.agents,
                agent,
                id,
                ApprovalResponder::Human(principal),
                approved,
                reason,
            )
            .await
    }

    /// Account and namespace of this binding's agent actors.
    #[must_use]
    pub const fn within(&self) -> &ActorReference {
        &self.within
    }

    /// Stages `text` once on `agent`'s private volume under the agent's
    /// admitted authority, keyed by the agent, the input `kind`, its sender
    /// and `intent`. The original source binds the request before expansion;
    /// every retry must name that same source, even if a command file changed.
    ///
    /// # Errors
    /// When the admission does not write the agent's private volume, or the
    /// store fails.
    pub async fn stage_text(
        &self,
        agent: &AgentPath,
        kind: &str,
        by: &Sender,
        intent: &IntentId,
        (source, text): (&str, &str),
    ) -> Result<FileRef, Error> {
        self.staged_input(agent, kind, by, intent, (source, Some(text)))
            .await?
            .ok_or(Error::Record("a staged input is missing its file"))
    }

    /// The text [`Self::stage_text`] already staged for this input, if any:
    /// a replay answers from it after checking the original source identity.
    ///
    /// # Errors
    /// As [`Self::stage_text`].
    pub async fn staged_text(
        &self,
        agent: &AgentPath,
        kind: &str,
        by: &Sender,
        intent: &IntentId,
        source: &str,
    ) -> Result<Option<FileRef>, Error> {
        self.staged_input(agent, kind, by, intent, (source, None))
            .await
    }

    /// Stages `text` once, or with `None` only observes what was staged.
    async fn staged_input(
        &self,
        agent: &AgentPath,
        kind: &str,
        by: &Sender,
        intent: &IntentId,
        (source, text): (&str, Option<&str>),
    ) -> Result<Option<FileRef>, Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        let authority = agent_authority(&self.issuer, &self.admission, agent, &state.volumes, &[])
            .await
            .map_err(turn)?;
        let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.hosting.input.v1");
        // Like the inbox's intent key, the staged input is the sender's own:
        // two senders' same intent never share an operation.
        let sender = crate::agent::sender_text(by);
        for part in [agent.as_str(), kind, sender.as_str(), intent.as_str()] {
            hasher.update(&(part.len() as u64).to_le_bytes());
            hasher.update(part.as_bytes());
        }
        let mut operation = [0; 16];
        operation.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        let operation = OperationId::from_bytes(operation);
        // The existing publication owns both the submitted source identity
        // and expanded bytes; no separate replay record is needed.
        let path = || format!("inputs/{operation}-{}.txt", blake3::hash(source.as_bytes()));
        let publisher = FilesystemContentPublisher::new(
            Arc::clone(&self.volumes),
            &authority.private.volume,
            &authority.verifier,
            &authority.scope,
            self.maximum_bytes,
            None,
        )
        .map_err(turn)?;
        let Some(text) = text else {
            let (staged, _) = publisher
                .observe_staged_once(operation, self.maximum_bytes, ContentAccess::default())
                .map_err(turn)?
                .await
                .map_err(turn)?;
            if staged.as_ref().is_some_and(|file| file.path() != path()) {
                return Err(Error::Conflict("input identity reused with changed source"));
            }
            return Ok(staged);
        };
        let path = path();
        let staged = publisher
            .stage_once(
                operation,
                &path,
                text.as_bytes(),
                "text/plain",
                kind,
                ContentAccess::default(),
            )
            .map_err(turn)?
            .await
            .map_err(turn)?;
        Ok(Some(staged.0))
    }

    /// Verifies original scope identity, audience, lifetime and attenuation
    /// before observing an agent's committed bindings. Performs no IO.
    pub fn admit_path(&self, agent: &AgentPath) -> Result<(), Error> {
        self.admission.verify(&self.issuer, agent).map_err(turn)?;
        Ok(())
    }

    /// Revalidates an existing agent's admission against its committed volume
    /// bindings, before restoring a platform-owned route. Performs no IO.
    pub fn admit_agent(&self, agent: &AgentPath, volumes: &[VolumeBinding]) -> Result<(), Error> {
        super::agent_turns::AdmittedAgent::verify(&self.issuer, &self.admission, agent, volumes)
            .map_err(turn)?;
        Ok(())
    }

    /// Validates a proposed root before placement or storage IO.
    pub fn admit_root(&self, request: &RootRequest) -> Result<(), Error> {
        let path = AgentPath::root(&request.agent);
        let private = request
            .fresh
            .iter()
            .chain(&request.retained)
            .find(|binding| binding.name == super::PRIVATE_VOLUME)
            .ok_or_else(|| turn(crate::Error::Invalid("agent has no private volume".into())))?;
        // Admission needs the original private binding, not a copy of every
        // proposed volume. Authority issuance remains on the execution path.
        let authority = super::agent_turns::AdmittedAgent::verify(
            &self.issuer,
            &self.admission,
            &path,
            std::slice::from_ref(private),
        )
        .map_err(turn)?;
        for binding in &request.fresh {
            authority.authorize_volume(&binding.volume).map_err(turn)?;
        }
        Ok(())
    }

    /// Prepares root `request` under intent `intent`.
    ///
    /// Before any IO it verifies the caller's admission for the root (the
    /// [`agent_authority`] check turns make: authentic, live, this audience
    /// and agent, and writing the private volume) and that the admission
    /// writes every fresh volume; a read-only or mismatched admission is
    /// refused here, having read and written nothing. Retained volumes keep
    /// exactly the access the admission grants them. Then it prepares the one
    /// sealed commit creating the fresh volumes and the root. Preparation
    /// performs no stream IO; it writes the fresh volumes' initial objects to
    /// the object store, held unpublished by the returned commit.
    ///
    /// # Errors
    /// [`crate::Error::Unauthorized`] (as [`Error::Turn`]) when the
    /// admission does not cover the root; otherwise a store failure.
    pub async fn prepare_root(
        &self,
        intent: &IntentId,
        request: RootRequest,
    ) -> Result<Prepared, Error> {
        self.admit_root(&request)?;
        let RootRequest {
            agent,
            fresh,
            retained,
            brief,
            policy,
        } = request;
        let volumes: Vec<VolumeBinding> = fresh.iter().chain(&retained).cloned().collect();
        let Spawn { child, request } = self.agents.prepare_root(
            intent,
            ForkAdopted {
                agent,
                parent: None,
                forked_at: 0,
                conversation_tail: 0,
                checkpoint: None,
                volumes,
                brief,
                policy,
            },
        )?;
        let fs = &self.volumes.filesystem;
        let cancellation = CancellationToken::new();
        let mut creations = Vec::with_capacity(fresh.len());
        for binding in &fresh {
            let (name, operation) = self
                .volumes
                .volume_creation(&binding.volume)
                .map_err(turn)?;
            creations.push(CreateSpec {
                name: WorkspaceName::new(name, &WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(|failure| turn(super::map_error(failure.error)))?
                    .value,
                initialization: self.volumes.volume_initialization(),
                creation: CreationOperation::populated(operation, &[]),
            });
        }
        let mut commit = FilesystemCommit::new(request);
        fs.prepare_create_many(
            &creations,
            &mut commit,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|failure| turn(super::map_error(failure.error)))?;
        fs.seal_commit(&mut commit, WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|failure| turn(failure.error.into()))?;
        Ok(Prepared { child, commit })
    }

    /// Submits `prepared` once and settles it: committed, the root is hosted;
    /// uncertain, the unchanged commit comes back to resubmit.
    ///
    /// # Errors
    /// When another creation holds the agent, or on a store failure.
    pub async fn submit_root(&self, prepared: Prepared) -> Result<RootSubmission, Error> {
        let Prepared { child, commit } = prepared;
        let outcome = self
            .volumes
            .filesystem
            .submit_commit(commit, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| turn(failure.error.into()))?
            .value;
        match outcome {
            FilesystemCommitOutcome::Committed(_) => {
                self.agents.host(&child)?;
                Ok(RootSubmission::Created(child))
            }
            FilesystemCommitOutcome::Rejected(_) => {
                Err(Error::Record("agent exists with another creation"))
            }
            FilesystemCommitOutcome::Unknown { commit, .. } => {
                Ok(RootSubmission::Unknown(Prepared { child, commit }))
            }
        }
    }

    /// Creates root `request`: exactly [`Self::prepare_root`] then
    /// [`Self::submit_root`]. An uncertain result is
    /// [`RootSubmission::Unknown`], still owning the prepared commit and its
    /// publication holds, to resubmit with [`Self::submit_root`].
    ///
    /// # Errors
    /// As [`Self::prepare_root`] and [`Self::submit_root`].
    pub async fn create_root(
        &self,
        intent: &IntentId,
        request: RootRequest,
    ) -> Result<RootSubmission, Error> {
        self.submit_root(self.prepare_root(intent, request).await?)
            .await
    }
}

fn turn(error: crate::Error) -> Error {
    Error::Turn(Box::new(error))
}
