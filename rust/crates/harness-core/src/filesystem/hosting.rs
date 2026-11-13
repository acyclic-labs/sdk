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
        IntentId, Sender, Spawn, WorkspaceEntry,
    },
    approvals::ApprovalResponder,
    conversation::{ContentAccess, ContentPublisher, FileRef, VolumeRef},
    core::{Authority, AuthorityRoot, RuntimeAuthority, Scope},
    interaction::{ApprovalBinding, Interaction},
    resources::ProviderRef,
};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::Clock};
use acyclic_fs::{
    AsyncObjectStore, CancellationToken, FilesystemCommit, FilesystemCommitOutcome, Fs,
    StreamAuthorityStore, WorkBudget, distributed::AuthorityProvider,
};
use std::sync::Arc;

enum StagedInput<'a> {
    Text {
        source: &'a str,
        text: Option<&'a str>,
    },
    Content(&'a crate::model::ModelContent),
    Notice(&'a crate::conversation::wire::Notice),
}

type Host<P, O> = FilesystemHost<StreamAuthorityStore<P>, O>;
mod directory;
mod generation;
/// The turn host of a binding's agents.
pub type Turner<P, O> = FilesystemTurnHost<P, StreamAuthorityStore<P>, O>;

mod client_files;
mod input_preview;
mod journal;
mod sessions;

#[cfg(all(target_os = "linux", not(target_arch = "wasm32")))]
mod guest;
#[cfg(all(target_os = "linux", not(target_arch = "wasm32")))]
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
    /// The user's launch-time peer choice, committed with the root itself.
    pub peer: bool,
    /// The root agent.
    pub agent: AgentId,
    /// Volumes created, empty, in the root's own commit; the agent's private
    /// volume is one of them. The journal is created implicitly, outside this
    /// table; an admitted caller must also grant its creation and write.
    pub fresh: Vec<WorkspaceEntry>,
    /// Existing volumes the root binds as they are.
    pub retained: Vec<WorkspaceEntry>,
    /// Task brief, already staged, if any.
    pub brief: Option<FileRef>,
    /// Approval policy file, if any.
    pub policy: Option<FileRef>,
}

/// A prepared root: the one sealed commit creating its fresh volumes, its
/// inbox with `ForkAdopted`, its actor state and its activation, together
/// with the holds keeping the fresh volumes' objects until it settles. Only
/// [`Prepared::submit`] submits it, consuming it; an uncertain submission
/// hands it back to resubmit exactly.
pub struct Prepared {
    child: AgentPath,
    commit: FilesystemCommit,
}

impl Prepared {
    /// Composes an admitted adoption with exactly the explicitly supplied fresh
    /// volumes in one sealed commit. Include a fresh journal when it is needed.
    /// The caller authenticates the adoption and fresh bindings before IO.
    /// Preparation retains publication custody without making the root visible.
    ///
    /// # Errors
    /// Invalid placement, conflicting volume creation or storage failure.
    pub async fn prepare<P, O>(
        host: &Host<P, O>,
        Spawn { child, request }: Spawn,
        fresh: &[&VolumeRef],
    ) -> Result<Self, Error>
    where
        P: AuthorityProvider + ?Sized + 'static,
        O: AsyncObjectStore + 'static,
    {
        let fs = &host.filesystem;
        let cancellation = CancellationToken::new();
        let mut creations = Vec::with_capacity(fresh.len());
        for volume in fresh {
            creations.push(host.volume_create_spec(volume).map_err(turn)?);
        }
        let mut commit = FilesystemCommit::new(request);
        let created = fs
            .prepare_create_many(
                &creations,
                &mut commit,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| turn(super::map_error(failure.error)))?;
        super::history::capture_creation(fs, &child, &created.value, &mut commit)
            .await
            .map_err(|error| turn(crate::Error::Storage(error.to_string())))?;
        fs.seal_commit(&mut commit, WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|failure| turn(failure.error.into()))?;
        Ok(Self { child, commit })
    }

    /// Submits this sealed root once and hosts only a committed root.
    /// An uncertain result returns the unchanged commit and all its holds.
    ///
    /// # Errors
    /// The activation host uses another provider, another creation holds the
    /// root, or submission or activation fails.
    pub async fn submit<P, O>(
        self,
        host: &Host<P, O>,
        agents: &AgentHost<P>,
    ) -> Result<RootSubmission, Error>
    where
        P: AuthorityProvider + ?Sized + 'static,
        O: AsyncObjectStore + 'static,
    {
        if !Arc::ptr_eq(agents.provider(), &host.filesystem.stream_provider()) {
            return Err(Error::Record("root activation uses another provider"));
        }
        let Self { child, commit } = self;
        let outcome = host
            .filesystem
            .submit_commit(commit, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| turn(failure.error.into()))?
            .value;
        match outcome {
            FilesystemCommitOutcome::Committed(_) => {
                agents.host(&child)?;
                Ok(RootSubmission::Created(child))
            }
            FilesystemCommitOutcome::Rejected(_) => {
                Err(Error::Record("agent exists with another creation"))
            }
            FilesystemCommitOutcome::Unknown { commit, .. } => {
                Ok(RootSubmission::Unknown(Self { child, commit }))
            }
        }
    }

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
    maximum_bytes: Option<u64>,
    agents: Arc<AgentHost<P>>,
    turns: Arc<Turner<P, O>>,
    volumes: Arc<Host<P, O>>,
    spawns: Arc<FilesystemSpawnVolumes<P, O>>,
    clock: Arc<dyn Clock>,
    handler: Arc<dyn acyclic_actors::guest::Handler>,
    composer: Arc<crate::agent::AgentComposer<P>>,
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
            spawns: self.spawns.clone(),
            clock: self.clock.clone(),
            handler: self.handler.clone(),
            composer: self.composer.clone(),
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
        cuts: Option<Arc<dyn super::VolumeResources>>,
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
        cuts: Option<Arc<dyn super::VolumeResources>>,
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
        let ancestry = acyclic_fs::StreamGitCompatStore::new(StreamAuthorityStore::new(
            filesystem.stream_provider(),
        ));
        let volumes = FilesystemHost::new(filesystem, &provider, None)
            .and_then(|host| host.with_project_families(ancestry))
            .map_err(turn)?;
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
        (turns, cuts): (Turns, Option<Arc<dyn super::VolumeResources>>),
        configure: impl FnOnce(Turner<P, O>) -> Turner<P, O>,
    ) -> Result<Self, Error> {
        let stream = volumes.filesystem.stream_provider();
        let maximum_bytes = turns.limits.maximum_bytes;
        let model = turns.executors.model_selection();
        // The preset's policy binds before any agent runs under it.
        turns.approval_policy.bind().map_err(turn)?;
        let turner = FilesystemTurnHost::new(
            Arc::clone(&stream),
            within.clone(),
            Arc::clone(&volumes),
            issuer.clone(),
            admission.clone(),
            turns.executors,
            turns.limits,
            turns.approval_policy,
        )
        .with_filesystem_tools();
        let turner = Arc::new(configure(turner));
        let templates = turner.templates();
        let driver = ExecutorDriver::new(Arc::clone(&turner));
        // A host keeping state beside its volumes (a lazy checkout) chooses
        // each spawn's fork cut, so a child's project is a related fork.
        let spawns = FilesystemSpawnVolumes::new(Arc::clone(&volumes));
        let spawns = Arc::new(match cuts {
            Some(cuts) => spawns.with_volume_resources(cuts),
            None => spawns,
        });
        let handler = Arc::new(
            AgentHandler::new(crate::agent::actor_store(&stream)?, driver, turns.actors)
                .map_err(|_| Error::Record("agent handler manifest"))?
                .with_spawn_volumes(spawns.clone())
                .with_close(),
        );
        let composer = Arc::new(crate::agent::AgentComposer::new(
            stream.clone(),
            Some(spawns.clone()),
            None,
        ));
        let agents = AgentHost::hosting(
            stream,
            within.clone(),
            handler.clone(),
            turns.clock.clone(),
            Some(composer.clone()),
        )?
        .with_model(model);
        // The host pins roots to the same creation reader these turns use.
        let agents = Arc::new(match templates {
            Some(templates) => agents.with_templates(templates),
            None => agents,
        });
        Ok(Self {
            issuer,
            admission,
            within,
            maximum_bytes,
            agents,
            turns: turner,
            volumes,
            spawns,
            clock: turns.clock,
            handler,
            composer,
        })
    }

    /// Runs deliveries through the host-injected guest boundary. Admission,
    /// turn composition, root publication and spawn participants remain those
    /// of this binding; the guest owns only delivery execution.
    pub fn hosting(
        mut self,
        handler: Arc<dyn acyclic_actors::guest::Handler>,
    ) -> Result<Self, Error> {
        // The guest composes the same spawns as this binding's own handler,
        // and roots keep the creation reader its turns use.
        self.agents = Arc::new(self.agents.rehosted(
            handler.clone(),
            self.clock.clone(),
            Some(self.composer.clone()),
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

    /// The composer this binding's agents commit custody with. Any later host
    /// of those agents needs it to settle that custody.
    #[must_use]
    pub fn composer(&self) -> Arc<dyn acyclic_actors::guest::Composer> {
        self.composer.clone()
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
    /// `root`'s stream subtree, matching status subtree and tool output; with the session
    /// authority admitting it.
    /// Independent roots also receive read-only peer discovery; child enrollment
    /// never inherits that resource or session-directory authority.
    ///
    /// # Errors
    /// When `root` is absent or its scope does not cover its observation.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn client_credential(
        &self,
        root: &AgentPath,
        calls: &str,
        clock: Arc<dyn acyclic_stream::UnixMillisClock>,
    ) -> Result<super::AgentObservation, Error> {
        self.client_observation(root, calls, clock, None).await
    }

    /// Explicit owner enrollment for the root directory of this placement.
    /// This is a platform operation, never callable by a session request.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn session_credential(
        &self,
        root: &AgentPath,
        calls: &str,
        directory: &str,
        clock: Arc<dyn acyclic_stream::UnixMillisClock>,
    ) -> Result<super::AgentObservation, Error> {
        self.admit_sessions()?;
        self.client_observation(root, calls, clock, Some(directory))
            .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Session admission for this endpoint's root uses the same audience as
    /// its client enrollment. Local roots have a conversation audience;
    /// admitted workloads retain their original host-selected audience.
    pub fn client_authority(
        &self,
        root: &AgentPath,
        clock: Arc<dyn acyclic_stream::UnixMillisClock>,
    ) -> Result<acyclic_stream::session::Authority, Error> {
        self.admit_path(root)?;
        let issuer = self.admission.verify(&self.issuer, root).map_err(turn)?;
        super::session_authority(
            issuer
                .native()
                .ok_or(Error::Record("client admission requires native custody"))?,
            clock,
        )
        .map_err(turn)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn observation_parent<'a>(&'a self, fallback: &'a Scope) -> &'a Scope {
        match &self.admission {
            TurnAdmission::Admitted { scope, .. } => scope,
            TurnAdmission::LocalRoot => self.issuer.caller().map_or(fallback, |(_, scope)| scope),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn client_observation(
        &self,
        root: &AgentPath,
        calls: &str,
        clock: Arc<dyn acyclic_stream::UnixMillisClock>,
        directory: Option<&str>,
    ) -> Result<super::AgentObservation, Error> {
        self.admit_path(root)?;
        let state = self.agents.state(root).await?;
        let (journal, authority) = self.turns.journal_authority(&state).await.map_err(turn)?;
        let peers = self.peer_resource();
        let invocations = [calls, peers.as_str()];
        let calls = if root.depth() == 0 {
            &invocations[..]
        } else {
            &invocations[..1]
        };
        self.volumes
            .client_observation(
                (&authority, self.observation_parent(&authority.scope)),
                &state.path,
                &journal,
                clock,
                calls,
                directory,
            )
            .await
            .map_err(turn)
    }

    /// The `fs:/` resource of `agent`'s tool output, whose follow announces
    /// each new chunk.
    ///
    /// # Errors
    /// When `agent` is absent or its journal cannot be opened.
    pub async fn tool_resource(&self, agent: &AgentPath) -> Result<String, Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        self.admit_agent(agent, &state.volumes)?;
        let journal =
            crate::conversation::journal_volume(self.volumes.provider.clone(), &agent.id())?;
        self.volumes.tool_resource(&journal).await.map_err(turn)
    }

    /// Native enrollment of a selected agent's read-only observation on an
    /// existing session. Platform custody uses its original admitted credential
    /// instead; the host never creates signing custody from that invocation.
    /// The enrollment derives from the selected agent's scope, not from the
    /// caller's command credential, and carries no actor invocation grant. Its
    /// recipient remains the session serving `root`, independently of the
    /// selected agent's scope audience.
    #[cfg(not(target_arch = "wasm32"))]
    #[inline(never)]
    pub fn selected_observation<'a>(
        &'a self,
        root: &'a AgentPath,
        agent: &'a AgentPath,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<(String, Vec<u8>)>, Error>> {
        Box::pin(async move {
            self.admit_path(agent)?;
            if self.issuer.native().is_none() {
                return Ok(None);
            }
            let recipient = self.admission.verify(&self.issuer, root).map_err(turn)?;
            let audience = recipient
                .native()
                .ok_or(Error::Record("observation recipient has no native custody"))?
                .audience_resource()
                .map_err(turn)?;
            let state = self.agents.state(agent).await?;
            let (journal, authority) = self.turns.journal_authority(&state).await.map_err(turn)?;
            let clock = ObservationClock(self.clock.clone());
            let (resource, credential, _) = self
                .volumes
                .observation_credential(
                    (&authority, self.observation_parent(&authority.scope)),
                    &state.path,
                    &journal,
                    &clock,
                    &[],
                    None,
                    audience,
                )
                .await
                .map_err(turn)?;
            Ok(Some((resource, credential)))
        })
    }

    /// Query current project heads through the selected agent's original grant
    /// and persisted read policy, without listing project files or old turns.
    pub async fn project_heads(
        &self,
        agent: &AgentPath,
    ) -> Result<Vec<crate::agent::HistoryVolume>, Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        self.turns
            .project_heads(&state, crate::conversation::VolumeOperation::Read)
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
    /// `agent`'s authorized reader: its bound volumes, its own published
    /// conversation files, or an exact adopted file.
    /// Original scope and persisted policy still apply; control families stay
    /// private. Returns the bytes and the file's length.
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
        self.read_file_selected(agent, file, None, offset, maximum)
            .await
    }

    /// Read an exact file attested by the selected committed conversation
    /// record, including inherited turns. No source-volume grant is issued.
    pub async fn read_conversation_file(
        &self,
        agent: &AgentPath,
        file: &FileRef,
        sequence: u64,
        offset: u64,
        maximum: u64,
    ) -> Result<(Vec<u8>, u64), Error> {
        self.read_file_selected(agent, file, Some(sequence), offset, maximum)
            .await
    }

    async fn read_file_selected(
        &self,
        agent: &AgentPath,
        file: &FileRef,
        sequence: Option<u64>,
        offset: u64,
        maximum: u64,
    ) -> Result<(Vec<u8>, u64), Error> {
        let reader = self.reader(agent, Some(file), sequence).await?;
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

    /// Authority for a host-owned filesystem command on this agent's bound
    /// volumes. This retains the endpoint's original admission, including
    /// read-only and workload attenuation, just like its ordinary turns.
    pub async fn volume_scope(
        &self,
        agent: &AgentPath,
    ) -> Result<(Scope, crate::core::AuthorityVerifier), Error> {
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
        Ok((authority.scope, authority.verifier))
    }

    /// Authenticated reader of the selected agent's bound volumes, for shared
    /// file projections such as a session's diff. Enrollment cannot widen its
    /// original admission or read another session's private content.
    pub async fn content_reader(
        &self,
        agent: &AgentPath,
    ) -> Result<Arc<super::FilesystemContentVerifier<StreamAuthorityStore<P>, O>>, Error> {
        self.reader(agent, None, None).await
    }

    async fn reader(
        &self,
        agent: &AgentPath,
        file: Option<&FileRef>,
        sequence: Option<u64>,
    ) -> Result<Arc<super::FilesystemContentVerifier<StreamAuthorityStore<P>, O>>, Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        let external = match (sequence, file) {
            (Some(sequence), Some(file)) if sequence < state.conversation_tail => {
                let entry = crate::agent::conversation_entry(
                    self.agents.provider().as_ref(),
                    &agent.conversation()?,
                    sequence,
                )
                .await?;
                if !entry.references_file(file) {
                    return Err(Error::Record(
                        "conversation record does not name the requested file",
                    ));
                }
                std::slice::from_ref(file)
            }
            (Some(_), _) => {
                return Err(Error::Record(
                    "conversation file proof is outside the committed prefix",
                ));
            }
            (None, Some(file))
                if file.volume().is_agent_journal()
                    && file.volume().provider() == &self.volumes.provider
                    && file.volume().owner()
                        == &crate::conversation::VolumeOwner::Agent(
                            crate::AgentId::from_bytes(agent.id_bytes()),
                        )
                    && [
                        super::InternalContentClass::Conversation,
                        super::InternalContentClass::ToolOutput,
                    ]
                    .iter()
                    .any(|class| class.contains_path(file.path())) =>
            {
                // Only the host's published output families admit an unsequenced
                // journal reference. Pending inputs still require the committed
                // conversation proof above, even though their bytes are staged.
                self.volumes.file_generation(file, None).map_err(turn)?;
                std::slice::from_ref(file)
            }
            (None, file) => file.map_or(&[][..], |file| state.adoption_grant(file)),
        };
        let authority = agent_authority(
            &self.issuer,
            &self.admission,
            &state.path,
            &state.volumes,
            external,
        )
        .await
        .map_err(turn)?;
        super::FilesystemContentVerifier::new(
            Arc::clone(&self.volumes),
            &authority.verifier,
            &authority.scope,
            self.maximum_bytes,
            None,
        )
        .map_err(turn)
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
        at: Option<&FileRef>,
        offset: u64,
        maximum: u64,
    ) -> Result<(Vec<u8>, u64), Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        let (journal, authority) = self.turns.journal_authority(&state).await.map_err(turn)?;
        if at.is_some_and(|file| file.volume() != &journal)
            || (path.starts_with(super::InternalContentClass::ToolResult.prefix()) && at.is_none())
        {
            return Err(Error::Record(
                "tool result requires its original journal generation",
            ));
        }
        let generation = at
            .map(|file| self.volumes.file_generation(file, None))
            .transpose()
            .map_err(turn)?;
        let grant = super::ContentGrant::verify_directory_read(
            &authority.verifier,
            &authority.scope,
            &journal,
            super::TOOL_OUTPUT,
            &mut None,
        )
        .map_err(turn)?;
        let (bytes, _selection, length) = self
            .volumes
            .read_private_range(
                &journal,
                &grant,
                path,
                generation.as_ref(),
                acyclic_fs::ByteRange {
                    offset,
                    length: maximum,
                },
                crate::conversation::ContentAccess::default(),
            )
            .await
            .map_err(turn)?;
        Ok((bytes.to_vec(), length))
    }

    /// The approval `agent` waits on: its original status sequence and exact
    /// action binding. The request body stays at its immutable source.
    ///
    /// # Errors
    /// When `agent` is absent or its waiting phase has no published status.
    pub async fn pending_approval(
        &self,
        agent: &AgentPath,
    ) -> Result<Option<(IntentId, u64, ApprovalBinding)>, Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        let crate::agent::Phase::WaitingApproval {
            id,
            binding: Some(binding),
        } = state.phase
        else {
            return Ok(None);
        };
        let status = state
            .status_tail
            .checked_sub(1)
            .ok_or(Error::Record("waiting approval has no published status"))?;
        Ok(Some((id, status, binding)))
    }

    /// Reads the immutable request named by one original status, even after
    /// that approval settled. A caller supplies no request identity or path.
    pub async fn read_approval_request(
        &self,
        agent: &AgentPath,
        status: u64,
        offset: u64,
        maximum: u64,
    ) -> Result<(acyclic_stream::OwnedBytes, u64), Error> {
        use crate::agent::AgentRecord as _;
        self.admit_path(agent)?;
        let record = crate::store::operations::one_record(
            self.agents.provider(),
            &agent.status()?,
            status,
            &mut None,
        )
        .await
        .map_err(turn)?
        .ok_or(Error::Record("approval status is absent"))?;
        let crate::agent::Phase::WaitingApproval { id, .. } =
            crate::agent::StatusRecord::decode(&record.value)?.phase
        else {
            return Err(Error::Record("status names no approval request"));
        };
        let state = self.agents.state(agent).await?;
        let (journal, authority) = self.turns.journal_authority(&state).await.map_err(turn)?;
        let grant = super::ContentGrant::verify(
            &authority.verifier,
            &authority.scope,
            &journal,
            crate::conversation::VolumeOperation::Read,
            &mut None,
        )
        .map_err(turn)?;
        let path = format!(
            "{}{}",
            super::InternalContentClass::Interaction.prefix(),
            crate::approvals::request_path(id.as_str())
        );
        let (file, bytes) = self
            .volumes
            .read_internal_range(
                &journal,
                &grant,
                &path,
                super::InternalContentClass::Interaction,
                acyclic_fs::ByteRange {
                    offset,
                    length: maximum,
                },
                None,
            )
            .await
            .map_err(turn)?;
        Ok((bytes, file.descriptor().byte_length()))
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
        verdict: crate::interaction::ApprovalVerdict,
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
            .and_then(|(pending, _, shown)| (&pending == id).then_some(shown));
        let shown = match pending {
            Some(shown) => &shown == binding,
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
                verdict,
                reason,
            )
            .await
    }

    /// Account and namespace of this binding's agent actors.
    #[must_use]
    pub const fn within(&self) -> &ActorReference {
        &self.within
    }

    /// The pin of a `role` child of `parent`, admitted by `parent`'s role.
    /// A pin never changes once adopted, so this admission holds for the
    /// step that applies the spawn.
    ///
    /// # Errors
    /// [`Error::SpawnRefused`] when these turns bind no roles, `parent` has
    /// no role, or its role does not admit `role`; or the state read fails.
    pub async fn child_profile(
        &self,
        parent: &AgentPath,
        role: &str,
    ) -> Result<crate::agent::CreationConfig, Error> {
        self.agents.child_profile(parent, role).await
    }

    /// Stages `text` once on `agent`'s journal volume under the agent's
    /// admitted authority, keyed by the agent, the input `kind`, its sender
    /// and `intent`. The original source binds the request before expansion;
    /// every retry must name that same source, even if a command file changed.
    ///
    /// # Errors
    /// When the admission does not write the agent's journal volume, or the
    /// store fails.
    pub async fn stage_text(
        &self,
        agent: &AgentPath,
        kind: &str,
        by: &Sender,
        intent: &IntentId,
        (source, text): (&str, &str),
    ) -> Result<FileRef, Error> {
        self.staged_input(
            agent,
            kind,
            by,
            intent,
            StagedInput::Text {
                source,
                text: Some(text),
            },
        )
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
        self.staged_input(
            agent,
            kind,
            by,
            intent,
            StagedInput::Text { source, text: None },
        )
        .await
    }

    /// Stages ordered text and immutable file references through the same
    /// sender/intent publication as text input. Every reference must already
    /// be readable under this agent's admission; input cannot grant access.
    /// Retries retain the original bytes and reject changed content.
    ///
    /// # Errors
    /// Invalid user content, an ungranted file, changed intent, or store failure.
    pub async fn stage_prompt(
        &self,
        agent: &AgentPath,
        by: &Sender,
        intent: &IntentId,
        content: &crate::model::ModelContent,
    ) -> Result<FileRef, Error> {
        self.staged_input(agent, "message", by, intent, StagedInput::Content(content))
            .await?
            .ok_or(Error::Record("a staged input is missing its file"))
    }

    /// Prepares one primitive event under its original sender and intent.
    /// Waking content uses the same immutable input publication as a message;
    /// only that file reference enters the inbox, queue and settled entry.
    ///
    /// # Errors
    /// Invalid Notice content, missing authority, changed intent or store failure.
    pub async fn stage_notice(
        &self,
        agent: &AgentPath,
        by: &Sender,
        intent: &IntentId,
        notice: &crate::conversation::wire::Notice,
    ) -> Result<crate::agent::InboxRecord, Error> {
        notice
            .validate()
            .map_err(|error| turn(crate::Error::Invalid(error.to_string())))?;
        let notice = if notice.wake {
            let file = self
                .staged_input(agent, "notice", by, intent, StagedInput::Notice(notice))
                .await?
                .ok_or(Error::Record("a staged notice is missing its file"))?;
            crate::agent::NoticePayload::Waking(Box::new(file))
        } else {
            self.admit_path(agent)?;
            crate::agent::NoticePayload::Quiet(notice.clone())
        };
        Ok(crate::agent::InboxRecord::Notice {
            id: intent.clone(),
            notice,
        })
    }

    /// Stages `text` once, or with `None` only observes what was staged.
    async fn staged_input(
        &self,
        agent: &AgentPath,
        kind: &str,
        by: &Sender,
        intent: &IntentId,
        input: StagedInput<'_>,
    ) -> Result<Option<FileRef>, Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        let directory = match &input {
            StagedInput::Notice(_) => "inputs/notices",
            StagedInput::Text { .. } | StagedInput::Content(_) => "inputs",
        };
        let encoded;
        let (source, payload, extension) = match input {
            StagedInput::Notice(notice) => {
                encoded = crate::contract::canonical_json_bytes(notice)
                    .map_err(|error| turn(crate::Error::Invalid(error.to_string())))?;
                (
                    encoded.as_slice(),
                    Some((encoded.as_slice(), crate::agent::NOTICE_MEDIA_TYPE)),
                    "json",
                )
            }
            StagedInput::Text { source, text } => (
                source.as_bytes(),
                text.map(|text| (text.as_bytes(), "text/plain")),
                "txt",
            ),
            StagedInput::Content(content) => {
                let authority =
                    agent_authority(&self.issuer, &self.admission, agent, &state.volumes, &[])
                        .await
                        .map_err(turn)?;
                encoded = crate::agent::encode_user_input(content).map_err(turn)?;
                for file in content.file_refs() {
                    crate::conversation::ContentGrant::verify_read(
                        &authority.verifier,
                        &authority.scope,
                        file,
                    )
                    .map_err(turn)?;
                }
                (
                    encoded.as_slice(),
                    Some((encoded.as_slice(), crate::agent::USER_INPUT_MEDIA_TYPE)),
                    "json",
                )
            }
        };
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
        let leaf = format!("{operation}-{}.{extension}", blake3::hash(source));
        let (journal, authority) = self.turns.journal_authority(&state).await.map_err(turn)?;
        let publisher = FilesystemContentPublisher::new(
            Arc::clone(&self.volumes),
            &journal,
            &authority.verifier,
            &authority.scope,
            self.maximum_bytes,
            None,
        )
        .map_err(turn)?;
        let (prior, _) = publisher
            .observe_staged_once(
                operation,
                None,
                self.maximum_bytes,
                ContentAccess::default(),
            )
            .map_err(turn)?
            .await
            .map_err(turn)?;
        if prior
            .as_ref()
            .is_some_and(|file| file.path().rsplit('/').next() != Some(leaf.as_str()))
        {
            return Err(Error::Conflict("input identity reused with changed source"));
        }
        let Some((bytes, media_type)) = payload else {
            return Ok(prior);
        };
        // Placement is selected only for a new operation. Replays retain the
        // original publication and still verify its bytes and metadata below.
        let path = prior.as_ref().map_or_else(
            || format!("{directory}/{leaf}"),
            |file| file.path().to_owned(),
        );
        let staged = publisher
            .stage_once(
                operation,
                &path,
                bytes,
                media_type,
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
    pub fn admit_agent(&self, agent: &AgentPath, volumes: &[WorkspaceEntry]) -> Result<(), Error> {
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
            .find(|binding| binding.role == crate::agent::WorkspaceRole::Private)
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
            if let Some(volume) = binding.as_volume() {
                authority.authorize_volume(volume).map_err(turn)?;
                if let TurnAdmission::Admitted { scope, .. } = &self.admission {
                    self.volumes
                        .authorize_volume_creation(scope, volume)
                        .map_err(turn)?;
                }
            }
        }
        let journal =
            crate::conversation::journal_volume(self.volumes.provider.clone(), &request.agent)
                .map_err(turn)?;
        authority.authorize_host_volume(&journal).map_err(turn)?;
        if let TurnAdmission::Admitted { scope, .. } = &self.admission {
            crate::conversation::ContentGrant::authorize_agent_volume(
                &self.issuer.verifier(&mut None).map_err(turn)?,
                scope,
                crate::AgentId::from_bytes(path.id_bytes()),
                &journal,
                crate::conversation::VolumeOperation::Write,
                &mut None,
            )
            .map_err(turn)?;
            self.volumes
                .authorize_volume_creation(scope, &journal)
                .map_err(turn)?;
        }
        if request
            .fresh
            .iter()
            .chain(&request.retained)
            .any(|binding| binding.as_volume() == Some(&journal))
        {
            return Err(Error::Record("the agent journal is not a workspace"));
        }
        Ok(())
    }

    /// Prepares root `request` under intent `intent`.
    ///
    /// Before any IO it verifies the caller's admission for the root (the
    /// [`agent_authority`] check turns make: authentic, live, this audience
    /// and agent, and bound to the private owner) and that the admission
    /// writes every fresh volume; a read-only or mismatched admission is
    /// refused here, having read and written nothing. Retained volumes keep
    /// exactly the access the admission grants them. A retained private writer
    /// is checked against its real persisted policy before preparing storage;
    /// this reads policy metadata, never its file contents. Then it prepares the one
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
        self.prepare_root_request(intent, request, false, None)
            .await
    }

    /// Prepare an independent root and its directory membership in one commit.
    /// The serving placement initializes its directory once before admission.
    pub async fn prepare_session_root(
        &self,
        intent: &IntentId,
        request: RootRequest,
        questions: Option<bool>,
    ) -> Result<Prepared, Error> {
        self.admit_sessions()?;
        self.prepare_root_request(intent, request, true, questions)
            .await
    }

    async fn prepare_root_request(
        &self,
        intent: &IntentId,
        request: RootRequest,
        indexed: bool,
        questions: Option<bool>,
    ) -> Result<Prepared, Error> {
        self.admit_root(&request)?;
        if let TurnAdmission::Admitted { scope, .. } = &self.admission {
            for binding in &request.retained {
                if binding.role == crate::agent::WorkspaceRole::Private {
                    self.volumes
                        .authorize_retained_writer(scope, binding.volume()?)
                        .await
                        .map_err(turn)?;
                }
            }
        }
        if self.turns.templates().is_some() {
            self.volumes.seed_layer().await.map_err(turn)?;
        }
        let RootRequest {
            peer,
            agent,
            fresh,
            retained,
            brief,
            policy,
        } = request;
        let volumes: Vec<WorkspaceEntry> = fresh.iter().chain(&retained).cloned().collect();
        let Spawn { child, mut request } = self.agents.prepare_root_with_peer(
            intent,
            ForkAdopted {
                profile: None,
                agent,
                parent: None,
                forked_at: 0,
                conversation_tail: 0,
                checkpoint: None,
                volumes,
                brief,
                policy,
            },
            questions.unwrap_or(self.turns.question_plugin()),
            peer,
        )?;
        if indexed {
            self.index_session(&child, &mut request)?;
        }
        let journal =
            crate::conversation::journal_volume(self.volumes.provider.clone(), &child.id())
                .map_err(turn)?;
        let fresh = fresh
            .iter()
            .filter_map(WorkspaceEntry::as_volume)
            .chain(std::iter::once(&journal))
            .collect::<Vec<_>>();
        Prepared::prepare(&self.volumes, Spawn { child, request }, &fresh).await
    }

    /// Submits `prepared` once and settles it: committed, the root is hosted;
    /// uncertain, the unchanged commit comes back to resubmit.
    ///
    /// # Errors
    /// When another creation holds the agent, or on a store failure.
    pub async fn submit_root(&self, prepared: Prepared) -> Result<RootSubmission, Error> {
        prepared.submit(&self.volumes, &self.agents).await
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

#[cfg(not(target_arch = "wasm32"))]
struct ObservationClock(Arc<dyn Clock>);

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_stream::UnixMillisClock for ObservationClock {
    fn now_unix_millis(&self) -> u64 {
        self.0.now_unix_ms()
    }
}

fn turn(error: crate::Error) -> Error {
    Error::Turn(Box::new(error))
}
