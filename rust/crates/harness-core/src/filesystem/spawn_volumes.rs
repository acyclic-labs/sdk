//! [`SpawnVolumes`] on Filesystem volumes placed in the agents' own stream: a
//! child forks each parent volume at its current head, except its private
//! volume, which is created empty, and a session-shared volume, which it
//! shares. The child's brief stays a file on its parent's private volume,
//! named by its adoption.
//! Every fork and creation joins the parent step's commit through one sealed
//! [`FilesystemCommit`], so the children and their volumes appear together.

use super::FilesystemHost;
use crate::{
    agent::{
        AgentPath, AgentRecord as _, AgentState, Error, InboxRecord, Out, SpawnVolumes,
        WorkspaceEntry, WorkspaceInherit, WorkspaceSource,
    },
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
};
use acyclic_actors::{
    addressing::ActorReference,
    guest::{Checkpoint, Composer, Composition, GuestError, GuestFuture, Participant},
};
use acyclic_fs::{
    AsyncObjectStore, CancellationToken, CreateSpec, FilesystemCommit, ForkSpec, IdempotencyKey,
    StreamAuthorityStore, WorkBudget, WorkspaceName, distributed::AuthorityProvider,
};
use acyclic_stream::{BoxProviderFuture, CommitOutcome, CommitRequest, Record, StreamError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[cfg(test)]
mod tests;

type Host<P, O> = FilesystemHost<StreamAuthorityStore<P>, O>;

/// A fork cut either needs a new source selection or failed permanently.
#[derive(Debug, thiserror::Error)]
pub enum ForkCutError {
    /// The source no longer matches the generation selected by the caller.
    #[error("fork source differs from its selected generation")]
    SourceChanged,
    /// Preparing the selected cut failed for another reason.
    #[error(transparent)]
    Failed(#[from] crate::Error),
}

impl From<ForkCutError> for GuestError {
    fn from(error: ForkCutError) -> Self {
        match error {
            ForkCutError::SourceChanged => Self::PreconditionChanged,
            ForkCutError::Failed(error) => crate::Error::guest(error),
        }
    }
}

/// Spawned volumes of agents whose store shares the volumes' authority stream.
pub struct FilesystemSpawnVolumes<P: ?Sized + 'static, O: 'static> {
    host: Arc<Host<P, O>>,
    cuts: Option<Arc<dyn VolumeResources>>,
    companions: Option<Arc<dyn crate::agent::Companions>>,
}

/// Physical resources kept beside logical volumes. One owner prepares each
/// spawn's lazy fork cut and releases a closing agent's physical bindings;
/// logical volumes and retained history outlive those physical views.
pub trait VolumeResources: acyclic_stream::ProviderPlatform + 'static {
    /// The generation of workspace `source` that `destination` forks. An
    /// exact `generation` retains that cut or refuses a changed source.
    fn cut<'a>(
        &'a self,
        source: &'a str,
        destination: &'a str,
        operation: acyclic_fs::OperationId,
        generation: Option<acyclic_fs::GenerationId>,
    ) -> BoxProviderFuture<'a, Result<Option<acyclic_fs::GenerationId>, ForkCutError>>;

    /// Releases physical resources belonging to these original bindings. This
    /// does not delete the logical volumes or their retained history.
    fn release<'a>(
        &'a self,
        agent: &'a AgentPath,
        bindings: &'a [WorkspaceEntry],
    ) -> BoxProviderFuture<'a, Result<(), crate::Error>>;
}

impl<P: ?Sized, O> FilesystemSpawnVolumes<P, O> {
    /// Volumes on `host`, whose authority must be the agents' stream
    /// provider.
    #[must_use]
    pub fn new(host: Arc<Host<P, O>>) -> Self {
        Self {
            host,
            cuts: None,
            companions: None,
        }
    }

    /// Forks each source at the generation `cuts` chooses.
    #[must_use]
    pub fn with_volume_resources(mut self, cuts: Arc<dyn VolumeResources>) -> Self {
        self.cuts = Some(cuts);
        self
    }

    /// Create host-owned companions from the same validated child bindings as
    /// its volumes. Guests need neither this factory nor its host authority.
    #[must_use]
    pub fn with_companions(mut self, companions: Arc<dyn crate::agent::Companions>) -> Self {
        self.companions = Some(companions);
        self
    }
}

/// One child volume: forked from `source`, or created empty without one.
struct Planned {
    source: Option<VolumeRef>,
    destination: VolumeRef,
    key: [u8; 16],
    binding: crate::agent::EntryId,
    history: Option<crate::agent::HistorySelection>,
}

fn key(child: &AgentPath, entry: crate::agent::EntryId) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.agent.spawn-volume.v1");
    hasher.update(child.as_str().as_bytes());
    hasher.update(&[0]);
    hasher.update(&entry.into_bytes());
    let mut bytes = [0; 16];
    hasher.finalize_xof().fill(&mut bytes);
    bytes
}

/// Creation identity of an agent's forked binding. Observation of its original
/// generation uses this same receipt, independently of later edits or merges.
pub fn spawned_operation(
    child: &AgentPath,
    binding: crate::agent::EntryId,
) -> acyclic_fs::OperationId {
    acyclic_fs::OperationId::from_bytes(key(child, binding))
}

fn invalid(error: crate::Error) -> Error {
    Error::Turn(Box::new(error))
}

/// The volume `child` binds in place of its parent's non-shared `binding`,
/// and the creation operation of the spawn commit that creates it. A host
/// that keeps state beside a forked volume prepares it through [`VolumeResources`].
fn spawned_volume(
    child: &AgentPath,
    binding: &WorkspaceEntry,
) -> Result<(VolumeRef, acyclic_fs::OperationId), Error> {
    let source = binding.volume()?;
    let owner = match source.owner() {
        VolumeOwner::Agent(_) => VolumeOwner::Agent(crate::AgentId::from_bytes(child.id_bytes())),
        VolumeOwner::Session(_) if source.class() == VolumeClass::SessionShared => {
            VolumeOwner::Session(child.id().to_string())
        }
        other => other.clone(),
    };
    let destination = VolumeRef::new(
        source.provider().clone(),
        format!("workspace-{}-{}", binding.id, child.id().as_str()),
        source.class(),
        owner,
        &mut None,
    )
    .map_err(invalid)?;
    Ok((destination, spawned_operation(child, binding.id)))
}

/// The volume spawned child `child` binds in place of its parent's volume
/// `binding`: the one naming a host resolving a child's forked volume (its
/// `agents/<name>` branch) shares with the spawn that created it.
///
/// # Errors
/// When the derived volume reference is invalid.
pub fn spawned_binding(child: &AgentPath, binding: &WorkspaceEntry) -> Result<VolumeRef, Error> {
    if binding.inherit == WorkspaceInherit::Share && !fresh_session_board(child, binding) {
        return Ok(binding.volume()?.clone());
    }
    Ok(spawned_volume(child, binding)?.0)
}

// A shared harness board belongs to one session tree. A new independent
// root owns a fresh board; explicit user Share entries retain their source.
fn fresh_session_board(child: &AgentPath, binding: &WorkspaceEntry) -> bool {
    child.parent().is_none() && binding.role == crate::agent::WorkspaceRole::Shared
}

/// The child's binding of `parent`'s volume `binding`, and how it comes to exist.
fn plan(
    child: &AgentPath,
    binding: &WorkspaceEntry,
) -> Result<(WorkspaceEntry, Option<Planned>), Error> {
    binding.check()?;
    if matches!(binding.source, WorkspaceSource::Subtree(_)) {
        return Ok((
            WorkspaceEntry {
                source: WorkspaceSource::Subtree(child.clone()),
                ..binding.clone()
            },
            None,
        ));
    }
    let source = binding.volume()?;
    if binding.inherit == WorkspaceInherit::Share && !fresh_session_board(child, binding) {
        return Ok((binding.clone(), None));
    }
    let (destination, key) = spawned_volume(child, binding)?;
    let fork = binding.inherit == WorkspaceInherit::Fork;
    Ok((
        WorkspaceEntry {
            source: crate::agent::WorkspaceSource::Volume(destination.clone()),
            ..binding.clone()
        },
        Some(Planned {
            source: fork.then(|| source.clone()),
            destination,
            key: key.into_bytes(),
            binding: binding.id,
            history: None,
        }),
    ))
}

/// [`Participant::kind`] of a spawn's volume request.
const SPAWN: &str = "acyclic.harness.agent.volumes.v1";

/// One spawn the parent's step requests volumes for. Everything else (the
/// child, its bindings, every fork source and creation) is re-derived from
/// the parent's state by [`children`].
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Requested {
    intent: String,
    history: Option<crate::agent::HistorySelection>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VolumeRequest {
    children: Vec<Requested>,
    history: bool,
}

/// One binding plan for both independent roots and nested children. The
/// destination's path never changes the filesystem fork mechanism.
pub(super) fn fork_bindings(
    child: &AgentPath,
    source: &[WorkspaceEntry],
    history: Option<&crate::agent::HistorySelection>,
) -> Result<Vec<WorkspaceEntry>, Error> {
    Ok(planned_bindings(child, source, history)?.0)
}

fn planned_bindings(
    child: &AgentPath,
    source: &[WorkspaceEntry],
    history: Option<&crate::agent::HistorySelection>,
) -> Result<(Vec<WorkspaceEntry>, Vec<Planned>), Error> {
    let mut bindings = Vec::with_capacity(source.len());
    let mut planned = Vec::new();
    for binding in source {
        let (bound, mut created) = plan(child, binding)?;
        if let Some(created) = &mut created {
            created.history = history.cloned();
        }
        bindings.push(bound);
        planned.extend(created);
    }
    Ok((bindings, planned))
}

/// The one planning function: each requested child's bindings and the volumes
/// its spawn creates, derived only from `parent`'s own bindings.
fn children(
    parent: &AgentState,
    requested: &[Requested],
) -> Result<(Vec<Vec<WorkspaceEntry>>, Vec<Planned>, Vec<AgentPath>), Error> {
    let mut bindings = Vec::with_capacity(requested.len());
    let mut planned = Vec::new();
    let mut journals = Vec::with_capacity(requested.len());
    for Requested { intent, history } in requested {
        let child = parent.path.child(&crate::agent::AgentId::derive(
            &parent.path.id(),
            intent.as_str(),
        ))?;
        let (volumes, created) = planned_bindings(&child, &parent.volumes, history.as_ref())?;
        bindings.push(volumes);
        planned.extend(created);
        journals.push(child);
    }
    Ok((bindings, planned, journals))
}

/// Canonical request bytes: re-encoding must reproduce them exactly.
fn requested(participant: &Participant) -> Result<VolumeRequest, GuestError> {
    if participant.kind != SPAWN {
        return Err(guest("participant is not a spawn volume request"));
    }
    let requested: VolumeRequest = serde_json::from_slice(&participant.plan).map_err(guest)?;
    if serde_json::to_vec(&requested).map_err(guest)? != participant.plan.as_ref() {
        return Err(guest("spawn volume request is not canonical"));
    }
    Ok(requested)
}

impl<P, O> SpawnVolumes for FilesystemSpawnVolumes<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    fn release<'a>(
        &'a self,
        agent: &'a AgentPath,
        bindings: &'a [WorkspaceEntry],
    ) -> BoxProviderFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            if let Some(resources) = &self.cuts {
                resources.release(agent, bindings).await.map_err(invalid)?;
            }
            Ok(())
        })
    }

    fn bind(&self, parent: &AgentState, outs: &mut [Out]) -> Result<Option<Participant>, Error> {
        let requested: Vec<Requested> = outs
            .iter()
            .filter_map(|out| match out {
                Out::Spawn {
                    intent, history, ..
                } => Some(Requested {
                    intent: intent.clone(),
                    history: history.clone(),
                }),
                _ => None,
            })
            .collect();
        let (bindings, _, journals) = children(parent, &requested)?;
        let mut bindings = bindings.into_iter();
        let history = outs.iter().any(|out| matches!(out, Out::Conversation(_)));
        for out in outs {
            if let Out::Spawn { volumes, .. } = out {
                *volumes = bindings
                    .next()
                    .ok_or(Error::Record("spawn bindings are incomplete"))?;
            }
        }
        if journals.is_empty() && !history {
            return Ok(None);
        }
        Ok(Some(Participant {
            kind: SPAWN.to_owned(),
            plan: serde_json::to_vec(&VolumeRequest {
                children: requested,
                history,
            })
            .map_err(|_| Error::Record("spawn volume request does not encode"))?
            .into(),
        }))
    }
}

impl<P, O> Composer for FilesystemSpawnVolumes<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Opens a possibly untrusted spawn request: the parent is the actor's own
    /// committed agent (its last checkpoint, or before one its committed
    /// `ForkAdopted`), its bindings are the committed ones, and every child
    /// and volume is re-derived from them by the same [`children`] as `bind`.
    /// The handler's checkpoint is never trusted for them.
    fn open(
        &self,
        actor: &ActorReference,
        previous: Option<&Checkpoint>,
        delivered: Option<&Record>,
        next: &Checkpoint,
        participant: &Participant,
    ) -> Result<Box<dyn Composition>, GuestError> {
        let requested = requested(participant)?;
        let next = crate::agent::checkpoint_head(next).map_err(guest)?;
        let parent = crate::agent::committed_head(previous, delivered).map_err(guest)?;
        if parent.state.path != next.state.path || parent.table != next.table {
            return Err(guest(
                "spawn parent bindings differ from its committed state",
            ));
        }
        if parent.state.path.id().as_str() != actor.id() {
            return Err(guest("spawn parent is not the publishing actor"));
        }
        let history = next.state.conversation_tail > parent.state.conversation_tail;
        if requested.history != history {
            return Err(guest("spawn volume request creates nothing"));
        }
        Ok(Box::new(StoredSpawn {
            actor: actor.clone(),
            companions: self.companions.clone(),
            host: Arc::clone(&self.host),
            cuts: self.cuts.clone(),
            parent,
            next,
            requested,
            fork: None,
        }))
    }
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static>
    FilesystemSpawnVolumes<P, O>
{
    /// An independent root composes the same fresh private volume, retained
    /// generation forks, lazy cuts and head fences as every child spawn.
    pub(super) fn root_volumes<'a>(
        &'a self,
        source: &'a AgentState,
        child: &'a AgentPath,
        history: Option<&'a crate::agent::HistorySelection>,
        request: CommitRequest,
        policy: Option<crate::approvals::ApprovalPolicy>,
    ) -> BoxProviderFuture<'a, Result<FilesystemCommit, GuestError>> {
        Box::pin(async move {
            let (_, planned) = planned_bindings(child, &source.volumes, history).map_err(guest)?;
            SpawnFork {
                host: Arc::clone(&self.host),
                cuts: self.cuts.clone(),
                planned,
                companions: Vec::new(),
                journals: vec![child.clone()],
                root_policy: policy
                    .map(super::hosting::policy::RootPolicySeed::new)
                    .transpose()
                    .map_err(guest)?,
                parent: source.path.clone(),
                history: None,
                commit: None,
            }
            .compose(request)
            .await
        })
    }
}

/// The table identity is checked at admission; its contents are read only
/// when the composition needs the original volume bindings for sealing.
struct StoredSpawn<P: ?Sized + 'static, O: 'static> {
    actor: ActorReference,
    companions: Option<Arc<dyn crate::agent::Companions>>,
    host: Arc<Host<P, O>>,
    cuts: Option<Arc<dyn VolumeResources>>,
    parent: crate::agent::checkpoint::Head,
    next: crate::agent::checkpoint::Head,
    requested: VolumeRequest,
    fork: Option<SpawnFork<P, O>>,
}

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Composition
    for StoredSpawn<P, O>
{
    fn revalidate(&self, conflicts: &[acyclic_stream::CommitConflict]) -> bool {
        self.fork
            .as_ref()
            .is_some_and(|fork| fork.revalidate(conflicts))
    }
    fn seal<'a>(&'a mut self, request: CommitRequest) -> GuestFuture<'a, CommitRequest> {
        Box::pin(async move {
            if self.fork.is_none() {
                let provider = self.host.filesystem.stream_provider();
                let parent = self
                    .parent
                    .clone()
                    .load(provider.as_ref())
                    .await
                    .map_err(Error::into_guest)?;
                let next = self
                    .next
                    .clone()
                    .restore(parent.volumes.clone())
                    .map_err(guest)?;
                let (bindings, planned, journals) =
                    children(&parent, &self.requested.children).map_err(guest)?;
                if journals.is_empty() && !self.requested.history {
                    return Err(guest("spawn volume request creates nothing"));
                }
                let mut companions = Vec::new();
                if let Some(factory) = &self.companions {
                    for (child, bindings) in journals.iter().zip(&bindings) {
                        let actor = ActorReference::new(
                            self.actor.account().as_bytes(),
                            self.actor.namespace().as_str().as_bytes(),
                            child.id().as_str().as_bytes(),
                        )
                        .map_err(guest)?;
                        companions.extend(factory.create(&actor, child, bindings).map_err(guest)?);
                    }
                }
                self.fork = Some(SpawnFork {
                    host: self.host.clone(),
                    cuts: self.cuts.clone(),
                    planned,
                    companions,
                    journals,
                    root_policy: None,
                    parent: parent.path.clone(),
                    history: self.requested.history.then_some((parent, next)),
                    commit: None,
                });
            }
            self.fork
                .as_mut()
                .ok_or_else(|| guest("spawn plan is missing"))?
                .seal(request)
                .await
        })
    }
    fn settle<'a>(
        self: Box<Self>,
        outcome: Result<&'a CommitOutcome, &'a StreamError>,
    ) -> GuestFuture<'a, ()> {
        Box::pin(async move {
            match self.fork {
                Some(fork) => Box::new(fork).settle(outcome).await,
                None => Ok(()),
            }
        })
    }
}

/// The participant forking and creating one step's child volumes.
struct SpawnFork<P: ?Sized + 'static, O: 'static> {
    host: Arc<Host<P, O>>,
    cuts: Option<Arc<dyn VolumeResources>>,
    planned: Vec<Planned>,
    companions: Vec<acyclic_stream::CommitEffect>,
    journals: Vec<AgentPath>,
    root_policy: Option<super::hosting::policy::RootPolicySeed>,
    parent: AgentPath,
    history: Option<(AgentState, AgentState)>,
    commit: Option<FilesystemCommit>,
}

fn guest(error: impl std::fmt::Display) -> GuestError {
    GuestError::Handler(error.to_string())
}

impl<P, O> SpawnFork<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    async fn compose(&self, request: CommitRequest) -> Result<FilesystemCommit, GuestError> {
        // The original actor and its new companions all append the one
        // activation log. Use the actor publication's canonical hard-effect
        // join, rather than introduce overlapping mutation groups.
        let deadline = request.deadline_unix_millis;
        let mut joined = acyclic_actors::local::PreparedPublish::foreign(request.idempotency_key);
        for effect in request
            .effects
            .into_iter()
            .chain(self.companions.iter().cloned())
        {
            joined.join(effect).map_err(guest)?;
        }
        let mut request = joined.into_request().map_err(guest)?;
        request.deadline_unix_millis = deadline;
        let fs = &self.host.filesystem;
        let cancellation = CancellationToken::new();
        let text = |volume: &VolumeRef| volume.storage_name(&mut None).map_err(guest);
        let name = |volume: &VolumeRef| -> Result<WorkspaceName, GuestError> {
            WorkspaceName::new(text(volume)?, &WorkBudget::UNBOUNDED, &cancellation)
                .map(|receipt| receipt.value)
                .map_err(|failure| crate::Error::guest(failure.error))
        };
        let mut heads = Vec::new();
        for planned in &self.planned {
            if let Some(source) = &planned.source {
                let saved = if source.class() == VolumeClass::Project {
                    match &planned.history {
                        Some(selection) => {
                            let at = selection
                                .conversation
                                .checked_sub(1)
                                .ok_or_else(|| guest("history fork cut is empty"))?;
                            let saved = super::history::read(
                                fs.stream_provider().as_ref(),
                                &self.parent,
                                at,
                            )
                            .await
                            .map_err(crate::Error::guest)?;
                            if saved.conversation != selection.conversation
                                || saved.checkpoint != selection.checkpoint
                            {
                                return Err(guest(
                                    "history fork selection differs from its descriptor",
                                ));
                            }
                            Some(
                                saved
                                    .volumes
                                    .into_iter()
                                    .find(|volume| volume.binding.id == planned.binding)
                                    .ok_or_else(|| {
                                        guest("history fork project binding is absent")
                                    })?,
                            )
                        }
                        None => None,
                    }
                } else {
                    None
                };
                let source_name = match &saved {
                    Some(saved) => saved.retained_workspace.clone(),
                    None => text(source)?,
                };
                let workspace = fs
                    .open_workspace(&source_name, WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| crate::Error::guest(failure.error))?
                    .value;
                let selected = saved.as_ref().map(|saved| saved.retained_generation);
                let cut = match &self.cuts {
                    Some(cuts) => cuts
                        .cut(
                            &source_name,
                            &text(&planned.destination)?,
                            acyclic_fs::OperationId::from_bytes(planned.key),
                            selected,
                        )
                        .await
                        .map_err(GuestError::from)?
                        .or(selected),
                    None => selected,
                };
                let at_head = cut.is_none();
                let generation = match cut {
                    Some(cut) => {
                        workspace
                            .generation(cut, WorkBudget::UNBOUNDED, &cancellation)
                            .await
                            .map_err(|failure| crate::Error::guest(failure.error))?
                            .value
                    }
                    None => {
                        workspace
                            .head(WorkBudget::UNBOUNDED, &cancellation)
                            .await
                            .map_err(|failure| crate::Error::guest(failure.error))?
                            .value
                    }
                };
                heads.push((generation, at_head));
            }
        }
        let mut forks = Vec::with_capacity(heads.len());
        let mut creations = Vec::new();
        let mut sources = heads.iter();
        for planned in &self.planned {
            let destination = name(&planned.destination)?;
            match &planned.source {
                Some(_) => forks.push(ForkSpec {
                    destination,
                    source: &sources
                        .next()
                        .ok_or_else(|| guest("spawn source missing"))?
                        .0,
                    paths: None,
                    idempotency_key: IdempotencyKey::from_bytes(planned.key),
                }),
                None => creations.push(CreateSpec {
                    name: destination,
                    initialization: self.host.volume_initialization(),
                    creation: acyclic_fs::OperationId::from_bytes(planned.key).into(),
                }),
            }
        }
        let journal_creations_start = creations.len();
        for child in &self.journals {
            let journal =
                crate::conversation::journal_volume(self.host.provider.clone(), &child.id())
                    .map_err(guest)?;
            let mut creation: CreateSpec<'_> =
                self.host.volume_create_spec(&journal).map_err(guest)?;
            if let Some(policy) = &self.root_policy {
                creation.creation = acyclic_fs::CreationOperation::populated(
                    creation.creation.operation_id,
                    policy.files(),
                );
            }
            creations.push(creation);
        }
        let mut commit = FilesystemCommit::new(request);
        if let Some((parent, next)) = &self.history {
            super::history::capture(fs, parent, next, self.cuts.as_deref(), &mut commit)
                .await
                .map_err(|error| error.with_context("capturing turn volume history"))?;
        }
        let prepared = fs
            .prepare_fork_many(&forks, &mut commit, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| guest(format!("preparing spawned volume forks: {}", failure.error)))?
            .value;
        // Sources read one after another are one consistent cut only if none
        // advanced before the commit applies: each source forked at its head
        // must still be at that head, or the whole spawn is refused. A
        // retained cut is fixed by its creation operation, so it is not
        // conditioned, and a single source is trivially consistent.
        if heads.len() > 1 {
            for (generation, _) in heads.iter().filter(|(_, at_head)| *at_head) {
                fs.require_head(
                    generation,
                    &mut commit,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(|failure| crate::Error::guest(failure.error))?;
            }
        }
        let created = fs
            .prepare_create_many(
                &creations,
                &mut commit,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| crate::Error::guest(failure.error))?
            .value;
        if let Some(policy) = &self.root_policy {
            let [child] = self.journals.as_slice() else {
                return Err(guest("root policy requires exactly one root journal"));
            };
            let generation = created
                .get(journal_creations_start)
                .ok_or_else(|| guest("root journal creation is missing"))?
                .generation;
            let journal =
                crate::conversation::journal_volume(self.host.provider.clone(), &child.id())
                    .map_err(crate::Error::guest)?;
            policy
                .bind(
                    commit
                        .request_mut()
                        .ok_or_else(|| guest("root creation is sealed"))?,
                    child,
                    &journal,
                    generation,
                )
                .map_err(crate::Error::guest)?;
        }
        for agent in &self.journals {
            super::history::capture_creation(
                fs,
                agent,
                |workspace| {
                    prepared
                        .iter()
                        .find(|entry| entry.workspace == workspace)
                        .map(|entry| entry.generation)
                        .or_else(|| {
                            created
                                .iter()
                                .find(|entry| entry.workspace == workspace)
                                .map(|entry| entry.generation)
                        })
                },
                &mut commit,
            )
            .await?;
        }
        fs.seal_commit(&mut commit, WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|failure| crate::Error::guest(failure.error))?;
        Ok(commit)
    }
}

impl<P, O> Composition for SpawnFork<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    fn revalidate(&self, conflicts: &[acyclic_stream::CommitConflict]) -> bool {
        self.history.is_some()
            && !conflicts.is_empty()
            && conflicts.iter().all(|conflict| {
                matches!(
                    conflict,
                    acyclic_stream::CommitConflict::Head { .. }
                        | acyclic_stream::CommitConflict::Tail { .. }
                )
            })
    }

    fn seal<'a>(&'a mut self, request: CommitRequest) -> GuestFuture<'a, CommitRequest> {
        Box::pin(async move {
            // A sealed commit is frozen: a retry resends its exact bytes and key.
            if self.commit.is_none() {
                self.commit = Some(self.compose(request).await?);
            }
            let commit = self
                .commit
                .as_ref()
                .ok_or_else(|| guest("spawn commit unsealed"))?;
            Ok(commit.request().clone())
        })
    }

    fn settle<'a>(
        self: Box<Self>,
        outcome: Result<&'a CommitOutcome, &'a StreamError>,
    ) -> GuestFuture<'a, ()> {
        Box::pin(async move {
            let Self { host, commit, .. } = *self;
            // A definite refusal committed nothing: dropping the commit
            // releases its prepared holds.
            if let (Some(commit), Ok(outcome)) = (commit, outcome) {
                host.filesystem
                    .settle_commit(
                        commit,
                        outcome.clone(),
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| crate::Error::guest(failure.error))?;
            }
            Ok(())
        })
    }
}
