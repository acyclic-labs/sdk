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
        AgentPath, AgentRecord as _, AgentState, Error, InboxRecord, IntentId, MAX_OPEN_CHILDREN,
        Out, SpawnVolumes, VolumeBinding,
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

type Host<P, O> = FilesystemHost<StreamAuthorityStore<P>, O>;

/// Spawned volumes of agents whose store shares the volumes' authority stream.
pub struct FilesystemSpawnVolumes<P: ?Sized + 'static, O: 'static> {
    host: Arc<Host<P, O>>,
    cuts: Option<Arc<dyn ForkCuts>>,
}

/// The generation a spawn forks each source volume at. A host that keeps
/// state beside a volume (a lazy checkout's fork cut) retains that cut for
/// `destination` under the spawn's creation `operation`, reusing a cut a
/// stopped attempt retained, and returns its generation; `None` forks the
/// source's head.
pub trait ForkCuts: acyclic_stream::ProviderPlatform + 'static {
    /// The generation of `source` that `destination` forks.
    fn cut<'a>(
        &'a self,
        source: &'a VolumeRef,
        destination: &'a VolumeRef,
        operation: acyclic_fs::OperationId,
    ) -> BoxProviderFuture<'a, crate::Result<Option<acyclic_fs::GenerationId>>>;
}

impl<P: ?Sized, O> FilesystemSpawnVolumes<P, O> {
    /// Volumes on `host`, whose authority must be the agents' stream
    /// provider.
    #[must_use]
    pub fn new(host: Arc<Host<P, O>>) -> Self {
        Self { host, cuts: None }
    }

    /// Forks each source at the generation `cuts` chooses.
    #[must_use]
    pub fn with_fork_cuts(mut self, cuts: Arc<dyn ForkCuts>) -> Self {
        self.cuts = Some(cuts);
        self
    }
}

/// One child volume: forked from `source`, or created empty without one.
struct Planned {
    source: Option<VolumeRef>,
    destination: VolumeRef,
    key: [u8; 16],
}

fn key(child: &AgentPath, name: &str) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.agent.spawn-volume.v1");
    hasher.update(child.as_str().as_bytes());
    hasher.update(&[0]);
    hasher.update(name.as_bytes());
    let mut bytes = [0; 16];
    hasher.finalize_xof().fill(&mut bytes);
    bytes
}

fn invalid(error: crate::Error) -> Error {
    Error::Turn(Box::new(error))
}

/// The volume `child` binds in place of its parent's non-shared `binding`,
/// and the creation operation of the spawn commit that creates it. A host
/// that keeps state beside a forked volume prepares it through [`ForkCuts`].
fn spawned_volume(
    child: &AgentPath,
    binding: &VolumeBinding,
) -> Result<(VolumeRef, acyclic_fs::OperationId), Error> {
    let source = &binding.volume;
    let owner = match source.owner() {
        VolumeOwner::Agent(_) => {
            VolumeOwner::Agent(crate::AgentId::from_bytes(child.id().to_bytes()))
        }
        other => other.clone(),
    };
    let destination = VolumeRef::new(
        source.provider().clone(),
        format!("{}-{}", binding.name, child.id().as_str()),
        source.class(),
        owner,
        &mut None,
    )
    .map_err(invalid)?;
    Ok((
        destination,
        acyclic_fs::OperationId::from_bytes(key(child, &binding.name)),
    ))
}

/// The volume spawned child `child` binds in place of its parent's volume
/// `binding`: the one naming a host resolving a child's forked volume (its
/// `agents/<name>` branch) shares with the spawn that created it.
///
/// # Errors
/// When the derived volume reference is invalid.
pub fn spawned_binding(child: &AgentPath, binding: &VolumeBinding) -> Result<VolumeRef, Error> {
    if binding.volume.class() == VolumeClass::SessionShared {
        return Ok(binding.volume.clone());
    }
    Ok(spawned_volume(child, binding)?.0)
}

/// The child's binding of `parent`'s volume `binding`, and how it comes to exist.
fn plan(
    child: &AgentPath,
    binding: &VolumeBinding,
) -> Result<(VolumeBinding, Option<Planned>), Error> {
    let source = &binding.volume;
    if source.class() == VolumeClass::SessionShared {
        return Ok((binding.clone(), None));
    }
    let (destination, key) = spawned_volume(child, binding)?;
    let fork = binding.name != super::PRIVATE_VOLUME;
    Ok((
        VolumeBinding {
            name: binding.name.clone(),
            volume: destination.clone(),
        },
        Some(Planned {
            source: fork.then(|| source.clone()),
            destination,
            key: key.into_bytes(),
        }),
    ))
}

/// [`Participant::kind`] of a spawn's volume request.
const SPAWN: &str = "acyclic.harness.agent.spawn-volumes.v1";

/// One spawn the parent's step requests volumes for. Everything else (the
/// child, its bindings, every fork source and creation) is re-derived from
/// the parent's state by [`children`].
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Requested {
    intent: IntentId,
}

/// The one planning function: each requested child's bindings and the volumes
/// its spawn creates, derived only from `parent`'s own bindings.
fn children(
    parent: &AgentState,
    requested: &[Requested],
) -> Result<(Vec<Vec<VolumeBinding>>, Vec<Planned>), Error> {
    if requested.len() > MAX_OPEN_CHILDREN {
        return Err(Error::Record("spawn requests too many children"));
    }
    let mut bindings = Vec::with_capacity(requested.len());
    let mut planned = Vec::new();
    for Requested { intent } in requested {
        let child = parent.path.child(&crate::agent::AgentId::derive(
            &parent.path.id(),
            intent.as_str(),
        ))?;
        let mut volumes = Vec::with_capacity(parent.volumes.len());
        for binding in &parent.volumes {
            let (bound, created) = plan(&child, binding)?;
            volumes.push(bound);
            planned.extend(created);
        }
        bindings.push(volumes);
    }
    Ok((bindings, planned))
}

/// Canonical request bytes: re-encoding must reproduce them exactly.
fn requested(participant: &Participant) -> Result<Vec<Requested>, GuestError> {
    if participant.kind != SPAWN {
        return Err(guest("participant is not a spawn volume request"));
    }
    let requested: Vec<Requested> = serde_json::from_slice(&participant.plan).map_err(guest)?;
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
    fn bind(&self, parent: &AgentState, outs: &mut [Out]) -> Result<Option<Participant>, Error> {
        let requested: Vec<Requested> = outs
            .iter()
            .filter_map(|out| match out {
                Out::Spawn { intent, .. } => Some(Requested {
                    intent: intent.clone(),
                }),
                _ => None,
            })
            .collect();
        let (bindings, planned) = children(parent, &requested)?;
        let mut bindings = bindings.into_iter();
        for out in outs {
            if let Out::Spawn { volumes, .. } = out {
                *volumes = bindings
                    .next()
                    .ok_or(Error::Record("spawn bindings are incomplete"))?;
            }
        }
        if planned.is_empty() {
            return Ok(None);
        }
        Ok(Some(Participant {
            kind: SPAWN.to_owned(),
            plan: serde_json::to_vec(&requested)
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
        let next = crate::agent::checkpoint_state(next).map_err(guest)?;
        let parent = match (previous, delivered) {
            (Some(previous), _) => crate::agent::checkpoint_state(previous).map_err(guest)?,
            (None, Some(record)) => match InboxRecord::decode(&record.value).map_err(guest)? {
                InboxRecord::ForkAdopted(adopted) => {
                    let path = match &adopted.parent {
                        Some(parent) => parent.child(&adopted.agent).map_err(guest)?,
                        None => AgentPath::root(&adopted.agent),
                    };
                    AgentState::adopted(path, &adopted)
                }
                _ => return Err(guest("first agent input is not ForkAdopted")),
            },
            (None, None) => return Err(guest("spawn has no committed parent")),
        };
        if parent.path != next.path || parent.volumes != next.volumes {
            return Err(guest(
                "spawn parent bindings differ from its committed state",
            ));
        }
        if parent.path.id().as_str() != actor.id() {
            return Err(guest("spawn parent is not the publishing actor"));
        }
        let (_, planned) = children(&parent, &requested).map_err(guest)?;
        if planned.is_empty() {
            return Err(guest("spawn volume request creates nothing"));
        }
        Ok(Box::new(SpawnFork {
            host: Arc::clone(&self.host),
            cuts: self.cuts.clone(),
            planned,
            commit: None,
        }))
    }
}

/// The participant forking and creating one step's child volumes.
struct SpawnFork<P: ?Sized + 'static, O: 'static> {
    host: Arc<Host<P, O>>,
    cuts: Option<Arc<dyn ForkCuts>>,
    planned: Vec<Planned>,
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
        let fs = &self.host.filesystem;
        let cancellation = CancellationToken::new();
        let text = |volume: &VolumeRef| volume.storage_name(&mut None).map_err(guest);
        let name = |volume: &VolumeRef| -> Result<WorkspaceName, GuestError> {
            WorkspaceName::new(text(volume)?, &WorkBudget::UNBOUNDED, &cancellation)
                .map(|receipt| receipt.value)
                .map_err(|failure| guest(failure.error))
        };
        let mut heads = Vec::new();
        for planned in &self.planned {
            if let Some(source) = &planned.source {
                let workspace = fs
                    .open_workspace(text(source)?, WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| guest(failure.error))?
                    .value;
                let cut = match &self.cuts {
                    Some(cuts) => cuts
                        .cut(
                            source,
                            &planned.destination,
                            acyclic_fs::OperationId::from_bytes(planned.key),
                        )
                        .await
                        .map_err(guest)?,
                    None => None,
                };
                let at_head = cut.is_none();
                let generation = match cut {
                    Some(cut) => {
                        workspace
                            .generation(cut, WorkBudget::UNBOUNDED, &cancellation)
                            .await
                            .map_err(|failure| guest(failure.error))?
                            .value
                    }
                    None => {
                        workspace
                            .head(WorkBudget::UNBOUNDED, &cancellation)
                            .await
                            .map_err(|failure| guest(failure.error))?
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
        let mut commit = FilesystemCommit::new(request);
        fs.prepare_fork_many(&forks, &mut commit, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| guest(failure.error))?;
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
                .map_err(|failure| guest(failure.error))?;
            }
        }
        fs.prepare_create_many(
            &creations,
            &mut commit,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|failure| guest(failure.error))?;
        fs.seal_commit(&mut commit, WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|failure| guest(failure.error))?;
        Ok(commit)
    }
}

impl<P, O> Composition for SpawnFork<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
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
                    .map_err(|failure| guest(failure.error))?;
            }
            Ok(())
        })
    }
}
