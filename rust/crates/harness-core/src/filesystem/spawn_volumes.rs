//! [`SpawnVolumes`] on Filesystem volumes placed in the agents' own stream: a
//! child forks each parent volume at its current head, except its private
//! volume, which starts empty, and a session-shared volume, which it shares.
//! Every fork and creation joins the parent step's commit through one sealed
//! [`FilesystemCommit`], so the children and their volumes appear together.

use super::FilesystemHost;
use crate::{
    agent::{AgentPath, AgentState, Error, Out, SpawnVolumes, VolumeBinding},
    conversation::{VolumeClass, VolumeOwner, VolumeRef},
};
use acyclic_actors::guest::{Composition, GuestError, GuestFuture};
use acyclic_fs::{
    AsyncObjectStore, CancellationToken, CreateSpec, FilesystemCommit, ForkSpec, IdempotencyKey,
    StreamAuthorityStore, WorkBudget, WorkspaceName, distributed::AuthorityProvider,
};
use acyclic_stream::{CommitOutcome, CommitRequest};
use std::sync::Arc;

type Host<P, O> = FilesystemHost<StreamAuthorityStore<P>, O>;

/// Spawned volumes of agents whose store shares the volumes' authority stream.
pub struct FilesystemSpawnVolumes<P: 'static, O: 'static> {
    host: Arc<Host<P, O>>,
}

impl<P, O> FilesystemSpawnVolumes<P, O> {
    /// Volumes on `host`, whose authority must be the agents' stream provider.
    #[must_use]
    pub const fn new(host: Arc<Host<P, O>>) -> Self {
        Self { host }
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

/// The child's binding of `parent`'s volume `binding`, and how it comes to exist.
fn plan(
    child: &AgentPath,
    binding: &VolumeBinding,
) -> Result<(VolumeBinding, Option<Planned>), Error> {
    let source = &binding.volume;
    if source.class() == VolumeClass::SessionShared {
        return Ok((binding.clone(), None));
    }
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
    let fork = binding.name != super::PRIVATE_VOLUME;
    Ok((
        VolumeBinding {
            name: binding.name.clone(),
            volume: destination.clone(),
        },
        Some(Planned {
            source: fork.then(|| source.clone()),
            destination,
            key: key(child, &binding.name),
        }),
    ))
}

impl<P, O> SpawnVolumes for FilesystemSpawnVolumes<P, O>
where
    P: AuthorityProvider + 'static,
    O: AsyncObjectStore + 'static,
{
    fn bind(
        &self,
        parent: &AgentState,
        outs: &mut [Out],
    ) -> Result<Option<Box<dyn Composition>>, Error> {
        let mut planned = Vec::new();
        for out in outs {
            let Out::Spawn {
                intent, volumes, ..
            } = out
            else {
                continue;
            };
            let child = parent
                .path
                .child(&crate::agent::AgentId::derive(&parent.path.id(), intent.as_str()))?;
            volumes.clear();
            for binding in &parent.volumes {
                let (bound, created) = plan(&child, binding)?;
                volumes.push(bound);
                planned.extend(created);
            }
        }
        Ok((!planned.is_empty()).then(|| {
            Box::new(SpawnFork {
                host: Arc::clone(&self.host),
                planned,
                commit: None,
            }) as Box<dyn Composition>
        }))
    }
}

/// The participant forking and creating one step's child volumes.
struct SpawnFork<P: 'static, O: 'static> {
    host: Arc<Host<P, O>>,
    planned: Vec<Planned>,
    commit: Option<FilesystemCommit>,
}

fn guest(error: impl std::fmt::Display) -> GuestError {
    GuestError::Handler(error.to_string())
}

impl<P, O> SpawnFork<P, O>
where
    P: AuthorityProvider + 'static,
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
                let head = workspace
                    .head(WorkBudget::UNBOUNDED, &cancellation)
                    .await
                    .map_err(|failure| guest(failure.error))?
                    .value;
                heads.push(head);
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
                    source: sources.next().ok_or_else(|| guest("spawn source missing"))?,
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
        fs.prepare_create_many(&creations, &mut commit, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| guest(failure.error))?;
        fs.seal_commit(&mut commit, WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|failure| guest(failure.error))?;
        Ok(commit)
    }
}

impl<P, O> Composition for SpawnFork<P, O>
where
    P: AuthorityProvider + 'static,
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

    fn settle<'a>(self: Box<Self>, outcome: &'a CommitOutcome) -> GuestFuture<'a, ()> {
        Box::pin(async move {
            let Self { host, commit, .. } = *self;
            if let Some(commit) = commit {
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
