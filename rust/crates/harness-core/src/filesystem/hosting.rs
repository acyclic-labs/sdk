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
        AgentHost, AgentId, AgentPath, Error, ExecutorDriver, ForkAdopted, IntentId, Sender, Spawn,
        VolumeBinding,
    },
    conversation::{ContentAccess, ContentGrant, ContentPublisher, FileRef, VolumeOperation},
    core::{Authority, AuthorityRoot, Scope},
    resources::ProviderRef,
};
use acyclic_actors::{addressing::ActorReference, domain::ActorLimits, local::Clock};
use acyclic_fs::{
    AsyncObjectStore, CancellationToken, CreateSpec, CreationOperation, EmbeddedCapabilities,
    FilesystemCommit, FilesystemCommitOutcome, Fs, StreamAuthorityStore, WorkBudget, WorkspaceName,
    distributed::AuthorityProvider,
};
use std::sync::Arc;

type Host<P, O> = FilesystemHost<StreamAuthorityStore<P>, O>;

#[cfg(all(target_os = "linux", feature = "native-execution"))]
mod guest;
#[cfg(all(target_os = "linux", feature = "native-execution"))]
pub use guest::LocalGuestEnrollment;

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
pub struct Placement<P, O> {
    /// The admitted stream instance.
    pub stream: Arc<P>,
    /// Volume content objects.
    pub objects: O,
    /// What the volume store guarantees.
    pub capabilities: EmbeddedCapabilities,
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
}

/// A host's custody, a caller's admission and the one placement they act on,
/// bound once. Every root, turn and spawn of this binding uses this placement.
pub struct Bound<P: AuthorityProvider + 'static, O: 'static> {
    issuer: AuthorityRoot,
    admission: TurnAdmission,
    within: ActorReference,
    maximum_bytes: u64,
    agents: Arc<AgentHost<P>>,
    volumes: Arc<Host<P, O>>,
}

impl<P, O> Bound<P, O>
where
    P: AuthorityProvider + 'static,
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
            issuer,
            TurnAdmission::Admitted {
                audience: caller.audience,
                scope: caller.scope,
            },
            placement,
            turns,
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
        Self::new(issuer, TurnAdmission::LocalRoot, placement, turns)
    }

    fn new(
        issuer: AuthorityRoot,
        admission: TurnAdmission,
        placement: Placement<P, O>,
        turns: Turns,
    ) -> Result<Self, Error> {
        let Placement {
            stream,
            objects,
            capabilities,
            provider,
            within,
        } = placement;
        let volumes = FilesystemHost::new(
            Fs::new(
                StreamAuthorityStore::new(Arc::clone(&stream)),
                objects,
                capabilities,
            ),
            &provider,
            None,
        )
        .map_err(turn)?;
        let maximum_bytes = turns.limits.maximum_bytes;
        let driver = ExecutorDriver::new(Arc::new(FilesystemTurnHost::new(
            Arc::clone(&stream),
            Arc::clone(&volumes),
            issuer.clone(),
            admission.clone(),
            turns.executors,
            turns.limits,
        )));
        let spawns = Arc::new(FilesystemSpawnVolumes::new(
            Arc::clone(&volumes),
            issuer.clone(),
            admission.clone(),
            maximum_bytes,
        ));
        let agents = Arc::new(AgentHost::with_spawn_volumes(
            stream,
            driver,
            within.clone(),
            turns.actors,
            turns.clock,
            spawns,
        )?);
        Ok(Self {
            issuer,
            admission,
            within,
            maximum_bytes,
            agents,
            volumes,
        })
    }

    /// The admission every agent of this binding derives its scope from, for
    /// its turns ([`super::FilesystemTurnHost`]) and spawns
    /// ([`super::FilesystemSpawnVolumes`]).
    #[must_use]
    pub const fn admission(&self) -> &TurnAdmission {
        &self.admission
    }

    /// The agents this binding creates and hosts.
    #[must_use]
    pub const fn agents(&self) -> &Arc<AgentHost<P>> {
        &self.agents
    }

    /// Account and namespace of this binding's agent actors.
    #[must_use]
    pub const fn within(&self) -> &ActorReference {
        &self.within
    }

    /// Stages `text` once on `agent`'s private volume under the agent's
    /// admitted authority, keyed by the agent, the input `kind`, its sender
    /// and `intent`,
    /// so every retry names the same file.
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
        text: &str,
    ) -> Result<FileRef, Error> {
        let state = self.agents.state(agent).await?;
        let authority = agent_authority(&self.issuer, &self.admission, agent, &state.volumes, &[])
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
        let publisher = FilesystemContentPublisher::new(
            Arc::clone(&self.volumes),
            &authority.private.volume,
            &authority.verifier,
            &authority.scope,
            self.maximum_bytes,
            None,
        )
        .map_err(turn)?;
        let path = format!("inputs/{operation}.txt");
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
        Ok(staged.0)
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
        let RootRequest {
            agent,
            fresh,
            retained,
            brief,
            policy,
        } = request;
        let path = AgentPath::root(&agent);
        let owner = crate::AgentId::from_bytes(agent.to_bytes());
        let volumes: Vec<VolumeBinding> = fresh.iter().chain(&retained).cloned().collect();
        {
            let authority = agent_authority(&self.issuer, &self.admission, &path, &volumes, &[])
                .map_err(turn)?;
            for binding in &fresh {
                ContentGrant::verify(
                    &authority.verifier,
                    &authority.scope,
                    &binding.volume,
                    VolumeOperation::Write,
                    u64::MAX,
                    &mut None,
                )
                .map_err(turn)?;
                // A structured `filesystem:` grant passes that check by its
                // label alone, so the filesystem's own decision over its
                // ceiling must admit writing each fresh volume too.
                super::agent_turns::require_structured_write(
                    &authority.scope,
                    owner,
                    &binding.volume,
                )
                .map_err(turn)?;
            }
        }
        let Spawn { child, request } = self.agents.prepare_root(
            intent,
            ForkAdopted {
                agent,
                parent: None,
                forked_at: 0,
                conversation_tail: 0,
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
