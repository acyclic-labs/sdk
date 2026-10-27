//! The admitted root of one hosted agent tree: the association of a host's
//! signing custody, the admission its platform verified for the caller, and
//! the actual agent and volume stores, made once and used for every root the
//! caller creates. The same [`Bound`] serves local, self-hosted and managed
//! hosting; only how the platform verifies its caller differs.
//!
//! A root's fresh volumes and its adoption are one commit:
//! [`Bound::prepare_root`] verifies every authority the root needs before any
//! IO, then prepares that commit, and [`Bound::create_root`] is exactly that
//! commit submitted once.

use super::{FilesystemHost, TurnAdmission, agent_authority};
use crate::{
    agent::{
        AgentHost, AgentId, AgentPath, Error, ForkAdopted, IntentId, Spawn, Submitted,
        VolumeBinding, submit,
    },
    conversation::{ContentGrant, FileRef, VolumeOperation},
    core::{Authority, AuthorityRoot, Scope},
};
use acyclic_fs::{
    AsyncObjectStore, CancellationToken, CreateSpec, CreationOperation, FilesystemCommit,
    StreamAuthorityStore, WorkBudget, WorkspaceName, distributed::AuthorityProvider,
};
use std::sync::Arc;

type Host<P, O> = FilesystemHost<StreamAuthorityStore<P>, O>;

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

/// A prepared root: the one commit creating its fresh volumes, its inbox with
/// `ForkAdopted`, its actor state and its activation. Submitting
/// [`Self::request`] once is the creation; after
/// [`Error::Unknown`] its exact bytes are resubmitted.
#[derive(Clone, Debug)]
pub struct Prepared {
    /// The root agent's path.
    pub child: AgentPath,
    /// The whole creation, sealed.
    pub request: acyclic_stream::CommitRequest,
}

/// A host's custody, a caller's admission and the stores they act on, bound
/// once. Every root and turn of this binding uses these same stores.
pub struct Bound<P: AuthorityProvider + 'static, O: 'static> {
    issuer: AuthorityRoot,
    admission: TurnAdmission,
    agents: Arc<AgentHost<P>>,
    volumes: Arc<Host<P, O>>,
}

impl<P, O> Bound<P, O>
where
    P: AuthorityProvider + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Binds `caller`, as the host's platform verified it, to the host's
    /// signing custody `issuer` and its stores. `volumes`' authority must be
    /// `agents`' stream, so a root's volumes and adoption commit together.
    #[must_use]
    pub fn admitted(
        issuer: AuthorityRoot,
        caller: VerifiedCaller,
        agents: Arc<AgentHost<P>>,
        volumes: Arc<Host<P, O>>,
    ) -> Self {
        Self::new(
            issuer,
            TurnAdmission::Admitted {
                audience: caller.audience,
                scope: caller.scope,
            },
            agents,
            volumes,
        )
    }

    /// The explicit self-host enrollment: the host's own custody is the root
    /// of every agent it creates. Never selected by a request.
    #[must_use]
    pub fn local_root(
        issuer: AuthorityRoot,
        agents: Arc<AgentHost<P>>,
        volumes: Arc<Host<P, O>>,
    ) -> Self {
        Self::new(issuer, TurnAdmission::LocalRoot, agents, volumes)
    }

    const fn new(
        issuer: AuthorityRoot,
        admission: TurnAdmission,
        agents: Arc<AgentHost<P>>,
        volumes: Arc<Host<P, O>>,
    ) -> Self {
        Self {
            issuer,
            admission,
            agents,
            volumes,
        }
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

    /// Prepares root `request` under intent `intent`.
    ///
    /// Before any IO it verifies the caller's admission for the root (the
    /// [`agent_authority`] check turns make: authentic, live, this audience
    /// and agent, and writing the private volume) and that the admission
    /// writes every fresh volume; a read-only or mismatched admission is
    /// refused here, having read and written nothing. Retained volumes keep
    /// exactly the access the admission grants them. Then it prepares the one
    /// commit creating the fresh volumes and the root, reading only the
    /// volumes' authority.
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
            .map_err(|failure| turn(super::map_error(failure.error)))?;
        Ok(Prepared {
            child,
            request: commit.request().clone(),
        })
    }

    /// Creates root `request`: exactly [`Self::prepare_root`] submitted once,
    /// then hosted. Retrying the same `intent` replays exactly; on
    /// [`Error::Unknown`], resubmit its exact request with [`submit`].
    ///
    /// # Errors
    /// As [`Self::prepare_root`], or when another creation holds the agent.
    pub async fn create_root(
        &self,
        intent: &IntentId,
        request: RootRequest,
    ) -> Result<AgentPath, Error> {
        let Prepared { child, request } = self.prepare_root(intent, request).await?;
        match submit(&**self.agents.provider(), request).await? {
            Submitted::Committed(_) => {}
            Submitted::Conflict(_) | Submitted::InboxMoved(_) | Submitted::RecipientClosed(_) => {
                return Err(Error::Record("agent exists with another creation"));
            }
        }
        self.agents.host(&child)?;
        Ok(child)
    }
}

fn turn(error: crate::Error) -> Error {
    Error::Turn(Box::new(error))
}
