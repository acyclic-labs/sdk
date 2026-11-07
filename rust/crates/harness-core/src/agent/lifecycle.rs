//! Host-side lifecycle of the provider resources and physical volumes an agent holds.
//!
//! Pausing an agent suspends them through [`super::AgentHost::pause`]. Archive
//! releases them in the agent's own closing step: the handler asks for the
//! [`CLOSE`] participant, and the host's [`AgentComposer`] re-derives the close
//! from the committed checkpoints and releases its original volumes, then runs
//! [`AgentLifecycle::close`] before the
//! step's commit is first submitted. So an agent whose closing step committed
//! holds nothing, credentials never enter the handler, and a retried step
//! repeats only idempotent releases.

use super::{AgentPath, AgentState, Error, SpawnVolumes, VolumeBinding};
use acyclic_actors::{
    addressing::ActorReference,
    guest::{Checkpoint, Composer, Composition, GuestError, GuestFuture, Participant},
};
use acyclic_stream::{BoxProviderFuture, CommitOutcome, CommitRequest, Record, StreamError};
use std::sync::Arc;

/// Participant kind of a closing step's release of the agent's resources.
pub const CLOSE: &str = "acyclic.harness.agent.close.v2";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CloseRequest {
    agent: AgentPath,
    volumes: Option<(String, Vec<u8>)>,
}

/// Releases or parks what an agent holds outside its streams.
pub trait AgentLifecycle: acyclic_stream::ProviderPlatform + 'static {
    /// The agent is paused: park what it holds (suspend its Machines).
    fn pause<'a>(&'a self, agent: &'a AgentPath) -> BoxProviderFuture<'a, Result<(), Error>>;

    /// The agent is closing: release what it holds (destroy its Machines,
    /// revoke its grants). Runs again, idempotently, when the step retries.
    fn close<'a>(&'a self, agent: &'a AgentPath) -> BoxProviderFuture<'a, Result<(), Error>>;
}

/// The participant a closing step asks for.
pub(super) fn close_participant(
    agent: &AgentPath,
    volumes: Option<Participant>,
) -> Result<Participant, Error> {
    let request = CloseRequest {
        agent: agent.clone(),
        volumes: volumes.map(|participant| (participant.kind, participant.plan.to_vec())),
    };
    Ok(Participant {
        kind: CLOSE.to_owned(),
        plan: crate::contract::canonical_json_bytes(&request)
            .map_err(|_| Error::Record("close request does not encode"))?
            .into(),
    })
}

/// The host composer of agent steps: spawned volumes and closing releases.
pub struct AgentComposer {
    volumes: Option<Arc<dyn SpawnVolumes>>,
    lifecycle: Option<Arc<dyn AgentLifecycle>>,
}

impl AgentComposer {
    /// Composes spawns through `volumes` and closes through `lifecycle`.
    #[must_use]
    pub fn new(
        volumes: Option<Arc<dyn SpawnVolumes>>,
        lifecycle: Option<Arc<dyn AgentLifecycle>>,
    ) -> Self {
        Self { volumes, lifecycle }
    }
}

fn guest(error: impl std::fmt::Display) -> GuestError {
    GuestError::Handler(error.to_string())
}

impl Composer for AgentComposer {
    fn open(
        &self,
        actor: &ActorReference,
        previous: Option<&Checkpoint>,
        delivered: Option<&Record>,
        next: &Checkpoint,
        participant: &Participant,
    ) -> Result<Box<dyn Composition>, GuestError> {
        if participant.kind != CLOSE {
            return match &self.volumes {
                Some(volumes) => volumes.open(actor, previous, delivered, next, participant),
                None => Err(guest("this host composes no spawned volumes")),
            };
        }
        if self.lifecycle.is_none() && self.volumes.is_none() {
            return Err(guest("this host releases no agent resources"));
        }
        // Re-derived from the committed checkpoints, never from the request:
        // the publishing agent is open before this step and closed after it.
        let closed: AgentState = super::checkpoint_state(next).map_err(guest)?;
        let request: CloseRequest = serde_json::from_slice(&participant.plan).map_err(guest)?;
        if crate::contract::canonical_json_bytes(&request).map_err(guest)?
            != participant.plan.as_ref()
        {
            return Err(guest("close request is not canonical"));
        }
        let original = super::checkpoint::committed_state(previous, delivered).map_err(guest)?;
        if !closed.closed
            || original.closed
            || original.path != closed.path
            || original.volumes != closed.volumes
            || closed.path.id().as_str() != actor.id()
            || request.agent != closed.path
        {
            return Err(guest("close request is not this agent's closing step"));
        }
        let volumes = match request.volumes {
            Some((kind, plan)) => Some(
                self.volumes
                    .as_ref()
                    .ok_or_else(|| guest("this host composes no volumes"))?
                    .open(
                        actor,
                        previous,
                        delivered,
                        next,
                        &Participant {
                            kind,
                            plan: plan.into(),
                        },
                    )?,
            ),
            None => None,
        };
        Ok(Box::new(Closing {
            lifecycle: self.lifecycle.clone(),
            owner: self.volumes.clone(),
            bindings: original.volumes,
            agent: closed.path,
            volumes,
        }))
    }
}

/// Releases one closing agent's resources before its step first commits.
struct Closing {
    lifecycle: Option<Arc<dyn AgentLifecycle>>,
    owner: Option<Arc<dyn SpawnVolumes>>,
    bindings: Vec<VolumeBinding>,
    agent: AgentPath,
    volumes: Option<Box<dyn Composition>>,
}

impl Composition for Closing {
    fn revalidate(&self, conflicts: &[acyclic_stream::CommitConflict]) -> bool {
        self.volumes
            .as_ref()
            .is_some_and(|volumes| volumes.revalidate(conflicts))
    }
    fn seal<'a>(&'a mut self, request: CommitRequest) -> GuestFuture<'a, CommitRequest> {
        Box::pin(async move {
            let request = match &mut self.volumes {
                Some(volumes) => volumes.seal(request).await?,
                None => request,
            };
            if let Some(owner) = &self.owner {
                owner
                    .release(&self.agent, &self.bindings)
                    .await
                    .map_err(guest)?;
            }
            if let Some(lifecycle) = &self.lifecycle {
                lifecycle.close(&self.agent).await.map_err(guest)?;
            }
            Ok(request)
        })
    }

    fn settle<'a>(
        self: Box<Self>,
        outcome: Result<&'a CommitOutcome, &'a StreamError>,
    ) -> GuestFuture<'a, ()> {
        Box::pin(async move {
            match self.volumes {
                Some(volumes) => volumes.settle(outcome).await,
                None => Ok(()),
            }
        })
    }
}
