//! Host-side lifecycle of the provider resources an agent holds (its Machines).
//!
//! Pausing an agent suspends them through [`super::AgentHost::pause`]. Archive
//! releases them in the agent's own closing step: the handler asks for the
//! [`CLOSE`] participant, and the host's [`AgentComposer`] re-derives the close
//! from the committed checkpoints and runs [`AgentLifecycle::close`] before the
//! step's commit is first submitted. So an agent whose closing step committed
//! holds nothing, credentials never enter the handler, and a retried step
//! repeats only idempotent releases.

use super::{AgentPath, AgentState, Error, SpawnVolumes};
use acyclic_actors::{
    addressing::ActorReference,
    guest::{Checkpoint, Composer, Composition, GuestError, GuestFuture, Participant},
};
use acyclic_stream::{BoxProviderFuture, CommitOutcome, CommitRequest, Record, StreamError};
use std::sync::Arc;

/// Participant kind of a closing step's release of the agent's resources.
pub const CLOSE: &str = "acyclic.harness.agent.close.v1";

/// Releases or parks what an agent holds outside its streams.
pub trait AgentLifecycle: acyclic_stream::ProviderPlatform + 'static {
    /// The agent is paused: park what it holds (suspend its Machines).
    fn pause<'a>(&'a self, agent: &'a AgentPath) -> BoxProviderFuture<'a, Result<(), Error>>;

    /// The agent is closing: release what it holds (destroy its Machines,
    /// revoke its grants). Runs again, idempotently, when the step retries.
    fn close<'a>(&'a self, agent: &'a AgentPath) -> BoxProviderFuture<'a, Result<(), Error>>;
}

/// The participant a closing step asks for.
pub(super) fn close_participant(agent: &AgentPath) -> Participant {
    Participant {
        kind: CLOSE.to_owned(),
        plan: agent.as_str().as_bytes().to_vec().into(),
    }
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
        let lifecycle = self
            .lifecycle
            .clone()
            .ok_or_else(|| guest("this host releases no agent resources"))?;
        // Re-derived from the committed checkpoints, never from the request:
        // the publishing agent is open before this step and closed after it.
        let closed: AgentState = super::checkpoint_state(next).map_err(guest)?;
        let was_open = match previous {
            Some(previous) => !super::checkpoint_state(previous).map_err(guest)?.closed,
            None => true,
        };
        if !closed.closed
            || !was_open
            || closed.path.id().as_str() != actor.id()
            || participant.plan.as_ref() != closed.path.as_str().as_bytes()
        {
            return Err(guest("close request is not this agent's closing step"));
        }
        Ok(Box::new(Closing {
            lifecycle,
            agent: closed.path,
        }))
    }
}

/// Releases one closing agent's resources before its step first commits.
struct Closing {
    lifecycle: Arc<dyn AgentLifecycle>,
    agent: AgentPath,
}

impl Composition for Closing {
    fn seal<'a>(&'a mut self, request: CommitRequest) -> GuestFuture<'a, CommitRequest> {
        Box::pin(async move {
            self.lifecycle.close(&self.agent).await.map_err(guest)?;
            Ok(request)
        })
    }

    fn settle<'a>(
        self: Box<Self>,
        _outcome: Result<&'a CommitOutcome, &'a StreamError>,
    ) -> GuestFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}
