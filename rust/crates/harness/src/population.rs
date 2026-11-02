//! Live population execution over the ordinary history fork and status follow.
//! The caller reads the arms file from its volume and writes each yielded row
//! to a results file; this module owns no tools, background task or transport.

use acyclic_harness_client::{
    AgentClientError, AgentCommand, AgentControls, AgentExchangeError, AgentReply,
    PreparedAgentInvocation, PreparedCommand,
};
use acyclic_harness_core::{
    agent::{AgentPath, Delivered, Phase, StatusRecord},
    experiments::{Population, Trial},
    model::ModelContent,
};
use futures::{Stream, StreamExt as _, stream};
use serde::Serialize;
use std::num::NonZeroUsize;

/// One actual settled fork. Scoring and measured provider cost are joined from
/// this agent's original output and call records, never inferred from success.
#[derive(Debug, Serialize)]
pub struct CompletedRun {
    /// Result row format.
    pub version: u32,
    /// Admitted file and role-snapshot digest.
    pub population: String,
    /// Paired replicate number.
    pub replicate: u64,
    /// Arm identity.
    pub arm: String,
    /// Actual fork returned by the host.
    pub agent: AgentPath,
    /// Original committed settlement status.
    pub status: StatusRecord,
}

/// Identifies a failed trial even when active runs complete out of order.
#[derive(Debug, thiserror::Error)]
#[error("population {population}, arm {arm}, replicate {replicate}: {cause}")]
pub struct FailedRun {
    /// Admitted experiment identity.
    pub population: String,
    /// Treatment identity.
    pub arm: String,
    /// Paired replicate number.
    pub replicate: u64,
    /// Failure, retaining exact publication custody where required.
    #[source]
    pub cause: Error,
}

/// A failed execution retains its failing step and exact replay custody.
#[derive(thiserror::Error)]
pub enum Error {
    /// The control failed, potentially after commit. Retain the original
    /// invocation; no automatically reconstructed request is submitted.
    #[error(
        "fork population trial: {cause}; recovery: replay the retained invocation with AgentControls::execute"
    )]
    Fork {
        /// Exact prepared bytes, including the original intent.
        invocation: Box<PreparedAgentInvocation<'static>>,
        /// Original canonical failure.
        cause: AgentExchangeError,
    },
    /// A producer or its session ended; never treated as successful completion.
    #[error(
        "follow population settlement for {agent}: {cause}; recovery: reopen AgentControls::observe_status for this child"
    )]
    Follow {
        /// Child whose lifecycle producer failed.
        agent: AgentPath,
        /// Original typed failure.
        cause: AgentClientError,
    },
    /// Pure preparation refusal.
    #[error(
        "prepare population fork: {0}; recovery: correct the arms file and re-admit Population"
    )]
    Prepare(String),
    /// The endpoint did not return the requested child's committed admission.
    #[error(
        "admit population fork: unexpected reply; recovery: inspect the retained experiment and source agent before resuming"
    )]
    Reply,
}

impl std::fmt::Debug for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

/// Runs every admitted arm from the same saved cut. `in_flight` is explicit
/// user credit, bounding active follows and futures rather than population size.
/// Each fork requires the host's exact immutable role snapshot from admission.
///
/// Dropping the stream releases follows, but does not archive already committed
/// agents. Resume the identical population to recover the same fork intents.
pub fn run<'a>(
    population: &'a Population,
    controls: &'a AgentControls,
    in_flight: NonZeroUsize,
) -> impl Stream<Item = Result<CompletedRun, FailedRun>> + 'a {
    stream::iter(population.trials())
        .map(move |trial| async move {
            execute(population, controls, trial)
                .await
                .map_err(|cause| FailedRun {
                    population: population.digest(),
                    arm: trial.arm.name.clone(),
                    replicate: trial.replicate,
                    cause,
                })
        })
        .buffer_unordered(in_flight.get())
}

async fn execute(
    population: &Population,
    controls: &AgentControls,
    trial: Trial<'_>,
) -> Result<CompletedRun, Error> {
    let id = trial
        .intent()
        .map_err(|error| Error::Prepare(error.to_string()))?;
    let command = AgentCommand::Fork {
        parent: trial.file.cut.agent.clone(),
        id,
        at: trial.file.cut.turn,
        brief: ModelContent::Text(trial.arm.prompt.clone()),
        role: trial.arm.role.clone(),
        profile_snapshot: Some(population.profile_snapshot()),
    };
    let prepared =
        PreparedCommand::new(&command).map_err(|error| Error::Prepare(error.to_string()))?;
    let invocation = controls
        .prepare(prepared)
        .map_err(|error| Error::Prepare(error.to_string()))?;
    let reply = controls
        .execute(&invocation)
        .await
        .map_err(|cause| Error::Fork {
            invocation: Box::new(invocation),
            cause,
        })?;
    let AgentReply::Spawned {
        agent,
        delivery: Delivered::Committed { .. },
    } = reply
    else {
        return Err(Error::Reply);
    };
    // Fork selects a zero-based saved turn; its inherited prefix ends at +1.
    // Admission checked this addition before any I/O.
    let inherited = trial.file.cut.turn + 1;
    let failed_follow = |cause| Error::Follow {
        agent: agent.clone(),
        cause,
    };
    let mut observer = controls
        .observe_status(agent.clone())
        .map_err(failed_follow)?;
    loop {
        // The producer is this fork's actor step, which publishes its status
        // in the same commit as its turn. A closed follow returns Follow.
        let status = observer.next().await.map_err(failed_follow)?;
        if complete(&status, inherited) {
            return Ok(CompletedRun {
                version: 1,
                population: population.digest(),
                replicate: trial.replicate,
                arm: trial.arm.name.clone(),
                agent,
                status,
            });
        }
    }
}

fn complete(status: &StatusRecord, inherited: u64) -> bool {
    matches!(status.phase, Phase::Done { .. })
        || (matches!(status.phase, Phase::Idle) && status.conversation > inherited)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_idle_and_waiting_children_cannot_finish_a_trial() {
        let mut status = StatusRecord {
            phase: Phase::Idle,
            cursor: 7,
            conversation: 3,
        };
        assert!(!complete(&status, 3));
        status.conversation = 4;
        status.phase = Phase::WaitingChildren { any: false };
        assert!(!complete(&status, 3));
        status.phase = Phase::Idle;
        assert!(complete(&status, 3));
    }
}
