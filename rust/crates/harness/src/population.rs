//! Live population execution over the ordinary history fork and status follow.
//! The caller reads the arms file from its volume and writes each yielded row
//! to a results file; this module owns no tools, background task or transport.

use acyclic_harness_client::{
    AgentClientError, AgentCommand, AgentControls, AgentExchangeError, AgentReply,
    PreparedAgentInvocation, PreparedCommand,
};
use acyclic_harness_core::{
    agent::{
        AgentOutcome, AgentPath, ChildRelease, Delivered, IntentId, Phase, Settlement, StatusRecord,
    },
    experiments::{Population, Trial},
    model::ModelContent,
};
use futures::{Stream, StreamExt as _, stream};
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;

/// One actual settled fork. Scoring and measured provider cost are joined from
/// this agent's original output and call records, never inferred from success.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Original stable trial identity, also used for its release command.
    pub id: IntentId,
    /// Original zero-based status position from the producer's follow frame.
    pub status_sequence: u64,
    /// Original committed settlement status.
    pub status: StatusRecord,
}

/// A retained original result whose parent has acknowledged membership removal.
#[derive(Debug, Serialize)]
pub struct ReleasedRun {
    /// Original result, including its version and exact output cut.
    #[serde(flatten)]
    pub run: CompletedRun,
    /// Host-retained original cut, independently checked against `run`.
    pub settlement: Settlement,
    /// Receipt published by the parent's membership-removal commit.
    pub release: ChildRelease,
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
    /// Fork preparation, uncertain exchange or mismatched response.
    #[error("fork population trial: {0}")]
    Fork(CommandFailure),
    /// Retention must succeed before the selected original output is closed.
    #[error(
        "retain population result: {cause}; recovery: persist the retained run, then call population::release"
    )]
    Retain {
        /// Original selected result; no archive has been submitted.
        run: Box<CompletedRun>,
        /// The caller's original retention/scoring error.
        cause: crate::Error,
    },
    /// The original cut and exact uncertain release invocation remain owned.
    #[error(
        "release population trial: {cause}; recovery: replay the retained invocation with AgentControls::execute, or call population::release with the retained original run"
    )]
    Release {
        /// Original result, already passed to the retention callback.
        run: Box<CompletedRun>,
        /// Shared preparation/exchange failure.
        cause: CommandFailure,
    },
    /// An archived status must not replace the result retained before closing.
    #[error(
        "resume population trial {agent}: original result is required; recovery: read the retained CompletedRun and call population::release"
    )]
    OriginalRequired {
        /// Child whose latest status is an archive transition.
        agent: AgentPath,
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
}

/// One command path shared by fork and release, preserving exact custody.
#[derive(thiserror::Error)]
pub enum CommandFailure {
    /// Pure refusal before transport.
    #[error("prepare command: {0}; recovery: correct the command and call AgentControls::prepare")]
    Prepare(String),
    /// The command may already have committed.
    #[error(
        "exchange command: {cause}; recovery: replay the retained invocation with AgentControls::execute"
    )]
    Exchange {
        /// Exact original serialized request and intent.
        invocation: Box<PreparedAgentInvocation<'static>>,
        /// Original typed refusal or transport failure.
        cause: AgentExchangeError,
    },
    /// No result may be inferred from a different reply or cut.
    #[error(
        "validate command reply: original cut differs; recovery: inspect the retained original command and result before resuming"
    )]
    Reply,
}

impl std::fmt::Debug for CommandFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

/// Runs every admitted arm from the same saved cut. `in_flight` is explicit
/// user credit, held through retention and the parent's release acknowledgment.
/// Each fork requires the host's exact immutable role snapshot from admission.
///
/// Dropping the stream releases follows, but does not archive already committed
/// agents. `retain` must durably save each original run and its scored output
/// before returning. Recover pending releases from those files via [`release`];
/// an archived latest status cannot rediscover the original cut. The first
/// failure stops admission and drops other in-flight futures, without retrying.
pub fn run<'a, F>(
    population: &'a Population,
    controls: &'a AgentControls,
    in_flight: NonZeroUsize,
    retain: &'a F,
) -> impl Stream<Item = Result<ReleasedRun, FailedRun>> + 'a
where
    F: AsyncFn(&CompletedRun) -> crate::Result<()> + 'a,
{
    let active = stream::iter(population.trials())
        .map(move |trial| async move {
            execute(population, controls, trial, retain)
                .await
                .map_err(|cause| FailedRun {
                    population: population.digest(),
                    arm: trial.arm.name.clone(),
                    replicate: trial.replicate,
                    cause,
                })
        })
        .buffer_unordered(in_flight.get());
    stream::try_unfold(active, |mut active| async move {
        let Some(result) = active.next().await else {
            return Ok(None);
        };
        Ok(Some((result?, active)))
    })
}

async fn execute<F>(
    population: &Population,
    controls: &AgentControls,
    trial: Trial<'_>,
    retain: &F,
) -> Result<ReleasedRun, Error>
where
    F: AsyncFn(&CompletedRun) -> crate::Result<()>,
{
    let id = trial
        .intent()
        .map_err(|error| Error::Fork(CommandFailure::Prepare(error.to_string())))?;
    let command = AgentCommand::Fork {
        parent: trial.file.cut.agent.clone(),
        id: id.clone(),
        at: trial.file.cut.turn,
        brief: ModelContent::Text(trial.arm.prompt.clone()),
        role: trial.arm.role.clone(),
        profile_snapshot: Some(population.profile_snapshot()),
    };
    let reply = exchange(controls, &command).await.map_err(Error::Fork)?;
    let AgentReply::Spawned {
        agent,
        delivery: Delivered::Committed { .. },
    } = reply
    else {
        return Err(Error::Fork(CommandFailure::Reply));
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
        let observed = observer.next().await.map_err(failed_follow)?;
        if matches!(
            observed.status.phase,
            Phase::Done {
                outcome: AgentOutcome::Archived
            }
        ) {
            return Err(Error::OriginalRequired { agent });
        }
        if complete(&observed.status, inherited) {
            let run = CompletedRun {
                version: 1,
                population: population.digest(),
                replicate: trial.replicate,
                arm: trial.arm.name.clone(),
                agent,
                id,
                status_sequence: observed.sequence,
                status: observed.status,
            };
            if let Err(cause) = retain(&run).await {
                return Err(Error::Retain {
                    run: Box::new(run),
                    cause,
                });
            }
            return release(controls, run).await;
        }
    }
}

/// Close an already retained original result and await parent-owned release.
/// Exact retries use this same status sequence and intent, including after an
/// uncertain archive commit. No status/settlement rediscovery is performed.
pub async fn release(controls: &AgentControls, run: CompletedRun) -> Result<ReleasedRun, Error> {
    let result = if run.version != 1
        || !matches!(run.status.phase, Phase::Idle | Phase::Done { .. })
        || matches!(
            run.status.phase,
            Phase::Done {
                outcome: AgentOutcome::Archived
            }
        ) {
        Err(CommandFailure::Prepare(
            "unsupported or unsettled retained run".into(),
        ))
    } else {
        exchange(
            controls,
            &AgentCommand::Archive {
                agent: run.agent.clone(),
                id: run.id.clone(),
                status: run.status_sequence,
            },
        )
        .await
    };
    match result {
        Ok(AgentReply::Released {
            settlement,
            release,
        }) if settlement.status == run.status_sequence
            && settlement.cursor == run.status.cursor
            && settlement.conversation == run.status.conversation =>
        {
            Ok(ReleasedRun {
                run,
                settlement,
                release,
            })
        }
        result => Err(Error::Release {
            run: Box::new(run),
            cause: result.err().unwrap_or(CommandFailure::Reply),
        }),
    }
}

async fn exchange(
    controls: &AgentControls,
    command: &AgentCommand,
) -> Result<AgentReply, CommandFailure> {
    let prepared = PreparedCommand::new(command)
        .map_err(|error| CommandFailure::Prepare(error.to_string()))?;
    let invocation = controls
        .prepare(prepared)
        .map_err(|error| CommandFailure::Prepare(error.to_string()))?;
    controls
        .execute(&invocation)
        .await
        .map_err(|cause| CommandFailure::Exchange {
            invocation: Box::new(invocation),
            cause,
        })
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
