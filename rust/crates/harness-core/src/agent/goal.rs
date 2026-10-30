//! Goal continuation: a turn that settles with an active GOAL.md wakes its agent
//! again through the agent's own inbox.
//!
//! The settling step sends one self-addressed [`Message`] whose body is the
//! observed GOAL.md, in the same commit as the settlement, so the next turn is an
//! ordinary inbox input: no timer, no polling. The loop's position is carried by
//! the message identity, so a replayed step recomputes the same continuation and
//! the agent keeps no goal state. An exhausted allowance rewrites GOAL.md as
//! `blocked` instead.

use super::{AgentState, Error, InboxRecord, IntentId, Message, Out, Sender, TurnInput};
use crate::{
    OperationId,
    goal::{GoalContinuationPolicy, GoalStatus, GoalView},
};
use acyclic_stream::{BoxProviderFuture, StreamPath};
use serde::{Deserialize, Serialize};

/// Identity prefix of a goal continuation: `goal:{loop}:{n}`, where `loop` is
/// the turn that started the loop and `n` counts its continuations from 1.
pub const GOAL_INTENT: &str = "goal:";

/// The agent's goal file, bound for one turn by its host.
pub trait TurnGoal: acyclic_stream::ProviderPlatform {
    /// The continuation allowance of one goal loop.
    fn policy(&self) -> GoalContinuationPolicy;

    /// GOAL.md as observed now, read once; `None` when absent.
    fn read(&self) -> BoxProviderFuture<'_, crate::Result<Option<GoalView>>>;

    /// Replaces the observed `goal` with `contents`, at most once per `key`.
    fn rewrite<'a>(
        &'a self,
        goal: &'a GoalView,
        contents: &'a str,
        key: &'a str,
    ) -> BoxProviderFuture<'a, crate::Result<()>>;
}

/// The goal loop `input` belongs to and how many continuations it has had: a
/// continuation names both; any other input starts a loop of its own `turn`.
fn continuation(agent: &super::AgentPath, input: &TurnInput) -> Option<(OperationId, u32)> {
    if let TurnInput::Message {
        from: Sender::Agent { path },
        id,
        ..
    } = input
        && path == agent
        && let Some((run, count)) = id
            .as_str()
            .strip_prefix(GOAL_INTENT)
            .and_then(|rest| rest.rsplit_once(':'))
        && let Ok(run) = OperationId::parse(run)
        && let Ok(count) = count.parse()
        && count > 0
    {
        return Some((run, count));
    }
    None
}

fn position(agent: &AgentState, input: &TurnInput, turn: OperationId) -> (OperationId, u32) {
    continuation(&agent.path, input).unwrap_or((turn, 0))
}

/// The committed goal run of a self-continuation, never a caller-supplied run.
pub(super) fn continuing(agent: &super::AgentPath, input: &TurnInput) -> Option<OperationId> {
    continuation(agent, input).map(|(run, _)| run)
}

/// A completed goal references its canonical conversation entry.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Completed {
    pub sequence: u64,
}

impl super::AgentRecord for Completed {}

pub(super) fn result(agent: &super::AgentPath, run: OperationId) -> Result<StreamPath, Error> {
    StreamPath::try_from(format!("{}/goals/{run}", agent.as_str()))
        .map_err(|_| Error::Path("invalid goal result path"))
}

/// What `agent`'s turn `turn`, started by `input`, does about its goal once it
/// settles: the continuation to send, or nothing. An exhausted allowance
/// rewrites GOAL.md as `blocked` and sends nothing.
pub async fn settle(
    agent: &AgentState,
    input: &TurnInput,
    turn: OperationId,
    goal: &dyn TurnGoal,
) -> Result<Option<Out>, Error> {
    let failed = |error| Error::Turn(Box::new(error));
    // Goals continue root agents; a child reports to its parent when its
    // brief is done, whatever its forked scratch holds.
    if agent.parent.is_some() {
        return Ok(None);
    }
    let policy = goal.policy();
    let (run, count) = position(agent, input, turn);
    let view = loop {
        let Some(view) = goal.read().await.map_err(failed)? else {
            return Ok(None);
        };
        if view.status != GoalStatus::Active {
            return Ok(None);
        }
        if count < policy.max_continuations {
            break view;
        }
        // A concurrent edit wins the conditioned rewrite: decide again on it.
        match goal
            .rewrite(&view, &policy.blocked(&view), &format!("goal-block:{turn}"))
            .await
        {
            Ok(()) => return Ok(None),
            Err(crate::Error::Conflict(_)) => continue,
            Err(error) => return Err(failed(error)),
        }
    };
    let id = IntentId::try_from(format!("{GOAL_INTENT}{run}:{}", count + 1))?;
    Ok(Some(Out::Send {
        to: agent.path.clone(),
        record: InboxRecord::Message(Message {
            from: Sender::Agent {
                path: agent.path.clone(),
            },
            id,
            body: Box::new(view.source),
            attachments: None,
        }),
    }))
}
