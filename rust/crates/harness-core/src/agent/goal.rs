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
use acyclic_stream::BoxProviderFuture;

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
fn position(agent: &AgentState, input: &TurnInput, turn: OperationId) -> (String, u32) {
    if let TurnInput::Message {
        from: Sender::Agent { path },
        id,
        ..
    } = input
        && *path == agent.path
        && let Some((run, count)) = id
            .as_str()
            .strip_prefix(GOAL_INTENT)
            .and_then(|rest| rest.rsplit_once(':'))
        && let Ok(count) = count.parse()
    {
        return (run.to_owned(), count);
    }
    (turn.to_string(), 0)
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
    let Some(view) = goal.read().await.map_err(failed)? else {
        return Ok(None);
    };
    if view.status != GoalStatus::Active {
        return Ok(None);
    }
    let policy = goal.policy();
    let (run, count) = position(agent, input, turn);
    if count >= policy.max_continuations {
        let contents = policy.blocked(&view);
        goal.rewrite(&view, &contents, &format!("goal-block:{turn}"))
            .await
            .map_err(failed)?;
        return Ok(None);
    }
    let id = IntentId::try_from(format!("{GOAL_INTENT}{run}:{}", count + 1))?;
    Ok(Some(Out::Send {
        to: agent.path.clone(),
        record: InboxRecord::Message(Message {
            from: Sender::Agent {
                path: agent.path.clone(),
            },
            id,
            body: Box::new(view.source),
        }),
    }))
}
