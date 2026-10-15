//! Goal file state, per-settle continuation identities and finite loop policies.

use acyclic_harness::{
    Error, OperationId, Result, TaskId,
    conversation::VolumeClass,
    goal::{GoalContinuation, GoalContinuationPolicy, GoalStatus, GoalView},
};
use serde::Deserialize;

#[path = "goal_files/continuations.rs"]
mod continuations;
#[path = "goal_files/parsing.rs"]
mod parsing;
#[path = "goal_files/writes.rs"]
mod writes;
