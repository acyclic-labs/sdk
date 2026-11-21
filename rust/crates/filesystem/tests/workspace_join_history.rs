//! Workspace join planning, replay, and disjoint publication contracts.

use crate::fixtures;
#[path = "support/join_plan.rs"]
mod join_plan;
use crate::transaction;
#[path = "workspace_join_history/workspace/mod.rs"]
mod workspace;
