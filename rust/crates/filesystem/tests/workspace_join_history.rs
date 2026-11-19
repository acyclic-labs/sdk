//! Workspace join planning, replay, and disjoint publication contracts.

#[macro_use]
#[path = "support/unbounded.rs"]
mod unbounded;
#[path = "support/workspace.rs"]
mod fixtures;
#[path = "support/join_plan.rs"]
mod join_plan;
#[path = "support/transaction.rs"]
mod transaction;
#[path = "workspace_join_history/workspace/mod.rs"]
mod workspace;
