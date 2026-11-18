//! Native workspace materialization and path restoration contracts.
#![cfg(not(target_arch = "wasm32"))]

#[macro_use]
#[path = "support/unbounded.rs"]
mod unbounded;
#[path = "support/selected_budget.rs"]
mod selected_budget;
#[path = "support/transaction.rs"]
mod transaction;
#[path = "workspace_materialization/workspace/mod.rs"]
mod workspace;
