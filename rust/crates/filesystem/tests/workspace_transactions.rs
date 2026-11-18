//! Public workspace transaction and rebase contracts.

#[macro_use]
#[path = "support/unbounded.rs"]
mod unbounded;
#[path = "support/workspace.rs"]
mod fixtures;
#[path = "support/selected_budget.rs"]
mod selected_budget;
#[path = "workspace_transactions/workspace/mod.rs"]
mod workspace;
