//! Public workspace contracts.
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(loom))]
acyclic_test_support::counting_allocator!();

#[macro_use]
#[path = "support/unbounded.rs"]
mod unbounded;
#[path = "support/allow_publication.rs"]
mod allow_publication;
#[path = "support/workspace.rs"]
mod fixtures;
#[path = "support/selected_budget.rs"]
mod selected_budget;
#[path = "support/transaction.rs"]
mod transaction;

#[path = "workspace_append.rs"]
mod workspace_append;
#[path = "workspace_context_contracts.rs"]
mod workspace_context_contracts;
#[path = "workspace_contracts.rs"]
mod workspace_contracts;
#[path = "workspace_creation_race.rs"]
mod workspace_creation_race;
#[path = "workspace_fork_selection.rs"]
mod workspace_fork_selection;
#[path = "workspace_join_history.rs"]
mod workspace_join_history;
#[path = "workspace_materialization.rs"]
mod workspace_materialization;
#[path = "workspace_merge.rs"]
mod workspace_merge;
#[path = "workspace_rebase.rs"]
mod workspace_rebase;
#[path = "workspace_restore.rs"]
mod workspace_restore;
#[path = "workspace_simulation.rs"]
mod workspace_simulation;
#[path = "workspace_transactions.rs"]
mod workspace_transactions;
