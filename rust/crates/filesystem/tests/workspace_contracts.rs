//! Public contracts for workspace generations and conditional mutations.

#[macro_use]
#[path = "support/unbounded.rs"]
mod unbounded;

#[path = "support/workspace.rs"]
mod fixtures;
#[path = "support/transaction.rs"]
mod transaction;

#[path = "workspace_contracts/workspace.rs"]
mod workspace;
