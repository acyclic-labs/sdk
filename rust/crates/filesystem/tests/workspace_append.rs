//! Workspace staged append contracts.

#[macro_use]
#[path = "support/unbounded.rs"]
mod unbounded;

#[path = "support/append.rs"]
mod append;
#[path = "support/transaction.rs"]
mod transaction;
#[path = "workspace_append/workspace/mod.rs"]
mod workspace;
