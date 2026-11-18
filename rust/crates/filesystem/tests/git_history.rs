//! Git history, command sequencing and branch transitions through the public API.

#[macro_use]
#[path = "support/unbounded.rs"]
mod unbounded;
#[path = "support/git_history.rs"]
mod fixtures;
#[path = "git_history/git_compat/mod.rs"]
mod git_compat;
#[path = "support/transaction.rs"]
mod transaction;
