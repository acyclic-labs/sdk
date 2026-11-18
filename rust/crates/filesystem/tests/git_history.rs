//! Git history, command sequencing and branch transitions through the public API.

#[path = "support/git_history.rs"]
mod fixtures;
#[path = "git_history/git_compat/mod.rs"]
mod git_compat;
#[path = "support/transaction.rs"]
mod transaction;
