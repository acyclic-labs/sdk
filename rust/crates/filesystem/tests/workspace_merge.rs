//! Workspace joins across independently created sibling paths.

#[macro_use]
#[path = "support/unbounded.rs"]
mod unbounded;

#[path = "support/workspace.rs"]
mod fixtures;

#[path = "workspace_merge/workspace.rs"]
mod workspace;
