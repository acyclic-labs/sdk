//! Deterministic workspace creation conflicts over the Streams provider interface.

#[macro_use]
#[path = "support/unbounded.rs"]
mod unbounded;
#[path = "support/workspace.rs"]
mod fixtures;
#[path = "workspace_creation_race/workspace/mod.rs"]
mod workspace;
