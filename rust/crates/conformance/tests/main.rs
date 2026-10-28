//! Existing integration scenarios share one executable entrypoint.

#[cfg(feature = "inference")]
#[path = "inference.rs"]
mod inference;

#[path = "runner_cli.rs"]
mod runner_cli;

