//! Existing integration scenarios share one executable entrypoint.

#![deny(warnings)]

#[path = "cli_regressions.rs"]
mod cli_regressions;

#[path = "profile_planner.rs"]
mod profile_planner;
