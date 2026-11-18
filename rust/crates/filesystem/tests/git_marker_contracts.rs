//! Conflict-marker recognition over arbitrarily chunked file content.

#![cfg(not(loom))]

#[path = "git_marker_contracts/git_compat/mod.rs"]
mod git_compat;
