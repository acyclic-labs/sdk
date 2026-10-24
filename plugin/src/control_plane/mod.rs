//! Host-managed sessions over the existing adapter catalog.

use crate::*;

const GIT_OPERATION_LEASE_MILLIS: u64 = 15 * 60 * 1_000;

mod access;
mod allocation;
mod discard;
mod git;
mod inspection;
mod leases;
mod lifecycle;
mod merging;
mod post_tool;
mod pre_tool;
mod publication;
mod root;
mod routes;
mod routing;
mod source_deletions;
mod spawning;
mod state;
mod subagents;
mod workspaces;

pub(crate) use allocation::*;
pub(crate) use leases::*;
pub(crate) use lifecycle::*;
pub(crate) use routes::*;
pub(crate) use state::*;
#[cfg(test)]
#[path = "../root_allocation_tests.rs"]
mod root_allocation_tests;
