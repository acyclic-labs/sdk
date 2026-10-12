//! Host-managed sessions over the existing adapter catalog.

use crate::*;

const GIT_OPERATION_LEASE_MILLIS: u64 = 15 * 60 * 1_000;

mod allocation;
mod routes;
mod leases;
mod state;
mod lifecycle;
mod root;
mod routing;
mod inspection;
mod pre_tool;
mod git;
mod subagents;
mod post_tool;
mod spawning;
mod publication;
mod merging;
mod discard;
mod workspaces;
mod source_deletions;

pub(crate) use allocation::*;
pub(crate) use routes::*;
pub(crate) use leases::*;
pub(crate) use state::*;
pub(crate) use lifecycle::*;
pub(crate) use root::*;
pub(crate) use git::*;
#[cfg(test)]
#[path = "../root_allocation_tests.rs"]
mod root_allocation_tests;
