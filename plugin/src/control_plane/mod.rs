//! Host-managed sessions over the existing adapter catalog.

use crate::*;

const GIT_OPERATION_LEASE_MILLIS: u64 = 15 * 60 * 1_000;

mod access;
/// Erases a filesystem command future where the control plane awaits it.
///
/// The command's concrete body (checkout, capture, publication) then never
/// nests in the layout of the control-plane future that awaits it, so no
/// caller's layout grows with the filesystem's call depth.
pub(crate) fn fs_call<'a, T>(
    future: impl std::future::Future<Output = T> + Send + 'a,
) -> acyclic_stream::BoxProviderFuture<'a, T> {
    // Unbudgeted: the control plane holds no allocation budget; each
    // filesystem command opens its own inside this frame.
    Box::pin(future)
}

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

/// Served requests spend the original native budget; each box is charged or says why not.
#[cfg(test)]
#[test]
fn dispatch_makes_no_unpaid_box() {
    acyclic_test_support::boxes::assert_all_paid(&[
        (
            "service/dispatch.rs",
            include_str!("../service/dispatch.rs"),
        ),
        ("control_plane/mod.rs", include_str!("mod.rs")),
    ]);
}
