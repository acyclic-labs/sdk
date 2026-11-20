//! Git compatibility and merge-driver contracts.
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(loom))]
acyclic_test_support::counting_allocator!();

#[path = "git_history.rs"]
mod git_history;
#[path = "git_marker_contracts.rs"]
mod git_marker_contracts;
#[path = "merge_driver_contracts.rs"]
mod merge_driver_contracts;
