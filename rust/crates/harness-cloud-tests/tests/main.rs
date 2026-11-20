//! Local-cloud Actors and Objects integration tests.
#![cfg(not(loom))]

acyclic_test_support::counting_allocator!();

#[path = "../../harness/tests/support/progress_guard.rs"]
mod progress_guard;
#[path = "../../harness/tests/support/progress_registry.rs"]
mod progress_registry;
#[path = "../../harness/tests/support/stall.rs"]
mod stall;

#[path = "../../harness/tests/local_cloud_objects/mod.rs"]
mod local_cloud_objects;

#[path = "../../harness/tests/local_cloud_actors/mod.rs"]
mod local_cloud_actors;
