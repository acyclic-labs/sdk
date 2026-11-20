//! Real-binary end-to-end harness scenarios.
//!
//! One test binary: every family shares the harness support modules, so a
//! second binary would compile them again for no coverage.
acyclic_test_support::counting_allocator!();

#[path = "../../harness/tests/support/progress_guard.rs"]
mod progress_guard;
#[path = "../../harness/tests/support/progress_registry.rs"]
mod progress_registry;
#[path = "../../harness/tests/support/stall.rs"]
mod stall;

include!("e2e/root.rs");

#[path = "skills_context.rs"]
mod skills_context;
