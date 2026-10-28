//! Run the production directory selector's tests and paired allocation profiles.
#[cfg(test)]
acyclic_test_support::counting_allocator!();

#[cfg(test)]
use acyclic_harness_core::native_environment;
// The included page derives its layout through `crate::HeapLayout`.
use acyclic_harness_core::HeapLayout;
#[path = "../../harness-core/src/memory_store/directory/page.rs"]
mod page;
