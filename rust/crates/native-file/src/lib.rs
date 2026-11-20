//! Native file primitives shared by Acyclic host consumers.
#[cfg(test)]
acyclic_test_support::counting_allocator!();

pub mod directory;
pub mod io_progress;
#[cfg(all(target_os = "linux", not(any(test, feature = "test-support"))))]
pub mod kernel_keyring;
pub mod sync;
