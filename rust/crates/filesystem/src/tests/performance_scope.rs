pub(super) use acyclic_fs_work::performance::*;

#[cfg(not(target_arch = "wasm32"))]
#[cfg(any(not(unit_test_split), unit_test_foundation))]
#[path = "performance_suite.rs"]
mod tests;
