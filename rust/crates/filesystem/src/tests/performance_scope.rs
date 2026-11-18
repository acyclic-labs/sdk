pub(super) use acyclic_fs_work::performance::*;

#[cfg(not(target_arch = "wasm32"))]
#[path = "performance_suite.rs"]
mod tests;
