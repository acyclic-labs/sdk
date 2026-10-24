#![doc = include_str!("../README.md")]

#[cfg(all(not(target_arch = "wasm32"), feature = "runtime"))]
mod runtime;
#[cfg(all(not(target_arch = "wasm32"), feature = "runtime"))]
pub use runtime::*;

#[cfg(all(not(target_arch = "wasm32"), feature = "task-context"))]
pub mod task;

#[cfg(feature = "codegen")]
pub mod codegen;

#[cfg(all(not(target_arch = "wasm32"), any(test, feature = "test-support")))]
pub mod tests;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test_capture;
