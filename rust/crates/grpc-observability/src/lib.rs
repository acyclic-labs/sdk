#![doc = include_str!("../README.md")]

#[cfg(all(not(target_arch = "wasm32"), feature = "runtime"))]
mod runtime;
#[cfg(all(not(target_arch = "wasm32"), feature = "runtime"))]
pub use runtime::*;

#[cfg(feature = "codegen")]
pub mod codegen;
