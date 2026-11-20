#![doc = include_str!("../README.md")]

#[cfg(all(test, not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

pub mod persistence;
pub mod wire_codec;
