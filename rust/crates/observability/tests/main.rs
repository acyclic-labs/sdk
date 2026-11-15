//! The observability integration tests share one test binary, so the library
//! and its dependencies link once. Every module counts allocations through the
//! same process allocator.
#![cfg(not(target_arch = "wasm32"))]

acyclic_test_support::counting_allocator!();

mod intervals;
mod operations;
mod output;
