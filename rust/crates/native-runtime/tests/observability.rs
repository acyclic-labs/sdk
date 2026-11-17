//! Observability integration tests that exercise native runtime spans and output.
#![cfg(not(target_arch = "wasm32"))]

acyclic_test_support::counting_allocator!();

#[path = "observability/operations.rs"]
mod operations;
#[path = "observability/output.rs"]
mod output;
