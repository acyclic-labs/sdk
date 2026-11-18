//! Public follow and filtered-follow contracts.

#[cfg(not(target_arch = "wasm32"))]
#[path = "../src/filtered_follow_fixture.rs"]
mod filtered_follow_fixture;
#[cfg(not(target_arch = "wasm32"))]
mod filtered_follow_tests;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_stream::*;
#[cfg(not(target_arch = "wasm32"))]
mod follow_tests;
#[cfg(not(target_arch = "wasm32"))]
mod head_tests;
