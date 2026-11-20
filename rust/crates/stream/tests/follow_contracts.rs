//! Public follow and filtered-follow contracts.

#[cfg(not(target_arch = "wasm32"))]
use acyclic_stream_contract::filtered_follow_fixture;
#[cfg(not(target_arch = "wasm32"))]
mod filtered_follow_tests;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_stream::*;
#[cfg(not(target_arch = "wasm32"))]
mod head_tests;
