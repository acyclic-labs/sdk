//! Provider prompt-cache integration cases.
#![cfg(not(target_arch = "wasm32"))]
#![recursion_limit = "112"]

mod integration_helpers;
use integration_helpers::TestResult;

#[path = "integration/prefix_cache.rs"]
mod prefix_cache;
