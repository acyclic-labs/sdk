#![doc = include_str!("../README.md")]

#[cfg(all(test, not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();
pub use acyclic_stream_contract_core::*;

#[cfg(all(any(feature = "test-support", test), not(target_arch = "wasm32")))]
#[doc(hidden)]
pub mod filtered_follow_fixture;
#[doc(hidden)]
pub mod follow_index;
#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub mod obs;
pub mod pull;
#[doc(hidden)]
pub mod snapshot;
pub mod testing;
#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub mod transport_credential;

include!("revision_tracker.rs");
include!("revision_applied.rs");
