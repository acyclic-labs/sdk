//! Actors integration scenarios with ordinary synchronization.

#[cfg(all(not(loom), not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

#[cfg(all(not(loom), not(target_arch = "wasm32")))]
#[path = "digest_admission.rs"]
mod digest_admission;

#[cfg(not(loom))]
#[path = "descriptor_contract.rs"]
mod descriptor_contract;

#[cfg(not(loom))]
#[path = "guest_activation.rs"]
mod guest_activation;

#[cfg(not(loom))]
#[path = "guest_contract.rs"]
mod guest_contract;

#[cfg(not(loom))]
#[path = "guest_metadata.rs"]
mod guest_metadata;

#[cfg(all(not(loom), not(target_arch = "wasm32")))]
#[path = "live_update.rs"]
mod live_update;
