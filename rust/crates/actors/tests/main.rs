//! Contract scenarios and opt-in models share one test entrypoint.
// Ordinary async tests retain std synchronization; the model runner selects the
// two Loom modules with the explicit workspace wrapper and model-checking feature.

#[cfg(all(not(loom), not(target_arch = "wasm32")))]
acyclic_test_support::counting_allocator!();

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

#[cfg(not(loom))]
#[path = "protoc_path.rs"]
mod protoc_path;

#[cfg(not(loom))]
#[path = "timer_contract.rs"]
mod timer_contract;

#[cfg(all(loom, feature = "model-checking", not(target_arch = "wasm32")))]
#[path = "guest_loom.rs"]
mod guest_loom;

#[cfg(all(loom, feature = "model-checking", not(target_arch = "wasm32")))]
#[path = "timer_loom.rs"]
mod timer_loom;

#[cfg(all(loom, not(feature = "model-checking")))]
compile_error!(
    "STEP_FAILED actors-model-routing: enable model-checking; recovery: RUSTC_WORKSPACE_WRAPPER=./scripts/loom-rustc.sh cargo test --locked -p acyclic-actors --features model-checking --test main"
);
