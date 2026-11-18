//! Contract scenarios and opt-in models share one test entrypoint.
// Ordinary async tests retain std synchronization; the model runner selects the
// guest Loom module with the explicit loom cfg and model-checking feature.

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

#[cfg(all(loom, feature = "model-checking", not(target_arch = "wasm32")))]
#[path = "guest_loom.rs"]
mod guest_loom;

#[cfg(all(loom, not(feature = "model-checking")))]
compile_error!(
    "STEP_FAILED actors-model-routing: enable model-checking; recovery: bazel test --@rules_rust//rust/settings:per_crate_rustc_flag=//rust/crates/actors:@--cfg=loom //rust/crates/actors:main --test_filter=guest_loom::"
);
