//! The same public vectors over each explicitly selected real backend.

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[path = "local.rs"]
mod local;

#[cfg(all(feature = "browser", target_arch = "wasm32"))]
#[path = "browser.rs"]
mod browser;

#[cfg(not(any(
    all(feature = "local", not(target_arch = "wasm32")),
    all(feature = "browser", target_arch = "wasm32")
)))]
compile_error!(
    "STEP_FAILED agent-host-test-backend: select a backend for this architecture; recovery: cargo test --locked -p acyclic-agent-host-conformance --features local --test main (native), or bash scripts/run-browser-tests.sh cargo test --locked -p acyclic-agent-host-conformance --features browser --target wasm32-unknown-unknown --test main"
);
