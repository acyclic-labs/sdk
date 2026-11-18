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
    "STEP_FAILED agent-host-test-backend: select a backend for this architecture; recovery: bazel test //rust/crates/conformance/agent-host:main (native), or bash scripts/qualify-agent-host.sh (native and browser)"
);
