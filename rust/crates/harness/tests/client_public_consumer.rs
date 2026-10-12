//! Reducer-backed public consumer compiled identically for native and WASM.
#[path = "../examples/client_views.rs"]
mod consumer;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_test::*;
#[cfg(target_arch = "wasm32")]
wasm_bindgen_test_configure!(run_in_browser);

#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), test)]
fn identical_public_harness_trace() {
    consumer::trace();
}

#[cfg(feature = "filesystem")]
#[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
#[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
async fn filesystem_edit_reconciles_and_retries_original_operation() {
    consumer::filesystem_trace().await.unwrap();
}
