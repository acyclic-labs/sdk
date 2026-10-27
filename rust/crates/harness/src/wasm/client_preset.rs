//! Default configuration comes from the same public preset used by native hosts.
use wasm_bindgen::prelude::*;

/// Return plain configuration data, not a connected or configured agent.
/// Callers may override any field before submitting it to their host.
#[wasm_bindgen(js_name = graphcoderPreset, unchecked_return_type = "Preset")]
pub fn graphcoder_preset() -> Result<JsValue, JsValue> {
    super::to_js_client_admitted(&acyclic_harness_core::preset::Preset::graphcoder())
}
