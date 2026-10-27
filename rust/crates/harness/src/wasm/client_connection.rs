//! The public client selection policy is implemented once in Rust.
use acyclic_harness_client::{ConnectionOptions, resolve_connection_options};
use wasm_bindgen::prelude::*;

#[wasm_bindgen(typescript_custom_section)]
const CONFIGURATION_ERROR: &str = r#"
/** A configuration refusal before any transport is constructed. */
export interface ClientConnectionError extends Error {
    readonly name: 'client_invalid_endpoint' | 'client_missing_credential' | 'client_invalid_credential';
}
"#;

#[wasm_bindgen]
pub struct WasmConnectionOptions {
    options: ConnectionOptions,
}

#[wasm_bindgen]
impl WasmConnectionOptions {
    pub fn endpoint(&self) -> String {
        self.options.endpoint().to_owned()
    }

    pub fn credential(&self) -> String {
        self.options.credential().to_owned()
    }
}

/// No transport or credential issuance occurs while resolving configuration.
#[wasm_bindgen(js_name = resolveClientConnection)]
pub fn resolve_client_connection(
    endpoint: Option<String>,
    credential: Option<String>,
    environment_endpoint: Option<String>,
    environment_credential: Option<String>,
) -> Result<WasmConnectionOptions, JsValue> {
    let options = resolve_connection_options(
        endpoint.as_deref(),
        credential.as_deref(),
        environment_endpoint.as_deref(),
        environment_credential.as_deref(),
    )
    .map_err(|cause| {
        let error = js_sys::Error::new(&cause.to_string());
        error.set_name(cause.code());
        JsValue::from(error)
    })?;
    Ok(WasmConnectionOptions { options })
}
