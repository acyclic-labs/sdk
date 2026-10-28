//! Standalone Node/browser projections of the existing Rust constructors.
use super::*;
use wasm_bindgen::prelude::*;

fn error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

/// Parse canonical bytes into explicit segments; never resolve a secret.
#[wasm_bindgen(js_name = parseSecretReference)]
pub fn parse_secret_reference(input: &str) -> Result<Vec<String>, JsValue> {
    SecretReference::new(input.to_owned())
        .map(|reference| reference.segments().map(str::to_owned).collect())
        .map_err(error)
}

/// Format segments without decoding, case folding, aliases or default scopes.
#[wasm_bindgen(js_name = formatSecretReference)]
pub fn format_secret_reference(segments: Vec<String>) -> Result<String, JsValue> {
    SecretReference::from_segments(&segments)
        .map(String::from)
        .map_err(error)
}

/// Return selected name/reference JSON using the existing selection grammar.
/// Syntax admission grants no authority to resolve the reference or read bytes.
#[wasm_bindgen(js_name = selectSecretReference)]
pub fn select_secret_reference(name: String, reference: String) -> Result<String, JsValue> {
    let selected = acyclic_actors::domain::SecretSelection::new(name, reference).map_err(error)?;
    let selection = SecretRef {
        name: selected.name().to_owned(),
        reference: SecretReference::new(selected.reference().to_owned()).map_err(error)?,
    };
    serde_json::to_string(&selection).map_err(error)
}
