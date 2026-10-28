//! Standalone Node/browser projections of the existing Rust constructors.
use super::*;
use js_sys::{Array, JsString, Reflect};
use wasm_bindgen::prelude::*;

fn error(path: &str) -> JsValue {
    let error = js_sys::Error::new(
        "STEP secret-reference failed: invalid selection; recovery: supply a canonical secrets:/ reference and an explicit environment name",
    );
    error.set_name("SecretReferenceError");
    // These values are fixed schema identifiers, never submitted secret bytes.
    for (key, value) in [("code", "invalid_argument"), ("path", path)] {
        let _ = Reflect::set(&error, &JsValue::from_str(key), &JsValue::from_str(value));
    }
    error.into()
}

// JS string length is a lower bound on UTF-8 bytes. The existing ASCII grammar
// remains authoritative after this admission; no valid reference is excluded.
// Taking JsValue avoids wasm-bindgen's eager String/Vec<String> allocation.
fn text(input: &JsValue, maximum: u32, path: &str) -> Result<String, JsValue> {
    let string = input.dyn_ref::<JsString>().ok_or_else(|| error(path))?;
    if string.length() > maximum {
        return Err(error(path));
    }
    input.as_string().ok_or_else(|| error(path))
}

/// Parse canonical bytes into explicit segments; never resolve a secret.
#[wasm_bindgen(js_name = parseSecretReference)]
pub fn parse_secret_reference(
    #[wasm_bindgen(unchecked_param_type = "string")] input: &JsValue,
) -> Result<Vec<String>, JsValue> {
    SecretReference::new(text(input, 256, "reference")?)
        .map(|reference| reference.segments().map(str::to_owned).collect())
        .map_err(|_| error("reference"))
}

/// Format segments without decoding, case folding, aliases or default scopes.
#[wasm_bindgen(js_name = formatSecretReference)]
pub fn format_secret_reference(
    #[wasm_bindgen(unchecked_param_type = "string[]")] input: &JsValue,
) -> Result<String, JsValue> {
    if !Array::is_array(input) {
        return Err(error("reference"));
    }
    let input = input.unchecked_ref::<Array>();
    // Even empty segments need a separator. This follows from the original
    // 256-byte reference ceiling, rather than introducing a path-depth rule.
    let count = input.length();
    let mut length = 9_u32.saturating_add(count.saturating_sub(1));
    if length > 256 {
        return Err(error("reference"));
    }
    let mut segments = Vec::new();
    for index in 0..count {
        let segment = text(&input.get(index), 256 - length, "reference")?;
        length += segment.len() as u32;
        if length > 256 {
            return Err(error("reference"));
        }
        segments.push(segment);
    }
    SecretReference::from_segments(&segments)
        .map(String::from)
        .map_err(|_| error("reference"))
}

/// Return selected name/reference JSON using the existing selection grammar.
/// Syntax admission grants no authority to resolve the reference or read bytes.
#[wasm_bindgen(js_name = selectSecretReference)]
pub fn select_secret_reference(
    #[wasm_bindgen(unchecked_param_type = "string")] name: &JsValue,
    #[wasm_bindgen(unchecked_param_type = "string")] reference: &JsValue,
) -> Result<String, JsValue> {
    let selected = acyclic_actors::domain::SecretSelection::new(
        text(name, 63, "name")?,
        text(reference, 256, "reference")?,
    )
    .map_err(|_| error("selection"))?;
    let selection = SecretRef {
        name: selected.name().to_owned(),
        reference: SecretReference::new(selected.reference().to_owned())
            .map_err(|_| error("reference"))?,
    };
    serde_json::to_string(&selection).map_err(|_| error("selection"))
}
