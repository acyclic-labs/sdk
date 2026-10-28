//! Binary adapter: generated public messages and Rust rules remain authoritative.
use acyclic_objects::{Digest, Error, MemoryObjects, json, wire};
use wasm_bindgen::prelude::*;

/// Throws an `Error` named `ObjectsV1Error` carrying the numeric wire `code`.
fn error(value: Error) -> JsValue {
    let result = js_sys::Error::new(&value.to_string());
    result.set_name("ObjectsV1Error");
    // Defining a data property on a fresh, unfrozen `Error` cannot fail.
    let _ = js_sys::Reflect::set(&result, &"code".into(), &JsValue::from(value.code as i32));
    result.into()
}
/// Accepts a JavaScript byte limit as a number: wasm-bindgen would silently wrap a `usize` argument above 2^32, so reject non-integers and saturate.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "the value is a non-negative integer and `as` saturates at usize::MAX"
)]
fn byte_limit(value: f64) -> Result<usize, JsValue> {
    if value.is_nan() || value < 0.0 || value.fract() != 0.0 {
        return Err(error(wire::ErrorCode::InvalidArgument.into()));
    }
    Ok(value as usize)
}

#[wasm_bindgen]
pub fn encode_objects_v1_json(name: &str, bytes: &[u8], maximum: f64) -> Result<Vec<u8>, JsValue> {
    json::encode_binary(name, bytes, byte_limit(maximum)?).map_err(error)
}
#[wasm_bindgen]
pub fn decode_objects_v1_json(name: &str, bytes: &[u8], maximum: f64) -> Result<Vec<u8>, JsValue> {
    json::decode_binary(name, bytes, byte_limit(maximum)?).map_err(error)
}
#[wasm_bindgen]
pub fn objects_v1_http_type(route: &str, output: bool) -> Result<String, JsValue> {
    let (_, input, response) = acyclic_objects::HTTP_ROUTES
        .iter()
        .find(|(name, _, _)| *name == route)
        .ok_or_else(|| error(wire::ErrorCode::InvalidArgument.into()))?;
    Ok(if output { *response } else { *input }.to_owned())
}

/// The BLAKE3-256 digest naming `bytes` as a plain blob.
#[wasm_bindgen]
#[must_use]
pub fn objects_v1_digest(bytes: &[u8]) -> Vec<u8> {
    Digest::of(bytes).as_bytes().to_vec()
}
/// Checks one route's binary reply against its binary request, as the Rust clients do.
#[wasm_bindgen]
pub fn verify_objects_v1(route: &str, input: &[u8], output: &[u8]) -> Result<(), JsValue> {
    acyclic_objects::route::verify(route, input, output).map_err(error)
}

/// One in-memory account served through the canonical route dispatch.
#[wasm_bindgen]
pub struct ObjectsV1Memory {
    inner: MemoryObjects,
}
#[wasm_bindgen]
impl ObjectsV1Memory {
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: MemoryObjects::memory(),
        }
    }
    /// Serves one route's binary input message, returning its binary output.
    pub async fn invoke(&self, route: String, bytes: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        acyclic_objects::route::invoke(&self.inner, &route, &bytes)
            .await
            .map_err(error)
    }
}

impl Default for ObjectsV1Memory {
    fn default() -> Self {
        Self::new()
    }
}
