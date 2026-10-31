//! Binary adapter: generated public messages and Rust rules remain authoritative.
use acyclic_objects::binary::javascript::{ObjectsV1Request, js_error as error};
use acyclic_objects::{Blobs, Digest, MemoryObjects, json, wire};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

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
        .chain(acyclic_objects::HTTP_STREAM_ROUTES)
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
pub fn verify_objects_v1(route: &str, input: Vec<u8>, output: Vec<u8>) -> Result<(), JsValue> {
    acyclic_objects::route::verify(route, input.into(), output.into()).map_err(error)
}

/// One in-memory account served through the canonical route dispatch.
#[wasm_bindgen]
pub struct ObjectsV1Memory {
    inner: Arc<dyn Blobs>,
}
#[wasm_bindgen]
impl ObjectsV1Memory {
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MemoryObjects::memory()),
        }
    }
    /// Owns one lazy canonical request; close also cancels a pending next.
    pub fn open(
        &self,
        route: String,
        bytes: &js_sys::Uint8Array,
    ) -> Result<ObjectsV1Request, JsValue> {
        ObjectsV1Request::serve(Arc::clone(&self.inner), route, bytes)
    }

    /// Register directly with this Rust account's physical producer.
    pub fn observe(&self, bytes: &js_sys::Uint8Array) -> Result<ObjectsV1Request, JsValue> {
        ObjectsV1Request::observe_blobs(Arc::clone(&self.inner), bytes)
    }

    /// Serves the same original request through its single reply.
    pub async fn invoke(
        &self,
        route: String,
        bytes: &js_sys::Uint8Array,
    ) -> Result<js_sys::Uint8Array, JsValue> {
        let reply = self.open(route, bytes)?.body().await.map_err(error)?;
        Ok(js_sys::Uint8Array::from(reply.as_slice()))
    }
}

impl Default for ObjectsV1Memory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
