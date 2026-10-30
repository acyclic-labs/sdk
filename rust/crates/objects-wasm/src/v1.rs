//! Binary adapter: generated public messages and Rust rules remain authoritative.
use acyclic_objects::binary::javascript::{js_error as error, js_reply};
use acyclic_objects::{Digest, Error, MemoryObjects, json, wire};
use futures::StreamExt as _;
use std::rc::Rc;
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
    /// Owns one lazy canonical request; close also cancels a pending next.
    pub fn open(
        &self,
        route: String,
        bytes: &js_sys::Uint8Array,
    ) -> Result<ObjectsV1Request, JsValue> {
        let (bytes, ()) = acyclic_stream::javascript_bytes::snapshot(bytes, |_| Ok(()))?;
        let account = self.inner.clone();
        let request = futures::stream::once(async move {
            acyclic_objects::route::invoke_at(&account, &route, bytes.into()).await
        })
        .boxed_local();
        Ok(ObjectsV1Request {
            state: Rc::new(acyclic_stream::pull::PullCursor::new(
                request,
                wire::ErrorCode::Unavailable.into(),
            )),
        })
    }

    /// Serves the same original request through its single reply.
    pub async fn invoke(
        &self,
        route: String,
        bytes: &js_sys::Uint8Array,
    ) -> Result<js_sys::Uint8Array, JsValue> {
        let request = self.open(route, bytes)?;
        let reply = request
            .state
            .next()
            .await
            .map_err(error)?
            .ok_or_else(|| error(wire::ErrorCode::Unavailable.into()))?;
        Ok(js_sys::Uint8Array::from(reply.0.as_slice()))
    }
}

/// One original account operation using the shared pull/cancellation owner.
#[wasm_bindgen]
pub struct ObjectsV1Request {
    state: Rc<acyclic_stream::pull::PullCursor<(Vec<u8>, Option<acyclic_stream::Revision>), Error>>,
}

#[wasm_bindgen]
impl ObjectsV1Request {
    /// Resolve one original reply, or null when closed or consumed.
    #[wasm_bindgen(unchecked_return_type = "ObjectsV1BinaryReply | null")]
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        match self.state.next().await.map_err(error)? {
            Some((body, revision)) => js_reply(&body, revision.as_ref()),
            None => Ok(JsValue::NULL),
        }
    }

    /// Cancel the original operation and settle its outstanding pull.
    pub fn close(&self) {
        self.state.close();
    }
}

impl Default for ObjectsV1Memory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
