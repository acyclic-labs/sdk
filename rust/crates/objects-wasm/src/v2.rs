//! Binary adapter: generated public messages and Rust rules remain authoritative.
use acyclic_objects::v2::{
    Error, MemoryObjects, MemoryOptions, ObjectsProvider, json, request, wire,
};
use bytes::Bytes;
use prost::Message;
use wasm_bindgen::prelude::*;

fn error(value: Error) -> JsValue {
    let result = js_sys::Object::new();
    let _ = js_sys::Reflect::set(&result, &"code".into(), &JsValue::from(value.code as i32));
    let _ = js_sys::Reflect::set(&result, &"message".into(), &value.to_string().into());
    result.into()
}
fn decode<T: Message + Default>(bytes: &[u8]) -> Result<T, JsValue> {
    T::decode(bytes).map_err(|_| error(wire::ErrorCode::InvalidArgument.into()))
}
fn frames(value: &impl Message) -> js_sys::Array {
    let result = js_sys::Array::new();
    result.push(&js_sys::Uint8Array::from(value.encode_to_vec().as_slice()));
    result
}

#[wasm_bindgen]
pub fn validate_objects_v2_request(
    route: &str,
    bytes: &[u8],
    body_length: u64,
) -> Result<(), JsValue> {
    request::validate_binary(route, bytes, body_length).map_err(error)
}
#[wasm_bindgen]
pub fn encode_objects_v2_json(
    name: &str,
    bytes: &[u8],
    maximum: usize,
) -> Result<Vec<u8>, JsValue> {
    json::encode_binary(name, bytes, maximum).map_err(error)
}
#[wasm_bindgen]
pub fn decode_objects_v2_json(
    name: &str,
    bytes: &[u8],
    maximum: usize,
) -> Result<Vec<u8>, JsValue> {
    json::decode_binary(name, bytes, maximum).map_err(error)
}
#[wasm_bindgen]
pub fn objects_v2_http_type(route: &str, output: bool) -> Result<String, JsValue> {
    let (_, input, response) = acyclic_objects::v2::HTTP_ROUTES
        .iter()
        .find(|(name, _, _)| *name == route)
        .ok_or_else(|| error(wire::ErrorCode::InvalidArgument.into()))?;
    Ok(if output { *response } else { *input }.to_owned())
}
#[wasm_bindgen]
pub fn validate_objects_v2_response(
    route: &str,
    query: &[u8],
    bytes: &[u8],
    body_length: u64,
) -> Result<(), JsValue> {
    acyclic_objects::v2::response::validate_binary(route, query, bytes, body_length).map_err(error)
}
#[wasm_bindgen]
pub fn validate_objects_v2_get_header(
    query: &[u8],
    bytes: &[u8],
    maximum: u64,
) -> Result<u64, JsValue> {
    acyclic_objects::v2::response::validate_get_header(query, bytes, maximum).map_err(error)
}

/// Validates one bounded download body frame and returns its remaining range.
#[wasm_bindgen]
pub fn validate_objects_v2_get_body(body_length: u64, remaining: u64) -> Result<u64, JsValue> {
    acyclic_objects::v2::response::validate_get_body(body_length, remaining).map_err(error)
}

/// Maps hosted HTTP status/detail values through the canonical Objects error
/// vocabulary before they cross the browser boundary.
#[wasm_bindgen]
pub fn objects_v2_http_error_code(status: u16, detail: Option<i32>) -> i32 {
    acyclic_objects::v2::response::http_error_code(status, detail) as i32
}

/// Maps Connect/tonic status/detail values through the canonical Objects
/// error vocabulary before they cross the browser boundary.
#[wasm_bindgen]
pub fn objects_v2_grpc_error_code(status: u32, detail: Option<i32>) -> i32 {
    acyclic_objects::v2::response::grpc_error_code(status, detail) as i32
}

/// Validates the HTTPS or loopback HTTP endpoint policy used by native and
/// browser Objects clients.
#[wasm_bindgen]
pub fn validate_objects_v2_http_endpoint(endpoint: &str) -> Result<(), JsValue> {
    acyclic_objects::v2::response::validate_http_endpoint(endpoint).map_err(error)
}

#[wasm_bindgen]
pub struct ObjectsV2Memory {
    inner: MemoryObjects,
}
#[wasm_bindgen]
impl ObjectsV2Memory {
    #[wasm_bindgen(constructor)]
    pub fn new(maximum_bytes: u64, maximum_entries: usize) -> Result<Self, JsValue> {
        Ok(Self {
            inner: MemoryObjects::new(MemoryOptions {
                maximum_bytes: usize::try_from(maximum_bytes)
                    .map_err(|_| error(wire::ErrorCode::QuotaExceeded.into()))?,
                maximum_entries,
            })
            .map_err(error)?,
        })
    }
    #[allow(clippy::too_many_lines)]
    pub async fn invoke(
        &self,
        route: String,
        bytes: Vec<u8>,
        body: Vec<u8>,
        maximum: u64,
    ) -> Result<js_sys::Array, JsValue> {
        request::validate_binary(&route, &bytes, body.len() as u64).map_err(error)?;
        macro_rules! unary {
            ($method:ident) => {
                frames(&self.inner.$method(decode(&bytes)?).await.map_err(error)?)
            };
        }
        Ok(match route.as_str() {
            "buckets/create" => unary!(create_bucket),
            "buckets/head" => unary!(head_bucket),
            "buckets/delete" => unary!(delete_bucket),
            "objects/put" => frames(
                &self
                    .inner
                    .put(decode(&bytes)?, Bytes::from(body))
                    .await
                    .map_err(error)?,
            ),
            "objects/head" => unary!(head),
            "objects/delete" => unary!(delete),
            "objects/list" => unary!(list),
            "multipart/create" => unary!(create_multipart),
            "multipart/upload-part" => frames(
                &self
                    .inner
                    .upload_part(decode(&bytes)?, Bytes::from(body))
                    .await
                    .map_err(error)?,
            ),
            "multipart/list-parts" => unary!(list_parts),
            "multipart/complete" => unary!(complete_multipart),
            "multipart/abort" => unary!(abort_multipart),
            "objects/get" => {
                let object = self
                    .inner
                    .get(decode(&bytes)?, maximum)
                    .await
                    .map_err(error)?;
                let result = frames(&wire::GetObjectResponse {
                    frame: Some(wire::get_object_response::Frame::Header(object.header)),
                });
                for chunk in object.body.chunks(65_536) {
                    let value = wire::GetObjectResponse {
                        frame: Some(wire::get_object_response::Frame::Body(chunk.to_vec())),
                    };
                    result.push(&js_sys::Uint8Array::from(value.encode_to_vec().as_slice()));
                }
                result
            }
            _ => return Err(error(wire::ErrorCode::InvalidArgument.into())),
        })
    }
}
