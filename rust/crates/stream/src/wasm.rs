//! Browser boundary for canonical Stream request validation.

use futures::{
    FutureExt, StreamExt,
    future::{Either, select},
};
use js_sys::{Array, BigInt, Date, Object, Reflect, Uint8Array};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tsify_next::Tsify;
use wasm_bindgen::{JsCast, prelude::*};

use crate::{
    MAX_COMMAND_BYTES, MemoryStream, StreamError, StreamProvider, memory, wire, wire_codec,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, Tsify)]
#[serde(rename_all = "snake_case")]
#[tsify(from_wasm_abi, into_wasm_abi)]
#[allow(missing_docs)]
pub enum StreamErrorCode {
    InvalidPath,
    InvalidArgument,
    LimitExceeded,
    NotFound,
    AlreadyExists,
    Retired,
    PrefixNotRetained,
    OutOfRange,
    IdempotencyMismatch,
    Capacity,
    AccessDenied,
    Unavailable,
    HierarchyChanged,
    DeadlineElapsed,
    Unsupported,
}

impl StreamErrorCode {
    #[cfg(test)]
    const ALL: [Self; 15] = [
        Self::InvalidPath,
        Self::InvalidArgument,
        Self::LimitExceeded,
        Self::NotFound,
        Self::AlreadyExists,
        Self::Retired,
        Self::PrefixNotRetained,
        Self::OutOfRange,
        Self::IdempotencyMismatch,
        Self::Capacity,
        Self::AccessDenied,
        Self::Unavailable,
        Self::HierarchyChanged,
        Self::DeadlineElapsed,
        Self::Unsupported,
    ];

    const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidPath => "invalid_path",
            Self::InvalidArgument => "invalid_argument",
            Self::LimitExceeded => "limit_exceeded",
            Self::NotFound => "not_found",
            Self::AlreadyExists => "already_exists",
            Self::Retired => "retired",
            Self::PrefixNotRetained => "prefix_not_retained",
            Self::OutOfRange => "out_of_range",
            Self::IdempotencyMismatch => "idempotency_mismatch",
            Self::Capacity => "capacity",
            Self::AccessDenied => "access_denied",
            Self::Unavailable => "unavailable",
            Self::HierarchyChanged => "hierarchy_changed",
            Self::DeadlineElapsed => "deadline_elapsed",
            Self::Unsupported => "unsupported",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "invalid_path" => Self::InvalidPath,
            "invalid_argument" => Self::InvalidArgument,
            "limit_exceeded" => Self::LimitExceeded,
            "not_found" => Self::NotFound,
            "already_exists" => Self::AlreadyExists,
            "retired" => Self::Retired,
            "prefix_not_retained" => Self::PrefixNotRetained,
            "out_of_range" => Self::OutOfRange,
            "idempotency_mismatch" => Self::IdempotencyMismatch,
            "capacity" => Self::Capacity,
            "access_denied" => Self::AccessDenied,
            "unavailable" => Self::Unavailable,
            "hierarchy_changed" => Self::HierarchyChanged,
            "deadline_elapsed" => Self::DeadlineElapsed,
            "unsupported" => Self::Unsupported,
            _ => return None,
        })
    }
}

fn error_code(error: &StreamError) -> StreamErrorCode {
    match error {
        StreamError::InvalidPath => StreamErrorCode::InvalidPath,
        StreamError::InvalidArgument => StreamErrorCode::InvalidArgument,
        StreamError::LimitExceeded => StreamErrorCode::LimitExceeded,
        StreamError::NotFound => StreamErrorCode::NotFound,
        StreamError::AlreadyExists => StreamErrorCode::AlreadyExists,
        StreamError::Retired => StreamErrorCode::Retired,
        StreamError::PrefixNotRetained => StreamErrorCode::PrefixNotRetained,
        StreamError::OutOfRange => StreamErrorCode::OutOfRange,
        StreamError::IdempotencyMismatch => StreamErrorCode::IdempotencyMismatch,
        StreamError::Capacity => StreamErrorCode::Capacity,
        StreamError::AccessDenied => StreamErrorCode::AccessDenied,
        StreamError::Unavailable => StreamErrorCode::Unavailable,
        StreamError::HierarchyChanged => StreamErrorCode::HierarchyChanged,
        StreamError::DeadlineElapsed => StreamErrorCode::DeadlineElapsed,
        StreamError::Unsupported => StreamErrorCode::Unsupported,
    }
}

fn error_code_str(error: &StreamError) -> &'static str {
    error_code(error).as_str()
}

#[allow(clippy::needless_pass_by_value)]
fn js_error(error: StreamError) -> JsValue {
    let value = js_sys::Error::new(&error.to_string());
    let _property_result = Reflect::set(
        value.as_ref(),
        &JsValue::from_str("code"),
        &JsValue::from_str(error_code(&error).as_str()),
    );
    value.into()
}

/// Return whether a code can be emitted by this WASM adapter.
///
/// Keeping this validator beside the Rust error mapping prevents the TypeScript adapter from
/// maintaining a second, potentially stale list of base Stream error codes.
#[wasm_bindgen]
pub fn is_stream_error_code(value: &str) -> bool {
    StreamErrorCode::from_str(value).is_some()
}

/// Project a hosted HTTP error code onto the public Stream error vocabulary.
///
/// The hosted API may report either the Rust-owned wire code or a public alias.
/// Unknown values and a commit-only alias on another route return no value.
#[wasm_bindgen(js_name = publicHttpErrorCode)]
pub fn public_http_error_code(raw: &str, route: &str) -> Option<String> {
    let code = match raw {
        "stream_not_found" => "stream_not_found",
        "destination_exists" => "destination_exists",
        "stream_retired" => "stream_retired",
        "cursor_trimmed" => "cursor_trimmed",
        "capacity_exhausted" => "capacity_exhausted",
        "commit_not_found" => "commit_not_found",
        _ => match StreamErrorCode::from_str(raw)? {
            StreamErrorCode::InvalidPath => "invalid_path",
            StreamErrorCode::InvalidArgument => "invalid_argument",
            StreamErrorCode::LimitExceeded => "limit_exceeded",
            StreamErrorCode::NotFound => {
                if route == "commits/read" {
                    "commit_not_found"
                } else {
                    "stream_not_found"
                }
            }
            StreamErrorCode::AlreadyExists => "destination_exists",
            StreamErrorCode::Retired => "stream_retired",
            StreamErrorCode::PrefixNotRetained => "prefix_not_retained",
            StreamErrorCode::OutOfRange => "cursor_trimmed",
            StreamErrorCode::IdempotencyMismatch => "idempotency_mismatch",
            StreamErrorCode::Capacity => "capacity_exhausted",
            StreamErrorCode::AccessDenied => "access_denied",
            StreamErrorCode::Unavailable => "unavailable",
            StreamErrorCode::HierarchyChanged => "hierarchy_changed",
            StreamErrorCode::DeadlineElapsed => "deadline_elapsed",
            StreamErrorCode::Unsupported => "unsupported",
        },
    };
    if code == "commit_not_found" && route != "commits/read" {
        return None;
    }
    Some(code.to_owned())
}

/// Type-only bridge for the complete Rust-owned Stream error-code contract.
#[wasm_bindgen(js_name = __streamErrorCodeContract)]
pub fn stream_error_code_contract(value: StreamErrorCode) -> StreamErrorCode {
    value
}

fn decode<T: Message + Default>(input: &[u8]) -> Result<T, JsValue> {
    T::decode(input).map_err(|_| js_error(StreamError::InvalidArgument))
}

fn check_command_size(input: &[u8]) -> Result<(), JsValue> {
    if input.len() > MAX_COMMAND_BYTES {
        Err(js_error(StreamError::LimitExceeded))
    } else {
        Ok(())
    }
}

/// Stateful browser provider backed by the canonical Rust memory provider.
///
/// Unary operations use `dispatch(operation, request_bytes)` and return the
/// corresponding protobuf response bytes. `read` and `children` return arrays
/// of encoded stream response messages because protobuf streams have no single
/// finite response envelope.
#[wasm_bindgen]
pub struct WasmMemoryStream {
    provider: MemoryStream,
}

struct FollowState {
    stream: Mutex<Option<crate::RecordStream>>,
    cancel: tokio::sync::watch::Sender<bool>,
}

/// One Rust-backed live follow cursor.
///
/// `next` releases the state lock before awaiting the stream, so `close` can
/// always signal a pending call and promptly release its cursor.
#[wasm_bindgen]
pub struct WasmFollow {
    state: Arc<FollowState>,
}

async fn dispatch_commit(provider: &MemoryStream, input: &[u8]) -> Result<Vec<u8>, JsValue> {
    let request = decode::<wire::CommitRequest>(input)?;
    let deadline_unix_millis = request.deadline_unix_millis;
    let request = wire_codec::commit_from_wire(request).map_err(js_error)?;
    let response = match deadline_unix_millis {
        Some(deadline) => provider
            .commit_before(request, deadline)
            .await
            .map_err(js_error)?,
        None => provider.commit(request).await.map_err(js_error)?,
    };
    Ok(wire_codec::commit_outcome_to_wire(response).encode_to_vec())
}

#[wasm_bindgen]
impl WasmMemoryStream {
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new() -> Self {
        Self {
            provider: MemoryStream::default(),
        }
    }

    /// Executes one finite unary operation over canonical protobuf bytes.
    #[wasm_bindgen]
    pub async fn dispatch(&self, operation: &str, input: &[u8]) -> Result<Vec<u8>, JsValue> {
        check_command_size(input)?;
        let output = match operation {
            "inspect_idempotency" => {
                let request = decode::<wire::InspectIdempotencyRequest>(input)?;
                let key = crate::IdempotencyKey::new(request.idempotency_key).map_err(js_error)?;
                let response = self
                    .provider
                    .inspect_idempotency(key)
                    .await
                    .map_err(js_error)?;
                match response {
                    Some(value) => wire_codec::observation_to_wire(value).encode_to_vec(),
                    None => wire::InspectIdempotencyResponse { observation: None }.encode_to_vec(),
                }
            }
            "tail" => {
                let request = decode::<wire::TailRequest>(input)?;
                let path = wire_codec::path(request.path).map_err(js_error)?;
                let tail = self.provider.tail(path).await.map_err(js_error)?;
                wire::TailResponse {
                    tail,
                    trim_point: None,
                }
                .encode_to_vec()
            }
            "bounds" => {
                let request = decode::<wire::TailRequest>(input)?;
                let path = wire_codec::path(request.path).map_err(js_error)?;
                let bounds = self.provider.bounds(path).await.map_err(js_error)?;
                wire::TailResponse {
                    tail: bounds.tail,
                    trim_point: Some(bounds.trim_point),
                }
                .encode_to_vec()
            }
            "children_page" => {
                let request = decode::<wire::ChildrenPageRequest>(input)?;
                let request = wire_codec::children_page_from_wire(request).map_err(js_error)?;
                let page = self
                    .provider
                    .children_page(request)
                    .await
                    .map_err(js_error)?;
                wire_codec::children_page_to_wire(page).encode_to_vec()
            }
            "append" => {
                let request = decode::<wire::AppendRequest>(input)?;
                let request = wire_codec::append_from_wire(request).map_err(js_error)?;
                let response = self.provider.append(request).await.map_err(js_error)?;
                wire_codec::append_outcome_to_wire(response).encode_to_vec()
            }
            "fork" => {
                let request = decode::<wire::ForkRequest>(input)?;
                let request = wire_codec::fork_from_wire(request).map_err(js_error)?;
                let response = self.provider.fork(request).await.map_err(js_error)?;
                wire_codec::fork_receipt_to_wire(&response).encode_to_vec()
            }
            "trim" => {
                let request = decode::<wire::TrimRequest>(input)?;
                let (path, before, key) = wire_codec::trim_from_wire(request).map_err(js_error)?;
                let key = key.ok_or_else(|| js_error(StreamError::InvalidArgument))?;
                let response = self
                    .provider
                    .trim(path, before, key)
                    .await
                    .map_err(js_error)?;
                wire_codec::trim_receipt_to_wire(&response).encode_to_vec()
            }
            "delete" => {
                let request = decode::<wire::DeleteRequest>(input)?;
                let (path, key) = wire_codec::delete_from_wire(request).map_err(js_error)?;
                let key = key.ok_or_else(|| js_error(StreamError::InvalidArgument))?;
                let response = self.provider.delete(path, key).await.map_err(js_error)?;
                wire_codec::delete_receipt_to_wire(&response).encode_to_vec()
            }
            "commit" => dispatch_commit(&self.provider, input).await?,
            "read_commit" => {
                let request = decode::<wire::ReadCommitRequest>(input)?;
                let commit_id = <[u8; 32]>::try_from(request.commit_id.as_ref())
                    .map_err(|_| js_error(StreamError::InvalidArgument))?;
                let response = self
                    .provider
                    .read_commit(crate::CommitId::from_bytes(commit_id))
                    .await
                    .map_err(js_error)?;
                wire_codec::envelope_to_wire(response).encode_to_vec()
            }
            _ => return Err(js_error(StreamError::InvalidArgument)),
        };
        Ok(output)
    }

    /// Reads one bounded page, returning encoded `ReadResponse` messages.
    #[wasm_bindgen(unchecked_return_type = "Uint8Array[]")]
    pub async fn read(&self, input: &[u8]) -> Result<JsValue, JsValue> {
        check_command_size(input)?;
        let request = decode::<wire::ReadRequest>(input)?;
        let request = wire_codec::read_from_wire(request).map_err(js_error)?;
        let records = self.provider.read(request).await.map_err(js_error)?;
        let mut output = Vec::new();
        futures::pin_mut!(records);
        while let Some(record) = records.next().await {
            let record = record.map_err(js_error)?;
            output.push(
                wire::ReadResponse {
                    record: Some(wire::Record {
                        sequence: record.sequence,
                        value: record.value,
                        commit_id: record.commit_id.as_bytes().to_vec().into(),
                    }),
                }
                .encode_to_vec(),
            );
        }
        Ok(bytes_array(output))
    }

    /// Lists one fixed-snapshot child page, returning encoded `ChildrenResponse` messages.
    #[wasm_bindgen(unchecked_return_type = "Uint8Array[]")]
    pub async fn children(&self, input: &[u8]) -> Result<JsValue, JsValue> {
        check_command_size(input)?;
        let request = decode::<wire::ChildrenRequest>(input)?;
        let request = wire_codec::children_from_wire(request).map_err(js_error)?;
        let children = self.provider.children(request).await.map_err(js_error)?;
        let mut output = Vec::new();
        futures::pin_mut!(children);
        while let Some(child) = children.next().await {
            let child = child.map_err(js_error)?;
            output.push(
                wire::ChildrenResponse {
                    child: Some(wire::Child {
                        path: child.path.to_string(),
                    }),
                }
                .encode_to_vec(),
            );
        }
        Ok(bytes_array(output))
    }

    /// Opens a live follow cursor backed by the canonical provider.
    #[wasm_bindgen]
    pub async fn open_follow(&self, input: &[u8]) -> Result<WasmFollow, JsValue> {
        check_command_size(input)?;
        let request = decode::<wire::FollowRequest>(input)?;
        let (path, from) = wire_codec::follow_from_wire(request).map_err(js_error)?;
        let stream = self.provider.follow(path, from).await.map_err(js_error)?;
        let (cancel, _) = tokio::sync::watch::channel(false);
        Ok(WasmFollow {
            state: Arc::new(FollowState {
                stream: Mutex::new(Some(stream)),
                cancel,
            }),
        })
    }
}

fn bytes_array(values: Vec<Vec<u8>>) -> JsValue {
    let array = Array::new();
    for value in values {
        array.push(&Uint8Array::from(value.as_slice()));
    }
    array.into()
}

#[wasm_bindgen]
impl WasmFollow {
    /// Waits for one record. Returns `null` after close or stream termination.
    #[wasm_bindgen(unchecked_return_type = "Uint8Array | null")]
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        let Some(mut stream) = self
            .state
            .stream
            .lock()
            .ok()
            .and_then(|mut guard| guard.take())
        else {
            return Ok(JsValue::NULL);
        };
        let mut cancellation = self.state.cancel.subscribe();
        if *cancellation.borrow() {
            return Ok(JsValue::NULL);
        }
        let next = stream.next().fuse();
        let cancelled = cancellation.changed().fuse();
        futures::pin_mut!(next, cancelled);
        match select(next, cancelled).await {
            Either::Left((Some(Ok(record)), _)) => {
                if let Ok(mut guard) = self.state.stream.lock() {
                    if *self.state.cancel.borrow() {
                        return Ok(JsValue::NULL);
                    }
                    guard.replace(stream);
                }
                Ok(JsValue::from(Uint8Array::from(
                    wire::ReadResponse {
                        record: Some(wire::Record {
                            sequence: record.sequence,
                            value: record.value,
                            commit_id: record.commit_id.as_bytes().to_vec().into(),
                        }),
                    }
                    .encode_to_vec()
                    .as_slice(),
                )))
            }
            Either::Left((Some(Err(error)), _)) => Err(js_error(error)),
            Either::Left((None, _)) | Either::Right((_, _)) => Ok(JsValue::NULL),
        }
    }

    /// Cancels the cursor and wakes any pending `next` call.
    #[wasm_bindgen]
    pub fn close(&self) {
        self.state.cancel.send_replace(true);
        if let Ok(mut guard) = self.state.stream.lock() {
            guard.take();
        }
    }
}

/// Validate canonical protobuf bytes for one append request.
///
/// The empty string means that the request passed the same domain validators as
/// the in-memory provider. Otherwise this returns one stable error code.
#[wasm_bindgen(js_name = validateAppendRequest)]
pub fn validate_append_request(input: &[u8]) -> String {
    let result = check_command_size(input)
        .map_err(|_| StreamError::LimitExceeded)
        .and_then(|_| wire::AppendRequest::decode(input).map_err(|_| StreamError::InvalidArgument))
        .and_then(wire_codec::append_from_wire)
        .and_then(|request| {
            memory::validate_records(&request.records)?;
            memory::validate_append_size(&request)
        });
    result.map_or_else(|error| error_code_str(&error).to_owned(), |_| String::new())
}

/// Validate one canonical Stream path using the same parser used by every
/// provider and wire decoder.
///
/// The empty string means success; failures use the stable Stream error code
/// consumed by the TypeScript adapter.
#[wasm_bindgen(js_name = validatePath)]
pub fn validate_path(path: &str) -> String {
    crate::StreamPath::new(path)
        .map_or_else(|error| error_code_str(&error).to_owned(), |_| String::new())
}

/// Validate one JavaScript representation of a canonical Stream sequence.
///
/// JavaScript passes the decimal spelling of its `bigint`; Rust owns the
/// unsigned 64-bit range accepted by every Stream wire field.
#[wasm_bindgen(js_name = validateSequence)]
pub fn validate_sequence(value: &str) -> String {
    if value.parse::<u64>().is_ok() {
        String::new()
    } else {
        error_code_str(&StreamError::InvalidArgument).to_owned()
    }
}

/// Normalize and encode canonical protobuf bytes for one commit request.
///
/// The returned bytes use the same deterministic ordering as the in-memory
/// provider. Validation failures are thrown as stable error codes.
#[wasm_bindgen(js_name = normalizeCommitRequest)]
pub fn normalize_commit_request(input: &[u8]) -> Result<Vec<u8>, JsValue> {
    normalize_commit_bytes(input).map_err(JsValue::from_str)
}

fn normalize_commit_bytes(input: &[u8]) -> Result<Vec<u8>, &'static str> {
    if input.len() > MAX_COMMAND_BYTES {
        return Err("limit_exceeded");
    }
    let request = wire::CommitRequest::decode(input).map_err(|_| "invalid_argument")?;
    let deadline_unix_millis = request.deadline_unix_millis;
    let mut request =
        wire_codec::commit_from_wire(request).map_err(|error| error_code_str(&error))?;
    memory::normalize_commit(&mut request).map_err(|error| error_code_str(&error))?;
    memory::validate_commit_shape(&request).map_err(|error| error_code_str(&error))?;
    let mut normalized = wire_codec::commit_to_wire(&request);
    normalized.deadline_unix_millis = deadline_unix_millis;
    Ok(normalized.encode_to_vec())
}

/// Validate one canonical protobuf request at the browser boundary.
///
/// `kind` is deliberately a small closed set so callers cannot accidentally
/// select a different validator after adding a new wire message. The empty
/// string means success; failures use the same stable codes as the append and
/// commit entry points.
#[wasm_bindgen(js_name = validateRequest)]
pub fn validate_request(kind: &str, input: &[u8]) -> String {
    let result = check_command_size(input)
        .map_err(|_| StreamError::LimitExceeded)
        .and_then(|_| match kind {
            "tail" => wire::TailRequest::decode(input)
                .map_err(|_| StreamError::InvalidArgument)
                .and_then(|request| wire_codec::path(request.path).map(|_| ())),
            "fork" => wire::ForkRequest::decode(input)
                .map_err(|_| StreamError::InvalidArgument)
                .and_then(wire_codec::fork_from_wire)
                .and_then(|request| {
                    if request.source == request.destination {
                        return Err(StreamError::InvalidArgument);
                    }
                    memory::validate_fork_size(&request)
                }),
            "trim" => wire::TrimRequest::decode(input)
                .map_err(|_| StreamError::InvalidArgument)
                .and_then(wire_codec::trim_from_wire)
                .map(|_| ()),
            "delete" => wire::DeleteRequest::decode(input)
                .map_err(|_| StreamError::InvalidArgument)
                .and_then(wire_codec::delete_from_wire)
                .map(|_| ()),
            "read" => wire::ReadRequest::decode(input)
                .map_err(|_| StreamError::InvalidArgument)
                .and_then(wire_codec::read_from_wire)
                .and_then(|request| memory::validate_limit(request.limit)),
            "follow" => wire::FollowRequest::decode(input)
                .map_err(|_| StreamError::InvalidArgument)
                .and_then(wire_codec::follow_from_wire)
                .map(|_| ()),
            "children" => wire::ChildrenRequest::decode(input)
                .map_err(|_| StreamError::InvalidArgument)
                .and_then(wire_codec::children_from_wire)
                .and_then(|request| memory::validate_limit(request.limit)),
            _ => Err(StreamError::InvalidArgument),
        });
    result.map_or_else(|error| error_code_str(&error).to_owned(), |_| String::new())
}

/// Encode one protobuf request into the hosted Stream HTTP JSON shape.
///
/// Protobuf remains the only request contract crossing from TypeScript into
/// Rust.  Rust owns the conversion of uint64 values and opaque bytes to the
/// decimal and base64 spellings required by the hosted API, keeping the HTTP
/// adapter from maintaining a second scalar conversion table.
#[wasm_bindgen(js_name = encodeHttpRequest)]
pub fn encode_http_request(route: &str, input: &[u8]) -> Result<String, JsValue> {
    check_command_size(input)?;
    http::encode(route, input).map_err(JsValue::from_str)
}

/// Validate one hosted HTTP JSON success response using the same path, width,
/// identity, and tagged-union rules as the canonical Stream domain.
///
/// The HTTP adapter keeps its intentionally simple JSON representation (u64
/// values are decimal strings and opaque bytes are base64). This entry point
/// validates that representation using the same Rust projection used by
/// `decodeHttpResponse`, without crossing a second scalar schema boundary.
#[wasm_bindgen(js_name = validateHttpResponse)]
pub fn validate_http_response(route: &str, response_json: &str) -> Result<(), JsValue> {
    let value: Value = serde_json::from_str(response_json)
        .map_err(|error| JsValue::from_str(&format!("invalid JSON: {error}")))?;
    http::validate(route, &value).map_err(JsValue::from_str)
}

/// Validate and project one hosted HTTP JSON success response into the public
/// JavaScript shape. Rust owns the scalar widths and tagged response schema:
/// decimal uint64 strings become `bigint`, base64 bytes become `Uint8Array`,
/// and token timestamps become `Date` values before the value crosses the
/// browser boundary.
#[wasm_bindgen(js_name = decodeHttpResponse, unchecked_return_type = "unknown")]
pub fn decode_http_response(route: &str, response_json: &str) -> Result<JsValue, JsValue> {
    let value: Value = serde_json::from_str(response_json)
        .map_err(|error| JsValue::from_str(&format!("invalid JSON: {error}")))?;
    http::decode(route, &value).map_err(JsValue::from_str)
}

/// Decode one unary memory-provider response from canonical protobuf bytes
/// into the public JavaScript result shape. Rust owns the response oneofs,
/// scalar widths, copied byte buffers, and camelCase projection at this
/// boundary; TypeScript keeps only request adaptation and cursor lifecycle.
#[wasm_bindgen(js_name = projectMemoryResponse, unchecked_return_type = "unknown")]
pub fn project_memory_response(operation: &str, input: &[u8]) -> Result<JsValue, JsValue> {
    http::decode_wire_response(operation, input).map_err(JsValue::from_str)
}

mod http {
    use super::*;

    type Result<T = ()> = std::result::Result<T, &'static str>;

    fn json_object(entries: Vec<(&str, Value)>) -> Value {
        Value::Object(
            entries
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value))
                .collect(),
        )
    }

    fn json_string(value: impl Into<String>) -> Value {
        Value::String(value.into())
    }

    fn json_u64(value: u64) -> Value {
        json_string(value.to_string())
    }

    fn json_bytes(value: &[u8]) -> Value {
        json_string(encode_base64(value))
    }

    fn optional_key_json(value: Option<&crate::IdempotencyKey>) -> Option<Value> {
        value.map(|value| json_bytes(value.as_bytes()))
    }

    fn options_json(
        sequence_name: &str,
        sequence: Option<u64>,
        key: Option<&crate::IdempotencyKey>,
    ) -> Option<Value> {
        let mut entries = Vec::new();
        if let Some(sequence) = sequence {
            entries.push((sequence_name, json_u64(sequence)));
        }
        if let Some(key) = optional_key_json(key) {
            entries.push(("idempotencyKey", key));
        }
        (!entries.is_empty()).then(|| json_object(entries))
    }

    fn valid_token_operation(value: &str) -> bool {
        matches!(
            value,
            "list"
                | "read"
                | "follow"
                | "append"
                | "fork"
                | "create"
                | "trim"
                | "delete"
                | "commit"
        )
    }

    fn valid_token_expiry(value: &str) -> bool {
        let bytes = value.as_bytes();
        let Some((unit, digits)) = bytes.split_last() else {
            return false;
        };
        !digits.is_empty()
            && digits.iter().all(|byte| byte.is_ascii_digit())
            && matches!(*unit, b's' | b'm' | b'h' | b'd')
    }

    fn validate_token_request(request: &wire::CreateTokenRequest) -> Result {
        if !valid_token_expiry(&request.expires_in) || request.allow.is_empty() {
            return Err("invalid_argument");
        }
        for grant in &request.allow {
            wire_codec::path(grant.path.clone()).map_err(|error| error_code_str(&error))?;
            if grant.operations.is_empty()
                || grant
                    .operations
                    .iter()
                    .any(|operation| !valid_token_operation(operation))
            {
                return Err("invalid_argument");
            }
        }
        Ok(())
    }

    fn request_json_append(input: &[u8]) -> Result<Value> {
        let request = wire::AppendRequest::decode(input).map_err(|_| "invalid_argument")?;
        let request =
            wire_codec::append_from_wire(request).map_err(|error| error_code_str(&error))?;
        memory::validate_records(&request.records).map_err(|error| error_code_str(&error))?;
        memory::validate_append_size(&request).map_err(|error| error_code_str(&error))?;
        let values = request
            .records
            .iter()
            .map(|value| json_bytes(value))
            .collect();
        let mut entries = vec![
            ("path", json_string(request.path.to_string())),
            ("values", Value::Array(values)),
        ];
        if let Some(options) =
            options_json("ifTail", request.if_tail, request.idempotency_key.as_ref())
        {
            entries.push(("options", options));
        }
        Ok(json_object(entries))
    }

    fn request_json_fork(input: &[u8]) -> Result<Value> {
        let request = wire::ForkRequest::decode(input).map_err(|_| "invalid_argument")?;
        let request =
            wire_codec::fork_from_wire(request).map_err(|error| error_code_str(&error))?;
        if request.source == request.destination {
            return Err("invalid_argument");
        }
        memory::validate_fork_size(&request).map_err(|error| error_code_str(&error))?;
        let mut entries = vec![
            ("source", json_string(request.source.to_string())),
            ("destination", json_string(request.destination.to_string())),
        ];
        if let Some(options) =
            options_json("atTail", request.at_tail, request.idempotency_key.as_ref())
        {
            entries.push(("options", options));
        }
        Ok(json_object(entries))
    }

    fn request_json_commit(input: &[u8]) -> Result<Value> {
        let request = wire::CommitRequest::decode(input).map_err(|_| "invalid_argument")?;
        let mut request =
            wire_codec::commit_from_wire(request).map_err(|error| error_code_str(&error))?;
        memory::normalize_commit(&mut request).map_err(|error| error_code_str(&error))?;
        memory::validate_commit_shape(&request).map_err(|error| error_code_str(&error))?;
        let conditions = request
            .conditions
            .iter()
            .map(|condition| match condition {
                crate::CommitCondition::Tail { path, expected } => json_object(vec![
                    ("path", json_string(path.to_string())),
                    ("ifTail", json_u64(*expected)),
                ]),
                crate::CommitCondition::Absent { path } => json_object(vec![
                    ("path", json_string(path.to_string())),
                    ("ifAbsent", Value::Bool(true)),
                ]),
            })
            .collect();
        let mutations = request
            .mutations
            .iter()
            .map(|mutation| match mutation {
                crate::CommitMutation::Append { path, records } => json_object(vec![(
                    "append",
                    json_object(vec![
                        ("path", json_string(path.to_string())),
                        (
                            "values",
                            Value::Array(records.iter().map(|value| json_bytes(value)).collect()),
                        ),
                    ]),
                )]),
                crate::CommitMutation::Fork {
                    source,
                    destination,
                    at_tail,
                    records,
                } => json_object(vec![(
                    "fork",
                    json_object(vec![
                        ("source", json_string(source.to_string())),
                        ("destination", json_string(destination.to_string())),
                        ("atTail", json_u64(*at_tail)),
                        (
                            "values",
                            Value::Array(records.iter().map(|record| json_bytes(record)).collect()),
                        ),
                    ]),
                )]),
                crate::CommitMutation::Trim { path, before } => json_object(vec![(
                    "trim",
                    json_object(vec![
                        ("path", json_string(path.to_string())),
                        ("before", json_u64(*before)),
                    ]),
                )]),
                crate::CommitMutation::Delete { path } => json_object(vec![(
                    "delete",
                    json_object(vec![("path", json_string(path.to_string()))]),
                )]),
            })
            .collect();
        Ok(json_object(vec![
            (
                "request",
                json_object(vec![
                    ("conditions", Value::Array(conditions)),
                    ("mutations", Value::Array(mutations)),
                ]),
            ),
            (
                "options",
                json_object(vec![(
                    "idempotencyKey",
                    json_bytes(request.idempotency_key.as_bytes()),
                )]),
            ),
        ]))
    }

    fn request_json_tokens_create(input: &[u8]) -> Result<Value> {
        let request = wire::CreateTokenRequest::decode(input).map_err(|_| "invalid_argument")?;
        validate_token_request(&request)?;
        let allow = request
            .allow
            .iter()
            .map(|grant| {
                let mut entries = vec![(
                    "path",
                    json_string(
                        wire_codec::path(grant.path.clone())
                            .map_err(|error| error_code_str(&error))?
                            .to_string(),
                    ),
                )];
                if let Some(subtree) = grant.subtree {
                    entries.push(("subtree", Value::Bool(subtree)));
                }
                entries.push((
                    "operations",
                    Value::Array(
                        grant
                            .operations
                            .iter()
                            .map(|operation| json_string(operation.clone()))
                            .collect(),
                    ),
                ));
                Ok(json_object(entries))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(json_object(vec![
            ("expiresIn", json_string(request.expires_in)),
            ("allow", Value::Array(allow)),
        ]))
    }

    fn request_json(route: &str, input: &[u8]) -> Result<Value> {
        if input.len() > MAX_COMMAND_BYTES {
            return Err("limit_exceeded");
        }
        match route {
            "idempotency/inspect" => {
                let request = wire::InspectIdempotencyRequest::decode(input)
                    .map_err(|_| "invalid_argument")?;
                let key = crate::IdempotencyKey::new(request.idempotency_key)
                    .map_err(|_| "invalid_argument")?;
                Ok(json_object(vec![(
                    "idempotencyKey",
                    json_bytes(key.as_bytes()),
                )]))
            }
            "tail" => {
                let request = wire::TailRequest::decode(input).map_err(|_| "invalid_argument")?;
                let path = wire_codec::path(request.path).map_err(|_| "invalid_path")?;
                Ok(json_object(vec![("path", json_string(path.to_string()))]))
            }
            "append" => request_json_append(input),
            "fork" => request_json_fork(input),
            "trim" => {
                let request = wire::TrimRequest::decode(input).map_err(|_| "invalid_argument")?;
                let (path, before, key) =
                    wire_codec::trim_from_wire(request).map_err(|error| error_code_str(&error))?;
                let mut entries = vec![
                    ("path", json_string(path.to_string())),
                    ("before", json_u64(before)),
                ];
                if let Some(key) = key {
                    entries.push(("idempotencyKey", json_bytes(key.as_bytes())));
                }
                Ok(json_object(entries))
            }
            "delete" => {
                let request = wire::DeleteRequest::decode(input).map_err(|_| "invalid_argument")?;
                let (path, key) = wire_codec::delete_from_wire(request)
                    .map_err(|error| error_code_str(&error))?;
                let mut entries = vec![("path", json_string(path.to_string()))];
                if let Some(key) = key {
                    entries.push(("idempotencyKey", json_bytes(key.as_bytes())));
                }
                Ok(json_object(entries))
            }
            "read" => {
                let request = wire::ReadRequest::decode(input).map_err(|_| "invalid_argument")?;
                let request =
                    wire_codec::read_from_wire(request).map_err(|error| error_code_str(&error))?;
                memory::validate_limit(request.limit).map_err(|error| error_code_str(&error))?;
                Ok(json_object(vec![
                    ("path", json_string(request.path.to_string())),
                    ("from", json_u64(request.from)),
                    ("limit", Value::from(request.limit)),
                ]))
            }
            "children" => {
                let request =
                    wire::ChildrenRequest::decode(input).map_err(|_| "invalid_argument")?;
                let request = wire_codec::children_from_wire(request)
                    .map_err(|error| error_code_str(&error))?;
                memory::validate_limit(request.limit).map_err(|error| error_code_str(&error))?;
                let mut entries = vec![("limit", Value::from(request.limit))];
                if let Some(parent) = request.parent {
                    entries.insert(0, ("parent", json_string(parent.to_string())));
                }
                Ok(json_object(entries))
            }
            "commit" => request_json_commit(input),
            "commits/read" => {
                let request =
                    wire::ReadCommitRequest::decode(input).map_err(|_| "invalid_argument")?;
                let commit_id = <[u8; 32]>::try_from(request.commit_id.as_ref())
                    .map_err(|_| "invalid_argument")?;
                Ok(json_object(vec![("commitId", json_bytes(&commit_id))]))
            }
            "tokens/create" => request_json_tokens_create(input),
            _ => Err("invalid_argument"),
        }
    }

    pub(super) fn encode(route: &str, input: &[u8]) -> Result<String> {
        serde_json::to_string(&request_json(route, input)?).map_err(|_| "could not encode request")
    }

    fn record_json(value: &wire::Record) -> Value {
        json_object(vec![
            ("sequence", json_u64(value.sequence)),
            ("value", json_bytes(value.value.as_ref())),
            ("commitId", json_bytes(value.commit_id.as_ref())),
        ])
    }

    fn append_value(value: wire::AppendResponse) -> Result<Value> {
        let outcome = value.outcome.ok_or("invalid_response")?;
        Ok(match outcome {
            wire::append_response::Outcome::Committed(receipt) => json_object(vec![
                ("ok", Value::Bool(true)),
                ("start", json_u64(receipt.start)),
                ("end", json_u64(receipt.end)),
                ("tail", json_u64(receipt.tail)),
                ("commitId", json_bytes(receipt.commit_id.as_ref())),
            ]),
            wire::append_response::Outcome::Conflict(conflict) => json_object(vec![
                ("ok", Value::Bool(false)),
                ("code", json_string("tail_conflict")),
                ("actualTail", json_u64(conflict.actual_tail)),
            ]),
        })
    }

    fn fork_value(value: wire::ForkReceipt) -> Value {
        json_object(vec![
            ("source", json_string(value.source)),
            ("destination", json_string(value.destination)),
            ("forkedAt", json_u64(value.forked_at)),
            ("tail", json_u64(value.tail)),
            ("commitId", json_bytes(value.commit_id.as_ref())),
        ])
    }

    fn trim_value(value: wire::TrimReceipt) -> Value {
        json_object(vec![
            ("path", json_string(value.path)),
            ("trimPoint", json_u64(value.trim_point)),
            ("commitId", json_bytes(value.commit_id.as_ref())),
        ])
    }

    fn delete_value(value: wire::DeleteReceipt) -> Value {
        json_object(vec![
            ("path", json_string(value.path)),
            ("commitId", json_bytes(value.commit_id.as_ref())),
        ])
    }

    fn mutation_value(value: &wire::CommittedMutation) -> Result<Value> {
        let mutation = value.mutation.as_ref().ok_or("invalid_response")?;
        Ok(match mutation {
            wire::committed_mutation::Mutation::Append(value) => json_object(vec![
                ("type", json_string("append")),
                ("path", json_string(value.path.clone())),
                ("start", json_u64(value.start)),
                ("end", json_u64(value.end)),
                ("tail", json_u64(value.tail)),
                (
                    "records",
                    Value::Array(value.records.iter().map(record_json).collect()),
                ),
            ]),
            wire::committed_mutation::Mutation::Fork(value) => json_object(vec![
                ("type", json_string("fork")),
                ("source", json_string(value.source.clone())),
                ("destination", json_string(value.destination.clone())),
                ("forkedAt", json_u64(value.forked_at)),
                ("tail", json_u64(value.tail)),
                (
                    "records",
                    Value::Array(value.records.iter().map(record_json).collect()),
                ),
            ]),
            wire::committed_mutation::Mutation::Trim(value) => json_object(vec![
                ("type", json_string("trim")),
                ("path", json_string(value.path.clone())),
                ("trimPoint", json_u64(value.trim_point)),
            ]),
            wire::committed_mutation::Mutation::Delete(value) => json_object(vec![
                ("type", json_string("delete")),
                ("path", json_string(value.path.clone())),
            ]),
        })
    }

    fn envelope_value(value: wire::CommittedEnvelope) -> Result<Value> {
        let mutations = value
            .mutations
            .iter()
            .map(mutation_value)
            .collect::<Result<Vec<_>>>()?;
        Ok(json_object(vec![
            ("commitId", json_bytes(value.commit_id.as_ref())),
            ("mutations", Value::Array(mutations)),
        ]))
    }

    fn conflict_value(value: &wire::CommitConflict) -> Result<Value> {
        let conflict = value.conflict.as_ref().ok_or("invalid_response")?;
        Ok(match conflict {
            wire::commit_conflict::Conflict::Tail(value) => {
                let mut entries = vec![
                    ("path", json_string(value.path.clone())),
                    ("expectedTail", json_u64(value.expected)),
                ];
                if let Some(actual) = value.actual {
                    entries.push(("actualTail", json_u64(actual)));
                }
                json_object(entries)
            }
            wire::commit_conflict::Conflict::Exists(value) => json_object(vec![
                ("path", json_string(value.path.clone())),
                ("expectedAbsent", Value::Bool(true)),
                ("actual", json_string("exists")),
            ]),
            wire::commit_conflict::Conflict::Retired(value) => json_object(vec![
                ("path", json_string(value.path.clone())),
                ("expectedAbsent", Value::Bool(true)),
                ("actual", json_string("retired")),
            ]),
        })
    }

    fn commit_value(value: wire::CommitResponse) -> Result<Value> {
        let outcome = value.outcome.ok_or("invalid_response")?;
        Ok(match outcome {
            wire::commit_response::Outcome::Committed(envelope) => {
                // Commit responses can be larger than the bounded command
                // request that produced them. Validate the complete envelope
                // before deriving its compact public result so nested records
                // cannot bypass identity and width checks.
                let envelope_json = envelope_value(envelope.clone())?;
                validate("commits/read", &envelope_json)?;
                let mut tails = serde_json::Map::new();
                let mut forks = Vec::new();
                for mutation in &envelope.mutations {
                    let mutation = mutation.mutation.as_ref().ok_or("invalid_response")?;
                    match mutation {
                        wire::committed_mutation::Mutation::Append(value) => {
                            tails.insert(value.path.clone(), json_u64(value.tail));
                        }
                        wire::committed_mutation::Mutation::Fork(value) => {
                            forks.push(json_object(vec![
                                ("path", json_string(value.destination.clone())),
                                ("tail", json_u64(value.tail)),
                            ]))
                        }
                        wire::committed_mutation::Mutation::Trim(_)
                        | wire::committed_mutation::Mutation::Delete(_) => {}
                    }
                }
                json_object(vec![
                    ("ok", Value::Bool(true)),
                    ("commitId", json_bytes(envelope.commit_id.as_ref())),
                    ("tails", Value::Object(tails)),
                    ("forks", Value::Array(forks)),
                ])
            }
            wire::commit_response::Outcome::Conflict(conflicts) => json_object(vec![
                ("ok", Value::Bool(false)),
                ("code", json_string("conflict")),
                (
                    "conflicts",
                    Value::Array(
                        conflicts
                            .conflicts
                            .iter()
                            .map(conflict_value)
                            .collect::<Result<Vec<_>>>()?,
                    ),
                ),
            ]),
        })
    }

    fn observation_value(value: wire::InspectIdempotencyResponse) -> Result<Option<Value>> {
        let Some(observation) = value.observation else {
            return Ok(None);
        };
        let outcome = observation.outcome.ok_or("invalid_response")?;
        let outcome = match outcome {
            wire::idempotency_observation::Outcome::Append(value) => json_object(vec![
                ("type", json_string("append")),
                ("outcome", append_value(value)?),
            ]),
            wire::idempotency_observation::Outcome::Fork(value) => json_object(vec![
                ("type", json_string("fork")),
                ("receipt", fork_value(value)),
            ]),
            wire::idempotency_observation::Outcome::Trim(value) => json_object(vec![
                ("type", json_string("trim")),
                ("receipt", trim_value(value)),
            ]),
            wire::idempotency_observation::Outcome::Delete(value) => json_object(vec![
                ("type", json_string("delete")),
                ("receipt", delete_value(value)),
            ]),
            wire::idempotency_observation::Outcome::Commit(value) => json_object(vec![
                ("type", json_string("commit")),
                ("outcome", commit_value(value)?),
            ]),
        };
        Ok(Some(json_object(vec![
            (
                "idempotencyKey",
                json_bytes(observation.idempotency_key.as_ref()),
            ),
            (
                "requestDigest",
                json_bytes(observation.request_digest.as_ref()),
            ),
            ("outcome", outcome),
        ])))
    }

    pub(super) fn decode_wire_response(operation: &str, input: &[u8]) -> Result<JsValue> {
        let (route, value) = match operation {
            "inspect_idempotency" => {
                let value = wire::InspectIdempotencyResponse::decode(input)
                    .map_err(|_| "invalid_response")?;
                let Some(value) = observation_value(value)? else {
                    return Ok(JsValue::UNDEFINED);
                };
                ("idempotency/inspect", value)
            }
            "append" => (
                "append",
                append_value(wire::AppendResponse::decode(input).map_err(|_| "invalid_response")?)?,
            ),
            "fork" => (
                "fork",
                fork_value(wire::ForkReceipt::decode(input).map_err(|_| "invalid_response")?),
            ),
            "trim" => (
                "trim",
                trim_value(wire::TrimReceipt::decode(input).map_err(|_| "invalid_response")?),
            ),
            "delete" => (
                "delete",
                delete_value(wire::DeleteReceipt::decode(input).map_err(|_| "invalid_response")?),
            ),
            "commit" => (
                "commit",
                commit_value(wire::CommitResponse::decode(input).map_err(|_| "invalid_response")?)?,
            ),
            "read_commit" => (
                "commits/read",
                envelope_value(
                    wire::CommittedEnvelope::decode(input).map_err(|_| "invalid_response")?,
                )?,
            ),
            _ => return Err("invalid_argument"),
        };
        decode(route, &value)
    }

    fn object(value: &Value) -> Result<&serde_json::Map<String, Value>> {
        value.as_object().ok_or("expected object")
    }
    fn field<'a>(item: &'a serde_json::Map<String, Value>, name: &str) -> Result<&'a Value> {
        item.get(name).ok_or("missing field")
    }
    fn string(value: &Value) -> Result<&str> {
        value
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or("expected non-empty string")
    }
    fn encoded_bytes(value: &Value) -> Result<&str> {
        value.as_str().ok_or("expected base64 string")
    }
    fn u64_string(value: &Value) -> Result<u64> {
        let value = string(value)?;
        if !value.as_bytes().iter().all(|byte| byte.is_ascii_digit())
            || (value.len() > 1 && value.starts_with('0'))
        {
            return Err("expected decimal uint64 string");
        }
        value.parse().map_err(|_| "expected decimal uint64 string")
    }
    fn boolean(value: &Value) -> Result<bool> {
        value.as_bool().ok_or("expected boolean")
    }
    fn array(value: &Value) -> Result<&Vec<Value>> {
        value.as_array().ok_or("expected array")
    }
    fn path(value: &Value) -> Result<()> {
        let value = string(value)?;
        crate::StreamPath::new(value)
            .map(|_| ())
            .map_err(|_| "invalid path")
    }
    fn bytes(value: &Value) -> Result<Vec<u8>> {
        let value = encoded_bytes(value)?;
        decode_base64(value).ok_or("expected base64")
    }
    fn id(value: &Value) -> Result<()> {
        (bytes(value)?.len() == 32)
            .then_some(())
            .ok_or("expected 32-byte id")
    }
    fn key(value: &Value) -> Result<()> {
        let len = bytes(value)?.len();
        (len > 0 && len <= crate::MAX_IDEMPOTENCY_KEY_BYTES)
            .then_some(())
            .ok_or("invalid idempotency key")
    }
    fn set(object: &Object, name: &str, value: &JsValue) -> Result {
        Reflect::set(object.as_ref(), &JsValue::from_str(name), value)
            .map(|_| ())
            .map_err(|_| "could not construct response value")
    }

    fn string_js(value: &Value) -> Result<JsValue> {
        Ok(JsValue::from_str(string(value)?))
    }

    fn bigint_js(value: &Value) -> Result<JsValue> {
        Ok(JsValue::from(BigInt::from(u64_string(value)?)))
    }

    fn bytes_js(value: &Value) -> Result<JsValue> {
        Ok(Uint8Array::from(bytes(value)?.as_slice()).into())
    }

    fn array_js<F>(value: &Value, mut convert: F) -> Result<JsValue>
    where
        F: FnMut(&Value) -> Result<JsValue>,
    {
        let result = Array::new();
        for item in array(value)? {
            result.push(&convert(item)?);
        }
        Ok(result.into())
    }

    fn record_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        id(field(item, "commitId")?)?;
        let result = Object::new();
        set(&result, "sequence", &bigint_js(field(item, "sequence")?)?)?;
        set(&result, "value", &bytes_js(field(item, "value")?)?)?;
        set(&result, "commitId", &bytes_js(field(item, "commitId")?)?)?;
        Ok(result.into())
    }

    fn append_result_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        let result = Object::new();
        let ok = boolean(field(item, "ok")?)?;
        set(&result, "ok", &JsValue::from_bool(ok))?;
        if ok {
            let start = u64_string(field(item, "start")?)?;
            let end = u64_string(field(item, "end")?)?;
            let tail = u64_string(field(item, "tail")?)?;
            if start > end || end > tail {
                return Err("invalid append positions");
            }
            id(field(item, "commitId")?)?;
            set(&result, "start", &bigint_js(field(item, "start")?)?)?;
            set(&result, "end", &bigint_js(field(item, "end")?)?)?;
            set(&result, "tail", &bigint_js(field(item, "tail")?)?)?;
            set(&result, "commitId", &bytes_js(field(item, "commitId")?)?)?;
            return Ok(result.into());
        }
        if string(field(item, "code")?)? != "tail_conflict" {
            return Err("invalid append conflict code");
        }
        set(&result, "code", &string_js(field(item, "code")?)?)?;
        set(
            &result,
            "actualTail",
            &bigint_js(field(item, "actualTail")?)?,
        )?;
        Ok(result.into())
    }

    fn fork_receipt_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        path(field(item, "source")?)?;
        path(field(item, "destination")?)?;
        let forked_at = u64_string(field(item, "forkedAt")?)?;
        let tail = u64_string(field(item, "tail")?)?;
        if forked_at > tail {
            return Err("invalid fork positions");
        }
        id(field(item, "commitId")?)?;
        let result = Object::new();
        set(&result, "source", &string_js(field(item, "source")?)?)?;
        set(
            &result,
            "destination",
            &string_js(field(item, "destination")?)?,
        )?;
        set(&result, "forkedAt", &bigint_js(field(item, "forkedAt")?)?)?;
        set(&result, "tail", &bigint_js(field(item, "tail")?)?)?;
        set(&result, "commitId", &bytes_js(field(item, "commitId")?)?)?;
        Ok(result.into())
    }

    fn trim_receipt_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        path(field(item, "path")?)?;
        u64_string(field(item, "trimPoint")?)?;
        id(field(item, "commitId")?)?;
        let result = Object::new();
        set(&result, "path", &string_js(field(item, "path")?)?)?;
        set(&result, "trimPoint", &bigint_js(field(item, "trimPoint")?)?)?;
        set(&result, "commitId", &bytes_js(field(item, "commitId")?)?)?;
        Ok(result.into())
    }

    fn delete_receipt_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        path(field(item, "path")?)?;
        id(field(item, "commitId")?)?;
        let result = Object::new();
        set(&result, "path", &string_js(field(item, "path")?)?)?;
        set(&result, "commitId", &bytes_js(field(item, "commitId")?)?)?;
        Ok(result.into())
    }

    fn conflict_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        path(field(item, "path")?)?;
        let result = Object::new();
        set(&result, "path", &string_js(field(item, "path")?)?)?;
        if item.get("expectedAbsent") == Some(&Value::Bool(true)) {
            match string(field(item, "actual")?)? {
                "exists" | "retired" => {}
                _ => return Err("invalid conflict state"),
            }
            set(&result, "expectedAbsent", &JsValue::TRUE)?;
            set(&result, "actual", &string_js(field(item, "actual")?)?)?;
        } else {
            u64_string(field(item, "expectedTail")?)?;
            if let Some(actual) = item.get("actualTail") {
                u64_string(actual)?;
            }
            set(
                &result,
                "expectedTail",
                &bigint_js(field(item, "expectedTail")?)?,
            )?;
            if let Some(actual) = item.get("actualTail") {
                set(&result, "actualTail", &bigint_js(actual)?)?;
            }
        }
        Ok(result.into())
    }

    fn tails_js(value: &Value) -> Result<JsValue> {
        let null_prototype: Object = JsValue::NULL.unchecked_into();
        let result = Object::create(&null_prototype);
        for (path_name, tail) in object(value)? {
            // A null prototype keeps `__proto__` as an own data property.
            crate::StreamPath::new(path_name).map_err(|_| "invalid path")?;
            set(&result, path_name, &bigint_js(tail)?)?;
        }
        Ok(result.into())
    }

    fn commit_result_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        let result = Object::new();
        let ok = boolean(field(item, "ok")?)?;
        set(&result, "ok", &JsValue::from_bool(ok))?;
        if ok {
            id(field(item, "commitId")?)?;
            set(&result, "commitId", &bytes_js(field(item, "commitId")?)?)?;
            set(&result, "tails", &tails_js(field(item, "tails")?)?)?;
            set(
                &result,
                "forks",
                &array_js(field(item, "forks")?, |value| {
                    let fork = object(value)?;
                    path(field(fork, "path")?)?;
                    u64_string(field(fork, "tail")?)?;
                    let result = Object::new();
                    set(&result, "path", &string_js(field(fork, "path")?)?)?;
                    set(&result, "tail", &bigint_js(field(fork, "tail")?)?)?;
                    Ok(result.into())
                })?,
            )?;
        } else {
            if string(field(item, "code")?)? != "conflict" {
                return Err("invalid commit conflict code");
            }
            set(&result, "code", &string_js(field(item, "code")?)?)?;
            set(
                &result,
                "conflicts",
                &array_js(field(item, "conflicts")?, conflict_js)?,
            )?;
        }
        Ok(result.into())
    }

    fn mutation_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        let result = Object::new();
        let kind = string(field(item, "type")?)?;
        set(&result, "type", &JsValue::from_str(kind))?;
        match kind {
            "append" => {
                path(field(item, "path")?)?;
                let start = u64_string(field(item, "start")?)?;
                let end = u64_string(field(item, "end")?)?;
                let tail = u64_string(field(item, "tail")?)?;
                if start > end || end > tail {
                    return Err("invalid append positions");
                }
                set(&result, "path", &string_js(field(item, "path")?)?)?;
                set(&result, "start", &bigint_js(field(item, "start")?)?)?;
                set(&result, "end", &bigint_js(field(item, "end")?)?)?;
                set(&result, "tail", &bigint_js(field(item, "tail")?)?)?;
                set(
                    &result,
                    "records",
                    &array_js(field(item, "records")?, record_js)?,
                )?;
            }
            "fork" => {
                path(field(item, "source")?)?;
                path(field(item, "destination")?)?;
                let forked_at = u64_string(field(item, "forkedAt")?)?;
                let tail = u64_string(field(item, "tail")?)?;
                if forked_at > tail {
                    return Err("invalid fork positions");
                }
                set(&result, "source", &string_js(field(item, "source")?)?)?;
                set(
                    &result,
                    "destination",
                    &string_js(field(item, "destination")?)?,
                )?;
                set(&result, "forkedAt", &bigint_js(field(item, "forkedAt")?)?)?;
                set(&result, "tail", &bigint_js(field(item, "tail")?)?)?;
                if item.get("records").is_some() {
                    set(
                        &result,
                        "records",
                        &array_js(field(item, "records")?, record_js)?,
                    )?;
                }
            }
            "trim" => {
                path(field(item, "path")?)?;
                u64_string(field(item, "trimPoint")?)?;
                set(&result, "path", &string_js(field(item, "path")?)?)?;
                set(&result, "trimPoint", &bigint_js(field(item, "trimPoint")?)?)?;
            }
            "delete" => {
                path(field(item, "path")?)?;
                set(&result, "path", &string_js(field(item, "path")?)?)?;
            }
            _ => return Err("invalid mutation type"),
        }
        Ok(result.into())
    }

    fn envelope_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        id(field(item, "commitId")?)?;
        let result = Object::new();
        set(&result, "commitId", &bytes_js(field(item, "commitId")?)?)?;
        set(
            &result,
            "mutations",
            &array_js(field(item, "mutations")?, mutation_js)?,
        )?;
        Ok(result.into())
    }

    fn idempotency_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        key(field(item, "idempotencyKey")?)?;
        if bytes(field(item, "requestDigest")?)?.len() != 32 {
            return Err("requestDigest must contain 32 bytes");
        }
        let result = Object::new();
        set(
            &result,
            "idempotencyKey",
            &bytes_js(field(item, "idempotencyKey")?)?,
        )?;
        set(
            &result,
            "requestDigest",
            &bytes_js(field(item, "requestDigest")?)?,
        )?;
        let outcome = object(field(item, "outcome")?)?;
        let outcome_result = Object::new();
        let kind = string(field(outcome, "type")?)?;
        set(&outcome_result, "type", &JsValue::from_str(kind))?;
        match kind {
            "append" => set(
                &outcome_result,
                "outcome",
                &append_result_js(field(outcome, "outcome")?)?,
            )?,
            "fork" => set(
                &outcome_result,
                "receipt",
                &fork_receipt_js(field(outcome, "receipt")?)?,
            )?,
            "trim" => set(
                &outcome_result,
                "receipt",
                &trim_receipt_js(field(outcome, "receipt")?)?,
            )?,
            "delete" => set(
                &outcome_result,
                "receipt",
                &delete_receipt_js(field(outcome, "receipt")?)?,
            )?,
            "commit" => set(
                &outcome_result,
                "outcome",
                &commit_result_js(field(outcome, "outcome")?)?,
            )?,
            _ => return Err("invalid idempotency outcome type"),
        }
        set(&result, "outcome", &outcome_result.into())?;
        Ok(result.into())
    }

    fn token_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        let result = Object::new();
        set(&result, "token", &string_js(field(item, "token")?)?)?;
        let timestamp = string(field(item, "expiresAt")?)?;
        let date = Date::new(&JsValue::from_str(timestamp));
        if date.get_time().is_nan() {
            return Err("expiresAt must be an ISO timestamp");
        }
        set(&result, "expiresAt", &date.into())?;
        Ok(result.into())
    }

    pub(super) fn decode(route: &str, value: &Value) -> Result<JsValue> {
        crate::http_validation::validate(route, value)?;
        match crate::HTTP_RESPONSE_CONTRACT
            .iter()
            .find_map(|(candidate, kind)| (*candidate == route).then_some(*kind))
        {
            Some("sequence") => bigint_js(value),
            Some("append") => append_result_js(value),
            Some("fork") => fork_receipt_js(value),
            Some("trim") => trim_receipt_js(value),
            Some("delete") => delete_receipt_js(value),
            Some("records") => array_js(value, record_js),
            Some("children") => array_js(value, |value| {
                let item = object(value)?;
                path(field(item, "path")?)?;
                let result = Object::new();
                set(&result, "path", &string_js(field(item, "path")?)?)?;
                Ok(result.into())
            }),
            Some("commit") => commit_result_js(value),
            Some("envelope") => envelope_js(value),
            Some("observation") => {
                if value.is_null() {
                    Ok(JsValue::NULL)
                } else {
                    idempotency_js(value)
                }
            }
            Some("token") => token_js(value),
            _ => Err("unknown HTTP route"),
        }
    }

    pub(super) fn validate(route: &str, value: &Value) -> Result {
        crate::http_validation::validate(route, value)
    }
    fn encode_base64(value: &[u8]) -> String {
        const TABLE: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let table_char = |index: u8| TABLE.get(index as usize).copied().unwrap_or_default() as char;
        let mut output = String::with_capacity(value.len().div_ceil(3) * 4);
        for chunk in value.chunks(3) {
            let Some(&first) = chunk.first() else {
                continue;
            };
            output.push(table_char(first >> 2));
            if chunk.len() == 1 {
                output.push(table_char((first & 0x03) << 4));
                output.push_str("==");
                continue;
            }
            let Some(&second) = chunk.get(1) else {
                continue;
            };
            output.push(table_char(((first & 0x03) << 4) | (second >> 4)));
            if chunk.len() == 2 {
                output.push(table_char((second & 0x0f) << 2));
                output.push('=');
                continue;
            }
            let Some(&third) = chunk.get(2) else {
                continue;
            };
            output.push(table_char(((second & 0x0f) << 2) | (third >> 6)));
            output.push(table_char(third & 0x3f));
        }
        output
    }
    fn decode_base64(value: &str) -> Option<Vec<u8>> {
        if !value.len().is_multiple_of(4) {
            return None;
        }
        let mut output = Vec::with_capacity(value.len() / 4 * 3);
        let bytes = value.as_bytes();
        for (index, chunk) in bytes.chunks_exact(4).enumerate() {
            let &[a_byte, b_byte, c_byte, d_byte] = chunk else {
                return None;
            };
            let a = sextet(a_byte)?;
            let b = sextet(b_byte)?;
            let c = if c_byte == b'=' { 0 } else { sextet(c_byte)? };
            let d = if d_byte == b'=' { 0 } else { sextet(d_byte)? };
            if c_byte == b'=' {
                if d_byte != b'=' || b & 0x0f != 0 {
                    return None;
                }
            } else if d_byte == b'=' && (c & 0x03 != 0 || index + 1 != bytes.len() / 4) {
                return None;
            }
            output.push(a << 2 | b >> 4);
            if c_byte != b'=' {
                output.push(b << 4 | c >> 2);
            }
            if d_byte != b'=' {
                output.push(c << 6 | d);
            }
        }
        Some(output)
    }
    fn sextet(value: u8) -> Option<u8> {
        match value {
            b'A'..=b'Z' => Some(value - b'A'),
            b'a'..=b'z' => Some(value - b'a' + 26),
            b'0'..=b'9' => Some(value - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::{CommitCondition, CommitMutation, CommitRequest, IdempotencyKey, StreamPath};

    #[test]
    fn stream_error_codes_are_closed_and_stable() {
        for code in StreamErrorCode::ALL {
            assert!(is_stream_error_code(code.as_str()));
        }
        assert!(!is_stream_error_code("stream_not_found"));
        assert!(!is_stream_error_code("unknown"));
        assert_eq!(error_code(&StreamError::Retired).as_str(), "retired");
    }

    #[test]
    fn hosted_http_errors_use_the_public_route_aware_vocabulary() {
        assert_eq!(
            public_http_error_code("not_found", "tail").as_deref(),
            Some("stream_not_found")
        );
        assert_eq!(
            public_http_error_code("not_found", "commits/read").as_deref(),
            Some("commit_not_found")
        );
        assert_eq!(
            public_http_error_code("out_of_range", "read").as_deref(),
            Some("cursor_trimmed")
        );
        assert_eq!(
            public_http_error_code("capacity", "append").as_deref(),
            Some("capacity_exhausted")
        );
        assert_eq!(
            public_http_error_code("stream_not_found", "read").as_deref(),
            Some("stream_not_found")
        );
        assert_eq!(public_http_error_code("commit_not_found", "read"), None);
        assert_eq!(public_http_error_code("unknown", "read"), None);
    }

    #[test]
    fn append_validation_uses_domain_limits() {
        let request = wire::AppendRequest {
            path: "events".to_owned(),
            records: vec![Bytes::from_static(b"value")],
            if_tail: None,
            idempotency_key: None,
        };
        assert_eq!(validate_append_request(&request.encode_to_vec()), "");

        let invalid = wire::AppendRequest {
            path: "/events".to_owned(),
            ..request
        };
        assert_eq!(
            validate_append_request(&invalid.encode_to_vec()),
            "invalid_path"
        );
    }

    #[test]
    fn path_validation_uses_the_canonical_stream_parser() {
        assert_eq!(validate_path("events/run_42"), "");
        assert_eq!(validate_path("/events"), "invalid_path");
        assert_eq!(validate_path("events//run_42"), "invalid_path");
        let mut too_many = "a".to_owned();
        for _ in 0..crate::MAX_ITEMS {
            too_many.push_str("/a");
        }
        assert_eq!(validate_path(&too_many), "limit_exceeded");
    }

    #[test]
    fn sequence_validation_uses_the_canonical_unsigned_width() {
        assert_eq!(validate_sequence("0"), "");
        assert_eq!(validate_sequence("18446744073709551615"), "");
        assert_eq!(
            validate_sequence("18446744073709551616"),
            "invalid_argument"
        );
        assert_eq!(validate_sequence("-1"), "invalid_argument");
    }

    #[test]
    fn commit_normalization_is_canonical_and_shape_checked() -> Result<(), StreamError> {
        let path = StreamPath::new("events")?;
        let key = IdempotencyKey::new(Bytes::from_static(b"request"))?;
        let request = CommitRequest {
            conditions: vec![CommitCondition::Absent { path: path.clone() }],
            mutations: vec![CommitMutation::Append {
                path,
                records: vec![Bytes::from_static(b"value")],
            }],
            idempotency_key: key,
        };
        let wire = wire_codec::commit_to_wire(&request);
        let normalized = normalize_commit_bytes(&wire.encode_to_vec())
            .map_err(|_| StreamError::InvalidArgument)?;
        assert_eq!(normalized, wire.encode_to_vec());

        let invalid = wire::CommitRequest {
            conditions: Vec::new(),
            ..wire
        };
        assert_eq!(
            normalize_commit_bytes(&invalid.encode_to_vec()),
            Err("limit_exceeded")
        );
        Ok(())
    }

    #[test]
    fn request_validation_uses_wire_decoding_and_domain_limits() {
        let valid = wire::ReadRequest {
            path: "events".to_owned(),
            from: 0,
            limit: 10,
        };
        assert_eq!(validate_request("read", &valid.encode_to_vec()), "");

        let invalid_path = wire::TailRequest {
            path: "/events".to_owned(),
        };
        assert_eq!(
            validate_request("tail", &invalid_path.encode_to_vec()),
            "invalid_path"
        );

        let invalid_limit = wire::ChildrenRequest {
            parent: Some("events".to_owned()),
            limit: 0,
        };
        assert_eq!(
            validate_request("children", &invalid_limit.encode_to_vec()),
            "limit_exceeded"
        );
        assert_eq!(validate_request("unknown", &[]), "invalid_argument");
    }

    #[test]
    fn fork_validation_rejects_same_paths_and_oversized_payloads() {
        let same = wire::ForkRequest {
            source: "events".to_owned(),
            destination: "events".to_owned(),
            at_tail: None,
            idempotency_key: None,
        };
        assert_eq!(
            validate_request("fork", &same.encode_to_vec()),
            "invalid_argument"
        );

        let oversized = wire::ForkRequest {
            source: "a".repeat(crate::MAX_PATH_BYTES + 1),
            destination: "b".to_owned(),
            at_tail: None,
            idempotency_key: None,
        };
        assert_eq!(
            validate_request("fork", &oversized.encode_to_vec()),
            "invalid_path"
        );
    }
}
