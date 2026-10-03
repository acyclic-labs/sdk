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
    const ALL: [Self; 14] = [
        Self::InvalidPath,
        Self::InvalidArgument,
        Self::LimitExceeded,
        Self::NotFound,
        Self::AlreadyExists,
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
            StreamErrorCode::PrefixNotRetained => "prefix_not_retained",
            StreamErrorCode::OutOfRange => "out_of_range",
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
                wire::TailResponse { tail }.encode_to_vec()
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
                        committed_at_micros: record.committed_at_micros,
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
                            committed_at_micros: record.committed_at_micros,
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

/// Validates the endpoint policy shared by native and browser HTTP clients.
/// HTTPS is required for hosted endpoints; HTTP is allowed only for loopback
/// fixture servers. The return value is empty for a valid endpoint.
#[wasm_bindgen(js_name = validateHttpEndpoint)]
pub fn validate_http_endpoint(endpoint: &str) -> String {
    crate::http_validation::validate_endpoint(endpoint)
        .map_or_else(str::to_owned, |_| String::new())
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
            "children_page" => wire::ChildrenPageRequest::decode(input)
                .map_err(|_| StreamError::InvalidArgument)
                .and_then(wire_codec::children_page_from_wire)
                .and_then(|request| memory::validate_children_page_request(&request)),
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

/// Validate a hosted read page against the request cursor captured by the
/// caller. Rust owns record shape and cursor contiguity; the HTTP adapter only
/// supplies the response text and its request-relative starting position.
#[wasm_bindgen(js_name = validateHttpReadResponse)]
pub fn validate_http_read_response(response_json: &str, from: u64) -> Result<(), JsValue> {
    let value: Value = serde_json::from_str(response_json)
        .map_err(|error| JsValue::from_str(&format!("invalid JSON: {error}")))?;
    crate::http_validation::validate_read_from(&value, from).map_err(JsValue::from_str)
}

/// Validates one hosted read page and returns its canonical follow cursor.
#[wasm_bindgen(js_name = nextHttpFollowCursor)]
pub fn next_http_follow_cursor(response_json: &str, from: u64) -> Result<u64, JsValue> {
    let value: Value = serde_json::from_str(response_json)
        .map_err(|error| JsValue::from_str(&format!("invalid JSON: {error}")))?;
    crate::http_validation::next_follow_cursor(&value, from).map_err(JsValue::from_str)
}

/// Validates request-relative child-page semantics through the canonical Rust
/// provider rules before a public page reaches a TypeScript caller.
#[wasm_bindgen(js_name = validateChildrenPageResponse)]
pub fn validate_children_page_response(request: &[u8], response: &[u8]) -> Result<(), JsValue> {
    let request = wire::ChildrenPageRequest::decode(request)
        .map_err(|_| js_error(StreamError::InvalidArgument))?;
    let response = wire::ChildrenPageResponse::decode(response)
        .map_err(|_| js_error(StreamError::InvalidArgument))?;
    let request = wire_codec::children_page_from_wire(request).map_err(js_error)?;
    let hierarchy_version: [u8; 32] = response
        .hierarchy_version
        .as_ref()
        .try_into()
        .map_err(|_| js_error(StreamError::InvalidArgument))?;
    let page = crate::ChildrenPage {
        hierarchy_version: crate::CommitId::from_bytes(hierarchy_version),
        children: response
            .children
            .into_iter()
            .map(|child| {
                Ok(crate::Child {
                    path: crate::StreamPath::new(child.path)
                        .map_err(|_| StreamError::InvalidPath)?,
                })
            })
            .collect::<Result<_, StreamError>>()
            .map_err(js_error)?,
        next_after: response
            .next_after
            .map(|path| crate::StreamPath::new(path).map_err(|_| StreamError::InvalidPath))
            .transpose()
            .map_err(js_error)?,
    };
    memory::validate_children_page_response(&request, &page).map_err(js_error)
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

    pub(super) fn encode(route: &str, input: &[u8]) -> Result<String> {
        crate::http_codec::encode(route, input)
    }
    pub(super) fn decode_wire_response(operation: &str, input: &[u8]) -> Result<JsValue> {
        let route = match operation {
            "inspect_idempotency" => "idempotency/inspect",
            "read_commit" => "commits/read",
            "children_page" => "children/page",
            "append" | "fork" | "commit" => operation,
            _ => return Err("invalid_argument"),
        };
        match crate::http_response::value(route, input)? {
            Some(value) => decode(route, &value),
            None => Ok(JsValue::UNDEFINED),
        }
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
        set(
            &result,
            "committedAtMicros",
            &bigint_js(field(item, "committedAtMicros")?)?,
        )?;
        Ok(result.into())
    }

    fn children_page_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        id(field(item, "hierarchyVersion")?)?;
        let result = Object::new();
        set(
            &result,
            "hierarchyVersion",
            &bytes_js(field(item, "hierarchyVersion")?)?,
        )?;
        set(
            &result,
            "children",
            &array_js(field(item, "children")?, |value| {
                let child = object(value)?;
                path(field(child, "path")?)?;
                let result = Object::new();
                set(&result, "path", &string_js(field(child, "path")?)?)?;
                Ok(result.into())
            })?,
        )?;
        if let Some(next_after) = item.get("nextAfter").filter(|value| !value.is_null()) {
            set(&result, "nextAfter", &string_js(next_after)?)?;
        }
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

    fn conflict_js(value: &Value) -> Result<JsValue> {
        let item = object(value)?;
        path(field(item, "path")?)?;
        let result = Object::new();
        set(&result, "path", &string_js(field(item, "path")?)?)?;
        if item.get("expectedAbsent") == Some(&Value::Bool(true)) {
            match string(field(item, "actual")?)? {
                "exists" => {}
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
            Some("records") => array_js(value, record_js),
            Some("children") => array_js(value, |value| {
                let item = object(value)?;
                path(field(item, "path")?)?;
                let result = Object::new();
                set(&result, "path", &string_js(field(item, "path")?)?)?;
                Ok(result.into())
            }),
            Some("children_page") => children_page_js(value),
            Some("commit") => commit_result_js(value),
            Some("envelope") => envelope_js(value),
            Some("observation") => {
                if value.is_null() {
                    Ok(JsValue::UNDEFINED)
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
    fn decode_base64(value: &str) -> Option<Vec<u8>> {
        if !value.len().is_multiple_of(4) {
            return None;
        }
        let mut output = Vec::with_capacity(value.len() / 4 * 3);
        let bytes = value.as_bytes();
        for (index, chunk) in bytes.as_chunks::<4>().0.iter().enumerate() {
            let &[a_byte, b_byte, c_byte, d_byte] = chunk;
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
            Some("out_of_range")
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
    fn endpoint_validation_matches_the_hosted_transport_policy() {
        for endpoint in [
            "https://stream.example",
            "http://localhost:3000/fixture",
            "http://127.0.0.1:3000/fixture",
            "http://[::1]:3000/fixture",
        ] {
            assert_eq!(validate_http_endpoint(endpoint), "", "{endpoint}");
        }
        for endpoint in [
            "http://stream.example",
            "https://user@stream.example",
            "https://stream.example/?query=1",
            "https://stream.example/#fragment",
            "https://stream.example:not-a-port",
        ] {
            assert_eq!(
                validate_http_endpoint(endpoint),
                "invalid_endpoint",
                "{endpoint}"
            );
        }
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
