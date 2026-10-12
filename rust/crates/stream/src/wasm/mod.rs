//! Browser boundary for canonical Stream request validation.

use futures::{
    FutureExt, StreamExt,
    future::{Either, select},
};
use js_sys::{Array, BigInt, Date, Object, Reflect, Uint8Array};
use prost::Message;
use serde_json::Value;
use std::sync::Mutex;
use tsify::Ts;
use wasm_bindgen::{JsCast, prelude::*};

use crate::{
    MAX_COMMAND_BYTES, MemoryStream, StreamError, StreamProvider, memory, wire, wire_codec,
};

#[allow(
    clippy::needless_pass_by_value,
    reason = "passed to map_err, which hands over the error by value"
)]
fn js_error(error: StreamError) -> JsValue {
    let value = js_sys::Error::new(&error.to_string());
    let _property_result = Reflect::set(
        value.as_ref(),
        &JsValue::from_str("code"),
        &JsValue::from_str(error.code()),
    );
    value.into()
}

/// Return whether a code can be emitted by this WASM adapter.
#[wasm_bindgen]
pub fn is_stream_error_code(value: &str) -> bool {
    StreamError::from_code(value).is_some()
}

/// Project a hosted HTTP error code onto the public Stream error vocabulary.
#[wasm_bindgen(js_name = publicHttpErrorCode)]
pub fn public_http_error_code(raw: &str, route: &str) -> Option<String> {
    let code = match raw {
        "stream_not_found" => "stream_not_found",
        "destination_exists" => "destination_exists",
        "capacity_exhausted" => "capacity_exhausted",
        "commit_not_found" => "commit_not_found",
        _ => match StreamError::from_code(raw)? {
            StreamError::NotFound if route == "commits/read" => "commit_not_found",
            StreamError::NotFound => "stream_not_found",
            StreamError::AlreadyExists => "destination_exists",
            StreamError::Capacity => "capacity_exhausted",
            other => other.code(),
        },
    };
    if code == "commit_not_found" && route != "commits/read" {
        return None;
    }
    Some(code.to_owned())
}

/// Type-only bridge for the complete Rust-owned Stream error-code contract.
#[wasm_bindgen(js_name = __streamErrorCodeContract)]
pub fn stream_error_code_contract(value: Ts<StreamError>) -> Ts<StreamError> {
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

/// Browser ABI over canonical Rust memory or durable providers.
#[wasm_bindgen]
pub struct WasmStream {
    provider: Box<dyn StreamProvider>,
}

async fn dispatch_commit(provider: &dyn StreamProvider, input: &[u8]) -> Result<Vec<u8>, JsValue> {
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
impl WasmStream {
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new() -> Self {
        Self {
            provider: Box::new(MemoryStream::default()),
        }
    }

    /// Opens the canonical Rust provider on one durable `IndexedDB` journal.
    #[wasm_bindgen(js_name = openBrowser)]
    pub async fn open_browser(
        name: String,
        maximum_commands: u32,
        maximum_journal_bytes: u64,
    ) -> Result<WasmStream, JsValue> {
        let limits = crate::BrowserStreamLimits {
            commands: u64::from(maximum_commands),
            journal_bytes: maximum_journal_bytes,
            ..Default::default()
        };
        Ok(Self {
            provider: Box::new(
                crate::BrowserStream::open(&name, limits)
                    .await
                    .map_err(js_error)?,
            ),
        })
    }

    /// Executes one finite unary operation over canonical protobuf bytes.
    #[wasm_bindgen]
    pub async fn dispatch(&self, operation: &str, input: &[u8]) -> Result<Vec<u8>, JsValue> {
        check_command_size(input)?;
        let output = match operation {
            "acknowledge_delivery" => {
                let request = decode::<wire::AcknowledgeDeliveryRequest>(input)?;
                if request.delivery_token.len() != 32 {
                    return Err(js_error(StreamError::InvalidArgument));
                }
                // Memory and IndexedDB are not hosted delivery authorities.
                return Err(js_error(StreamError::Unsupported));
            }
            "inspect_idempotency" => {
                let request = decode::<wire::InspectIdempotencyRequest>(input)?;
                let key = crate::IdempotencyKey::new(request.idempotency_key).map_err(js_error)?;
                let response = self
                    .provider
                    .inspect_idempotency(key)
                    .await
                    .map_err(js_error)?;
                match response {
                    Some(value) => wire::InspectIdempotencyResponse {
                        observation: Some(wire_codec::observation_wire(value)),
                    }
                    .encode_to_vec(),
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
            "commit" => dispatch_commit(self.provider.as_ref(), input).await?,
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
                wire_codec::read_response_wire(vec![wire_codec::record_wire(record)])
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
            stream: Mutex::new(Some(stream)),
            cancel,
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

mod follow;
mod projection;
mod validation;
use follow::WasmFollow;
#[cfg(test)]
mod tests;
