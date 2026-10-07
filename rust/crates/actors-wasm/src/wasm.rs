//! Browser ABI for the Rust-owned Actors client facade.
//!
//! Transport construction, request validation, typed operations, and service
//! error decoding live in `acyclic_actors::client`. This module only owns the
//! JavaScript byte boundary, cancellation, and JS error projection.

use std::{future::Future, rc::Rc};

use acyclic_actors::{ContractError, MAX_BINDINGS, MAX_SUBSCRIPTIONS, client, wire};
use prost::Message;
use tokio_util::sync::CancellationToken;
use wasm_bindgen::{JsValue, prelude::*};

/// JavaScript names and byte-level aliases are emitted from this Rust boundary.
#[wasm_bindgen(typescript_custom_section)]
const ACTORS_TYPES: &'static str = r#"
import type { ActorId, CodeSha256, PositiveU64 } from "@acyclic-labs/actors/types";

export type ActorsWireBytes = Uint8Array;

export interface ActorsError extends Error {
  readonly code: string;
  readonly grpcCode?: number;
  readonly grpcName?: string;
  readonly serviceCode?: number;
  readonly serviceMessage?: string;
  /** Stable Rust-owned semantic conversion category. */
  readonly semanticCode?: string;
  /** Raw enum value preserved when the semantic category carries one. */
  readonly semanticValue?: number;
  /** Contract admission category nested inside a semantic conversion error. */
  readonly contractCode?: string;
}

export function validateCreateActor(request: ActorsWireBytes): void;
export function validateUpdateActor(request: ActorsWireBytes): void;
export function validateAddSubscription(request: ActorsWireBytes): void;
export function PositiveU64(value: bigint): PositiveU64;
"#;

fn js_error(code: &str, message: impl AsRef<str>) -> JsValue {
    let error = js_sys::Error::new(message.as_ref());
    let _ = js_sys::Reflect::set(&error, &JsValue::from_str("code"), &JsValue::from_str(code));
    error.into()
}

fn invalid(message: impl AsRef<str>) -> JsValue {
    js_error("invalid_argument", message)
}

fn decode<M: Message + Default>(bytes: &[u8]) -> Result<M, JsValue> {
    if bytes.len() > crate::MAX_MESSAGE_BYTES {
        return Err(js_error(
            "message_too_large",
            "Actors message exceeds the configured bound",
        ));
    }
    M::decode(bytes).map_err(|_| invalid("malformed Actors protobuf request"))
}

fn encoded<M: Message>(message: &M) -> Result<JsValue, JsValue> {
    let bytes = message.encode_to_vec();
    if bytes.len() > crate::MAX_MESSAGE_BYTES {
        return Err(js_error(
            "message_too_large",
            "Actors message exceeds the configured bound",
        ));
    }
    Ok(js_sys::Uint8Array::from(bytes.as_slice()).into())
}

fn contract_error(error: ContractError) -> JsValue {
    match error {
        ContractError::InvalidArgument => invalid(error.to_string()),
        ContractError::LimitExceeded => js_error("limit_exceeded", error.to_string()),
        ContractError::DuplicateName => js_error("duplicate_name", error.to_string()),
    }
}

fn validate_create(request: &wire::CreateActorRequest) -> Result<(), JsValue> {
    acyclic_actors::validate_create(request).map_err(contract_error)
}

fn validate_update(request: &wire::UpdateActorRequest) -> Result<(), JsValue> {
    acyclic_actors::validate_update(request).map_err(contract_error)
}

fn validate_add(request: &wire::AddSubscriptionRequest) -> Result<(), JsValue> {
    acyclic_actors::validate_add_subscription(request).map_err(contract_error)
}

/// Constructs the Rust-owned nominal Actor identity while preserving its
/// ergonomic string representation in generated TypeScript.
#[wasm_bindgen(
    js_name = ActorId,
    unchecked_return_type = "ActorId"
)]
/// WebAssembly binding fn for actor_id.
pub fn actor_id(
    #[wasm_bindgen(unchecked_param_type = "string")] value: String,
) -> Result<String, JsValue> {
    acyclic_actors::domain::ActorId::new(value)
        .map(|value| value.as_str().to_owned())
        .map_err(semantic_error)
}

/// Constructs the Rust-owned nominal SHA-256 digest while preserving its
/// ergonomic `Uint8Array` representation in generated TypeScript.
#[wasm_bindgen(
    js_name = CodeSha256,
    unchecked_return_type = "CodeSha256"
)]
/// WebAssembly binding fn for code_sha256.
pub fn code_sha256(
    #[wasm_bindgen(unchecked_param_type = "Uint8Array")] value: JsValue,
) -> Result<JsValue, JsValue> {
    let value = js_sys::Uint8Array::new(&value).to_vec();
    acyclic_actors::domain::CodeSha256::new(value)
        .map(|value| js_sys::Uint8Array::from(value.as_bytes().as_slice()).into())
        .map_err(semantic_error)
}

/// Constructs the Rust-owned strictly positive `u64` semantic value without
/// converting through a JavaScript `number`.
#[wasm_bindgen(
    js_name = PositiveU64,
    unchecked_return_type = "PositiveU64"
)]
/// WebAssembly binding fn for positive_u64.
pub fn positive_u64(
    #[wasm_bindgen(unchecked_param_type = "bigint")] value: JsValue,
) -> Result<JsValue, JsValue> {
    if !value.is_bigint() {
        return Err(invalid("PositiveU64 requires a bigint"));
    }
    let value = value.unchecked_ref::<js_sys::BigInt>();
    let spelling = JsValue::from(value.to_string(10).map_err(JsValue::from)?)
        .as_string()
        .ok_or_else(|| invalid("PositiveU64 bigint spelling is invalid"))?;
    let raw = spelling
        .parse::<u64>()
        .map_err(|_| invalid("PositiveU64 must be an unsigned 64-bit bigint"))?;
    acyclic_actors::domain::PositiveU64::new(raw)
        .map(|value| JsValue::from(js_sys::BigInt::from(value.get())))
        .map_err(semantic_error)
}

fn grpc_code(code: i32) -> &'static str {
    match code {
        0 => "ok",
        1 => "cancelled",
        2 => "unknown",
        3 => "invalid_argument",
        4 => "deadline_exceeded",
        5 => "not_found",
        6 => "already_exists",
        7 => "permission_denied",
        8 => "resource_exhausted",
        9 => "failed_precondition",
        10 => "aborted",
        11 => "out_of_range",
        12 => "unimplemented",
        13 => "internal",
        14 => "unavailable",
        15 => "data_loss",
        16 => "unauthenticated",
        _ => "unknown",
    }
}

fn semantic_error(error: acyclic_actors::domain::DomainError) -> JsValue {
    let message = error.to_string();
    let (semantic_code, semantic_value, contract_code, code) = match error {
        acyclic_actors::domain::DomainError::EmptyActorId => {
            ("empty_actor_id", None, None, "semantic_error")
        }
        acyclic_actors::domain::DomainError::InvalidCodeSha256 => {
            ("invalid_code_sha256", None, None, "semantic_error")
        }
        acyclic_actors::domain::DomainError::Contract(contract) => {
            let contract_code = match contract {
                ContractError::InvalidArgument => "invalid_argument",
                ContractError::LimitExceeded => "limit_exceeded",
                ContractError::DuplicateName => "duplicate_name",
            };
            ("contract", None, Some(contract_code), contract_code)
        }
        acyclic_actors::domain::DomainError::UnknownActorState(value) => {
            ("unknown_actor_state", Some(value), None, "semantic_error")
        }
        acyclic_actors::domain::DomainError::UnknownSubscriptionState(value) => (
            "unknown_subscription_state",
            Some(value),
            None,
            "semantic_error",
        ),
        acyclic_actors::domain::DomainError::UnknownErrorCode(value) => {
            ("unknown_error_code", Some(value), None, "semantic_error")
        }
        acyclic_actors::domain::DomainError::MissingMessage => {
            ("missing_message", None, None, "semantic_error")
        }
        acyclic_actors::domain::DomainError::InvalidSubscription => {
            ("invalid_subscription", None, None, "semantic_error")
        }
        acyclic_actors::domain::DomainError::InvalidBinding => {
            ("invalid_binding", None, None, "semantic_error")
        }
    };
    let error = js_error(code, message);
    let _ = js_sys::Reflect::set(
        &error,
        &JsValue::from_str("semanticCode"),
        &JsValue::from_str(semantic_code),
    );
    if let Some(value) = semantic_value {
        let _ = js_sys::Reflect::set(
            &error,
            &JsValue::from_str("semanticValue"),
            &JsValue::from_f64(f64::from(value)),
        );
    }
    if let Some(contract_code) = contract_code {
        let _ = js_sys::Reflect::set(
            &error,
            &JsValue::from_str("contractCode"),
            &JsValue::from_str(contract_code),
        );
    }
    error
}

fn facade_error(error: client::Error) -> JsValue {
    match error {
        client::Error::Configuration(message) => invalid(message),
        client::Error::Transport(message) => js_error("unavailable", message),
        client::Error::Contract(error) => contract_error(error),
        client::Error::Semantic(error) => semantic_error(error),
        client::Error::Service {
            grpc_code: code,
            detail,
        } => {
            let error = js_error(grpc_code(code), "Actors service request failed");
            let _ = js_sys::Reflect::set(
                &error,
                &JsValue::from_str("grpcCode"),
                &JsValue::from_f64(f64::from(code)),
            );
            let _ = js_sys::Reflect::set(
                &error,
                &JsValue::from_str("grpcName"),
                &JsValue::from_str(grpc_code(code)),
            );
            if let Some(detail) = detail {
                let _ = js_sys::Reflect::set(
                    &error,
                    &JsValue::from_str("serviceCode"),
                    &JsValue::from_f64(f64::from(detail.code)),
                );
                let _ = js_sys::Reflect::set(
                    &error,
                    &JsValue::from_str("serviceMessage"),
                    &JsValue::from_str(&detail.message),
                );
            }
            error
        }
        client::Error::Cancelled => {
            js_error("cancelled", "Actors operation cancelled by the caller")
        }
    }
}

/// Thin JavaScript handle over the canonical Rust cancellation token.
#[wasm_bindgen]
pub struct CancellationHandle {
    token: CancellationToken,
}

#[wasm_bindgen]
impl CancellationHandle {
    #[wasm_bindgen(constructor)]
    /// WebAssembly binding fn for new.
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    /// WebAssembly binding fn for cancel.
    pub fn cancel(&self) {
        self.token.cancel();
    }

    /// WebAssembly binding fn for cancelled.
    pub fn cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
}

impl CancellationHandle {
    fn token(&self) -> CancellationToken {
        self.token.clone()
    }
}

async fn await_operation<F, T>(
    future: F,
    cancellation: Option<&CancellationHandle>,
) -> Result<T, JsValue>
where
    F: Future<Output = Result<T, client::Error>>,
{
    let token = cancellation.map(CancellationHandle::token);
    client::run_with_cancellation(future, token)
        .await
        .map_err(facade_error)
}

async fn encode_domain_operation<T, W, F>(
    future: F,
    cancellation: Option<&CancellationHandle>,
) -> Result<JsValue, JsValue>
where
    W: Message + From<T>,
    F: Future<Output = Result<T, client::Error>>,
{
    let value = await_operation(future, cancellation).await?;
    encoded(&W::from(value))
}

fn request_bytes(value: JsValue) -> Result<Vec<u8>, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Err(invalid("request bytes are required"));
    }
    let bytes = js_sys::Uint8Array::new(&value).to_vec();
    if bytes.is_empty() {
        return Err(invalid("request bytes are required"));
    }
    Ok(bytes)
}

/// Browser Actors client backed by the canonical Rust facade.
#[wasm_bindgen]
pub struct ActorsClient {
    inner: Rc<client::Client>,
}

#[wasm_bindgen]
impl ActorsClient {
    /// Creates a browser client. `Client::from_browser` is synchronous because
    /// tonic-web transport construction does not perform network I/O.
    #[wasm_bindgen(constructor)]
    pub fn new(endpoint: String, token: String) -> Result<Self, JsValue> {
        let inner = client::Client::from_browser(&endpoint, &token).map_err(facade_error)?;
        Ok(Self {
            inner: Rc::new(inner),
        })
    }

    #[wasm_bindgen(js_name = validateCreateActor)]
    /// WebAssembly binding fn for validate_create_actor.
    pub fn validate_create_actor(request: JsValue) -> Result<(), JsValue> {
        validate_create(&decode(&request_bytes(request)?)?)
    }

    #[wasm_bindgen(js_name = validateUpdateActor)]
    /// WebAssembly binding fn for validate_update_actor.
    pub fn validate_update_actor(request: JsValue) -> Result<(), JsValue> {
        validate_update(&decode(&request_bytes(request)?)?)
    }

    #[wasm_bindgen(js_name = validateAddSubscription)]
    /// WebAssembly binding fn for validate_add_subscription.
    pub fn validate_add_subscription(request: JsValue) -> Result<(), JsValue> {
        validate_add(&decode(&request_bytes(request)?)?)
    }

    #[wasm_bindgen(js_name = createActor, unchecked_return_type = "ActorsWireBytes")]
    pub async fn create_actor(
        &self,
        #[wasm_bindgen(unchecked_param_type = "ActorsWireBytes")] request: JsValue,
        #[wasm_bindgen(unchecked_param_type = "CancellationHandle")]
        cancellation: &CancellationHandle,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::CreateActorRequest>(&request_bytes(request)?)?;
        let request = acyclic_actors::domain::CreateActorRequest::try_from(request)
            .map_err(client::Error::Semantic)
            .map_err(facade_error)?;
        encode_domain_operation::<_, wire::CreateActorResponse, _>(
            self.inner.create_actor(&request),
            Some(cancellation),
        )
        .await
    }

    #[wasm_bindgen(js_name = updateActor, unchecked_return_type = "ActorsWireBytes")]
    pub async fn update_actor(
        &self,
        #[wasm_bindgen(unchecked_param_type = "ActorsWireBytes")] request: JsValue,
        #[wasm_bindgen(unchecked_param_type = "CancellationHandle")]
        cancellation: &CancellationHandle,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::UpdateActorRequest>(&request_bytes(request)?)?;
        let request = acyclic_actors::domain::UpdateActorRequest::try_from(request)
            .map_err(client::Error::Semantic)
            .map_err(facade_error)?;
        encode_domain_operation::<_, wire::UpdateActorResponse, _>(
            self.inner.update_actor(&request),
            Some(cancellation),
        )
        .await
    }

    #[wasm_bindgen(js_name = inspectActor, unchecked_return_type = "ActorsWireBytes")]
    pub async fn inspect_actor(
        &self,
        #[wasm_bindgen(unchecked_param_type = "ActorsWireBytes")] request: JsValue,
        #[wasm_bindgen(unchecked_param_type = "CancellationHandle")]
        cancellation: &CancellationHandle,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::InspectActorRequest>(&request_bytes(request)?)?;
        let request = acyclic_actors::domain::InspectActorRequest::try_from(request)
            .map_err(client::Error::Semantic)
            .map_err(facade_error)?;
        encode_domain_operation::<_, wire::InspectActorResponse, _>(
            self.inner.inspect_actor(&request),
            Some(cancellation),
        )
        .await
    }

    #[wasm_bindgen(js_name = addSubscription, unchecked_return_type = "ActorsWireBytes")]
    pub async fn add_subscription(
        &self,
        #[wasm_bindgen(unchecked_param_type = "ActorsWireBytes")] request: JsValue,
        #[wasm_bindgen(unchecked_param_type = "CancellationHandle")]
        cancellation: &CancellationHandle,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::AddSubscriptionRequest>(&request_bytes(request)?)?;
        let request = acyclic_actors::domain::AddSubscriptionRequest::try_from(request)
            .map_err(client::Error::Semantic)
            .map_err(facade_error)?;
        encode_domain_operation::<_, wire::AddSubscriptionResponse, _>(
            self.inner.add_subscription(&request),
            Some(cancellation),
        )
        .await
    }

    #[wasm_bindgen(js_name = removeSubscription, unchecked_return_type = "ActorsWireBytes")]
    pub async fn remove_subscription(
        &self,
        #[wasm_bindgen(unchecked_param_type = "ActorsWireBytes")] request: JsValue,
        #[wasm_bindgen(unchecked_param_type = "CancellationHandle")]
        cancellation: &CancellationHandle,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::RemoveSubscriptionRequest>(&request_bytes(request)?)?;
        let request = acyclic_actors::domain::RemoveSubscriptionRequest::try_from(request)
            .map_err(client::Error::Semantic)
            .map_err(facade_error)?;
        encode_domain_operation::<_, wire::RemoveSubscriptionResponse, _>(
            self.inner.remove_subscription(&request),
            Some(cancellation),
        )
        .await
    }

    #[wasm_bindgen(js_name = resumeSubscription, unchecked_return_type = "ActorsWireBytes")]
    pub async fn resume_subscription(
        &self,
        #[wasm_bindgen(unchecked_param_type = "ActorsWireBytes")] request: JsValue,
        #[wasm_bindgen(unchecked_param_type = "CancellationHandle")]
        cancellation: &CancellationHandle,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::ResumeSubscriptionRequest>(&request_bytes(request)?)?;
        let request = acyclic_actors::domain::ResumeSubscriptionRequest::try_from(request)
            .map_err(client::Error::Semantic)
            .map_err(facade_error)?;
        encode_domain_operation::<_, wire::ResumeSubscriptionResponse, _>(
            self.inner.resume_subscription(&request),
            Some(cancellation),
        )
        .await
    }

    #[wasm_bindgen(js_name = checkpointActor, unchecked_return_type = "ActorsWireBytes")]
    pub async fn checkpoint_actor(
        &self,
        #[wasm_bindgen(unchecked_param_type = "ActorsWireBytes")] request: JsValue,
        #[wasm_bindgen(unchecked_param_type = "CancellationHandle")]
        cancellation: &CancellationHandle,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::CheckpointActorRequest>(&request_bytes(request)?)?;
        let request = acyclic_actors::domain::CheckpointActorRequest::try_from(request)
            .map_err(client::Error::Semantic)
            .map_err(facade_error)?;
        encode_domain_operation::<_, wire::CheckpointActorResponse, _>(
            self.inner.checkpoint_actor(&request),
            Some(cancellation),
        )
        .await
    }

    #[wasm_bindgen(js_name = invokeActor, unchecked_return_type = "ActorsWireBytes")]
    pub async fn invoke_actor(
        &self,
        #[wasm_bindgen(unchecked_param_type = "ActorsWireBytes")] request: JsValue,
        #[wasm_bindgen(unchecked_param_type = "CancellationHandle")]
        cancellation: &CancellationHandle,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::InvokeActorRequest>(&request_bytes(request)?)?;
        let request = acyclic_actors::domain::InvokeActorRequest::try_from(request)
            .map_err(client::Error::Semantic)
            .map_err(facade_error)?;
        encode_domain_operation::<_, wire::InvokeActorResponse, _>(
            self.inner.invoke_actor(&request),
            Some(cancellation),
        )
        .await
    }
}

#[allow(dead_code)]
const _CONTRACT_LIMITS: (usize, usize) = (MAX_SUBSCRIPTIONS, MAX_BINDINGS);
