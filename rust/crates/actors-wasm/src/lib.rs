//! WebAssembly bridge for the Rust-owned Actors client.
//!
//! Only protobuf bytes cross this boundary. Endpoint, credential, message
//! size, contract, and semantic conversion policy remain in `acyclic-actors`.

#![cfg(target_arch = "wasm32")]

use acyclic_actors::{client, domain, wire};
use js_sys::{BigInt, Function, Reflect, Uint8Array};
use prost::Message;
use tokio_util::sync::CancellationToken;
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::prelude::*;

fn js_error(code: &str, message: impl std::fmt::Display) -> JsValue {
    let error = js_sys::Error::new(&message.to_string());
    error.set_name("ActorsError");
    let _ = js_sys::Reflect::set(&error, &JsValue::from_str("code"), &JsValue::from_str(code));
    error.into()
}

fn map_error(error: client::Error) -> JsValue {
    let code = error.code_name();
    js_error(code, error)
}

fn nominal_error(error: domain::DomainError) -> JsValue {
    let code = error.code_name();
    js_error(code, error)
}

fn nominal_string(value: &JsValue, field: &str) -> Result<String, JsValue> {
    value
        .as_string()
        .ok_or_else(|| js_error("invalid_argument", format!("{field} must be a string")))
}

/// Rust-backed nominal constructor for `ActorId`.
#[wasm_bindgen(js_name = "ActorId")]
pub fn actor_id(
    #[wasm_bindgen(unchecked_param_type = "string")] value: &JsValue,
) -> Result<String, JsValue> {
    let value = nominal_string(value, "actor_id")?;
    domain::ActorId::new(value)
        .map(|value| value.as_str().to_owned())
        .map_err(nominal_error)
}

/// Rust-backed nominal constructor for `CodeSha256`.
#[wasm_bindgen(js_name = "CodeSha256")]
pub fn code_sha256(
    #[wasm_bindgen(unchecked_param_type = "Uint8Array")] value: JsValue,
) -> Result<Vec<u8>, JsValue> {
    let value: Uint8Array = value
        .dyn_into()
        .map_err(|_| js_error("invalid_argument", "code_sha256 must be a Uint8Array"))?;
    domain::CodeSha256::new(value.to_vec())
        .map(|value| value.as_bytes().to_vec())
        .map_err(nominal_error)
}

/// Rust-backed nominal constructor for `PositiveU64`.
#[wasm_bindgen(js_name = "PositiveU64")]
pub fn positive_u64(value: BigInt) -> Result<u64, JsValue> {
    let value = u64::try_from(value)
        .map_err(|_| js_error("not_positive", "value must be a lossless positive u64"))?;
    domain::PositiveU64::new(value)
        .map(domain::PositiveU64::get)
        .map_err(nominal_error)
}

/// Rust-backed nominal constructor for the true-only current-head marker.
#[wasm_bindgen(js_name = "CurrentHeadMarker")]
pub fn current_head_marker(
    #[wasm_bindgen(unchecked_param_type = "boolean")] value: &JsValue,
) -> Result<bool, JsValue> {
    let value = value
        .as_bool()
        .ok_or_else(|| js_error("invalid_argument", "current_head_marker must be a boolean"))?;
    domain::subscription_start::CurrentHeadMarker::try_from(value)
        .map(bool::from)
        .map_err(nominal_error)
}

fn decode<T: Message + Default>(bytes: &[u8]) -> Result<T, JsValue> {
    T::decode(bytes).map_err(|error| js_error("invalid_argument", error))
}

fn decode_semantic<T, D>(bytes: &[u8]) -> Result<D, JsValue>
where
    T: Message + Default,
    D: TryFrom<T>,
    D::Error: std::fmt::Display,
{
    D::try_from(decode::<T>(bytes)?).map_err(|error| js_error("invalid_argument", error))
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "prost encoders consume the generated response value at the ABI boundary"
)]
fn encode<T: Message>(message: T) -> Vec<u8> {
    message.encode_to_vec()
}

struct AbortRegistration {
    signal: JsValue,
    callback: Closure<dyn FnMut(JsValue)>,
    token: CancellationToken,
}

impl AbortRegistration {
    fn new(signal: Option<JsValue>) -> Result<Option<Self>, JsValue> {
        let Some(signal) = signal.filter(|value| !value.is_undefined() && !value.is_null()) else {
            return Ok(None);
        };
        let token = CancellationToken::new();
        let callback_token = token.clone();
        let callback = Closure::wrap(Box::new(move |_event: JsValue| {
            callback_token.cancel();
        }) as Box<dyn FnMut(JsValue)>);
        let add = Reflect::get(&signal, &JsValue::from_str("addEventListener"))?
            .dyn_into::<Function>()?;
        add.call2(
            &signal,
            &JsValue::from_str("abort"),
            callback.as_ref().unchecked_ref(),
        )?;
        let aborted = Reflect::get(&signal, &JsValue::from_str("aborted"))?
            .as_bool()
            .unwrap_or(false);
        if aborted {
            token.cancel();
        }
        Ok(Some(Self {
            signal,
            callback,
            token,
        }))
    }

    fn token(&self) -> CancellationToken {
        self.token.clone()
    }
}

impl Drop for AbortRegistration {
    #[allow(
        clippy::useless_conversion,
        reason = "wasm-bindgen's dynamic cast error is already a JsValue"
    )]
    fn drop(&mut self) {
        if let Ok(remove) = Reflect::get(&self.signal, &JsValue::from_str("removeEventListener"))
            .and_then(|value| value.dyn_into::<Function>().map_err(Into::into))
        {
            let _ = remove.call2(
                &self.signal,
                &JsValue::from_str("abort"),
                self.callback.as_ref().unchecked_ref(),
            );
        }
    }
}

/// Connected browser Actors client. Transport and validation are implemented
/// by the shared Rust client; this object only provides the JS ABI.
#[wasm_bindgen]
pub struct ActorsClient {
    inner: client::Client,
}

#[wasm_bindgen]
impl ActorsClient {
    /// Connect to an HTTPS endpoint using the canonical browser gRPC-Web transport.
    #[wasm_bindgen]
    pub async fn connect(
        endpoint: String,
        token: String,
        signal: Option<JsValue>,
    ) -> Result<ActorsClient, JsValue> {
        let registration = AbortRegistration::new(signal)?;
        let cancellation = registration.as_ref().map(AbortRegistration::token);
        let result =
            client::run_with_cancellation(client::connect(&endpoint, &token), cancellation).await;
        drop(registration);
        result.map(|inner| Self { inner }).map_err(map_error)
    }

    async fn run<T, F>(&self, signal: Option<JsValue>, operation: F) -> Result<T, JsValue>
    where
        F: std::future::Future<Output = Result<T, client::Error>>,
    {
        let registration = AbortRegistration::new(signal)?;
        let cancellation = registration.as_ref().map(AbortRegistration::token);
        let result = client::run_with_cancellation(operation, cancellation).await;
        drop(registration);
        result.map_err(map_error)
    }

    /// Returns the transport selected by the Rust client.
    #[wasm_bindgen(getter)]
    pub fn transport(&self) -> String {
        self.inner.transport().to_owned()
    }

    /// Execute `CreateActor` with an encoded protobuf request.
    pub async fn create_actor(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        let request =
            decode_semantic::<wire::CreateActorRequest, domain::CreateActorRequest>(request)?;
        self.run(signal, self.inner.create_actor(&request))
            .await
            .map(|value| encode(wire::CreateActorResponse::from(value)))
    }

    /// Execute `UpdateActor` with an encoded protobuf request.
    pub async fn update_actor(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        let request =
            decode_semantic::<wire::UpdateActorRequest, domain::UpdateActorRequest>(request)?;
        self.run(signal, self.inner.update_actor(&request))
            .await
            .map(|value| encode(wire::UpdateActorResponse::from(value)))
    }

    /// Execute `InspectActor` with an encoded protobuf request.
    pub async fn inspect_actor(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        let request =
            decode_semantic::<wire::InspectActorRequest, domain::InspectActorRequest>(request)?;
        self.run(signal, self.inner.inspect_actor(&request))
            .await
            .map(|value| encode(wire::InspectActorResponse::from(value)))
    }

    /// Execute `AddSubscription` with an encoded protobuf request.
    pub async fn add_subscription(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        let request = decode_semantic::<
            wire::AddSubscriptionRequest,
            domain::AddSubscriptionRequest,
        >(request)?;
        self.run(signal, self.inner.add_subscription(&request))
            .await
            .map(|value| encode(wire::AddSubscriptionResponse::from(value)))
    }

    /// Execute `RemoveSubscription` with an encoded protobuf request.
    pub async fn remove_subscription(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        let request = decode_semantic::<
            wire::RemoveSubscriptionRequest,
            domain::RemoveSubscriptionRequest,
        >(request)?;
        self.run(signal, self.inner.remove_subscription(&request))
            .await
            .map(|value| encode(wire::RemoveSubscriptionResponse::from(value)))
    }

    /// Execute `ResumeSubscription` with an encoded protobuf request.
    pub async fn resume_subscription(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        let request = decode_semantic::<
            wire::ResumeSubscriptionRequest,
            domain::ResumeSubscriptionRequest,
        >(request)?;
        self.run(signal, self.inner.resume_subscription(&request))
            .await
            .map(|value| encode(wire::ResumeSubscriptionResponse::from(value)))
    }

    /// Execute `CheckpointActor` with an encoded protobuf request.
    pub async fn checkpoint_actor(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        let request = decode_semantic::<
            wire::CheckpointActorRequest,
            domain::CheckpointActorRequest,
        >(request)?;
        self.run(signal, self.inner.checkpoint_actor(&request))
            .await
            .map(|value| encode(wire::CheckpointActorResponse::from(value)))
    }

    /// Execute `InvokeActor` with an encoded protobuf request.
    pub async fn invoke_actor(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        let request =
            decode_semantic::<wire::InvokeActorRequest, domain::InvokeActorRequest>(request)?;
        self.run(signal, self.inner.invoke_actor(&request))
            .await
            .map(|value| encode(wire::InvokeActorResponse::from(value)))
    }
}
