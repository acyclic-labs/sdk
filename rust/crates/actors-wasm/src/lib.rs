//! WebAssembly boundary for the Rust Actors client.
#![cfg(target_arch = "wasm32")]

use acyclic_actors::{client, domain};
use js_sys::{BigInt, Function, Reflect, Uint8Array};
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
    let metadata = error.metadata();
    let error = js_error(&metadata.code, &metadata.message);
    match serde_wasm_bindgen::to_value(&metadata) {
        Ok(metadata) => {
            let _ = Reflect::set(&error, &JsValue::from_str("metadata"), &metadata);
            error
        }
        Err(error) => js_error("internal", error),
    }
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

}

impl Drop for AbortRegistration {
    fn drop(&mut self) {
        if let Ok(remove) = Reflect::get(&self.signal, &JsValue::from_str("removeEventListener"))
            .and_then(|value| value.dyn_into::<Function>())
        {
            let _ = remove.call2(
                &self.signal,
                &JsValue::from_str("abort"),
                self.callback.as_ref().unchecked_ref(),
            );
        }
    }
}

/// Browser binding for the shared Actors client.
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
        let cancellation = registration.as_ref().map(|registration| registration.token.clone());
        let result =
            client::run_with_cancellation(client::connect(&endpoint, &token, None), cancellation).await;
        drop(registration);
        result.map(|inner| Self { inner }).map_err(map_error)
    }

    async fn run<T, F>(&self, signal: Option<JsValue>, operation: F) -> Result<T, JsValue>
    where
        F: std::future::Future<Output = Result<T, client::Error>>,
    {
        let registration = AbortRegistration::new(signal)?;
        let cancellation = registration.as_ref().map(|registration| registration.token.clone());
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
        self.run(signal, self.inner.call("CreateActor", request)).await
    }

    /// Execute `UpdateActor` with an encoded protobuf request.
    pub async fn update_actor(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        self.run(signal, self.inner.call("UpdateActor", request)).await
    }

    /// Execute `InspectActor` with an encoded protobuf request.
    pub async fn inspect_actor(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        self.run(signal, self.inner.call("InspectActor", request)).await
    }

    /// Execute `AddSubscription` with an encoded protobuf request.
    pub async fn add_subscription(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        self.run(signal, self.inner.call("AddSubscription", request)).await
    }

    /// Execute `RemoveSubscription` with an encoded protobuf request.
    pub async fn remove_subscription(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        self.run(signal, self.inner.call("RemoveSubscription", request)).await
    }

    /// Execute `ResumeSubscription` with an encoded protobuf request.
    pub async fn resume_subscription(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        self.run(signal, self.inner.call("ResumeSubscription", request)).await
    }

    /// Execute `CheckpointActor` with an encoded protobuf request.
    pub async fn checkpoint_actor(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        self.run(signal, self.inner.call("CheckpointActor", request)).await
    }

    /// Execute `InvokeActor` with an encoded protobuf request.
    pub async fn invoke_actor(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        self.run(signal, self.inner.call("InvokeActor", request)).await
    }

    /// Execute `DeleteActor` with an encoded protobuf request.
    pub async fn delete_actor(
        &self,
        request: &[u8],
        signal: Option<JsValue>,
    ) -> Result<Vec<u8>, JsValue> {
        self.run(signal, self.inner.call("DeleteActor", request)).await
    }
}
