//! WebAssembly bridge for the Rust-owned Actors client.
//!
//! Only protobuf bytes cross this boundary. Endpoint, credential, message
//! size, contract, and semantic conversion policy remain in `acyclic-actors`.

#![cfg(target_arch = "wasm32")]

use acyclic_actors::{client, domain, wire};
use prost::Message;
use wasm_bindgen::prelude::*;

fn js_error(code: &str, message: impl std::fmt::Display) -> JsValue {
    let error = js_sys::Error::new(&message.to_string());
    error.set_name("ActorsError");
    let _ = js_sys::Reflect::set(&error, &JsValue::from_str("code"), &JsValue::from_str(code));
    error.into()
}

fn map_error(error: client::Error) -> JsValue {
    let code = match error {
        client::Error::Configuration(_) => "configuration",
        client::Error::Transport(_) => "transport",
        client::Error::Service { .. } => "service",
        client::Error::Contract(_) => "contract",
        client::Error::Semantic(_) => "semantic",
        client::Error::Cancelled => "cancelled",
    };
    js_error(code, error)
}

fn decode<T: Message + Default>(bytes: &[u8]) -> Result<T, JsValue> {
    T::decode(bytes).map_err(|error| js_error("invalid-request", error))
}

fn decode_semantic<T, D>(bytes: &[u8]) -> Result<D, JsValue>
where
    T: Message + Default,
    D: TryFrom<T>,
    D::Error: std::fmt::Display,
{
    D::try_from(decode::<T>(bytes)?).map_err(|error| js_error("invalid-request", error))
}

fn encode<T: Message>(message: T) -> Vec<u8> {
    message.encode_to_vec()
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
    pub async fn connect(endpoint: String, token: String) -> Result<ActorsClient, JsValue> {
        client::connect(&endpoint, &token)
            .await
            .map(|inner| Self { inner })
            .map_err(map_error)
    }

    /// Returns the transport selected by the Rust client.
    #[wasm_bindgen(getter)]
    pub fn transport(&self) -> String {
        self.inner.transport().to_owned()
    }

    /// Execute `CreateActor` with an encoded protobuf request.
    pub async fn create_actor(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        let request =
            decode_semantic::<wire::CreateActorRequest, domain::CreateActorRequest>(request)?;
        self.inner
            .create_actor(&request)
            .await
            .map(|value| encode(wire::CreateActorResponse::from(value)))
            .map_err(map_error)
    }

    /// Execute `UpdateActor` with an encoded protobuf request.
    pub async fn update_actor(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        let request =
            decode_semantic::<wire::UpdateActorRequest, domain::UpdateActorRequest>(request)?;
        self.inner
            .update_actor(&request)
            .await
            .map(|value| encode(wire::UpdateActorResponse::from(value)))
            .map_err(map_error)
    }

    /// Execute `InspectActor` with an encoded protobuf request.
    pub async fn inspect_actor(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        let request =
            decode_semantic::<wire::InspectActorRequest, domain::InspectActorRequest>(request)?;
        self.inner
            .inspect_actor(&request)
            .await
            .map(|value| encode(wire::InspectActorResponse::from(value)))
            .map_err(map_error)
    }

    /// Execute `AddSubscription` with an encoded protobuf request.
    pub async fn add_subscription(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        let request = decode_semantic::<
            wire::AddSubscriptionRequest,
            domain::AddSubscriptionRequest,
        >(request)?;
        self.inner
            .add_subscription(&request)
            .await
            .map(|value| encode(wire::AddSubscriptionResponse::from(value)))
            .map_err(map_error)
    }

    /// Execute `RemoveSubscription` with an encoded protobuf request.
    pub async fn remove_subscription(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        let request = decode_semantic::<
            wire::RemoveSubscriptionRequest,
            domain::RemoveSubscriptionRequest,
        >(request)?;
        self.inner
            .remove_subscription(&request)
            .await
            .map(|value| encode(wire::RemoveSubscriptionResponse::from(value)))
            .map_err(map_error)
    }

    /// Execute `ResumeSubscription` with an encoded protobuf request.
    pub async fn resume_subscription(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        let request = decode_semantic::<
            wire::ResumeSubscriptionRequest,
            domain::ResumeSubscriptionRequest,
        >(request)?;
        self.inner
            .resume_subscription(&request)
            .await
            .map(|value| encode(wire::ResumeSubscriptionResponse::from(value)))
            .map_err(map_error)
    }

    /// Execute `CheckpointActor` with an encoded protobuf request.
    pub async fn checkpoint_actor(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        let request = decode_semantic::<
            wire::CheckpointActorRequest,
            domain::CheckpointActorRequest,
        >(request)?;
        self.inner
            .checkpoint_actor(&request)
            .await
            .map(|value| encode(wire::CheckpointActorResponse::from(value)))
            .map_err(map_error)
    }

    /// Execute `InvokeActor` with an encoded protobuf request.
    pub async fn invoke_actor(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        let request =
            decode_semantic::<wire::InvokeActorRequest, domain::InvokeActorRequest>(request)?;
        self.inner
            .invoke_actor(&request)
            .await
            .map(|value| encode(wire::InvokeActorResponse::from(value)))
            .map_err(map_error)
    }
}
