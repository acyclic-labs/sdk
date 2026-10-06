#![forbid(unsafe_code)]
//! Protobuf byte boundary for the canonical inference contract.

mod schema;

/// Return the current Rust-owned Run terminal metadata for code generators.
pub fn run_terminal_metadata_native() -> Result<String, &'static str> {
    schema::terminal_metadata()
}

/// Return canonical protobuf fixed-byte field metadata for code generators.
pub fn fixed_width_metadata_native() -> Result<String, &'static str> {
    schema::fixed_width_metadata()
}

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
use js_sys::Array;

#[cfg(target_arch = "wasm32")]
use prost::Message;

/// Opaque Rust-owned state for an ordered Run watch.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub struct WatchRunState {
    inner: acyclic_inference::WatchRunState,
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
impl WatchRunState {
    /// Whether a terminal event has already been consumed.
    #[wasm_bindgen(getter)]
    pub fn terminal(&self) -> bool {
        self.inner.is_terminal()
    }

    /// Validate one protobuf Run event and advance this state in place.
    #[wasm_bindgen]
    pub fn advance(&mut self, event: &[u8]) -> Result<(), JsValue> {
        self.inner.advance_wire(event).map_err(JsValue::from_str)
    }

    /// Confirm that the stream ended after a terminal event.
    #[wasm_bindgen]
    pub fn finish(&self) -> Result<(), JsValue> {
        self.inner.finish().map_err(JsValue::from_str)
    }
}

/// Validate the exact generated message shape and caller-bound identity.
/// Byte arrays remain byte arrays across this boundary; no JSON or JS number
/// conversion is involved.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn validate_customer_wire(
    kind: &str,
    message: &[u8],
    expected: &[u8],
    related: &[u8],
) -> Result<(), JsValue> {
    acyclic_inference::validate_customer_wire(kind, message, expected, related)
        .map_err(JsValue::from_str)
}

/// Validate one remote bearer credential using the shared Rust policy.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn validate_remote_web_credential(token: &str) -> Result<(), JsValue> {
    use acyclic_sdk_contract_wire::{BEARER_NO_CRLF, credential};
    credential::validate(BEARER_NO_CRLF, token)
        .then_some(())
        .ok_or_else(|| JsValue::from_str("invalid bearer credential"))
}

/// Rust-owned browser client for the complete Inference service surface.
///
/// The TypeScript package passes generated protobuf bytes across this boundary;
/// transport selection, authenticated handshake, route derivation, response
/// decoding, size limits, and streaming validation remain in Rust.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub struct BrowserInferenceClient {
    inner: acyclic_inference::client::Client,
}

#[cfg(target_arch = "wasm32")]
fn decode<T: Message + Default>(bytes: &[u8]) -> Result<T, JsValue> {
    T::decode(bytes)
        .map_err(|error| JsValue::from_str(&format!("invalid Inference request: {error}")))
}

#[cfg(target_arch = "wasm32")]
fn encode<T: Message>(value: T) -> Vec<u8> {
    value.encode_to_vec()
}

#[cfg(target_arch = "wasm32")]
fn client_error(error: acyclic_inference::client::Error) -> JsValue {
    JsValue::from_str(&error.to_string())
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
impl BrowserInferenceClient {
    /// Connect using the Rust-owned best browser transport and handshake.
    #[wasm_bindgen(js_name = connect)]
    pub async fn connect_js(endpoint: String, token: String) -> Result<Self, JsValue> {
        let inner = acyclic_inference::client::Client::connect(&endpoint, &token)
            .await
            .map_err(client_error)?;
        Ok(Self { inner })
    }

    /// Connect with an explicit response bound for advanced consumers.
    #[wasm_bindgen(js_name = connectWithLimit)]
    pub async fn connect_with_limit_js(
        endpoint: String,
        token: String,
        maximum_response_bytes: u32,
    ) -> Result<Self, JsValue> {
        let inner = acyclic_inference::client::Client::connect_with_limit(
            &endpoint,
            &token,
            usize::try_from(maximum_response_bytes)
                .map_err(|_| JsValue::from_str("response bound is too large"))?,
        )
        .await
        .map_err(client_error)?;
        Ok(Self { inner })
    }

    /// Returns the negotiated Rust transport name.
    #[wasm_bindgen(js_name = transport)]
    pub fn transport_js(&self) -> String {
        match self.inner.transport() {
            acyclic_inference::client::Transport::Grpc => "grpc".to_owned(),
            acyclic_inference::client::Transport::Http => "http".to_owned(),
        }
    }

    /// List model capabilities through the negotiated Rust transport.
    #[wasm_bindgen(js_name = listModels)]
    pub async fn list_models(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        Ok(encode(
            self.inner
                .list(&decode(request)?)
                .await
                .map_err(client_error)?,
        ))
    }

    /// Create one durable inference context.
    #[wasm_bindgen(js_name = createContext)]
    pub async fn create_context(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        Ok(encode(
            self.inner
                .create_context(&decode(request)?)
                .await
                .map_err(client_error)?,
        ))
    }

    /// Inspect one durable inference context.
    #[wasm_bindgen(js_name = inspectContext)]
    pub async fn inspect_context(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        Ok(encode(
            self.inner
                .inspect_context(&decode(request)?)
                .await
                .map_err(client_error)?,
        ))
    }

    /// Apply one context mutation.
    #[wasm_bindgen(js_name = mutateContext)]
    pub async fn mutate_context(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        Ok(encode(
            self.inner
                .mutate_context(&decode(request)?)
                .await
                .map_err(client_error)?,
        ))
    }

    /// Retain one warm context.
    #[wasm_bindgen(js_name = retainWarm)]
    pub async fn retain_warm(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        Ok(encode(
            self.inner
                .retain_warm(&decode(request)?)
                .await
                .map_err(client_error)?,
        ))
    }

    /// Inspect one retained warm context.
    #[wasm_bindgen(js_name = inspectWarm)]
    pub async fn inspect_warm(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        Ok(encode(
            self.inner
                .inspect_warm(&decode(request)?)
                .await
                .map_err(client_error)?,
        ))
    }

    /// Renew one retained warm context.
    #[wasm_bindgen(js_name = renewWarm)]
    pub async fn renew_warm(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        Ok(encode(
            self.inner
                .renew_warm(&decode(request)?)
                .await
                .map_err(client_error)?,
        ))
    }

    /// Release one retained warm context.
    #[wasm_bindgen(js_name = releaseWarm)]
    pub async fn release_warm(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        Ok(encode(
            self.inner
                .release_warm(&decode(request)?)
                .await
                .map_err(client_error)?,
        ))
    }

    /// Start one recoverable inference run.
    #[wasm_bindgen(js_name = generateRun)]
    pub async fn generate_run(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        Ok(encode(
            self.inner
                .generate_run(&decode(request)?)
                .await
                .map_err(client_error)?,
        ))
    }

    /// Inspect one inference run.
    #[wasm_bindgen(js_name = inspectRun)]
    pub async fn inspect_run(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        Ok(encode(
            self.inner
                .inspect_run(&decode(request)?)
                .await
                .map_err(client_error)?,
        ))
    }

    /// Collect the ordered events for one inference run.
    #[wasm_bindgen(js_name = watchRun)]
    pub async fn watch_run(&self, request: &[u8]) -> Result<Array, JsValue> {
        let events = self
            .inner
            .watch_run(&decode(request)?)
            .await
            .map_err(client_error)?;
        let result = Array::new();
        for event in events {
            result.push(&js_sys::Uint8Array::from(encode(event).as_slice()));
        }
        Ok(result)
    }

    /// Cancel one inference run idempotently.
    #[wasm_bindgen(js_name = cancelRun)]
    pub async fn cancel_run(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        Ok(encode(
            self.inner
                .cancel_run(&decode(request)?)
                .await
                .map_err(client_error)?,
        ))
    }

    /// Create one durable evaluation.
    #[wasm_bindgen(js_name = createEvaluation)]
    pub async fn create_evaluation(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        Ok(encode(
            self.inner
                .create_evaluation(&decode(request)?)
                .await
                .map_err(client_error)?,
        ))
    }

    /// Inspect one durable evaluation.
    #[wasm_bindgen(js_name = inspectEvaluation)]
    pub async fn inspect_evaluation(&self, request: &[u8]) -> Result<Vec<u8>, JsValue> {
        Ok(encode(
            self.inner
                .inspect_evaluation(&decode(request)?)
                .await
                .map_err(client_error)?,
        ))
    }
}

/// Decide from a validated Run view whether watching at this cursor is already complete.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn watch_run_start_wire(
    message: &[u8],
    expected: &[u8],
    from_sequence: &str,
) -> Result<bool, JsValue> {
    acyclic_inference::watch_run_start_wire(message, expected, from_sequence)
        .map_err(JsValue::from_str)
}

/// Start Rust-owned state for a validated Run watch.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub fn watch_run_start_state_wire(
    message: &[u8],
    expected: &[u8],
    from_sequence: &str,
) -> Result<WatchRunState, JsValue> {
    acyclic_inference::watch_run_start_state_wire(message, expected, from_sequence)
        .map(|inner| WatchRunState { inner })
        .map_err(JsValue::from_str)
}
