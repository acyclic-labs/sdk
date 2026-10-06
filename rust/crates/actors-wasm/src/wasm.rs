//! Browser ABI for the Rust-owned Actors client facade.
//!
//! Transport construction, request validation, typed operations, and service
//! error decoding live in `acyclic_actors::client`. This module only owns the
//! JavaScript byte boundary, cancellation, and JS error projection.

use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
};

use acyclic_actors::{ContractError, MAX_BINDINGS, MAX_SUBSCRIPTIONS, client, wire};
use futures::{
    channel::oneshot,
    future::{Either, select},
    pin_mut,
};
use prost::Message;
use wasm_bindgen::{JsValue, prelude::*};

/// JavaScript names and byte-level aliases are emitted from this Rust boundary.
#[wasm_bindgen(typescript_custom_section)]
const ACTORS_TYPES: &'static str = r#"
export type ActorsWireBytes = Uint8Array;

export interface ActorsError extends Error {
  readonly code: string;
  readonly grpcCode?: string;
  readonly serviceCode?: number;
}

export function validateCreateActor(request: ActorsWireBytes): void;
export function validateUpdateActor(request: ActorsWireBytes): void;
export function validateAddSubscription(request: ActorsWireBytes): void;
"#;

fn js_error(code: &str, message: impl AsRef<str>) -> JsValue {
    let error = js_sys::Error::new(message.as_ref());
    let _ = js_sys::Reflect::set(&error, &JsValue::from_str("code"), &JsValue::from_str(code));
    error.into()
}

fn invalid(message: impl AsRef<str>) -> JsValue {
    js_error("invalid_argument", message)
}

fn busy() -> JsValue {
    js_error(
        "client_busy",
        "the Actors client is already executing an operation",
    )
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

fn facade_error(error: client::Error) -> JsValue {
    match error {
        client::Error::Configuration(message) => invalid(message),
        client::Error::Transport(message) => js_error("unavailable", message),
        client::Error::Service {
            grpc_code: code,
            detail,
        } => {
            let error = js_error(grpc_code(code), "Actors service request failed");
            let _ = js_sys::Reflect::set(
                &error,
                &JsValue::from_str("grpcCode"),
                &JsValue::from_str(grpc_code(code)),
            );
            if let Some(detail) = detail {
                let _ = js_sys::Reflect::set(
                    &error,
                    &JsValue::from_str("serviceCode"),
                    &JsValue::from_f64(f64::from(detail.code)),
                );
            }
            error
        }
    }
}

/// One explicit cancellation source. A cancelled handle is terminal and must
/// be replaced for the next operation.
#[wasm_bindgen]
pub struct CancellationHandle {
    sender: Rc<RefCell<Option<oneshot::Sender<()>>>>,
    requested: Rc<Cell<bool>>,
}

#[wasm_bindgen]
impl CancellationHandle {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            sender: Rc::new(RefCell::new(None)),
            requested: Rc::new(Cell::new(false)),
        }
    }

    pub fn cancel(&self) {
        self.requested.set(true);
        if let Ok(mut sender) = self.sender.try_borrow_mut() {
            if let Some(sender) = sender.take() {
                let _ = sender.send(());
            }
        }
    }

    pub fn cancelled(&self) -> bool {
        self.requested.get()
    }
}

impl CancellationHandle {
    fn begin(&self) -> Result<oneshot::Receiver<()>, JsValue> {
        let (sender, receiver) = oneshot::channel();
        let mut current = self.sender.try_borrow_mut().map_err(|_| busy())?;
        if self.requested.get() {
            return Err(js_error(
                "client_busy",
                "a cancelled handle cannot be reused",
            ));
        }
        if current.is_some() {
            return Err(busy());
        }
        *current = Some(sender);
        Ok(receiver)
    }

    fn finish(&self) {
        if let Ok(mut sender) = self.sender.try_borrow_mut() {
            sender.take();
        }
    }
}

async fn await_operation<F, T>(
    future: F,
    cancellation: Option<&CancellationHandle>,
) -> Result<T, JsValue>
where
    F: Future<Output = Result<T, client::Error>>,
{
    let operation = async { future.await.map_err(facade_error) };
    let Some(cancellation) = cancellation else {
        return operation.await;
    };
    let receiver = cancellation.begin()?;
    let cancel = async {
        let _ = receiver.await;
        Err(js_error(
            "cancelled",
            "Actors operation cancelled by the caller",
        ))
    };
    pin_mut!(operation);
    pin_mut!(cancel);
    let result = match select(operation, cancel).await {
        Either::Left((result, _)) => result,
        Either::Right((result, _)) => result,
    };
    cancellation.finish();
    result
}

async fn encode_operation<T, F>(
    future: F,
    cancellation: Option<&CancellationHandle>,
) -> Result<JsValue, JsValue>
where
    T: Message,
    F: Future<Output = Result<T, client::Error>>,
{
    encoded(&await_operation(future, cancellation).await?)
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
    inner: RefCell<client::Client>,
}

#[wasm_bindgen]
impl ActorsClient {
    /// Creates a browser client. `Client::from_browser` is synchronous because
    /// tonic-web transport construction does not perform network I/O.
    #[wasm_bindgen(constructor)]
    pub fn new(endpoint: String, token: String) -> Result<Self, JsValue> {
        let inner = client::Client::from_browser(&endpoint, &token).map_err(facade_error)?;
        Ok(Self {
            inner: RefCell::new(inner),
        })
    }

    #[wasm_bindgen(js_name = validateCreateActor)]
    pub fn validate_create_actor(request: JsValue) -> Result<(), JsValue> {
        validate_create(&decode(&request_bytes(request)?)?)
    }

    #[wasm_bindgen(js_name = validateUpdateActor)]
    pub fn validate_update_actor(request: JsValue) -> Result<(), JsValue> {
        validate_update(&decode(&request_bytes(request)?)?)
    }

    #[wasm_bindgen(js_name = validateAddSubscription)]
    pub fn validate_add_subscription(request: JsValue) -> Result<(), JsValue> {
        validate_add(&decode(&request_bytes(request)?)?)
    }

    #[wasm_bindgen(js_name = createActor, unchecked_param_type = "ActorsWireBytes", unchecked_return_type = "ActorsWireBytes")]
    pub async fn create_actor(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::CreateActorRequest>(&request_bytes(request)?)?;
        let client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        encode_operation(client.create_actor(&request), cancellation).await
    }

    #[wasm_bindgen(js_name = updateActor, unchecked_param_type = "ActorsWireBytes", unchecked_return_type = "ActorsWireBytes")]
    pub async fn update_actor(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::UpdateActorRequest>(&request_bytes(request)?)?;
        let client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        encode_operation(client.update_actor(&request), cancellation).await
    }

    #[wasm_bindgen(js_name = inspectActor, unchecked_param_type = "ActorsWireBytes", unchecked_return_type = "ActorsWireBytes")]
    pub async fn inspect_actor(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::InspectActorRequest>(&request_bytes(request)?)?;
        let client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        encode_operation(client.inspect_actor(&request), cancellation).await
    }

    #[wasm_bindgen(js_name = addSubscription, unchecked_param_type = "ActorsWireBytes", unchecked_return_type = "ActorsWireBytes")]
    pub async fn add_subscription(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::AddSubscriptionRequest>(&request_bytes(request)?)?;
        let client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        encode_operation(client.add_subscription(&request), cancellation).await
    }

    #[wasm_bindgen(js_name = removeSubscription, unchecked_param_type = "ActorsWireBytes", unchecked_return_type = "ActorsWireBytes")]
    pub async fn remove_subscription(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::RemoveSubscriptionRequest>(&request_bytes(request)?)?;
        let client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        encode_operation(client.remove_subscription(&request), cancellation).await
    }

    #[wasm_bindgen(js_name = resumeSubscription, unchecked_param_type = "ActorsWireBytes", unchecked_return_type = "ActorsWireBytes")]
    pub async fn resume_subscription(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::ResumeSubscriptionRequest>(&request_bytes(request)?)?;
        let client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        encode_operation(client.resume_subscription(&request), cancellation).await
    }

    #[wasm_bindgen(js_name = checkpointActor, unchecked_param_type = "ActorsWireBytes", unchecked_return_type = "ActorsWireBytes")]
    pub async fn checkpoint_actor(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::CheckpointActorRequest>(&request_bytes(request)?)?;
        let client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        encode_operation(client.checkpoint_actor(&request), cancellation).await
    }

    #[wasm_bindgen(js_name = invokeActor, unchecked_param_type = "ActorsWireBytes", unchecked_return_type = "ActorsWireBytes")]
    pub async fn invoke_actor(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::InvokeActorRequest>(&request_bytes(request)?)?;
        let client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        encode_operation(client.invoke_actor(&request), cancellation).await
    }
}

#[allow(dead_code)]
const _CONTRACT_LIMITS: (usize, usize) = (MAX_SUBSCRIPTIONS, MAX_BINDINGS);

#[cfg(test)]
mod cancellation_tests {
    use super::*;

    #[test]
    fn handle_starts_idle_and_cancel_is_explicit() {
        let handle = CancellationHandle::new();
        assert!(!handle.cancelled());
        handle.cancel();
        assert!(handle.cancelled());
    }

    #[test]
    fn normal_completion_allows_handle_reuse() {
        let handle = CancellationHandle::new();
        let first = handle.begin();
        assert!(first.is_ok());
        assert!(!handle.cancelled());
        handle.finish();

        let second = handle.begin();
        assert!(second.is_ok());
        assert!(!handle.cancelled());
        handle.finish();
    }

    #[test]
    fn begin_rejects_concurrent_use_until_finished() {
        let handle = CancellationHandle::new();
        let active = handle.begin();
        assert!(active.is_ok());
        assert!(handle.begin().is_err());

        handle.finish();
        assert!(handle.begin().is_ok());
        handle.finish();
    }

    #[test]
    fn inflight_cancellation_is_terminal_and_requires_fresh_handle() {
        let handle = CancellationHandle::new();
        let mut polls = 0;
        let operation =
            futures::future::poll_fn(|_| -> std::task::Poll<Result<(), client::Error>> {
                polls += 1;
                if polls == 1 {
                    handle.cancel();
                }
                std::task::Poll::Pending
            });

        let result = futures::executor::block_on(await_operation(operation, Some(&handle)));
        assert!(result.is_err());
        assert_eq!(polls, 1);
        assert!(handle.cancelled());
        assert!(handle.begin().is_err());
    }
}
