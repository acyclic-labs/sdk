//! Browser gRPC-Web client and generated operation bindings.

use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
};

use acyclic_actors::{ContractError, MAX_BINDINGS, MAX_SUBSCRIPTIONS};
use futures::{
    channel::oneshot,
    future::{Either, select},
    pin_mut,
};
use prost::Message;
use tonic::{
    Request, Status,
    codegen::InterceptedService,
    metadata::{Ascii, MetadataValue},
    service::Interceptor,
};
use wasm_bindgen::{JsValue, prelude::*};

use crate::wire;

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

#[derive(Clone)]
struct BearerAuth(MetadataValue<Ascii>);

impl Interceptor for BearerAuth {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", self.0.clone());
        Ok(request)
    }
}

type Transport = tonic_web_wasm_client::Client;
type GeneratedClient =
    wire::actors_service_client::ActorsServiceClient<InterceptedService<Transport, BearerAuth>>;

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

fn grpc_code(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "ok",
        tonic::Code::Cancelled => "cancelled",
        tonic::Code::Unknown => "unknown",
        tonic::Code::InvalidArgument => "invalid_argument",
        tonic::Code::DeadlineExceeded => "deadline_exceeded",
        tonic::Code::NotFound => "not_found",
        tonic::Code::AlreadyExists => "already_exists",
        tonic::Code::PermissionDenied => "permission_denied",
        tonic::Code::ResourceExhausted => "resource_exhausted",
        tonic::Code::FailedPrecondition => "failed_precondition",
        tonic::Code::Aborted => "aborted",
        tonic::Code::OutOfRange => "out_of_range",
        tonic::Code::Unimplemented => "unimplemented",
        tonic::Code::Internal => "internal",
        tonic::Code::Unavailable => "unavailable",
        tonic::Code::DataLoss => "data_loss",
        tonic::Code::Unauthenticated => "unauthenticated",
    }
}

fn status_error(status: Status) -> JsValue {
    let error = js_error(grpc_code(status.code()), status.message());
    let _ = js_sys::Reflect::set(
        &error,
        &JsValue::from_str("grpcCode"),
        &JsValue::from_str(grpc_code(status.code())),
    );
    if !status.details().is_empty() {
        if let Ok(detail) = wire::Error::decode(status.details()) {
            let _ = js_sys::Reflect::set(
                &error,
                &JsValue::from_str("serviceCode"),
                &JsValue::from_f64(f64::from(detail.code)),
            );
        }
    }
    error
}

/// One explicit cancellation source. A cancellation handle belongs to one
/// in-flight operation and never infers cancellation from an arbitrary error.
#[wasm_bindgen]
pub struct CancellationHandle {
    sender: Rc<RefCell<Option<oneshot::Sender<()>>>>,
    active: Rc<Cell<bool>>,
    requested: Rc<Cell<bool>>,
}

#[wasm_bindgen]
impl CancellationHandle {
    /// Creates an unused cancellation handle.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            sender: Rc::new(RefCell::new(None)),
            active: Rc::new(Cell::new(false)),
            requested: Rc::new(Cell::new(false)),
        }
    }

    /// Requests cancellation of the associated operation.
    pub fn cancel(&self) {
        self.requested.set(true);
        if let Ok(mut sender) = self.sender.try_borrow_mut() {
            if let Some(sender) = sender.take() {
                let _ = sender.send(());
            }
        }
    }

    /// Returns whether cancellation has already been requested.
    pub fn cancelled(&self) -> bool {
        self.requested.get()
    }
}

impl CancellationHandle {
    fn begin(&self) -> Result<oneshot::Receiver<()>, JsValue> {
        let (sender, receiver) = oneshot::channel();
        let mut current = self.sender.try_borrow_mut().map_err(|_| busy())?;
        if self.active.get() {
            return Err(js_error(
                "client_busy",
                "cancellation handle is already in use",
            ));
        }
        // A handle can be reused after an operation completes. Reset its
        // per-operation state only once the previous operation has completed.
        self.requested.set(false);
        self.active.set(true);
        *current = Some(sender);
        Ok(receiver)
    }

    fn finish(&self) {
        if let Ok(mut sender) = self.sender.try_borrow_mut() {
            sender.take();
            self.active.set(false);
        }
    }
}

async fn await_response<F, M>(
    future: F,
    cancellation: Option<&CancellationHandle>,
) -> Result<tonic::Response<M>, JsValue>
where
    F: Future<Output = Result<tonic::Response<M>, Status>>,
{
    let Some(cancellation) = cancellation else {
        return future.await.map_err(status_error);
    };
    let receiver = cancellation.begin()?;
    let cancel = async move {
        let _ = receiver.await;
        Err(Status::cancelled("operation cancelled by the caller"))
    };
    pin_mut!(future);
    pin_mut!(cancel);
    let result = match select(future, cancel).await {
        Either::Left((result, _)) => result,
        Either::Right((result, _)) => result,
    };
    cancellation.finish();
    result.map_err(status_error)
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

/// Browser Actors client using the maintained tonic gRPC-Web transport.
#[wasm_bindgen]
pub struct ActorsClient {
    inner: RefCell<GeneratedClient>,
}

#[wasm_bindgen]
impl ActorsClient {
    /// Creates a browser client with bearer authentication.
    #[wasm_bindgen(constructor)]
    pub fn new(endpoint: String, token: String) -> Result<Self, JsValue> {
        let is_http = endpoint.starts_with("http://") || endpoint.starts_with("https://");
        if !is_http || endpoint.contains(['@', '\r', '\n', '#', '?']) {
            return Err(invalid(
                "endpoint must be an HTTP(S) URL without userinfo, query, or fragment",
            ));
        }
        if token.trim().is_empty() || token.contains(['\r', '\n']) {
            return Err(invalid("bearer token is required"));
        }
        let mut authorization: MetadataValue<Ascii> = format!("Bearer {token}")
            .parse()
            .map_err(|_| invalid("bearer token is not valid HTTP metadata"))?;
        authorization.set_sensitive(true);
        let transport = Transport::new(endpoint);
        let client = wire::actors_service_client::ActorsServiceClient::with_interceptor(
            transport,
            BearerAuth(authorization),
        )
        .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(crate::MAX_MESSAGE_BYTES);
        Ok(Self {
            inner: RefCell::new(client),
        })
    }

    /// Validates a CreateActor protobuf before it is sent.
    #[wasm_bindgen(js_name = validateCreateActor)]
    pub fn validate_create_actor(request: JsValue) -> Result<(), JsValue> {
        validate_create(&decode(&request_bytes(request)?)?)
    }

    /// Validates an UpdateActor protobuf before it is sent.
    #[wasm_bindgen(js_name = validateUpdateActor)]
    pub fn validate_update_actor(request: JsValue) -> Result<(), JsValue> {
        validate_update(&decode(&request_bytes(request)?)?)
    }

    /// Validates an AddSubscription protobuf before it is sent.
    #[wasm_bindgen(js_name = validateAddSubscription)]
    pub fn validate_add_subscription(request: JsValue) -> Result<(), JsValue> {
        validate_add(&decode(&request_bytes(request)?)?)
    }

    #[wasm_bindgen(
        js_name = createActor,
        unchecked_param_type = "ActorsWireBytes",
        unchecked_return_type = "ActorsWireBytes"
    )]
    pub async fn create_actor(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::CreateActorRequest>(&request_bytes(request)?)?;
        validate_create(&request)?;
        let mut client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        let response = client.create_actor(Request::new(request));
        encoded(&await_response(response, cancellation).await?.into_inner())
    }

    #[wasm_bindgen(
        js_name = updateActor,
        unchecked_param_type = "ActorsWireBytes",
        unchecked_return_type = "ActorsWireBytes"
    )]
    pub async fn update_actor(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::UpdateActorRequest>(&request_bytes(request)?)?;
        validate_update(&request)?;
        let mut client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        let response = client.update_actor(Request::new(request));
        encoded(&await_response(response, cancellation).await?.into_inner())
    }

    #[wasm_bindgen(
        js_name = inspectActor,
        unchecked_param_type = "ActorsWireBytes",
        unchecked_return_type = "ActorsWireBytes"
    )]
    pub async fn inspect_actor(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::InspectActorRequest>(&request_bytes(request)?)?;
        let mut client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        let response = client.inspect_actor(Request::new(request));
        encoded(&await_response(response, cancellation).await?.into_inner())
    }

    #[wasm_bindgen(
        js_name = addSubscription,
        unchecked_param_type = "ActorsWireBytes",
        unchecked_return_type = "ActorsWireBytes"
    )]
    pub async fn add_subscription(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::AddSubscriptionRequest>(&request_bytes(request)?)?;
        validate_add(&request)?;
        let mut client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        let response = client.add_subscription(Request::new(request));
        encoded(&await_response(response, cancellation).await?.into_inner())
    }

    #[wasm_bindgen(
        js_name = removeSubscription,
        unchecked_param_type = "ActorsWireBytes",
        unchecked_return_type = "ActorsWireBytes"
    )]
    pub async fn remove_subscription(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::RemoveSubscriptionRequest>(&request_bytes(request)?)?;
        let mut client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        let response = client.remove_subscription(Request::new(request));
        encoded(&await_response(response, cancellation).await?.into_inner())
    }

    #[wasm_bindgen(
        js_name = resumeSubscription,
        unchecked_param_type = "ActorsWireBytes",
        unchecked_return_type = "ActorsWireBytes"
    )]
    pub async fn resume_subscription(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::ResumeSubscriptionRequest>(&request_bytes(request)?)?;
        let mut client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        let response = client.resume_subscription(Request::new(request));
        encoded(&await_response(response, cancellation).await?.into_inner())
    }

    #[wasm_bindgen(
        js_name = checkpointActor,
        unchecked_param_type = "ActorsWireBytes",
        unchecked_return_type = "ActorsWireBytes"
    )]
    pub async fn checkpoint_actor(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::CheckpointActorRequest>(&request_bytes(request)?)?;
        let mut client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        let response = client.checkpoint_actor(Request::new(request));
        encoded(&await_response(response, cancellation).await?.into_inner())
    }

    #[wasm_bindgen(
        js_name = invokeActor,
        unchecked_param_type = "ActorsWireBytes",
        unchecked_return_type = "ActorsWireBytes"
    )]
    pub async fn invoke_actor(
        &self,
        request: JsValue,
        cancellation: Option<&CancellationHandle>,
    ) -> Result<JsValue, JsValue> {
        let request = decode::<wire::InvokeActorRequest>(&request_bytes(request)?)?;
        let mut client = self.inner.try_borrow_mut().map_err(|_| busy())?;
        let response = client.invoke_actor(Request::new(request));
        encoded(&await_response(response, cancellation).await?.into_inner())
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
    fn begin_resets_request_state_for_reuse() {
        let handle = CancellationHandle::new();
        handle.cancel();
        assert!(handle.cancelled());

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
    fn cancel_notifies_receiver_and_releases_sender() {
        let handle = CancellationHandle::new();
        let mut receiver = match handle.begin() {
            Ok(receiver) => receiver,
            Err(_) => return,
        };

        handle.cancel();
        assert!(handle.cancelled());
        assert!(matches!(receiver.try_recv(), Ok(Some(()))));

        // Cancellation wakes the operation but does not permit reuse until
        // its cleanup has run, so the old operation cannot clear a new one.
        assert!(handle.begin().is_err());
        handle.finish();
        assert!(handle.begin().is_ok());
        handle.finish();
    }

    #[test]
    fn inflight_cancellation_terminates_without_replaying_operation() {
        let handle = CancellationHandle::new();
        let mut polls = 0;
        let operation = futures::future::poll_fn(
            |_| -> std::task::Poll<Result<tonic::Response<()>, Status>> {
                polls += 1;
                if polls == 1 {
                    handle.cancel();
                }
                std::task::Poll::Pending
            },
        );

        let result = futures::executor::block_on(await_response(operation, Some(&handle)));
        assert!(result.is_err());
        assert_eq!(polls, 1);

        // Cancellation is terminal for this operation. The sender was
        // consumed and cleanup leaves the handle available for a new one.
        assert!(handle.cancelled());
        assert!(handle.begin().is_ok());
        handle.finish();
    }
}
