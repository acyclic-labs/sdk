//! N-API bridge for the canonical Rust remote Inference client.
//!
//! Requests and responses cross the JavaScript boundary as protobuf bytes. The
//! bridge owns no transport or contract policy: connection selection,
//! authenticated handshakes, HTTP limits, route derivation, and operation
//! behavior remain in `acyclic_inference::client::Client`.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use acyclic_inference::{client, wire};
use napi::bindgen_prelude::{Buffer, Error, Result, Status};
use napi_derive::napi;
use prost::Message;
use tokio::sync::Notify;

const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");

fn native_error(context: &str, error: impl std::fmt::Display) -> Error {
    Error::new(Status::GenericFailure, format!("{context}: {error}"))
}

fn client_error(error: client::Error) -> Error {
    native_error("Inference operation failed", error)
}

fn decode_wire<T: Message + Default>(value: &Buffer, operation: &str) -> Result<T> {
    T::decode(value.as_ref()).map_err(|error| native_error(operation, error))
}

fn encode_wire<T: Message>(value: &T, operation: &str) -> Result<Buffer> {
    let mut bytes = Vec::with_capacity(value.encoded_len());
    value
        .encode(&mut bytes)
        .map_err(|error| native_error(operation, error))?;
    Ok(Buffer::from(bytes))
}

fn bounded_limit(value: u32) -> Result<usize> {
    usize::try_from(value).map_err(|error| native_error("response bound", error))
}

fn cancelled_error() -> Error {
    Error::new(Status::GenericFailure, "Inference operation cancelled")
}

async fn cancellable<T, Operation>(
    operation: Operation,
    cancellation: Option<CancellationState>,
) -> Result<T>
where
    Operation: Future<Output = std::result::Result<T, client::Error>>,
{
    let Some(cancellation) = cancellation else {
        return operation.await.map_err(client_error);
    };
    if cancellation.cancelled.load(Ordering::Acquire) {
        return Err(cancelled_error());
    }
    tokio::select! {
        result = operation => result.map_err(client_error),
        () = cancellation.wake.notified() => Err(cancelled_error()),
    }
}

#[derive(Clone)]
struct CancellationState {
    cancelled: Arc<AtomicBool>,
    wake: Arc<Notify>,
}

/// A per-operation cancellation handle for native Inference calls.
#[napi]
pub struct NativeInferenceCancellation {
    state: CancellationState,
}

impl Default for NativeInferenceCancellation {
    fn default() -> Self {
        Self {
            state: CancellationState {
                cancelled: Arc::new(AtomicBool::new(false)),
                wake: Arc::new(Notify::new()),
            },
        }
    }
}

#[napi]
impl NativeInferenceCancellation {
    /// Creates a cancellation handle in the non-cancelled state.
    #[napi(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancels the attached Rust operation and wakes its transport future.
    #[napi]
    pub fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Release);
        self.state.wake.notify_waiters();
    }

    /// Whether cancellation has been requested.
    #[napi(getter)]
    pub fn cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Acquire)
    }
}

fn cancellation_state(
    cancellation: Option<&NativeInferenceCancellation>,
) -> Option<CancellationState> {
    cancellation.map(|value| value.state.clone())
}

/// Native Inference client backed directly by the canonical Rust client.
#[napi]
pub struct NativeInferenceClient {
    inner: Arc<client::Client>,
}

impl NativeInferenceClient {
    fn from_client(inner: client::Client) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[napi]
impl NativeInferenceClient {
    /// Connects using the Rust-owned native transport preference and handshake.
    #[napi(factory)]
    pub async fn connect(
        endpoint: String,
        token: String,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Self> {
        let inner = cancellable(
            client::Client::connect(&endpoint, &token),
            cancellation_state(cancellation),
        )
        .await?;
        Ok(Self::from_client(inner))
    }

    /// Connects with an explicit Rust-owned response byte ceiling.
    #[napi(factory, js_name = "connectWithLimit")]
    pub async fn connect_with_limit(
        endpoint: String,
        token: String,
        maximum_response_bytes: u32,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Self> {
        let maximum_response_bytes = bounded_limit(maximum_response_bytes)?;
        let inner = cancellable(
            client::Client::connect_with_limit(&endpoint, &token, maximum_response_bytes),
            cancellation_state(cancellation),
        )
        .await?;
        Ok(Self::from_client(inner))
    }

    /// Connects with a caller-pinned native CA certificate.
    #[napi(factory, js_name = "connectWithCa")]
    pub async fn connect_with_ca(
        endpoint: String,
        token: String,
        ca_certificate_pem: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Self> {
        let inner = cancellable(
            client::Client::connect_with_ca(&endpoint, &token, ca_certificate_pem.as_ref()),
            cancellation_state(cancellation),
        )
        .await?;
        Ok(Self::from_client(inner))
    }

    /// Connects with a response ceiling and caller-pinned native CA certificate.
    #[napi(factory, js_name = "connectWithLimitAndCa")]
    pub async fn connect_with_limit_and_ca(
        endpoint: String,
        token: String,
        maximum_response_bytes: u32,
        ca_certificate_pem: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Self> {
        let maximum_response_bytes = bounded_limit(maximum_response_bytes)?;
        let inner = cancellable(
            client::Client::connect_with_limit_and_ca(
                &endpoint,
                &token,
                maximum_response_bytes,
                ca_certificate_pem.as_ref(),
            ),
            cancellation_state(cancellation),
        )
        .await?;
        Ok(Self::from_client(inner))
    }

    /// Returns the native bridge package version.
    #[napi]
    pub fn version() -> String {
        PACKAGE_VERSION.to_owned()
    }

    /// Returns the transport selected by the canonical Rust client.
    #[napi]
    pub fn transport(&self) -> String {
        match self.inner.transport() {
            client::Transport::Grpc => "grpc".to_owned(),
            client::Transport::Http => "http".to_owned(),
        }
    }

    /// Lists model capabilities through the negotiated Rust transport.
    #[napi(js_name = "listModels")]
    pub async fn list_models(
        &self,
        request: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Buffer> {
        let request = decode_wire::<wire::ListModelsRequest>(&request, "list_models request")?;
        let response = cancellable(self.inner.list(&request), cancellation_state(cancellation)).await?;
        encode_wire(&response, "list_models response")
    }

    /// Creates one durable inference context.
    #[napi(js_name = "createContext")]
    pub async fn create_context(
        &self,
        request: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Buffer> {
        let request = decode_wire::<wire::CreateContextRequest>(&request, "create_context request")?;
        let response = cancellable(self.inner.create_context(&request), cancellation_state(cancellation)).await?;
        encode_wire(&response, "create_context response")
    }

    /// Inspects one durable inference context.
    #[napi(js_name = "inspectContext")]
    pub async fn inspect_context(
        &self,
        request: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Buffer> {
        let request = decode_wire::<wire::InspectContextRequest>(&request, "inspect_context request")?;
        let response = cancellable(self.inner.inspect_context(&request), cancellation_state(cancellation)).await?;
        encode_wire(&response, "inspect_context response")
    }

    /// Applies one context mutation.
    #[napi(js_name = "mutateContext")]
    pub async fn mutate_context(
        &self,
        request: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Buffer> {
        let request = decode_wire::<wire::MutateContextRequest>(&request, "mutate_context request")?;
        let response = cancellable(self.inner.mutate_context(&request), cancellation_state(cancellation)).await?;
        encode_wire(&response, "mutate_context response")
    }

    /// Retains one warm context.
    #[napi(js_name = "retainWarm")]
    pub async fn retain_warm(
        &self,
        request: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Buffer> {
        let request = decode_wire::<wire::RetainWarmRequest>(&request, "retain_warm request")?;
        let response = cancellable(self.inner.retain_warm(&request), cancellation_state(cancellation)).await?;
        encode_wire(&response, "retain_warm response")
    }

    /// Inspects one retained warm context.
    #[napi(js_name = "inspectWarm")]
    pub async fn inspect_warm(
        &self,
        request: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Buffer> {
        let request = decode_wire::<wire::InspectWarmRequest>(&request, "inspect_warm request")?;
        let response = cancellable(self.inner.inspect_warm(&request), cancellation_state(cancellation)).await?;
        encode_wire(&response, "inspect_warm response")
    }

    /// Renews one retained warm context.
    #[napi(js_name = "renewWarm")]
    pub async fn renew_warm(
        &self,
        request: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Buffer> {
        let request = decode_wire::<wire::RenewWarmRequest>(&request, "renew_warm request")?;
        let response = cancellable(self.inner.renew_warm(&request), cancellation_state(cancellation)).await?;
        encode_wire(&response, "renew_warm response")
    }

    /// Releases one retained warm context.
    #[napi(js_name = "releaseWarm")]
    pub async fn release_warm(
        &self,
        request: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Buffer> {
        let request = decode_wire::<wire::ReleaseWarmRequest>(&request, "release_warm request")?;
        let response = cancellable(self.inner.release_warm(&request), cancellation_state(cancellation)).await?;
        encode_wire(&response, "release_warm response")
    }

    /// Starts one recoverable inference run.
    #[napi(js_name = "generateRun")]
    pub async fn generate_run(
        &self,
        request: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Buffer> {
        let request = decode_wire::<wire::GenerateRunRequest>(&request, "generate_run request")?;
        let response = cancellable(self.inner.generate_run(&request), cancellation_state(cancellation)).await?;
        encode_wire(&response, "generate_run response")
    }

    /// Inspects one inference run.
    #[napi(js_name = "inspectRun")]
    pub async fn inspect_run(
        &self,
        request: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Buffer> {
        let request = decode_wire::<wire::InspectRunRequest>(&request, "inspect_run request")?;
        let response = cancellable(self.inner.inspect_run(&request), cancellation_state(cancellation)).await?;
        encode_wire(&response, "inspect_run response")
    }

    /// Collects the ordered events for one inference run.
    #[napi(js_name = "watchRun")]
    pub async fn watch_run(
        &self,
        request: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Vec<Buffer>> {
        let request = decode_wire::<wire::WatchRunRequest>(&request, "watch_run request")?;
        let events = cancellable(self.inner.watch_run(&request), cancellation_state(cancellation)).await?;
        events
            .iter()
            .map(|event| encode_wire(event, "watch_run response"))
            .collect()
    }

    /// Cancels one inference run idempotently.
    #[napi(js_name = "cancelRun")]
    pub async fn cancel_run(
        &self,
        request: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Buffer> {
        let request = decode_wire::<wire::InspectRunRequest>(&request, "cancel_run request")?;
        let response = cancellable(self.inner.cancel_run(&request), cancellation_state(cancellation)).await?;
        encode_wire(&response, "cancel_run response")
    }

    /// Creates one durable evaluation.
    #[napi(js_name = "createEvaluation")]
    pub async fn create_evaluation(
        &self,
        request: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Buffer> {
        let request = decode_wire::<wire::CreateEvaluationRequest>(&request, "create_evaluation request")?;
        let response = cancellable(self.inner.create_evaluation(&request), cancellation_state(cancellation)).await?;
        encode_wire(&response, "create_evaluation response")
    }

    /// Inspects one durable evaluation.
    #[napi(js_name = "inspectEvaluation")]
    pub async fn inspect_evaluation(
        &self,
        request: Buffer,
        cancellation: Option<&NativeInferenceCancellation>,
    ) -> Result<Buffer> {
        let request = decode_wire::<wire::InspectEvaluationRequest>(&request, "inspect_evaluation request")?;
        let response = cancellable(self.inner.inspect_evaluation(&request), cancellation_state(cancellation)).await?;
        encode_wire(&response, "inspect_evaluation response")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protobuf_boundary_preserves_empty_request() -> Result<()> {
        let request = wire::ListModelsRequest::default();
        let bytes = encode_wire(&request, "list_models request")?;
        let decoded = decode_wire::<wire::ListModelsRequest>(&bytes, "list_models request")?;
        assert_eq!(decoded, request);
        Ok(())
    }

    #[test]
    fn malformed_protobuf_is_rejected_at_the_boundary() -> Result<()> {
        let Err(error) = decode_wire::<wire::ListModelsRequest>(
            &Buffer::from(vec![0xff]),
            "list_models request",
        ) else {
            return Err(native_error(
                "malformed protobuf test",
                "decoder accepted invalid bytes",
            ));
        };
        assert!(error.to_string().contains("list_models request"));
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_drops_pending_operation() -> Result<()> {
        let cancellation = NativeInferenceCancellation::new();
        let pending = tokio::spawn(cancellable(
            std::future::pending::<std::result::Result<(), client::Error>>(),
            cancellation_state(Some(&cancellation)),
        ));
        cancellation.cancel();
        let result = pending
            .await
            .map_err(|join| native_error("cancellation test", join))?;
        let Err(error) = result else {
            return Err(native_error(
                "cancellation test",
                "cancelled operation completed successfully",
            ));
        };
        assert!(error.to_string().contains("Inference operation cancelled"));
        Ok(())
    }
}
