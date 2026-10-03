//! N-API bridge for the canonical Rust remote Stream gRPC provider.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use acyclic_stream::{
    AppendOutcome, AppendRequest, ForkReceipt, ForkRequest, ReadRequest, StreamError, StreamPath,
    StreamProvider,
    grpc::{self, Client},
};
use bytes::Bytes;
use futures::StreamExt as _;
use napi::bindgen_prelude::{Buffer, Error, Result, Status};
use napi_derive::napi;
use tokio::sync::Notify;

const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");

fn native_error(context: &str, error: impl std::fmt::Display) -> Error {
    Error::new(Status::GenericFailure, format!("{context}: {error}"))
}

fn parse_u64(value: &str, field: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .map_err(|error| native_error(field, error))
}

fn stream_error(error: StreamError) -> Error {
    native_error("Stream operation failed", error)
}

fn connect_error(error: grpc::ConnectError) -> Error {
    native_error("Stream connection failed", error)
}

fn id(value: &[u8]) -> Buffer {
    Buffer::from(value.to_vec())
}

fn record(value: acyclic_stream::Record) -> NativeRecord {
    NativeRecord {
        sequence: value.sequence.to_string(),
        value: Buffer::from(value.value.to_vec()),
        commit_id: id(value.commit_id.as_bytes()),
        committed_at_micros: value.committed_at_micros.to_string(),
    }
}

fn fork_receipt(value: ForkReceipt) -> NativeForkReceipt {
    NativeForkReceipt {
        source: value.source.to_string(),
        destination: value.destination.to_string(),
        forked_at: value.forked_at.to_string(),
        tail: value.tail.to_string(),
        commit_id: id(value.commit_id.as_bytes()),
    }
}

/// Inputs for a native remote Stream connection.
#[napi(object)]
pub struct NativeStreamConnectOptions {
    /// Independently reachable HTTPS endpoints. The canonical Rust client bounds this at 16.
    pub endpoints: Vec<String>,
    /// Account-bound bearer credential.
    pub bearer_token: String,
    /// Optional caller-pinned private CA certificate in PEM form.
    pub ca_certificate_pem: Option<Buffer>,
}

/// Canonical append outcome. `committed` selects the receipt fields or `actual_tail`.
#[napi(object)]
pub struct NativeAppendResult {
    /// Whether the batch was committed.
    pub committed: bool,
    /// First sequence when committed.
    pub start: Option<String>,
    /// Exclusive end sequence when committed.
    pub end: Option<String>,
    /// Resulting tail when committed.
    pub tail: Option<String>,
    /// Actual tail when the caller's CAS precondition conflicted.
    pub actual_tail: Option<String>,
    /// Immutable envelope identity when committed.
    pub commit_id: Option<Buffer>,
}

/// One canonical Stream record with precision-safe sequence values.
#[napi(object)]
pub struct NativeRecord {
    /// Zero-based sequence as a decimal string.
    pub sequence: String,
    /// Opaque record bytes.
    pub value: Buffer,
    /// Immutable envelope identity.
    pub commit_id: Buffer,
    /// Replicated commit time in Unix microseconds as a decimal string.
    pub committed_at_micros: String,
}

/// Result of a finite read or a cancelled follow.
#[napi(object)]
pub struct NativeRecordBatch {
    /// Records yielded before the operation ended or was cancelled.
    pub records: Vec<NativeRecord>,
    /// Whether the caller's cancellation handle ended the operation.
    pub cancelled: bool,
}

/// One canonical direct child path.
#[napi(object)]
pub struct NativeChild {
    /// Permanent child path.
    pub path: String,
}

/// A native cancellation handle for a Rust follow operation.
#[napi]
pub struct NativeStreamCancellation {
    cancelled: Arc<AtomicBool>,
    wake: Arc<Notify>,
}

#[napi]
impl NativeStreamCancellation {
    /// Creates a cancellation handle in the non-cancelled state.
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            wake: Arc::new(Notify::new()),
        }
    }

    /// Wakes and cancels every Rust follow operation using this handle.
    #[napi]
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.wake.notify_waiters();
    }

    /// Whether cancellation has been requested.
    #[napi(getter)]
    pub fn cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Native remote Stream client backed directly by `acyclic_stream::grpc::Client`.
#[napi]
pub struct NativeStreamClient {
    inner: Arc<Client>,
}

#[napi]
impl NativeStreamClient {
    /// Connects to the canonical Rust gRPC endpoint pool.
    #[napi(factory)]
    pub async fn connect(options: NativeStreamConnectOptions) -> Result<Self> {
        let client = match options.ca_certificate_pem {
            Some(certificate) => {
                Client::connect_endpoints_with_ca_certificate(
                    options.endpoints,
                    options.bearer_token,
                    certificate.as_ref(),
                )
                .await
            }
            None => Client::connect_endpoints(options.endpoints, options.bearer_token).await,
        }
        .map_err(connect_error)?;
        Ok(Self {
            inner: Arc::new(client),
        })
    }

    /// Returns the native bridge package version.
    #[napi]
    pub fn version() -> String {
        PACKAGE_VERSION.to_owned()
    }

    /// Returns the canonical endpoint-pool bound.
    #[napi]
    pub fn max_endpoints() -> u32 {
        grpc::MAX_ENDPOINTS as u32
    }

    /// Reads the current tail through the Rust provider.
    #[napi]
    pub async fn tail(&self, path: String) -> Result<String> {
        let path = StreamPath::new(path).map_err(stream_error)?;
        self.inner
            .tail(path)
            .await
            .map(|value| value.to_string())
            .map_err(stream_error)
    }

    /// Atomically appends records through the Rust provider.
    #[napi]
    pub async fn append(
        &self,
        path: String,
        records: Vec<Buffer>,
        if_tail: Option<String>,
        idempotency_key: Option<Buffer>,
    ) -> Result<NativeAppendResult> {
        let request = AppendRequest {
            path: StreamPath::new(path).map_err(stream_error)?,
            records: records
                .into_iter()
                .map(|value| Bytes::from(value.to_vec()))
                .collect(),
            if_tail: if_tail
                .as_deref()
                .map(|value| parse_u64(value, "if_tail"))
                .transpose()?,
            idempotency_key: idempotency_key
                .map(|value| acyclic_stream::IdempotencyKey::new(Bytes::from(value.to_vec())))
                .transpose()
                .map_err(stream_error)?,
        };
        match self.inner.append(request).await.map_err(stream_error)? {
            AppendOutcome::Committed(receipt) => Ok(NativeAppendResult {
                committed: true,
                start: Some(receipt.start.to_string()),
                end: Some(receipt.end.to_string()),
                tail: Some(receipt.tail.to_string()),
                actual_tail: None,
                commit_id: Some(id(receipt.commit_id.as_bytes())),
            }),
            AppendOutcome::TailConflict { actual_tail } => Ok(NativeAppendResult {
                committed: false,
                start: None,
                end: None,
                tail: None,
                actual_tail: Some(actual_tail.to_string()),
                commit_id: None,
            }),
        }
    }

    /// Reads a bounded finite page through the Rust provider.
    #[napi]
    pub async fn read(&self, path: String, from: String, limit: u32) -> Result<NativeRecordBatch> {
        let request = ReadRequest {
            path: StreamPath::new(path).map_err(stream_error)?,
            from: parse_u64(&from, "from")?,
            limit,
        };
        let mut records = self.inner.read(request).await.map_err(stream_error)?;
        let mut values = Vec::new();
        while let Some(value) = records.next().await {
            values.push(record(value.map_err(stream_error)?));
        }
        Ok(NativeRecordBatch {
            records: values,
            cancelled: false,
        })
    }

    /// Follows a path and cancels by dropping the canonical Rust stream.
    #[napi]
    pub async fn follow(
        &self,
        path: String,
        from: String,
        cancellation: Option<&NativeStreamCancellation>,
    ) -> Result<NativeRecordBatch> {
        let path = StreamPath::new(path).map_err(stream_error)?;
        let from = parse_u64(&from, "from")?;
        let cancellation =
            cancellation.map(|value| (Arc::clone(&value.cancelled), Arc::clone(&value.wake)));
        let mut records = self.inner.follow(path, from).await.map_err(stream_error)?;
        let mut values = Vec::new();
        loop {
            if cancellation
                .as_ref()
                .is_some_and(|(cancelled, _)| cancelled.load(Ordering::Acquire))
            {
                return Ok(NativeRecordBatch {
                    records: values,
                    cancelled: true,
                });
            }
            let next = match cancellation.as_ref() {
                Some((_, wake)) => {
                    tokio::select! {
                        value = records.next() => value,
                        () = wake.notified() => return Ok(NativeRecordBatch { records: values, cancelled: true }),
                    }
                }
                None => records.next().await,
            };
            let Some(value) = next else {
                return Ok(NativeRecordBatch {
                    records: values,
                    cancelled: false,
                });
            };
            values.push(record(value.map_err(stream_error)?));
        }
    }

    /// Forks an immutable prefix through the Rust provider.
    #[napi]
    pub async fn fork(
        &self,
        source: String,
        destination: String,
        at_tail: Option<String>,
        idempotency_key: Option<Buffer>,
    ) -> Result<NativeForkReceipt> {
        let request = ForkRequest {
            source: StreamPath::new(source).map_err(stream_error)?,
            destination: StreamPath::new(destination).map_err(stream_error)?,
            at_tail: at_tail
                .as_deref()
                .map(|value| parse_u64(value, "at_tail"))
                .transpose()?,
            idempotency_key: idempotency_key
                .map(|value| acyclic_stream::IdempotencyKey::new(Bytes::from(value.to_vec())))
                .transpose()
                .map_err(stream_error)?,
        };
        self.inner
            .fork(request)
            .await
            .map(fork_receipt)
            .map_err(stream_error)
    }

    /// Lists one fixed-snapshot direct-child page through the Rust provider.
    #[napi]
    pub async fn children(&self, parent: Option<String>, limit: u32) -> Result<Vec<NativeChild>> {
        let parent = parent
            .as_deref()
            .map(StreamPath::new)
            .transpose()
            .map_err(stream_error)?;
        let mut children = self
            .inner
            .children(acyclic_stream::ChildrenRequest { parent, limit })
            .await
            .map_err(stream_error)?;
        let mut values = Vec::new();
        while let Some(value) = children.next().await {
            values.push(NativeChild {
                path: value.map_err(stream_error)?.path.to_string(),
            });
        }
        Ok(values)
    }
}

/// Native bridge version and canonical bounds for package qualification.
#[napi]
pub fn native_stream_capabilities() -> NativeStreamCapabilities {
    NativeStreamCapabilities {
        version: PACKAGE_VERSION.to_owned(),
        max_endpoints: grpc::MAX_ENDPOINTS as u32,
        max_endpoint_uri_bytes: grpc::MAX_ENDPOINT_URI_BYTES as u32,
        max_ca_certificate_bytes: grpc::MAX_CA_CERTIFICATE_BYTES as u32,
        operation_deadline_ms: 10_000,
        endpoint_attempt_timeout_ms: 1_000,
        follow_attempt_timeout_ms: 500,
        retry_delay_ms: 10,
    }
}

/// Rust-owned native remote Stream capabilities.
#[napi(object)]
pub struct NativeStreamCapabilities {
    /// Package version.
    pub version: String,
    /// Maximum endpoint pool size.
    pub max_endpoints: u32,
    /// Maximum endpoint URI width.
    pub max_endpoint_uri_bytes: u32,
    /// Maximum private CA bundle width.
    pub max_ca_certificate_bytes: u32,
    /// Canonical unary operation deadline.
    pub operation_deadline_ms: u32,
    /// Canonical endpoint attempt timeout.
    pub endpoint_attempt_timeout_ms: u32,
    /// Canonical follow endpoint attempt timeout.
    pub follow_attempt_timeout_ms: u32,
    /// Canonical retry delay.
    pub retry_delay_ms: u32,
}

/// Result of an immutable-prefix fork.
#[napi(object)]
pub struct NativeForkReceipt {
    /// Source path.
    pub source: String,
    /// Destination path.
    pub destination: String,
    /// Prefix sequence inherited by the destination.
    pub forked_at: String,
    /// Destination tail after the fork.
    pub tail: String,
    /// Immutable envelope identity.
    pub commit_id: Buffer,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connection_rejects_insecure_endpoint_before_network_io() {
        let error = match Client::connect_endpoints(["http://127.0.0.1:1"], "fixture").await {
            Ok(_) => {
                assert!(false, "HTTP endpoint must be rejected");
                return;
            }
            Err(error) => error,
        };
        assert!(matches!(error, grpc::ConnectError::InsecureEndpoint));
    }

    #[tokio::test]
    async fn connection_rejects_more_than_canonical_endpoint_bound() {
        let endpoints = (0..=grpc::MAX_ENDPOINTS)
            .map(|index| format!("https://endpoint-{index}.invalid"))
            .collect::<Vec<_>>();
        let error = match Client::connect_endpoints(endpoints, "fixture").await {
            Ok(_) => {
                assert!(false, "endpoint pool must be bounded");
                return;
            }
            Err(error) => error,
        };
        assert!(matches!(error, grpc::ConnectError::EndpointLimit));
    }

    #[tokio::test]
    async fn connection_rejects_empty_private_ca_before_network_io() {
        let error = match Client::connect_endpoints_with_ca_certificate(
            ["https://endpoint.invalid"],
            "fixture",
            [],
        )
        .await
        {
            Ok(_) => {
                assert!(false, "empty private CA must be rejected");
                return;
            }
            Err(error) => error,
        };
        assert!(matches!(error, grpc::ConnectError::InvalidCaCertificate));
    }

    #[test]
    fn cancellation_is_woken_without_touching_transport_policy() {
        let cancellation = NativeStreamCancellation::new();
        assert!(!cancellation.cancelled());
        cancellation.cancel();
        assert!(cancellation.cancelled());
        assert_eq!(
            NativeStreamClient::max_endpoints(),
            grpc::MAX_ENDPOINTS as u32
        );
    }
}
