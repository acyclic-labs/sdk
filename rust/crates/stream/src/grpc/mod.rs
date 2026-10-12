//! Authenticated gRPC adapter for the canonical Stream provider contract.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use prost::Message;
use thiserror::Error;
use tonic::{
    Code, Request, Response, Status,
    metadata::{Ascii, MetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
#[cfg(test)]
use tracing::Instrument as _;
use tracing::field::Empty;
#[cfg(test)]
use tracing::instrument::WithSubscriber as _;

use crate::obs;
use crate::wire_codec::{
    append_outcome_from_wire, append_outcome_to_wire, commit_id, commit_outcome_from_wire,
    commit_outcome_to_wire, append_from_wire, commit_from_wire, condition_wire, envelope_from_wire, envelope_to_wire,
    fork_receipt_to_wire, mutation_wire, observation_from_wire, observation_wire,
    optional_key, path, read_response_records, read_response_wire, record, record_wire,
};
use crate::{
    AppendOutcome, AppendRequest, Child, ChildStream, ChildrenPage, ChildrenPageRequest,
    ChildrenRequest, CommitId, CommitOutcome, CommitRequest, CommittedEnvelope, ForkReceipt,
    ForkRequest, IdempotencyKey, IdempotencyObservation, ReadRequest, Record, RecordStream,
    StreamBounds, StreamError, StreamPath, StreamProvider, wire,
};

const OPERATION_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);
const OPERATION_ENDPOINT_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const FOLLOW_ENDPOINT_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(10);
/// Maximum independently reachable endpoints in one operation or follow pool.
pub const MAX_ENDPOINTS: usize = 16;
/// Maximum canonical URI bytes accepted for one endpoint.
pub const MAX_ENDPOINT_URI_BYTES: usize = 2_048;
pub use crate::MAX_BEARER_TOKEN_BYTES;
pub use crate::MAX_CA_CERTIFICATE_BYTES;

/// Connection configuration failure.
#[derive(Debug, Error)]
pub enum ConnectError {
    /// Endpoint URI was invalid or the TLS connection failed.
    #[error("invalid or unavailable Stream endpoint: {0}")]
    Endpoint(#[from] tonic::transport::Error),
    /// Customer endpoints must use authenticated TLS.
    #[error("Stream endpoints must use https")]
    InsecureEndpoint,
    /// Bearer credential cannot be represented as HTTP metadata.
    #[error("invalid Stream bearer credential")]
    InvalidCredential,
    /// Caller-supplied private CA bundle is empty or exceeds its fixed bound.
    #[error("invalid Stream private CA certificate")]
    InvalidCaCertificate,
    /// The platform certificate verifier could not be initialized.
    #[error("Stream platform certificate verifier: {0}")]
    PlatformVerifier(#[source] rustls::Error),
    /// At least one independently reachable endpoint is required.
    #[error("at least one Stream endpoint is required")]
    NoEndpoints,
    /// Endpoint count or URI bytes exceed the fixed client bound.
    #[error("Stream endpoint pool exceeds its fixed bound")]
    EndpointLimit,
}

/// Authenticated remote provider.
#[derive(Clone)]
pub struct Client {
    channels: Arc<[Channel]>,
    authorization: MetadataValue<Ascii>,
    preferred: Arc<AtomicUsize>,
    acknowledge_deliveries: bool,
}

/// Thin server adapter from the canonical wire service to one provider.
pub struct Service<P> {
    provider: Arc<P>,
}

impl<P> Service<P> {
    /// Binds the generated server to one semantic provider.
    #[must_use]
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }
}

impl<P> Clone for Service<P> {
    fn clone(&self) -> Self {
        Self {
            provider: Arc::clone(&self.provider),
        }
    }
}

fn check_command_size<T: Message>(request: &T) -> Result<(), Status> {
    if request.encoded_len() > crate::MAX_COMMAND_BYTES {
        Err(error_status(&StreamError::LimitExceeded))
    } else {
        Ok(())
    }
}

mod connection;
mod calls;
mod provider;
mod records;
mod delivery;
mod server;
mod observation;
mod protocol;
use delivery::DeliveryAcknowledgements;
use observation::{Completion, decode_response, observe_body};
use protocol::*;
#[cfg(test)]
mod tests;
