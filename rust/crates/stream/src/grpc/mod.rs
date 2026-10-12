//! Authenticated gRPC adapter for the canonical Stream provider contract.

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
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
    commit_outcome_to_wire, condition_from_wire, condition_wire, envelope_from_wire,
    envelope_to_wire, fork_receipt_to_wire, mutation_from_wire, mutation_wire,
    observation_from_wire, observation_wire, optional_key, path, read_response_records,
    read_response_wire, record, record_wire,
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

#[cfg(test)]
#[allow(
    clippy::indexing_slicing,
    reason = "fixture indexes and captured field lookups intentionally fail the test if expected evidence is absent"
)]
mod tests;

include!("connect_error.rs");

include!("client.rs");

include!("service.rs");

include!("protocol.rs");

include!("record_cursor.rs");

include!("active_records.rs");

include!("completion.rs");

include!("rpc_response.rs");

include!("observed_body.rs");
