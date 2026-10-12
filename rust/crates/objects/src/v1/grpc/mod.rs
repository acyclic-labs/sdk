//! Authenticated, bounded gRPC client for logical Objects v1.
use super::response::object_info as info;
use super::{
    Download, Error, NativeObjectsError, Object, ObjectsProvider, UploadBody, request, response,
    upload, wire,
};
use crate::obs;
use acyclic_stream::OwnedBytes;
use futures::{FutureExt, StreamExt, TryFutureExt, stream};
use prost::Message;
use tonic::{
    Request,
    metadata::{Ascii, MetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity},
};

const MESSAGE_BYTES: usize = 16 * 1024 * 1024;
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_TRANSFER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1800);

use super::{
    Guarded, MAX_BEARER_TOKEN_BYTES, MAX_PEM_BYTES, NativeTransportErrors, ObjectsNativeFuture,
    ObjectsNativeRequest, ObjectsPutReceipt, native_future,
};
use std::sync::Arc;

/// Client configuration or connection failure.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// Endpoint, credential, or private CA violates the client contract.
    #[error("invalid Objects gRPC configuration")]
    InvalidConfiguration,
    /// Connection could not be established.
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    /// The platform certificate verifier could not be initialized.
    #[error("Objects platform certificate verifier: {0}")]
    PlatformVerifier(#[source] rustls::Error),
}

/// Logical Objects client. Every request carries a sensitive bearer credential.
#[derive(Clone)]
pub struct GrpcObjects {
    channel: Channel,
    authorization: MetadataValue<Ascii>,
    transfer_timeout: std::time::Duration,
}

mod connection;
mod download_stream;
mod error;
mod frames;
mod provider;
mod upload_stream;
mod wire_body;
use error::{bounded, error_from_status, ok, protocol};
use frames::*;
use wire_body::{ObjectsChannel, PreparedWireBody};
