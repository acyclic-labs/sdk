//! Authenticated logical Objects HTTP gateway with bounded Protobuf JSON/NDJSON.
use super::{
    Download, Error, HTTP_ROUTES, NativeObjectsError, Object, ObjectsProvider, UploadBody, json,
    request, response, upload, wire,
};
use crate::obs;
use bytes::Bytes;
use futures::{StreamExt, stream};
use prost::Message;
use reqwest::{
    Body, Client, Response, Url,
    header::{AUTHORIZATION, HeaderValue},
};

/// One NDJSON line, bounded by the base64 of one compressed 8 MiB body frame.
const JSON_FRAME_BYTES: usize = 12 * 1024 * 1024;
const REQUEST_BYTES: usize = 16 * 1024 * 1024;

/// Authenticated native HTTP client for the logical Objects v1 gateway.
#[derive(Clone)]
pub struct HttpObjects {
    transport: Client,
    endpoint: Url,
    authorization: HeaderValue,
    maximum: usize,
}
use super::{
    MAX_BEARER_TOKEN_BYTES, MAX_PEM_BYTES, NativeTransportErrors, ObjectsNativeFuture,
    ObjectsNativeRequest, ObjectsPutReceipt, native_future,
};
use acyclic_stream::OwnedBytes;
use std::sync::Arc;

mod connection;
mod download_stream;
mod frames;
mod provider;
mod transport;
mod upload_stream;
use frames::{Frames, line, media_type};
