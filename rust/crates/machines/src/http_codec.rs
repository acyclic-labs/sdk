//! Machines POST routes, relative to `/v1/`, owned by the SDK.
//!
//! Server-streaming routes use the same NDJSON convention as Inference watch:
//! one canonical Protobuf JSON message per line (`application/x-ndjson`).
//! The adapter encodes each event here and appends one newline. Producer loss
//! before Destroyed is a stream failure, never a fabricated terminal event.
use std::sync::OnceLock;

use prost::Message as _;
use prost_reflect::{DescriptorPool, MethodDescriptor};

use crate::MachinesMethod;

/// One SDK-owned route; Edge dispatches with the canonical Machines method.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpRoute {
    pub path: &'static str,
    pub method: MachinesMethod,
}

/// The sole Machines HTTP/JSON route table. Streaming comes from the descriptor.
pub const HTTP_ROUTES: &[HttpRoute] = &[
    HttpRoute {
        path: "machines/read-results",
        method: MachinesMethod::ReadResults,
    },
    HttpRoute {
        path: "machines/follow-events",
        method: MachinesMethod::FollowEvents,
    },
];

impl HttpRoute {
    /// Original RPC descriptor, including its server-streaming flag.
    pub fn descriptor(self) -> Result<MethodDescriptor, &'static str> {
        static POOL: OnceLock<Option<DescriptorPool>> = OnceLock::new();
        let pool = POOL
            .get_or_init(|| DescriptorPool::decode(crate::FILE_DESCRIPTOR_SET).ok())
            .as_ref()
            .ok_or("Machines descriptor is unavailable")?;
        let route = self.method.authorization().route;
        let (service, method) = route
            .trim_start_matches('/')
            .rsplit_once('/')
            .ok_or("Machines method route is invalid")?;
        pool.get_service_by_name(service)
            .and_then(|service| service.methods().find(|value| value.name() == method))
            .ok_or("Machines method is unavailable")
    }
}

fn route(path: &str) -> Result<MethodDescriptor, &'static str> {
    HTTP_ROUTES
        .iter()
        .find(|route| route.path == path)
        .ok_or("unknown Machines HTTP route")?
        .descriptor()
}

/// Decodes canonical Protobuf JSON under the adapter's per-frame protective bound.
/// Authentication and semantic admission still belong to the service.
pub fn decode_http_request(
    path: &str,
    json: &[u8],
    maximum: usize,
) -> Result<Vec<u8>, acyclic_protobuf_json::Error> {
    let method = route(path).map_err(|_| acyclic_protobuf_json::Error::Wire)?;
    acyclic_protobuf_json::decode(method.input(), json, maximum)
        .map(|message| message.encode_to_vec())
}

/// Encodes a response or one event, without its NDJSON newline, as Inference does.
/// The adapter's writer owns the same protective frame bound as other JSON RPCs.
pub fn encode_http_response(
    path: &str,
    bytes: &[u8],
    writer: &mut impl std::io::Write,
) -> Result<(), acyclic_protobuf_json::Error> {
    let method = route(path).map_err(|_| acyclic_protobuf_json::Error::Wire)?;
    acyclic_protobuf_json::encode_to_writer(method.output(), bytes, writer)
}
