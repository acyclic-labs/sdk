//! Provider subscriptions end at the same accepted stop file as their owner.
use super::*;
use futures::StreamExt as _;
use http_body_util::{BodyStream, StreamBody};
use tonic::{body::Body, codegen::http};

pub(super) fn streaming(path: &str) -> bool {
    matches!(path.rsplit('/').next(), Some("Read" | "Follow" | "Children" | "Session"))
}

pub(super) fn close_on_stop(
    response: http::Response<Body>,
    stopped: impl Future<Output = Result<()>> + Send + 'static,
) -> http::Response<Body> {
    #[cfg(acyclic_harness_loom)]
    if model::omit_stop() {
        return response;
    }
    let (parts, body) = response.into_parts();
    // Preserve data and trailers without copying frames. Unary acknowledgements,
    // including the stop append itself, keep their original settlement path.
    http::Response::from_parts(parts, Body::new(StreamBody::new(BodyStream::new(body).take_until(stopped))))
}

#[cfg(all(test, not(acyclic_harness_loom)))]
#[test]
fn every_streaming_provider_method_has_a_shutdown_boundary() {
    let methods = [
        include_str!("../../../../stream/proto/stream/v1/stream.proto"),
        include_str!("../../../../stream/proto/stream/v1/session.proto"),
    ].into_iter().flat_map(|source| source.lines())
        .filter(|line| line.contains("rpc "))
        .map(|line| {
            let method = line.trim().strip_prefix("rpc ").unwrap().split('(').next().unwrap();
            (method, line.contains("returns (stream "))
        });
    for (method, expected) in methods {
        assert_eq!(streaming(&format!("/acyclic.stream.v1/Service/{method}")), expected, "{method}");
    }
}

#[cfg(acyclic_harness_loom)]
mod model;
