//! Authenticated client for the generated Actors v1 service.

use crate::wire;
use acyclic_workers::BearerAuth;
pub use acyclic_workers::grpc::ConnectError;
use prost::Message;
use tonic::{Status, transport::Channel};
use tracing::field::Empty;

/// Generated client with account authentication on every request.
pub type Client = wire::actors_service_client::ActorsServiceClient<
    tonic::service::interceptor::InterceptedService<TracedChannel, BearerAuth>,
>;

/// Shared transport-status channel retaining the originating trace context.
pub type TracedChannel = acyclic_grpc_observability::TracedChannel<Channel>;

/// Statically named actors RPC span factory.
pub(crate) fn transport_span(rpc: Option<&str>) -> tracing::Span {
    tracing::trace_span!(
        "acyclic.actors.grpc.transport",
        rpc,
        rpc.code = Empty,
        outcome = Empty,
        outcome.scope = "grpc_transport",
        error.kind = Empty
    )
}
/// Connect trusting exactly `ca` when supplied, otherwise the platform's roots.
pub async fn connect(
    endpoint: &str,
    token: &str,
    ca: Option<&[u8]>,
) -> Result<Client, ConnectError> {
    let authorization = BearerAuth::new(token).map_err(|()| ConnectError::InvalidCredential)?;
    let channel = acyclic_workers::grpc::connect_channel(endpoint, ca).await?;
    Ok(
        wire::actors_service_client::ActorsServiceClient::with_interceptor(
            TracedChannel::new(channel, transport_span),
            authorization,
        )
        .max_decoding_message_size(16 * 1024 * 1024)
        .max_encoding_message_size(16 * 1024 * 1024),
    )
}

/// Decode the canonical semantic error carried in gRPC status details.
#[must_use]
pub fn error_detail(status: &Status) -> Option<wire::Error> {
    let detail = wire::Error::decode(status.details()).ok()?;
    (wire::ErrorCode::try_from(detail.code).ok()? != wire::ErrorCode::Unspecified).then_some(detail)
}
