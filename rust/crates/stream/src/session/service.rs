use super::*;
use tonic::{Request, Response, Status, Streaming};

/// Thin platform gRPC adapter for one provider's multiplexed follow sessions.
pub struct Service<P> {
    provider: Arc<P>,
    node: String,
    limits: Limits,
}
impl<P> Service<P> {
    /// Binds an authenticated host's provider and node identity.
    pub fn new(provider: Arc<P>, node: String, limits: Limits) -> Self {
        Self {
            provider,
            node,
            limits,
        }
    }
}
#[tonic::async_trait]
impl<P: StreamProvider> wire::stream_session_service_server::StreamSessionService for Service<P> {
    type SessionStream = BoxStream<'static, Result<wire::SessionFrame, Status>>;

    async fn session(
        &self,
        request: Request<Streaming<wire::SessionFrame>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let incoming = request
            .into_inner()
            .map(|frame| frame.map_err(|_| StreamError::Unavailable))
            .boxed();
        let session = serve(
            Arc::clone(&self.provider),
            self.node.clone(),
            self.limits,
            incoming,
        )
        .map_err(|error| Status::invalid_argument(error.code()))?;
        Ok(Response::new(
            session
                .map(|frame| frame.map_err(|error| Status::failed_precondition(error.code())))
                .boxed(),
        ))
    }
}
