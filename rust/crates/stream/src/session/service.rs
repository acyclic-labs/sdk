use super::*;
use tonic::{Request, Response, Status, Streaming};

/// Thin platform gRPC adapter for one provider's multiplexed follow sessions.
pub struct Service<P> {
    provider: Arc<P>,
    authority: Arc<Authority>,
    node: String,
    limits: Limits,
    resources: Option<Arc<dyn ResourceProvider>>,
    calls: Option<Arc<dyn CallProvider>>,
}
impl<P> Service<P> {
    /// Binds an authenticated host's provider and node identity.
    pub fn new(provider: Arc<P>, authority: Arc<Authority>, node: String, limits: Limits) -> Self {
        Self {
            provider,
            authority,
            node,
            limits,
            resources: None,
            calls: None,
        }
    }

    /// Installs owning product dispatchers; native and browser hosts use the
    /// same session admission and result credit, with no private control route.
    pub fn with_products(
        mut self,
        resources: Option<Arc<dyn ResourceProvider>>,
        calls: Arc<dyn CallProvider>,
    ) -> Self {
        self.resources = resources;
        self.calls = Some(calls);
        self
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
        let session = serve_on_with_products(
            Arc::clone(&self.provider),
            Arc::clone(&self.authority),
            self.resources.clone(),
            self.calls.clone(),
            self.node.clone(),
            self.limits,
            incoming,
            Arc::new(crate::runtime::PlatformExecutor),
        )
        .map_err(|error| Status::invalid_argument(error.code()))?;
        Ok(Response::new(
            session
                .map(|frame| frame.map_err(|error| Status::failed_precondition(error.code())))
                .boxed(),
        ))
    }
}
