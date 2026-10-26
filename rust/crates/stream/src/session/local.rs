//! In-process platform I/O over the same multiplexed session implementation.
use super::{Client, Limits, ResourceProvider, transport};
use crate::runtime::Executor;
use crate::{BoxProviderFuture, StreamError, StreamProvider};
use futures::StreamExt as _;
use std::sync::Arc;

struct Local<P: ?Sized> {
    provider: Arc<P>,
    executor: Arc<dyn Executor>,
    authority: Arc<super::Authority>,
    resources: Option<Arc<dyn ResourceProvider>>,
}
impl<P: StreamProvider + ?Sized> transport::Transport for Local<P> {
    fn session(
        &self,
        source: transport::Requests,
    ) -> BoxProviderFuture<'_, Result<transport::Responses, StreamError>> {
        Box::pin(async move {
            super::serve_on_with_resources(
                self.provider.clone(),
                self.authority.clone(),
                self.resources.clone(),
                "local".into(),
                Limits::default(),
                Box::pin(source.map(Ok)),
                self.executor.clone(),
            )
        })
    }
}

impl Client {
    /// Shares the platform pool with a caller-selected in-process provider.
    /// The provider supplies authority; no socket, daemon or second protocol runs.
    pub fn in_process<P: StreamProvider + ?Sized>(
        provider: Arc<P>,
        authority: Arc<super::Authority>,
        executor: Arc<dyn Executor>,
    ) -> Self {
        Self::local(provider, authority, None, executor)
    }

    /// The in-process pool whose product follows, such as `fs:/` filesystem
    /// changes, are resolved by their owning provider, exactly as a node
    /// session serving the same resources would.
    pub fn in_process_with_resources<P: StreamProvider + ?Sized>(
        provider: Arc<P>,
        authority: Arc<super::Authority>,
        resources: Arc<dyn ResourceProvider>,
        executor: Arc<dyn Executor>,
    ) -> Self {
        Self::local(provider, authority, Some(resources), executor)
    }

    fn local<P: StreamProvider + ?Sized>(
        provider: Arc<P>,
        authority: Arc<super::Authority>,
        resources: Option<Arc<dyn ResourceProvider>>,
        executor: Arc<dyn Executor>,
    ) -> Self {
        let endpoint: transport::Endpoint = Arc::new(Local {
            provider,
            executor: executor.clone(),
            authority,
            resources,
        });
        let local = endpoint.clone();
        Self::with_transport(
            endpoint,
            Arc::new(move |node| {
                let local = local.clone();
                Box::pin(async move {
                    if node.node_id != "local" {
                        return Err(StreamError::Unsupported);
                    }
                    Ok(local)
                })
            }),
            executor,
        )
    }
}
