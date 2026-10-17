//! In-process platform I/O over the same multiplexed session implementation.
use super::{Client, Limits, runtime::Executor, transport};
use crate::{BoxProviderFuture, StreamError, StreamProvider};
use futures::StreamExt as _;
use std::sync::Arc;

struct Local<P: ?Sized>(Arc<P>, Arc<dyn Executor>);
impl<P: StreamProvider + ?Sized> transport::Transport for Local<P> {
    fn session(
        &self,
        source: transport::Requests,
    ) -> BoxProviderFuture<'_, Result<transport::Responses, StreamError>> {
        Box::pin(async move {
            super::serve_on(
                self.0.clone(),
                "local".into(),
                Limits::default(),
                Box::pin(source.map(Ok)),
                self.1.clone(),
            )
        })
    }
}

impl Client {
    /// Shares the platform pool with a caller-selected in-process provider.
    /// The provider supplies authority; no socket, daemon or second protocol runs.
    pub fn in_process<P: StreamProvider + ?Sized>(
        provider: Arc<P>,
        executor: Arc<dyn Executor>,
    ) -> Self {
        let endpoint: transport::Endpoint = Arc::new(Local(provider, executor.clone()));
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
