//! In-process platform I/O over the same multiplexed session implementation.
use super::{CallProvider, Client, Limits, Products, ResourceProvider, SocketProvider, transport};
use crate::runtime::Executor;
use crate::{BoxProviderFuture, StreamError, StreamProvider};
use futures::StreamExt as _;
use std::sync::Arc;

struct Local<P: ?Sized> {
    provider: Arc<P>,
    executor: Arc<dyn Executor>,
    authority: Arc<super::Authority>,
    products: Products,
    lifetime: Option<tokio::sync::watch::Receiver<()>>,
}
impl<P: StreamProvider + ?Sized> transport::Transport for Local<P> {
    fn session(
        &self,
        source: transport::Requests,
    ) -> BoxProviderFuture<'_, Result<transport::Responses, StreamError>> {
        Box::pin(async move {
            super::serve_on_with_products(
                self.provider.clone(),
                self.authority.clone(),
                self.products.clone(),
                "local".into(),
                Limits::default(),
                Box::pin(source.map(Ok)),
                self.executor.clone(),
                self.lifetime.clone().map(|mut lifetime| {
                    Box::pin(async move {
                        let _ = lifetime.changed().await;
                    }) as BoxProviderFuture<'static, ()>
                }),
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
        Self::local(provider, authority, Products::default(), executor, None)
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
        Self::local(
            provider,
            authority,
            Products {
                resources: Some(resources),
                ..Products::default()
            },
            executor,
            None,
        )
    }

    /// Uses the identical channel admission/credit/dispatch as a network host.
    /// When supplied, the host's lifetime closes outstanding calls and follows.
    pub fn in_process_with_products<P: StreamProvider + ?Sized>(
        provider: Arc<P>,
        authority: Arc<super::Authority>,
        resources: Option<Arc<dyn ResourceProvider>>,
        calls: Arc<dyn CallProvider>,
        executor: Arc<dyn Executor>,
        lifetime: Option<tokio::sync::watch::Receiver<()>>,
    ) -> Self {
        Self::local(
            provider,
            authority,
            Products {
                resources,
                calls: Some(calls),
                sockets: None,
            },
            executor,
            lifetime,
        )
    }

    /// The in-process pool whose actor sockets are hosted by `sockets`, on
    /// the same admission, credit and channel lifetime as a network host.
    pub fn in_process_with_sockets<P: StreamProvider + ?Sized>(
        provider: Arc<P>,
        authority: Arc<super::Authority>,
        calls: Option<Arc<dyn CallProvider>>,
        sockets: Arc<dyn SocketProvider>,
        executor: Arc<dyn Executor>,
        lifetime: Option<tokio::sync::watch::Receiver<()>>,
    ) -> Self {
        Self::local(
            provider,
            authority,
            Products {
                resources: None,
                calls,
                sockets: Some(sockets),
            },
            executor,
            lifetime,
        )
    }

    fn local<P: StreamProvider + ?Sized>(
        provider: Arc<P>,
        authority: Arc<super::Authority>,
        products: Products,
        executor: Arc<dyn Executor>,
        lifetime: Option<tokio::sync::watch::Receiver<()>>,
    ) -> Self {
        let endpoint: transport::Endpoint = Arc::new(Local {
            provider,
            executor: executor.clone(),
            authority,
            products,
            lifetime,
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
