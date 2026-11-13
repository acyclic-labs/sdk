//! Account routing composes the existing call, socket and actor owners.
use super::{AccountService, AdmissionPolicy, Error, NodeActor, NodeId, Paths};
use crate::{
    addressing::AccountId,
    domain::ActorLimits,
    guest::Handler as _,
    local::{Clock, Runtime, Sockets, Store},
    sockets::SocketOpen,
};
use acyclic_stream::{
    BoxProviderFuture, IdempotencyKey, StreamError, StreamPath, StreamProvider,
    runtime::Executor,
    session::{
        CallAuthorization, CallInput, CallProvider, CallTarget, PreparedSocketOpen, SocketProvider,
    },
};
use bytes::Bytes;
use std::sync::Arc;

/// The account's directory/router owns node actors, never their session roots.
/// Calls and sockets share the host's existing authenticated session endpoint;
/// one existing Actors dispatcher handles every enrolled node's lifecycle.
pub struct AccountNode<P: StreamProvider + ?Sized + 'static> {
    account: AccountId,
    paths: Paths,
    service: AccountService<P>,
    handler: Arc<NodeActor>,
    runtime: Runtime<P>,
    sockets: Arc<Sockets<P>>,
    calls: Option<Arc<dyn CallProvider>>,
    queries: Option<Arc<dyn SocketProvider>>,
}

impl<P: StreamProvider + ?Sized + 'static> AccountNode<P> {
    /// Open the account's node-actor store. It is separate from agent
    /// stores, so file, shell and same-node inbox traffic never wakes this host.
    /// The storage owner supplies the account's membership and reservation policy.
    pub async fn open(
        account: AccountId,
        provider: Arc<P>,
        paths: Paths,
        policy: Arc<dyn AdmissionPolicy>,
        limits: ActorLimits,
        clock: Arc<dyn Clock>,
        executor: Arc<dyn Executor>,
    ) -> Result<Self, Error> {
        let handler = Arc::new(NodeActor::new(limits).map_err(|_| Error::InvalidRecord)?);
        let root = StreamPath::new(format!("{}/node-actors", paths.0))?;
        let sockets = Arc::new(
            Sockets::open(
                Store::new(provider.clone(), root.clone()),
                executor,
                clock.clone(),
            )
            .await?,
        );
        let service = AccountService::new(account.clone(), provider.clone(), paths.clone(), policy);
        Ok(Self {
            account,
            paths,
            service,
            handler: handler.clone(),
            runtime: Runtime::new(Store::new(provider, root), handler, clock),
            sockets,
            calls: None,
            queries: None,
        })
    }

    /// Bind the deployment's ordinary products to the same session dispatcher.
    /// Routing is selected by its versioned protocol, never by trying one
    /// product's authorization and falling back after a refusal.
    #[must_use]
    pub fn with_products(
        mut self,
        calls: Arc<dyn CallProvider>,
        queries: Arc<dyn SocketProvider>,
    ) -> Self {
        self.calls = Some(calls);
        self.queries = Some(queries);
        self
    }

    /// Host-authorized node enrollment, before issuing its node credential.
    /// The original caller key must survive uncertain completion and reopen;
    /// replay cannot reset a live NodeLink or its generation.
    pub async fn enroll(&self, node: &NodeId, key: IdempotencyKey) -> Result<(), Error> {
        let request = self.handler.creation(
            self.runtime.store(),
            &self.paths,
            self.account.as_bytes(),
            node,
            key,
        )?;
        let outcome = self.runtime.store().provider().commit(request).await?;
        crate::local::PreparedPublish::settle(&outcome)?;
        Ok(())
    }

    /// Drive only node actors through the existing event-owned dispatcher.
    /// The serving host owns this future and drops it on endpoint shutdown.
    pub fn run(
        &self,
        report: impl Fn(&crate::addressing::ActorReference, &crate::local::Error),
    ) -> impl std::future::Future<Output = Result<std::convert::Infallible, crate::local::Error>>
    {
        self.runtime.run(report)
    }
}

impl<P: StreamProvider + ?Sized + 'static> CallProvider for AccountNode<P> {
    fn target(&self, invocation: &[u8]) -> Result<CallTarget, StreamError> {
        if self.service.handles(invocation) {
            self.service.target(invocation)
        } else {
            self.calls
                .as_ref()
                .ok_or(StreamError::Unsupported)?
                .target(invocation)
        }
    }

    fn call(
        &self,
        invocation: Bytes,
        authorization: CallAuthorization,
    ) -> BoxProviderFuture<'static, Result<Bytes, StreamError>> {
        if self.service.handles(&invocation) {
            self.service.call(invocation, authorization)
        } else {
            match &self.calls {
                Some(calls) => calls.call(invocation, authorization),
                None => Box::pin(async { Err(StreamError::Unsupported) }),
            }
        }
    }

    fn call_input(
        &self,
        invocation: Bytes,
        authorization: CallAuthorization,
        input: CallInput,
    ) -> BoxProviderFuture<'static, Result<Bytes, StreamError>> {
        if !self.service.handles(&invocation)
            && let Some(calls) = &self.calls
        {
            return calls.call_input(invocation, authorization, input);
        }
        Box::pin(async { Err(StreamError::Unsupported) })
    }
}

impl<P: StreamProvider + ?Sized + 'static> SocketProvider for AccountNode<P> {
    fn prepare(self: Arc<Self>, open: Bytes) -> Result<Box<dyn PreparedSocketOpen>, StreamError> {
        if let Ok(decoded) = SocketOpen::decode(&open)
            && self.handler.accepts_actor(&decoded.actor)
        {
            if decoded.actor.account() != &self.account {
                return Err(StreamError::AccessDenied);
            }
            return Ok(self.sockets.clone().prepare_decoded(decoded));
        }
        self.queries
            .clone()
            .ok_or(StreamError::Unsupported)?
            .prepare(open)
    }
}
