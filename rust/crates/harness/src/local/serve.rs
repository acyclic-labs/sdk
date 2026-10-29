//! The default local composition of the shared typed server entry point.
use super::{LocalHarness, LocalHarnessConfig, LocalStream};
use crate::{
    Error, Result,
    server::{Composition, Hosted, Verifier},
};
use acyclic_fs::LocalObjectBackend;
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

impl Composition for LocalHarnessConfig {
    type Stream = LocalStream;
    type Objects = LocalObjectBackend;
    type Owner = Arc<LocalHarness>;
    type Error = Error;

    fn open(
        self,
        verifier: &Arc<dyn Verifier>,
    ) -> BoxProviderFuture<'static, Result<Hosted<LocalStream, LocalObjectBackend, Self::Owner>>>
    {
        let config = self.served(Arc::clone(verifier));
        Box::pin(async move {
            let local = Arc::new(LocalHarness::open(config).await?);
            local.agents().await?;
            let server = local
                .agents
                .get()
                .ok_or_else(|| Error::Conflict("agents are not hosted".into()))?
                .server()
                .clone();
            let changes = server.bound().changes().map_err(super::storage)?;
            Ok(Hosted::new(
                server,
                local.clock.clone(),
                local,
                Some(Arc::new(changes)),
            ))
        })
    }
}
