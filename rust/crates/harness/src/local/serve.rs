//! The default local composition of the shared typed server entry point.
use super::{LocalHarness, LocalHarnessConfig, LocalStream};
use crate::{
    Error, Result,
    server::{Composition, Hosted},
};
use acyclic_fs::LocalObjectBackend;
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;

impl Composition for LocalHarnessConfig {
    type Stream = LocalStream;
    type Objects = LocalObjectBackend;
    type Owner = Arc<LocalHarness>;

    fn open(
        self,
    ) -> BoxProviderFuture<'static, Result<Hosted<LocalStream, LocalObjectBackend, Self::Owner>>>
    {
        Box::pin(async move {
            let local = Arc::new(LocalHarness::open(self.served()).await?);
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
