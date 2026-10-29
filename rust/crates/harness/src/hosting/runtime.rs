//! Native ownership of the shared Actors serving loop.
use super::Server;
use crate::{Error, Result};
use acyclic_fs::{AsyncObjectStore, distributed::AuthorityProvider};
use std::sync::Arc;
use tokio::sync::watch;

/// One running native application. Dropping it aborts its deliveries and closes
/// its Session lifetime. Delivery or runtime failure closes the same lifetime.
pub struct Runtime {
    task: tokio::task::JoinHandle<()>,
    lifetime: watch::Receiver<()>,
    failure: watch::Receiver<Option<String>>,
}

impl Runtime {
    /// Starts the application's existing Actors runtime, with no second loop or
    /// per-agent task. A failed delivery retains its queued input for recovery.
    pub fn start<P, O>(server: Arc<Server<P, O>>) -> Self
    where
        P: AuthorityProvider + 'static,
        O: AsyncObjectStore + 'static,
    {
        let (alive, lifetime) = watch::channel(());
        let (failure, mut failed) = watch::channel(None);
        let completion = failed.clone();
        let task = tokio::spawn(async move {
            tokio::select! {
                error = server.run(|actor, error| {
                    tracing::error!(?actor, %error, "actor delivery failed");
                    failure.send_replace(Some(error.to_string()));
                }) => { failure.send_replace(Some(error.to_string())); }
                _ = failed.changed() => {}
            }
            // The lifetime closes after cancellation drops the active delivery.
            drop(alive);
        });
        Self {
            task,
            lifetime,
            failure: completion,
        }
    }

    /// The event consumed by the canonical Session engine. Producer loss, even
    /// before a client connects, closes the Session instead of parking forever.
    #[must_use]
    pub fn lifetime(&self) -> watch::Receiver<()> {
        self.lifetime.clone()
    }

    /// Waits for the actual runtime failure or the loss of its task producer.
    /// Each observer shares the same retained failure; no polling or history.
    pub fn completion(&self) -> impl Future<Output = Result<()>> + Send + 'static {
        let mut failure = self.failure.clone();
        async move {
            let reason = match failure.wait_for(Option::is_some).await {
                Ok(reason) => reason.as_ref().map_or_else(
                    || "Actors runtime completion producer closed".into(),
                    Clone::clone,
                ),
                Err(_) => "Actors runtime completion producer closed".into(),
            };
            Err(Error::Storage(reason))
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.task.abort();
    }
}
