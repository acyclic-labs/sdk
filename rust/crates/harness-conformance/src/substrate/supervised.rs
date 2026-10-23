//! Runtime-owned subscriptions, independent of attached scenario clients.
use super::*;
use acyclic_harness::package::Snapshot;
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};

type Completion = Shared<BoxFuture<'static, std::result::Result<Snapshot, String>>>;
struct Worker {
    completion: Completion,
    abort: tokio::task::AbortHandle,
}

/// Owns reference runtime workers until actor settlement or host shutdown.
pub struct Supervised<P: StreamProvider> {
    runtime: Arc<InProcessRuntime<P>>,
    workers: Mutex<std::collections::BTreeMap<(AgentId, u64), Worker>>,
}
impl<P: StreamProvider> Supervised<P> {
    /// Retains the runtime independently from each public request future.
    pub fn new(runtime: InProcessRuntime<P>) -> Self {
        Self {
            runtime: Arc::new(runtime),
            workers: Mutex::new(Default::default()),
        }
    }
    fn supervise(&self, placement: Placement) -> Result<Completion> {
        let mut workers = self
            .workers
            .lock()
            .map_err(|_| Error::Storage("fixture workers poisoned".into()))?;
        let key = (placement.actor, placement.epoch);
        if workers
            .get(&key)
            .is_some_and(|worker| matches!(worker.completion.peek(), Some(Err(_))))
        {
            workers.remove(&key);
        }
        let worker = workers.entry(key).or_insert_with(|| {
            let runtime = self.runtime.clone();
            let task =
                tokio::spawn(
                    async move { runtime.run(placement).await.map_err(|e| e.to_string()) },
                );
            let abort = task.abort_handle();
            let completion = async move { task.await.map_err(|e| e.to_string())? }
                .boxed()
                .shared();
            Worker { completion, abort }
        });
        Ok(worker.completion.clone())
    }
}
impl<P: StreamProvider> Drop for Supervised<P> {
    fn drop(&mut self) {
        if let Ok(workers) = self.workers.lock() {
            for worker in workers.values() {
                worker.abort.abort();
            }
        }
    }
}
impl<P: StreamProvider> ActorRuntime for Supervised<P> {
    fn install(&self, package: Arc<dyn ActorPackage>) -> BoxProviderFuture<'_, Result<()>> {
        self.runtime.install(package)
    }
    fn place(&self, placement: Placement, input: Value) -> BoxProviderFuture<'_, Result<Snapshot>> {
        Box::pin(async move {
            let snapshot = self.runtime.place(placement.clone(), input).await?;
            let _completion = self.supervise(placement)?;
            Ok(snapshot)
        })
    }
    fn deliver(
        &self,
        placement: Placement,
        event: Event,
    ) -> BoxProviderFuture<'_, Result<Snapshot>> {
        self.runtime.deliver(placement, event)
    }
    fn recover(&self, placement: Placement) -> BoxProviderFuture<'_, Result<Snapshot>> {
        Box::pin(async move {
            // Recheck authorization and ownership for every caller, even when
            // a completed worker already has a retained observation.
            self.runtime.recover(placement.clone()).await?;
            self.supervise(placement)?.await.map_err(Error::Storage)
        })
    }
    fn cancel(
        &self,
        placement: Placement,
        event: Event,
    ) -> BoxProviderFuture<'_, Result<Snapshot>> {
        self.runtime.cancel(placement, event)
    }
}
