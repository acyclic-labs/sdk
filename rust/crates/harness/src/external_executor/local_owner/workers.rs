//! Each task runs independently; the workspace retains every join through drain.
use super::*;
use futures::{Stream, StreamExt};

pub(super) fn start<F>(work: F) -> impl Future<Output = Result<()>> + Send + 'static
where
    F: Future<Output = ()> + Send + 'static,
{
    #[cfg(not(acyclic_harness_loom))]
    let worker = tokio::spawn(work);
    #[cfg(acyclic_harness_loom)]
    let worker = loom::thread::spawn(move || loom::future::block_on(work));
    async move {
        #[cfg(not(acyclic_harness_loom))]
        return worker.await.map_err(storage);
        #[cfg(acyclic_harness_loom)]
        worker
            .join()
            .map_err(|_| Error::Storage("task worker panicked".into()))
    }
}

pub(super) async fn completed<S: Stream<Item = Result<()>> + Unpin>(workers: &mut S) -> Option<()> {
    if let Err(error) = workers.next().await? {
        tracing::warn!(%error, "request task worker failed");
    }
    Some(())
}

pub(super) async fn drain<S: Stream<Item = Result<()>> + Unpin>(workers: &mut S) {
    #[cfg(acyclic_harness_loom)]
    if model::early_drain() {
        return;
    }
    while completed(workers).await.is_some() {}
}

#[cfg(acyclic_harness_loom)]
mod model;

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
