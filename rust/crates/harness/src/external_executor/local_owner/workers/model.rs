//! The production launch and drain path uses Loom workers and their real joins.
use super::*;
use std::cell::Cell;

loom::thread_local! { static EARLY_DRAIN: Cell<bool> = Cell::new(false); }

pub(super) fn early_drain() -> bool {
    EARLY_DRAIN.with(Cell::get)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::FuturesUnordered;
    use loom::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    fn independent(broken: bool) {
        let caller = loom::thread::current().id();
        let independent = Arc::new(AtomicBool::new(true));
        let settled = Arc::new(AtomicUsize::new(0));
        let mut workers: FuturesUnordered<
            std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>>,
        > = FuturesUnordered::new();
        for _ in 0..2 {
            let independent = Arc::clone(&independent);
            let settled = Arc::clone(&settled);
            let work = async move {
                independent.fetch_and(loom::thread::current().id() != caller, Ordering::SeqCst);
                settled.fetch_add(1, Ordering::SeqCst);
            };
            if broken {
                // The previous workspace loop retained unspawned futures: all
                // work ran only while the discovering caller polled that loop.
                workers.push(Box::pin(async move {
                    work.await;
                    Ok(())
                }));
            } else {
                workers.push(Box::pin(start(work)));
            }
        }
        loom::future::block_on(drain(&mut workers));
        assert_eq!(settled.load(Ordering::SeqCst), 2);
        assert!(
            independent.load(Ordering::SeqCst),
            "task execution shared its caller's scheduling context"
        );
    }

    #[test]
    fn task_polls_are_not_owned_by_the_workspace_discovery_loop() {
        loom::model(|| independent(false));
        let failure = std::panic::catch_unwind(|| loom::model(|| independent(true)))
            .expect_err("inline task execution was not detected");
        let message = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(message.contains("scheduling context"), "{message}");
    }

    fn retirement(broken: bool) {
        EARLY_DRAIN.with(|fault| fault.set(broken));
        let provider = Arc::new(());
        let settled = Arc::new(AtomicUsize::new(0));
        let mut workers = FuturesUnordered::new();
        for _ in 0..2 {
            let provider = Arc::clone(&provider);
            let settled = Arc::clone(&settled);
            workers.push(Box::pin(start(async move {
                loom::thread::yield_now();
                settled.fetch_add(1, Ordering::SeqCst);
                drop(provider);
            })));
        }
        loom::future::block_on(drain(&mut workers));
        let closed = Arc::try_unwrap(provider).is_ok();
        // Even the broken control joins before asserting so no worker capture
        // outlives this model execution when an interleaving rejects retirement.
        EARLY_DRAIN.with(|fault| fault.set(false));
        loom::future::block_on(drain(&mut workers));
        assert!(closed, "provider retired before its task workers settled");
        assert_eq!(settled.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn every_task_join_precedes_provider_retirement() {
        loom::model(|| retirement(false));
        let failure = std::panic::catch_unwind(|| loom::model(|| retirement(true)))
            .expect_err("early retirement was not detected");
        let message = failure
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| failure.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(message.contains("provider retired before"), "{message}");
    }
}
