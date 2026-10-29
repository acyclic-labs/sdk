//! Scheduling for owned provider and actor work, independent of transports.
use crate::{BoxProviderFuture, ProviderPlatform};

pub(crate) mod signal;
pub(crate) mod sources;
pub(crate) mod sync;
pub use sources::ReadySources;

/// Gives other host tasks one turn without spawning detached work.
///
/// A browser self-wake schedules a microtask, which cannot make `IndexedDB`
/// progress. The browser adapter posts a cancellable event-loop task instead.
/// Callers retain ownership of the future and bound the number of turns.
pub async fn yield_now() -> Result<(), crate::StreamError> {
    #[cfg(all(target_arch = "wasm32", feature = "wasm"))]
    return browser::yield_now().await;
    #[cfg(not(all(target_arch = "wasm32", feature = "wasm")))]
    {
        let mut yielded = false;
        futures::future::poll_fn(|cx| {
            if yielded {
                std::task::Poll::Ready(Ok(()))
            } else {
                yielded = true;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        })
        .await
    }
}

/// Scheduling supplied by the caller; the pool has no runtime dependency.
pub trait Executor: ProviderPlatform {
    /// Runs one owned pool operation until completion or cancellation.
    fn spawn(&self, future: BoxProviderFuture<'static, ()>);
    /// Deadline or reconnect backoff; never used to poll a follow.
    fn sleep(&self, milliseconds: u32) -> BoxProviderFuture<'static, ()>;
}

/// Native Tokio or browser event-loop scheduling.
#[derive(Default)]
pub struct PlatformExecutor;

#[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
impl PlatformExecutor {
    /// Submits an owned future without inserting an additional caller box.
    /// Custody fields outside an inner future box therefore outlive that box.
    pub fn spawn_owned<F>(&self, mut future: F)
    where
        F: std::future::Future<Output = ()> + crate::ProviderTask + Unpin + 'static,
    {
        // A ready operation needs no scheduler allocation. Unpin lets a
        // pending operation transfer safely to the platform-owned executor,
        // whose first poll installs its actual waker before waiting.
        if std::pin::Pin::new(&mut future)
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
            .is_ready()
        {
            return;
        }
        #[cfg(not(target_arch = "wasm32"))]
        drop(acyclic_native_runtime::spawn(future));
        #[cfg(all(target_arch = "wasm32", feature = "wasm"))]
        wasm_bindgen_futures::spawn_local(future);
    }
}
#[cfg(all(target_arch = "wasm32", feature = "wasm"))]
#[path = "session/runtime/browser.rs"]
mod browser;

#[cfg(not(target_arch = "wasm32"))]
impl Executor for PlatformExecutor {
    fn spawn(&self, future: BoxProviderFuture<'static, ()>) {
        self.spawn_owned(future);
    }
    fn sleep(&self, milliseconds: u32) -> BoxProviderFuture<'static, ()> {
        Box::pin(async move {
            tokio::time::sleep(std::time::Duration::from_millis(u64::from(milliseconds))).await;
        })
    }
}
#[cfg(all(target_arch = "wasm32", feature = "wasm"))]
impl Executor for PlatformExecutor {
    fn spawn(&self, future: BoxProviderFuture<'static, ()>) {
        self.spawn_owned(future);
    }
    fn sleep(&self, milliseconds: u32) -> BoxProviderFuture<'static, ()> {
        Box::pin(browser::delay(milliseconds))
    }
}

#[cfg(all(test, feature = "test-support", not(target_arch = "wasm32")))]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use acyclic_native_runtime::testing::{CapturedClock, Schedule};

    #[test]
    fn owned_ready_task_completes_without_a_runtime_or_another_box() {
        let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let signal = std::sync::Arc::clone(&completed);
        let future: BoxProviderFuture<'static, ()> = Box::pin(async move {
            signal.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        PlatformExecutor.spawn_owned(future);
        assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn owned_pending_task_is_transferred_and_finishes()
    -> Result<(), Box<dyn std::error::Error>> {
        let (send, receive) = futures::channel::oneshot::channel();
        let (complete, finished) = futures::channel::oneshot::channel();
        let future: BoxProviderFuture<'static, ()> = Box::pin(async move {
            let result = receive.await;
            let _ = complete.send(result);
        });
        PlatformExecutor.spawn_owned(future);
        send.send(()).map_err(|()| "owned receiver dropped")?;
        finished.await??;
        Ok(())
    }

    async fn child_clock<const ROUTED: bool>() -> Result<Option<u64>, Box<dyn std::error::Error>> {
        let (schedule, mut events) = Schedule::seeded(71);
        schedule.set_unix_millis(100);
        let operation = schedule.scope(async {
            let (sent, received) = futures::channel::oneshot::channel();
            let child = Box::pin(async move {
                // Force the owned task through its pending handoff before
                // observing context. A ready first poll does not exercise it.
                assert!(yield_now().await.is_ok());
                let clock = CapturedClock::capture().map(|clock| clock.unix_millis());
                let _ = sent.send(clock);
            });
            if ROUTED {
                PlatformExecutor.spawn(child);
            } else {
                // The former raw spawn leaves this owned future outside the
                // captured clock, entropy and native-work schedule.
                drop(tokio::spawn(child));
            }
            let result = received.await;
            schedule.drained().await;
            result
        });
        tokio::pin!(operation);
        loop {
            tokio::select! {
                biased;
                event = events.recv() => event.ok_or("lost native schedule boundary")?.release(),
                result = &mut operation => return result.map_err(Into::into),
            }
        }
    }

    #[tokio::test]
    async fn platform_executor_preserves_owned_native_context()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(child_clock::<true>().await?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn negative_control_raw_tokio_spawn_loses_native_context()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(child_clock::<false>().await?, None);
        Ok(())
    }
}
