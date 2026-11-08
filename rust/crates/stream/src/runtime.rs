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
    pub fn spawn_owned<F>(&self, future: F)
    where
        F: std::future::Future<Output = ()> + crate::ProviderTask + Unpin + 'static,
    {
        #[cfg(not(target_arch = "wasm32"))]
        acyclic_native_runtime::spawn_eager(future);
        #[cfg(all(target_arch = "wasm32", feature = "wasm"))]
        {
            let mut future = future;
            if std::pin::Pin::new(&mut future)
                .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                .is_pending()
            {
                wasm_bindgen_futures::spawn_local(future);
            }
        }
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

    #[tokio::test]
    async fn physical_eager_task_retains_its_original_native_producer()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut original = None;
        for release_first in [true, false] {
            let trace = physical_eager_trace(release_first).await?;
            if let Some(original) = &original {
                assert_eq!(
                    &trace, original,
                    "physical completion timing changed the seeded trace"
                );
            } else {
                original = Some(trace);
            }
        }
        Ok(())
    }

    async fn physical_eager_trace(
        release_first: bool,
    ) -> Result<(Vec<(u64, &'static str)>, Vec<String>), Box<dyn std::error::Error>> {
        let (schedule, mut events) = Schedule::seeded(71);
        let (entered, started) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        // Change the real worker's completion order without changing the seed,
        // operation, or simulated boundary. Channel closure ends either wait.
        let producer = std::thread::spawn(move || -> std::io::Result<()> {
            if release_first {
                release.send(()).map_err(std::io::Error::other)?;
            }
            started.recv().map_err(std::io::Error::other)?;
            if !release_first {
                release.send(()).map_err(std::io::Error::other)?;
            }
            Ok(())
        });
        let operation = schedule.scope(async {
            let (sent, received) = futures::channel::oneshot::channel();
            acyclic_native_runtime::run_blocking_io(
                move || {
                    PlatformExecutor.spawn_owned(Box::pin(async move {
                        let result = acyclic_native_runtime::run_blocking_io(
                            move || {
                                entered.send(()).map_err(std::io::Error::other)?;
                                released.recv().map_err(std::io::Error::other)?;
                                Ok(42)
                            },
                            None,
                        )
                        .expect("original native submission")
                        .await
                        .and_then(std::convert::identity);
                        let _ = sent.send(result);
                    }));
                },
                None,
            )?
            .await?;
            assert_eq!(received.await??, 42);
            schedule.drained().await;
            Ok::<(), Box<dyn std::error::Error>>(())
        });
        tokio::pin!(operation);
        loop {
            tokio::select! {
                biased;
                event = events.recv() => event.ok_or("lost original native producer boundary")?.release(),
                result = &mut operation => { result?; break; },
            }
        }
        assert_eq!(
            schedule
                .trace()
                .iter()
                .filter(|event| event.starts_with("native:"))
                .count(),
            2,
            "both physical producers must retain the original schedule"
        );
        producer
            .join()
            .expect("physical completion producer panicked")?;
        Ok((schedule.polls(), schedule.trace()))
    }

    async fn child_clock<const ROUTED: bool>() -> Result<Option<u64>, Box<dyn std::error::Error>> {
        let (schedule, mut events) = Schedule::seeded(71);
        schedule.set_unix_millis(100);
        let operation = schedule.scope(async {
            let (sent, received) = futures::channel::oneshot::channel();
            let (release, entered) = futures::channel::oneshot::channel();
            let child = Box::pin(async move {
                // Force the owned task through its pending handoff before
                // observing context. A ready first poll does not exercise it.
                if entered.await.is_err() {
                    return;
                }
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
            assert!(release.send(()).is_ok());
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
