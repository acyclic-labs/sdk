//! Scheduling for owned provider and actor work, independent of transports.
use crate::{BoxProviderFuture, ProviderPlatform};

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
#[cfg(all(target_arch = "wasm32", feature = "wasm"))]
#[path = "session/runtime/browser.rs"]
mod browser;

#[cfg(not(target_arch = "wasm32"))]
impl Executor for PlatformExecutor {
    fn spawn(&self, future: BoxProviderFuture<'static, ()>) {
        drop(acyclic_native_runtime::spawn(future));
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
        wasm_bindgen_futures::spawn_local(future);
    }
    fn sleep(&self, milliseconds: u32) -> BoxProviderFuture<'static, ()> {
        Box::pin(browser::delay(milliseconds))
    }
}

#[cfg(all(test, feature = "test-support", not(target_arch = "wasm32")))]
#[cfg(not(acyclic_stream_loom))]
mod tests {
    use super::*;
    use acyclic_native_runtime::testing::{CapturedClock, Schedule};

    async fn child_clock<const ROUTED: bool>() -> Result<Option<u64>, Box<dyn std::error::Error>> {
        let (schedule, mut events) = Schedule::seeded(71);
        schedule.set_unix_millis(100);
        let operation = schedule.scope(async {
            let (sent, received) = futures::channel::oneshot::channel();
            let child = Box::pin(async move {
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
