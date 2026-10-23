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
        tokio::spawn(future);
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
