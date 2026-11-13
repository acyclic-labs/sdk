//! Native process capture for the live `shell` tool: owned invocation input,
//! the capture engine, and the control that stops a command when its signed
//! scope expires or its turn is cancelled.

use super::{NativeProcessResult, NativeProcessStop, NativeProcessStopKind};
use crate::{Error, Result};

#[doc(hidden)]
pub mod capture;
mod invocation;
#[cfg(any(not(target_arch = "wasm32"), all(test, unix)))]
#[doc(hidden)]
pub mod shell;
pub use capture::NativeProcessObserver;
#[doc(hidden)]
pub use invocation::Invocation;

/// Runs filesystem preparation off the executor while retaining its original
/// workspace and volume custody until physical completion, even on caller loss.
#[doc(hidden)]
pub async fn prepare_with_custody<T: Send + 'static, R: Send + 'static>(
    custody: T,
    prepare: impl FnOnce() -> Result<R> + Send + 'static,
) -> Result<(R, T)> {
    use std::future::Future;

    let completion = acyclic_native_runtime::run_blocking_io(move || (prepare(), custody), None)
        .map_err(Error::process_io)?;
    let mut completion = std::pin::pin!(completion);
    let (result, custody) = std::future::poll_fn(|context| {
        // A callback may admit inline I/O, but preparation can traverse its
        // own projection and must leave the executor free to serve it.
        let _defer = acyclic_native_runtime::InlineBlocking::defer();
        completion.as_mut().poll(context)
    })
    .await
    .map_err(Error::process_io)?;
    Ok((result?, custody))
}
