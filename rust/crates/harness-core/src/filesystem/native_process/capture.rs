use super::*;

/// Observes borrowed output without copying it or changing the captured result.
pub type NativeProcessObserver =
    dyn for<'a> Fn(acyclic_native_runtime::ProcessOutput<'a>) -> std::io::Result<()> + Send + Sync;

// The same bounded ownership future runs on the worker in both cases. A
// simulation worker yields its permit at Pending so the owner can make progress.
#[doc(hidden)]
pub fn wait_control<F: std::future::Future>(
    runtime: &tokio::runtime::Handle,
    construct: impl FnOnce() -> F,
) -> F::Output {
    // Timers and IO resources require the runtime during construction too.
    // Keep this entry confined to the explicit asynchronous control check.
    let _entered = runtime.enter();
    let future = construct();
    #[cfg(feature = "test-support")]
    {
        acyclic_native_runtime::testing::blocking_on(future)
    }
    #[cfg(not(feature = "test-support"))]
    runtime.block_on(future)
}

#[doc(hidden)]
pub fn control_error(cause: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Interrupted, cause)
}

/// Retains ownership in the worker through cleanup when its waiter is cancelled.
/// The facade's checkout model exercises this same production boundary.
#[doc(hidden)]
pub fn with_custody<T, R>(mut custody: T, operation: impl FnOnce(&mut T) -> R) -> (R, T) {
    let result = operation(&mut custody);
    (result, custody)
}

/// The owner callbacks of one spawned capture, moved between its turns.
struct Handler<T, O, C> {
    custody: T,
    observer: O,
    check: C,
    wake: Option<acyclic_native_runtime::ProcessWake>,
}

impl<T, O, C> acyclic_native_runtime::CaptureHandler for Handler<T, O, C>
where
    T: Send + 'static,
    O: FnMut(&mut T, acyclic_native_runtime::ProcessOutput<'_>) -> std::io::Result<()>
        + Send
        + 'static,
    C: Fn() -> std::io::Result<()> + Send + 'static,
{
    fn take_wakes(&mut self) -> std::io::Result<acyclic_native_runtime::CaptureWakes> {
        Ok(self.wake.take().into())
    }

    fn check(&mut self) -> std::io::Result<()> {
        (self.check)()
    }

    fn observe(
        &mut self,
        chunk: acyclic_native_runtime::ProcessOutput<'_>,
    ) -> std::io::Result<acyclic_native_runtime::ProcessInput> {
        (self.observer)(&mut self.custody, chunk)?;
        Ok(Default::default())
    }
}

#[doc(hidden)]
pub async fn capture_with_custody<T: Send + 'static>(
    invocation: Invocation,
    observer: impl FnMut(&mut T, acyclic_native_runtime::ProcessOutput<'_>) -> std::io::Result<()>
    + Send
    + 'static,
    check: impl Fn() -> std::io::Result<()> + Send + 'static,
    custody: T,
    wake: Option<acyclic_native_runtime::ProcessWake>,
) -> Result<(Option<NativeProcessResult>, T)> {
    // Native capture callbacks may await durable publication on Tokio's
    // blocking pool, so the shared launch core runs them on main's
    // independent native host service, never on a Tokio worker.
    let handler = Handler {
        custody,
        observer,
        check,
        wake,
    };
    match invocation.command.capture(invocation.host, handler).await {
        Ok(outcome) => Ok((Some(result(outcome.result)), outcome.handler.custody)),
        // Nothing ran: a definite stop, published like any other result.
        Err(acyclic_native_runtime::LaunchFailure::Refused { failure, handler }) => {
            Ok((Some(result(Err(failure))), handler.custody))
        }
        // The command may have run: only its custody comes back.
        Err(acyclic_native_runtime::LaunchFailure::Indeterminate {
            handler: Some(handler),
        }) => Ok((None, handler.custody)),
        Err(acyclic_native_runtime::LaunchFailure::Indeterminate { handler: None }) => {
            Err(Error::Storage("native process observer interrupted".into()))
        }
    }
}

fn result(
    output: std::result::Result<
        std::process::Output,
        acyclic_native_runtime::ProcessCaptureFailure,
    >,
) -> NativeProcessResult {
    match output {
        Ok(output) => NativeProcessResult {
            success: output.status.success(),
            exit_code: output.status.code(),
            stdout: output.stdout,
            stderr: output.stderr,
            stop: None,
        },
        Err(failure) => NativeProcessResult {
            success: false,
            exit_code: failure.status.and_then(|status| status.code()),
            stop: Some(NativeProcessStop {
                kind: match failure.stop() {
                    acyclic_native_runtime::CaptureStop::Timeout => NativeProcessStopKind::Timeout,
                    acyclic_native_runtime::CaptureStop::OutputLimit => {
                        NativeProcessStopKind::OutputLimit
                    }
                    acyclic_native_runtime::CaptureStop::ControlStopped => {
                        NativeProcessStopKind::ControlStopped
                    }
                    acyclic_native_runtime::CaptureStop::Failed => {
                        NativeProcessStopKind::CaptureFailed
                    }
                },
                message: failure.error.to_string().chars().take(128).collect(),
                cleanup_completed: failure.cleanup_completed,
            }),
            stdout: failure.stdout,
            stderr: failure.stderr,
        },
    }
}

#[cfg(test)]
mod tests;
