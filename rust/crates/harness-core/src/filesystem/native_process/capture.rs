use super::*;
use std::{
    process::{Command, Stdio},
    time::Duration,
};
mod owner;
#[doc(hidden)]
pub use owner::OwnerControl;

/// Observes borrowed output without copying it or changing the captured result.
pub type NativeProcessObserver =
    dyn for<'a> Fn(acyclic_native_runtime::ProcessOutput<'a>) -> std::io::Result<()> + Send + Sync;

pub(super) async fn capture<P: StreamProvider + 'static>(
    owner: &Arc<TaskJournalOwner<P>>,
    request: &NativeProcessRequest,
    observer: Option<Arc<NativeProcessObserver>>,
) -> Result<Option<NativeProcessResult>> {
    let control = OwnerControl::open(Arc::clone(owner)).await?;
    owner.verify(false, &mut None).await?;
    let check_owner = Arc::clone(owner);
    let invocation = Invocation::from(request);
    let runtime = tokio::runtime::Handle::current();
    let control_timeout = Duration::from_millis(u64::from(invocation.control_timeout_ms));
    let wake = control.wake();
    let check = move || {
        control.check(|| {
            check_on_runtime(
                &runtime,
                control_timeout,
                check_owner.verify(false, &mut None),
            )
        })
    };
    capture_checked(invocation, observer, check, Some(wake)).await
}

// Both production checks and the runtime-construction regression enter the
// same bounded control path. Seeded host callbacks yield at original Pending.
fn check_on_runtime(
    runtime: &tokio::runtime::Handle,
    control_timeout: Duration,
    check: impl std::future::Future<Output = Result<()>>,
) -> std::io::Result<()> {
    wait_control(runtime, || async {
        tokio::time::timeout(control_timeout, check).await
    })
    .map_err(control_error)?
    .map_err(control_error)
}

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

pub(crate) async fn capture_checked(
    invocation: Invocation,
    observer: Option<Arc<NativeProcessObserver>>,
    check: impl Fn() -> std::io::Result<()> + Send + 'static,
    wake: Option<acyclic_native_runtime::ProcessWake>,
) -> Result<Option<NativeProcessResult>> {
    capture_with_custody(
        invocation,
        move |_: &mut (), chunk| {
            if let Some(observer) = &observer {
                observer(chunk)?;
            }
            Ok(())
        },
        check,
        (),
        wake,
    )
    .await
    .map(|(output, ())| output)
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
    exchange: Option<crate::mcp::stdio::McpStdioExchange>,
    /// The exchange's native allocation, held until capture ends.
    _hold: crate::NativeCustody,
}

impl<T, O, C> acyclic_native_runtime::CaptureHandler for Handler<T, O, C>
where
    T: Send + 'static,
    O: FnMut(&mut T, acyclic_native_runtime::ProcessOutput<'_>) -> std::io::Result<()>
        + Send
        + 'static,
    C: Fn() -> std::io::Result<()> + Send + 'static,
{
    fn wakes(&self) -> &[acyclic_native_runtime::ProcessWake] {
        self.wake.as_slice()
    }

    fn check(&mut self) -> std::io::Result<()> {
        (self.check)()
    }

    fn observe(
        &mut self,
        chunk: acyclic_native_runtime::ProcessOutput<'_>,
    ) -> std::io::Result<acyclic_native_runtime::ProcessInput> {
        observe_exchange(
            &mut self.custody,
            &mut self.observer,
            &mut self.exchange,
            chunk,
        )
    }
}

fn observe_exchange<T>(
    custody: &mut T,
    observer: &mut impl FnMut(&mut T, acyclic_native_runtime::ProcessOutput<'_>) -> std::io::Result<()>,
    exchange: &mut Option<crate::mcp::stdio::McpStdioExchange>,
    chunk: acyclic_native_runtime::ProcessOutput<'_>,
) -> std::io::Result<acyclic_native_runtime::ProcessInput> {
    observer(custody, chunk)?;
    let acyclic_native_runtime::ProcessOutput::Stdout(chunk) = chunk else {
        return Ok(Default::default());
    };
    let Some(exchange) = exchange.as_mut() else {
        return Ok(Default::default());
    };
    if exchange.is_complete() {
        return Ok(Default::default());
    }
    let (bytes, _hold) = exchange
        .push(chunk, None)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()))?;
    Ok(acyclic_native_runtime::ProcessInput {
        bytes,
        complete: exchange.is_complete(),
    })
}

type CaptureResult = std::io::Result<
    std::result::Result<std::process::Output, acyclic_native_runtime::ProcessCaptureFailure>,
>;

/// What the spawn turn left: a finished result, or a running capture.
enum Started<T, H> {
    Finished(CaptureResult, T),
    Running(
        acyclic_native_runtime::TaskHandle<
            std::io::Result<acyclic_native_runtime::CaptureOutcome<H>>,
        >,
    ),
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
    // blocking pool, so they run on main's independent native host service.
    // Spawning runs there too; the running child then holds no worker: the
    // capture awaits readiness between its turns.
    let spawner = acyclic_native_runtime::TaskSpawner::current();
    let started = acyclic_native_runtime::run_blocking_io(
        move || {
            let mut handler = Handler {
                custody,
                observer,
                check,
                wake,
                exchange: None,
                _hold: None,
            };
            match start(&invocation, &mut handler) {
                Ok(Ok((tree, input))) => {
                    let schedule = acyclic_native_runtime::CaptureSchedule::new(
                        Duration::from_millis(u64::from(invocation.timeout_ms)),
                    );
                    // Transfer physical custody before returning from the
                    // worker. Losing the waiter cannot drop an unsubmitted tree.
                    Started::Running(spawner.spawn(tree.capture(
                        schedule,
                        invocation.maximum_output_bytes,
                        input,
                        handler,
                    )))
                }
                Ok(Err(output)) => Started::Finished(Ok(output), handler.custody),
                Err(error) => Started::Finished(Err(error), handler.custody),
            }
        },
        None,
    )
    .map_err(|error| Error::Storage(format!("native process capture admission: {error}")))?
    .await
    .map_err(|error| Error::Storage(format!("native process observer interrupted: {error}")))?;
    let (output, custody) = match started {
        Started::Finished(output, custody) => (output, custody),
        Started::Running(capture) => {
            // Ownership stays with the spawned capture until process cleanup
            // finishes, even if its awaiting task is cancelled.
            let outcome = capture.await.and_then(|outcome| outcome).map_err(|error| {
                Error::Storage(format!("native process observer interrupted: {error}"))
            })?;
            (Ok(outcome.result), outcome.handler.custody)
        }
    };
    Ok((result(output), custody))
}

/// A spawned tree and its owned input, or the output an installed test
/// provider answered with instead of a process.
type Spawned = std::result::Result<
    (
        acyclic_native_runtime::ProcessTree,
        Option<acyclic_native_runtime::OwnedCaptureInput>,
    ),
    std::result::Result<std::process::Output, acyclic_native_runtime::ProcessCaptureFailure>,
>;

/// Checks intent, opens the exchange and spawns the tree.
fn start<T>(
    invocation: &Invocation,
    handler: &mut Handler<
        T,
        impl FnMut(&mut T, acyclic_native_runtime::ProcessOutput<'_>) -> std::io::Result<()>,
        impl Fn() -> std::io::Result<()>,
    >,
) -> std::io::Result<Spawned> {
    let mut command = Command::new(&invocation.executable);
    if !invocation.inherit_environment {
        command.env_clear();
    }
    command
        .args(&invocation.argv)
        .current_dir(&invocation.cwd)
        .envs(&invocation.environment)
        .stdin(
            if invocation.mcp_stdio.is_some() || invocation.standard_input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            },
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    (handler.check)()?;
    let exchange = invocation
        .mcp_stdio
        .as_ref()
        .map(|request| request.exchange(None))
        .transpose()
        .map_err(|error| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
        })?;
    let (exchange, first, hold) = match exchange {
        Some((exchange, first, hold)) => (Some(exchange), first, hold),
        None => (None, Vec::new(), None),
    };
    handler.exchange = exchange;
    handler._hold = hold;
    #[cfg(feature = "test-support")]
    {
        let input = invocation
            .mcp_stdio
            .as_ref()
            .map(|request| acyclic_native_runtime::CaptureInput::Exchange {
                initial: &first,
                maximum: request.maximum_bytes as usize,
            })
            .or_else(|| {
                invocation
                    .standard_input
                    .as_deref()
                    .map(acyclic_native_runtime::CaptureInput::Literal)
            });
        let Handler {
            custody,
            observer,
            check,
            exchange,
            ..
        } = handler;
        let mut observe = |chunk: acyclic_native_runtime::ProcessOutput<'_>| {
            observe_exchange(custody, observer, exchange, chunk)
        };
        if let Some(output) = acyclic_native_runtime::testing::capture_process(
            acyclic_native_runtime::testing::ProcessCapture {
                command: &command,
                timeout: Duration::from_millis(u64::from(invocation.timeout_ms)),
                maximum: invocation.maximum_output_bytes,
                input,
            },
            &mut || check(),
            &mut observe,
        ) {
            return Ok(Err(output));
        }
    }
    let input = match &invocation.mcp_stdio {
        Some(request) => Some(acyclic_native_runtime::OwnedCaptureInput::Exchange {
            initial: first,
            maximum: request.maximum_bytes as usize,
        }),
        None => invocation
            .standard_input
            .clone()
            .map(acyclic_native_runtime::OwnedCaptureInput::Literal),
    };
    let tree = match &invocation.process_custody {
        Some(custody) => {
            acyclic_native_runtime::ProcessTree::spawn_in_custody(&mut command, custody)?
        }
        None => acyclic_native_runtime::ProcessTree::spawn(&mut command)?,
    };
    Ok(Ok((tree, input)))
}

fn result(
    output: std::io::Result<
        std::result::Result<std::process::Output, acyclic_native_runtime::ProcessCaptureFailure>,
    >,
) -> Option<NativeProcessResult> {
    let result = match output {
        Ok(Ok(output)) => NativeProcessResult {
            success: output.status.success(),
            exit_code: output.status.code(),
            stdout: output.stdout,
            stderr: output.stderr,
            stop: None,
        },
        Ok(Err(failure)) => NativeProcessResult {
            success: false,
            exit_code: failure.status.and_then(|status| status.code()),
            stdout: failure.stdout,
            stderr: failure.stderr,
            stop: Some(NativeProcessStop {
                kind: match failure.error.kind() {
                    std::io::ErrorKind::TimedOut => NativeProcessStopKind::Timeout,
                    std::io::ErrorKind::FileTooLarge => NativeProcessStopKind::OutputLimit,
                    std::io::ErrorKind::Interrupted => NativeProcessStopKind::ControlStopped,
                    _ => NativeProcessStopKind::CaptureFailed,
                },
                message: failure.error.to_string().chars().take(128).collect(),
                cleanup_completed: failure.cleanup_completed,
            }),
        },
        Err(_) => return None,
    };
    Some(result)
}

#[cfg(test)]
mod tests;
