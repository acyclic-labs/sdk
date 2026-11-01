use super::*;
use acyclic_native_runtime::{ProcessCommand, ProcessStdio};

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

type CaptureResult = std::io::Result<
    std::result::Result<std::process::Output, acyclic_native_runtime::ProcessCaptureFailure>,
>;

/// What the spawn turn left: a finished result, or a running capture.
enum Started<H: acyclic_native_runtime::CaptureHandler> {
    Finished(CaptureResult, H),
    Running(
        acyclic_native_runtime::TaskHandle<
            std::io::Result<acyclic_native_runtime::CaptureOutcome<H>>,
        >,
    ),
    Streaming(acyclic_native_runtime::ProcessCaptureStream<H>),
}

/// Whether output is collected or owned solely by the streaming observer.
pub enum OutputRetention {
    /// Collect the complete output up to the invocation's protective limit.
    Collect,
    /// Deliver bounded chunks to the observer and retain only terminal status.
    Observe,
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
    retention: OutputRetention,
) -> Result<(Option<NativeProcessResult>, T)> {
    start_capture_with_custody(invocation, observer, check, custody, wake, retention)
        .await?
        .await
}

/// Starts the original capture and returns its owned completion future.
/// The spawn turn transfers the tree into the native capture owner before
/// returning. Moving this future preserves the original process, callbacks,
/// output credit, lifetime deadline and cleanup join; it launches no new task.
/// A caller may race a borrowed completion against foreground presentation,
/// then transfer that SAME completion to its admitted background owner.
///
/// The caller must transfer its original stop-on-drop guard and authority
/// with the completion before acknowledging background admission. This API
/// does not publish a durable task identity or terminal Notice. Losing an
/// observed stream closes output credit and cancels its original capture;
/// collected output retains the existing detached-task cancellation contract.
#[doc(hidden)]
pub async fn start_capture_with_custody<T: Send + 'static>(
    invocation: Invocation,
    observer: impl FnMut(&mut T, acyclic_native_runtime::ProcessOutput<'_>) -> std::io::Result<()>
    + Send
    + 'static,
    check: impl Fn() -> std::io::Result<()> + Send + 'static,
    custody: T,
    wake: Option<acyclic_native_runtime::ProcessWake>,
    retention: OutputRetention,
) -> Result<
    impl std::future::Future<Output = Result<(Option<NativeProcessResult>, T)>> + Send + 'static,
> {
    let completion = start_capture(
        invocation,
        Handler {
            custody,
            observer,
            check,
            wake,
        },
        retention,
    )
    .await?;
    Ok(async move {
        let (result, handler) = completion.await?;
        Ok((result, handler.custody))
    })
}

/// Starts capture with its original native handler and returns owned completion.
/// The handler retains input production, wake signals, output observation and
/// retirement in one owner. No callback is wrapped or copied. Interactive input
/// uses the invocation's original allowance and the handler's native wake;
/// producer loss must wake that source and return a typed error from `input`.
/// The caller retains the same authority and background-admission obligations
/// as [`start_capture_with_custody`].
#[doc(hidden)]
pub async fn start_capture<H: acyclic_native_runtime::CaptureHandler>(
    invocation: Invocation,
    mut handler: H,
    retention: OutputRetention,
) -> Result<
    impl std::future::Future<Output = Result<(Option<NativeProcessResult>, H)>> + Send + 'static,
> {
    // Native capture callbacks may await durable publication on Tokio's
    // blocking pool, so they run on main's independent native host service.
    // Spawning runs there too; the running child then holds no worker: the
    // capture awaits readiness between its turns.
    let spawner = acyclic_native_runtime::TaskSpawner::current();
    let started =
        acyclic_native_runtime::run_blocking_io(
            move || {
                let schedule = invocation.schedule;
                let maximum_output_bytes = invocation.maximum_output_bytes;
                match start(invocation, &mut handler) {
                    Ok(Ok((tree, input))) => {
                        // Transfer physical custody before returning from the
                        // worker. Losing the waiter cannot drop an unsubmitted tree.
                        match retention {
                            OutputRetention::Collect => Started::Running(spawner.spawn(
                                tree.capture(schedule, maximum_output_bytes, input, handler),
                            )),
                            OutputRetention::Observe => Started::Streaming(tree.stream_capture(
                                &spawner,
                                schedule,
                                maximum_output_bytes,
                                input,
                                handler,
                            )),
                        }
                    }
                    Ok(Err(output)) => Started::Finished(Ok(output), handler),
                    Err(error) => Started::Finished(Err(error), handler),
                }
            },
            None,
        )
        .map_err(|error| Error::Storage(format!("native process capture admission: {error}")))?
        .await
        .map_err(|error| Error::Storage(format!("native process observer interrupted: {error}")))?;
    Ok(async move {
        let (output, handler) = match started {
            Started::Finished(output, handler) => (output, handler),
            Started::Running(capture) => {
                // Ownership stays with the spawned capture until process cleanup
                // finishes, even if its awaiting task is cancelled.
                let outcome = capture.await.and_then(|outcome| outcome).map_err(|error| {
                    Error::Storage(format!("native process observer interrupted: {error}"))
                })?;
                (Ok(outcome.result), outcome.handler)
            }
            Started::Streaming(mut stream) => {
                while stream.recv().await.is_some() {}
                let outcome = stream.finish().await.map_err(|error| {
                Error::Storage(format!("native process stream interrupted: {error}; recovery: reconcile the original command"))
            })?;
                (Ok(outcome.result), outcome.handler)
            }
        };
        Ok((result(output), handler))
    })
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

/// Checks intent and spawns the tree.
fn start(
    invocation: Invocation,
    handler: &mut impl acyclic_native_runtime::CaptureHandler,
) -> std::io::Result<Spawned> {
    let mut command = ProcessCommand::new(&invocation.executable);
    if !invocation.inherit_environment {
        command.env_clear();
    }
    command
        .args(&invocation.argv)
        .current_dir(&invocation.cwd)
        .envs(&invocation.environment)
        .stdin(if invocation.standard_input.is_some() {
            ProcessStdio::piped()
        } else {
            ProcessStdio::null()
        })
        .stdout(ProcessStdio::piped())
        .stderr(ProcessStdio::piped());
    handler.check()?;
    #[cfg(feature = "test-support")]
    {
        // The installed provider calls these sequentially on this worker.
        // Borrow the same handler for each callback without another owner.
        let handler = std::cell::RefCell::new(&mut *handler);
        if let Some(output) = acyclic_native_runtime::testing::capture_process(
            acyclic_native_runtime::testing::ProcessCapture {
                command: &command,
                schedule: invocation.schedule,
                maximum: invocation.maximum_output_bytes,
                input: invocation.standard_input.as_ref(),
            },
            &mut || handler.borrow_mut().check(),
            &mut |chunk| handler.borrow_mut().observe(chunk),
        ) {
            return Ok(Err(output));
        }
    }
    let input = invocation.standard_input;
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
