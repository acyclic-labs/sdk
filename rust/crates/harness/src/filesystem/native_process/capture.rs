use super::*;
use std::{
    process::{Command, Stdio},
    time::Duration,
};

/// Observes borrowed output without copying it or changing the captured result.
pub type NativeProcessObserver =
    dyn for<'a> Fn(acyclic_native_runtime::ProcessOutput<'a>) -> std::io::Result<()> + Send + Sync;

pub(super) async fn capture<P: StreamProvider + 'static>(
    owner: &Arc<TaskJournalOwner<P>>,
    request: &NativeProcessRequest,
    observer: Option<Arc<NativeProcessObserver>>,
) -> Result<Option<NativeProcessResult>> {
    owner.verify(false, &mut None).await?;
    let check_owner = Arc::clone(owner);
    let invocation = Invocation::from(request);
    let runtime = tokio::runtime::Handle::current();
    let control_timeout = Duration::from_millis(u64::from(invocation.control_timeout_ms));
    let check = move || {
        runtime
            .block_on(tokio::time::timeout(
                control_timeout,
                check_owner.verify(false, &mut None),
            ))
            .map_err(control_error)?
            .map_err(control_error)
    };
    capture_checked(invocation, observer, check).await
}

pub(crate) fn control_error(
    cause: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Interrupted, cause)
}

pub(crate) async fn capture_checked(
    invocation: Invocation,
    observer: Option<Arc<NativeProcessObserver>>,
    check: impl Fn() -> std::io::Result<()> + Send + 'static,
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
    )
    .await
    .map(|(output, ())| output)
}

// Ownership stays in the worker until process cleanup finishes, even if its
// awaiting task is cancelled. The checkout Loom model uses this same boundary.
pub(crate) fn with_custody<T, R>(mut custody: T, operation: impl FnOnce(&mut T) -> R) -> (R, T) {
    let result = operation(&mut custody);
    (result, custody)
}

pub(crate) async fn capture_with_custody<T: Send + 'static>(
    invocation: Invocation,
    mut observer: impl FnMut(&mut T, acyclic_native_runtime::ProcessOutput<'_>) -> std::io::Result<()>
    + Send
    + 'static,
    check: impl Fn() -> std::io::Result<()> + Send + 'static,
    custody: T,
) -> Result<(Option<NativeProcessResult>, T)> {
    let (output, custody) = tokio::task::spawn_blocking(move || {
        with_custody(custody, |custody| {
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
            check()?;
            let mut process = acyclic_native_runtime::ProcessTree::spawn(&mut command)?;
            let mut next_check = std::time::Instant::now();
            let poll = || {
                if std::time::Instant::now() >= next_check {
                    check()?;
                    next_check = std::time::Instant::now()
                        + Duration::from_millis(u64::from(invocation.cancellation_poll_ms));
                }
                Ok(())
            };
            let timeout = Duration::from_millis(u64::from(invocation.timeout_ms));
            let maximum_output = invocation.maximum_output_bytes;
            let exchange = invocation
                .mcp_stdio
                .as_ref()
                .map(|request| request.exchange(None))
                .transpose()
                .map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, error.to_string())
                })?;
            let (mut exchange, first, _hold) = match exchange {
                Some((exchange, first, hold)) => (Some(exchange), first, hold),
                None => (None, Vec::new(), None),
            };
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
            let output = process.wait_with_output(
                timeout,
                maximum_output,
                input,
                poll,
                |chunk| {
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
                    let (bytes, _hold) = exchange.push(chunk, None).map_err(|error| {
                        std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
                    })?;
                    Ok(acyclic_native_runtime::ProcessInput {
                        bytes,
                        complete: exchange.is_complete(),
                    })
                },
                None,
                None,
            );
            Ok::<_, std::io::Error>(output)
        })
    })
    .await
    .map_err(|error| Error::Storage(format!("native process observer interrupted: {error}")))?;
    Ok((result(output), custody))
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
