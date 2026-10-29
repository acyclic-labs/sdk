//! Native execution transport; admission and process creation stay with the
//! original local owner. No shell, reader task or output collector is added.
use super::*;
use acyclic_native_runtime::{CaptureHandler, ProcessCaptureStream};

struct Output<H: CaptureHandler> {
    machine: MachineId,
    capture: Option<ProcessCaptureStream<H>>,
    stderr: Option<Vec<u8>>,
}

/// Moves an admitted process capture into the public Machines output stream.
/// The capture retains its original callbacks, authority and physical custody.
/// Pulling advances only one native batch; dropping closes its original credit
/// and wakes cleanup even when the process is silent or the reader is stalled.
/// A terminal frame follows pipe EOF and the original physical cleanup join.
pub fn output<H: CaptureHandler>(
    machine: MachineId,
    capture: ProcessCaptureStream<H>,
) -> ExecStream {
    Box::pin(stream::unfold(
        Output {
            machine,
            capture: Some(capture),
            stderr: None,
        },
        |mut state| async move {
            if let Some(bytes) = state.stderr.take() {
                return Some((Ok(ExecOutput::Stderr(bytes)), state));
            }
            let capture = state.capture.as_mut()?;
            while let Some(chunk) = capture.recv().await {
                if !chunk.stdout.is_empty() {
                    state.stderr = (!chunk.stderr.is_empty()).then_some(chunk.stderr);
                    return Some((Ok(ExecOutput::Data(chunk.stdout)), state));
                }
                if !chunk.stderr.is_empty() {
                    return Some((Ok(ExecOutput::Stderr(chunk.stderr)), state));
                }
            }
            let result = state.capture.take()?.finish().await;
            let terminal = match result {
                Ok(outcome) => match outcome.result {
                    Ok(output) => output.status.code().map_or_else(
                        || {
                            ExecOutput::Stopped(ExecStop {
                                kind: ExecStopKind::CaptureFailed,
                                message: "native process exited without a numeric exit code".into(),
                                cleanup_completed: true,
                            })
                        },
                        ExecOutput::ExitCode,
                    ),
                    Err(failure) => ExecOutput::Stopped(ExecStop {
                        kind: match failure.error.kind() {
                            std::io::ErrorKind::TimedOut => ExecStopKind::Timeout,
                            std::io::ErrorKind::FileTooLarge => ExecStopKind::OutputLimit,
                            std::io::ErrorKind::Interrupted => ExecStopKind::ControlStopped,
                            _ => ExecStopKind::CaptureFailed,
                        },
                        message: failure.error.to_string(),
                        cleanup_completed: failure.cleanup_completed,
                    }),
                },
                Err(_) => {
                    return Some((
                        Err(ProviderError::CommandIndeterminate(state.machine)),
                        state,
                    ));
                }
            };
            Some((Ok(terminal), state))
        },
    ))
}

#[cfg(test)]
mod tests;
