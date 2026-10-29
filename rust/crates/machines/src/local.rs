//! Native execution over the public command and output contract. The local
//! host retains workspace admission; no shell or output reader is inserted.
use super::*;
use acyclic_native_runtime::{CaptureHandler, ProcessCaptureStream};
use futures::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

mod process;
pub use process::{LocalExecution, LocalMachine, LocalOptions};
mod provider;
pub use provider::provider;

struct Output<H: CaptureHandler, C> {
    machine: MachineId,
    capture: Option<ProcessCaptureStream<H>>,
    stderr: Option<Vec<u8>>,
    custody: Option<C>,
    completed: fn(H) -> C,
    failure: Option<ProviderError>,
}

// Callback custody is never pinned or projected as a future.
impl<H: CaptureHandler, C> Unpin for Output<H, C> {}

impl<H: CaptureHandler, C> Output<H, C> {
    fn new(machine: MachineId, capture: ProcessCaptureStream<H>, completed: fn(H) -> C) -> Self {
        Self {
            machine,
            capture: Some(capture),
            stderr: None,
            custody: None,
            completed,
            failure: None,
        }
    }

    fn failed(
        machine: MachineId,
        error: ProviderError,
        custody: Option<C>,
        completed: fn(H) -> C,
    ) -> Self {
        Self {
            machine,
            capture: None,
            stderr: None,
            custody,
            completed,
            failure: Some(error),
        }
    }

    async fn finish(mut self) -> Result<C, ProviderError> {
        if let Some(capture) = self.capture.take() {
            let outcome = capture
                .finish()
                .await
                .map_err(|_| ProviderError::CommandIndeterminate(self.machine))?;
            return Ok((self.completed)(outcome.handler));
        }
        self.custody
            .take()
            .ok_or(ProviderError::CommandIndeterminate(self.machine))
    }
}

impl<H: CaptureHandler, C> Stream for Output<H, C> {
    type Item = Result<ExecOutput, ProviderError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let state = self.get_mut();
        if let Some(error) = state.failure.take() {
            return Poll::Ready(Some(Err(error)));
        }
        if let Some(bytes) = state.stderr.take() {
            return Poll::Ready(Some(Ok(ExecOutput::Stderr(bytes))));
        }
        let Some(capture) = state.capture.as_mut() else {
            return Poll::Ready(None);
        };
        match std::task::ready!(capture.poll_recv(context)) {
            Some(chunk) if !chunk.stdout.is_empty() => {
                state.stderr = (!chunk.stderr.is_empty()).then_some(chunk.stderr);
                return Poll::Ready(Some(Ok(ExecOutput::Data(chunk.stdout))));
            }
            Some(chunk) if !chunk.stderr.is_empty() => return Poll::Ready(Some(Ok(ExecOutput::Stderr(chunk.stderr)))),
            Some(_) => return Poll::Ready(Some(Err(ProviderError::Rejected("native capture emitted an empty batch; recovery: repair the original capture producer".into())))),
            None => {}
        }
        let outcome = std::task::ready!(capture.poll_finish(context));
        state.capture = None;
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(_) => {
                return Poll::Ready(Some(Err(ProviderError::CommandIndeterminate(
                    state.machine,
                ))));
            }
        };
        state.custody = Some((state.completed)(outcome.handler));
        let terminal = match outcome.result {
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
        };
        Poll::Ready(Some(Ok(terminal)))
    }
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
    Box::pin(Output::new(machine, capture, std::convert::identity))
}

#[cfg(test)]
mod provider_tests;
#[cfg(test)]
mod tests;
