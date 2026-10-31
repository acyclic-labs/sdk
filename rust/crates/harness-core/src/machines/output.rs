//! Delayed spill over one original machine execution stream.
use crate::{
    Error, OperationId, Result,
    conversation::{ContentAccess, ContentPublisher, ContentSource},
    tool::ToolResult,
    tools::shell::{OutputTail, bounded_result},
};
use acyclic_fs::{CancellationToken, kernel::OwnedSourceChunk, performance::WorkBudget};
use acyclic_machines::{ExecOutput, ExecStop, ExecStopKind, ExecStream};
use acyclic_stream::OwnedBytes;
use futures::StreamExt;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll, ready},
};

/// Captures the original ordinary execution without another reader or task.
/// Small results remain in the tool journal. Overflow streams the complete
/// output through the caller's same operation-bound publication, retaining
/// only the bounded presentation and one original producer frame.
///
/// Before dispatch, the caller must admit this publisher's exact volume and
/// grant at the target's `.volumes/scratch` binding, including its streaming
/// source support and physical output reservation. Presentation demand is not
/// a physical output limit. This function neither selects nor mounts a target,
/// launches a command, retries it, nor supplies native allocation custody.
/// Missing producer termination is indeterminate, never a successful file.
pub async fn capture_shell_output(
    output: ExecStream,
    publisher: &dyn ContentPublisher,
    operation: OperationId,
    access: ContentAccess<'_, '_>,
    maximum_render_bytes: u64,
) -> Result<ToolResult> {
    let mut source = ExecSource {
        output,
        tail: OutputTail::new(maximum_render_bytes.min(30_000) as usize),
        prefix: None,
        pending: None,
        terminal: None,
        failed: false,
    };
    // Until overflow, this bounded prefix is the entire result. Its allocation
    // is also the presentation allocation; no speculative scratch write occurs.
    while source.terminal.is_none() {
        let frame = source
            .output
            .next()
            .await
            .ok_or(Error::Indeterminate(operation))?
            .map_err(|_| Error::Indeterminate(operation))?;
        match frame {
            ExecOutput::Data(bytes) => {
                if source
                    .prepare_spill(bytes, false)
                    .map_err(|_| Error::Indeterminate(operation))?
                {
                    break;
                }
            }
            ExecOutput::Stderr(bytes) => {
                if source
                    .prepare_spill(bytes, true)
                    .map_err(|_| Error::Indeterminate(operation))?
                {
                    break;
                }
            }
            terminal => source
                .finish(terminal)
                .map_err(|_| Error::Indeterminate(operation))?,
        }
    }
    let path = format!("shell-{operation}.log");
    let spill = format!(".volumes/scratch/{path}");
    if source.terminal.is_some() {
        let result = bounded_result(source.result(None)?, maximum_render_bytes)?;
        if result.value["omitted_bytes"].as_u64().unwrap_or_default() == 0 {
            return Ok(result);
        }
        // JSON escaping can exhaust presentation before raw-byte overflow.
        // The prefix remains complete here, including its original terminal.
        source.prefix = Some(copy_prefix(source.tail.as_slices()));
    }
    let publication = publisher
        .stage_source_once(
            operation,
            &path,
            Pin::new(&mut source),
            "application/octet-stream",
            "Shell output",
            access,
        )?
        .await;
    let (file, _hold) = match publication {
        Err(_) if source.failed => return Err(Error::Indeterminate(operation)),
        other => other?,
    };
    if source.terminal.is_none()
        || source.prefix.is_some()
        || source.pending.is_some()
        || file.volume() != publisher.volume()
        || file.path() != path
        || file.descriptor().byte_length() != source.tail.total_bytes()
    {
        // In particular, a retained once receipt cannot attest a new execution
        // that this source did not drain. Reconciliation belongs before launch.
        return Err(Error::Indeterminate(operation));
    }
    let mut result = source.result(Some(&spill))?;
    result.value["file"] = serde_json::to_value(file).map_err(|cause| {
        Error::Invalid(format!("shell spill receipt encoding failed: {cause}; recovery: reconcile the original shell publication"))
    })?;
    bounded_result(result, maximum_render_bytes)
}

enum Terminal {
    Exit(i32),
    Stopped(ExecStop),
}

struct ExecSource {
    output: ExecStream,
    tail: OutputTail,
    prefix: Option<OwnedBytes>,
    pending: Option<OwnedBytes>,
    terminal: Option<Terminal>,
    failed: bool,
}

impl ExecSource {
    fn prepare_spill(&mut self, bytes: Vec<u8>, stderr: bool) -> io::Result<bool> {
        nonempty(&bytes)?;
        let prefix = &mut self.prefix;
        let mut overflow = false;
        self.tail.observe_with(&bytes, stderr, |first, second| {
            overflow = true;
            if !first.is_empty() || !second.is_empty() {
                *prefix = Some(copy_prefix((first, second)));
            }
            Ok(())
        })?;
        if overflow {
            self.pending = Some(bytes.into());
            return Ok(true);
        }
        Ok(false)
    }

    fn finish(&mut self, frame: ExecOutput) -> io::Result<()> {
        self.terminal = Some(match frame {
            ExecOutput::ExitCode(code) => Terminal::Exit(code),
            ExecOutput::Stopped(stop)
                if stop.cleanup_completed && stop.kind != ExecStopKind::CaptureFailed =>
            {
                Terminal::Stopped(stop)
            }
            _ => {
                return Err(io::Error::other(
                    "shell capture has no completed terminal receipt; recovery: reconcile the original command without redispatch",
                ));
            }
        });
        Ok(())
    }

    fn result(&mut self, spill: Option<&str>) -> Result<ToolResult> {
        let (exit, stop) = match self.terminal.as_ref() {
            Some(Terminal::Exit(code)) => (Some(*code), None),
            Some(Terminal::Stopped(stop)) => {
                let kind = match stop.kind {
                    ExecStopKind::Timeout => "timeout",
                    ExecStopKind::Expired | ExecStopKind::ControlStopped => "control_stopped",
                    ExecStopKind::OutputLimit => "output_limit",
                    ExecStopKind::CaptureFailed => "capture_failed",
                };
                (None, Some(serde_json::json!({"kind": kind, "message": stop.message, "cleanup_completed": true})))
            }
            None => return Err(Error::Invalid("shell capture settlement lacks the original terminal; recovery: reconcile the original command".into())),
        };
        let (stdout, stderr, omitted) = self.tail.text();
        Ok(ToolResult {
            value: serde_json::json!({"success": exit == Some(0) && stop.is_none(),
                "exit_code": exit, "stop": stop, "stdout": stdout, "stderr": stderr,
                "omitted_bytes": omitted, "spill": spill}),
        })
    }

    fn poll_chunk(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        cancellation: &CancellationToken,
    ) -> Poll<io::Result<OwnedBytes>> {
        let result = self.poll_chunk_inner(context, maximum, cancellation);
        if matches!(result, Poll::Ready(Err(_))) {
            self.failed = true;
        }
        result
    }

    fn poll_chunk_inner(
        &mut self,
        context: &mut Context<'_>,
        maximum: usize,
        cancellation: &CancellationToken,
    ) -> Poll<io::Result<OwnedBytes>> {
        if cancellation.is_cancelled() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "shell capture cancelled; recovery: reconcile the original command",
            )));
        }
        if maximum == 0 && (self.prefix.is_some() || self.pending.is_some()) {
            return Poll::Ready(Err(io::Error::other(
                "shell capture source credit exhausted; recovery: retain the original source and fund its next read",
            )));
        }
        if let Some(bytes) =
            take_chunk(&mut self.prefix, maximum).or_else(|| take_chunk(&mut self.pending, maximum))
        {
            return Poll::Ready(Ok(bytes));
        }
        if self.terminal.is_some() {
            return Poll::Ready(Ok(OwnedBytes::from_static(b"")));
        }
        match ready!(futures::Stream::poll_next(self.output.as_mut(), context)) {
            Some(Ok(ExecOutput::Data(bytes))) => {
                nonempty(&bytes)?;
                self.tail.observe(&bytes, false)?;
                self.pending = Some(bytes.into());
            }
            Some(Ok(ExecOutput::Stderr(bytes))) => {
                nonempty(&bytes)?;
                self.tail.observe(&bytes, true)?;
                self.pending = Some(bytes.into());
            }
            Some(Ok(terminal)) => {
                self.finish(terminal)?;
                return Poll::Ready(Ok(OwnedBytes::from_static(b"")));
            }
            Some(Err(cause)) => {
                return Poll::Ready(Err(io::Error::other(format!(
                    "shell capture producer failed: {cause}; recovery: reconcile the original command without redispatch"
                ))));
            }
            None => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "shell capture producer ended without terminal; recovery: reconcile the original command without redispatch",
                )));
            }
        }
        if maximum == 0 {
            return Poll::Ready(Err(io::Error::other(
                "shell capture source credit exhausted; recovery: retain the original source and fund its next read",
            )));
        }
        Poll::Ready(take_chunk(&mut self.pending, maximum).ok_or_else(|| {
            io::Error::other(
                "shell capture lost its accepted frame; recovery: reconcile the original command",
            )
        }))
    }
}

impl ContentSource for ExecSource {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        destination: &mut [u8],
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Poll<io::Result<usize>> {
        let maximum = destination
            .len()
            .min(usize::try_from(budget.source_bytes_read).unwrap_or(destination.len()));
        let bytes = ready!(self.get_mut().poll_chunk(context, maximum, cancellation))?;
        destination[..bytes.len()].copy_from_slice(&bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_read_owned(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        maximum: usize,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Poll<io::Result<Option<OwnedSourceChunk>>> {
        let maximum = maximum.min(usize::try_from(budget.source_bytes_read).unwrap_or(maximum));
        self.get_mut()
            .poll_chunk(context, maximum, cancellation)
            .map(|result| {
                result.map(|bytes| {
                    let capacity = bytes.retained_capacity().unwrap_or(bytes.len()) as u64;
                    Some(OwnedSourceChunk::claimed(bytes, capacity))
                })
            })
    }
}

fn take_chunk(pending: &mut Option<OwnedBytes>, maximum: usize) -> Option<OwnedBytes> {
    let bytes = pending.take()?;
    if bytes.len() <= maximum {
        return Some(bytes);
    }
    *pending = Some(bytes.slice(maximum..));
    Some(bytes.slice(..maximum))
}

fn copy_prefix((first, second): (&[u8], &[u8])) -> OwnedBytes {
    let mut bytes = Vec::with_capacity(first.len() + second.len());
    bytes.extend_from_slice(first);
    bytes.extend_from_slice(second);
    bytes.into()
}

fn nonempty(bytes: &[u8]) -> io::Result<()> {
    if bytes.is_empty() {
        return Err(io::Error::other(
            "shell capture producer emitted empty data; recovery: omit empty producer frames and retain explicit termination",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::ContentSourceAdapter;
    use acyclic_fs::model::{CheckoutMode, GenerationSelector, Lifecycle, VolumeConfig};
    use futures::channel::mpsc;

    #[tokio::test]
    async fn original_exec_frames_stage_exactly_and_producer_loss_refuses()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let cancellation = CancellationToken::new();
        let volume = acyclic_fs::Fs::memory()
            .create_volume(
                VolumeConfig::portable(Lifecycle::Ephemeral),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let checkout = volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        // The first overflow may have an empty prefix, or must preserve a
        // complete initial prefix. Neither case can introduce an early EOF.
        for prefix in [b"".as_slice(), b"first".as_slice()] {
            for terminal in [true, false] {
                let (sender, input) = mpsc::unbounded();
                sender.unbounded_send(Ok(ExecOutput::Stderr(b"/last".to_vec())))?;
                if terminal {
                    sender.unbounded_send(Ok(ExecOutput::ExitCode(0)))?;
                }
                drop(sender);
                let mut source = ExecSource {
                    output: input.boxed(),
                    tail: OutputTail::new(5),
                    prefix: None,
                    pending: None,
                    terminal: None,
                    failed: false,
                };
                if !prefix.is_empty() {
                    assert!(!source.prepare_spill(prefix.to_vec(), false)?);
                }
                assert!(source.prepare_spill(b"continued".to_vec(), false)?);
                let mut adapter = ContentSourceAdapter::new(Pin::new(&mut source));
                let staged = checkout
                    .stage_content(&mut adapter, 64, WorkBudget::UNBOUNDED, &cancellation)
                    .await;
                if terminal {
                    let mut expected =
                        std::io::Cursor::new([prefix, b"continued", b"/last"].concat());
                    let original = checkout
                        .stage_content(&mut expected, 64, WorkBudget::UNBOUNDED, &cancellation)
                        .await?;
                    assert_eq!(staged?.value, original.value);
                    assert_eq!(source.tail.total_bytes(), (prefix.len() + 14) as u64);
                    assert_eq!(source.tail.text().1, "/last");
                } else {
                    assert!(staged.is_err());
                    assert!(source.failed);
                    assert!(source.terminal.is_none());
                }
            }
        }
        Ok(())
    }
}
