//! One owner for native command output, cancellation and wall-clock expiry.
//! Extracted from adapter draft #1690 at 310e58e941e8dd82036cf415b52b66eab216b063.

use crate::{
    ExecCommand, ExecOutput, ExecStop, ExecStopKind, ExecStream, ExpirationPolicy, MachineId,
    MachineOutput, ProviderError,
};
use acyclic_native_runtime::WallAlarm;
use futures::{Stream, StreamExt, future::BoxFuture};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU8, Ordering},
    },
    task::{Context, Poll},
};
use tokio::sync::{Notify, mpsc, oneshot};

/// One dispatched guest command. Adapters provide a single stop operation;
/// dropping its output alone must never be mistaken for stopping the guest.
pub struct ExecSession {
    /// Exact guest output; the exit code follows both fully drained streams.
    pub output: ExecStream,
    /// Stops the guest process and its descendants. Never redispatches the command.
    pub stop: BoxFuture<'static, Result<(), ProviderError>>,
}

/// Collects binary stdout from one internal command, requiring a confirmed zero
/// exit. Stderr remains separate and is never mistaken for file contents.
pub async fn collect_success(
    mut output: ExecStream,
    step: &'static str,
) -> Result<Vec<u8>, ProviderError> {
    let mut data = Vec::new();
    while let Some(frame) = output.next().await {
        match frame? {
            ExecOutput::Stdout(bytes) => data.extend(bytes),
            ExecOutput::Stderr(_) => {}
            ExecOutput::ExitCode(0) => return Ok(data),
            ExecOutput::ExitCode(code) => {
                return Err(ProviderError::Rejected(format!(
                    "{step} failed with exit {code}; recovery: inspect the registered image and machine before retrying with a fresh operation"
                )));
            }
            ExecOutput::Stopped(stop) => {
                return Err(ProviderError::Rejected(format!(
                    "{step} stopped: {}; recovery: inspect the machine before retrying with a fresh operation",
                    stop.message
                )));
            }
        }
    }
    Err(ProviderError::Unavailable)
}

/// Holds a confirmed terminal code until transport-owned metadata is retired.
/// Cancellation still belongs to `owned_exec` while cleanup runs.
pub fn cleanup_before_exit(
    output: ExecStream,
    cleanup: impl Future<Output = Result<(), ProviderError>> + Send + 'static,
) -> ExecStream {
    futures::stream::try_unfold(
        (output, Some(Box::pin(cleanup))),
        |(mut output, mut cleanup)| async move {
            let Some(frame) = output.next().await else {
                return Ok(None);
            };
            let frame = frame?;
            if matches!(frame, ExecOutput::ExitCode(_))
                && let Some(cleanup) = cleanup.take()
            {
                cleanup.await?;
            }
            Ok(Some((frame, (output, cleanup))))
        },
    )
    .boxed()
}

const RUNNING: u8 = 0;
const CANCELLED: u8 = 1;
const EXPIRED: u8 = 2;
const FINISHED: u8 = 3;
const TIMED_OUT: u8 = 4;
const OUTPUT_LIMIT: u8 = 5;
const CAPTURE_FAILED: u8 = 6;
const RETAINED_BYTES: usize = 65_536;

#[derive(Default)]
struct Control {
    reason: AtomicU8,
    changed: Notify,
}

impl Control {
    fn stop(&self, reason: u8) {
        if self
            .reason
            .compare_exchange(RUNNING, reason, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.changed.notify_one();
        }
    }

    async fn stopped(&self) {
        let changed = self.changed.notified();
        if self.reason.load(Ordering::SeqCst) == RUNNING {
            changed.await;
        }
    }
}

struct OutputView {
    output: mpsc::Receiver<Result<ExecOutput, ProviderError>>,
    terminal: Option<oneshot::Receiver<Result<Option<ExecStop>, ProviderError>>>,
    control: Arc<Control>,
}

impl Stream for OutputView {
    type Item = Result<ExecOutput, ProviderError>;
    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.output.poll_recv(context) {
            Poll::Ready(None) => {}
            other => return other,
        }
        let Some(terminal) = self.terminal.as_mut() else {
            return Poll::Ready(None);
        };
        let result = match Pin::new(terminal).poll(context) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        self.terminal = None;
        match result {
            Ok(Ok(None)) => Poll::Ready(None),
            Ok(Ok(Some(stop))) => Poll::Ready(Some(Ok(ExecOutput::Stopped(stop)))),
            Ok(Err(error)) => Poll::Ready(Some(Err(error))),
            Err(_) => Poll::Ready(Some(Err(ProviderError::Unavailable))),
        }
    }
}

impl Drop for OutputView {
    fn drop(&mut self) {
        self.control.stop(CANCELLED);
    }
}

#[derive(Default)]
struct Console {
    start: u64,
    data: VecDeque<u8>,
}

/// Bounded observed console history shared by every remote machine transport.
#[derive(Clone, Default)]
pub struct ConsoleHistory(Arc<Mutex<BTreeMap<MachineId, Console>>>);

impl ConsoleHistory {
    fn append(&self, machine: MachineId, data: &[u8]) {
        let mut consoles = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let console = consoles.entry(machine).or_default();
        console.data.extend(data);
        let discarded = console.data.len().saturating_sub(RETAINED_BYTES);
        console.data.drain(..discarded);
        console.start = console.start.saturating_add(discarded as u64);
    }

    /// Reads observed bytes, exposing any gap after old output was discarded.
    pub fn read(
        &self,
        machine: MachineId,
        offset: u64,
        limit: u32,
    ) -> Result<MachineOutput, ProviderError> {
        if !(1..=crate::MAX_OUTPUT_READ_BYTES).contains(&limit) {
            return Err(ProviderError::Invalid(
                "console read admission failed; recovery: use limit 1..=65536".into(),
            ));
        }
        let consoles = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(console) = consoles.get(&machine) else {
            return Ok(MachineOutput {
                machine,
                offset,
                data: Vec::new(),
            });
        };
        let offset = offset.max(console.start);
        let skip = usize::try_from(offset - console.start).unwrap_or(usize::MAX);
        Ok(MachineOutput {
            machine,
            offset,
            data: console
                .data
                .iter()
                .skip(skip)
                .take(limit as usize)
                .copied()
                .collect(),
        })
    }
}

/// Returns immediately with a drop-cancelling output view. Cancellation ownership
/// is prepared before the first transport request. Output backpressure cannot
/// prevent cancellation or expiry from waking the owner.
pub fn owned_exec(
    machine: MachineId,
    command: &ExecCommand,
    machine_expiry_unix_ms: Option<u64>,
    history: ConsoleHistory,
    session: ExecSession,
) -> Result<ExecStream, ProviderError> {
    let now = now_unix_ms();
    let deadline = command
        .expiry_unix_ms()
        .into_iter()
        .chain(machine_expiry_unix_ms)
        .fold(
            now.saturating_add(u64::from(command.timeout_ms())),
            u64::min,
        );
    if deadline <= now {
        return Err(ProviderError::Rejected("guest command admission expired; recovery: acquire unexpired authority before dispatch".into()));
    }
    let control = Arc::new(Control::default());
    let wake = Arc::clone(&control);
    let reason = if command
        .expiry_unix_ms()
        .into_iter()
        .chain(machine_expiry_unix_ms)
        .any(|expiry| expiry <= now.saturating_add(u64::from(command.timeout_ms())))
    {
        EXPIRED
    } else {
        TIMED_OUT
    };
    let alarm = WallAlarm::register(deadline, Arc::new(move || wake.stop(reason)))
        .map_err(|error| ProviderError::Rejected(format!("guest expiry alarm registration failed: {error}; recovery: restore the OS wall-clock alarm service before dispatch")))?;
    let (sender, receiver) = mpsc::channel(8);
    let (terminal, terminal_result) = oneshot::channel();
    let owner = Arc::clone(&control);
    let maximum = command.maximum_output_bytes();
    tokio::spawn(async move {
        let result = drive(machine, session, &owner, &sender, history, maximum).await;
        // Disarm before publishing a terminal result. A late view drop cannot
        // signal a process that has already exited and been drained.
        drop(alarm);
        owner.reason.store(FINISHED, Ordering::SeqCst);
        let _ = terminal.send(result);
    });
    Ok(Box::pin(OutputView {
        output: receiver,
        terminal: Some(terminal_result),
        control,
    }))
}

async fn drive(
    machine: MachineId,
    mut session: ExecSession,
    control: &Control,
    sender: &mpsc::Sender<Result<ExecOutput, ProviderError>>,
    history: ConsoleHistory,
    maximum: Option<u64>,
) -> Result<Option<ExecStop>, ProviderError> {
    let mut remaining = maximum.unwrap_or(u64::MAX);
    let result: Result<(), ProviderError> = 'owner: loop {
        tokio::select! {
            biased;
            () = control.stopped() => break Err(ProviderError::Cancelled),
            frame = session.output.next() => {
                let frame = match frame {
                    Some(Ok(frame)) => frame,
                    Some(Err(error)) => break Err(error),
                    None => break Err(ProviderError::CommandIndeterminate(machine)),
                };
                let stderr = matches!(frame, ExecOutput::Stderr(_));
                match frame {
                    ExecOutput::Stdout(data) | ExecOutput::Stderr(data) => {
                    if data.is_empty() {
                        break Err(ProviderError::Rejected("guest output frame validation failed; recovery: inspect the provider transport without replaying the command".into()));
                    }
                    let allowed = data.len().min(usize::try_from(remaining).unwrap_or(usize::MAX));
                    remaining -= allowed as u64;
                    for bytes in data[..allowed].chunks(RETAINED_BYTES) {
                        history.append(machine, bytes);
                        let frame = if stderr { ExecOutput::Stderr(bytes.to_vec()) } else { ExecOutput::Stdout(bytes.to_vec()) };
                        if let Err(error) = send_frame(control, sender, frame).await {
                            break 'owner Err(error);
                        }
                    }
                    if allowed < data.len() {
                        control.stop(OUTPUT_LIMIT);
                        break Err(ProviderError::Cancelled);
                    }
                    },
                    ExecOutput::ExitCode(code) => {
                        if let Err(error) = send_frame(control, sender, ExecOutput::ExitCode(code)).await {
                            break Err(error);
                        }
                        return Ok(None);
                    }
                    ExecOutput::Stopped(stop) => return Ok(Some(stop)),
                }
            }
        }
    };
    // On any non-terminal stream failure, retire the guest before releasing its
    // owner. A lost cancellation reply is uncertainty, never permission to retry.
    control.stop(CAPTURE_FAILED);
    let cleanup = session.stop.await;
    let (kind, message) = match control.reason.load(Ordering::SeqCst) {
        EXPIRED => (
            ExecStopKind::Expired,
            "command authority expired".to_owned(),
        ),
        TIMED_OUT => (
            ExecStopKind::Timeout,
            "command capture timed out".to_owned(),
        ),
        OUTPUT_LIMIT => (
            ExecStopKind::OutputLimit,
            "command output allowance exceeded".to_owned(),
        ),
        CANCELLED => (
            ExecStopKind::ControlStopped,
            "command output view dropped".to_owned(),
        ),
        _ => (
            ExecStopKind::CaptureFailed,
            result.err().map_or_else(
                || "command capture failed".to_owned(),
                |error| error.to_string(),
            ),
        ),
    };
    let message = cleanup.as_ref().err().map_or(message.clone(), |error| {
        format!("{message}; cleanup failed: {error}")
    });
    Ok(Some(ExecStop {
        kind,
        message: message.chars().take(128).collect(),
        cleanup_completed: cleanup.is_ok(),
    }))
}

async fn send_frame(
    control: &Control,
    sender: &mpsc::Sender<Result<ExecOutput, ProviderError>>,
    frame: ExecOutput,
) -> Result<(), ProviderError> {
    tokio::select! {
        biased;
        () = control.stopped() => Err(ProviderError::Cancelled),
        sent = sender.send(Ok(frame)) => sent.map_err(|_| ProviderError::Cancelled),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use std::time::Duration;
    use tokio::sync::oneshot;

    fn machine() -> MachineId {
        MachineId::from_digest([1; 32])
    }
    fn command() -> Result<ExecCommand, ProviderError> {
        ExecCommand::new(vec!["true".into()], 5_000)
    }

    async fn stopped(receiver: oneshot::Receiver<()>) -> Result<(), Box<dyn std::error::Error>> {
        tokio::time::timeout(Duration::from_secs(1), receiver).await??;
        Ok(())
    }

    #[tokio::test]
    async fn drop_stops_a_start_before_its_reply_arrives() -> Result<(), Box<dyn std::error::Error>>
    {
        let (entered, accepted) = oneshot::channel();
        let (_reply, late_reply) = oneshot::channel::<()>();
        let (stop, observed) = oneshot::channel();
        let output = owned_exec(
            machine(),
            &command()?,
            None,
            ConsoleHistory::default(),
            ExecSession {
                output: stream::once(async move {
                    let _ = entered.send(());
                    late_reply.await.map_err(|_| ProviderError::Unavailable)?;
                    Ok(ExecOutput::ExitCode(0))
                })
                .boxed(),
                stop: Box::pin(async move {
                    let _ = stop.send(());
                    Ok(())
                }),
            },
        )?;
        accepted.await?;
        drop(output);
        stopped(observed).await
    }

    #[tokio::test]
    async fn quiet_unpolled_output_expires_on_the_wall_clock()
    -> Result<(), Box<dyn std::error::Error>> {
        let (stop, observed) = oneshot::channel();
        let command = command()?.with_expiry(super::now_unix_ms() + 50);
        let mut output = owned_exec(
            machine(),
            &command,
            None,
            ConsoleHistory::default(),
            ExecSession {
                output: stream::pending().boxed(),
                stop: Box::pin(async move {
                    let _ = stop.send(());
                    Ok(())
                }),
            },
        )?;
        stopped(observed).await?;
        assert!(matches!(
            output.next().await,
            Some(Ok(ExecOutput::Stopped(ExecStop {
                kind: ExecStopKind::Expired,
                cleanup_completed: true,
                ..
            })))
        ));
        assert!(output.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn backpressure_cannot_prevent_drop_cancellation()
    -> Result<(), Box<dyn std::error::Error>> {
        let (entered, accepted) = oneshot::channel();
        let (stop, observed) = oneshot::channel();
        let output = owned_exec(
            machine(),
            &command()?,
            None,
            ConsoleHistory::default(),
            ExecSession {
                output: stream::once(async move {
                    let _ = entered.send(());
                    Ok(ExecOutput::Stdout(vec![0; RETAINED_BYTES]))
                })
                .chain(stream::repeat(Ok(ExecOutput::Stdout(vec![
                    0;
                    RETAINED_BYTES
                ]))))
                .boxed(),
                stop: Box::pin(async move {
                    let _ = stop.send(());
                    Ok(())
                }),
            },
        )?;
        accepted.await?;
        tokio::task::yield_now().await;
        drop(output);
        stopped(observed).await
    }

    #[tokio::test]
    async fn transport_chunks_are_split_without_altering_streams_or_terminal_code()
    -> Result<(), Box<dyn std::error::Error>> {
        let data: Vec<u8> = (0..RETAINED_BYTES + 3)
            .map(|index| (index % 256) as u8)
            .collect();
        let history = ConsoleHistory::default();
        let mut output = owned_exec(
            machine(),
            &command()?,
            None,
            history.clone(),
            ExecSession {
                output: stream::iter([
                    Ok(ExecOutput::Stdout(data.clone())),
                    Ok(ExecOutput::Stderr(data.clone())),
                    Ok(ExecOutput::ExitCode(7)),
                ])
                .boxed(),
                stop: Box::pin(async { Err(ProviderError::Unavailable) }),
            },
        )?;
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        while let Some(frame) = output.next().await {
            match frame? {
                ExecOutput::Stdout(bytes) => {
                    assert!(bytes.len() <= RETAINED_BYTES);
                    stdout.extend(bytes);
                }
                ExecOutput::Stderr(bytes) => {
                    assert!(bytes.len() <= RETAINED_BYTES);
                    stderr.extend(bytes);
                }
                ExecOutput::ExitCode(code) => {
                    assert_eq!(code, 7);
                    break;
                }
                ExecOutput::Stopped(stop) => {
                    return Err(format!("unexpected stop: {stop:?}").into());
                }
            }
        }
        assert_eq!(stdout, data);
        assert_eq!(stderr, data);
        assert!(output.next().await.is_none());
        let retained = history.read(machine(), 0, 65_536)?;
        assert_eq!(retained.offset, (data.len() * 2 - RETAINED_BYTES) as u64);
        assert_eq!(
            retained.data,
            data.get(3..).ok_or("missing retained suffix")?
        );
        Ok(())
    }

    #[tokio::test]
    async fn terminal_exit_does_not_signal_an_exited_guest()
    -> Result<(), Box<dyn std::error::Error>> {
        let (stop, observed) = oneshot::channel();
        let history = ConsoleHistory::default();
        let mut output = owned_exec(
            machine(),
            &command()?,
            None,
            history.clone(),
            ExecSession {
                output: stream::iter([
                    Ok(ExecOutput::Stdout(vec![0, 255])),
                    Ok(ExecOutput::Stderr(b"err".to_vec())),
                    Ok(ExecOutput::ExitCode(7)),
                ])
                .boxed(),
                stop: Box::pin(async move {
                    let _ = stop.send(());
                    Ok(())
                }),
            },
        )?;
        assert_eq!(
            output.next().await,
            Some(Ok(ExecOutput::Stdout(vec![0, 255])))
        );
        assert_eq!(
            output.next().await,
            Some(Ok(ExecOutput::Stderr(b"err".to_vec())))
        );
        assert_eq!(output.next().await, Some(Ok(ExecOutput::ExitCode(7))));
        assert!(output.next().await.is_none());
        assert_eq!(
            history.read(machine(), 0, 65_536)?.data,
            [0, 255, b'e', b'r', b'r']
        );
        assert!(observed.await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn combined_output_limit_keeps_the_prefix_and_reports_failed_cleanup()
    -> Result<(), Box<dyn std::error::Error>> {
        let command = command()?.with_output_limit(3);
        let mut output = owned_exec(
            machine(),
            &command,
            None,
            ConsoleHistory::default(),
            ExecSession {
                output: stream::iter([
                    Ok(ExecOutput::Stdout(vec![0, 255])),
                    Ok(ExecOutput::Stderr(b"err".to_vec())),
                    Ok(ExecOutput::ExitCode(0)),
                ])
                .boxed(),
                stop: Box::pin(async { Err(ProviderError::Unavailable) }),
            },
        )?;
        assert_eq!(
            output.next().await,
            Some(Ok(ExecOutput::Stdout(vec![0, 255])))
        );
        assert_eq!(
            output.next().await,
            Some(Ok(ExecOutput::Stderr(b"e".to_vec())))
        );
        assert!(matches!(
            output.next().await,
            Some(Ok(ExecOutput::Stopped(ExecStop {
                kind: ExecStopKind::OutputLimit,
                cleanup_completed: false,
                ..
            })))
        ));
        assert!(output.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn timeout_is_distinct_from_authority_expiry() -> Result<(), Box<dyn std::error::Error>> {
        let command = ExecCommand::new(vec!["true".into()], 25)?;
        let mut output = owned_exec(
            machine(),
            &command,
            None,
            ConsoleHistory::default(),
            ExecSession {
                output: stream::pending().boxed(),
                stop: Box::pin(async { Ok(()) }),
            },
        )?;
        let frame = tokio::time::timeout(Duration::from_secs(1), output.next()).await?;
        assert!(matches!(
            frame,
            Some(Ok(ExecOutput::Stopped(ExecStop {
                kind: ExecStopKind::Timeout,
                cleanup_completed: true,
                ..
            })))
        ));
        assert!(output.next().await.is_none());
        Ok(())
    }
}

/// Resolves a fixed machine lifetime and refuses expired or idle deadlines.

pub fn expiration_deadline(
    policy: ExpirationPolicy,
    created_at: u64,
) -> Result<Option<u64>, ProviderError> {
    let deadline = match policy {
        ExpirationPolicy::Never => return Ok(None),
        ExpirationPolicy::AtUnixMs(deadline) => deadline,
        ExpirationPolicy::MaxAge(duration) => created_at.checked_add(u64::try_from(duration.as_millis()).map_err(|_| ProviderError::Invalid("machine expiration admission failed; recovery: use a duration representable in milliseconds".into()))?)
            .ok_or_else(|| ProviderError::Invalid("machine expiration overflow; recovery: choose a shorter maximum age".into()))?,
        ExpirationPolicy::Idle(_) => return Err(ProviderError::Unsupported("idle expiration admission failed; recovery: use MaxAge or AtUnixMs".into())),
    };
    if deadline <= now_unix_ms() {
        return Err(ProviderError::Rejected(
            "machine expiration admission failed; recovery: choose a future deadline".into(),
        ));
    }
    Ok(Some(deadline))
}

/// Shared real wall clock for native command admission and provider observations.
pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}
