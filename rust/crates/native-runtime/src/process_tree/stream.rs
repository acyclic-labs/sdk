//! Credit-driven output handoff from the same native capture pump.
use super::*;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;
use tokio::sync::mpsc;

/// Owned pipe buffers, moved from the pump without copying their payload.
/// Each channel contains at most one 64 KiB read.
pub struct ProcessChunk {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// The output receiver and original cleanup owner of one process tree.
/// Dropping it closes output credit, waking capture even for a quiet child.
/// The detached owner still joins the original tree before releasing custody.
pub struct ProcessCaptureStream<H: CaptureHandler> {
    receiver: mpsc::Receiver<ProcessChunk>,
    completion: crate::TaskHandle<io::Result<CaptureOutcome<H>>>,
}

impl<H: CaptureHandler> ProcessCaptureStream<H> {
    /// Takes one bounded pair of buffers. `None` means the producer finished;
    /// use `finish` to distinguish its terminal outcome from producer loss.
    pub async fn recv(&mut self) -> Option<ProcessChunk> {
        self.receiver.recv().await
    }

    /// Joins physical cleanup and returns original callbacks and custody.
    /// Calling before output EOF cancels remaining output and the process.
    pub async fn finish(self) -> io::Result<CaptureOutcome<H>> {
        drop(self.receiver);
        self.completion.await?
    }
}

impl ProcessTree {
    /// Streams from the existing capture pump with one queued output batch.
    /// A slow reader holds no worker and admits no more pipe reads. Owner
    /// notifications, the original deadline and receiver loss remain active
    /// while credit is withheld. No process or payload copy is introduced.
    pub fn stream_capture<H: CaptureHandler>(
        self,
        timeout: Duration,
        max_bytes: usize,
        input: Option<OwnedCaptureInput>,
        handler: H,
    ) -> ProcessCaptureStream<H> {
        let (sender, receiver) = mpsc::channel(1);
        let completion = crate::spawn(self.capture_streamed(
            CaptureSchedule::new(timeout),
            max_bytes,
            input,
            handler,
            sender,
        ));
        ProcessCaptureStream {
            receiver,
            completion,
        }
    }

    async fn capture_streamed<H: CaptureHandler>(
        self,
        schedule: CaptureSchedule,
        max_bytes: usize,
        input: Option<OwnedCaptureInput>,
        handler: H,
        sender: mpsc::Sender<ProcessChunk>,
    ) -> io::Result<CaptureOutcome<H>> {
        let (mut tree, mut pump, mut handler) = match self
            .start_capture(schedule, max_bytes, input, handler)
            .await?
        {
            Ok(started) => started,
            Err(outcome) => return Ok(outcome),
        };
        pump.stream_output();
        let result = 'capture: loop {
            // Reserve before touching either pipe. The other wait contains
            // only control/deadline sources, so a full pipe cannot spin it.
            let permit = loop {
                match sender.try_reserve() {
                    Ok(permit) => break Ok(permit),
                    Err(mpsc::error::TrySendError::Closed(_)) => break Err(receiver_lost()),
                    Err(mpsc::error::TrySendError::Full(_)) => {}
                }
                let control = match pump.arm_control(handler.wakes()) {
                    Ok(control) => control,
                    Err(error) => break 'capture Err(error),
                };
                match control_first(control, sender.reserve()).await {
                    Some(Ok(permit)) => break Ok(permit),
                    Some(Err(_)) => break Err(receiver_lost()),
                    None => {
                        let (checked, returned) = crate::run_blocking_io(
                            move || {
                                let checked = (|| {
                                    for wake in handler.wakes() {
                                        wake.consume()?;
                                    }
                                    handler.check()?;
                                    pump.check_deadline()
                                })();
                                (checked, (tree, pump, handler))
                            },
                            None,
                        )?
                        .await?;
                        (tree, pump, handler) = returned;
                        if let Err(error) = checked {
                            break Err(error);
                        }
                    }
                }
            };
            let permit = match permit {
                Ok(permit) => permit,
                Err(error) => break Err(error),
            };
            let (turn, returned, bytes) = crate::run_blocking_io(
                move || {
                    let turn = pump::handled_turn(&mut pump, &mut tree, &mut handler);
                    let bytes = pump.take_output();
                    (turn, (tree, pump, handler), bytes)
                },
                None,
            )?
            .await?;
            (tree, pump, handler) = returned;
            let [stdout, stderr] = bytes;
            let yielded = !stdout.is_empty() || !stderr.is_empty();
            if yielded {
                permit.send(ProcessChunk { stdout, stderr });
            } else {
                drop(permit);
            }
            match turn {
                pump::Turn::Done(result) => break result,
                pump::Turn::Wait if yielded => {}
                pump::Turn::Wait => {
                    let wait = match pump.arm_with_wake(&mut tree, Some(handler.wakes())) {
                        Ok(wait) => wait,
                        Err(error) => break Err(error),
                    };
                    if control_first(sender.closed(), wait).await.is_none() {
                        break Err(receiver_lost());
                    }
                }
            }
        };
        crate::run_blocking_io(
            move || {
                let result = pump.finish(&mut tree, result, None);
                drop(tree);
                CaptureOutcome { handler, result }
            },
            None,
        )?
        .await
    }
}

fn receiver_lost() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "process output receiver closed")
}

// The stop source wins a simultaneous credit/readiness notification.
async fn control_first<C: Future<Output = ()>, F: Future>(
    control: C,
    work: F,
) -> Option<F::Output> {
    let mut control = pin!(control);
    let mut work = pin!(work);
    poll_fn(|context| {
        if control.as_mut().poll(context).is_ready() {
            return Poll::Ready(None);
        }
        work.as_mut().poll(context).map(Some)
    })
    .await
}

#[cfg(test)]
mod tests;
