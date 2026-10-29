//! Credit-driven output handoff from the same native capture pump.
use super::*;
use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;
use tokio::sync::mpsc;

/// Owned pipe buffers, moved from the pump without copying their payload.
/// Each channel contains at most one 64 KiB read.
pub struct ProcessChunk {
    /// Exact newly read standard-output bytes.
    pub stdout: Vec<u8>,
    /// Exact newly read standard-error bytes.
    pub stderr: Vec<u8>,
}

// Declaration order also governs failed submission and unwinding: physical
// tree cleanup precedes release of the handler's original funded custody.
struct Capture<H> {
    tree: ProcessTree,
    pump: pump::Pump<'static>,
    handler: H,
    wakes: CaptureWakes,
}

impl<H: CaptureHandler> Capture<H> {
    fn check(mut self) -> (io::Result<()>, Self) {
        let checked = (|| {
            for wake in self.wakes.as_slice() {
                wake.consume()?;
            }
            self.handler.check()?;
            self.pump.check_deadline()
        })();
        (checked, self)
    }

    fn turn(mut self, streaming: bool) -> (pump::Turn, Self, [Vec<u8>; 2]) {
        let turn = pump::handled_turn(
            &mut self.pump,
            &mut self.tree,
            &mut self.handler,
            self.wakes.as_slice(),
        );
        let bytes = if streaming {
            self.pump.take_output()
        } else {
            [Vec::new(), Vec::new()]
        };
        (turn, self, bytes)
    }

    fn finish(mut self, result: io::Result<()>) -> CaptureOutcome<H> {
        let result = self.pump.finish(&mut self.tree, result, None);
        drop(self.tree);
        CaptureOutcome::new(self.handler, result)
    }
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

    /// Polls the original credit receiver without allocating a reader future.
    pub fn poll_recv(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> Poll<Option<ProcessChunk>> {
        self.receiver.poll_recv(context)
    }

    /// Joins the original cleanup task without another completion channel.
    /// Calling before output EOF closes credit and cancels remaining capture.
    pub fn poll_finish(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<CaptureOutcome<H>>> {
        self.receiver.close();
        std::pin::Pin::new(&mut self.completion)
            .poll(context)
            .map(|result| result.and_then(std::convert::identity))
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
    /// `spawner` retains the invoking task's submission context, so a spawn
    /// worker can transfer custody before returning the output receiver.
    pub fn stream_capture<H: CaptureHandler>(
        self,
        spawner: &crate::TaskSpawner,
        schedule: impl Into<CaptureSchedule>,
        max_bytes: usize,
        input: Option<OwnedCaptureInput>,
        handler: H,
    ) -> ProcessCaptureStream<H> {
        let (sender, receiver) = mpsc::channel(1);
        let completion = spawner.spawn(self.capture_driven(
            schedule.into(),
            max_bytes,
            input,
            handler,
            Some(sender),
        ));
        ProcessCaptureStream {
            receiver,
            completion,
        }
    }

    pub(super) async fn capture_driven<H: CaptureHandler>(
        self,
        schedule: CaptureSchedule,
        max_bytes: usize,
        input: Option<OwnedCaptureInput>,
        handler: H,
        sender: Option<mpsc::Sender<ProcessChunk>>,
    ) -> io::Result<CaptureOutcome<H>> {
        let streaming = sender.is_some();
        // The initial output credit is the new, empty channel. Setup and the
        // first pipe turn share one worker submission with the owner callbacks.
        let started = crate::run_blocking_io(
            move || {
                self.start_capture(schedule, max_bytes, input, handler).map(
                    |(tree, mut pump, handler, wakes)| {
                        if streaming {
                            pump.stream_output();
                        }
                        Capture {
                            tree,
                            pump,
                            handler,
                            wakes,
                        }
                        .turn(streaming)
                    },
                )
            },
            None,
        )?
        .await?;
        let (turn, mut capture, bytes) = match started {
            Ok(started) => started,
            Err(outcome) => return Ok(outcome),
        };
        let mut initial = Some((turn, bytes));
        let result = 'capture: loop {
            // Reserve before touching either pipe. The other wait contains
            // only control/deadline sources, so a full pipe cannot spin it.
            let permit = if let Some(sender) = sender.as_ref() {
                Some(loop {
                    match sender.try_reserve() {
                        Ok(permit) => break permit,
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            break 'capture Err(receiver_lost());
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {}
                    }
                    let control = match capture.pump.arm_control(capture.wakes.as_slice()) {
                        Ok(control) => control,
                        Err(error) => break 'capture Err(error),
                    };
                    match control_first(control, sender.reserve()).await {
                        Some(Ok(permit)) => break permit,
                        Some(Err(_)) => break 'capture Err(receiver_lost()),
                        None => {
                            let (checked, returned) =
                                crate::run_blocking_io(move || capture.check(), None)?.await?;
                            capture = returned;
                            if let Err(error) = checked {
                                break 'capture Err(error);
                            }
                        }
                    }
                })
            } else {
                None
            };
            let (turn, bytes) = match initial.take() {
                Some(initial) => initial,
                None => {
                    let (turn, returned, bytes) =
                        crate::run_blocking_io(move || capture.turn(streaming), None)?.await?;
                    capture = returned;
                    (turn, bytes)
                }
            };
            let [stdout, stderr] = bytes;
            let yielded = !stdout.is_empty() || !stderr.is_empty();
            if let Some(permit) = permit {
                if yielded {
                    permit.send(ProcessChunk { stdout, stderr });
                }
            }
            match turn {
                pump::Turn::Done(result) => break result,
                pump::Turn::Wait if yielded => {}
                pump::Turn::Wait => {
                    let wait = match capture
                        .pump
                        .arm_with_wake(&mut capture.tree, Some(capture.wakes.as_slice()))
                    {
                        Ok(wait) => wait,
                        Err(error) => break Err(error),
                    };
                    if let Some(sender) = sender.as_ref() {
                        if control_first(sender.closed(), wait).await.is_none() {
                            break Err(receiver_lost());
                        }
                    } else {
                        wait.await;
                    }
                }
            }
        };
        crate::run_blocking_io(move || capture.finish(result), None)?.await
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
