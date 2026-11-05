//! Credit-driven output handoff from the same native capture pump.
use super::*;
use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
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

type CaptureTurn<H> = (pump::Turn, Capture<H>, [Vec<u8>; 2]);

impl<H: CaptureHandler> Capture<H> {
    fn start(
        tree: ProcessTree,
        schedule: CaptureSchedule,
        max_bytes: Option<usize>,
        input: Option<OwnedCaptureInput>,
        handler: H,
        streaming: bool,
    ) -> Result<CaptureTurn<H>, CaptureOutcome<H>> {
        let (tree, mut pump, handler, wakes) =
            tree.start_capture(schedule, max_bytes, input, handler)?;
        if streaming {
            pump.stream_output();
        }
        Ok(Self {
            tree,
            pump,
            handler,
            wakes,
        }
        .turn(streaming))
    }

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

    fn turn(mut self, streaming: bool) -> CaptureTurn<H> {
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

    async fn reserve(
        mut self,
        sender: &mpsc::Sender<ProcessChunk>,
    ) -> io::Result<(io::Result<mpsc::Permit<'_, ProcessChunk>>, Self)> {
        let reserved = loop {
            match sender.try_reserve() {
                Ok(permit) => break Ok(permit),
                Err(mpsc::error::TrySendError::Closed(_)) => break Err(receiver_lost()),
                Err(mpsc::error::TrySendError::Full(_)) => {}
            }
            let control = match self.pump.arm_control(self.wakes.as_slice()) {
                Ok(control) => control,
                Err(error) => break Err(error),
            };
            let reserved = match control {
                Some(control) => {
                    control_first(async { control.await.into_result() }, sender.reserve()).await
                }
                None => Ok(Some(sender.reserve().await)),
            };
            match reserved {
                Ok(Some(Ok(permit))) => break Ok(permit),
                Ok(Some(Err(_))) => break Err(receiver_lost()),
                Err(error) => break Err(error),
                Ok(None) => {
                    let (checked, returned) =
                        crate::run_blocking_io(move || self.check(), None)?.await?;
                    self = returned;
                    if let Err(error) = checked {
                        break Err(error);
                    }
                }
            }
        };
        Ok((reserved, self))
    }
}

/// Completion of the original owned capture task. Dropping detaches its
/// observer; the task retains physical custody through cleanup. Cancellation
/// uses the original handler's stop event, never task abortion.
pub struct ProcessCapture<H: CaptureHandler> {
    pub(super) completion: crate::TaskHandle<io::Result<CaptureOutcome<H>>>,
}

impl<H: CaptureHandler> Future for ProcessCapture<H> {
    type Output = io::Result<CaptureOutcome<H>>;

    fn poll(self: Pin<&mut Self>, context: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().completion)
            .poll(context)
            .map(|result| result.and_then(std::convert::identity))
    }
}

/// The output receiver and original cleanup owner of one process tree.
/// Dropping it closes output credit, waking capture even for a quiet child.
/// The detached owner still joins the original tree before releasing custody.
pub struct ProcessCaptureStream<H: CaptureHandler> {
    receiver: mpsc::Receiver<ProcessChunk>,
    completion: ProcessCapture<H>,
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
        Pin::new(&mut self.completion).poll(context)
    }

    /// Joins physical cleanup and returns original callbacks and custody.
    /// Calling before output EOF cancels remaining output and the process.
    pub async fn finish(self) -> io::Result<CaptureOutcome<H>> {
        drop(self.receiver);
        self.completion.await
    }
}

/// Replays a scoped simulation's answer through the same credit channel:
/// its output as one batch, then its terminal outcome.
#[cfg(feature = "test-support")]
impl<H: CaptureHandler> ProcessCaptureStream<H> {
    pub(super) fn answered(spawner: &crate::TaskSpawner, mut outcome: CaptureOutcome<H>) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let (stdout, stderr) = match &mut outcome.result {
            Ok(output) => (&mut output.stdout, &mut output.stderr),
            Err(failure) => (&mut failure.stdout, &mut failure.stderr),
        };
        let chunk = ProcessChunk {
            stdout: std::mem::take(stdout),
            stderr: std::mem::take(stderr),
        };
        let completion = spawner.spawn(async move {
            if !(chunk.stdout.is_empty() && chunk.stderr.is_empty()) {
                // A closed receiver discards output, as for a live capture.
                drop(sender.send(chunk).await);
            }
            Ok(outcome)
        });
        Self {
            receiver,
            completion: ProcessCapture { completion },
        }
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
        max_bytes: impl Into<Option<usize>>,
        input: Option<OwnedCaptureInput>,
        handler: H,
    ) -> ProcessCaptureStream<H> {
        let (sender, receiver) = mpsc::channel(1);
        let completion = ProcessCapture {
            completion: spawner.spawn(self.capture_driven(
                schedule.into(),
                max_bytes.into(),
                input,
                handler,
                Some(sender),
            )),
        };
        ProcessCaptureStream {
            receiver,
            completion,
        }
    }

    pub(super) async fn capture_driven<H: CaptureHandler>(
        self,
        schedule: CaptureSchedule,
        max_bytes: Option<usize>,
        input: Option<OwnedCaptureInput>,
        handler: H,
        sender: Option<mpsc::Sender<ProcessChunk>>,
    ) -> io::Result<CaptureOutcome<H>> {
        let streaming = sender.is_some();
        // The initial output credit is the new, empty channel. Setup and the
        // first pipe turn share one worker submission with the owner callbacks.
        let started = crate::run_blocking_io(
            move || Capture::start(self, schedule, max_bytes, input, handler, streaming),
            None,
        )?
        .await?;
        let (turn, mut capture, bytes) = match started {
            Ok(started) => started,
            Err(outcome) => return Ok(outcome),
        };
        let mut initial = Some((turn, bytes));
        let result = loop {
            // Reserve before touching either pipe. The other wait contains
            // only control/deadline sources, so a full pipe cannot spin it.
            let permit = if let Some(sender) = sender.as_ref() {
                let (reserved, returned) = capture.reserve(sender).await?;
                capture = returned;
                match reserved {
                    Ok(permit) => Some(permit),
                    Err(error) => break Err(error),
                }
            } else {
                None
            };
            let (turn, bytes) = if let Some(initial) = initial.take() {
                initial
            } else {
                let (turn, returned, bytes) =
                    crate::run_blocking_io(move || capture.turn(streaming), None)?.await?;
                capture = returned;
                (turn, bytes)
            };
            let [stdout, stderr] = bytes;
            let yielded = !stdout.is_empty() || !stderr.is_empty();
            if let Some(permit) = permit
                && yielded
            {
                permit.send(ProcessChunk { stdout, stderr });
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
                        match control_first(
                            async {
                                sender.closed().await;
                                Ok(())
                            },
                            wait,
                        )
                        .await
                        {
                            Ok(Some(result)) => {
                                if let Err(error) = result.into_result() {
                                    break Err(error);
                                }
                            }
                            Ok(None) => break Err(receiver_lost()),
                            Err(error) => break Err(error),
                        }
                    } else if let Err(error) = wait.await.into_result() {
                        break Err(error);
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
async fn control_first<C: Future<Output = io::Result<()>>, F: Future>(
    control: C,
    work: F,
) -> io::Result<Option<F::Output>> {
    let mut control = pin!(control);
    let mut work = pin!(work);
    poll_fn(|context| {
        if let Poll::Ready(result) = control.as_mut().poll(context) {
            return Poll::Ready(result.map(|()| None));
        }
        work.as_mut().poll(context).map(|output| Ok(Some(output)))
    })
    .await
}

#[cfg(test)]
mod tests;
