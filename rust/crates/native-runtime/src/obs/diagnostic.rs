use super::WaitSnapshot;
use std::{
    fmt,
    future::Future,
    io::{self, Write},
    pin::Pin,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::{Context, Poll},
};

/// A diagnostic producer ended without its ordinary output result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticFailure {
    /// The accepted job or service producer disappeared.
    ProducerDropped,
    /// The output implementation panicked; the service remains usable.
    WriterPanicked,
}

impl fmt::Display for DiagnosticFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ProducerDropped => "diagnostic producer dropped",
            Self::WriterPanicked => "diagnostic output panicked",
        })
    }
}

impl std::error::Error for DiagnosticFailure {}

/// Optional observation of physical diagnostic completion.
///
/// Dropping this does not wait, cancel the writer or release its owned snapshot.
/// Cancellation and cleanup must not await a potentially blocked output sink.
pub struct DiagnosticCompletion(tokio::sync::oneshot::Receiver<io::Result<()>>);

impl Future for DiagnosticCompletion {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().0).poll(context).map(|result| {
            result.unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    DiagnosticFailure::ProducerDropped,
                ))
            })
        })
    }
}

struct Permit(Arc<AtomicBool>);

impl Drop for Permit {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

struct Job {
    snapshot: WaitSnapshot,
    output: Box<dyn Write + Send>,
    result: tokio::sync::oneshot::Sender<io::Result<()>>,
    permit: Permit,
}

impl Job {
    fn run(self) {
        let Self {
            snapshot,
            mut output,
            result,
            permit,
        } = self;
        let written = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            // Batch formatting fragments into bounded output chunks. Extract
            // the parts even on error: BufWriter's Drop must not retry output.
            let mut buffered = io::BufWriter::with_capacity(8192, &mut output);
            let result = snapshot.write_to(&mut buffered);
            drop(buffered.into_parts());
            result
        }))
        .unwrap_or_else(|_| Err(io::Error::other(DiagnosticFailure::WriterPanicked)));
        // Retire the physical writer and snapshot before releasing admission
        // or publishing completion. Observer cancellation cannot bypass this.
        drop(permit);
        let _ = result.send(written);
    }
}

struct Service {
    sender: mpsc::SyncSender<Job>,
    busy: Arc<AtomicBool>,
}

impl Service {
    fn start() -> io::Result<Self> {
        let (sender, receiver) = mpsc::sync_channel::<Job>(1);
        std::thread::Builder::new()
            .name("acyclic-diagnostics".into())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    job.run();
                }
            })?;
        Ok(Self {
            sender,
            busy: Arc::new(AtomicBool::new(false)),
        })
    }

    fn submit(
        &self,
        snapshot: WaitSnapshot,
        output: impl Write + Send + 'static,
    ) -> io::Result<DiagnosticCompletion> {
        self.busy
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))?;
        let permit = Permit(Arc::clone(&self.busy));
        let (result, completion) = tokio::sync::oneshot::channel();
        self.sender
            .try_send(Job {
                snapshot,
                output: Box::new(output),
                result,
                permit,
            })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => io::ErrorKind::WouldBlock.into(),
                mpsc::TrySendError::Disconnected(_) => io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    DiagnosticFailure::ProducerDropped,
                ),
            })?;
        Ok(DiagnosticCompletion(completion))
    }
}

static SERVICE: LazyLock<io::Result<Service>> = LazyLock::new(Service::start);

/// Starts the one process-wide diagnostic service before accepting requests.
/// It owns one worker and admits at most one physical report, including output
/// blocked on a full pipe. It shares no workers with storage, capture or Tokio.
pub fn initialize_diagnostic_service() -> io::Result<()> {
    service().map(|_| ())
}

fn service() -> io::Result<&'static Service> {
    SERVICE
        .as_ref()
        .map_err(|error| io::Error::new(error.kind(), error.to_string()))
}

impl WaitSnapshot {
    /// Nonblocking admission of this owned observation and writer.
    ///
    /// Returns `WouldBlock` if the one physical report slot is occupied. No
    /// queue of observers or history is accumulated. Accepted work starts
    /// without an async runtime, even if the returned completion is dropped.
    /// Initialize the service at startup to exclude thread creation from this
    /// O(1) admission check and ownership handoff. Rendering and retiring a
    /// snapshot are O(W) in the captured waits; rejection retires the supplied
    /// snapshot on the caller. Rendering runs off coordination and flushes once
    /// at the report boundary, never for individual lines.
    pub fn try_write_to(
        self,
        output: impl Write + Send + 'static,
    ) -> io::Result<DiagnosticCompletion> {
        service()?.submit(self, output)
    }
}

#[cfg(test)]
mod tests;
