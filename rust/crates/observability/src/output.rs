//! Nonblocking diagnostic formatting into storage supplied by the caller.
use crate::{
    ExportStats,
    export::{Delivery, DeliveryRecord, Refusal, count, increment},
};
use std::{
    fmt,
    future::Future,
    io,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};
use tokio::io::{AsyncWrite, AsyncWriteExt as _};
use tokio::sync::{Semaphore, mpsc, watch};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{Layer, layer::Context};

/// The first formatting or physical output failure, retained without tracing it.
#[derive(Clone, Debug)]
pub enum OutputFailure {
    /// The original consumer closed, without a physical output failure receipt.
    ConsumerClosed,
    /// The failure producer disappeared without a physical write receipt.
    SourceLost,
    /// A formatter refused after admission; no partial record was published.
    Formatting,
    /// The original asynchronous writer failed. No successful receipt is inferred.
    Write(Arc<io::Error>),
}

impl fmt::Display for OutputFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConsumerClosed => f.write_str("diagnostic output consumer closed"),
            Self::SourceLost => f.write_str("diagnostic output failure producer lost"),
            Self::Formatting => f.write_str("diagnostic formatting failed"),
            Self::Write(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for OutputFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Write(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

/// Explicit refusal of one diagnostic; these are never operation outcomes.
#[derive(Clone, Debug)]
pub enum OutputRefusal {
    /// No record credit, or the admitted buffer could not hold this entire record.
    Full,
    /// The original consumer is closed.
    Closed,
    /// The original first formatter or sink failure remains terminal.
    Failed(OutputFailure),
}

impl fmt::Display for OutputRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => f.write_str("diagnostic output credit exhausted"),
            Self::Closed => f.write_str("diagnostic output consumer closed"),
            Self::Failed(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for OutputRefusal {}

#[derive(Default)]
struct Terminals {
    published: AtomicU64,
    pending: AtomicUsize,
    writer_accepted: AtomicU64,
    discarded: AtomicU64,
    incomplete: AtomicU64,
}

struct Storage {
    buffers: Mutex<Vec<Box<[u8]>>>,
    terminals: Terminals,
}

/// Record dispositions, never a count of bytes consumed or durable effects.
///
/// At quiescence, before saturation, `published = pending + writer_accepted +
/// discarded + incomplete`. Admission followed by format refusal or failed
/// channel publication is excluded. Individual atomics are not a transactional
/// snapshot; concurrent publication/retirement can temporarily cross observations.
/// Every cumulative counter saturates to `None`, never a wrapped count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TextStats {
    /// Successfully published records, excluding admission/format refusals.
    pub published: Option<u64>,
    /// Published records not yet retired, including records held after writing.
    pub pending: usize,
    /// The original writer accepted each entire record. For an unbuffered
    /// native sink this is kernel acceptance, never downstream consumption.
    /// A buffered writer's success cannot certify its own physical flush.
    pub writer_accepted: Option<u64>,
    /// Published records dropped while still queued, before consumer access.
    pub discarded: Option<u64>,
    /// A dequeued record retired without a successful original write receipt.
    /// Its accepted prefix is unknown; no byte count is inferred.
    pub incomplete: Option<u64>,
}

/// Caller-owned text storage and the same credited delivery used by [`crate::Export`].
///
/// Callbacks never perform OS output, await credit, or allocate formatting scratch.
/// Every record takes one original buffer before invoking its formatter. The
/// caller admits the buffer allocations and fixed carrier metadata; no default
/// capacity or implicit byte allowance exists. A record exceeding its buffer is
/// refused in full. User-supplied `Debug`/`Display` implementations remain
/// responsible for their own computation and allocations.
///
/// Use this layer directly rather than putting it behind `fmt::MakeWriter`:
/// tracing-subscriber's formatter allocates its TLS string before that hook.
#[derive(Clone)]
pub struct TextOutput {
    delivery: Delivery<Buffer>,
    buffers: Arc<Storage>,
    failure: watch::Sender<Option<OutputFailure>>,
}

impl TextOutput {
    /// Moves explicit storage into delivery. All buffers must have the same
    /// size, so admission never searches for a fitting buffer. Empty storage
    /// grants zero records. Initialization examines each supplied buffer once.
    /// Dropped records return their original allocation without clearing bytes.
    pub fn channel(buffers: Vec<Box<[u8]>>) -> io::Result<(Self, TextReceiver)> {
        if buffers.len() > Semaphore::MAX_PERMITS
            || buffers.iter().any(|buffer| {
                buffers
                    .first()
                    .is_some_and(|first| first.len() != buffer.len())
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "diagnostic storage needs uniform buffers and representable record credit",
            ));
        }
        let (delivery, receiver) = Delivery::channel(Arc::new(Semaphore::new(buffers.len())));
        let (failure, _) = watch::channel(None);
        let output = Self {
            delivery,
            buffers: Arc::new(Storage {
                buffers: Mutex::new(buffers),
                terminals: Terminals::default(),
            }),
            failure,
        };
        let receiver = TextReceiver {
            receiver,
            failure: output.failure.clone(),
        };
        Ok((output, receiver))
    }

    /// Event-fed retained/full/closed accounting, shared with typed exports.
    /// Buffer byte capacity is fixed by the original caller-supplied storage.
    pub fn stats(&self) -> ExportStats {
        self.delivery.stats()
    }

    /// Fixed event-fed terminal accounting on the same original record lifetime.
    pub fn terminal_stats(&self) -> TextStats {
        let counters = &self.buffers.terminals;
        TextStats {
            published: count(&counters.published),
            pending: counters.pending.load(Ordering::Relaxed),
            writer_accepted: count(&counters.writer_accepted),
            discarded: count(&counters.discarded),
            incomplete: count(&counters.incomplete),
        }
    }

    /// Formats directly into one admitted buffer. Exhausted credit invokes no
    /// formatter, copies no fields and performs no output work. Dequeue does
    /// not return credit; only the original record's retirement does.
    pub fn record(
        &self,
        format: impl FnOnce(&mut dyn fmt::Write) -> fmt::Result,
    ) -> Result<(), OutputRefusal> {
        if let Some(error) = self.failure.borrow().clone() {
            if matches!(error, OutputFailure::ConsumerClosed) {
                self.delivery.closed();
                return Err(OutputRefusal::Closed);
            }
            return Err(OutputRefusal::Failed(error));
        }
        let admission = self.delivery.admit().map_err(|refusal| match refusal {
            Refusal::Full => OutputRefusal::Full,
            Refusal::Closed => OutputRefusal::Closed,
        })?;
        // The semaphore and this stack are the same inventory. Neither a
        // formatter nor user destruction runs while its mutex is held.
        let bytes = self
            .buffers
            .buffers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()
            .expect("admitted diagnostic storage");
        let mut buffer = Buffer {
            bytes: Some(bytes),
            length: 0,
            overflow: false,
            disposition: Disposition::Unpublished,
            pool: self.buffers.clone(),
        };
        let formatted = format(&mut buffer);
        if buffer.overflow {
            self.delivery.full();
            return Err(OutputRefusal::Full);
        }
        if formatted.is_err() {
            retain_failure(&self.failure, || OutputFailure::Formatting);
            return Err(OutputRefusal::Failed(
                self.failure
                    .borrow()
                    .clone()
                    .expect("retained format failure"),
            ));
        }
        buffer.disposition = Disposition::Queued;
        self.buffers
            .terminals
            .pending
            .fetch_add(1, Ordering::Relaxed);
        match self.delivery.publish(admission, buffer) {
            Ok(()) => {
                increment(&self.buffers.terminals.published);
                Ok(())
            }
            Err(mut record) => {
                record.value.disposition = Disposition::Unpublished;
                self.buffers
                    .terminals
                    .pending
                    .fetch_sub(1, Ordering::Relaxed);
                Err(OutputRefusal::Closed)
            }
        }
    }

    /// Waits for the original first failure event. The returned observer holds
    /// no producer, record, buffer, operation or tracing span.
    pub fn failed(&self) -> impl Future<Output = OutputFailure> + Send + 'static {
        failed(self.failure.subscribe())
    }
}

impl<S: Subscriber> Layer<S> for TextOutput {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let _ = self.record(|output| {
            write!(
                output,
                "{} {}",
                event.metadata().level(),
                event.metadata().target()
            )?;
            let mut visitor = Fields {
                output,
                result: Ok(()),
            };
            event.record(&mut visitor);
            visitor.result?;
            writeln!(visitor.output)
        });
    }
}

struct Fields<'a> {
    output: &'a mut dyn fmt::Write,
    result: fmt::Result,
}
impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if self.result.is_ok() {
            self.result = write!(self.output, " {}={value:?}", field.name());
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Disposition {
    Unpublished,
    Queued,
    Held,
    Writing,
    WriterAccepted,
}

struct Buffer {
    bytes: Option<Box<[u8]>>,
    length: usize,
    overflow: bool,
    disposition: Disposition,
    pool: Arc<Storage>,
}
impl fmt::Write for Buffer {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        let bytes = self.bytes.as_mut().expect("live diagnostic storage");
        let Some(end) = self
            .length
            .checked_add(text.len())
            .filter(|end| *end <= bytes.len())
        else {
            self.overflow = true;
            return Err(fmt::Error);
        };
        bytes[self.length..end].copy_from_slice(text.as_bytes());
        self.length = end;
        Ok(())
    }
}
impl Drop for Buffer {
    fn drop(&mut self) {
        let counters = &self.pool.terminals;
        match self.disposition {
            Disposition::Unpublished => {}
            Disposition::Queued => increment(&counters.discarded),
            Disposition::Held | Disposition::Writing => increment(&counters.incomplete),
            Disposition::WriterAccepted => increment(&counters.writer_accepted),
        }
        if self.disposition != Disposition::Unpublished {
            counters.pending.fetch_sub(1, Ordering::Relaxed);
        }
        // The stack retains its original capacity: each live record owns one
        // removed entry, and return occurs before its shared permit retires.
        self.pool
            .buffers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(self.bytes.take().expect("original diagnostic storage"));
    }
}

/// One original text record, retaining storage and credit through physical write.
pub struct TextRecord(DeliveryRecord<Buffer>);
impl TextRecord {
    /// Borrows initialized bytes. This is not a receipt of consumer acceptance.
    pub fn bytes(&self) -> &[u8] {
        &self.0.value.bytes.as_ref().expect("live record")[..self.0.value.length]
    }

    /// Makes one write attempt through the original writer, retaining storage
    /// and credit even after success until this record drops. Dropping this
    /// future or returning an error leaves an incomplete record with an unknown
    /// accepted prefix. It cannot be retried under a fresh write identity.
    /// External writes using `bytes` cannot produce a completed disposition.
    pub async fn write_to<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> io::Result<()> {
        self.begin()?;
        writer.write_all(self.bytes()).await?;
        self.0.value.disposition = Disposition::WriterAccepted;
        Ok(())
    }

    /// Moves this original record, including its buffer and credit, into one
    /// owned write. `write` must return the SAME writer and record only after
    /// accepting all original bytes, and retain them through accepted physical
    /// work even if its observing future drops. It must not retry an unknown
    /// prefix. Native owned-write primitives provide that contract directly.
    ///
    /// Only an observed successful return marks writer acceptance. Observer
    /// loss leaves the record incomplete until its actual owner drops it; this
    /// future's drop cannot release storage still held by accepted work.
    pub async fn write_owned<W, F>(
        mut self,
        writer: W,
        write: impl FnOnce(W, Self) -> F,
    ) -> io::Result<(W, Self)>
    where
        F: Future<Output = io::Result<(W, Self)>>,
    {
        self.begin()?;
        let (writer, mut record) = write(writer, self).await?;
        record.0.value.disposition = Disposition::WriterAccepted;
        Ok((writer, record))
    }

    fn begin(&mut self) -> io::Result<()> {
        if self.0.value.disposition != Disposition::Held {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "diagnostic record already submitted; retain the original write outcome",
            ));
        }
        self.0.value.disposition = Disposition::Writing;
        Ok(())
    }
}

impl AsRef<[u8]> for TextRecord {
    fn as_ref(&self) -> &[u8] {
        self.bytes()
    }
}

/// Delivery for the host's existing loop, with no worker or shutdown service.
/// The host retains each record across its original asynchronous write. On
/// shutdown it closes this receiver and drops pending unsent records; Drop
/// never writes or flushes. Kernel-accepted prefixes remain accepted.
pub struct TextReceiver {
    receiver: mpsc::UnboundedReceiver<DeliveryRecord<Buffer>>,
    failure: watch::Sender<Option<OutputFailure>>,
}
impl TextReceiver {
    /// Drives delivery beside the SAME original operation/report/shutdown future.
    /// No task is spawned. Formatting or sink loss closes text admission while
    /// the original future remains runnable through its existing terminal policy.
    /// At that terminal, queued text is discarded and any pending write is
    /// incomplete, never certified delivered. The original result is preserved.
    ///
    /// The supplied AsyncWrite must retain no borrowed bytes after its pending
    /// future is dropped. Synchronous inherited sinks require their original
    /// owned-write completion instead; this method cannot establish that custody.
    pub async fn observe<F, W>(
        self,
        original: F,
        writer: &mut W,
    ) -> (F::Output, Option<OutputFailure>)
    where
        F: Future,
        W: AsyncWrite + Unpin,
    {
        self.observe_with(original, async |mut record| record.write_to(writer).await)
            .await
    }

    /// Drives the same delivery loop using an original owned-write adapter.
    /// The adapter receives the entire record, never a detached byte copy.
    /// Use [`TextRecord::write_owned`] to retain its credit through accepted
    /// native work and record only the original successful completion. Adapter
    /// loss is not a physical terminal; outstanding native custody survives it.
    pub async fn observe_with<F, D>(
        mut self,
        original: F,
        mut deliver: D,
    ) -> (F::Output, Option<OutputFailure>)
    where
        F: Future,
        D: AsyncFnMut(TextRecord) -> io::Result<()>,
    {
        enum First<R> {
            Original(R),
            Delivery,
            Failure(OutputFailure),
        }
        let failure = failed(self.failure.subscribe());
        tokio::pin!(original);
        let first = {
            let delivery = async {
                while let Some(record) = self.recv().await {
                    if let Err(error) = deliver(record).await {
                        self.fail(error);
                        break;
                    }
                }
            };
            tokio::pin!(delivery);
            tokio::select! {
                result = &mut original => First::Original(result),
                () = &mut delivery => First::Delivery,
                error = failure => First::Failure(error),
            }
        };
        // An error can be published inside the original future's completing
        // poll. Observe it independently of the select winner, before our own
        // normal close publishes ConsumerClosed. No recheck loop is needed.
        let retained = self.failure.borrow().clone();
        self.close();
        drop(self);
        match first {
            First::Original(result) => (result, retained),
            First::Delivery => (original.await, retained),
            First::Failure(error) => (original.await, retained.or(Some(error))),
        }
    }

    /// Waits on original record publication or producer closure.
    pub async fn recv(&mut self) -> Option<TextRecord> {
        self.receiver.recv().await.map(|mut record| {
            record.value.disposition = Disposition::Held;
            TextRecord(record)
        })
    }

    /// Rejects new records. Existing records can be drained or dropped by the
    /// original host; closing supplies no physical-output completion guarantee.
    pub fn close(&mut self) {
        self.receiver.close();
        retain_failure(&self.failure, || OutputFailure::ConsumerClosed);
    }

    /// Retains the original first write failure and closes admission. Formatting
    /// failure cannot be replaced by a later generic output error, or vice versa.
    pub fn fail(&mut self, error: io::Error) -> OutputFailure {
        retain_failure(&self.failure, || OutputFailure::Write(Arc::new(error)));
        self.close();
        self.failure
            .borrow()
            .clone()
            .expect("retained output failure")
    }
}

impl Drop for TextReceiver {
    fn drop(&mut self) {
        self.close();
    }
}

fn retain_failure(
    sender: &watch::Sender<Option<OutputFailure>>,
    make: impl FnOnce() -> OutputFailure,
) {
    sender.send_if_modified(|first| {
        if first.is_none() {
            *first = Some(make());
            true
        } else {
            false
        }
    });
}

async fn failed(mut receiver: watch::Receiver<Option<OutputFailure>>) -> OutputFailure {
    loop {
        if let Some(error) = receiver.borrow_and_update().clone() {
            return error;
        }
        if receiver.changed().await.is_err() {
            return OutputFailure::SourceLost;
        }
    }
}
