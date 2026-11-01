//! Nonblocking diagnostic formatting into storage supplied by the caller.
use crate::{
    ExportStats,
    export::{Delivery, DeliveryRecord, Refusal},
};
use std::{
    fmt,
    future::Future,
    io,
    sync::{Arc, Mutex, PoisonError},
};
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
    /// A formatter refused after admission; no partial record was published.
    Formatting,
    /// The original asynchronous writer failed. No successful receipt is inferred.
    Write(Arc<io::Error>),
}

impl fmt::Display for OutputFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConsumerClosed => f.write_str("diagnostic output consumer closed"),
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

type Buffers = Arc<Mutex<Vec<Box<[u8]>>>>;

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
    buffers: Buffers,
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
            buffers: Arc::new(Mutex::new(buffers)),
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
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()
            .expect("admitted diagnostic storage");
        let mut buffer = Buffer {
            bytes: Some(bytes),
            length: 0,
            overflow: false,
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
        if self.delivery.publish(admission, buffer) {
            Ok(())
        } else {
            Err(OutputRefusal::Closed)
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

struct Buffer {
    bytes: Option<Box<[u8]>>,
    length: usize,
    overflow: bool,
    pool: Buffers,
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
        // The stack retains its original capacity: each live record owns one
        // removed entry, and return occurs before its shared permit retires.
        self.pool
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
    /// Waits on original record publication or producer closure.
    pub async fn recv(&mut self) -> Option<TextRecord> {
        self.receiver.recv().await.map(TextRecord)
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
            return OutputFailure::Write(Arc::new(io::ErrorKind::BrokenPipe.into()));
        }
    }
}
