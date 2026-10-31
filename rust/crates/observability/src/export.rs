//! Credited delivery of the collector's original terminal samples.
use crate::{OwnedSample, Sample, Sink};
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError, mpsc};

#[derive(Default)]
struct Counters {
    retained: AtomicUsize,
    high_water: AtomicUsize,
    full: AtomicU64,
    closed: AtomicU64,
}

fn increment(counter: &AtomicU64) {
    // Saturation is explicit unknown evidence, never a wrapped zero.
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        value.checked_add(1)
    });
}

fn count(counter: &AtomicU64) -> Option<u64> {
    let value = counter.load(Ordering::Relaxed);
    (value != u64::MAX).then_some(value)
}

/// Independent output credit for terminal samples. Recording never waits for
/// credit or output I/O. The host drains the receiver using its existing loop;
/// this adapter creates no task, thread, timer or second operation lifecycle.
#[derive(Clone)]
pub struct Export {
    sender: mpsc::UnboundedSender<ExportRecord>,
    credit: Arc<Semaphore>,
    counters: Arc<Counters>,
    details: bool,
}

/// Exact admission counters for this exporter, independent of SLO outcomes.
/// Fields are individual atomic observations, not a transactional snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExportStats {
    /// Accepted records still being prepared, queued, or held by the consumer.
    pub retained: usize,
    /// Maximum simultaneously retained records, updated on every admission.
    /// This includes spikes entirely between calls to `stats`.
    pub high_water: usize,
    /// Samples refused for unavailable credit; `None` means counter saturation.
    pub full: Option<u64>,
    /// Samples refused after consumer or credit closure; `None` means saturation.
    pub closed: Option<u64>,
}

impl Export {
    /// Uses the caller's explicit record credit. A permit follows each record
    /// through consumer processing, not just until it leaves the queue. Every
    /// queued record owns a permit, so the delivery channel cannot grow beyond
    /// admitted credit. No copy is made before credit is acquired.
    ///
    /// Detail retains at most the first static `error.kind`; paths, payloads,
    /// resource identities and error text must never be supplied in that field.
    pub fn channel(credit: Arc<Semaphore>, details: bool) -> (Self, ExportReceiver) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let export = Self {
            sender,
            credit,
            counters: Arc::default(),
            details,
        };
        (export, ExportReceiver(receiver))
    }

    /// Returns event-fed accounting without examining queued records.
    pub fn stats(&self) -> ExportStats {
        ExportStats {
            retained: self.counters.retained.load(Ordering::Relaxed),
            high_water: self.counters.high_water.load(Ordering::Relaxed),
            full: count(&self.counters.full),
            closed: count(&self.counters.closed),
        }
    }
}

impl Export {
    fn publish(&self, make_sample: impl FnOnce() -> OwnedSample) {
        if self.sender.is_closed() {
            increment(&self.counters.closed);
            return;
        }
        let permit = match Arc::clone(&self.credit).try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                increment(&self.counters.full);
                return;
            }
            Err(TryAcquireError::Closed) => {
                increment(&self.counters.closed);
                return;
            }
        };
        let retained = self.counters.retained.fetch_add(1, Ordering::Relaxed) + 1;
        self.counters
            .high_water
            .fetch_max(retained, Ordering::Relaxed);
        // This owner exists before any copying, so unwinding cannot strand
        // credit or its retained count while a record is being constructed.
        let admission = Admission {
            _permit: permit,
            counters: Arc::clone(&self.counters),
        };
        let mut sample = make_sample();
        if !self.details {
            sample.error_kind = None;
        }
        let record = ExportRecord {
            sample,
            _admission: admission,
        };
        if self.sender.send(record).is_err() {
            increment(&self.counters.closed);
        }
    }
}

impl Sink for Export {
    fn error_details(&self) -> bool {
        self.details
    }

    fn record(&self, sample: &Sample<'_>) {
        self.publish(|| OwnedSample {
            span_id: sample.span_id.clone(),
            metadata: sample.metadata,
            rpc: sample.rpc.map(str::to_owned),
            error_kind: if self.details {
                sample.error_kind.map(Into::into)
            } else {
                None
            },
            elapsed: sample.elapsed,
            outcome: sample.outcome,
        });
    }

    fn record_owned(&self, sample: OwnedSample) {
        self.publish(|| sample);
    }
}

/// One original terminal sample, with its output credit retained through drop.
/// It cannot be cloned into uncredited retained output.
pub struct ExportRecord {
    sample: OwnedSample,
    // Fields drop in declaration order: owned evidence precedes its credit.
    _admission: Admission,
}

impl ExportRecord {
    /// Borrows the original evidence while retaining its credit.
    pub fn sample(&self) -> Sample<'_> {
        self.sample.sample()
    }
}

struct Admission {
    _permit: OwnedSemaphorePermit,
    counters: Arc<Counters>,
}

impl Drop for Admission {
    fn drop(&mut self) {
        // Retire the record before the permit becomes reusable.
        self.counters.retained.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Host-owned delivery end. Dropping it rejects future exports immediately;
/// dropping all exporters ends `recv` even when a consumer retains old records.
pub struct ExportReceiver(mpsc::UnboundedReceiver<ExportRecord>);

impl ExportReceiver {
    /// Waits on the original channel producer event; `None` is producer closure.
    pub async fn recv(&mut self) -> Option<ExportRecord> {
        self.0.recv().await
    }
}
