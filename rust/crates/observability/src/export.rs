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

pub(crate) fn increment(counter: &AtomicU64) {
    // Saturation is explicit unknown evidence, never a wrapped zero.
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        value.checked_add(1)
    });
}

pub(crate) fn count(counter: &AtomicU64) -> Option<u64> {
    let value = counter.load(Ordering::Relaxed);
    (value != u64::MAX).then_some(value)
}

/// Independent output credit for terminal samples. Recording never waits for
/// credit or output I/O. The host drains the receiver using its existing loop;
/// this adapter creates no task, thread, timer or second operation lifecycle.
#[derive(Clone)]
pub struct Export {
    delivery: Delivery<OwnedSample>,
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
    /// Detail retains the first static `error.kind` and fixed producer-declared
    /// `work.*` totals. Paths, payloads, resource identities and error text must
    /// never be supplied as failure categories.
    pub fn channel(credit: Arc<Semaphore>, details: bool) -> (Self, ExportReceiver) {
        let (delivery, receiver) = Delivery::channel(credit);
        let export = Self { delivery, details };
        (export, ExportReceiver(receiver))
    }

    /// Returns event-fed accounting without examining queued records.
    pub fn stats(&self) -> ExportStats {
        self.delivery.stats()
    }
}

impl Export {
    fn publish(&self, make_sample: impl FnOnce() -> OwnedSample) {
        let Ok(admission) = self.delivery.admit() else {
            return;
        };
        let mut sample = make_sample();
        if !self.details {
            sample.error_kind = None;
            sample.work = None;
            sample.publication = None;
            sample.native_task = None;
            sample.capture_retirement = None;
            sample.binding = None;
            #[cfg(feature = "research")]
            {
                sample.interval = None;
            }
        }
        let _ = self.delivery.publish(admission, sample);
    }
}

impl Sink for Export {
    #[cfg(feature = "research")]
    fn interval_details(&self) -> bool {
        self.details
    }

    fn error_details(&self) -> bool {
        self.details
    }

    fn record(&self, sample: &Sample<'_>) {
        self.publish(|| OwnedSample {
            span_id: sample.span_id.clone(),
            parent_id: sample.parent_id.cloned(),
            durability: sample.durability,
            native_task: self.details.then_some(sample.native_task).flatten(),
            capture_retirement: self.details.then_some(sample.capture_retirement).flatten(),
            publication: self.details.then_some(sample.publication).flatten(),
            work: self.details.then_some(sample.work).flatten(),
            binding: self.details.then(|| sample.binding.cloned()).flatten(),
            metadata: sample.metadata,
            rpc: sample.rpc.map(str::to_owned),
            error_kind: if self.details {
                sample.error_kind.map(Into::into)
            } else {
                None
            },
            elapsed: sample.elapsed,
            #[cfg(feature = "research")]
            interval: self.details.then_some(sample.interval).flatten(),
            outcome: sample.outcome,
        });
    }

    fn record_owned(&self, sample: OwnedSample) {
        self.publish(|| sample);
    }

    fn work_details(&self) -> bool {
        self.details
    }
    fn publication_details(&self) -> bool {
        self.details
    }
    fn native_task_details(&self) -> bool {
        self.details
    }
    fn capture_retirement_details(&self) -> bool {
        self.details
    }
    fn binding_details(&self) -> bool {
        self.details
    }
}

/// One original terminal sample, with its output credit retained through drop.
/// It cannot be cloned into uncredited retained output.
pub struct ExportRecord(DeliveryRecord<OwnedSample>);

impl ExportRecord {
    /// Borrows the original evidence while retaining its credit.
    pub fn sample(&self) -> Sample<'_> {
        self.0.value.sample()
    }
}

pub(crate) struct Admission {
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
pub struct ExportReceiver(mpsc::UnboundedReceiver<DeliveryRecord<OwnedSample>>);

impl ExportReceiver {
    /// Waits on the original channel producer event; `None` is producer closure.
    pub async fn recv(&mut self) -> Option<ExportRecord> {
        self.0.recv().await.map(ExportRecord)
    }
}

// Typed samples and text use this same record admission and retirement path.
pub(crate) struct Delivery<T> {
    sender: mpsc::UnboundedSender<DeliveryRecord<T>>,
    credit: Arc<Semaphore>,
    counters: Arc<Counters>,
}

impl<T> Clone for Delivery<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            credit: self.credit.clone(),
            counters: self.counters.clone(),
        }
    }
}

pub(crate) enum Refusal {
    Full,
    Closed,
}

pub(crate) struct DeliveryRecord<T> {
    // Payload retires before credit, including on failed publication.
    pub(crate) value: T,
    _admission: Admission,
}

impl<T> Delivery<T> {
    pub(crate) fn channel(
        credit: Arc<Semaphore>,
    ) -> (Self, mpsc::UnboundedReceiver<DeliveryRecord<T>>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (
            Self {
                sender,
                credit,
                counters: Arc::default(),
            },
            receiver,
        )
    }

    pub(crate) fn stats(&self) -> ExportStats {
        ExportStats {
            retained: self.counters.retained.load(Ordering::Relaxed),
            high_water: self.counters.high_water.load(Ordering::Relaxed),
            full: count(&self.counters.full),
            closed: count(&self.counters.closed),
        }
    }

    pub(crate) fn full(&self) {
        increment(&self.counters.full);
    }

    pub(crate) fn closed(&self) {
        increment(&self.counters.closed);
    }

    pub(crate) fn admit(&self) -> Result<Admission, Refusal> {
        if self.sender.is_closed() {
            increment(&self.counters.closed);
            return Err(Refusal::Closed);
        }
        let permit = match self.credit.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) => {
                self.full();
                return Err(Refusal::Full);
            }
            Err(TryAcquireError::Closed) => {
                increment(&self.counters.closed);
                return Err(Refusal::Closed);
            }
        };
        let retained = self.counters.retained.fetch_add(1, Ordering::Relaxed) + 1;
        self.counters
            .high_water
            .fetch_max(retained, Ordering::Relaxed);
        Ok(Admission {
            _permit: permit,
            counters: self.counters.clone(),
        })
    }

    pub(crate) fn publish(&self, admission: Admission, value: T) -> Result<(), DeliveryRecord<T>> {
        match self.sender.send(DeliveryRecord {
            value,
            _admission: admission,
        }) {
            Ok(()) => Ok(()),
            Err(error) => {
                increment(&self.counters.closed);
                Err(error.0)
            }
        }
    }
}
