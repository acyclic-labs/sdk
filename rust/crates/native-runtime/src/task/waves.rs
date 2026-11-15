//! One persistence job per wave of concurrent operations on one owner.
//!
//! Each [`spawn_persistence`](super::spawn_persistence) job costs a span, a
//! completion and a worker hop. An owner whose operations are short, such as
//! one append to a log under its lock, would otherwise pay that hop per
//! operation. Here one driver task takes everything queued so far as a wave
//! and applies it in one job, in submission order, so `n` concurrent
//! operations cost one job, not `n`.
//!
//! A wave is taken, and its results delivered, on the driver task, never on
//! the worker: what a wave holds and when its callers resume follow the task
//! schedule and the job's own completion, as a single job's would, so a
//! seeded simulation stays deterministic.
use std::sync::Arc;

/// Applies one operation on the worker, returning what resumes its caller.
type Operation = Box<dyn FnOnce() -> Delivery + Send>;
type Delivery = Box<dyn FnOnce() + Send>;

struct Queue {
    /// Applied in submission order.
    ordinary: Vec<Operation>,
    /// Applied after every ordinary operation of their wave.
    barriers: Vec<Operation>,
    /// Whether a driver task owns the queue. At most one does.
    driving: bool,
}

type Shared = parking_lot::Mutex<Queue>;

/// The persistence jobs of one owner whose operations apply serially, such as
/// appends to one log under its lock.
pub struct PersistenceWaves {
    queue: Arc<Shared>,
    medium: &'static str,
}

impl PersistenceWaves {
    /// An idle owner; `medium` names the producer in wait reports.
    #[must_use]
    pub fn new(medium: &'static str) -> Self {
        Self {
            queue: Arc::new(parking_lot::Mutex::new(Queue {
                ordinary: Vec::new(),
                barriers: Vec::new(),
                driving: false,
            })),
            medium,
        }
    }

    /// Applies `work` once, after every operation submitted before it.
    ///
    /// Submission is eager: once this returns, `work` runs even if the
    /// returned future is dropped. A submission from inside a persistence job
    /// is refused with [`std::io::ErrorKind::WouldBlock`] before it is queued,
    /// as [`spawn_persistence`](super::spawn_persistence) refuses one.
    pub fn submit<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = std::io::Result<T>> + Send + use<T> {
        self.enqueue(false, work)
    }

    /// Applies `work` after every ordinary operation of its wave, so one
    /// barrier, such as a flush, covers every write that wave applied. A
    /// barrier runs no earlier than [`submit`](Self::submit) would run it.
    pub fn barrier<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = std::io::Result<T>> + Send + use<T> {
        self.enqueue(true, work)
    }

    fn enqueue<T: Send + 'static>(
        &self,
        barrier: bool,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = std::io::Result<T>> + Send + use<T> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let accepted = if super::PERSISTENCE_JOB.with(|job| job.borrow().is_some()) {
            Err(std::io::Error::from(std::io::ErrorKind::WouldBlock))
        } else {
            let operation: Operation = Box::new(move || {
                let value = work();
                // A dropped receiver is a caller that stopped waiting; the
                // work itself has already applied.
                Box::new(move || drop(sender.send(value)))
            });
            let start = {
                let mut queue = self.queue.lock();
                if barrier {
                    queue.barriers.push(operation);
                } else {
                    queue.ordinary.push(operation);
                }
                !std::mem::replace(&mut queue.driving, true)
            };
            if start {
                drop(super::spawn(drive(
                    Driver(Arc::clone(&self.queue)),
                    self.medium,
                )));
            }
            Ok(())
        };
        async move {
            accepted?;
            receiver.await.map_err(|_| {
                std::io::Error::other("persistence wave failed before this operation applied")
            })
        }
    }
}

/// Owns a queue while its driver runs. Dropped unfinished, as when its task
/// is cancelled, it releases the queue and fails what it held: each waiting
/// caller sees an error, never a hang.
struct Driver(Arc<Shared>);

impl Drop for Driver {
    fn drop(&mut self) {
        let mut queue = self.0.lock();
        if !queue.driving {
            return;
        }
        queue.driving = false;
        let pending = (
            std::mem::take(&mut queue.ordinary),
            std::mem::take(&mut queue.barriers),
        );
        drop(queue);
        drop(pending);
    }
}

async fn drive(driver: Driver, medium: &'static str) {
    let key = super::PersistenceKey::new(Arc::as_ptr(&driver.0) as usize, 0);
    loop {
        let wave = {
            let mut queue = driver.0.lock();
            if queue.ordinary.is_empty() && queue.barriers.is_empty() {
                queue.driving = false;
                return;
            }
            let mut wave = std::mem::take(&mut queue.ordinary);
            wave.append(&mut queue.barriers);
            wave
        };
        // A failed job drops the operations it did not apply, failing their
        // callers; the next wave still runs.
        if let Ok(deliveries) = super::spawn_persistence(medium, "wave", key, move || {
            wave.into_iter()
                .map(|operation| operation())
                .collect::<Vec<_>>()
        })
        .await
        {
            for delivery in deliveries {
                delivery();
            }
        }
    }
}
