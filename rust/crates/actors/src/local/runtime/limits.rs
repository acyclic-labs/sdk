//! Admission and cooperative work bounds, shared by continuous and event hosts.
use super::*;

/// Runtime scheduling credit, independent of a package's resource policy.
/// The default has no active-delivery ceiling; work batches remain chunked.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeLimits {
    pub(super) active: [Option<usize>; 2],
    pub(super) work: usize,
}

impl RuntimeLimits {
    /// Reserve at most `active` ordinary deliveries and perform at most `work`
    /// immediate steps before yielding. Controls have their own capacity; the
    /// same work chunk limits each due-index batch. Default has no reservation.
    pub fn new(active: usize, work: usize) -> Result<Self, Error> {
        if active == 0 || active > tokio::sync::Semaphore::MAX_PERMITS || work == 0 {
            return Err(Error::Invalid(
                "runtime bounds must be positive and representable",
            ));
        }
        Ok(Self {
            active: [Some(active), None],
            work,
        })
    }

    /// Independently reserve control deliveries. None is truly unbounded.
    pub fn with_control_capacity(mut self, capacity: Option<usize>) -> Result<Self, Error> {
        if capacity
            .is_some_and(|capacity| capacity == 0 || capacity > tokio::sync::Semaphore::MAX_PERMITS)
        {
            return Err(Error::Invalid(
                "control capacity must be positive and representable",
            ));
        }
        self.active[1] = capacity;
        Ok(self)
    }

    pub(super) fn allows(self, class: usize, running: usize) -> bool {
        self.active[class].is_none_or(|capacity| running < capacity)
    }
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            active: [None, None],
            work: 64,
        }
    }
}

/// Yield to the executor without a timer or a new task.
pub(super) async fn yield_now() {
    let mut yielded = false;
    poll_fn(|context| {
        if std::mem::replace(&mut yielded, true) {
            Poll::Ready(())
        } else {
            context.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}
