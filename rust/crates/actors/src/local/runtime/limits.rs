//! Admission and cooperative work bounds, shared by continuous and event hosts.
use super::*;

/// Runtime scheduling bounds, independent of a package's resource policy.
/// Default admission is unbounded; an explicit bound represents host capacity.
/// The work chunk only yields and never limits total input or actor count.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeLimits {
    pub(super) active: Option<usize>,
    pub(super) work: usize,
}

impl RuntimeLimits {
    /// Admit at most `active` deliveries and perform at most `work` immediate
    /// steps before yielding. The same work bound limits each due-index batch.
    pub fn new(active: usize, work: usize) -> Result<Self, Error> {
        if active == 0 || active > tokio::sync::Semaphore::MAX_PERMITS || work == 0 {
            return Err(Error::Invalid(
                "runtime bounds must be positive and representable",
            ));
        }
        Ok(Self {
            active: Some(active),
            work,
        })
    }
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            active: None,
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
