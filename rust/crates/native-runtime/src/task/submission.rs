//! One atomic handoff for a retained job's capacity notifications.
use crate::sync::atomic::{AtomicUsize, Ordering};

const RUNNING: usize = 1;
const NOTIFIED: usize = 2;

#[derive(Default)]
pub(super) struct SubmissionHandoff(AtomicUsize);

impl SubmissionHandoff {
    /// Publish the wake and claim its submission in the same operation.
    pub(super) fn request(&self) -> bool {
        self.0.fetch_or(RUNNING | NOTIFIED, Ordering::AcqRel) & RUNNING == 0
    }

    pub(super) fn begin(&self) {
        self.0.fetch_and(!NOTIFIED, Ordering::AcqRel);
    }

    /// Release only if no notification arrived during the submission.
    pub(super) fn release(&self) -> bool {
        self.0
            .compare_exchange(RUNNING, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}
