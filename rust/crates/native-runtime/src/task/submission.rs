//! One atomic ownership/notification handoff for a reusable submission.
use crate::sync::atomic::{AtomicUsize, Ordering};

const OWNED: usize = 1;
const NOTIFIED: usize = 2;

#[derive(Default)]
pub(crate) struct SubmissionHandoff(AtomicUsize);
impl SubmissionHandoff {
    /// Records the event and elects its submitter in the same atomic operation.
    pub(crate) fn request(&self) -> bool {
        self.0.fetch_or(OWNED | NOTIFIED, Ordering::AcqRel) & OWNED == 0
    }
    pub(crate) fn consume(&self) {
        self.0.fetch_and(!NOTIFIED, Ordering::AcqRel);
    }
    /// Releases ownership only if no event arrived since consumption.
    pub(crate) fn release(&self) -> bool {
        self.0
            .compare_exchange(OWNED, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

#[cfg(all(test, loom))]
mod tests {
    use super::*;
    use loom::sync::{Arc, atomic::AtomicBool};

    #[test]
    fn capacity_notification_survives_submission_release() {
        loom::model(|| {
            let handoff = Arc::new(SubmissionHandoff::default());
            let pending = Arc::new(AtomicBool::new(false));
            assert!(handoff.request());
            handoff.consume();
            let wake_handoff = handoff.clone();
            let wake_pending = pending.clone();
            let wake = loom::thread::spawn(move || {
                wake_pending.store(true, Ordering::Release);
                if wake_handoff.request() {
                    loop {
                        wake_handoff.consume();
                        wake_pending.swap(false, Ordering::AcqRel);
                        if wake_handoff.release() {
                            break;
                        }
                    }
                }
            });
            while !handoff.release() {
                handoff.consume();
                pending.swap(false, Ordering::AcqRel);
            }
            wake.join().unwrap();
            assert!(
                !pending.load(Ordering::Acquire),
                "capacity notification lost its submitter"
            );
        });
    }
}
