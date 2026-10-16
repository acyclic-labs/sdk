//! Shared wake observer for the real query-follow models.
use loom::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Wake, Waker};
struct Observed(Arc<AtomicBool>);
impl Wake for Observed {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}
pub(super) fn wake_flag() -> (Waker, Arc<AtomicBool>) {
    let flag = Arc::new(AtomicBool::new(false));
    (
        Waker::from(std::sync::Arc::new(Observed(Arc::clone(&flag)))),
        flag,
    )
}
