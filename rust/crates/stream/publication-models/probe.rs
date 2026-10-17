use loom::sync::atomic::{AtomicUsize, Ordering};
use std::{
    sync::Arc,
    task::{Wake, Waker},
};

pub(crate) struct Probe {
    pub(crate) count: AtomicUsize,
}
impl Wake for Probe {
    fn wake(self: Arc<Self>) {
        crate::change::sync::assert_unlocked();
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}
pub(crate) fn probe() -> (Arc<Probe>, Waker) {
    let probe = Arc::new(Probe {
        count: AtomicUsize::new(0),
    });
    let wake = Waker::from(Arc::clone(&probe));
    (probe, wake)
}
