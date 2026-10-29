#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, Ordering};

/// Retirement is published before the runtime completes cancelled tasks.
pub(super) struct ShardHealth {
    available: AtomicBool,
    #[cfg(not(loom))]
    retired: tokio_util::sync::CancellationToken,
}

impl ShardHealth {
    pub(super) fn new() -> Self {
        Self {
            available: AtomicBool::new(true),
            #[cfg(not(loom))]
            retired: tokio_util::sync::CancellationToken::new(),
        }
    }

    pub(super) fn retire(&self) {
        self.available.store(false, Ordering::Release);
        #[cfg(not(loom))]
        self.retired.cancel();
    }

    pub(super) fn is_available(&self) -> bool {
        self.available.load(Ordering::Acquire)
    }

    #[cfg(not(loom))]
    pub(super) fn retired(&self) -> tokio_util::sync::WaitForCancellationFutureOwned {
        self.retired.clone().cancelled_owned()
    }
}
