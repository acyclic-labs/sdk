use super::*;

/// Borrowed framework-owned paths, valid only during their native callback.
pub(crate) struct EventBatch<'a> {
    pub(super) paths: &'a [*const c_char],
    pub(super) flags: &'a [u32],
    #[cfg(test)]
    pub(super) ids: &'a [u64],
}

impl<'a> EventBatch<'a> {
    pub(crate) fn iter(self) -> impl ExactSizeIterator<Item = (&'a Path, u32)> {
        self.paths.iter().zip(self.flags).map(|(&path, &flags)| {
            // SAFETY: the callback guarantees a live NUL-terminated string for
            // each path. No owned path or scratch buffer is created here.
            let bytes = unsafe { CStr::from_ptr(path) }.to_bytes();
            (Path::new(OsStr::from_bytes(bytes)), flags)
        })
    }

    /// Each event's id, in delivery order.
    #[cfg(test)]
    pub(crate) fn ids(&self) -> &'a [u64] {
        self.ids
    }
}

pub(crate) type BorrowedEventHandlerFn = dyn for<'a> Fn(EventBatch<'a>) + Send + Sync;

thread_local! {
    static ON_QUEUE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Marks the current thread as running the hub's serial queue, which it must
/// not then wait for.
pub(super) struct OnQueue(bool);

impl OnQueue {
    pub(super) fn enter() -> Self {
        Self(ON_QUEUE.with(|queue| queue.replace(true)))
    }
}

impl Drop for OnQueue {
    fn drop(&mut self) {
        ON_QUEUE.with(|queue| queue.set(self.0));
    }
}

pub(super) fn on_queue() -> bool {
    ON_QUEUE.with(std::cell::Cell::get)
}

pub(super) extern "C" fn drain(_: *mut c_void) {}

pub(super) extern "C" fn terminal_suffix(info: *mut c_void) {
    let _queue = OnQueue::enter();
    // SAFETY: defer_terminal gave the hub's serial queue exactly one raw Arc
    // reference. No callback can still be accessing its queue-local slot.
    let registration = unsafe { Arc::from_raw(info.cast::<Registration>()) };
    // SAFETY: this terminal suffix is the serial queue's sole remaining slot consumer.
    let Some(owner) = unsafe { &mut *registration.terminal.get() }.take() else {
        std::process::abort();
    };
    // A terminal that panics must not unwind into the queue.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if let Some(birth) = owner.birth.clone() {
            let mut work = crate::NativeMaterializationWork::clone(&birth);
            owner.finish_deferred(registration, &mut work);
        } else {
            let cancellation = crate::CancellationToken::new();
            let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
            owner.finish_deferred(registration, &mut work);
        }
    }));
}
