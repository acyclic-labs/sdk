use super::*;

/// Borrowed framework-owned paths, valid only during their native callback.
pub(crate) struct EventBatch<'a> {
    paths: &'a [*const c_char],
    flags: &'a [u32],
    ids: &'a [u64],
}

impl<'a> EventBatch<'a> {
    pub(crate) fn last_id(&self) -> Option<u64> {
        self.ids.iter().copied().max()
    }

    pub(crate) fn ids_wrapped(&self) -> bool {
        self.flags.iter().any(|flags| flags & 0x0000_0008 != 0)
    }

    pub(crate) fn iter(self) -> impl ExactSizeIterator<Item = (&'a Path, u32)> {
        self.paths.iter().zip(self.flags).map(|(&path, &flags)| {
            // SAFETY: the callback guarantees a live NUL-terminated string for
            // each path. No owned path or scratch buffer is created here.
            let bytes = unsafe { CStr::from_ptr(path) }.to_bytes();
            (Path::new(OsStr::from_bytes(bytes)), flags)
        })
    }
}

pub(crate) type BorrowedEventHandlerFn = dyn for<'a> Fn(EventBatch<'a>) + Send + Sync;

thread_local! {
    pub(super) static ACTIVE_STREAM: std::cell::Cell<FsEventStreamRef> = const { std::cell::Cell::new(std::ptr::null_mut()) };
}

struct ActiveStream(FsEventStreamRef);

impl Drop for ActiveStream {
    fn drop(&mut self) {
        ACTIVE_STREAM.with(|active| active.set(self.0));
    }
}

/// What a stream's callback reads; the stream owns it and frees it on release.
pub(super) struct StreamContext {
    pub(super) handler: Arc<BorrowedEventHandlerFn>,
    // Only the original serial callback queue accesses this preallocated slot.
    pub(super) terminal: std::cell::UnsafeCell<Option<EventStream>>,
}

// SAFETY: handlers are Send+Sync; the terminal slot has a single queue-local
// writer/consumer and every raw reference has a real Arc strong owner.
unsafe impl Send for StreamContext {}
// SAFETY: shared callbacks never access the queue-local terminal slot concurrently.
unsafe impl Sync for StreamContext {}

pub(super) extern "C" fn release_context(info: *const c_void) {
    // SAFETY: stream creation transferred exactly one Arc strong reference;
    // the native release callback consumes that reference exactly once.
    drop(unsafe { Arc::from_raw(info.cast::<StreamContext>()) });
}

pub(super) extern "C" fn drain(_: *mut c_void) {}

pub(super) extern "C" fn terminal_suffix(info: *mut c_void) {
    // SAFETY: defer_terminal gave this original serial queue exactly one raw
    // Arc reference. No callback can still be accessing its queue-local slot.
    let context = unsafe { Arc::from_raw(info.cast::<StreamContext>()) };
    // SAFETY: this terminal suffix is the serial queue's sole remaining slot consumer.
    let Some(owner) = unsafe { &mut *context.terminal.get() }.take() else {
        std::process::abort();
    };
    if let Some(birth) = owner.birth.clone() {
        let mut work = crate::NativeMaterializationWork::clone(&birth);
        owner.finish_deferred(context, &mut work);
    } else {
        let cancellation = crate::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        owner.finish_deferred(context, &mut work);
    }
}

pub(super) extern "C" fn stream_callback(
    stream: FsEventStreamRef,
    info: *mut c_void,
    event_count: usize,
    event_paths: *mut c_void,
    event_flags: *const u32,
    event_ids: *const u64,
) {
    // SAFETY: `info` is the stream's live `StreamContext`, and without
    // `kFSEventStreamCreateFlagUseCFTypes` the paths are an array of
    // `event_count` C strings, with as many flags.
    let (context, paths, flags, ids) = unsafe {
        (
            &*info.cast::<StreamContext>(),
            std::slice::from_raw_parts(event_paths.cast::<*const c_char>(), event_count),
            std::slice::from_raw_parts(event_flags, event_count),
            std::slice::from_raw_parts(event_ids, event_count),
        )
    };
    let _active = ActiveStream(ACTIVE_STREAM.with(|active| active.replace(stream)));
    (context.handler)(EventBatch { paths, flags, ids });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_search_fsevents_keeps_batch_ids_and_detects_wrapped_history() {
        let paths = [c"/fixture/one".as_ptr(), c"/fixture/two".as_ptr()];
        let ids = [42, 41];
        let flags = [ITEM_CREATED, ITEM_REMOVED];
        let batch = EventBatch {
            paths: &paths,
            flags: &flags,
            ids: &ids,
        };
        assert_eq!(batch.last_id(), Some(42));
        assert!(!batch.ids_wrapped());
        assert_eq!(batch.iter().len(), ids.len());
        let wrapped = EventBatch {
            paths: &paths,
            flags: &[0, 8],
            ids: &[u64::MAX, 1],
        };
        assert_eq!(wrapped.last_id(), Some(u64::MAX));
        assert!(wrapped.ids_wrapped());
        // A numeric maximum alone would acknowledge a post-wrap barrier
        // against unrelated pre-wrap delivery.
        assert!(wrapped.last_id().unwrap() > 1);
    }
}
