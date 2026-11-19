//! Process-local FSEvents controls for the isolated stress cases.

use super::*;
use std::time::Duration;

pub use super::super::callback::BorrowedEventHandlerFn as EventHandler;
pub use super::super::callback::EventBatch;

/// The kernel reports that a client must rescan affected paths.
pub const RESCAN: u32 = MUST_SCAN_SUBDIRS;
/// The kernel dropped events before delivery.
pub const DROPPED: u32 = USER_DROPPED | KERNEL_DROPPED;
/// The watched root moved or was replaced.
pub const ROOT_MOVED: u32 = ROOT_CHANGED;

/// The process-wide hub running the production routing and replay code.
#[derive(Clone, Copy)]
pub struct SharedHub(&'static Hub);

/// An owned registration whose drop drains in-flight delivery.
pub struct Watch(EventStream);

/// A delivery pause that resumes the native queue on drop.
pub struct PausedDelivery {
    _queue: Suspended,
}

/// An in-flight replacement of a registration's producer.
pub struct Rearm {
    hub: &'static Hub,
    registration: Arc<Registration>,
    before: u64,
}

impl SharedHub {
    /// Starts the process-wide native hub if it has not started yet.
    pub fn get() -> Result<Self, DemandError> {
        let cancellation = crate::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        Hub::get(&mut work).map(Self)
    }

    /// Registers one root and keeps ownership of its watch.
    pub fn watch(root: &Path, handler: Arc<EventHandler>) -> Result<Watch, DemandError> {
        let cancellation = crate::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        EventStream::start(&[root.to_path_buf()], handler, None, &mut work).map(Watch)
    }

    /// Returns the current number of live native streams.
    pub fn streams(self) -> usize {
        self.0.streams()
    }

    /// Maximum native streams used by the shared hub.
    pub const STREAM_CAPACITY: usize = SHARDS;

    /// Pauses the queue after all callbacks ahead of the pause have completed.
    pub fn pause_after_delivery(self) -> PausedDelivery {
        PausedDelivery {
            _queue: self.0.suspend_after_delivery(),
        }
    }

    /// Stops queued callbacks from starting, without waiting for them to be delivered.
    pub fn pause_delivery(self) -> PausedDelivery {
        PausedDelivery {
            _queue: Suspended::new(self.0.queue()),
        }
    }

    /// Records a one-shot event on the current thread's next callback drain.
    pub fn before_next_drain(hook: impl FnOnce() + 'static) {
        BEFORE_DRAIN.with(|current| *current.borrow_mut() = Some(Box::new(hook)));
    }

    /// Checks that a delivered callback can acquire both routing locks.
    pub fn locks_available(self) -> bool {
        self.0.arm.try_lock().is_ok() && self.0.state.try_lock().is_ok()
    }

    /// Requests a producer replacement and returns a handle to its completion event.
    pub fn rearm(self, watch: &Watch) -> Result<Rearm, DemandError> {
        let registration = watch
            .0
            .registration
            .as_ref()
            .ok_or_else(|| native_error("cannot re-arm an unregistered FSEvents watch"))?;
        let before = self.0.arms(registration);
        self.0.rearm(registration);
        Ok(Rearm {
            hub: self.0,
            registration: Arc::clone(registration),
            before,
        })
    }
}

impl Rearm {
    /// Waits for the replacement to arm or report its startup failure.
    pub fn wait(self, deadline: Duration) -> Result<(), DemandError> {
        self.hub
            .wait_for_arm(&self.registration, self.before, deadline)
    }
}

/// Native streams held open by the per-root capacity probe.
pub struct RawStreams {
    api: &'static Api,
    running: Vec<(FsEventStreamRef, DispatchQueue, bool)>,
    refused_at: Option<usize>,
}

impl RawStreams {
    /// Starts separate native streams until the framework refuses one or every root is watched.
    pub fn start(roots: &[PathBuf]) -> Result<Self, DemandError> {
        extern "C" fn ignore(
            _: FsEventStreamRef,
            _: *mut c_void,
            _: usize,
            _: *mut c_void,
            _: *const u32,
            _: *const u64,
        ) {
        }
        let cancellation = crate::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        let api = Api::get(&mut work)?;
        let mut streams = Self {
            api,
            running: Vec::new(),
            refused_at: None,
        };
        let context = FsEventStreamContext {
            version: 0,
            info: std::ptr::null_mut(),
            retain: None,
            release: None,
            copy_description: None,
        };
        for (index, root) in roots.iter().enumerate() {
            let (paths, bytes) = api.path_array(std::slice::from_ref(root), &mut work)?;
            // SAFETY: the path array and context are live and the callback takes no context.
            let stream = unsafe {
                (api.stream_create)(
                    std::ptr::null(),
                    ignore,
                    &raw const context,
                    paths,
                    EVENT_ID_SINCE_NOW,
                    0.0,
                    CREATE_FILE_EVENTS | CREATE_NO_DEFER,
                )
            };
            // SAFETY: the stream, if any, retained the array.
            unsafe { (api.release)(paths) };
            if let Err(error) = work.release(bytes) {
                if !stream.is_null() {
                    // SAFETY: the new stream has not been scheduled.
                    unsafe { (api.stream_release)(stream) };
                }
                return Err(error);
            }
            if stream.is_null() {
                streams.refused_at = Some(index + 1);
                break;
            }
            // SAFETY: the name is terminated and null attributes select a serial queue.
            let queue =
                unsafe { dispatch_queue_create(c"acyclic.fsevents".as_ptr(), std::ptr::null()) };
            if queue.is_null() {
                // SAFETY: the new stream has not been scheduled.
                unsafe { (api.stream_release)(stream) };
                return Err(native_error("cannot create the FSEvents dispatch queue"));
            }
            // SAFETY: both handles are live and the stream has a queue before it starts.
            let started = unsafe {
                (api.stream_set_dispatch_queue)(stream, queue);
                (api.stream_start)(stream) != 0
            };
            streams.running.push((stream, queue, started));
            if !started {
                streams.refused_at = Some(index + 1);
                break;
            }
        }
        Ok(streams)
    }

    /// Returns the one-based index of the first native stream the framework refused.
    pub fn refused_at(&self) -> Option<usize> {
        self.refused_at
    }
}

impl Drop for RawStreams {
    fn drop(&mut self) {
        for &(stream, queue, started) in &self.running {
            // SAFETY: each pair is owned here and the queue barrier drains its callbacks.
            unsafe {
                if started {
                    (self.api.stream_stop)(stream);
                }
                (self.api.stream_invalidate)(stream);
                dispatch_sync_f(queue, std::ptr::null_mut(), drain);
                (self.api.stream_release)(stream);
                dispatch_release(queue);
            }
        }
    }
}

/// Reports the process thread count observed by the kernel.
pub fn thread_count() -> i32 {
    // SAFETY: plain old data, filled by the kernel below.
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = i32::try_from(size_of::<libc::proc_taskinfo>()).unwrap_or(i32::MAX);
    // SAFETY: the buffer is a live `proc_taskinfo` of the given size.
    let read = unsafe {
        libc::proc_pidinfo(
            std::process::id().cast_signed(),
            libc::PROC_PIDTASKINFO,
            0,
            (&raw mut info).cast(),
            size,
        )
    };
    if read == size { info.pti_threadnum } else { -1 }
}
