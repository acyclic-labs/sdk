//! macOS `FSEvents`, loaded on first use.
//!
//! Linking `CoreServices` loads it and `CoreFoundation` into every process at
//! start, before `main`: about 1.1 ms of each start of an executable that most
//! of the time watches nothing, such as a hook. [`EventStream`] resolves the
//! few functions that it needs when the first one starts. It is the one
//! stream every watcher in the crate runs on: [`FsEventsWatcher`] translates
//! its events exactly as the `notify` `FSEvents` backend does, so it stands in
//! for that backend behind the same [`Watcher`] interface, and a native
//! source's watch reads them itself.

#![allow(
    unsafe_code,
    reason = "FSEvents is a C API resolved at run time; each call documents its contract"
)]

use notify::event::{
    CreateKind, DataChange, Flag, MetadataKind, ModifyKind, RemoveKind, RenameMode,
};
use notify::{Config, Error, Event, EventHandler, EventKind, RecursiveMode, Watcher, WatcherKind};
use std::collections::HashMap;
use std::ffi::{CStr, OsStr, c_char, c_void};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use crate::demand::{DemandWork, SourceDemandReadWork};
use crate::{DemandError, WorkBudget, WorkCounters};

type CfTypeRef = *const c_void;
type FsEventStreamRef = *mut c_void;
type DispatchQueue = *mut c_void;

type FsEventStreamCallback = extern "C" fn(
    stream: FsEventStreamRef,
    info: *mut c_void,
    event_count: usize,
    event_paths: *mut c_void,
    event_flags: *const u32,
    event_ids: *const u64,
);

#[repr(C)]
struct FsEventStreamContext {
    version: isize,
    info: *mut c_void,
    retain: Option<extern "C" fn(*const c_void) -> *const c_void>,
    release: Option<extern "C" fn(*const c_void)>,
    copy_description: Option<extern "C" fn(*const c_void) -> CfTypeRef>,
}

unsafe extern "C" {
    // libdispatch is part of libSystem, which every process links.
    fn dispatch_queue_create(label: *const c_char, attributes: *const c_void) -> DispatchQueue;
    fn dispatch_async_f(
        queue: DispatchQueue,
        context: *mut c_void,
        work: extern "C" fn(*mut c_void),
    );
    fn dispatch_sync_f(
        queue: DispatchQueue,
        context: *mut c_void,
        work: extern "C" fn(*mut c_void),
    );
    fn dispatch_release(object: DispatchQueue);
}

const CF_STRING_ENCODING_UTF8: u32 = 0x0800_0100;
const EVENT_ID_SINCE_NOW: u64 = u64::MAX;
const CREATE_NO_DEFER: u32 = 0x02;
const CREATE_FILE_EVENTS: u32 = 0x10;

pub(crate) const MUST_SCAN_SUBDIRS: u32 = 0x0000_0001;
pub(crate) const USER_DROPPED: u32 = 0x0000_0002;
pub(crate) const KERNEL_DROPPED: u32 = 0x0000_0004;
const HISTORY_DONE: u32 = 0x0000_0010;
const ROOT_CHANGED: u32 = 0x0000_0020;
pub(crate) const MOUNT: u32 = 0x0000_0040;
pub(crate) const UNMOUNT: u32 = 0x0000_0080;
pub(crate) const ITEM_CREATED: u32 = 0x0000_0100;
pub(crate) const ITEM_REMOVED: u32 = 0x0000_0200;
const INODE_META_MOD: u32 = 0x0000_0400;
pub(crate) const ITEM_RENAMED: u32 = 0x0000_0800;
const ITEM_MODIFIED: u32 = 0x0000_1000;
const FINDER_INFO_MOD: u32 = 0x0000_2000;
const ITEM_CHANGE_OWNER: u32 = 0x0000_4000;
const ITEM_XATTR_MOD: u32 = 0x0000_8000;
const IS_FILE: u32 = 0x0001_0000;
const IS_DIR: u32 = 0x0002_0000;
const IS_SYMLINK: u32 = 0x0004_0000;
const OWN_EVENT: u32 = 0x0008_0000;
const IS_HARDLINK: u32 = 0x0010_0000;
const ITEM_CLONED: u32 = 0x0040_0000;

type StringCreateWithBytes =
    unsafe extern "C" fn(CfTypeRef, *const u8, isize, u32, u8) -> CfTypeRef;
type ArrayCreate =
    unsafe extern "C" fn(CfTypeRef, *const CfTypeRef, isize, *const c_void) -> CfTypeRef;
type Release = unsafe extern "C" fn(CfTypeRef);
type StreamCreate = unsafe extern "C" fn(
    CfTypeRef,
    FsEventStreamCallback,
    *const FsEventStreamContext,
    CfTypeRef,
    u64,
    f64,
    u32,
) -> FsEventStreamRef;
type StreamSetDispatchQueue = unsafe extern "C" fn(FsEventStreamRef, DispatchQueue);
type StreamStart = unsafe extern "C" fn(FsEventStreamRef) -> u8;
type StreamAction = unsafe extern "C" fn(FsEventStreamRef);

/// The `CoreFoundation` and `CoreServices` functions that a watcher calls.
struct Api {
    string_create_with_bytes: StringCreateWithBytes,
    array_create: ArrayCreate,
    release: Release,
    type_array_callbacks: *const c_void,
    stream_create: StreamCreate,
    stream_set_dispatch_queue: StreamSetDispatchQueue,
    stream_start: StreamStart,
    stream_stop: StreamAction,
    stream_invalidate: StreamAction,
    stream_release: StreamAction,
}

// SAFETY: the table holds function addresses and the address of an immutable
// `CoreFoundation` constant, all valid for the life of the process.
unsafe impl Send for Api {}
// SAFETY: as for `Send`; nothing in the table is ever written after loading.
unsafe impl Sync for Api {}

impl Api {
    fn get() -> notify::Result<&'static Self> {
        let cancellation = crate::cancellation::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        Self::get_measured(&mut work).map_err(notify_error)
    }

    fn get_measured<W: SourceDemandReadWork>(work: &mut W) -> Result<&'static Self, DemandError> {
        static API: OnceLock<Api> = OnceLock::new();
        static LOADING: Mutex<()> = Mutex::new(());
        work.check()?;
        if let Some(api) = API.get() {
            return Ok(api);
        }
        // Process-global loader custody must be initialized outside a finite
        // original invocation; loading CoreServices has no bounded payload.
        if work.is_native_original() { return Err(DemandError::NativeWorkUnavailable); }
        let _loading = LOADING.lock().map_err(|_| DemandError::WorkerUnavailable)?;
        work.check()?;
        if let Some(api) = API.get() {
            return Ok(api);
        }
        let api = work.with_original_leaf(Self::load_measured)?;
        let _ = API.set(api);
        API.get().ok_or(DemandError::WorkerUnavailable)
    }

    fn load_measured(work: &mut DemandWork) -> Result<Self, DemandError> {
        const CORE_SERVICES: &CStr =
            c"/System/Library/Frameworks/CoreServices.framework/CoreServices";
        // Framework-private loader state has no canonical payload, but the
        // loader does allocate it. The successfully loaded handle lives for
        // the process, just as the function table does.
        work.check()?;
        work.work.admit(&WorkCounters {
            backend_read_operations: 1, backend_write_operations: 1,
            allocation_operations: 1, ..WorkCounters::default()
        }, &work.budget)?;
        work.charge(WorkCounters {
            backend_read_operations: 1, allocation_operations: 1,
            ..WorkCounters::default()
        })?;
        // SAFETY: the path is a valid C string. A successful bootstrap keeps
        // its loader reference for the process; failed bootstrap closes it.
        let framework = unsafe { libc::dlopen(CORE_SERVICES.as_ptr(), libc::RTLD_LAZY) };
        if framework.is_null() {
            return Err(native_error("cannot load CoreServices for FSEvents"));
        }
        let result = (|| {
            let mut symbol = |name: &CStr| {
                work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
                // SAFETY: the handle is open and the name is a valid C string.
                // Lookups include the frameworks linked by CoreServices.
                let address = unsafe { libc::dlsym(framework, name.as_ptr()) };
                if address.is_null() {
                    Err(native_error("FSEvents lacks a required framework symbol"))
                } else {
                    Ok(address)
                }
            };
            macro_rules! function {
                ($name:literal as $signature:ty) => {
                    // SAFETY: the named function has the declared C signature.
                    unsafe { std::mem::transmute::<*mut c_void, $signature>(symbol($name)?) }
                };
            }
            Ok(Self {
                string_create_with_bytes: function!(c"CFStringCreateWithBytes" as StringCreateWithBytes),
                array_create: function!(c"CFArrayCreate" as ArrayCreate),
                release: function!(c"CFRelease" as Release),
                type_array_callbacks: symbol(c"kCFTypeArrayCallBacks")?.cast_const(),
                stream_create: function!(c"FSEventStreamCreate" as StreamCreate),
                stream_set_dispatch_queue: function!(c"FSEventStreamSetDispatchQueue" as StreamSetDispatchQueue),
                stream_start: function!(c"FSEventStreamStart" as StreamStart),
                stream_stop: function!(c"FSEventStreamStop" as StreamAction),
                stream_invalidate: function!(c"FSEventStreamInvalidate" as StreamAction),
                stream_release: function!(c"FSEventStreamRelease" as StreamAction),
            })
        })();
        if result.is_err() {
            work.record(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })
                .expect("failed framework bootstrap cleanup was admitted");
            // SAFETY: this attempt holds one loader reference and has exposed
            // no function table or stream. Successful bootstrap never closes it.
            unsafe { libc::dlclose(framework) };
        }
        result
    }

    /// The array owns the canonical UTF-8 path payload and its pointer slots.
    /// Opaque CF headers add allocation operations, not guessed payload sizes.
    fn path_array_measured<W: SourceDemandReadWork>(
        &self, paths: &[PathBuf], work: &mut W,
    ) -> Result<(CfTypeRef, u64), DemandError> {
        let slots = u64::try_from(paths.len())
            .ok().and_then(|count| count.checked_mul(size_of::<CfTypeRef>() as u64))
            .ok_or(crate::WorkError::Overflow)?;
        let cleanup_records = u64::try_from(paths.len()).map_err(|_| crate::WorkError::Overflow)?;
        work.check()?;
        work.preflight(WorkCounters {
            items_examined: cleanup_records, ..WorkCounters::default()
        })?;
        work.with_original_leaf(|work| work.reserve(crate::materializer::NativeResourceCredit::Items, cleanup_records))?;
        let mut slot_allocation = match work.claim(slots) {
            Ok(allocation) => allocation,
            Err(error) => {
                work.with_original_leaf(|work| work.refund(crate::materializer::NativeResourceCredit::Items, cleanup_records).expect("original reservation is balanced"));
                return Err(error);
            }
        };
        let mut strings = Vec::new();
        let reserved = strings.try_reserve_exact(paths.len());
        let actual_slots = (strings.capacity() as u64).checked_mul(size_of::<CfTypeRef>() as u64)
            .ok_or(crate::WorkError::Overflow)?;
        let reconciled = work.reconcile_allocation(&mut slot_allocation, actual_slots);
        if let Err(error) = reserved.map_err(DemandError::Allocation).and(reconciled) {
            drop(strings);
            work.release(slot_allocation)?;
            work.with_original_leaf(|work| work.refund(crate::materializer::NativeResourceCredit::Items, cleanup_records).expect("original reservation is balanced"));
            return Err(error);
        }
        let mut cleanup_remaining = cleanup_records;
        let mut payload = 0_u64;
        let mut array_bytes = 0_u64;
        let mut created_array = std::ptr::null();
        let mut result = (|| {
            for path in paths {
                let bytes = path.as_os_str().as_bytes();
                let length = isize::try_from(bytes.len()).map_err(|_| DemandError::InvalidRequest)?;
                let owned = u64::try_from(bytes.len()).map_err(|_| crate::WorkError::Overflow)?;
                let copy = work.reserve_source_copy(owned)?;
                let cleanup = strings.len() as u64 + 1;
                work.preflight(WorkCounters {
                    backend_write_operations: cleanup + 1,
                    ..WorkCounters::default()
                })?;
                if owned == 0 {
                    work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })?;
                } else {
                    work.allocate(owned)?;
                }
                if let Err(error) = native_write(work, cleanup) {
                    work.release(owned)?;
                    return Err(error);
                }
                // SAFETY: the string copies the bytes, valid for their length.
                let string = unsafe {
                    (self.string_create_with_bytes)(
                        std::ptr::null(), bytes.as_ptr(), length, CF_STRING_ENCODING_UTF8, 0,
                    )
                };
                if string.is_null() {
                    work.release(owned)?;
                    return Err(DemandError::Io(std::io::Error::from(std::io::ErrorKind::NotFound)));
                }
                strings.push(string);
                payload += owned;
                if let Some(copy) = copy {
                    { let mut copy = copy; copy.record_completed(crate::WorkCounters { bytes_copied: owned, ..crate::WorkCounters::UNCHARGED }) }.map_err(crate::native_name::original_name_error)?;
                } else {
                    work.record(WorkCounters { bytes_copied: owned, ..WorkCounters::UNCHARGED })?;
                }
            }
            let count = isize::try_from(strings.len()).map_err(|_| DemandError::InvalidRequest)?;
            work.preflight(WorkCounters {
                backend_write_operations: strings.len() as u64 + 2,
                bytes_copied: slots, ..WorkCounters::default()
            })?;
            if slots == 0 {
                work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::default() })?;
            } else {
                work.allocate(slots)?;
            }
            array_bytes = slots;
            let copy = work.reserve_source_copy(slots)?;
            native_write(work, strings.len() as u64 + 1)?;
            // SAFETY: the array retains the valid strings with standard callbacks.
            let array = unsafe {
                (self.array_create)(
                    std::ptr::null(), strings.as_ptr(), count, self.type_array_callbacks,
                )
            };
            created_array = array;
            if array.is_null() {
                return Err(native_error("cannot create the FSEvents path array"));
            }
            if let Some(copy) = copy {
                { let mut copy = copy; copy.record_completed(crate::WorkCounters { bytes_copied: slots, ..crate::WorkCounters::UNCHARGED }) }.map_err(crate::native_name::original_name_error)?;
            } else {
                work.record(WorkCounters { bytes_copied: slots, ..WorkCounters::UNCHARGED })?;
            }
            Ok(array)
        })();
        while let Some(string) = strings.pop() {
            cleanup_remaining -= 1;
            // SAFETY: each original string reference is released exactly once.
            unsafe { (self.release)(string) };
            let retired = work.record(WorkCounters {
                backend_write_operations: 1, ..WorkCounters::default()
            });
            let visited = work.with_original_leaf(|work| work.complete(crate::materializer::NativeResourceCredit::Items, 1, 1));
            if let Err(error) = retired.and(visited) { result = Err(error); }
        }
        work.with_original_leaf(|work| work.refund(crate::materializer::NativeResourceCredit::Items, cleanup_remaining).expect("original reservation is balanced"));
        drop(strings);
        work.release(slot_allocation)?;
        match result {
            Ok(array) => Ok((array, payload + array_bytes)),
            Err(error) => {
                if !created_array.is_null() {
                    // SAFETY: the failed attempt still owns the array reference.
                    unsafe { (self.release)(created_array) };
                    let recorded = work.record(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() });
                    work.release(payload + array_bytes)?;
                    return recorded.and(Err(error));
                }
                work.release(payload + array_bytes)?;
                Err(error)
            }
        }
    }
}

fn native_error(message: &'static str) -> DemandError {
    DemandError::Io(std::io::Error::other(message))
}

fn notify_error(error: DemandError) -> Error {
    Error::from(crate::demand::native_demand_read_error(error))
}

/// Keep mandatory failure cleanup admissible without charging unused calls.
fn native_write<W: SourceDemandReadWork>(work: &mut W, cleanup: u64) -> Result<(), DemandError> {
    work.check()?;
    work.preflight(WorkCounters {
        backend_write_operations: cleanup + 1, ..WorkCounters::default()
    })?;
    work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::default() })
}

fn native_allocate<W: SourceDemandReadWork>(work: &mut W, cleanup: u64) -> Result<(), DemandError> {
    work.check()?;
    work.preflight(WorkCounters {
        backend_write_operations: cleanup + 1, allocation_operations: 1,
        ..WorkCounters::default()
    })?;
    work.charge(WorkCounters {
        backend_write_operations: 1, allocation_operations: 1,
        ..WorkCounters::default()
    })
}

/// Receives each batch of events a stream delivers: each path with its
/// flags, in the order the stream reported them.
pub(crate) type EventHandlerFn = dyn Fn(&[(PathBuf, u32)]) + Send + Sync;

/// Borrowed framework-owned paths, valid only during their native callback.
pub(crate) struct EventBatch<'a> {
    paths: &'a [*const c_char],
    flags: &'a [u32],
}

impl<'a> EventBatch<'a> {
    pub(crate) fn len(&self) -> usize { self.paths.len() }

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

enum StreamHandler {
    Owned(Arc<EventHandlerFn>),
    Borrowed(Arc<BorrowedEventHandlerFn>),
}

pub(crate) trait StreamTerminal: Send + Sync {
    fn stream_stopped(&self);
}

thread_local! {
    static ACTIVE_STREAM: std::cell::Cell<FsEventStreamRef> = const { std::cell::Cell::new(std::ptr::null_mut()) };
}

struct ActiveStream(FsEventStreamRef);

impl Drop for ActiveStream {
    fn drop(&mut self) { ACTIVE_STREAM.with(|active| active.set(self.0)); }
}

/// What a stream's callback reads; the stream owns it and frees it on release.
struct StreamContext {
    handler: StreamHandler,
    // Only the original serial callback queue accesses this preallocated slot.
    terminal: std::cell::UnsafeCell<Option<EventStream>>,
}

// SAFETY: handlers are Send+Sync; the terminal slot has a single queue-local
// writer/consumer and every raw reference has a real Arc strong owner.
unsafe impl Send for StreamContext {}
unsafe impl Sync for StreamContext {}

extern "C" fn release_context(info: *const c_void) {
    // SAFETY: stream creation transferred exactly one Arc strong reference;
    // the native release callback consumes that reference exactly once.
    drop(unsafe { Arc::from_raw(info.cast::<StreamContext>()) });
}

extern "C" fn drain(_: *mut c_void) {}

extern "C" fn terminal_suffix(info: *mut c_void) {
    // SAFETY: defer_terminal gave this original serial queue exactly one raw
    // Arc reference. No callback can still be accessing its queue-local slot.
    let context = unsafe { Arc::from_raw(info.cast::<StreamContext>()) };
    let owner = unsafe { &mut *context.terminal.get() }.take()
        .expect("the original stream's sole terminal owner is present");
    if let Some(birth) = owner.birth.clone() {
        let mut work = crate::demand::NativeSourceWork::from_original_work(&birth);
        owner.finish_deferred(context, &mut work);
    } else {
        let cancellation = crate::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        owner.finish_deferred(context, &mut work);
    }
}

extern "C" fn stream_callback(
    stream: FsEventStreamRef,
    info: *mut c_void,
    event_count: usize,
    event_paths: *mut c_void,
    event_flags: *const u32,
    _event_ids: *const u64,
) {
    // SAFETY: `info` is the stream's live `StreamContext`, and without
    // `kFSEventStreamCreateFlagUseCFTypes` the paths are an array of
    // `event_count` C strings, with as many flags.
    let (context, paths, flags) = unsafe {
        (
            &*info.cast::<StreamContext>(),
            std::slice::from_raw_parts(event_paths.cast::<*const c_char>(), event_count),
            std::slice::from_raw_parts(event_flags, event_count),
        )
    };
    let _active = ActiveStream(ACTIVE_STREAM.with(|active| active.replace(stream)));
    match &context.handler {
        StreamHandler::Borrowed(handler) => handler(EventBatch { paths, flags }),
        StreamHandler::Owned(handler) => {
            // The ordinary notify interface still receives its original owned
            // batch. Native source delivery instead admits each borrowed path
            // before materializing any SDK-owned change.
            let events = EventBatch { paths, flags }.iter()
                .map(|(path, flags)| (path.to_path_buf(), flags))
                .collect::<Vec<_>>();
            handler(&events);
        }
    }
}

/// One running `FSEvents` stream over some paths, delivering file events as
/// they happen on its own serial queue. Dropping it stops it, after any
/// batch in delivery.
pub(crate) struct EventStream {
    api: &'static Api,
    stream: FsEventStreamRef,
    queue: DispatchQueue,
    context: *const StreamContext,
    terminal: Option<Arc<dyn StreamTerminal>>,
    started: bool,
    scheduled: bool,
    owned_bytes: u64,
    ownership: crate::demand::SourcePayloadOwnership,
    cleanup: (u64, u64),
    original_cleanup: bool,
    birth: Option<crate::demand::OriginalDemandWork>,
}

// SAFETY: FSEvents streams and dispatch queues may be stopped and released
// from another thread; exclusive custody serializes every teardown.
unsafe impl Send for EventStream {}
// SAFETY: shared references do not mutate the native resource custody.
unsafe impl Sync for EventStream {}

impl EventStream {
    /// Initializes process-global framework services outside finite source work.
    pub(crate) fn initialize_services() -> notify::Result<()> {
        Api::get().map(|_| ())
    }
    /// Starts one stream over `paths`, with `handler` receiving its events.
    pub(crate) fn start(paths: &[PathBuf], handler: Arc<EventHandlerFn>) -> notify::Result<Self> {
        let cancellation = crate::cancellation::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        Self::start_measured(paths, handler, &mut work).map_err(notify_error)
    }

    pub(crate) fn start_measured(
        paths: &[PathBuf], handler: Arc<EventHandlerFn>, work: &mut DemandWork,
    ) -> Result<Self, DemandError> {
        Self::start_handler_measured(paths, StreamHandler::Owned(handler), None, work)
    }

    /// Publishes source custody's retained payload before callback exposure.
    /// The source owner retires that scalar only after native teardown returns.
    pub(crate) fn start_borrowed_measured(
        paths: &[PathBuf], handler: Arc<BorrowedEventHandlerFn>,
        published_bytes: Option<&AtomicU64>, work: &mut DemandWork,
    ) -> Result<Self, DemandError> {
        Self::start_handler_measured(paths, StreamHandler::Borrowed(handler), published_bytes, work)
    }

    pub(crate) fn start_borrowed_with_work<W: SourceDemandReadWork>(
        paths: &[PathBuf], handler: Arc<BorrowedEventHandlerFn>,
        published_bytes: Option<&AtomicU64>, work: &mut W,
    ) -> Result<Self, DemandError> {
        Self::start_handler_measured(paths, StreamHandler::Borrowed(handler), published_bytes, work)
    }

    /// A stopped stream has no remaining native work for its later Drop.
    pub(crate) fn stop_measured(&mut self, work: &mut DemandWork) -> Result<(), DemandError> {
        self.stop_with_work(work)
    }

    pub(crate) fn stop_with_work<W: SourceDemandReadWork>(
        &mut self, work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        if !self.birth_work(work) { work.preflight(self.cleanup_work())?; }
        self.cleanup_admitted(work)
    }

    /// Audits the original withheld physical suffix without admitting any new
    /// work or consulting cancellation of the root that funded native birth.
    pub(crate) fn ensure_cleanup_wait_available<W: SourceDemandReadWork>(
        &self, work: &W,
    ) -> Result<(), DemandError> {
        let required = self.cleanup_work();
        if !self.birth_work(work)
            || self.cleanup.0 < required.backend_read_operations
            || self.cleanup.1 < required.backend_write_operations
        {
            return Err(DemandError::NativeWorkUnavailable);
        }
        Ok(())
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.stream.is_null() && self.queue.is_null()
    }

    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        self.owned_bytes
    }

    pub(crate) fn set_terminal(&mut self, terminal: Arc<dyn StreamTerminal>) {
        self.terminal = Some(terminal);
    }

    fn cleanup_work(&self) -> WorkCounters {
        WorkCounters {
            backend_write_operations: u64::from(!self.stream.is_null())
                + u64::from(self.started) + u64::from(self.scheduled)
                + u64::from(!self.queue.is_null()),
            backend_read_operations: u64::from(!self.queue.is_null()),
            ..WorkCounters::default()
        }
    }

    fn birth_work<W: SourceDemandReadWork>(&self, work: &W) -> bool {
        self.original_cleanup && work.native_source_work().is_some_and(|work| {
            let current = work.original_demand_work();
            self.birth.as_ref().is_some_and(|birth| {
                Arc::ptr_eq(birth.meter(), current.meter())
                    && match (birth.resource_allowance(), current.resource_allowance()) {
                        (None, None) => true,
                        (Some(birth), Some(current)) => Arc::ptr_eq(birth, current),
                        _ => false,
                    }
            })
        })
    }

    fn record_cleanup<W: SourceDemandReadWork>(&mut self, work: &mut W, reads: u64, writes: u64) -> Result<(), DemandError> {
        let delta = WorkCounters { backend_read_operations: reads, backend_write_operations: writes, ..WorkCounters::default() };
        if !self.birth_work(work) { return work.record(delta); }
        if self.cleanup.0 < reads || self.cleanup.1 < writes { return Err(DemandError::InvalidRequest); }
        self.cleanup.0 -= reads;
        self.cleanup.1 -= writes;
        work.with_original_leaf(|work| crate::source_watch::record_cleanup_credits(work, delta))
    }

    /// Finishes every admitted native operation, even after cancellation or
    /// a recording error. Invalidation removes the producer before the serial
    /// queue barrier establishes that its last callback has returned.
    pub(crate) fn cleanup_admitted<W: SourceDemandReadWork>(&mut self, work: &mut W) -> Result<(), DemandError> {
        let mut result = Ok(());
        if self.started {
            // SAFETY: stop is valid only after the actual successful start.
            unsafe { (self.api.stream_stop)(self.stream) };
            self.started = false;
            result = result.and(self.record_cleanup(work, 0, 1));
        }
        if self.scheduled {
            // SAFETY: this actual stream was scheduled on its live queue.
            unsafe { (self.api.stream_invalidate)(self.stream) };
            self.scheduled = false;
            result = result.and(self.record_cleanup(work, 0, 1));
        }
        if !self.stream.is_null() && ACTIVE_STREAM.with(|active| active.get() == self.stream) {
            return result.and(self.defer_terminal(work));
        }
        if !self.queue.is_null() {
            // SAFETY: this original serial queue is live. No callback-owned
            // state mutex is held; the barrier waits for its pending callbacks.
            unsafe { dispatch_sync_f(self.queue, std::ptr::null_mut(), drain) };
            result = result.and(self.record_cleanup(work, 1, 0));
        }
        result = result.and(self.release_drained_handles(work));
        result = result.and(self.retire_drained_custody(work));
        if let Some(terminal) = self.terminal.take() { terminal.stream_stopped(); }
        result
    }

    fn release_drained_handles<W: SourceDemandReadWork>(&mut self, work: &mut W) -> Result<(), DemandError> {
        let mut result = Ok(());
        if !self.stream.is_null() {
            // SAFETY: invalidated and drained; release also frees the context.
            unsafe { (self.api.stream_release)(self.stream) };
            self.stream = std::ptr::null_mut();
            result = result.and(self.record_cleanup(work, 0, 1));
        }
        if !self.queue.is_null() {
            // SAFETY: no live stream or callback needs this queue reference.
            unsafe { dispatch_release(self.queue) };
            self.queue = std::ptr::null_mut();
            result = result.and(self.record_cleanup(work, 0, 1));
        }
        result
    }

    fn retire_drained_custody<W: SourceDemandReadWork>(&mut self, work: &mut W) -> Result<(), DemandError> {
        let mut result = Ok(());
        result = result.and(self.ownership.retire(self.owned_bytes));
        self.owned_bytes = 0;
        if let Some(birth) = &self.birth {
            let (reads, writes) = std::mem::take(&mut self.cleanup);
            crate::demand::NativeSourceWork::from_original_work(birth).with_original_leaf(|work| {
                crate::source_watch::cancel_cleanup_credits(work, WorkCounters {
                    backend_read_operations: reads, backend_write_operations: writes,
                    ..WorkCounters::UNCHARGED
                });
            });
        }
        result
    }

    fn defer_terminal<W: SourceDemandReadWork>(&mut self, work: &mut W) -> Result<(), DemandError> {
        let context = self.context;
        let queue = self.queue;
        let api = self.api;
        // SAFETY: this is the currently executing callback's live original
        // Arc context. Its serial queue is the slot's sole writer/consumer.
        let slot = unsafe { &mut *(*context).terminal.get() };
        assert!(slot.is_none(), "one original stream schedules one terminal suffix");
        let recorded = self.record_cleanup(work, 0, 1);
        // SAFETY: the stream still owns its raw Arc reference. This allocation-
        // free clone keeps the context alive through native release in the thunk.
        unsafe { Arc::increment_strong_count(context) };
        *slot = Some(std::mem::replace(self, Self {
            api, stream: std::ptr::null_mut(), queue: std::ptr::null_mut(),
            context: std::ptr::null(), terminal: None, started: false, scheduled: false,
            owned_bytes: 0, ownership: Default::default(), cleanup: (0, 0),
            original_cleanup: false, birth: None,
        }));
        // SAFETY: original serial queue + admitted scheduling stage. The same
        // queue consumes this sole raw Arc reference after prior callbacks drain.
        unsafe { dispatch_async_f(queue, context.cast_mut().cast(), terminal_suffix) };
        recorded
    }

    fn finish_deferred<W: SourceDemandReadWork>(
        mut self, context: Arc<StreamContext>, work: &mut W,
    ) {
        let terminal = self.terminal.take();
        let result = self.release_drained_handles(work);
        // The stream's native release consumed its original context reference;
        // our queued reference is last, so handler payloads physically drop now.
        drop(context);
        let result = result.and(self.retire_drained_custody(work));
        if let Err(error) = result {
            work.with_original_leaf(|work| work.record_native_io_failure(error));
        }
        if let Some(terminal) = terminal { terminal.stream_stopped(); }
        // The original custody and birth keeper outlive every physical suffix.
    }

    fn start_handler_measured<W: SourceDemandReadWork>(
        paths: &[PathBuf], handler: StreamHandler,
        published_bytes: Option<&AtomicU64>, work: &mut W,
    ) -> Result<Self, DemandError> {
        let api = Api::get_measured(work)?;
        let mut ownership = Some(crate::demand::SourcePayloadOwnership::new(work)?);
        let (paths, path_bytes) = api.path_array_measured(paths, work)?;
        let mut paths_live = true;
        let mut context = std::ptr::null::<StreamContext>();
        let mut context_bytes = 0;
        let mut stream = std::ptr::null_mut();
        let mut queue = std::ptr::null_mut();
        let mut started = false;
        let mut scheduled = false;
        let mut cleanup_reserved = false;
        let mut retained = false;
        let mut result = (|| {
            // Birth funds both ordinary drain and the same-queue self-drop
            // suffix. Whichever path does not run refunds its exact credit.
            work.check()?;
            work.with_original_leaf(|work| crate::source_watch::reserve_cleanup_credits(work, WorkCounters {
                backend_write_operations: 5, backend_read_operations: 1, ..WorkCounters::UNCHARGED
            }))?;
            cleanup_reserved = true;
            let requested_context_bytes = crate::source_watch::arc_control_bytes::<StreamContext>();
            work.allocate(requested_context_bytes)?;
            context_bytes = requested_context_bytes;
            context = Arc::into_raw(Arc::new(StreamContext {
                handler, terminal: std::cell::UnsafeCell::new(None),
            }));
            let stream_context = FsEventStreamContext {
                version: 0, info: context.cast_mut().cast(), retain: None,
                release: Some(release_context), copy_description: None,
            };
            native_allocate(work, 1)?;
            // SAFETY: a successful stream takes the sole raw context Arc and
            // retains the path array; a failed create takes neither.
            stream = unsafe {
                (api.stream_create)(
                    std::ptr::null(), stream_callback, &raw const stream_context,
                    paths, EVENT_ID_SINCE_NOW, 0.0, CREATE_FILE_EVENTS | CREATE_NO_DEFER,
                )
            };
            unsafe { (api.release)(paths) };
            paths_live = false;
            work.record(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })?;
            if stream.is_null() {
                return Err(native_error("cannot create an FSEvents stream"));
            }
            native_allocate(work, 0)?;
            let custody = ownership.as_ref().expect("startup custody");
            custody.retain(path_bytes + context_bytes, work)?;
            retained = custody.has_native_custody();
            // SAFETY: null attributes select the original serial callback queue.
            queue = unsafe { dispatch_queue_create(c"acyclic.fsevents".as_ptr(), std::ptr::null()) };
            if queue.is_null() {
                return Err(native_error("cannot create the FSEvents dispatch queue"));
            }
            native_write(work, 0)?;
            unsafe { (api.stream_set_dispatch_queue)(stream, queue) };
            scheduled = true;
            native_write(work, 0)?;
            if let Some(published) = published_bytes {
                published.store(path_bytes + context_bytes, Ordering::Release);
            }
            if unsafe { (api.stream_start)(stream) } == 0 {
                return Err(native_error("cannot start an FSEvents stream"));
            }
            started = true;
            work.check()?;
            if !retained { work.release(path_bytes + context_bytes)?; }
            Ok(Self {
                api, stream, queue, context, terminal: None, started, scheduled,
                owned_bytes: path_bytes + context_bytes,
                cleanup: if work.is_native_original() { (1, 5) } else { (0, 0) },
                original_cleanup: work.is_native_original(),
                birth: work.native_source_work().map(|work| work.original_demand_work().allocation_work()),
                ownership: ownership.take().expect("startup custody"),
            })
        })();
        if cleanup_reserved && !work.is_native_original() {
            work.with_original_leaf(|work| crate::source_watch::cancel_cleanup_credits(work, WorkCounters {
                backend_write_operations: 5, backend_read_operations: 1, ..WorkCounters::UNCHARGED
            }));
        }
        if result.is_err() {
            if paths_live {
                unsafe { (api.release)(paths) };
                let _cleanup = work.record(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED });
            }
            if stream.is_null() {
                if !context.is_null() {
                    // SAFETY: failed creation left the original raw Arc ours.
                    drop(unsafe { Arc::from_raw(context) });
                }
                if cleanup_reserved && work.is_native_original() {
                    work.with_original_leaf(|work| crate::source_watch::cancel_cleanup_credits(work, WorkCounters {
                        backend_write_operations: 5, backend_read_operations: 1, ..WorkCounters::UNCHARGED
                    }));
                }
            } else {
                let mut failed = Self {
                    api, stream, queue, context, terminal: None, started, scheduled,
                    owned_bytes: path_bytes + context_bytes,
                    ownership: if retained {
                        ownership.take().expect("retained failed stream custody")
                    } else { Default::default() },
                    cleanup: if cleanup_reserved && work.is_native_original() { (1, 5) } else { (0, 0) },
                    original_cleanup: cleanup_reserved && work.is_native_original(),
                    birth: work.native_source_work().map(|work| work.original_demand_work().allocation_work()),
                };
                let _cleanup = failed.cleanup_admitted(work);
            }
            if let Some(published) = published_bytes { published.store(0, Ordering::Release); }
            if !retained {
                let _cleanup = work.release(path_bytes + context_bytes);
            }
        }
        result
    }
}

impl Drop for EventStream {
    fn drop(&mut self) {
        if self.stream.is_null() && self.queue.is_null() { return; }
        if let Some(birth) = self.birth.clone() {
            let mut work = crate::demand::NativeSourceWork::from_original_work(&birth);
            let _ = self.cleanup_admitted(&mut work);
            return;
        }
        let cancellation = crate::cancellation::CancellationToken::new();
        let mut work = DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        self.stop_measured(&mut work)
            .expect("ordinary unbounded FSEvents cleanup is admissible");
    }
}

/// Translates one event's flags as the `notify` `FSEvents` backend does.
fn translate(flags: u32) -> Vec<Event> {
    let has = |flag| flags & flag != 0;
    let mut events = Vec::new();
    if has(HISTORY_DONE) {
        return events;
    }
    if has(MUST_SCAN_SUBDIRS) {
        let event = Event::new(EventKind::Other).set_flag(Flag::Rescan);
        events.push(if has(USER_DROPPED) {
            event.set_info("rescan: user dropped")
        } else if has(KERNEL_DROPPED) {
            event.set_info("rescan: kernel dropped")
        } else {
            event
        });
    }
    if has(ROOT_CHANGED) {
        events.push(
            Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::From)))
                .set_info("root changed"),
        );
    }
    if has(MOUNT) {
        events.push(Event::new(EventKind::Create(CreateKind::Other)).set_info("mount"));
    }
    if has(UNMOUNT) {
        events.push(Event::new(EventKind::Remove(RemoveKind::Other)).set_info("mount"));
    }
    let link_info = || {
        if has(IS_SYMLINK) {
            Some("is: symlink")
        } else if has(IS_HARDLINK) {
            Some("is: hardlink")
        } else if has(ITEM_CLONED) {
            Some("is: clone")
        } else {
            None
        }
    };
    if has(ITEM_CREATED) {
        events.push(if has(IS_DIR) {
            Event::new(EventKind::Create(CreateKind::Folder))
        } else if has(IS_FILE) {
            Event::new(EventKind::Create(CreateKind::File))
        } else {
            link_info().map_or_else(
                || Event::new(EventKind::Create(CreateKind::Any)),
                |info| Event::new(EventKind::Create(CreateKind::Other)).set_info(info),
            )
        });
    }
    if has(ITEM_REMOVED) {
        events.push(if has(IS_DIR) {
            Event::new(EventKind::Remove(RemoveKind::Folder))
        } else if has(IS_FILE) {
            Event::new(EventKind::Remove(RemoveKind::File))
        } else {
            link_info().map_or_else(
                || Event::new(EventKind::Remove(RemoveKind::Any)),
                |info| Event::new(EventKind::Remove(RemoveKind::Other)).set_info(info),
            )
        });
    }
    if has(ITEM_RENAMED) {
        events.push(Event::new(EventKind::Modify(ModifyKind::Name(
            RenameMode::Any,
        ))));
    }
    if has(INODE_META_MOD) {
        events.push(Event::new(EventKind::Modify(ModifyKind::Metadata(
            MetadataKind::Any,
        ))));
    }
    if has(FINDER_INFO_MOD) {
        events.push(
            Event::new(EventKind::Modify(ModifyKind::Metadata(MetadataKind::Other)))
                .set_info("meta: finder info"),
        );
    }
    if has(ITEM_CHANGE_OWNER) {
        events.push(Event::new(EventKind::Modify(ModifyKind::Metadata(
            MetadataKind::Ownership,
        ))));
    }
    if has(ITEM_XATTR_MOD) {
        events.push(Event::new(EventKind::Modify(ModifyKind::Metadata(
            MetadataKind::Extended,
        ))));
    }
    if has(ITEM_MODIFIED) {
        events.push(Event::new(EventKind::Modify(ModifyKind::Data(
            DataChange::Content,
        ))));
    }
    if has(OWN_EVENT) {
        events = events
            .into_iter()
            .map(|event| event.set_process_id(std::process::id()))
            .collect();
    }
    events
}

/// An `FSEvents` watcher over every watched path, restarted as they change.
pub struct FsEventsWatcher {
    handler: Arc<Mutex<dyn EventHandler>>,
    paths: Vec<PathBuf>,
    recursive: HashMap<PathBuf, bool>,
    stream: Option<EventStream>,
}

impl std::fmt::Debug for FsEventsWatcher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FsEventsWatcher")
            .field("paths", &self.paths)
            .field("recursive", &self.recursive)
            .field("running", &self.stream.is_some())
            .finish_non_exhaustive()
    }
}

impl FsEventsWatcher {
    fn restart(&mut self) -> notify::Result<()> {
        self.stream = None;
        if self.paths.is_empty() {
            return Ok(());
        }
        let (handler, recursive) = (Arc::clone(&self.handler), self.recursive.clone());
        self.stream = Some(EventStream::start(
            &self.paths,
            Arc::new(move |events: &[(PathBuf, u32)]| {
                for (path, flags) in events {
                    let watched = recursive.iter().any(|(root, recursive)| {
                        path.starts_with(root)
                            && (*recursive || path == root || path.parent() == Some(root.as_path()))
                    });
                    if !watched {
                        continue;
                    }
                    for event in translate(*flags) {
                        let event = event.add_path(path.clone());
                        if let Ok(mut handler) = handler.lock() {
                            handler.handle_event(Ok(event));
                        }
                    }
                }
            }),
        )?);
        Ok(())
    }
}

impl Watcher for FsEventsWatcher {
    fn new<F: EventHandler>(event_handler: F, _config: Config) -> notify::Result<Self> {
        // Resolved now, so a watcher that cannot run fails where it is made.
        Api::get()?;
        Ok(Self {
            handler: Arc::new(Mutex::new(event_handler)),
            paths: Vec::new(),
            recursive: HashMap::new(),
            stream: None,
        })
    }

    fn watch(&mut self, path: &Path, recursive_mode: RecursiveMode) -> notify::Result<()> {
        if !path.exists() {
            return Err(Error::path_not_found().add_path(path.to_path_buf()));
        }
        let canonical = path.canonicalize()?;
        self.paths.push(path.to_path_buf());
        self.recursive
            .insert(canonical, recursive_mode == RecursiveMode::Recursive);
        self.restart()
    }

    fn unwatch(&mut self, path: &Path) -> notify::Result<()> {
        let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        let watched = self.recursive.remove(&canonical).is_some();
        self.paths.retain(|candidate| candidate != path);
        self.restart()?;
        if watched {
            Ok(())
        } else {
            Err(Error::watch_not_found())
        }
    }

    fn kind() -> WatcherKind {
        WatcherKind::Fsevent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admitted_native_cleanup_keeps_the_original_meter_after_cancellation()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let mut start = DemandWork::new(
            WorkBudget::UNBOUNDED, &crate::cancellation::CancellationToken::new(),
        );
        let mut stream = EventStream::start_borrowed_measured(
            &[root.path().to_path_buf()], Arc::new(|_| {}), None, &mut start,
        )?;
        let cancellation = crate::cancellation::CancellationToken::new();
        let budget = WorkBudget {
            backend_write_operations: 4, backend_read_operations: 1,
            ..WorkBudget::default()
        };
        let mut stop = DemandWork::new(budget, &cancellation);
        // Exercise the mandatory continuation engine with a real native stream,
        // rather than public precancellation (which must leave its owner live).
        stop.work.admit(&stream.cleanup_work(), &stop.budget)?;
        cancellation.cancel();
        stream.cleanup_admitted(&mut stop)?;
        assert!(matches!(stop.check(), Err(DemandError::Cancelled)));
        assert!(stream.is_stopped());
        assert_eq!(stream.logical_owned_bytes(), 0);
        assert_eq!(stop.work.backend_write_operations, 4);
        assert_eq!(stop.work.backend_read_operations, 1);
        assert_eq!(stop.budget, budget);
        drop(stream);
        Ok(())
    }

    #[test]
    fn measured_stop_consumes_the_actual_native_handles_before_drop()
        -> Result<(), Box<dyn std::error::Error>>
    {
        let root = tempfile::tempdir()?;
        let mut start = DemandWork::new(WorkBudget {
            backend_read_operations: 32, backend_write_operations: 32,
            items_examined: 1, bytes_copied: 32 * 1024,
            allocation_operations: 16, peak_allocation_bytes: 32 * 1024,
            ..WorkBudget::default()
        }, &crate::cancellation::CancellationToken::new());
        let mut stream = EventStream::start_borrowed_measured(
            &[root.path().to_path_buf()], Arc::new(|_| {}), None, &mut start,
        )?;
        assert!(!stream.stream.is_null());
        assert!(!stream.queue.is_null());
        assert!(stream.logical_owned_bytes() > 0);
        assert_eq!(start.live_allocation_bytes(), 0);
        let mut stop = DemandWork::new(WorkBudget {
            backend_write_operations: 4, backend_read_operations: 1,
            ..WorkBudget::default()
        }, &crate::cancellation::CancellationToken::new());
        stream.stop_measured(&mut stop)?;
        assert!(stream.stream.is_null());
        assert!(stream.queue.is_null());
        assert!(stream.is_stopped());
        assert_eq!(stream.logical_owned_bytes(), 0);
        assert_eq!(stop.work, WorkCounters {
            backend_write_operations: 4, backend_read_operations: 1,
            ..WorkCounters::default()
        });
        let before = stop.work;
        stream.stop_measured(&mut stop)?;
        assert_eq!(stop.work, before);
        drop(stream);
        Ok(())
    }

    #[test]
    fn translation_matches_the_notify_backend() {
        assert!(translate(HISTORY_DONE | ITEM_CREATED).is_empty());
        let rescan = translate(MUST_SCAN_SUBDIRS | KERNEL_DROPPED);
        assert_eq!(rescan.len(), 1);
        assert_eq!(rescan[0].kind, EventKind::Other);
        assert_eq!(rescan[0].flag(), Some(Flag::Rescan));
        assert_eq!(rescan[0].info(), Some("rescan: kernel dropped"));
        let kinds = |flags| {
            translate(flags)
                .into_iter()
                .map(|event| event.kind)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            kinds(ITEM_CREATED | ITEM_MODIFIED | IS_FILE),
            [
                EventKind::Create(CreateKind::File),
                EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            ]
        );
        assert_eq!(
            kinds(ITEM_REMOVED | ITEM_RENAMED | IS_DIR),
            [
                EventKind::Remove(RemoveKind::Folder),
                EventKind::Modify(ModifyKind::Name(RenameMode::Any)),
            ]
        );
        assert_eq!(
            translate(ITEM_CREATED | IS_SYMLINK)[0].info(),
            Some("is: symlink")
        );
        assert_eq!(kinds(ITEM_CREATED), [EventKind::Create(CreateKind::Any)]);
    }

    #[test]
    fn watcher_delivers_file_events_without_linking_core_services() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let root = directory.path().canonicalize().expect("canonical root");
        let (sender, receiver) = std::sync::mpsc::channel();
        let mut watcher = FsEventsWatcher::new(sender, Config::default()).expect("watcher");
        watcher
            .watch(&root, RecursiveMode::Recursive)
            .expect("watch");
        let file = root.join("created");
        std::fs::write(&file, b"event").expect("write");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let event = receiver
                .recv_timeout(remaining)
                .expect("an FSEvents event")
                .expect("event");
            if event.paths.contains(&file) {
                break;
            }
        }
        watcher.unwatch(&root).expect("unwatch");
        assert!(matches!(
            watcher.unwatch(&root),
            Err(error) if matches!(error.kind, notify::ErrorKind::WatchNotFound)
        ));
    }
}
