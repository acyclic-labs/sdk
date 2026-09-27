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
use std::sync::{Arc, Mutex, OnceLock};

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
        static API: OnceLock<Result<Api, String>> = OnceLock::new();
        API.get_or_init(Self::load)
            .as_ref()
            .map_err(|error| Error::generic(error))
    }

    fn load() -> Result<Self, String> {
        const CORE_SERVICES: &CStr =
            c"/System/Library/Frameworks/CoreServices.framework/CoreServices";
        // SAFETY: the path is a valid C string; loading a system framework runs
        // only its own initializers, and the handle is never closed.
        let framework = unsafe { libc::dlopen(CORE_SERVICES.as_ptr(), libc::RTLD_LAZY) };
        if framework.is_null() {
            return Err("cannot load CoreServices for FSEvents".to_owned());
        }
        let symbol = |name: &CStr| {
            // SAFETY: the handle is open and the name is a valid C string. A
            // framework's lookup includes the frameworks that it links, so
            // `CoreFoundation` symbols resolve through `CoreServices`.
            let address = unsafe { libc::dlsym(framework, name.as_ptr()) };
            if address.is_null() {
                Err(format!("FSEvents lacks {}", name.to_string_lossy()))
            } else {
                Ok(address)
            }
        };
        macro_rules! function {
            ($name:literal as $signature:ty) => {
                // SAFETY: the address is the named function, whose C signature
                // the type states as the framework headers declare it.
                unsafe { std::mem::transmute::<*mut c_void, $signature>(symbol($name)?) }
            };
        }
        Ok(Self {
            string_create_with_bytes: function!(
                c"CFStringCreateWithBytes" as StringCreateWithBytes
            ),
            array_create: function!(c"CFArrayCreate" as ArrayCreate),
            release: function!(c"CFRelease" as Release),
            type_array_callbacks: symbol(c"kCFTypeArrayCallBacks")?.cast_const(),
            stream_create: function!(c"FSEventStreamCreate" as StreamCreate),
            stream_set_dispatch_queue: function!(
                c"FSEventStreamSetDispatchQueue" as StreamSetDispatchQueue
            ),
            stream_start: function!(c"FSEventStreamStart" as StreamStart),
            stream_stop: function!(c"FSEventStreamStop" as StreamAction),
            stream_invalidate: function!(c"FSEventStreamInvalidate" as StreamAction),
            stream_release: function!(c"FSEventStreamRelease" as StreamAction),
        })
    }

    /// An owned `CFArray` of the paths as `CFString`s.
    fn path_array(&self, paths: &[PathBuf]) -> notify::Result<CfTypeRef> {
        let mut strings = Vec::with_capacity(paths.len());
        let result = (|| {
            for path in paths {
                let bytes = path.as_os_str().as_bytes();
                let length = isize::try_from(bytes.len())
                    .map_err(|_| Error::generic("watched path is too long"))?;
                // SAFETY: the bytes are valid for their length, and the string
                // copies them.
                let string = unsafe {
                    (self.string_create_with_bytes)(
                        std::ptr::null(),
                        bytes.as_ptr(),
                        length,
                        CF_STRING_ENCODING_UTF8,
                        0,
                    )
                };
                if string.is_null() {
                    return Err(Error::path_not_found().add_path(path.clone()));
                }
                strings.push(string);
            }
            let count =
                isize::try_from(strings.len()).map_err(|_| Error::generic("too many paths"))?;
            // SAFETY: the values are valid `CFString`s, which the array
            // retains through the standard type callbacks.
            let array = unsafe {
                (self.array_create)(
                    std::ptr::null(),
                    strings.as_ptr(),
                    count,
                    self.type_array_callbacks,
                )
            };
            if array.is_null() {
                Err(Error::generic("cannot create the FSEvents path array"))
            } else {
                Ok(array)
            }
        })();
        for string in strings {
            // SAFETY: each string was created above and is released once.
            unsafe { (self.release)(string) };
        }
        result
    }
}

/// Receives each batch of events a stream delivers: each path with its
/// flags, in the order the stream reported them.
pub(crate) type EventHandlerFn = dyn Fn(&[(PathBuf, u32)]) + Send + Sync;

/// What a stream's callback reads; the stream owns it and frees it on release.
struct StreamContext {
    handler: Arc<EventHandlerFn>,
}

extern "C" fn release_context(info: *const c_void) {
    // SAFETY: `info` is the `StreamContext` boxed when the stream was created,
    // and the stream calls this exactly once, when it is released.
    drop(unsafe { Box::from_raw(info.cast::<StreamContext>().cast_mut()) });
}

extern "C" fn drain(_: *mut c_void) {}

extern "C" fn stream_callback(
    _stream: FsEventStreamRef,
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
    let events = paths
        .iter()
        .zip(flags)
        .map(|(&path, &flags)| {
            // SAFETY: each path is a NUL-terminated string that lives for the
            // call.
            let path = PathBuf::from(OsStr::from_bytes(
                unsafe { CStr::from_ptr(path) }.to_bytes(),
            ));
            (path, flags)
        })
        .collect::<Vec<_>>();
    (context.handler)(&events);
}

/// One running `FSEvents` stream over some paths, delivering file events as
/// they happen on its own serial queue. Dropping it stops it, after any
/// batch in delivery.
pub(crate) struct EventStream {
    api: &'static Api,
    stream: FsEventStreamRef,
    queue: DispatchQueue,
}

// SAFETY: `FSEvents` streams and dispatch queues may be stopped and
// released from any thread, which only `Drop` does.
unsafe impl Send for EventStream {}
// SAFETY: as for `Send`; nothing is reachable through `&self`.
unsafe impl Sync for EventStream {}

impl EventStream {
    /// Starts one stream over `paths`, with `handler` receiving its events.
    pub(crate) fn start(paths: &[PathBuf], handler: Arc<EventHandlerFn>) -> notify::Result<Self> {
        let api = Api::get()?;
        let paths = api.path_array(paths)?;
        let context = Box::into_raw(Box::new(StreamContext { handler }));
        let stream_context = FsEventStreamContext {
            version: 0,
            info: context.cast(),
            retain: None,
            release: Some(release_context),
            copy_description: None,
        };
        // SAFETY: the array holds `CFString` paths, the context is valid for
        // the call, and the stream takes ownership of `info`, freeing it
        // through `release_context`.
        let stream = unsafe {
            (api.stream_create)(
                std::ptr::null(),
                stream_callback,
                &raw const stream_context,
                paths,
                EVENT_ID_SINCE_NOW,
                0.0,
                CREATE_FILE_EVENTS | CREATE_NO_DEFER,
            )
        };
        // SAFETY: the stream retains the array it needs.
        unsafe { (api.release)(paths) };
        if stream.is_null() {
            // SAFETY: no stream took ownership of the context.
            drop(unsafe { Box::from_raw(context) });
            return Err(Error::generic("cannot create an FSEvents stream"));
        }
        // SAFETY: the label is a valid C string; a null attribute makes the
        // queue serial.
        let queue =
            unsafe { dispatch_queue_create(c"acyclic.fsevents".as_ptr(), std::ptr::null()) };
        // SAFETY: the stream is new and unscheduled, and the queue is live.
        unsafe { (api.stream_set_dispatch_queue)(stream, queue) };
        let running = Self { api, stream, queue };
        // SAFETY: the stream is scheduled on its queue.
        if unsafe { (api.stream_start)(stream) } == 0 {
            return Err(Error::generic("cannot start an FSEvents stream"));
        }
        Ok(running)
    }
}

impl Drop for EventStream {
    fn drop(&mut self) {
        // SAFETY: the stream was started on this queue. Once it stops, no new
        // callback is queued; the synchronous no-op waits for any queued one,
        // so releasing the stream, which frees its context, races nothing.
        unsafe {
            (self.api.stream_stop)(self.stream);
            dispatch_sync_f(self.queue, std::ptr::null_mut(), drain);
            (self.api.stream_invalidate)(self.stream);
            (self.api.stream_release)(self.stream);
            dispatch_release(self.queue);
        }
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
#[allow(clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

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
