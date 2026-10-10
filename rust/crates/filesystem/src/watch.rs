//! Bounded native change notifications for materialized checkouts.
//!
//! Watch events are acceleration hints, never filesystem truth. Consumers read
//! the affected paths from the host and produce ordinary authenticated checkout
//! mutations. Queue overflow, backend uncertainty, ambiguous rename, root
//! replacement, restart, or an unrepresentable path invalidates the hint stream
//! and requires a new bounded baseline scan.

use crate::NativeRootIdentity;
use crate::cancellation::{CancellationError, CancellationToken};
use crate::kernel::{LogicalName, NamespacePath, NamespacePathError};
use crate::model::{FilesystemProfile, VolumeLimits};
use crate::performance::{
    MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError,
};
use notify::event::{ModifyKind, RenameMode};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use std::mem::size_of;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use thiserror::Error;

/// The host's native event watcher. On macOS it loads `FSEvents` when the
/// first one is created instead of linking `CoreServices`, which would load it
/// into every process at start.
#[cfg(target_os = "macos")]
pub type NativeEventWatcher = crate::fsevents::FsEventsWatcher;
/// The host's native event watcher.
#[cfg(not(target_os = "macos"))]
pub type NativeEventWatcher = notify::RecommendedWatcher;

/// Native notification mechanism compiled for this target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeWatchBackend {
    /// Linux inotify; demand admission uses the held-root native producer.
    LinuxInotify,
    /// macOS `FSEvents`, loaded on first use; see [`NativeEventWatcher`].
    MacosFsevents,
    /// Windows directory-change notifications, with held-handle demand admission.
    WindowsReadDirectoryChanges,
    /// No target-specific native notification mechanism is available.
    Unsupported,
}

impl NativeWatchBackend {
    /// Stable SDK spelling used by generated-language capability surfaces.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LinuxInotify => "linux-inotify",
            Self::MacosFsevents => "macos-fsevents",
            Self::WindowsReadDirectoryChanges => "windows-read-directory-changes",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Exact guarantees supplied by the compiled native watcher.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeWatchCapabilities {
    /// Host notification mechanism selected at compile time.
    pub backend: NativeWatchBackend,
    /// Whether descendant changes can be requested recursively.
    pub recursive: bool,
    /// Whether a persisted cursor can prove continuity across process restart.
    ///
    /// When false, reopening always requires the authenticated baseline
    /// handshake. A process-local [`WatchSequence`] is never a restart token.
    pub persistent_restart: bool,
    /// Whether every poll fences replacement of the admitted root identity.
    pub root_identity_fencing: bool,
}

/// Returns compile-time native watcher guarantees without touching a user path.
#[must_use]
pub const fn native_watch_capabilities() -> NativeWatchCapabilities {
    #[cfg(target_os = "linux")]
    let backend = NativeWatchBackend::LinuxInotify;
    #[cfg(target_os = "macos")]
    let backend = NativeWatchBackend::MacosFsevents;
    #[cfg(target_os = "windows")]
    let backend = NativeWatchBackend::WindowsReadDirectoryChanges;
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    let backend = NativeWatchBackend::Unsupported;

    NativeWatchCapabilities {
        backend,
        recursive: !matches!(backend, NativeWatchBackend::Unsupported),
        persistent_restart: false,
        root_identity_fencing: cfg!(any(unix, windows)),
    }
}

/// Process-local watcher epoch. It changes after every completed baseline scan.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct WatchEpoch(u64);

impl WatchEpoch {
    /// Constructs an epoch from a persisted process-local adapter value.
    #[must_use]
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Returns the monotonic process-local value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Contiguous sequence within one [`WatchEpoch`].
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct WatchSequence(u64);

impl WatchSequence {
    /// Constructs a contiguous sequence from an adapter cursor.
    #[must_use]
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Returns the contiguous process-local value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// One bounded host-side change hint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchChange {
    /// A namespace entry appeared.
    Created(NamespacePath),
    /// File content or an unspecified property may have changed.
    Modified(NamespacePath),
    /// A rename whose other end is unknown bound this name: what it names
    /// now, subtree and all, whatever was there before. A same-kind entry
    /// keeps its identity, as for [`Self::Modified`].
    Arrived(NamespacePath),
    /// Metadata or named attributes may have changed.
    MetadataChanged(NamespacePath),
    /// A namespace entry disappeared.
    Removed(NamespacePath),
    /// One paired rename with exact old and new relative paths.
    Renamed {
        /// Old relative path.
        from: NamespacePath,
        /// New relative path.
        to: NamespacePath,
    },
}

/// Why watcher hints can no longer prove a contiguous change interval.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum WatchInvalidationReason {
    /// Every newly opened watcher starts without an authenticated baseline.
    #[error("an initial authenticated baseline is required")]
    InitialSnapshotRequired,
    /// The bounded notification queue overflowed.
    #[error("the native notification queue overflowed")]
    QueueOverflow,
    /// The native backend reported lost or unreliable events.
    #[error("the native backend requires a rescan")]
    NativeRescanRequired,
    /// The native backend returned an asynchronous error.
    #[error("the native backend reported an asynchronous error")]
    BackendError,
    /// A native path escaped or could not be represented by volume semantics.
    #[error("a native path cannot be represented by volume semantics")]
    UnrepresentablePath,
    /// A rename could not be paired without guessing.
    #[error("a native rename could not be paired exactly")]
    AmbiguousRename,
    /// The watched root itself was removed, renamed, or replaced.
    #[error("the watched root changed identity")]
    RootChanged,
}

/// One bounded watcher poll result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchBatch {
    /// A contiguous process-local interval of change hints.
    Changes {
        /// Baseline epoch against which paths are interpreted.
        epoch: WatchEpoch,
        /// Sequence assigned to the first returned change.
        first_sequence: WatchSequence,
        /// Sequence to request after applying every returned change.
        next_sequence: WatchSequence,
        /// Bounded hints moved from the native queue without path copies.
        changes: Vec<WatchChange>,
    },
    /// All hints from this epoch must be discarded and a baseline rescanned.
    RescanRequired {
        /// Invalidated watcher epoch.
        epoch: WatchEpoch,
        /// Exact invalidation class.
        reason: WatchInvalidationReason,
    },
}

/// Bounded native watcher configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeWatchOptions {
    /// Volume path/name bounds used while converting host paths.
    pub limits: VolumeLimits,
    /// Maximum queued native changes before typed invalidation.
    pub maximum_queued_changes: u32,
    /// Whether descendants are observed recursively.
    pub recursive: bool,
}

#[cfg(unix)]
fn open_native_root(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW);
    options.open(path)
}

#[cfg(windows)]
fn open_native_root(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::windows::fs::MetadataExt;
    use std::os::windows::fs::OpenOptionsExt;
    use windows::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0);
    let file = options.open(path)?;
    if file.metadata()?.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "watch root is a reparse point",
        ));
    }
    Ok(file)
}

#[cfg(not(any(unix, windows)))]
fn open_native_root(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::File::open(path)
}

const fn native_filesystem_profile() -> FilesystemProfile {
    #[cfg(unix)]
    {
        FilesystemProfile::Posix
    }
    #[cfg(windows)]
    {
        FilesystemProfile::Windows
    }
    #[cfg(not(any(unix, windows)))]
    {
        FilesystemProfile::Portable
    }
}

impl NativeWatchOptions {
    /// Conservative recursive defaults.
    #[must_use]
    pub const fn new(limits: VolumeLimits) -> Self {
        Self {
            limits,
            maximum_queued_changes: 16_384,
            recursive: true,
        }
    }

    fn validate(self) -> Result<Self, NativeWatchError> {
        if self.maximum_queued_changes == 0 {
            return Err(NativeWatchError::InvalidOptions);
        }
        Ok(self)
    }
}

#[derive(Debug)]
struct SharedState {
    invalidation: Option<WatchInvalidationReason>,
    #[cfg(target_os = "linux")]
    pending_rename: Option<PendingRename>,
    /// Host path of the outstanding fence cookie, cleared on delivery.
    fence: Option<PathBuf>,
    /// Host subtrees whose events are never hints, in fence preference order.
    excluded: Vec<PathBuf>,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct PendingRename {
    tracker: usize,
    from: NamespacePath,
    to: Option<NamespacePath>,
}

impl SharedState {
    const fn new(invalidation: Option<WatchInvalidationReason>) -> Self {
        Self {
            invalidation,
            #[cfg(target_os = "linux")]
            pending_rename: None,
            fence: None,
            excluded: Vec::new(),
        }
    }

    fn seal_invalidation(&mut self) -> Option<WatchInvalidationReason> {
        #[cfg(target_os = "linux")]
        if self.invalidation.is_none() && self.pending_rename.is_some() {
            self.invalidation = Some(WatchInvalidationReason::AmbiguousRename);
        }
        self.invalidation
    }
}

struct NativeEventContext {
    root: PathBuf,
    root_identity: NativeRootIdentity,
    profile: FilesystemProfile,
    limits: VolumeLimits,
    #[cfg(target_os = "linux")]
    maximum_queued_changes: u32,
}

enum NativeWatchDriver {
    Adapter(NativeEventWatcher),
    #[cfg(target_os = "linux")]
    Source { watcher: crate::source_watch::NativeSourceWatch, root: PathBuf },
}

impl NativeWatchDriver {
    fn watch(&mut self, path: &Path, mode: RecursiveMode) -> notify::Result<()> {
        match self {
            Self::Adapter(watcher) => watcher.watch(path, mode),
            #[cfg(target_os = "linux")]
            Self::Source { watcher, root } => {
                if mode != RecursiveMode::NonRecursive {
                    return Err(notify::Error::generic("the source driver is nonrecursive"));
                }
                let relative = path.strip_prefix(root)
                    .map_err(|_| notify::Error::generic("watch path escaped its held root"))?;
                let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
                watcher.admit_measured(relative, &mut work)
                    .map(|_| ()).map_err(|error| notify::Error::generic(&error.to_string()))
            }
        }
    }

    fn unwatch(&mut self, path: &Path) -> notify::Result<()> {
        match self {
            Self::Adapter(watcher) => watcher.unwatch(path),
            #[cfg(target_os = "linux")]
            Self::Source { watcher, root } => {
                let relative = path.strip_prefix(root)
                    .map_err(|_| notify::Error::generic("unwatch path escaped its held root"))?;
                let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
                watcher.unadmit_measured(relative, &mut work)
                    .map_err(|error| notify::Error::generic(&error.to_string()))
            }
        }
    }
}

#[cfg(target_os = "linux")]
struct LinuxHintInvalidation(Arc<Mutex<SharedState>>);

#[cfg(target_os = "linux")]
impl crate::source_watch::HostChangeSink for LinuxHintInvalidation {
    fn host_changed(&self, changes: &[crate::source_watch::HostChange]) {
        if changes.iter().any(|change| matches!(change, crate::source_watch::HostChange::Everything))
            && let Ok(mut state) = self.0.lock()
        {
            state.invalidation = Some(WatchInvalidationReason::NativeRescanRequired);
        }
    }
}

#[cfg(target_os = "linux")]
struct LinuxEventDelivery {
    context: NativeEventContext,
    sender: SyncSender<WatchChange>,
    shared: Arc<Mutex<SharedState>>,
    queued: Arc<AtomicU32>,
    fenced: Arc<Condvar>,
}

#[cfg(target_os = "linux")]
impl crate::source_watch::NativeEventSink for LinuxEventDelivery {
    fn event(&self, event: Event) {
        let mask = match event.kind {
            EventKind::Create(_) => libc::IN_CREATE,
            EventKind::Remove(_) => libc::IN_DELETE,
            EventKind::Modify(ModifyKind::Name(RenameMode::From)) => libc::IN_MOVED_FROM,
            EventKind::Modify(ModifyKind::Name(RenameMode::To)) => libc::IN_MOVED_TO,
            EventKind::Modify(ModifyKind::Metadata(_)) => libc::IN_ATTRIB,
            EventKind::Modify(_) => libc::IN_MODIFY,
            _ => 0,
        };
        let cancellation = CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        for path in &event.paths {
            if self.event_measured(path, mask, event.attrs.tracker().unwrap_or(0), &mut work).is_err() {
                let _ = self.invalidate_measured(&mut work);
                break;
            }
        }
    }

    fn invalidate_measured(
        &self, work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        work.check()?;
        let mut state = self.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        state.invalidation = Some(WatchInvalidationReason::NativeRescanRequired);
        self.fenced.notify_all();
        Ok(())
    }

    fn event_measured(
        &self, relative: &Path, mask: u32, cookie: usize, work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        use crate::demand::DemandError;
        work.check()?;
        let mut state = self.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        if state.fence.as_ref().is_some_and(|fence| fence.strip_prefix(&self.context.root) == Ok(relative)) {
            state.fence = None;
            self.fenced.notify_all();
        }
        if state.invalidation.is_some() { return Ok(()); }
        for excluded in &state.excluded {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            if let Ok(path) = excluded.strip_prefix(&self.context.root)
                && relative.starts_with(path)
            {
                return Ok(());
            }
        }
        if mask & (libc::IN_CREATE | libc::IN_DELETE | libc::IN_MODIFY | libc::IN_ATTRIB
            | libc::IN_MOVED_FROM | libc::IN_MOVED_TO) == 0
        {
            return Ok(());
        }
        if relative.as_os_str().is_empty() {
            state.invalidation = Some(WatchInvalidationReason::RootChanged);
            return Ok(());
        }
        if self.queued.load(Ordering::Acquire)
            .saturating_add(u32::from(state.pending_rename.is_some())) >= self.context.maximum_queued_changes
        {
            state.invalidation = Some(WatchInvalidationReason::QueueOverflow);
            return Ok(());
        }
        let (path, bytes) = match namespace_hint_measured(relative, self.context.profile, self.context.limits, work) {
            Ok(path) => path,
            Err(DemandError::InvalidRequest) => {
                state.invalidation = Some(WatchInvalidationReason::UnrepresentablePath);
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let result = (|| {
            if mask & libc::IN_MOVED_FROM != 0 {
                if cookie == 0 || state.pending_rename.is_some() {
                    state.invalidation = Some(WatchInvalidationReason::AmbiguousRename);
                } else {
                    state.pending_rename = Some(PendingRename { tracker: cookie, from: path, to: None });
                }
                return Ok(());
            }
            let output = WorkCounters { items_returned: 1, ..WorkCounters::default() };
            work.work.admit(&output, &work.budget)?;
            let change = if mask & libc::IN_MOVED_TO != 0 {
                if cookie == 0 || !state.pending_rename.as_ref().is_some_and(|pending| pending.tracker == cookie) {
                    state.invalidation = Some(WatchInvalidationReason::AmbiguousRename);
                    return Ok(());
                }
                let pending = state.pending_rename.take().expect("matched native rename cookie");
                WatchChange::Renamed { from: pending.from, to: path }
            } else if mask & libc::IN_CREATE != 0 {
                WatchChange::Created(path)
            } else if mask & libc::IN_DELETE != 0 {
                WatchChange::Removed(path)
            } else if mask & libc::IN_ATTRIB != 0 {
                WatchChange::MetadataChanged(path)
            } else {
                WatchChange::Modified(path)
            };
            match self.sender.try_send(change) {
                Ok(()) => {
                    work.record(output)?;
                    self.queued.fetch_add(1, Ordering::Release);
                }
                Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                    state.invalidation = Some(WatchInvalidationReason::QueueOverflow);
                }
            }
            Ok::<(), DemandError>(())
        })();
        work.release(bytes)?;
        result
    }
}

#[cfg(target_os = "linux")]
fn namespace_hint_measured(
    relative: &Path, profile: FilesystemProfile, limits: VolumeLimits, work: &mut crate::demand::DemandWork,
) -> Result<(NamespacePath, u64), crate::demand::DemandError> {
    use crate::demand::DemandError;
    let mut count = 0_usize;
    for component in relative.components() {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
        if !matches!(component, Component::Normal(_)) { return Err(DemandError::InvalidRequest); }
        count = count.checked_add(1).ok_or(WorkError::Overflow)?;
    }
    let backing = count.checked_mul(size_of::<LogicalName>()).ok_or(WorkError::Overflow)? as u64;
    work.allocate(backing)?;
    let mut claimed = backing;
    let mut components = Vec::with_capacity(count);
    let result = (|| {
        for component in relative.components() {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            let Component::Normal(name) = component else { return Err(DemandError::InvalidRequest); };
            let (encoding, bytes) = crate::native_name::host_name_bytes_measured(
                name, profile, limits.maximum_component_bytes, work,
            )?;
            claimed = claimed.checked_add(bytes.capacity() as u64).ok_or(WorkError::Overflow)?;
            components.push(LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
                .map_err(|_| DemandError::InvalidRequest)?);
        }
        NamespacePath::new(components, limits).map_err(|_| DemandError::InvalidRequest)
    })();
    match result {
        Ok(path) => Ok((path, claimed)),
        Err(error) => {
            work.release(claimed)?;
            Err(error)
        }
    }
}

#[cfg(windows)]
struct WindowsDirectoryWatch {
    path: Arc<Vec<u16>>,
    identity: NativeRootIdentity,
    watcher: crate::source_watch::NativeSourceWatch,
    owned_bytes: u64,
}

#[cfg(windows)]
struct WindowsHintDelivery {
    context: NativeEventContext,
    sender: SyncSender<WatchChange>,
    shared: Arc<Mutex<SharedState>>,
    queued: Arc<AtomicU32>,
    fenced: Arc<Condvar>,
}

#[cfg(windows)]
struct WindowsDirectorySink {
    prefix: Arc<Vec<u16>>,
    delivery: Arc<WindowsHintDelivery>,
}

#[cfg(windows)]
impl crate::source_watch::HostChangeSink for WindowsDirectorySink {
    fn host_changed(&self, changes: &[crate::source_watch::HostChange]) {
        use crate::source_watch::HostChange;
        use std::os::windows::ffi::OsStringExt as _;
        for change in changes {
            let path = match change {
                HostChange::Rebound(path) | HostChange::Altered(path) => path,
                HostChange::File(_) => continue,
                HostChange::Everything | HostChange::Unconfirmed => {
                    if let Ok(mut state) = self.delivery.shared.lock() {
                        state.invalidation = Some(WatchInvalidationReason::NativeRescanRequired);
                    }
                    continue;
                }
            };
            // Decoding/delivery is the existing driver's ordinary asynchronous
            // path, not finite directory admission. Preserve flexible UTF-16.
            let prefix = PathBuf::from(std::ffi::OsString::from_wide(&self.prefix));
            let absolute = self.delivery.context.root.join(prefix).join(path);
            let event = Ok(Event::new(EventKind::Modify(ModifyKind::Any)).add_path(absolute));
            accept_native_event(&event, &self.delivery.context, &self.delivery.sender,
                &self.delivery.shared, &self.delivery.queued);
            if let (Ok(event), Ok(mut state)) = (&event, self.delivery.shared.lock())
                && state.fence.as_ref().is_some_and(|fence| event.paths.contains(fence))
            {
                state.fence = None;
                self.delivery.fenced.notify_all();
            }
        }
    }
}

/// One live native watcher over a materialized checkout root.
///
/// Opening starts invalidated. Call [`Self::begin_rescan`], build an exact
/// baseline while notifications continue to queue, and call
/// [`Self::finish_rescan`] before polling. The same handshake repairs overflow
/// without losing changes concurrent with the baseline scan.
pub struct NativeWatch {
    watcher: NativeWatchDriver,
    root: PathBuf,
    recursive: bool,
    watched_directories: Vec<PathBuf>,
    receiver: Receiver<WatchChange>,
    #[cfg(windows)]
    windows_root: crate::native_host::HostRoot,
    #[cfg(windows)]
    windows_directories: Vec<WindowsDirectoryWatch>,
    #[cfg(windows)]
    windows_delivery: Arc<WindowsHintDelivery>,
    queued: Arc<AtomicU32>,
    shared: Arc<Mutex<SharedState>>,
    fenced: Arc<Condvar>,
    fences: u64,
    excluded: Vec<(NamespacePath, PathBuf)>,
    epoch: WatchEpoch,
    next_sequence: WatchSequence,
    rescan_in_progress: bool,
    root_identity: NativeRootIdentity,
    complete: bool,
}

impl NativeWatch {
    /// Returns exact guarantees for this watcher's compiled backend.
    #[must_use]
    pub const fn capabilities(&self) -> NativeWatchCapabilities {
        native_watch_capabilities()
    }

    /// Opens the platform-recommended event source.
    ///
    /// Linux uses inotify, macOS uses `FSEvents`, and Windows uses
    /// `ReadDirectoryChangesW` through the reviewed `notify` adapter. Native
    /// journals can later implement the same invalidation protocol.
    ///
    /// # Errors
    ///
    /// Rejects invalid bounds, an unavailable/non-directory root, allocation
    /// conversion, poisoned synchronization, or native watcher startup failure.
    pub fn open(
        root: impl AsRef<Path>,
        options: NativeWatchOptions,
    ) -> Result<Self, NativeWatchError> {
        Self::open_with_profile(root, native_filesystem_profile(), options)
    }

    /// Opens a watcher whose emitted names use the supplied volume profile.
    ///
    /// Use this when watcher paths feed a volume whose profile can differ from
    /// the host-native profile selected by [`Self::open`].
    ///
    /// # Errors
    ///
    /// Rejects invalid bounds, an unavailable/non-directory root, allocation
    /// conversion, poisoned synchronization, or native watcher startup failure.
    pub fn open_with_profile(
        root: impl AsRef<Path>,
        profile: FilesystemProfile,
        options: NativeWatchOptions,
    ) -> Result<Self, NativeWatchError> {
        Self::open_with_profile_measured(
            root, profile, options, WorkBudget::UNBOUNDED, &CancellationToken::new(),
        )
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
    }

    /// Opens the existing watcher with explicit preparation work/cancellation.
    /// Charges each root request and watcher registration before calling it;
    /// failure retains all admitted requests. Linux recursive registration is
    /// not constant-size and is rejected by this bounded root-only boundary.
    pub fn open_with_profile_measured(
        root: impl AsRef<Path>,
        profile: FilesystemProfile,
        options: NativeWatchOptions,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<Self>, NativeWatchError> {
        let span = crate::obs::span!(
            DEBUG,
            "acyclic.fs.watch.open",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
        );
        let mut work = WorkCounters::default();
        let mut charge = |write: bool| -> Result<(), NativeWatchError> {
            cancellation.check()?;
            let next = work.checked_add(WorkCounters {
                backend_read_operations: u64::from(!write),
                backend_write_operations: u64::from(write),
                ..WorkCounters::default()
            })?;
            next.verify(budget)?;
            work = next;
            Ok(())
        };
        let result = crate::obs::scope(&span, || {
            let options = options.validate()?;
            if cfg!(target_os = "linux") && options.recursive && budget.has_finite_allowances() {
                return Err(NativeWatchError::InvalidOptions);
            }
            let requested_root = root.as_ref();
            charge(false)?;
            let admission = requested_root.symlink_metadata()?;
            if admission.file_type().is_symlink() {
                return Err(NativeWatchError::RootIsNotDirectory);
            }
            charge(false)?;
            let root = requested_root.canonicalize()?;
            charge(false)?;
            let root_file = open_native_root(&root)?;
            charge(false)?;
            let metadata = root_file.metadata()?;
            if !metadata.is_dir() {
                return Err(NativeWatchError::RootIsNotDirectory);
            }
            charge(false)?;
            let root_identity = NativeRootIdentity::from_file(&root_file)?;
            #[cfg(windows)]
            let complete = {
                charge(false)?;
                crate::native_host::reports_every_change(&root_file)?
            };
            #[cfg(not(windows))]
            let complete = true;
            let capacity = usize::try_from(options.maximum_queued_changes)
                .map_err(|_| NativeWatchError::InvalidOptions)?;
            let (sender, receiver) = sync_channel(capacity);
            let queued = Arc::new(AtomicU32::new(0));
            let shared = Arc::new(Mutex::new(SharedState::new(Some(
                WatchInvalidationReason::InitialSnapshotRequired,
            ))));
            let fenced = Arc::new(Condvar::new());
            let callback_shared = Arc::clone(&shared);
            let callback_fenced = Arc::clone(&fenced);
            let callback_queued = Arc::clone(&queued);
            let callback_context = NativeEventContext {
                root: root.clone(),
                root_identity,
                profile,
                limits: options.limits,
                #[cfg(target_os = "linux")]
                maximum_queued_changes: options.maximum_queued_changes,
            };
            #[cfg(windows)]
            let windows_delivery = Arc::new(WindowsHintDelivery {
                context: NativeEventContext {
                    root: root.clone(), root_identity, profile, limits: options.limits,
                },
                sender: sender.clone(),
                shared: Arc::clone(&shared),
                queued: Arc::clone(&queued),
                fenced: Arc::clone(&fenced),
            });
            charge(true)?;
            #[cfg(target_os = "linux")]
            let linux_sender = sender.clone();
            let callback = move |event: notify::Result<Event>| {
                    accept_native_event(
                        &event,
                        &callback_context,
                        &sender,
                        &callback_shared,
                        &callback_queued,
                    );
                    // Signal only after the cookie's own hint is handled: delivery is
                    // ordered, so every earlier host change is already queued.
                    if let (Ok(event), Ok(mut state)) = (&event, callback_shared.lock())
                        && state
                            .fence
                            .as_ref()
                            .is_some_and(|fence| event.paths.contains(fence))
                    {
                        state.fence = None;
                        callback_fenced.notify_all();
                    }
                };
            #[cfg(target_os = "linux")]
            let mut watcher = if options.recursive {
                NativeWatchDriver::Adapter(NativeEventWatcher::new(callback, notify::Config::default())
                    .map_err(|error| NativeWatchError::Backend(error.to_string()))?)
            } else {
                let held_root = crate::native_host::HostRoot::from_held_dir_with_identity(
                    cap_std::fs::Dir::from_std_file(root_file), root_identity,
                );
                let events = Arc::new(LinuxEventDelivery {
                    context: NativeEventContext {
                        root: root.clone(), root_identity, profile, limits: options.limits,
                        maximum_queued_changes: options.maximum_queued_changes,
                    },
                    sender: linux_sender,
                    shared: Arc::clone(&shared),
                    queued: Arc::clone(&queued),
                    fenced: Arc::clone(&fenced),
                });
                let source = crate::source_watch::NativeSourceWatch::start_with_events(
                    &held_root, Arc::new(LinuxHintInvalidation(Arc::clone(&shared))), events,
                )?;
                NativeWatchDriver::Source { watcher: source, root: root.clone() }
            };
            #[cfg(not(target_os = "linux"))]
            let mut watcher = NativeWatchDriver::Adapter(
                NativeEventWatcher::new(callback, notify::Config::default())
                    .map_err(|error| NativeWatchError::Backend(error.to_string()))?,
            );
            charge(true)?;
            watcher
                .watch(
                    &root,
                    if options.recursive {
                        RecursiveMode::Recursive
                    } else {
                        RecursiveMode::NonRecursive
                    },
                )
                .map_err(|error| NativeWatchError::Backend(error.to_string()))?;
            cancellation.check()?;
            Ok(Self {
                watcher,
                watched_directories: vec![root.clone()],
                root,
                recursive: options.recursive,
                receiver,
                #[cfg(windows)]
                windows_root: crate::native_host::HostRoot::from_held_dir_with_identity(
                    cap_std::fs::Dir::from_std_file(root_file), root_identity,
                ),
                #[cfg(windows)]
                windows_directories: Vec::new(),
                #[cfg(windows)]
                windows_delivery,
                queued,
                shared,
                fenced,
                fences: 0,
                excluded: Vec::new(),
                epoch: WatchEpoch(0),
                next_sequence: WatchSequence(0),
                rescan_in_progress: false,
                root_identity,
                complete,
            })
        });
        crate::obs::measured_on(
            &span,
            result
                .map(|value| OperationReceipt { value, work })
                .map_err(|error| OperationFailure::new(error, work)),
            |receipt| &receipt.work,
        )
    }

    /// Whether the root's file system reports every change, or that it lost
    /// some. Where it does not (`ReFS`, network shares), a batch without a
    /// change proves nothing, and only a baseline scan finds what changed.
    #[must_use]
    pub const fn reports_every_change(&self) -> bool {
        self.complete
    }

    /// Adds one exact directory to a non-recursive native watcher.
    ///
    /// Recursive backends already cover the path and return immediately. A
    /// non-recursive watcher validates each requested prefix without following
    /// a symbolic link and admits directory subscriptions without enumeration.
    ///
    /// # Errors
    ///
    /// Rejects an absent/non-directory path, a symbolic-link prefix, a path
    /// that cannot be represented on this host, or a backend registration
    /// failure.
    pub fn watch_directory(&mut self, directory: &NamespacePath) -> Result<(), NativeWatchError> {
        #[cfg(not(target_os = "linux"))]
        return self.watch_directory_ordinary(directory);
        #[cfg(target_os = "linux")]
        {
            let cancellation = CancellationToken::new();
            let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
            self.watch_directory_measured(directory, &mut work).map_err(|error| match error {
                crate::demand::DemandError::Observation(error) => error,
                crate::demand::DemandError::Work(error) => NativeWatchError::Work(error),
                crate::demand::DemandError::Cancelled => NativeWatchError::Cancelled(CancellationError),
                crate::demand::DemandError::Io(error) => error.into(),
                error => NativeWatchError::Io(error.to_string()),
            })
        }
    }

    // Ordinary adapters may perform their existing unbounded path conversion.
    // Finite demand observation never calls their opaque registration API.
    #[cfg(not(target_os = "linux"))]
    fn watch_directory_ordinary(&mut self, directory: &NamespacePath) -> Result<(), NativeWatchError> {
        if self.recursive || directory.is_root() {
            return Ok(());
        }
        let relative = crate::native_capture::namespace_to_host_path(directory)
            .map_err(|_| NativeWatchError::UnrepresentablePath)?;
        let mut candidate = self.root.clone();
        for component in relative.components() {
            let Component::Normal(component) = component else {
                return Err(NativeWatchError::UnrepresentablePath);
            };
            candidate.push(component);
            let metadata = match candidate.symlink_metadata() {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    candidate.pop();
                    if candidate == self.root {
                        return Ok(());
                    }
                    break;
                }
                Err(error) => return Err(error.into()),
            };
            if metadata.file_type().is_symlink() {
                return Err(NativeWatchError::SymbolicLinkDirectory);
            }
            if !metadata.is_dir() {
                return Err(NativeWatchError::ObservedPathNotDirectory);
            }
        }
        if self.watched_directories.contains(&candidate) {
            return Ok(());
        }
        self.watcher.watch(&candidate, RecursiveMode::NonRecursive)
            .map_err(|error| NativeWatchError::Backend(error.to_string()))?;
        let at = self.watched_directories.partition_point(|path| path < &candidate);
        self.watched_directories.insert(at, candidate);
        Ok(())
    }

    fn watch_directory_measured(
        &mut self,
        directory: &NamespacePath,
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        work.check()?;
        if self.recursive || directory.is_root() {
            return Ok(());
        }
        #[cfg(windows)]
        return self.watch_directory_wide_measured(directory, work);
        #[cfg(not(any(windows, target_os = "linux")))]
        return Err(NativeWatchError::BoundedObservationUnavailable.into());
        #[cfg(target_os = "linux")]
        {
        let mut candidate = PathBuf::new();
        let mut candidate_bytes = 0_u64;
        let mut retained_bytes = 0_u64;
        let result = (|| {
            let mut baseline = (self.watched_directories.capacity() as u64)
                .checked_mul(size_of::<PathBuf>() as u64).ok_or(WorkError::Overflow)?;
            for path in &self.watched_directories {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                baseline = baseline.checked_add(path.capacity() as u64).ok_or(WorkError::Overflow)?;
            }
            work.adopt(baseline)?;
            retained_bytes = baseline;
            grow_watch_path_measured(&mut candidate, &self.root, work, &mut candidate_bytes)?;
            for component in directory.components() {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                let (name, name_bytes) = watch_host_name_measured(component, work)?;
                let mut host_components = Path::new(&name).components();
                if !matches!(host_components.next(), Some(Component::Normal(_)))
                    || host_components.next().is_some()
                {
                    drop(name);
                    work.release(name_bytes)?;
                    return Err(NativeWatchError::UnrepresentablePath.into());
                }
                let grown = grow_watch_path_measured(&mut candidate, Path::new(&name), work, &mut candidate_bytes);
                drop(name);
                work.release(name_bytes)?;
                grown?;
                work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::default() })?;
                let metadata = match candidate.symlink_metadata() {
                    Ok(metadata) => metadata,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        candidate.pop();
                        if candidate == self.root {
                            return Ok(());
                        }
                        break;
                    }
                    Err(error) => return Err(NativeWatchError::from(error).into()),
                };
                if metadata.file_type().is_symlink() {
                    return Err(NativeWatchError::SymbolicLinkDirectory.into());
                }
                if !metadata.is_dir() {
                    return Err(NativeWatchError::ObservedPathNotDirectory.into());
                }
            }
            match &self.watcher {
                NativeWatchDriver::Source { watcher, .. } => {
                    if !watcher.is_exact() {
                        return Err(NativeWatchError::BoundedObservationUnavailable.into());
                    }
                    let relative = candidate.strip_prefix(&self.root)
                        .map_err(|_| NativeWatchError::UnrepresentablePath)?;
                    if !watcher.admit_measured(relative, work)? { return Ok(()); }
                }
                NativeWatchDriver::Adapter(_) => {
                    return Err(NativeWatchError::BoundedObservationUnavailable.into());
                }
            }
            let mut left = 0_usize;
            let mut right = self.watched_directories.len();
            while left < right {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                let middle = left + (right - left) / 2;
                match self.watched_directories[middle].cmp(&candidate) {
                    std::cmp::Ordering::Equal => return Ok(()),
                    std::cmp::Ordering::Less => left = middle + 1,
                    std::cmp::Ordering::Greater => right = middle,
                }
            }
            if self.watched_directories.len() == self.watched_directories.capacity() {
                let capacity = self.watched_directories.len().checked_add(1).ok_or(WorkError::Overflow)?;
                let bytes = (capacity as u64).checked_mul(size_of::<PathBuf>() as u64)
                    .ok_or(WorkError::Overflow)?;
                let old_bytes = (self.watched_directories.capacity() as u64)
                    .checked_mul(size_of::<PathBuf>() as u64).ok_or(WorkError::Overflow)?;
                let copied = WorkCounters { bytes_copied: old_bytes, ..WorkCounters::default() };
                work.work.admit(&copied, &work.budget)?;
                work.allocate(bytes)?;
                work.record(copied)?;
                let mut grown = Vec::with_capacity(capacity);
                grown.append(&mut self.watched_directories);
                self.watched_directories = grown;
                work.release(old_bytes)?;
                retained_bytes = retained_bytes.checked_sub(old_bytes)
                    .and_then(|prior| prior.checked_add(bytes)).ok_or(WorkError::Overflow)?;
            }
            let shifted = WorkCounters {
                bytes_copied: ((self.watched_directories.len() - left) as u64)
                    .checked_mul(size_of::<PathBuf>() as u64).ok_or(WorkError::Overflow)?,
                ..WorkCounters::default()
            };
            work.work.admit(&shifted, &work.budget)?;
            work.record(shifted)?;
            self.watched_directories.insert(left, std::mem::take(&mut candidate));
            retained_bytes = retained_bytes.checked_add(candidate_bytes).ok_or(WorkError::Overflow)?;
            candidate_bytes = 0;
            Ok(())
        })();
        drop(candidate);
        work.release(candidate_bytes)?;
        work.release(retained_bytes)?;
        result
        }
    }

    #[cfg(target_os = "linux")]
    fn clear_linux_directories_measured(
        &mut self, work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        let NativeWatchDriver::Source { watcher, .. } = &self.watcher else { return Ok(()); };
        let mut retained = (self.watched_directories.capacity() as u64)
            .checked_mul(size_of::<PathBuf>() as u64).ok_or(WorkError::Overflow)?;
        for path in &self.watched_directories {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            retained = retained.checked_add(path.capacity() as u64).ok_or(WorkError::Overflow)?;
        }
        work.adopt(retained)?;
        let result = (|| {
            watcher.unadmit_measured(Path::new(""), work)?;
            while self.watched_directories.len() > 1 {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                let removed = self.watched_directories.pop().ok_or(crate::demand::DemandError::InvalidRequest)?;
                let bytes = removed.capacity() as u64;
                drop(removed);
                work.release(bytes)?;
                retained = retained.checked_sub(bytes).ok_or(WorkError::Overflow)?;
            }
            Ok(())
        })();
        work.release(retained)?;
        result
    }

    #[cfg(windows)]
    fn watch_directory_wide_measured(
        &mut self,
        directory: &NamespacePath,
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        use crate::demand::DemandError;
        let mut retained_bytes = 0_u64;
        let mut path_bytes = 0_u64;
        let result = (|| {
            let mut baseline = (self.windows_directories.capacity() as u64)
                .checked_mul(size_of::<WindowsDirectoryWatch>() as u64).ok_or(WorkError::Overflow)?;
            for observed in &self.windows_directories {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                baseline = baseline.checked_add(observed.owned_bytes).ok_or(WorkError::Overflow)?;
            }
            work.adopt(baseline)?;
            retained_bytes = baseline;
            let path = crate::demand::native_wide_path(directory, self.windows_delivery.context.limits, work)?;
            path_bytes = (path.capacity() as u64).checked_mul(2).ok_or(WorkError::Overflow)?;
            let result = (|| {
                let (mut held, _) = self.windows_root.open_dir_held_wide_with_identity_measured(&[], work)?;
                let mut component_start = 0;
                for index in 0..path.len() {
                    work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                    if path[index] != u16::from(b'\\') && index + 1 != path.len() {
                        continue;
                    }
                    let length = if path[index] == u16::from(b'\\') { index } else { index + 1 };
                    let prefix = &path[..length];
                    let (next, identity) = match self.windows_root.open_dir_component_wide_measured(
                        &held, &path[component_start..length], work,
                    ) {
                        Ok(observed) => observed,
                        Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                        Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotADirectory => {
                            return Err(DemandError::Observation(NativeWatchError::ObservedPathNotDirectory));
                        }
                        Err(DemandError::InvalidRequest) => {
                            return Err(DemandError::Observation(NativeWatchError::SymbolicLinkDirectory));
                        }
                        Err(error) => return Err(error),
                    };
                    held = next;
                    component_start = index + 1;
                    let mut found = None;
                    for (at, observed) in self.windows_directories.iter().enumerate() {
                        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                        if observed.path.as_slice() == prefix {
                            found = Some(at);
                            break;
                        }
                    }
                    if let Some(at) = found {
                        if self.windows_directories[at].identity == identity
                            && self.windows_directories[at].watcher.is_exact()
                        {
                            continue;
                        }
                        let copied = WorkCounters {
                            bytes_copied: ((self.windows_directories.len() - at - 1) as u64)
                                .checked_mul(size_of::<WindowsDirectoryWatch>() as u64).ok_or(WorkError::Overflow)?,
                            ..WorkCounters::default()
                        };
                        work.work.admit(&copied, &work.budget)?;
                        self.windows_directories[at].watcher.stop_measured(work)?;
                        work.record(copied)?;
                        let prior = self.windows_directories.remove(at);
                        let prior_bytes = prior.owned_bytes;
                        drop(prior);
                        work.release(prior_bytes)?;
                        retained_bytes = retained_bytes.checked_sub(prior_bytes).ok_or(WorkError::Overflow)?;
                    }
                    if self.windows_directories.len() == self.windows_directories.capacity() {
                        let count = self.windows_directories.len().checked_add(1).ok_or(WorkError::Overflow)?;
                        let bytes = (count as u64).checked_mul(size_of::<WindowsDirectoryWatch>() as u64)
                            .ok_or(WorkError::Overflow)?;
                        let old_bytes = (self.windows_directories.capacity() as u64)
                            .checked_mul(size_of::<WindowsDirectoryWatch>() as u64).ok_or(WorkError::Overflow)?;
                        let copied = WorkCounters { bytes_copied: old_bytes, ..WorkCounters::default() };
                        work.work.admit(&copied, &work.budget)?;
                        work.allocate(bytes)?;
                        work.record(copied)?;
                        let mut grown = Vec::with_capacity(count);
                        grown.append(&mut self.windows_directories);
                        self.windows_directories = grown;
                        work.release(old_bytes)?;
                        retained_bytes = retained_bytes.checked_sub(old_bytes)
                            .and_then(|prior| prior.checked_add(bytes)).ok_or(WorkError::Overflow)?;
                    }
                    let key_bytes = (prefix.len() as u64).checked_mul(2).ok_or(WorkError::Overflow)?;
                    let copied = WorkCounters { bytes_copied: key_bytes, ..WorkCounters::default() };
                    work.work.admit(&copied, &work.budget)?;
                    work.allocate(key_bytes)?;
                    work.record(copied)?;
                    let key = prefix.to_vec();
                    let key_arc_bytes = size_of::<Vec<u16>>() as u64;
                    if let Err(error) = work.allocate(key_arc_bytes) {
                        drop(key);
                        work.release(key_bytes)?;
                        return Err(error);
                    }
                    let key = Arc::new(key);
                    let sink_bytes = size_of::<WindowsDirectorySink>() as u64;
                    if let Err(error) = work.allocate(sink_bytes) {
                        drop(key);
                        work.release(key_bytes + key_arc_bytes)?;
                        return Err(error);
                    }
                    let sink = Arc::new(WindowsDirectorySink {
                        prefix: Arc::clone(&key), delivery: Arc::clone(&self.windows_delivery),
                    });
                    let watcher = match crate::source_watch::NativeSourceWatch::start_directory_measured(
                        &held, sink, self.complete, work,
                    ) {
                        Ok(watcher) => watcher,
                        Err(error) => {
                            drop(key);
                            work.release(key_bytes + key_arc_bytes + sink_bytes)?;
                            return Err(error);
                        }
                    };
                    let owned_bytes = key_bytes + key_arc_bytes + sink_bytes + watcher.owned_bytes();
                    self.windows_directories.push(WindowsDirectoryWatch { path: key, identity, watcher, owned_bytes });
                    retained_bytes = retained_bytes.checked_add(owned_bytes).ok_or(WorkError::Overflow)?;
                }
                Ok(())
            })();
            drop(path);
            work.release(path_bytes)?;
            path_bytes = 0;
            result
        })();
        work.release(path_bytes)?;
        work.release(retained_bytes)?;
        result
    }

    #[cfg(windows)]
    fn clear_windows_directories_measured(
        &mut self,
        work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        work.check()?;
        if self.windows_directories.is_empty() {
            return Ok(());
        }
        let mut bytes = (self.windows_directories.capacity() as u64)
            .checked_mul(size_of::<WindowsDirectoryWatch>() as u64).ok_or(WorkError::Overflow)?;
        for observed in &self.windows_directories {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            bytes = bytes.checked_add(observed.owned_bytes).ok_or(WorkError::Overflow)?;
        }
        work.adopt(bytes)?;
        let result = (|| {
            while let Some(observed) = self.windows_directories.last_mut() {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                observed.watcher.stop_measured(work)?;
                let removed = self.windows_directories.pop().ok_or(crate::demand::DemandError::InvalidRequest)?;
                let freed = removed.owned_bytes;
                drop(removed);
                work.release(freed)?;
                bytes = bytes.checked_sub(freed).ok_or(WorkError::Overflow)?;
            }
            self.windows_directories = Vec::new();
            Ok(())
        })();
        work.release(bytes)?;
        result
    }

    /// Adds every directory beneath the root, except the root names in
    /// `excluded`, to a non-recursive native watcher: what a baseline that
    /// captures the whole tree needs before it starts, so a change to
    /// anything it captured is reported. Recursive backends already cover
    /// the tree. A directory removed during the walk is skipped.
    ///
    /// # Errors
    ///
    /// Returns a listing or backend registration failure.
    pub fn watch_tree(&mut self, excluded: &[&str]) -> Result<(), NativeWatchError> {
        if self.recursive {
            return Ok(());
        }
        let mut pending = vec![self.root.clone()];
        while let Some(directory) = pending.pop() {
            let entries = match std::fs::read_dir(&directory) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(NativeWatchError::Io(error.to_string())),
            };
            for entry in entries {
                let entry = entry.map_err(|error| NativeWatchError::Io(error.to_string()))?;
                let is_directory = entry
                    .file_type()
                    .is_ok_and(|file_type| file_type.is_dir() && !file_type.is_symlink());
                if !is_directory
                    || (directory == self.root
                        && excluded.iter().any(|name| entry.file_name() == *name))
                {
                    continue;
                }
                let path = entry.path();
                if !self.watched_directories.contains(&path) {
                    match self.watcher.watch(&path, RecursiveMode::NonRecursive) {
                        Ok(()) => {
                            let at = self.watched_directories.partition_point(|candidate| candidate < &path);
                            self.watched_directories.insert(at, path.clone());
                        }
                        Err(_) if !path.is_dir() => continue,
                        Err(error) => return Err(NativeWatchError::Backend(error.to_string())),
                    }
                }
                pending.push(path);
            }
        }
        Ok(())
    }

    fn forget_removed_directories(&mut self, changes: &[WatchChange]) {
        if self.recursive {
            return;
        }
        let mut removed = Vec::new();
        for change in changes {
            match change {
                WatchChange::Removed(path) => removed.push(path),
                WatchChange::Renamed { from, to } => {
                    removed.push(from);
                    removed.push(to);
                }
                WatchChange::Created(_)
                | WatchChange::Modified(_)
                | WatchChange::Arrived(_)
                | WatchChange::MetadataChanged(_) => {}
            }
        }
        for path in removed {
            let Ok(relative) = crate::native_capture::namespace_to_host_path(path) else {
                continue;
            };
            let removed = self.root.join(relative);
            let stale = self
                .watched_directories
                .iter()
                .filter(|watched| **watched != self.root && watched.starts_with(&removed))
                .cloned()
                .collect::<Vec<_>>();
            for watched in stale {
                let _ = self.watcher.unwatch(&watched);
                self.watched_directories.retain(|candidate| candidate != &watched);
            }
        }
    }


    /// Admits a demand-backed lazy baseline without enumerating the root.
    ///
    /// The paired demand source must hold the same root capability and resolve
    /// first observations from the live root. Hints arriving after this
    /// boundary remain queued and must be captured before the corresponding
    /// operation barrier closes.
    ///
    /// # Errors
    ///
    /// Returns the same admission and root-identity failures as the ordinary
    /// rescan handshake.
    pub fn accept_lazy_baseline(&mut self) -> Result<WatchBatch, NativeWatchError> {
        self.accept_lazy_baseline_measured(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    }

    /// Admits the same lazy baseline with cumulative queue/cleanup/root work.
    pub fn accept_lazy_baseline_measured(
        &mut self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<WatchBatch>, NativeWatchError> {
        let begun = self.begin_rescan_measured(budget, cancellation)?;
        let remaining = begun.work.remaining(budget)
            .map_err(|error| OperationFailure::new(NativeWatchError::Work(error), begun.work))?;
        let finished = self.finish_rescan_measured(remaining, cancellation)
            .map_err(|failure| failure.map_with_prior_work(begun.work, |error| error))?;
        let work = begun.work.checked_add(finished.work)
            .map_err(|error| OperationFailure::new(NativeWatchError::Work(error), begun.work))?;
        Ok(OperationReceipt { value: finished.value, work })
    }

    /// Returns the exact root identity admitted when the watcher opened.
    #[must_use]
    pub const fn root_identity(&self) -> NativeRootIdentity {
        self.root_identity
    }

    /// Verifies that a separately held capture root is the directory admitted
    /// by this watcher. A mismatch permanently invalidates the current epoch.
    ///
    /// # Errors
    ///
    /// Returns a typed root-change or synchronization failure.
    pub fn verify_root_identity(
        &self,
        observed: NativeRootIdentity,
    ) -> Result<(), NativeWatchError> {
        if observed == self.root_identity {
            return Ok(());
        }
        self.shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?
            .invalidation = Some(WatchInvalidationReason::RootChanged);
        Err(NativeWatchError::RootChanged)
    }

    /// Starts a baseline scan and atomically discards only pre-scan hints.
    ///
    /// Native events arriving after this method returns remain queued. The
    /// caller must read the exact host tree before calling [`Self::finish_rescan`].
    ///
    /// # Errors
    ///
    /// Rejects a duplicate scan, epoch exhaustion, or poisoned watcher state.
    pub fn begin_rescan(&mut self) -> Result<WatchEpoch, NativeWatchError> {
        self.begin_rescan_measured(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    }

    /// Starts the original scan under explicit queue-drain and unwatch bounds.
    /// Failed cleanup leaves the scan in progress for explicit abort/recovery.
    pub fn begin_rescan_measured(
        &mut self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<WatchEpoch>, NativeWatchError> {
        let mut work = WorkCounters::default();
        let result = (|| {
            cancellation.check()?;
            if self.rescan_in_progress {
                return Err(NativeWatchError::RescanAlreadyInProgress);
            }
            let mut shared = self.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
            let queued = self.queued.load(Ordering::Acquire);
            for _ in 0..queued {
                admit_watch_work(&mut work, WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::default()
                }, budget, cancellation)?;
                self.receiver.try_recv().map_err(|_| NativeWatchError::StatePoisoned)?;
                self.queued.fetch_sub(1, Ordering::AcqRel);
            }
            shared.invalidation = None;
            #[cfg(target_os = "linux")]
            {
                shared.pending_rename = None;
            }
            self.epoch = WatchEpoch(self.epoch.0.checked_add(1).ok_or(NativeWatchError::SequenceExhausted)?);
            self.next_sequence = WatchSequence(0);
            drop(shared);
            self.rescan_in_progress = true;
            #[cfg(windows)]
            {
                let remaining = work.remaining(budget)?;
                let mut directory_work = crate::demand::DemandWork::new(remaining, cancellation);
                let cleanup = self.clear_windows_directories_measured(&mut directory_work);
                work = work.checked_add(directory_work.work)?;
                cleanup.map_err(|error| match error {
                    crate::demand::DemandError::Observation(error) => error,
                    crate::demand::DemandError::Work(error) => NativeWatchError::Work(error),
                    crate::demand::DemandError::Cancelled => NativeWatchError::Cancelled(CancellationError),
                    crate::demand::DemandError::Io(error) => error.into(),
                    error => NativeWatchError::Io(error.to_string()),
                })?;
            }
            #[cfg(target_os = "linux")]
            if !self.recursive {
                let mut directory_work = crate::demand::DemandWork::new(work.remaining(budget)?, cancellation);
                let cleanup = self.clear_linux_directories_measured(&mut directory_work);
                work = work.checked_add(directory_work.work)?;
                cleanup.map_err(|error| match error {
                    crate::demand::DemandError::Observation(error) => error,
                    crate::demand::DemandError::Work(error) => NativeWatchError::Work(error),
                    crate::demand::DemandError::Cancelled => NativeWatchError::Cancelled(CancellationError),
                    crate::demand::DemandError::Io(error) => error.into(),
                    error => NativeWatchError::Io(error.to_string()),
                })?;
            }
            #[cfg(not(target_os = "linux"))]
            if !self.recursive {
                while let Some(path) = self.watched_directories.iter().find(|path| **path != self.root) {
                    let bytes = u64::try_from(path.as_os_str().len()).map_err(|_| NativeWatchError::InvalidOptions)?;
                    admit_watch_work(&mut work, WorkCounters {
                        allocation_operations: 1,
                        bytes_copied: bytes,
                        peak_allocation_bytes: bytes,
                        backend_write_operations: 1,
                        ..WorkCounters::default()
                    }, budget, cancellation)?;
                    let path = path.clone();
                    self.watcher.unwatch(&path).map_err(|error| NativeWatchError::Backend(error.to_string()))?;
                    self.watched_directories.retain(|candidate| candidate != &path);
                }
            }
            Ok(self.epoch)
        })();
        result
            .map(|value| OperationReceipt { value, work })
            .map_err(|error| OperationFailure::new(error, work))
    }

    #[cfg(target_os = "windows")]
    fn accept_verified_baseline<T, E>(
        &mut self,
        verify: impl FnOnce() -> Result<Option<T>, E>,
    ) -> Result<Result<Option<T>, E>, NativeWatchError> {
        if self.rescan_in_progress {
            return Err(NativeWatchError::RescanAlreadyInProgress);
        }
        let mut shared = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        if shared.invalidation != Some(WatchInvalidationReason::InitialSnapshotRequired)
            || self.epoch.0 != 0
        {
            return Err(NativeWatchError::RescanAlreadyInProgress);
        }
        let verified = match verify() {
            Ok(Some(verified)) => verified,
            Ok(None) => return Ok(Ok(None)),
            Err(error) => return Ok(Err(error)),
        };
        // A callback that ran before admission could only observe the initial
        // invalidation and would have dropped its hint. The durable verifier
        // must cover that interval. Holding the lock now fences later callbacks.
        self.epoch = WatchEpoch(
            self.epoch
                .0
                .checked_add(1)
                .ok_or(NativeWatchError::SequenceExhausted)?,
        );
        self.next_sequence = WatchSequence(0);
        shared.invalidation = None;
        Ok(Ok(Some(verified)))
    }

    /// Admits an existing Windows baseline only when the volume USN journal
    /// proves that neither the watched root nor any entry on its volume
    /// changed since the checkpoint.
    ///
    /// Validation runs while callback delivery is fenced by the watcher state
    /// lock. Any discontinuity leaves the watcher invalidated and returns the
    /// exact baseline requirement.
    #[cfg(target_os = "windows")]
    pub fn accept_windows_usn_baseline(
        &mut self,
        checkpoint: crate::WindowsUsnCheckpoint,
    ) -> Result<Result<crate::WindowsUsnContinuity, crate::WindowsUsnError>, NativeWatchError> {
        let root = self.root.clone();
        self.accept_verified_baseline(|| {
            crate::validate_windows_usn_checkpoint(&root, checkpoint).map(|continuity| {
                (continuity == crate::WindowsUsnContinuity::Unchanged).then_some(continuity)
            })
        })
        .map(|result| {
            result.map(|admitted| {
                admitted.unwrap_or(crate::WindowsUsnContinuity::BaselineRequired(
                    crate::WindowsUsnDiscontinuity::VolumeAdvanced,
                ))
            })
        })
    }

    /// Seals the caller's baseline scan.
    ///
    /// If events overflowed during scanning, the returned batch remains
    /// invalidated and the scan must be repeated. Otherwise all concurrently
    /// queued hints form the first contiguous interval after the baseline.
    ///
    /// # Errors
    ///
    /// Rejects completion without a matching scan or poisoned state.
    pub fn finish_rescan(&mut self) -> Result<WatchBatch, NativeWatchError> {
        self.finish_rescan_measured(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    }

    /// Verifies the original physical root within the admitted scan budget.
    pub fn finish_rescan_measured(
        &mut self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<WatchBatch>, NativeWatchError> {
        let span = crate::obs::span!(
            DEBUG,
            "acyclic.fs.watch.finish_rescan",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
        );
        let mut work = WorkCounters::default();
        let result = crate::obs::scope(&span, || {
            if !self.rescan_in_progress {
                return Err(NativeWatchError::NoRescanInProgress);
            }
            let read = WorkCounters { backend_read_operations: 1, ..WorkCounters::default() };
            admit_watch_work(&mut work, read, budget, cancellation)?;
            let observed = match open_native_root(&self.root) {
                Ok(root) => {
                    admit_watch_work(&mut work, read, budget, cancellation)?;
                    NativeRootIdentity::from_file(&root).ok()
                }
                Err(_) => None,
            };
            cancellation.check()?;
            if observed != Some(self.root_identity) {
                self.shared
                    .lock()
                    .map_err(|_| NativeWatchError::StatePoisoned)?
                    .invalidation = Some(WatchInvalidationReason::RootChanged);
            }
            self.rescan_in_progress = false;
            let mut shared = self
                .shared
                .lock()
                .map_err(|_| NativeWatchError::StatePoisoned)?;
            Ok(shared.seal_invalidation().map_or(
                WatchBatch::Changes {
                    epoch: self.epoch,
                    first_sequence: self.next_sequence,
                    next_sequence: self.next_sequence,
                    changes: Vec::new(),
                },
                |reason| WatchBatch::RescanRequired {
                    epoch: self.epoch,
                    reason,
                },
            ))
        });
        crate::obs::measured_on(
            &span,
            result
                .map(|value| OperationReceipt { value, work })
                .map_err(|error| OperationFailure::new(error, work)),
            |receipt| &receipt.work,
        )
    }

    /// Aborts an in-progress baseline and returns the watcher to an explicitly
    /// invalidated, retryable state.
    ///
    /// Events that arrived during the failed scan remain bounded in the queue;
    /// the next [`Self::begin_rescan`] discards them before establishing a new
    /// baseline interval.
    ///
    /// # Errors
    ///
    /// Rejects an abort without a matching scan or poisoned watcher state.
    pub fn abort_rescan(
        &mut self,
        reason: WatchInvalidationReason,
    ) -> Result<(), NativeWatchError> {
        if !self.rescan_in_progress {
            return Err(NativeWatchError::NoRescanInProgress);
        }
        self.rescan_in_progress = false;
        self.shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?
            .invalidation = Some(reason);
        Ok(())
    }

    /// Stops reporting hints for events wholly inside `subtrees`.
    ///
    /// Excluded subtrees are invisible to the consumer (for example a VCS
    /// directory or reserved SDK state), so their churn never invalidates it.
    /// Their order is the [`Self::fence`] placement preference.
    ///
    /// # Errors
    ///
    /// Rejects the root, unrepresentable paths, or poisoned synchronization.
    pub fn exclude_subtrees(&mut self, subtrees: &[NamespacePath]) -> Result<(), NativeWatchError> {
        let excluded = subtrees
            .iter()
            .map(|subtree| {
                if subtree.is_root() {
                    return Err(NativeWatchError::UnrepresentablePath);
                }
                crate::native_capture::namespace_to_host_path(subtree)
                    .map(|relative| self.root.join(relative))
                    .map_err(|_| NativeWatchError::UnrepresentablePath)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?
            .excluded
            .clone_from(&excluded);
        self.excluded = subtrees.iter().cloned().zip(excluded).collect();
        Ok(())
    }

    /// Waits until every host change completed before this call is queued.
    ///
    /// Native delivery is asynchronous (`FSEvents` most visibly), so a poll
    /// alone can miss a write that finished before it. The fence creates and
    /// removes a uniquely named cookie inside the first excluded subtree that
    /// is a directory (creating the first one when none is), then waits for
    /// the cookie's ordered notification. Excluded cookies are never hints.
    ///
    /// A fence either proves delivery or invalidates the watcher: when the
    /// cookie cannot be placed, written, or observed within `timeout`, the
    /// next poll demands an authenticated rescan instead.
    ///
    /// # Errors
    ///
    /// Returns only poisoned synchronization.
    pub fn fence(&mut self, timeout: Duration) -> Result<(), NativeWatchError> {
        let span = crate::obs::span!(
            DEBUG,
            "acyclic.fs.watch.fence",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
        );
        let result = crate::obs::scope(&span, || {
            #[cfg(windows)]
            let timeout = if self.windows_directories.is_empty() {
                timeout
            } else {
                let started = Instant::now();
                for observed in &self.windows_directories {
                    observed.watcher.fence_for(timeout.saturating_sub(started.elapsed()))?;
                }
                timeout.saturating_sub(started.elapsed())
            };
            let delivered = self.fence_cookie().is_ok_and(|cookie| {
                self.await_cookie(&cookie, timeout)
                    .is_ok_and(|delivered| delivered)
            });
            let mut state = self
                .shared
                .lock()
                .map_err(|_| NativeWatchError::StatePoisoned)?;
            state.fence = None;
            if !delivered && state.invalidation.is_none() {
                state.invalidation = Some(WatchInvalidationReason::NativeRescanRequired);
            }
            Ok(())
        });
        crate::obs::outcome_on(&span, result)
    }

    fn fence_cookie(&mut self) -> Result<PathBuf, NativeWatchError> {
        let (directory, host_directory) = self.fence_directory()?;
        self.watch_directory(&directory)?;
        self.fences = self.fences.wrapping_add(1);
        Ok(host_directory.join(format!(
            ".acyclic-fence-{}-{}",
            std::process::id(),
            self.fences
        )))
    }

    /// Writes `cookie` and reports whether its notification arrived in time.
    fn await_cookie(&self, cookie: &Path, timeout: Duration) -> Result<bool, NativeWatchError> {
        let mut state = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        if state.invalidation.is_some() {
            return Ok(false);
        }
        state.fence = Some(cookie.to_path_buf());
        drop(state);
        std::fs::File::create_new(cookie)
            .and_then(|_| std::fs::remove_file(cookie))
            .map_err(|error| NativeWatchError::Io(error.to_string()))?;
        let deadline = Instant::now() + timeout;
        let mut state = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        while state.fence.is_some() && state.invalidation.is_none() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(false);
            }
            state = self
                .fenced
                .wait_timeout(state, remaining)
                .map_err(|_| NativeWatchError::StatePoisoned)?
                .0;
        }
        Ok(state.invalidation.is_none())
    }

    fn fence_directory(&self) -> Result<(NamespacePath, PathBuf), NativeWatchError> {
        for (subtree, host) in &self.excluded {
            match host.symlink_metadata() {
                Ok(metadata) if metadata.is_dir() => return Ok((subtree.clone(), host.clone())),
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(NativeWatchError::Io(error.to_string())),
            }
        }
        let (subtree, host) = self
            .excluded
            .first()
            .cloned()
            .ok_or(NativeWatchError::InvalidOptions)?;
        std::fs::create_dir(&host).map_err(|error| NativeWatchError::Io(error.to_string()))?;
        Ok((subtree, host))
    }

    /// Moves at most `maximum_changes` contiguous hints from the native queue.
    ///
    /// # Errors
    ///
    /// Fails before output allocation for zero bounds, cancellation, an active
    /// rescan, poisoned state, sequence overflow, allocation failure, or an
    /// exceeded exact work budget.
    pub fn poll(
        &mut self,
        maximum_changes: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<WatchBatch>, NativeWatchError> {
        cancellation
            .check()
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        if maximum_changes == 0 {
            return Err(OperationFailure::before_work(
                NativeWatchError::ZeroPollLimit,
            ));
        }
        if self.rescan_in_progress {
            return Err(OperationFailure::before_work(
                NativeWatchError::RescanInProgress,
            ));
        }
        if let Some(reason) = self.invalidation()? {
            return Ok(OperationReceipt {
                value: WatchBatch::RescanRequired {
                    epoch: self.epoch,
                    reason,
                },
                work: WorkCounters::default(),
            });
        }
        let root_work = WorkCounters {
            backend_read_operations: 1,
            ..WorkCounters::default()
        };
        root_work
            .verify(budget)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        self.verify_current_root()?;
        if let Some(reason) = self.invalidation()? {
            return Ok(OperationReceipt {
                value: WatchBatch::RescanRequired {
                    epoch: self.epoch,
                    reason,
                },
                work: root_work,
            });
        }
        self.poll_contiguous(maximum_changes, budget, root_work)
    }

    fn poll_contiguous(
        &mut self,
        maximum_changes: u32,
        budget: WorkBudget,
        initial_work: WorkCounters,
    ) -> MeasuredResult<OperationReceipt<WatchBatch>, NativeWatchError> {
        let queued = self.queued.load(Ordering::Acquire).min(maximum_changes);
        if queued == 0 {
            return Ok(OperationReceipt {
                value: WatchBatch::Changes {
                    epoch: self.epoch,
                    first_sequence: self.next_sequence,
                    next_sequence: self.next_sequence,
                    changes: Vec::new(),
                },
                work: initial_work,
            });
        }
        let capacity = usize::try_from(queued)
            .map_err(|_| OperationFailure::before_work(NativeWatchError::InvalidOptions))?;
        let allocation_bytes = capacity
            .checked_mul(size_of::<WatchChange>())
            .map(crate::foundation::usize_to_u64)
            .ok_or_else(|| {
                OperationFailure::before_work(NativeWatchError::Work(WorkError::Overflow))
            })?;
        let mut work = initial_work
            .checked_add(WorkCounters {
                allocation_operations: 1,
                peak_allocation_bytes: allocation_bytes,
                ..WorkCounters::default()
            })
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        work.verify(budget)
            .map_err(|error| OperationFailure::before_work(error.into()))?;
        let mut changes = Vec::new();
        changes
            .try_reserve_exact(capacity)
            .map_err(|_| OperationFailure::new(NativeWatchError::AllocationFailed, work))?;
        while changes.len() < capacity {
            match self.receiver.try_recv() {
                Ok(change) => {
                    self.queued.fetch_sub(1, Ordering::AcqRel);
                    changes.push(change);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.invalidate(WatchInvalidationReason::BackendError)?;
                    break;
                }
            }
        }
        if let Some(reason) = self.invalidation()? {
            return Ok(OperationReceipt {
                value: WatchBatch::RescanRequired {
                    epoch: self.epoch,
                    reason,
                },
                work,
            });
        }
        let count = crate::foundation::usize_to_u64(changes.len());
        let next = self
            .next_sequence
            .0
            .checked_add(count)
            .ok_or_else(|| OperationFailure::new(NativeWatchError::SequenceExhausted, work))?;
        work = work
            .checked_add(WorkCounters {
                items_examined: count,
                items_returned: count,
                ..WorkCounters::default()
            })
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        work.verify(budget)
            .map_err(|error| OperationFailure::new(error.into(), work))?;
        let first_sequence = self.next_sequence;
        self.next_sequence = WatchSequence(next);
        self.forget_removed_directories(&changes);
        Ok(OperationReceipt {
            value: WatchBatch::Changes {
                epoch: self.epoch,
                first_sequence,
                next_sequence: self.next_sequence,
                changes,
            },
            work,
        })
    }

    fn verify_current_root(&self) -> Result<(), OperationFailure<NativeWatchError>> {
        let observed = open_native_root(&self.root)
            .and_then(|root| NativeRootIdentity::from_file(&root))
            .ok();
        if observed == Some(self.root_identity) {
            return Ok(());
        }
        self.shared
            .lock()
            .map_err(|_| OperationFailure::before_work(NativeWatchError::StatePoisoned))?
            .invalidation = Some(WatchInvalidationReason::RootChanged);
        Ok(())
    }

    fn invalidation(
        &self,
    ) -> Result<Option<WatchInvalidationReason>, OperationFailure<NativeWatchError>> {
        self.shared
            .lock()
            .map(|mut state| state.seal_invalidation())
            .map_err(|_| OperationFailure::before_work(NativeWatchError::StatePoisoned))
    }

    fn invalidate(
        &self,
        reason: WatchInvalidationReason,
    ) -> Result<(), OperationFailure<NativeWatchError>> {
        let mut shared = self
            .shared
            .lock()
            .map_err(|_| OperationFailure::before_work(NativeWatchError::StatePoisoned))?;
        shared.invalidation.get_or_insert(reason);
        Ok(())
    }
}

fn admit_watch_work(
    work: &mut WorkCounters,
    proposed: WorkCounters,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), NativeWatchError> {
    cancellation.check()?;
    let next = work.checked_add(proposed)?;
    next.verify(budget)?;
    *work = next;
    Ok(())
}

impl crate::demand::DemandDirectoryObserver for Mutex<NativeWatch> {
    fn observe_directory(
        &self,
        directory: &NamespacePath,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::demand::DemandResult<()> {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        let result = (|| {
            work.check()?;
            self.lock()
                .map_err(|_| NativeWatchError::StatePoisoned)?
                .watch_directory_measured(directory, &mut work)
        })();
        result
            .map(|value| OperationReceipt { value, work: work.work })
            .map_err(|error| OperationFailure::new(error, work.work))
    }
}

#[cfg(target_os = "linux")]
/// Grows one owned path only after its logical capacity and copied bytes have
/// been admitted. Temporary name buffers are accounted separately.
fn grow_watch_path_measured(
    candidate: &mut PathBuf,
    part: &Path,
    work: &mut crate::demand::DemandWork,
    owned_bytes: &mut u64,
) -> Result<(), crate::demand::DemandError> {
    let last = candidate.as_os_str().as_encoded_bytes().last().copied();
    #[cfg(unix)]
    let trailing_separator = last == Some(b'/');
    let separator = usize::from(!candidate.as_os_str().is_empty() && !trailing_separator);
    let capacity = candidate.as_os_str().len()
        .checked_add(part.as_os_str().len())
        .and_then(|bytes| bytes.checked_add(separator)).ok_or(WorkError::Overflow)?;
    let bytes = capacity as u64;
    let copied = WorkCounters { bytes_copied: bytes, ..WorkCounters::default() };
    work.work.admit(&copied, &work.budget)?;
    work.allocate(bytes)?;
    work.record(copied)?;
    let mut grown = PathBuf::with_capacity(capacity);
    grown.push(candidate.as_path());
    grown.push(part);
    drop(std::mem::replace(candidate, grown));
    work.release(*owned_bytes)?;
    *owned_bytes = bytes;
    Ok(())
}

#[cfg(target_os = "linux")]
/// The same exact spelling conversion as native capture, with each temporary
/// owned buffer admitted rather than hiding conversion behind a watch charge.
fn watch_host_name_measured(
    component: &LogicalName,
    work: &mut crate::demand::DemandWork,
) -> Result<(std::ffi::OsString, u64), crate::demand::DemandError> {
    use crate::kernel::NameEncoding;
    use std::ffi::OsString;
    let source = component.as_bytes();
    match component.encoding() {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStringExt as _;
                let bytes = u64::try_from(source.len()).map_err(|_| WorkError::Overflow)?;
                work.work.admit(&WorkCounters { bytes_copied: bytes, ..WorkCounters::default() }, &work.budget)?;
                work.allocate(bytes)?;
                work.record(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() })?;
                Ok((OsString::from_vec(source.to_vec()), bytes))
            }
        }
        NameEncoding::WindowsUtf16Le => {
            let units = crate::kernel::types::utf16le_units(source)
                .ok_or(NativeWatchError::UnrepresentablePath)?;
            let mut bytes = 0_usize;
            let mut decoded_items = 0_u64;
            for decoded in char::decode_utf16(units.clone()) {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                let decoded = decoded.map_err(|_| NativeWatchError::UnrepresentablePath)?;
                bytes = bytes.checked_add(decoded.len_utf8()).ok_or(WorkError::Overflow)?;
                decoded_items = decoded_items.checked_add(1).ok_or(WorkError::Overflow)?;
            }
            let logical = bytes as u64;
            let copied = WorkCounters {
                bytes_copied: logical,
                items_examined: decoded_items,
                ..WorkCounters::default()
            };
            work.work.admit(&copied, &work.budget)?;
            work.allocate(logical)?;
            work.record(copied)?;
            let mut name = String::with_capacity(bytes);
            for decoded in char::decode_utf16(units) {
                name.push(decoded.map_err(|_| NativeWatchError::UnrepresentablePath)?);
            }
            Ok((OsString::from(name), logical))
        }
    }
}

fn accept_native_event(
    event: &notify::Result<Event>,
    context: &NativeEventContext,
    sender: &SyncSender<WatchChange>,
    shared: &Arc<Mutex<SharedState>>,
    queued: &Arc<AtomicU32>,
) {
    let Ok(mut state) = shared.lock() else {
        return;
    };
    if state.invalidation.is_some() {
        return;
    }
    if let Ok(event) = event
        && !event.paths.is_empty()
        && event.paths.iter().all(|path| {
            state
                .excluded
                .iter()
                .any(|excluded| path.starts_with(excluded))
        })
    {
        return;
    }
    let mapped = event
        .as_ref()
        .map_err(|_| WatchInvalidationReason::BackendError)
        .and_then(|event| map_native_event(event, context));
    let changes = match mapped {
        Ok(MappedNativeEvent::Changes(changes)) => changes,
        #[cfg(target_os = "linux")]
        Ok(MappedNativeEvent::RenameHalf {
            tracker,
            mode,
            path,
        }) => {
            match mode {
                RenameMode::From if state.pending_rename.is_none() => {
                    if queued.load(Ordering::Acquire) >= context.maximum_queued_changes {
                        state.invalidation = Some(WatchInvalidationReason::QueueOverflow);
                        return;
                    }
                    state.pending_rename = Some(PendingRename {
                        tracker,
                        from: path,
                        to: None,
                    });
                }
                RenameMode::To => {
                    let Some(pending) = state.pending_rename.as_mut() else {
                        state.invalidation = Some(WatchInvalidationReason::AmbiguousRename);
                        return;
                    };
                    if pending.tracker != tracker || pending.to.is_some() {
                        state.invalidation = Some(WatchInvalidationReason::AmbiguousRename);
                    } else {
                        pending.to = Some(path);
                    }
                }
                _ => state.invalidation = Some(WatchInvalidationReason::AmbiguousRename),
            }
            return;
        }
        #[cfg(target_os = "linux")]
        Ok(MappedNativeEvent::RenameBoth {
            tracker,
            changes,
        }) => {
            let matched = state.pending_rename.as_ref().is_some_and(|pending| {
                let Some(WatchChange::Renamed { from, to }) = changes.first() else { return false; };
                pending.tracker == tracker
                    && &pending.from == from
                    && pending.to.as_ref() == Some(to)
            });
            if !matched {
                state.invalidation = Some(WatchInvalidationReason::AmbiguousRename);
                return;
            }
            state.pending_rename = None;
            changes
        }
        Err(reason) => {
            state.invalidation = Some(reason);
            return;
        }
    };
    for change in changes {
        #[cfg(target_os = "linux")]
        if queued
            .load(Ordering::Acquire)
            .saturating_add(u32::from(state.pending_rename.is_some()))
            >= context.maximum_queued_changes
        {
            state.invalidation = Some(WatchInvalidationReason::QueueOverflow);
            break;
        }
        match sender.try_send(change) {
            Ok(()) => {
                queued.fetch_add(1, Ordering::Release);
            }
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                state.invalidation = Some(WatchInvalidationReason::QueueOverflow);
                break;
            }
        }
    }
}

enum MappedNativeEvent {
    Changes(Vec<WatchChange>),
    #[cfg(target_os = "linux")]
    RenameHalf {
        tracker: usize,
        mode: RenameMode,
        path: NamespacePath,
    },
    #[cfg(target_os = "linux")]
    RenameBoth {
        tracker: usize,
        changes: Vec<WatchChange>,
    },
}

fn map_native_event(
    event: &Event,
    context: &NativeEventContext,
) -> Result<MappedNativeEvent, WatchInvalidationReason> {
    validate_replayed_root_creation(event, &context.root, context.root_identity)?;
    #[cfg(target_os = "linux")]
    if let EventKind::Modify(ModifyKind::Name(mode)) = event.kind {
        if event.need_rescan() {
            return Err(WatchInvalidationReason::NativeRescanRequired);
        }
        let tracker = event
            .attrs
            .tracker()
            .ok_or(WatchInvalidationReason::AmbiguousRename)?;
        if matches!(mode, RenameMode::From | RenameMode::To) {
            let [path] = event.paths.as_slice() else {
                return Err(WatchInvalidationReason::AmbiguousRename);
            };
            let relative =
                relative_namespace_path(&context.root, path, context.profile, context.limits)?;
            if relative.is_root() {
                return Err(WatchInvalidationReason::RootChanged);
            }
            return Ok(MappedNativeEvent::RenameHalf {
                tracker,
                mode,
                path: relative,
            });
        }
        if mode == RenameMode::Both {
            let changes = map_event(event, &context.root, context.profile, context.limits)?;
            if !matches!(changes.as_slice(), [WatchChange::Renamed { .. }]) {
                return Err(WatchInvalidationReason::AmbiguousRename);
            }
            return Ok(MappedNativeEvent::RenameBoth { tracker, changes });
        }
    }
    map_event(event, &context.root, context.profile, context.limits).map(MappedNativeEvent::Changes)
}

fn validate_replayed_root_creation(
    event: &Event,
    root: &Path,
    admitted_root_identity: NativeRootIdentity,
) -> Result<(), WatchInvalidationReason> {
    #[cfg(target_os = "macos")]
    if event.paths.iter().any(|path| path == root) && matches!(event.kind, EventKind::Create(_)) {
        let observed = open_native_root(root)
            .and_then(|file| NativeRootIdentity::from_file(&file))
            .ok();
        if observed != Some(admitted_root_identity) {
            return Err(WatchInvalidationReason::RootChanged);
        }
    }
    #[cfg(not(target_os = "macos"))]
    let _ = (event, root, admitted_root_identity);
    Ok(())
}

fn map_event(
    event: &Event,
    root: &Path,
    profile: FilesystemProfile,
    limits: VolumeLimits,
) -> Result<Vec<WatchChange>, WatchInvalidationReason> {
    if event.need_rescan() {
        return Err(WatchInvalidationReason::NativeRescanRequired);
    }
    if matches!(event.kind, EventKind::Access(_)) {
        return Ok(Vec::new());
    }
    if event.paths.is_empty() || event.paths.len() > 2 {
        return Err(WatchInvalidationReason::UnrepresentablePath);
    }
    let mut paths = Vec::new();
    paths
        .try_reserve_exact(event.paths.len())
        .map_err(|_| WatchInvalidationReason::UnrepresentablePath)?;
    for path in &event.paths {
        // The watched root necessarily predates watcher admission. FSEvents
        // may replay a creation hint for it after the baseline handshake;
        // that hint cannot describe a new namespace entry and has no capture
        // target. Root removal and rename remain fail-closed below.
        #[cfg(target_os = "macos")]
        if path == root && matches!(event.kind, EventKind::Create(_)) {
            continue;
        }
        paths.push(relative_namespace_path(root, path, profile, limits)?);
    }
    if paths.iter().any(NamespacePath::is_root)
        && matches!(
            event.kind,
            EventKind::Remove(_) | EventKind::Modify(ModifyKind::Name(_))
        )
    {
        return Err(WatchInvalidationReason::RootChanged);
    }
    // FSEvents may report a coalesced change against the watched directory
    // itself without identifying the affected descendant. The volume root is
    // not a mutable namespace entry, so require a bounded baseline instead of
    // inventing an invalid root mutation or silently losing the change.
    #[cfg(target_os = "macos")]
    if paths.iter().any(NamespacePath::is_root) {
        return Err(WatchInvalidationReason::NativeRescanRequired);
    }
    if let EventKind::Modify(ModifyKind::Name(mode)) = event.kind {
        return match (mode, paths.as_slice()) {
            (RenameMode::Both, [from, to]) => Ok(vec![WatchChange::Renamed {
                from: from.clone(),
                to: to.clone(),
            }]),
            // inotify pairs both halves by cookie before they reach here
            // (see `map_native_event`), so a lone half on Linux means the
            // other half was lost.
            #[cfg(target_os = "linux")]
            _ => Err(WatchInvalidationReason::AmbiguousRename),
            // `ReadDirectoryChangesW` delivers each half as its own event and
            // says which it is: a new name arrived, subtree and all, or an old
            // one went away. Invalidating instead made every atomic save,
            // `mv`, and `git checkout` cost a full rescan.
            #[cfg(windows)]
            (RenameMode::To, [to]) => Ok(vec![WatchChange::Arrived(to.clone())]),
            #[cfg(windows)]
            (RenameMode::From, [from]) => Ok(vec![WatchChange::Removed(from.clone())]),
            // FSEvents never pairs renames nor says which end a path was, so
            // a lone half proves only that the path changed: look at it. The
            // capture resolves a modified hint by stat, treating a vanished
            // path as removed (subtree and all), so both ends of a rename
            // land exactly without guessing which was which.
            #[cfg(not(target_os = "linux"))]
            (_, paths) => Ok(paths.iter().cloned().map(WatchChange::Modified).collect()),
        };
    }
    let mut changes = Vec::new();
    changes
        .try_reserve_exact(paths.len())
        .map_err(|_| WatchInvalidationReason::UnrepresentablePath)?;
    for path in paths {
        changes.push(match event.kind {
            // FSEvents coalesces flags and can replay creation hints for
            // paths that predate the caller's baseline, so a Darwin creation
            // event only proves the path changed. The created hint's
            // identity-replacing capture semantics require the exact
            // notifications inotify and `ReadDirectoryChangesW` deliver.
            #[cfg(target_os = "macos")]
            EventKind::Create(_) => WatchChange::Modified(path),
            #[cfg(not(target_os = "macos"))]
            EventKind::Create(_) => WatchChange::Created(path),
            EventKind::Remove(_) => WatchChange::Removed(path),
            // The same coalescing applies to metadata hints: a Darwin
            // metadata event may stand for a coalesced data write, so it only
            // proves the path changed. Treating it as metadata-only lets a
            // capture skip restaging content that did change.
            #[cfg(target_os = "macos")]
            EventKind::Modify(ModifyKind::Metadata(_)) => WatchChange::Modified(path),
            #[cfg(not(target_os = "macos"))]
            EventKind::Modify(ModifyKind::Metadata(_)) => WatchChange::MetadataChanged(path),
            EventKind::Modify(_) | EventKind::Any | EventKind::Other => WatchChange::Modified(path),
            EventKind::Access(_) => continue,
        });
    }
    Ok(changes)
}

fn relative_namespace_path(
    root: &Path,
    path: &Path,
    profile: FilesystemProfile,
    limits: VolumeLimits,
) -> Result<NamespacePath, WatchInvalidationReason> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| WatchInvalidationReason::UnrepresentablePath)?;
    let mut components = Vec::new();
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(WatchInvalidationReason::UnrepresentablePath);
        };
        let (encoding, bytes) =
            crate::native_name::host_name_bytes(name, profile, limits.maximum_component_bytes)
                .map_err(|_| WatchInvalidationReason::UnrepresentablePath)?;
        let logical = LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
            .map_err(|_| WatchInvalidationReason::UnrepresentablePath)?;
        components.push(logical);
    }
    NamespacePath::new(components, limits)
        .map_err(|_error: NamespacePathError| WatchInvalidationReason::UnrepresentablePath)
}

/// Native watcher failures that are not recoverable through a baseline rescan.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum NativeWatchError {
    /// Queue or volume bounds are invalid.
    #[error("native watcher options are invalid")]
    InvalidOptions,
    /// The compiled nonrecursive adapter has no actual bounded admission API.
    #[error("bounded native directory observation is unavailable")]
    BoundedObservationUnavailable,
    /// Watch root is not a directory.
    #[error("native watcher root is not a directory")]
    RootIsNotDirectory,
    /// A demanded directory traverses a symbolic link and cannot be mapped
    /// back to one exact namespace path.
    #[error("native watcher directory traverses a symbolic link")]
    SymbolicLinkDirectory,
    /// A demanded observer path is not a directory.
    #[error("native watcher observed path is not a directory")]
    ObservedPathNotDirectory,
    /// A demanded namespace path cannot be represented exactly on this host.
    #[error("native watcher directory is not representable on this host")]
    UnrepresentablePath,
    /// A separately opened capture root does not match the watched directory.
    #[error("native watcher root changed identity")]
    RootChanged,
    /// Root path I/O failed.
    #[error("native watcher I/O failed: {0}")]
    Io(String),
    /// Native backend setup failed.
    #[error("native watcher backend failed: {0}")]
    Backend(String),
    /// Watcher state synchronization was poisoned.
    #[error("native watcher state is poisoned")]
    StatePoisoned,
    /// A baseline scan is already active.
    #[error("native watcher baseline scan is already active")]
    RescanAlreadyInProgress,
    /// No baseline scan is active.
    #[error("native watcher baseline scan is not active")]
    NoRescanInProgress,
    /// Polling while a baseline is being built is ambiguous.
    #[error("native watcher baseline scan is in progress")]
    RescanInProgress,
    /// Poll output must have a positive bound.
    #[error("native watcher poll limit must be non-zero")]
    ZeroPollLimit,
    /// Process-local cursor arithmetic overflowed.
    #[error("native watcher sequence is exhausted")]
    SequenceExhausted,
    /// Bounded output allocation failed.
    #[error("native watcher output allocation failed")]
    AllocationFailed,
    /// Polling was cooperatively cancelled.
    #[error(transparent)]
    Cancelled(#[from] CancellationError),
    /// Exact work overflowed or exceeded the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

impl From<std::io::Error> for NativeWatchError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

#[cfg(test)]
mod demand_admission_tests {
    use super::*;
    use crate::demand::{DemandDirectoryObserver, DemandError};

    fn namespace(path: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
        Ok(NamespacePath::from_portable(
            &crate::path::PortablePath::parse(path, VolumeLimits::default())?,
            VolumeLimits::default(),
        )?)
    }

    fn watcher(root: &Path) -> Result<Mutex<NativeWatch>, NativeWatchError> {
        let mut options = NativeWatchOptions::new(VolumeLimits::default());
        options.recursive = false;
        Ok(Mutex::new(NativeWatch::open_with_profile(
            root, FilesystemProfile::Portable, options,
        )?))
    }

    #[test]
    fn cancelled_observation_does_not_allocate_or_read() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let watch = watcher(root.path())?;
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let failure = watch.observe_directory(&namespace("/a/b")?, WorkBudget::UNBOUNDED, &cancellation)
            .expect_err("cancelled admission cannot inspect any prefix");
        assert!(matches!(failure.error, DemandError::Cancelled));
        assert_eq!(*failure.work, WorkCounters::default());
        assert_eq!(watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?.watched_directories.len(), 1);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn finite_observation_stops_before_the_next_prefix_read() -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir_all(root.path().join("a/b"))?;
        let watch = watcher(root.path())?;
        let path = namespace("/a/b")?;
        let cancellation = CancellationToken::new();
        let failure = watch.observe_directory(&path, WorkBudget {
            backend_read_operations: 1,
            ..WorkBudget::UNBOUNDED
        }, &cancellation).expect_err("the second metadata request must be denied");
        assert!(matches!(failure.error, DemandError::Work(WorkError::BudgetExceeded {
            counter: "backend_read_operations", ..
        })));
        assert_eq!(failure.work.backend_read_operations, 1);
        assert_eq!(failure.work.backend_write_operations, 0);
        assert_eq!(watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?.watched_directories.len(), 1);
        let receipt = watch.observe_directory(&path, WorkBudget {
            backend_read_operations: 6,
            backend_write_operations: 0,
            items_examined: 32,
            ..WorkBudget::UNBOUNDED
        }, &cancellation)?;
        assert_eq!(receipt.work.backend_read_operations, 6);
        assert_eq!(receipt.work.backend_write_operations, 0);
        assert_eq!(receipt.work.source_path_components, 0);
        assert!(receipt.work.items_examined >= 2);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn missing_directory_admits_its_existing_ancestor_with_finite_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        let receipt = watch.observe_directory(&namespace("/a/missing/deeper")?, WorkBudget {
            backend_read_operations: 4,
            backend_write_operations: 0,
            items_examined: 32,
            ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new())?;
        assert_eq!(receipt.work.backend_read_operations, 4);
        assert_eq!(receipt.work.backend_write_operations, 0);
        assert_eq!(receipt.work.source_path_components, 0);
        assert!(receipt.work.items_examined >= 2);
        let held = watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        assert!(held.watched_directories.contains(&held.root.join("a")));
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    fn zero_allocation_allowance_stops_before_any_native_prefix_request()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let watch = watcher(root.path())?;
        let failure = watch.observe_directory(&namespace("/absent")?, WorkBudget {
            allocation_operations: 0,
            ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new()).expect_err("the owned root path must be admitted first");
        assert!(matches!(failure.error, DemandError::Work(_)));
        assert_eq!(failure.work.allocation_operations, 0);
        assert_eq!(failure.work.bytes_copied, 0);
        assert_eq!(failure.work.backend_read_operations, 0);
        assert_eq!(failure.work.backend_write_operations, 0);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn denied_registration_keeps_the_subscription_set_unchanged()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        let path = namespace("/a")?;
        let failure = watch.observe_directory(&path, WorkBudget {
            backend_read_operations: 2,
            ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new()).expect_err("registration must be admitted before the backend call");
        assert!(matches!(failure.error, DemandError::Work(_)));
        assert_eq!(failure.work.backend_read_operations, 2);
        assert_eq!(failure.work.backend_write_operations, 0);
        assert_eq!(watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?.watched_directories.len(), 1);
        let receipt = watch.observe_directory(&path, WorkBudget {
            backend_read_operations: 3,
            backend_write_operations: 0,
            ..WorkBudget::UNBOUNDED
        }, &CancellationToken::new())?;
        assert_eq!(receipt.work.backend_read_operations, 3);
        assert_eq!(receipt.work.backend_write_operations, 0);
        Ok(())
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn nonrecursive_macos_observation_reports_unavailable_without_opaque_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        let failure = watch.observe_directory(&namespace("/a")?, WorkBudget::UNBOUNDED,
            &CancellationToken::new()).expect_err("opaque nonrecursive admission is not bounded");
        assert!(matches!(failure.error,
            DemandError::Observation(NativeWatchError::BoundedObservationUnavailable)));
        assert_eq!(*failure.work, WorkCounters::default());
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_finite_observer_uses_the_native_driver_and_preserves_cookie_paired_hints()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        std::fs::write(root.path().join("a/from"), b"data")?;
        let watch = watcher(root.path())?;
        watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?.accept_lazy_baseline()?;
        watch.observe_directory(&namespace("/a")?, WorkBudget {
            backend_read_operations: 3, backend_write_operations: 0,
            allocation_operations: 32, peak_allocation_bytes: 64 * 1024,
            bytes_copied: 4096, items_examined: 128,
            ..WorkBudget::default()
        }, &CancellationToken::new())?;
        assert!(matches!(&watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?.watcher,
            NativeWatchDriver::Source { .. }));
        std::fs::rename(root.path().join("a/from"), root.path().join("a/to"))?;
        let from = namespace("/a/from")?;
        let to = namespace("/a/to")?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let receipt = watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?
                .poll(16, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
            if matches!(receipt.value, WatchBatch::Changes { ref changes, .. }
                if changes.iter().any(|change| matches!(change,
                    WatchChange::Renamed { from: observed_from, to: observed_to }
                        if observed_from == &from && observed_to == &to)))
            {
                break;
            }
            assert!(Instant::now() < deadline, "native rename cookies were not preserved");
            std::thread::sleep(Duration::from_millis(10));
        }
        let cleanup = watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?
            .begin_rescan_measured(WorkBudget::UNBOUNDED, &CancellationToken::new())?;
        assert_eq!(cleanup.work.backend_write_operations, 1);
        Ok(())
    }

    #[cfg(windows)]
    fn windows_budget() -> WorkBudget {
        WorkBudget {
            backend_read_operations: 64,
            backend_write_operations: 16,
            allocation_operations: 64,
            peak_allocation_bytes: 512 * 1024,
            bytes_copied: 1024 * 1024,
            bytes_encoded: 1024 * 1024,
            items_examined: 512,
            ..WorkBudget::default()
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_observation_registers_real_held_prefixes_with_finite_receipts()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir_all(root.path().join("a/b"))?;
        let watch = watcher(root.path())?;
        let path = namespace("/a/b")?;
        let receipt = watch.observe_directory(&path, windows_budget(), &CancellationToken::new())?;
        assert_eq!(receipt.work.source_path_components, 0);
        assert!(receipt.work.backend_read_operations >= 8);
        assert_eq!(receipt.work.backend_write_operations, 4);
        assert!(receipt.work.peak_allocation_bytes >= 2 * 64 * 1024);
        let held = watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        assert_eq!(held.windows_directories.len(), 2);
        assert_eq!(held.windows_directories[0].path.as_slice(), &[u16::from(b'a')]);
        assert_eq!(held.windows_directories[1].path.as_slice(),
            &[u16::from(b'a'), u16::from(b'\\'), u16::from(b'b')]);
        drop(held);
        let repeated = watch.observe_directory(&path, windows_budget(), &CancellationToken::new())?;
        assert_eq!(repeated.work.backend_write_operations, 0);
        assert!(repeated.work.backend_read_operations > 0, "held identities must be revalidated");
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn windows_short_read_budget_preserves_started_native_requests_without_registering()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        let failure = watch.observe_directory(&namespace("/a")?, WorkBudget {
            backend_read_operations: 1,
            ..windows_budget()
        }, &CancellationToken::new()).expect_err("the next native directory open must be denied");
        assert!(matches!(failure.error, DemandError::Work(WorkError::BudgetExceeded {
            counter: "backend_read_operations", ..
        })));
        assert_eq!(failure.work.backend_read_operations, 1);
        assert_eq!(failure.work.backend_write_operations, 0);
        assert!(watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?.windows_directories.is_empty());
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn windows_later_prefix_denial_retains_the_admitted_ancestor_and_actual_work()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir_all(root.path().join("a/b"))?;
        let watch = watcher(root.path())?;
        let failure = watch.observe_directory(&namespace("/a/b")?, WorkBudget {
            backend_read_operations: 7,
            ..windows_budget()
        }, &CancellationToken::new()).expect_err("the later prefix must stop at the native limit");
        assert!(matches!(failure.error, DemandError::Work(WorkError::BudgetExceeded {
            counter: "backend_read_operations", ..
        })));
        assert_eq!(failure.work.backend_read_operations, 7);
        assert_eq!(failure.work.backend_write_operations, 2);
        let held = watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        assert_eq!(held.windows_directories.len(), 1);
        assert_eq!(held.windows_directories[0].path.as_slice(), &[u16::from(b'a')]);
        drop(held);
        watch.observe_directory(&namespace("/a/b")?, windows_budget(), &CancellationToken::new())?;
        assert_eq!(watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?.windows_directories.len(), 2);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn windows_rebound_prefix_replaces_the_old_held_identity_without_pinning_its_name()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        let path = namespace("/a")?;
        watch.observe_directory(&path, windows_budget(), &CancellationToken::new())?;
        let old_identity = watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?
            .windows_directories[0].identity;
        std::fs::rename(root.path().join("a"), root.path().join("old"))?;
        std::fs::create_dir(root.path().join("a"))?;
        let replaced = watch.observe_directory(&path, windows_budget(), &CancellationToken::new())?;
        assert!(replaced.work.backend_write_operations >= 3);
        let held = watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        assert_eq!(held.windows_directories.len(), 1);
        assert_ne!(held.windows_directories[0].identity, old_identity);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn windows_cancellation_after_real_admission_preserves_spent_work_and_registration()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir_all(root.path().join("a/b"))?;
        let watch = watcher(root.path())?;
        let cancellation = CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(windows_budget(), &cancellation);
        let mut held = watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        held.watch_directory_measured(&namespace("/a")?, &mut work)?;
        let spent = work.work;
        assert!(spent.backend_read_operations > 0);
        cancellation.cancel();
        assert!(matches!(held.watch_directory_measured(&namespace("/a/b")?, &mut work),
            Err(DemandError::Cancelled)));
        assert_eq!(work.work, spent);
        assert_eq!(held.windows_directories.len(), 1);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn windows_native_child_notifications_reach_the_existing_hint_queue_and_rescan_cleanup()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        std::fs::write(root.path().join("a/f"), b"before")?;
        let watch = watcher(root.path())?;
        watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?.accept_lazy_baseline()?;
        watch.observe_directory(&namespace("/a")?, windows_budget(), &CancellationToken::new())?;
        std::fs::write(root.path().join("a/f"), b"after")?;
        let expected = namespace("/a/f")?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let receipt = watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?
                .poll(16, WorkBudget::UNBOUNDED, &CancellationToken::new())?;
            if matches!(receipt.value, WatchBatch::Changes { ref changes, .. }
                if changes.iter().any(|change| matches!(change,
                    WatchChange::Modified(path) | WatchChange::MetadataChanged(path) if path == &expected)))
            {
                break;
            }
            assert!(Instant::now() < deadline, "the native child hint was not delivered");
            std::thread::sleep(Duration::from_millis(10));
        }
        let mut held = watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        let cleanup = held.begin_rescan_measured(windows_budget(), &CancellationToken::new())?;
        assert!(cleanup.work.backend_write_operations >= 1);
        assert!(held.windows_directories.is_empty());
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn windows_missing_prefix_keeps_only_the_existing_ancestor_subscription()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        std::fs::create_dir(root.path().join("a"))?;
        let watch = watcher(root.path())?;
        let receipt = watch.observe_directory(&namespace("/a/missing/deeper")?,
            windows_budget(), &CancellationToken::new())?;
        assert!(receipt.work.backend_read_operations > 0);
        assert_eq!(receipt.work.backend_write_operations, 2);
        assert_eq!(watch.lock().map_err(|_| NativeWatchError::StatePoisoned)?.windows_directories.len(), 1);
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/watch.rs"]
mod tests;
