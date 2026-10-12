//! Bounded native change notifications for materialized checkouts.

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

/// The host's native event watcher.
#[cfg(target_os = "macos")]
pub type NativeEventWatcher = crate::fsevents::FsEventsWatcher;
/// The host's native event watcher.
#[cfg(not(target_os = "macos"))]
pub type NativeEventWatcher = notify::RecommendedWatcher;

/// Exact guarantees supplied by the compiled native watcher.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeWatchCapabilities {
    /// Host notification mechanism selected at compile time.
    pub backend: &'static str,
    /// Whether descendants are observed recursively.
    pub recursive: bool,
    /// Whether a persisted cursor can prove continuity across process restart.
    pub persistent_restart: bool,
    /// Whether every poll fences replacement of the admitted root identity.
    pub root_identity_fencing: bool,
}

/// Returns compile-time native watcher guarantees without touching a user path.
#[must_use]
pub const fn native_watch_capabilities() -> NativeWatchCapabilities {
    #[cfg(target_os = "linux")]
    let backend = "linux-inotify";
    #[cfg(target_os = "macos")]
    let backend = "macos-fsevents";
    #[cfg(target_os = "windows")]
    let backend = "windows-read-directory-changes";
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    let backend = "unsupported";

    NativeWatchCapabilities {
        backend,
        recursive: cfg!(any(target_os = "linux", target_os = "macos", windows)),
        persistent_restart: false,
        root_identity_fencing: cfg!(any(unix, windows)),
    }
}

/// One bounded host-side change hint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchChange {
    /// A namespace entry appeared.
    Created(NamespacePath),
    /// File content or an unspecified property may have changed.
    Modified(NamespacePath),
    /// A rename whose other end is unknown bound this name: what it names now, subtree and all, whatever was there before.
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
    /// A demanded namespace path cannot be represented exactly on this host.
    #[error("a native path cannot be represented by volume semantics")]
    UnrepresentablePath,
    /// A rename could not be paired without guessing.
    #[error("a native rename could not be paired exactly")]
    AmbiguousRename,
    /// A separately opened capture root does not match the watched directory.
    #[error("the watched root changed identity")]
    RootChanged,
}

/// One bounded watcher poll result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchBatch {
    /// A contiguous process-local interval of change hints.
    Changes {
        /// Invalidated watcher epoch.
        epoch: u64,
        /// Sequence assigned to the first returned change.
        first_sequence: u64,
        /// Sequence to request after applying every returned change.
        next_sequence: u64,
        /// Bounded hints moved from the native queue without path copies.
        changes: Vec<WatchChange>,
        /// Original queued-payload custody; keep it until the moved hints die.
        original: WatchBatchOriginal,
    },
    /// All hints from this epoch must be discarded and a baseline rescanned.
    RescanRequired {
        /// Invalidated watcher epoch.
        epoch: u64,
        /// Exact invalidation class.
        reason: WatchInvalidationReason,
    },
}

/// Physical allocation custody moved with a native hint interval.
#[derive(Debug, Default)]
pub struct WatchBatchOriginal {
    allocations: Vec<[Option<crate::NativeAllocation>; 2]>,
    // A zero-byte split of the actual externally returned change-vector guard.
    storage: Option<crate::NativeAllocation>,
}

impl Clone for WatchBatchOriginal {
    fn clone(&self) -> Self { Self::default() }
}

impl PartialEq for WatchBatchOriginal {
    fn eq(&self, _other: &Self) -> bool { true }
}

impl Eq for WatchBatchOriginal {}

impl WatchBatchOriginal {
    pub(crate) fn change_has_native_payload_custody(&self, index: usize) -> bool {
        self.allocations.get(index)
            .is_some_and(|allocations| allocations.iter().any(Option::is_some))
    }

    pub(crate) fn has_native_changes_storage_custody(&self) -> bool { self.storage.is_some() }

    fn reserve<W: crate::demand::SourceDemandReadWork>(
        capacity: usize, retain_guards: bool, work: &mut W,
    ) -> Result<Self, NativeWatchError> {
        if !retain_guards { return Ok(Self::default()); }
        let mut allocations = Vec::new();
        if capacity != 0 {
            // Guard headers are private metadata, not newly owned hint data.
            work.charge(WorkCounters {
                allocation_operations: 1, ..WorkCounters::UNCHARGED
            }).map_err(demand_watch_error)?;
            allocations.try_reserve_exact(capacity)
                .map_err(|_| NativeWatchError::Io(std::io::ErrorKind::OutOfMemory.into()))?;
        }
        Ok(Self { allocations, storage: None })
    }
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
    #[cfg(any(target_os = "linux", windows))]
    pending_rename: Option<PendingRename>,
    /// Host path of the outstanding fence cookie, cleared on delivery.
    fence: Option<PathBuf>,
    /// Host subtrees whose events are never hints, in fence preference order.
    excluded: Vec<PathBuf>,
    excluded_allocation: Option<crate::NativeAllocation>,
    _queue_allocation: Option<crate::NativeAllocation>,
}

#[cfg(any(target_os = "linux", windows))]
#[derive(Debug)]
struct PendingRename {
    tracker: usize,
    from: NamespacePath,
    to: Option<NamespacePath>,
    allocation: Option<crate::NativeAllocation>,
}

struct QueuedWatchChange {
    change: WatchChange,
    allocations: [Option<crate::NativeAllocation>; 2],
}

impl SharedState {
    const fn new(invalidation: Option<WatchInvalidationReason>) -> Self {
        Self {
            invalidation,
            #[cfg(any(target_os = "linux", windows))]
            pending_rename: None,
            fence: None,
            excluded: Vec::new(),
            excluded_allocation: None,
            _queue_allocation: None,
        }
    }

    fn seal_invalidation(&mut self) -> Option<WatchInvalidationReason> {
        #[cfg(any(target_os = "linux", windows))]
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
    #[cfg(any(target_os = "linux", windows))]
    maximum_queued_changes: u32,
    _root_allocation: Option<crate::NativeAllocation>,
}

enum NativeWatchDriver {
    Adapter(NativeEventWatcher),
    Source(crate::source_watch::NativeSourceWatch),
    StartupFailure(Arc<crate::source_watch::SourceWatchStartupFailure>),
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl crate::source_watch::HostChangeSink for Mutex<SharedState> {
    fn logical_owned_bytes(&self) -> u64 { 0 }
    fn host_changed(
        &self, changes: &[crate::source_watch::HostChange], work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        invalidate_root_hints(self, changes, work)
    }

    fn host_changed_with_native_work(
        &self, changes: &[crate::source_watch::HostChange], work: &mut crate::demand::NativeSourceWork<'_>,
    ) -> Result<(), crate::demand::DemandError> {
        invalidate_root_hints(self, changes, work)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn invalidate_root_hints<W: crate::demand::SourceDemandReadWork>(
    state: &Mutex<SharedState>, changes: &[crate::source_watch::HostChange], work: &mut W,
) -> Result<(), crate::demand::DemandError> {
    work.check()?;
    for change in changes {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
        if cfg!(target_os = "macos") || matches!(change, crate::source_watch::HostChange::Everything) {
            state.lock().map_err(|_| NativeWatchError::StatePoisoned)?
                .invalidation = Some(WatchInvalidationReason::NativeRescanRequired);
        }
    }
    Ok(())
}

#[cfg(any(target_os = "linux", windows))]
struct NativeEventDelivery {
    context: NativeEventContext,
    sender: SyncSender<QueuedWatchChange>,
    shared: Arc<Mutex<SharedState>>,
    queued: Arc<AtomicU32>,
    fenced: Arc<Condvar>,
}

#[cfg(target_os = "linux")]
impl crate::source_watch::NativeEventSink for NativeEventDelivery {
    fn logical_owned_bytes(&self) -> u64 {
        size_of::<Self>() as u64 + self.context.root.capacity() as u64
    }

    fn invalidate_measured(
        &self, work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        self.invalidate_with_work(work)
    }

    fn event_measured(
        &self, relative: &Path, mask: u32, cookie: usize, work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        self.event_with_work(relative, mask, cookie, work)
    }

    fn event_with_native_work(
        &self, relative: &Path, mask: u32, cookie: usize, work: &mut crate::demand::NativeSourceWork<'_>,
    ) -> Result<(), crate::demand::DemandError> {
        self.event_with_work(relative, mask, cookie, work)
    }

    fn invalidate_with_native_work(
        &self, work: &mut crate::demand::NativeSourceWork<'_>,
    ) -> Result<(), crate::demand::DemandError> {
        self.invalidate_with_work(work)
    }
}

#[cfg(target_os = "linux")]
impl NativeEventDelivery {
    fn invalidate_with_work<W: crate::demand::SourceDemandReadWork>(
        &self, work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        work.check()?;
        {
            let mut state = self.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
            state.invalidation = Some(WatchInvalidationReason::NativeRescanRequired);
        }
        self.fenced.notify_all();
        Ok(())
    }

    fn event_with_work<W: crate::demand::SourceDemandReadWork>(
        &self, relative: &Path, mask: u32, cookie: usize, work: &mut W,
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
        let completes_rename = mask & libc::IN_MOVED_TO != 0
            && state.pending_rename.as_ref().is_some_and(|pending| pending.tracker == cookie);
        if !completes_rename && self.queued.load(Ordering::Acquire)
            .saturating_add(u32::from(state.pending_rename.is_some())) >= self.context.maximum_queued_changes
        {
            state.invalidation = Some(WatchInvalidationReason::QueueOverflow);
            return Ok(());
        }
        let payload = match namespace_hint_with_work(relative, self.context.profile, self.context.limits, work) {
            Ok(path) => path,
            Err(DemandError::InvalidRequest) => {
                state.invalidation = Some(WatchInvalidationReason::UnrepresentablePath);
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let bytes = payload.1;
        let result = (|| {
            let payload = payload;
            if mask & libc::IN_MOVED_FROM != 0 {
                if cookie == 0 || state.pending_rename.is_some() {
                    state.invalidation = Some(WatchInvalidationReason::AmbiguousRename);
                } else {
                    state.pending_rename = Some(PendingRename {
                        tracker: cookie, from: payload.0, to: None, allocation: payload.2,
                    });
                }
                return Ok(());
            }
            if mask & libc::IN_MOVED_TO != 0
                && (cookie == 0 || !state.pending_rename.as_ref().is_some_and(|pending| pending.tracker == cookie))
            {
                state.invalidation = Some(WatchInvalidationReason::AmbiguousRename);
                return Ok(());
            }
            work.charge(WorkCounters { items_returned: 1, ..WorkCounters::default() })?;
            let change = if mask & libc::IN_MOVED_TO != 0 {
                let pending = state.pending_rename.take().expect("matched native rename cookie");
                QueuedWatchChange {
                    change: WatchChange::Renamed { from: pending.from, to: payload.0 }, allocations: [pending.allocation, payload.2],
                }
            } else {
                let change = if mask & libc::IN_CREATE != 0 {
                    WatchChange::Created(payload.0)
                } else if mask & libc::IN_DELETE != 0 {
                    WatchChange::Removed(payload.0)
                } else if mask & libc::IN_ATTRIB != 0 {
                    WatchChange::MetadataChanged(payload.0)
                } else {
                    WatchChange::Modified(payload.0)
                };
                QueuedWatchChange { change, allocations: [payload.2, None] }
            };
            match self.sender.try_send(change) {
                Ok(()) => {
                    self.queued.fetch_add(1, Ordering::Release);
                }
                Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                    state.invalidation = Some(WatchInvalidationReason::QueueOverflow);
                }
            }
            Ok::<(), DemandError>(())
        })();
        if bytes != 0 { work.release(bytes)?; }
        result
    }
}

enum HintName<'a> {
    Host(&'a std::ffi::OsStr),
    #[cfg(windows)]
    Wide(&'a [u16]),
}

fn host_hint_name(component: Component<'_>) -> Result<HintName<'_>, crate::demand::DemandError> {
    match component {
        Component::Normal(name) => Ok(HintName::Host(name)),
        _ => Err(crate::demand::DemandError::InvalidRequest),
    }
}

fn namespace_hint_with_work<W: crate::demand::SourceDemandReadWork>(
    relative: &Path, profile: FilesystemProfile, limits: VolumeLimits, work: &mut W,
) -> Result<(NamespacePath, u64, Option<crate::NativeAllocation>), crate::demand::DemandError> {
    namespace_hint_names_with_work(relative.components().map(host_hint_name), profile, limits, work)
}

fn namespace_hint_names_with_work<'a, W: crate::demand::SourceDemandReadWork>(
    names: impl Iterator<Item = Result<HintName<'a>, crate::demand::DemandError>> + Clone,
    profile: FilesystemProfile, limits: VolumeLimits, work: &mut W,
) -> Result<(NamespacePath, u64, Option<crate::NativeAllocation>), crate::demand::DemandError> {
    use crate::demand::DemandError;
    work.check()?;
    let original = if work.is_native_original() {
        Some(work.native_source_work().ok_or(DemandError::NativeWorkUnavailable)?
            .materialization_work()?)
    } else {
        None
    };
    let mut count = 0_usize;
    for name in names.clone() {
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
        let _ = name?;
        count = count.checked_add(1).ok_or(WorkError::Overflow)?;
        if count > usize::from(limits.maximum_path_depth) {
            return Err(DemandError::InvalidRequest);
        }
    }
    let backing = count.checked_mul(size_of::<LogicalName>()).ok_or(WorkError::Overflow)? as u64;
    let allocation = if let Some(original) = &original {
        Some(original.claim(backing, u64::from(backing != 0), WorkCounters::UNCHARGED)
            .map_err(crate::native_name::original_name_error)?)
    } else {
        work.allocate(backing)?;
        None
    };
    // Drop physical components before their native allocation guard.
    let mut owned = (Vec::<LogicalName>::new(), allocation);
    let mut claimed = if owned.1.is_some() { 0 } else { backing };
    let result = (|| {
        let reserved = owned.0.try_reserve_exact(count);
        let actual = owned.0.capacity().checked_mul(size_of::<LogicalName>())
            .ok_or(WorkError::Overflow)? as u64;
        let reconciled = if let Some(allocation) = &mut owned.1 {
            allocation.reconcile_capacity(actual).map_err(crate::native_name::original_name_error)
        } else {
            work.reconcile_allocation(&mut claimed, actual)
        };
        reserved.map_err(DemandError::Allocation)?;
        reconciled?;
        for name in names {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            let name = name?;
            let ((encoding, bytes), mut allocation) = match name {
                HintName::Host(name) => crate::native_name::host_name_bytes_with_source_work(
                    name, profile, limits.maximum_component_bytes, work,
                ),
                #[cfg(windows)]
                HintName::Wide(name) => crate::native_name::host_wide_name_bytes_with_source_work(
                    name, profile, limits.maximum_component_bytes, work,
                ),
            }?;
            if allocation.is_none() {
                claimed = claimed.checked_add(bytes.capacity() as u64).ok_or(WorkError::Overflow)?;
            }
            let name = LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
                .map_err(|_| DemandError::InvalidRequest)?;
            // Keep the name before its guard on every custody-transfer error.
            let mut incoming = (name, allocation.take());
            if let (Some(owner), Some(allocation)) = (&mut owned.1, &mut incoming.1) {
                owner.absorb(allocation).map_err(NativeWatchError::from)?;
            }
            owned.0.push(incoming.0);
        }
        NamespacePath::new(std::mem::take(&mut owned.0), limits)
            .map_err(|_| DemandError::InvalidRequest)
    })();
    match result {
        Ok(path) => Ok((path, claimed, owned.1.take())),
        Err(error) => {
            drop(owned);
            if claimed != 0 { work.release(claimed)?; }
            Err(error)
        }
    }
}

#[cfg(windows)]
fn wide_hint_names(path: &[u16]) -> impl Iterator<Item = Result<HintName<'_>, crate::demand::DemandError>> + Clone {
    path.split(|unit| matches!(*unit, 47 | 92))
        .filter(|name| !name.is_empty()).map(|name| Ok(HintName::Wide(name)))
}

#[cfg(windows)]
struct WindowsDirectoryWatch {
    path: Arc<(Vec<u16>, Option<crate::NativeAllocation>)>,
    identity: NativeRootIdentity,
    watcher: crate::source_watch::NativeSourceWatch,
}

#[cfg(windows)]
struct WindowsDirectorySink {
    prefix: Arc<(Vec<u16>, Option<crate::NativeAllocation>)>,
    delivery: Arc<NativeEventDelivery>,
    _allocation: Option<crate::materializer::NativeAllocation>,
}

#[cfg(windows)]
impl crate::source_watch::HostChangeSink for WindowsDirectorySink {
    fn logical_owned_bytes(&self) -> u64 {
        size_of::<Self>() as u64
            + if self.prefix.0.is_empty() { std::mem::size_of_val(&*self.prefix) as u64 + self.prefix.0.capacity() as u64 * 2 } else { 0 }
    }
    fn host_changed(
        &self, changes: &[crate::source_watch::HostChange], work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        self.host_changed_with_work(changes, work)
    }

    fn host_changed_with_native_work(
        &self, changes: &[crate::source_watch::HostChange], work: &mut crate::demand::NativeSourceWork<'_>,
    ) -> Result<(), crate::demand::DemandError> {
        self.host_changed_with_work(changes, work)
    }

    fn host_changed_wide(
        &self, changes: &[crate::source_watch::HostChangeWide<'_>], work: &mut crate::demand::DemandWork,
    ) -> Result<(), crate::demand::DemandError> {
        self.host_changed_wide_with_work(changes, work)
    }

    fn host_changed_wide_with_native_work(
        &self, changes: &[crate::source_watch::HostChangeWide<'_>], work: &mut crate::demand::NativeSourceWork<'_>,
    ) -> Result<(), crate::demand::DemandError> {
        self.host_changed_wide_with_work(changes, work)
    }
}

#[cfg(windows)]
impl WindowsDirectorySink {
    fn host_changed_with_work<W: crate::demand::SourceDemandReadWork>(
        &self, changes: &[crate::source_watch::HostChange], work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        use crate::source_watch::HostChange;
        for change in changes {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            match change {
                HostChange::Rebound(path) | HostChange::Altered(path) => {
                    let names = wide_hint_names(&self.prefix.0).chain(path.components().map(host_hint_name));
                    self.deliver_names_measured(names, None, work)?;
                }
                HostChange::File(_) => {}
                HostChange::Everything | HostChange::Unconfirmed => self.invalidate_measured(work)?,
            }
        }
        Ok(())
    }

    fn host_changed_wide_with_work<W: crate::demand::SourceDemandReadWork>(
        &self, changes: &[crate::source_watch::HostChangeWide<'_>], work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        use crate::source_watch::HostChangeWide;
        let mut file = 1;
        for change in changes {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            if let HostChangeWide::File(identity) = change {
                file = usize::try_from(*identity).map_err(|_| WorkError::Overflow)?;
                break;
            }
        }
        for change in changes {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            match change {
                HostChangeWide::Path { action, path } => {
                    self.deliver_names_measured(wide_hint_names(&self.prefix.0).chain(wide_hint_names(path)),
                        self.prefix.0.is_empty().then_some((*action, file)), work)?;
                }
                HostChangeWide::File(_) => {}
                HostChangeWide::Everything | HostChangeWide::Unconfirmed => self.invalidate_measured(work)?,
            }
        }
        Ok(())
    }

    fn invalidate_measured<W: crate::demand::SourceDemandReadWork>(
        &self, work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        work.check()?;
        self.delivery.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?
            .invalidation = Some(WatchInvalidationReason::NativeRescanRequired);
        self.delivery.fenced.notify_all();
        Ok(())
    }

    fn deliver_names_measured<'a, W: crate::demand::SourceDemandReadWork>(
        &self,
        names: impl Iterator<Item = Result<HintName<'a>, crate::demand::DemandError>> + Clone,
        action: Option<(u32, usize)>,
        work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        use crate::demand::DemandError;
        work.check()?;
        let mut state = self.delivery.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        if state.invalidation.is_some() { return Ok(()); }
        let completes_rename = action.is_some_and(|(action, cookie)| action == 5
            && state.pending_rename.as_ref().is_some_and(|pending| pending.tracker == cookie));
        if !completes_rename && self.delivery.queued.load(Ordering::Acquire)
            .saturating_add(u32::from(state.pending_rename.is_some()))
            >= self.delivery.context.maximum_queued_changes
        {
            state.invalidation = Some(WatchInvalidationReason::QueueOverflow);
            return Ok(());
        }
        let payload = match namespace_hint_names_with_work(
            names, self.delivery.context.profile, self.delivery.context.limits, work,
        ) {
            Ok(path) => path,
            Err(DemandError::InvalidRequest) => {
                state.invalidation = Some(WatchInvalidationReason::UnrepresentablePath);
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let bytes = payload.1;
        let result = (|| {
            let payload = payload;
            for excluded in &state.excluded {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                let relative = excluded.strip_prefix(&self.delivery.context.root)
                    .map_err(|_| DemandError::InvalidRequest)?;
                if namespace_starts_with_host_measured(&payload.0, relative, work)? { return Ok(()); }
            }
            if payload.0.is_root() {
                state.invalidation = Some(WatchInvalidationReason::RootChanged);
                return Ok(());
            }
            if let Some((4, cookie)) = action {
                if state.pending_rename.is_some() {
                    state.invalidation = Some(WatchInvalidationReason::AmbiguousRename);
                } else {
                    state.pending_rename = Some(PendingRename {
                        tracker: cookie, from: payload.0, to: None, allocation: payload.2,
                    });
                }
                return Ok(());
            }
            if action.is_some_and(|(action, cookie)| action == 5
                && !state.pending_rename.as_ref().is_some_and(|pending| pending.tracker == cookie))
            {
                state.invalidation = Some(WatchInvalidationReason::AmbiguousRename);
                return Ok(());
            }
            work.charge(WorkCounters { items_returned: 1, ..WorkCounters::default() })?;
            let change = match action {
                Some((1, _)) => QueuedWatchChange { change: WatchChange::Created(payload.0), allocations: [payload.2, None] },
                Some((2, _)) => QueuedWatchChange { change: WatchChange::Removed(payload.0), allocations: [payload.2, None] },
                Some((5, _)) => {
                    let pending = state.pending_rename.take().expect("matched native rename identity");
                    QueuedWatchChange {
                    change: WatchChange::Renamed { from: pending.from, to: payload.0 }, allocations: [pending.allocation, payload.2],
                }
                }
                _ => QueuedWatchChange { change: WatchChange::Modified(payload.0), allocations: [payload.2, None] },
            };
            match self.delivery.sender.try_send(change) {
                Ok(()) => {
                    self.delivery.queued.fetch_add(1, Ordering::Release);
                }
                Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                    state.invalidation = Some(WatchInvalidationReason::QueueOverflow);
                }
            }
            Ok::<(), DemandError>(())
        })();
        if bytes != 0 { work.release(bytes)?; }
        result
    }
}

#[cfg(windows)]
fn namespace_starts_with_host_measured<W: crate::demand::SourceDemandReadWork>(
    path: &NamespacePath, host: &Path, work: &mut W,
) -> Result<bool, crate::demand::DemandError> {
    use std::os::windows::ffi::OsStrExt as _;
    let mut names = path.components().iter();
    for component in host.components() {
        work.charge(WorkCounters {
            items_examined: 1,
            ..WorkCounters::default()
        })?;
        let (Component::Normal(host), Some(name)) = (component, names.next()) else {
            return Ok(false);
        };
        if name.encoding() == crate::kernel::NameEncoding::WindowsUtf16Le {
            let mut bytes = name.as_bytes().chunks_exact(2);
            for unit in host.encode_wide() {
                work.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::default()
                })?;
                if bytes.next() != Some(unit.to_le_bytes().as_slice()) {
                    return Ok(false);
                }
            }
            if bytes.next().is_some() {
                return Ok(false);
            }
        } else {
            let Some(host) = host.to_str() else {
                return Ok(false);
            };
            if name.as_bytes().len() != host.len() {
                return Ok(false);
            }
            for (expected, actual) in name.as_bytes().iter().zip(host.as_bytes()) {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                if expected != actual { return Ok(false); }
            }
        }
    }
    Ok(true)
}

/// One live native watcher over a materialized checkout root.
pub struct NativeWatch {
    watcher: NativeWatchDriver,
    root: PathBuf,
    profile: FilesystemProfile,
    limits: VolumeLimits,
    recursive: bool,
    watched_directories: Vec<PathBuf>,
    receiver: Mutex<Receiver<QueuedWatchChange>>,
    held_root: NativeWatchHeldRoot,
    #[cfg(windows)]
    windows_directories: Vec<WindowsDirectoryWatch>,
    #[cfg(windows)]
    windows_backing_allocation: Option<crate::materializer::NativeAllocation>,
    #[cfg(windows)]
    windows_delivery: Arc<NativeEventDelivery>,
    queued: Arc<AtomicU32>,
    shared: Arc<Mutex<SharedState>>,
    fenced: Arc<Condvar>,
    fences: u64,
    epoch: u64,
    next_sequence: u64,
    rescan_in_progress: bool,
    root_identity: NativeRootIdentity,
    complete: bool,
    original_work: Option<crate::NativeMaterializationWork>,
    root_allocation: Option<crate::NativeAllocation>,
    pending_source_watch: Option<Arc<crate::source_watch::SourceWatchStartupFailure>>,
    owner_allocation: Option<crate::NativeAllocation>,
}

impl std::fmt::Debug for NativeWatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeWatch")
            .field("root_identity", &self.root_identity)
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

struct NativeWatchHeldRoot {
    root: Option<crate::native_host::HostRoot>,
    cleanup: Option<crate::materializer::NativeCleanupWrite>,
}

impl std::ops::Deref for NativeWatchHeldRoot {
    type Target = crate::native_host::HostRoot;
    fn deref(&self) -> &Self::Target {
        self.root
            .as_ref()
            .expect("a settled watcher has no remaining root operations")
    }
}

impl NativeWatchHeldRoot {
    fn close_with_native_work(
        &mut self, work: &crate::NativeMaterializationWork,
        birth: Option<&crate::NativeMaterializationWork>,
    ) -> Result<bool, crate::NativeTreeMaterializationError> {
        work.admit(WorkCounters::UNCHARGED)?;
        if self.root.is_none() { return Ok(false); }
        let mut superseded_cleanup = None;
        let cleanup = if birth.is_some_and(|original| original.shares_ledger(work)) {
            self.cleanup.as_mut().expect("native root close retains its original credit")
                .bind_current_work(work)?;
            self.cleanup.take().expect("native root close was reserved before its open")
        } else {
            // Admit the current invocation's close before taking custody. The birth reservation remains resident until physical close.
            let cleanup = work.reserve_cleanup_write()?;
            superseded_cleanup = self.cleanup.take();
            cleanup
        };
        let root = self.root.take().expect("the held directory remains owned until close admission");
        let ((), recorded) = cleanup.attempt(|| drop(root));
        drop(superseded_cleanup);
        recorded?;
        Ok(true)
    }
}

impl Drop for NativeWatchHeldRoot {
    fn drop(&mut self) {
        if let Some(root) = self.root.take() {
            if let Some(cleanup) = self.cleanup.take() {
                // This exact original close was reserved before opening. The primitive releases the meter before physical close.
                let _ = cleanup.attempt(|| drop(root));
            } else {
                drop(root);
            }
        }
    }
}

impl NativeWatch {
    /// Opens the platform-recommended event source.
    pub fn open(
        root: impl AsRef<Path>,
        options: NativeWatchOptions,
    ) -> Result<Self, NativeWatchError> {
        Self::open_with_profile(root, native_filesystem_profile(), options)
    }

    /// Opens a watcher whose emitted names use the supplied volume profile.
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

    /// Transfers an actual already-paid owner allocation on this same ledger.
    pub fn retain_owner_allocation(
        &mut self, incoming: &mut crate::NativeAllocation,
    ) -> Result<(), crate::NativeTreeMaterializationError> {
        let original = self.original_work.as_ref()
            .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?;
        if !original.shares_ledger(incoming.work())
            || !original.shares_resource_allowance(incoming.work()) {
            return Err(crate::NativeTreeMaterializationError::InvalidLayout);
        }
        if let Some(owner) = &mut self.owner_allocation {
            owner.absorb(incoming)
        } else {
            self.owner_allocation = Some(incoming.split_off(incoming.bytes())?);
            Ok(())
        }
    }

    /// Settles the same native producer, its child producers, and held root.
    pub fn shutdown_with_native_work(
        &mut self, original: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeWatchError> {
        original.admit(WorkCounters::UNCHARGED)?;
        let mut work = crate::demand::NativeSourceWork::from_native_materialization_work(original);
        if let Some(pending) = self.pending_source_watch.as_ref() {
            pending.stop_with_native_work(original).map_err(demand_watch_error)?;
            if !pending.is_stopped() { return Err(NativeWatchError::SourceUnavailable); }
            drop(self.pending_source_watch.take());
        }
        #[cfg(windows)]
        {
            self.clear_windows_directories_with_work(&mut work).map_err(demand_watch_error)?;
        }
        match &self.watcher {
            NativeWatchDriver::Source(watcher) => {
                watcher.stop_with_work(&mut work).map_err(demand_watch_error)?;
                if !watcher.is_stopped() { return Err(NativeWatchError::SourceUnavailable); }
            }
            NativeWatchDriver::StartupFailure(owner) => {
                owner.stop_with_native_work(original).map_err(demand_watch_error)?;
                if !owner.is_stopped() { return Err(NativeWatchError::SourceUnavailable); }
            }
            NativeWatchDriver::Adapter(_) => return Err(NativeWatchError::BoundedObservationUnavailable),
        }
        self.held_root.close_with_native_work(original, self.original_work.as_ref())?;
        loop {
            crate::demand::SourceDemandReadWork::charge(
                &mut work, WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED },
            ).map_err(demand_watch_error)?;
            match self.receiver.get_mut().map_err(|_| NativeWatchError::StatePoisoned)?.try_recv() {
                Ok(change) => {
                    self.queued.fetch_sub(1, Ordering::AcqRel);
                    drop(change);
                    original.record_admitted(WorkCounters::UNCHARGED)?;
                }
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        let mut state = self.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        #[cfg(any(target_os = "linux", windows))]
        let pending = state.pending_rename.take();
        let fence = state.fence.take();
        state.invalidation = Some(WatchInvalidationReason::NativeRescanRequired);
        drop(state);
        #[cfg(any(target_os = "linux", windows))]
        drop(pending);
        drop(fence);
        original.record_admitted(WorkCounters::UNCHARGED)?;
        Ok(true)
    }

    /// Opens the existing watcher with explicit preparation work/cancellation.
    pub fn open_with_profile_measured(
        root: impl AsRef<Path>,
        profile: FilesystemProfile,
        options: NativeWatchOptions,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<Self>, NativeWatchError> {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        let result = Self::open_with_profile_engine(root.as_ref(), profile, options, &mut work, None);
        let spent = work.receipt();
        result.map(|value| OperationReceipt { value, work: spent })
            .map_err(|error| OperationFailure::new(error, spent))
    }

    /// Opens the same held-root watcher on the invocation's original ledger.
    pub fn open_with_profile_with_native_work(
        root: &Path, profile: FilesystemProfile, options: NativeWatchOptions,
        original: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeWatchError> {
        Self::open_with_profile_engine(
            root, profile, options,
            &mut crate::demand::NativeSourceWork::from_native_materialization_work(original),
            Some(original),
        )
    }

    fn copy_startup_path<W: crate::demand::SourceDemandReadWork>(
        path: &Path, work: &mut W, original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(PathBuf, Option<crate::NativeAllocation>), NativeWatchError> {
        work.check().map_err(demand_watch_error)?;
        if let Some(original) = original {
            let (path, allocation) = original.path(path, None)?;
            Ok((path, Some(allocation)))
        } else {
            let (path, _) =
                crate::native_host::clone_path(path, work).map_err(demand_watch_error)?;
            Ok((path, None))
        }
    }

    fn startup_control<W: crate::demand::SourceDemandReadWork>(
        work: &mut W,
    ) -> Result<(), NativeWatchError> {
        // Refcount/control headers are not logical data, but this known controlled allocator call is still admitted before it happens.
        work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::UNCHARGED })
            .map_err(demand_watch_error)
    }

    fn open_with_profile_engine<W: crate::demand::SourceDemandReadWork>(
        requested_root: &Path, profile: FilesystemProfile, options: NativeWatchOptions,
        work: &mut W, original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<Self, NativeWatchError> {
        work.check().map_err(demand_watch_error)?;
        let options = options.validate()?;
        if cfg!(target_os = "linux") && options.recursive
            && (original.is_some() || work.remaining().map_err(demand_watch_error)?.has_finite_allowances())
        {
            return Err(NativeWatchError::InvalidOptions);
        }
        if original.is_some() && !cfg!(any(target_os = "linux", target_os = "macos", windows)) {
            return Err(NativeWatchError::NativeWorkUnavailable);
        }
        let use_adapter = if cfg!(target_os = "linux") {
            options.recursive
        } else if cfg!(windows) {
            false
        } else {
            original.is_none()
        };
        let ordinary_root;
        let root_path = if original.is_some() {
            requested_root
        } else {
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })
                .map_err(demand_watch_error)?;
            if requested_root.symlink_metadata()?.file_type().is_symlink() {
                return Err(NativeWatchError::RootIsNotDirectory);
            }
            work.charge(WorkCounters { backend_read_operations: 1, ..WorkCounters::UNCHARGED })
                .map_err(demand_watch_error)?;
            ordinary_root = requested_root.canonicalize()?;
            &ordinary_root
        };
        let cleanup = original
            .map(|work| work.allocation_work().reserve_cleanup_write())
            .transpose()?;
        let held =
            crate::native_host::HostRoot::open(root_path, work).map_err(demand_watch_error)?;
        let held_root = NativeWatchHeldRoot {
            root: Some(held),
            cleanup,
        };
        let metadata = held_root
            .stat(Path::new(""), work)
            .map_err(demand_watch_error)?;
        if !metadata.is_dir() {
            return Err(NativeWatchError::RootIsNotDirectory);
        }
        let root_identity = held_root.identity();
        if metadata.identity() != root_identity {
            return Err(NativeWatchError::RootChanged);
        }
        #[cfg(windows)]
        let complete = crate::native_host::reports_every_change(
            &held_root.directory_handle(), work,
        ).map_err(demand_watch_error)?;
        #[cfg(not(windows))]
        let complete = true;
        let (root, root_allocation) = Self::copy_startup_path(root_path, work, original)?;
        let capacity = usize::try_from(options.maximum_queued_changes)
            .map_err(|_| NativeWatchError::InvalidOptions)?;
        let queue_bytes = (capacity as u64).checked_mul(size_of::<WatchChange>() as u64)
            .ok_or(WorkError::Overflow)?;
        let queue_allocation = if let Some(original) = original {
            Some(original.claim(queue_bytes, u64::from((queue_bytes) != 0), crate::WorkCounters::UNCHARGED)?)
        } else {
            work.allocate(queue_bytes).map_err(demand_watch_error)?;
            None
        };
        Self::startup_control(work)?;
        let (sender, receiver) = sync_channel(capacity);
        Self::startup_control(work)?;
        let queued = Arc::new(AtomicU32::new(0));
        Self::startup_control(work)?;
        let mut state = SharedState::new(Some(WatchInvalidationReason::InitialSnapshotRequired));
        state._queue_allocation = queue_allocation;
        let shared = Arc::new(Mutex::new(state));
        Self::startup_control(work)?;
        let fenced = Arc::new(Condvar::new());
        #[cfg(windows)]
        let (context_root, context_allocation) = Self::copy_startup_path(&root, work, original)?;
        #[cfg(windows)]
        let context = NativeEventContext {
            root: context_root, root_identity, profile, limits: options.limits,
            #[cfg(any(target_os = "linux", windows))]
            maximum_queued_changes: options.maximum_queued_changes,
            _root_allocation: context_allocation,
        };
        #[cfg(windows)]
        let windows_delivery = {
            Self::startup_control(work)?;
            Arc::new(NativeEventDelivery {
                context, sender: sender.clone(), shared: Arc::clone(&shared),
                queued: Arc::clone(&queued), fenced: Arc::clone(&fenced),
            })
        };
        #[cfg(target_os = "linux")]
        let linux_sender = sender.clone();
        #[cfg(not(windows))]
        let callback = if use_adapter {
            let (context_root, context_allocation) = Self::copy_startup_path(&root, work, original)?;
            let context = NativeEventContext {
                root: context_root, root_identity, profile, limits: options.limits,
                #[cfg(target_os = "linux")]
                maximum_queued_changes: options.maximum_queued_changes,
                _root_allocation: context_allocation,
            };
            let callback_shared = Arc::clone(&shared);
            let callback_fenced = Arc::clone(&fenced);
            let callback_queued = Arc::clone(&queued);
            Some(move |event: notify::Result<Event>| {
                accept_native_event(&event, &context, &sender, &callback_shared, &callback_queued);
                if let (Ok(event), Ok(mut state)) = (&event, callback_shared.lock())
                    && state.fence.as_ref().is_some_and(|fence| event.paths.contains(fence))
                {
                    state.fence = None;
                    callback_fenced.notify_all();
                }
            })
        } else {
            None
        };
        let mut watched_directories = Vec::new();
        if use_adapter {
            work.allocate(0).map_err(demand_watch_error)?;
            watched_directories.try_reserve_exact(1).map_err(|error| {
                NativeWatchError::NativeWork(
                    crate::NativeTreeMaterializationError::AllocationFailed(error),
                )
            })?;
            let (directory, _) =
                crate::native_host::clone_path(&root, work).map_err(demand_watch_error)?;
            watched_directories.push(directory);
        }
        #[cfg(target_os = "linux")]
        let source_result = if options.recursive {
            None
        } else {
            let (event_root, event_allocation) = Self::copy_startup_path(&root, work, original)?;
            Self::startup_control(work)?;
            let events = Arc::new(NativeEventDelivery {
                context: NativeEventContext {
                    root: event_root, root_identity, profile, limits: options.limits,
                    maximum_queued_changes: options.maximum_queued_changes,
                    _root_allocation: event_allocation,
                },
                sender: linux_sender, shared: Arc::clone(&shared),
                queued: Arc::clone(&queued), fenced: Arc::clone(&fenced),
            });
            let sink = Arc::clone(&shared);
            Some(crate::source_watch::NativeSourceWatch::start_with_events_with_work(
                &held_root, sink, events, work,
            ))
        };
        #[cfg(windows)]
        let source_result = {
            Self::startup_control(work)?;
            let prefix = Arc::new((Vec::new(), None));
            Self::startup_control(work)?;
            let sink = Arc::new(WindowsDirectorySink {
                prefix, delivery: Arc::clone(&windows_delivery),
                _allocation: None,
            });
            Some(crate::source_watch::NativeSourceWatch::start_held_with_sink_with_work(
                &held_root, sink, complete, options.recursive, work,
            ))
        };
        #[cfg(target_os = "macos")]
        let source_result = if use_adapter {
            None
        } else {
            let sink = Arc::clone(&shared);
            Some(crate::source_watch::NativeSourceWatch::start_with_work(&held_root, sink, work))
        };
        #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
        let source_result: Option<Result<crate::source_watch::NativeSourceWatch, crate::DemandError>> = None;
        let watcher = if let Some(source) = source_result {
            match source {
                Ok(watcher) => NativeWatchDriver::Source(watcher),
                Err(crate::DemandError::SourceWatchStartup(owner)) => NativeWatchDriver::StartupFailure(owner),
                Err(error) => return Err(demand_watch_error(error)),
            }
        } else {
            Self::startup_control(work)?;
            #[cfg(not(windows))]
            let adapter = NativeEventWatcher::new(callback.expect("the legacy adapter owns its callback"), notify::Config::default())
                .map_err(NativeWatchError::Backend)?;
            #[cfg(windows)]
            unreachable!("Windows always uses its original held source");
            #[cfg(not(windows))]
            {
                work.charge(WorkCounters { backend_write_operations: 1, ..WorkCounters::UNCHARGED })
                    .map_err(demand_watch_error)?;
                let mut adapter = adapter;
                adapter.watch(&root, if options.recursive { RecursiveMode::Recursive } else { RecursiveMode::NonRecursive })
                    .map_err(NativeWatchError::Backend)?;
                NativeWatchDriver::Adapter(adapter)
            }
        };
        let watch = Self {
            watcher, root, profile, limits: options.limits, recursive: options.recursive, watched_directories,
            receiver: Mutex::new(receiver), held_root,
            #[cfg(windows)]
            windows_directories: Vec::new(),
            #[cfg(windows)]
            windows_backing_allocation: None,
            #[cfg(windows)]
            windows_delivery,
            queued, shared, fenced, fences: 0,
            epoch: 0, next_sequence: 0,
            rescan_in_progress: false, root_identity, complete,
            original_work: original.map(crate::NativeMaterializationWork::allocation_work),
            root_allocation, owner_allocation: None,
            pending_source_watch: None,
        };
        if let NativeWatchDriver::StartupFailure(cause) = &watch.watcher {
            return Err(NativeWatchError::SourceStartupFailed { cause: Arc::clone(cause), watch });
        }
        let checked = if let Some(original) = original {
            original.admit(WorkCounters::UNCHARGED)
        } else {
            work.check().map_err(crate::materializer::native_materialization_work_error)
        };
        match checked {
            Ok(()) => Ok(watch),
            Err(cause) => Err(NativeWatchError::StartupFailed { cause, watch }),
        }
    }

    /// Whether the root's file system reports every change, or that it lost some.
    #[must_use]
    pub const fn reports_every_change(&self) -> bool {
        self.complete
    }

    /// Adds one exact directory to a non-recursive native watcher.
    pub fn watch_directory(&mut self, directory: &NamespacePath) -> Result<(), NativeWatchError> {
        #[cfg(not(any(target_os = "linux", windows)))]
        return self.watch_directory_ordinary(directory);
        #[cfg(any(target_os = "linux", windows))]
        {
            let cancellation = CancellationToken::new();
            let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
            self.watch_directory_with_work(directory, &mut work).map_err(demand_watch_error)
        }
    }

    // Ordinary adapters may perform their existing unbounded path conversion. Finite demand observation never calls their opaque registration API.
    #[cfg(not(any(target_os = "linux", windows)))]
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
        let NativeWatchDriver::Adapter(watcher) = &mut self.watcher else {
            return Err(NativeWatchError::Backend(notify::Error::generic(
                "held child registrations use the native demand engine",
            )));
        };
        watcher.watch(&candidate, RecursiveMode::NonRecursive).map_err(NativeWatchError::Backend)?;
        let at = self.watched_directories.partition_point(|path| path < &candidate);
        self.watched_directories.insert(at, candidate);
        Ok(())
    }

    pub(crate) fn watch_directory_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self, directory: &NamespacePath, work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        work.check()?;
        if let Some(owner) = &self.pending_source_watch {
            return Err(crate::demand::DemandError::SourceWatchStartup(Arc::clone(owner)));
        }
        if self.recursive || directory.is_root() {
            return Ok(());
        }
        #[cfg(windows)]
        return self.watch_directory_wide_with_work(directory, work);
        #[cfg(not(any(windows, target_os = "linux")))]
        return Err(NativeWatchError::BoundedObservationUnavailable.into());
        #[cfg(target_os = "linux")]
        {
        let mut candidate = PathBuf::new();
        let mut candidate_bytes = 0_u64;
        let result = (|| {
            grow_watch_path_with_work(&mut candidate, &self.root, work, &mut candidate_bytes)?;
            for component in directory.components() {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                let (name, name_bytes) = watch_host_name_with_work(component, work)?;
                let mut host_components = Path::new(&name).components();
                if !matches!(host_components.next(), Some(Component::Normal(_)))
                    || host_components.next().is_some()
                {
                    drop(name);
                    work.release(name_bytes)?;
                    return Err(NativeWatchError::UnrepresentablePath.into());
                }
                let grown = grow_watch_path_with_work(&mut candidate, Path::new(&name), work, &mut candidate_bytes);
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
                    Err(error) => return Err(error.into()),
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
                    watcher.admit_with_work(relative, work)?;
                }
                NativeWatchDriver::Adapter(_) | NativeWatchDriver::StartupFailure(_) => {
                    return Err(NativeWatchError::BoundedObservationUnavailable.into());
                }
            }
            Ok(())
        })();
        drop(candidate);
        work.release(candidate_bytes)?;
        result
        }
    }

    #[cfg(target_os = "linux")]
    fn clear_linux_directories_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self, work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        let NativeWatchDriver::Source(watcher) = &self.watcher else { return Ok(()); };
        watcher.unadmit_with_work(Path::new(""), work)?;
        while self.watched_directories.len() > 1 {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            let removed = self.watched_directories.pop().ok_or(crate::demand::DemandError::InvalidRequest)?;
            drop(removed);
        }
        Ok(())
    }

    #[cfg(windows)]
    fn watch_directory_wide_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self, directory: &NamespacePath, work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        use crate::demand::DemandError;
        let mut retained_bytes = 0_u64;
        let mut path_bytes = 0_u64;
        let mut backing_claim = 0_u64;
        let result = (|| {
            let path = work.with_original_leaf(|work| {
                crate::demand::native_wide_path(
                    directory,
                    self.windows_delivery.context.limits,
                    work,
                )
            })?;
            path_bytes = (path.capacity() as u64)
                .checked_mul(2)
                .ok_or(WorkError::Overflow)?;
            let result = (|| {
                let mut held = self.held_root.open_dir_held_wide(&[], work)?;
                let mut component_start = 0;
                for index in 0..path.len() {
                    work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                    if path[index] != u16::from(b'\\') && index + 1 != path.len() {
                        continue;
                    }
                    let length = if path[index] == u16::from(b'\\') {
                        index
                    } else {
                        index + 1
                    };
                    let prefix = &path[..length];
                    let (next, identity) = match crate::native_host::open_windows_directory(
                        &held,
                        &path[component_start..length],
                        work,
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
                        if observed.path.0.as_slice() == prefix {
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
                        work.admit(copied)?;
                        self.windows_directories[at].watcher.stop_with_work(work)?;
                        if !self.windows_directories[at].watcher.is_stopped() {
                            return Err(DemandError::WorkerUnavailable);
                        }
                        work.record(copied)?;
                        let prior = self.windows_directories.remove(at);
                        drop(prior);
                    }
                    if self.windows_directories.len() == self.windows_directories.capacity() {
                        let count = self.windows_directories.len().checked_add(1).ok_or(WorkError::Overflow)?;
                        let bytes = (count as u64).checked_mul(size_of::<WindowsDirectoryWatch>() as u64)
                            .ok_or(WorkError::Overflow)?;
                        let old_bytes = (self.windows_directories.capacity() as u64)
                            .checked_mul(size_of::<WindowsDirectoryWatch>() as u64).ok_or(WorkError::Overflow)?;
                        let copied = WorkCounters { bytes_copied: old_bytes, ..WorkCounters::default() };
                        work.admit(copied)?;
                        let allocation = watch_claim_allocation(bytes, work)?;
                        let mut grown = Vec::with_capacity(count);
                        grown.append(&mut self.windows_directories);
                        self.windows_directories = grown;
                        drop(self.windows_backing_allocation.take());
                        if allocation.is_none() {
                            work.release(backing_claim)?;
                            retained_bytes = retained_bytes.checked_sub(backing_claim)
                                .and_then(|prior| prior.checked_add(bytes)).ok_or(WorkError::Overflow)?;
                            backing_claim = bytes;
                        }
                        self.windows_backing_allocation = allocation;
                        work.record(copied)?;
                    }
                    let key_bytes = (prefix.len() as u64).checked_mul(2).ok_or(WorkError::Overflow)?;
                    let copied = WorkCounters { bytes_copied: key_bytes, ..WorkCounters::default() };
                    work.admit(copied)?;
                    let allocation = watch_claim_allocation(key_bytes, work)?;
                    if allocation.is_none() {
                        retained_bytes = retained_bytes.checked_add(key_bytes).ok_or(WorkError::Overflow)?;
                    }
                    let mut key = (prefix.to_vec(), allocation);
                    work.record(copied)?;
                    let key_arc_bytes = std::mem::size_of_val(&key) as u64;
                    let mut allocation = watch_claim_allocation(key_arc_bytes, work)?;
                    if let (Some(key_allocation), Some(arc_allocation)) = (&mut key.1, &mut allocation) {
                        key_allocation.absorb(arc_allocation).map_err(NativeWatchError::from)?;
                    } else {
                        retained_bytes = retained_bytes.checked_add(key_arc_bytes).ok_or(WorkError::Overflow)?;
                    }
                    let key = Arc::new(key);
                    let sink_bytes = size_of::<WindowsDirectorySink>() as u64;
                    let allocation = watch_claim_allocation(sink_bytes, work)?;
                    if allocation.is_none() {
                        retained_bytes = retained_bytes.checked_add(sink_bytes).ok_or(WorkError::Overflow)?;
                    }
                    let sink = Arc::new(WindowsDirectorySink {
                        prefix: Arc::clone(&key), delivery: Arc::clone(&self.windows_delivery),
                        _allocation: allocation,
                    });
                    let watcher = match crate::source_watch::NativeSourceWatch::start_directory_with_work(
                        &held, sink, self.complete, work,
                    ) {
                        Ok(watcher) => watcher,
                        Err(crate::demand::DemandError::SourceWatchStartup(owner)) => {
                            self.pending_source_watch = Some(Arc::clone(&owner));
                            return Err(crate::demand::DemandError::SourceWatchStartup(owner));
                        }
                        Err(error) => return Err(error),
                    };
                    self.windows_directories.push(WindowsDirectoryWatch { path: key, identity, watcher });
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
    fn clear_windows_directories_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self, work: &mut W,
    ) -> Result<(), crate::demand::DemandError> {
        work.check()?;
        while let Some(observed) = self.windows_directories.last_mut() {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            observed.watcher.stop_with_work(work)?;
            if !observed.watcher.is_stopped() {
                return Err(crate::demand::DemandError::WorkerUnavailable);
            }
            let removed = self.windows_directories.pop().ok_or(crate::demand::DemandError::InvalidRequest)?;
            drop(removed);
        }
        self.windows_directories = Vec::new();
        drop(self.windows_backing_allocation.take());
        Ok(())
    }

    /// Adds every directory beneath the root, except the root names in `excluded`, to a non-recursive native watcher: what a baseline that captures the whole tree needs before it starts, so a change to anything it captured is reported.
    pub fn watch_tree(&mut self, excluded: &[&str]) -> Result<(), NativeWatchError> {
        if self.recursive {
            return Ok(());
        }
        let mut pending = vec![self.root.clone()];
        while let Some(directory) = pending.pop() {
            let entries = match std::fs::read_dir(&directory) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(NativeWatchError::Io(error)),
            };
            for entry in entries {
                let entry = entry.map_err(NativeWatchError::Io)?;
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
                let relative = path.strip_prefix(&self.root)
                    .map_err(|_| NativeWatchError::UnrepresentablePath)?;
                let cancellation = CancellationToken::new();
                let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
                let (namespace, bytes, allocation) = namespace_hint_with_work(relative, self.profile, self.limits, &mut work)
                    .map_err(demand_watch_error)?;
                let observed = self.watch_directory(&namespace);
                drop(namespace);
                let guarded = allocation.is_some();
                drop(allocation);
                if !guarded { work.release(bytes).map_err(demand_watch_error)?; }
                match observed {
                    Ok(()) => {}
                    Err(_) if !path.is_dir() => continue,
                    Err(error) => return Err(error),
                }
                pending.push(path);
            }
        }
        Ok(())
    }

    fn forget_removed_directories_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self, changes: &[WatchChange], work: &mut W,
        original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), NativeWatchError> {
        #[cfg(any(target_os = "linux", windows))]
        if matches!(self.watcher, NativeWatchDriver::Source(_)) {
            for change in changes {
                let removed = match change {
                    WatchChange::Removed(path) => [Some(path), None],
                    WatchChange::Renamed { from, to } => [Some(from), Some(to)],
                    _ => [None, None],
                };
                for path in removed.into_iter().flatten() {
                    let (relative, bytes, allocation) = if let Some(mut original) = original {
                        let (relative, allocation) =
                            crate::native_capture::namespace_to_host_path_with_work(path, &mut original)?;
                        (relative, 0, Some(allocation))
                    } else {
                        let (relative, bytes) =
                            crate::native_capture::namespace_to_host_path_with_work(path, work)
                                .map_err(demand_watch_error)?;
                        (relative, bytes, None)
                    };
                    let result = (|| {
                        #[cfg(target_os = "linux")]
                        {
                            let NativeWatchDriver::Source(watcher) = &self.watcher else {
                                unreachable!("the authoritative source driver remains owned");
                            };
                            // The source owns registrations; never copy an SDK root cache or adopt previously queued path data.
                            watcher.unadmit_with_work(&relative, work).map_err(demand_watch_error)
                        }
                        #[cfg(windows)]
                        {
                            use std::os::windows::ffi::OsStrExt;
                            let mut at = 0;
                            while at < self.windows_directories.len() {
                                work.charge(WorkCounters {
                                    items_examined: 1, ..WorkCounters::UNCHARGED
                                }).map_err(demand_watch_error)?;
                                let mut registered = self.windows_directories[at].path.0.iter();
                                let matches = relative
                                    .as_os_str()
                                    .encode_wide()
                                    .all(|unit| registered.next() == Some(&unit))
                                    && registered
                                        .next()
                                        .is_none_or(|unit| matches!(*unit, 47 | 92));
                                if !matches {
                                    at += 1;
                                    continue;
                                }
                                self.windows_directories[at].watcher.stop_with_work(work)
                                    .map_err(demand_watch_error)?;
                                if !self.windows_directories[at].watcher.is_stopped() {
                                    return Err(NativeWatchError::WorkerUnavailable);
                                }
                                work.charge(WorkCounters {
                                    bytes_copied: ((self.windows_directories.len() - at - 1) as u64)
                                        .checked_mul(size_of::<WindowsDirectoryWatch>() as u64)
                                        .ok_or(WorkError::Overflow)?,
                                    ..WorkCounters::UNCHARGED
                                }).map_err(demand_watch_error)?;
                                drop(self.windows_directories.remove(at));
                            }
                            Ok(())
                        }
                    })();
                    drop(relative);
                    drop(allocation);
                    let released = work.release(bytes).map_err(demand_watch_error);
                    result.and(released)?;
                }
            }
            return Ok(());
        }
        if self.recursive { return Ok(()); }
        if original.is_some() {
            return Err(NativeWatchError::BoundedObservationUnavailable);
        }
        // Only ordinary adapters retain their legacy opaque cache operations.
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
            let stale = self.watched_directories.iter()
                .filter(|watched| **watched != self.root && watched.starts_with(&removed))
                .cloned().collect::<Vec<_>>();
            for watched in stale {
                if let NativeWatchDriver::Adapter(watcher) = &mut self.watcher {
                    let _ = watcher.unwatch(&watched);
                }
                self.watched_directories.retain(|candidate| candidate != &watched);
            }
        }
        Ok(())
    }

    /// Admits a demand-backed lazy baseline without enumerating the root.
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
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        let result = self.accept_lazy_baseline_with_work(&mut work);
        result
            .map(|value| OperationReceipt { value, work: work.work })
            .map_err(|error| OperationFailure::new(error, work.work))
    }

    /// Admits the lazy baseline on the caller's original native work.
    pub fn accept_lazy_baseline_with_native_work(
        &mut self, work: &crate::NativeMaterializationWork,
    ) -> Result<WatchBatch, NativeWatchError> {
        let mut source = crate::demand::NativeSourceWork::from_native_materialization_work(work);
        self.accept_lazy_baseline_with_work(&mut source)
    }

    fn accept_lazy_baseline_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self, work: &mut W,
    ) -> Result<WatchBatch, NativeWatchError> {
        self.begin_rescan_with_work(work)?;
        self.finish_rescan_with_work(work)
    }

    /// Returns the exact root identity admitted when the watcher opened.
    #[must_use]
    pub const fn root_identity(&self) -> NativeRootIdentity {
        self.root_identity
    }

    /// Verifies that a separately held capture root is the directory admitted by this watcher.
    pub fn verify_root_identity(
        &self, observed: NativeRootIdentity, native: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeWatchError> {
        let mut work = crate::demand::NativeSourceWork::from_native_materialization_work(native);
        crate::native_host::NativeHostWork::check(&work).map_err(demand_watch_error)?;
        if observed == self.root_identity && self.root_unchanged_with_work(&mut work).map_err(demand_watch_error)? {
            return Ok(());
        }
        self.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?
            .invalidation = Some(WatchInvalidationReason::RootChanged);
        Err(NativeWatchError::RootChanged)
    }

    /// Starts a baseline scan and atomically discards only pre-scan hints.
    pub fn begin_rescan(&mut self) -> Result<u64, NativeWatchError> {
        let cancellation = CancellationToken::new();
        self.begin_rescan_with_work(&mut crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation))
    }

    fn begin_rescan_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self, work: &mut W,
    ) -> Result<u64, NativeWatchError> {
        work.check().map_err(demand_watch_error)?;
        if self.rescan_in_progress {
            return Err(NativeWatchError::RescanAlreadyInProgress);
        }
        let mut shared = self.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        let queued = self.queued.load(Ordering::Acquire);
        for _ in 0..queued {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })
                .map_err(demand_watch_error)?;
            let removed = self.receiver.get_mut().map_err(|_| NativeWatchError::StatePoisoned)?.try_recv().map_err(|_| NativeWatchError::StatePoisoned)?;
            self.queued.fetch_sub(1, Ordering::AcqRel);
            // Queued hints retain their service custody; this observer only discards the actual payload, even on a different allocation ledger.
            drop(removed);
            work.with_original_leaf(|actual| actual.reconcile_native_controls())
                .map_err(NativeWatchError::Work)?;
        }
        shared.invalidation = None;
        #[cfg(target_os = "linux")]
        {
            shared.pending_rename = None;
        }
        self.epoch = self.epoch.checked_add(1).ok_or(NativeWatchError::SequenceExhausted)?;
        self.next_sequence = 0;
        drop(shared);
        self.rescan_in_progress = true;
        #[cfg(windows)]
        self.clear_windows_directories_with_work(work).map_err(demand_watch_error)?;
        #[cfg(target_os = "linux")]
        if !self.recursive {
            self.clear_linux_directories_with_work(work).map_err(demand_watch_error)?;
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        if !self.recursive {
            if self.watched_directories.iter().any(|path| *path != self.root) {
                work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })
                    .map_err(demand_watch_error)?;
                work.check().map_err(demand_watch_error)?;
                return Err(NativeWatchError::BoundedObservationUnavailable);
            }
        }
        Ok(self.epoch)
    }

    /// Admits a baseline only when the Windows USN journal proves continuity.
    #[cfg(target_os = "windows")]
    pub fn accept_windows_usn_baseline(
        &mut self, checkpoint: crate::WindowsUsnCheckpoint,
    ) -> Result<Result<crate::WindowsUsnContinuity, crate::WindowsUsnError>, NativeWatchError> {
        if self.rescan_in_progress { return Err(NativeWatchError::RescanAlreadyInProgress); }
        let mut shared = self.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        if shared.invalidation != Some(WatchInvalidationReason::InitialSnapshotRequired)
            || self.epoch != 0 {
            return Err(NativeWatchError::RescanAlreadyInProgress);
        }
        let verified = match crate::validate_windows_usn_checkpoint(&self.root, checkpoint) {
            Ok(crate::WindowsUsnContinuity::Unchanged) => crate::WindowsUsnContinuity::Unchanged,
            Ok(_) => return Ok(Ok(crate::WindowsUsnContinuity::BaselineRequired(
                crate::WindowsUsnDiscontinuity::VolumeAdvanced,
            ))),
            Err(error) => return Ok(Err(error)),
        };
        // The journal verifies prior hints; this lock fences later callbacks.
        self.epoch = self.epoch.checked_add(1).ok_or(NativeWatchError::SequenceExhausted)?;
        self.next_sequence = 0;
        shared.invalidation = None;
        Ok(Ok(verified))
    }

    /// Seals the caller's baseline scan.
    pub fn finish_rescan(&mut self) -> Result<WatchBatch, NativeWatchError> {
        let cancellation = CancellationToken::new();
        self.finish_rescan_with_work(&mut crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation))
    }

    fn finish_rescan_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self, work: &mut W,
    ) -> Result<WatchBatch, NativeWatchError> {
        work.check().map_err(demand_watch_error)?;
        if !self.rescan_in_progress {
            return Err(NativeWatchError::NoRescanInProgress);
        }
        let unchanged = self.root_unchanged_with_work(work).map_err(demand_watch_error)?;
        work.check().map_err(demand_watch_error)?;
        let mut shared = self.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        if !unchanged {
            shared.invalidation = Some(WatchInvalidationReason::RootChanged);
        }
        self.rescan_in_progress = false;
        Ok(shared.seal_invalidation().map_or(
            WatchBatch::Changes {
                epoch: self.epoch,
                first_sequence: self.next_sequence,
                next_sequence: self.next_sequence,
                changes: Vec::new(),
                original: WatchBatchOriginal::default(),
            },
            |reason| WatchBatch::RescanRequired { epoch: self.epoch, reason },
        ))
    }

    /// Aborts an in-progress baseline and returns the watcher to an explicitly invalidated, retryable state.
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
    pub fn exclude_subtrees(&mut self, subtrees: &[NamespacePath]) -> Result<(), NativeWatchError> {
        let cancellation = CancellationToken::new();
        let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &cancellation);
        self.exclude_subtrees_with_work(subtrees, &mut work, None)
    }

    /// Copies exclusions on the caller's original native allocation ledger.
    pub fn exclude_subtrees_with_native_work(
        &mut self, subtrees: &[NamespacePath], work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeWatchError> {
        let mut source = crate::demand::NativeSourceWork::from_native_materialization_work(work);
        self.exclude_subtrees_with_work(subtrees, &mut source, Some(work))
    }

    fn exclude_subtrees_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self, subtrees: &[NamespacePath], work: &mut W,
        original: Option<&crate::NativeMaterializationWork>,
    ) -> Result<(), NativeWatchError> {
        work.check().map_err(demand_watch_error)?;
        // Keep the physical arrays before their ownership guards on every success and denial path; input and old arrays remain borrowed.
        let mut replacement = (Vec::<PathBuf>::new(), None);
        let shared_bytes = subtrees.len().checked_mul(std::mem::size_of::<PathBuf>())
            .ok_or(WorkError::Overflow)?;
        replacement.1 = if shared_bytes == 0 {
            None
        } else if let Some(original) = original {
            Some(original.claim(shared_bytes as u64, u64::from((shared_bytes as u64) != 0), crate::WorkCounters::UNCHARGED)?)
        } else {
            work.allocate(shared_bytes as u64).map_err(demand_watch_error)?;
            None
        };
        replacement.0.try_reserve_exact(subtrees.len())
            .map_err(|error| NativeWatchError::NativeWork(crate::NativeTreeMaterializationError::AllocationFailed(error)))?;
        let actual = replacement.0.capacity().checked_mul(std::mem::size_of::<PathBuf>())
            .ok_or(WorkError::Overflow)?;
        Self::reconcile_exclusion_capacity(&mut replacement.1, shared_bytes as u64, actual as u64, work)?;
        for subtree in subtrees {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })
                .map_err(demand_watch_error)?;
            if subtree.is_root() {
                return Err(NativeWatchError::UnrepresentablePath);
            }
            let relative = if let Some(mut native) = original {
                crate::native_capture::namespace_to_host_path_with_work(subtree, &mut native)
                    .map(|(path, allocation)| (path, Some(allocation), 0))?
            } else {
                crate::native_capture::namespace_to_host_path_with_work(subtree, work)
                    .map(|(path, bytes)| (path, None, bytes)).map_err(demand_watch_error)?
            };
            let mut host = if let Some(original) = original {
                original.path(&self.root, Some(&relative.0))
                    .map(|(path, allocation)| (path, Some(allocation)))?
            } else {
                let copied = self.root.as_os_str().len().checked_add(relative.0.as_os_str().len())
                    .ok_or(WorkError::Overflow)?;
                let capacity = copied.checked_add(1).ok_or(WorkError::Overflow)?;
                work.allocate_for(capacity as u64, WorkCounters {
                    bytes_copied: copied as u64, ..WorkCounters::default()
                }).map_err(demand_watch_error)?;
                let mut path = PathBuf::new();
                path.try_reserve_exact(capacity)
                    .map_err(|error| NativeWatchError::NativeWork(crate::NativeTreeMaterializationError::AllocationFailed(error)))?;
                Self::reconcile_exclusion_capacity(&mut None, capacity as u64, path.capacity() as u64, work)?;
                path.push(&self.root);
                path.push(&relative.0);
                (path, None)
            };
            let relative_bytes = relative.2;
            drop(relative);
            if relative_bytes != 0 {
                work.release(relative_bytes).map_err(demand_watch_error)?;
            }
            if let Some(allocation) = &mut host.1 {
                allocation.resize(host.0.capacity() as u64)?;
            }
            if let Some(owned) = &mut replacement.1 {
                if let Some(incoming) = &mut host.1 { owned.absorb(incoming)?; }
            } else {
                replacement.1 = host.1.take();
            }
            replacement.0.push(host.0);
        }
        work.check().map_err(demand_watch_error)?;
        let mut shared = self.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        let old = (
            std::mem::replace(&mut shared.excluded, replacement.0),
            std::mem::replace(&mut shared.excluded_allocation, replacement.1),
        );
        drop(shared);
        drop(old);
        Ok(())
    }

    fn reconcile_exclusion_capacity<W: crate::demand::SourceDemandReadWork>(
        allocation: &mut Option<crate::NativeAllocation>, admitted: u64, actual: u64, work: &mut W,
    ) -> Result<(), NativeWatchError> {
        if let Some(allocation) = allocation {
            allocation.resize(actual)?;
        } else if actual > admitted {
            // This is new allocator-selected capacity, not adopted old data.
            work.adopt(actual - admitted).map_err(demand_watch_error)?;
        } else if admitted > actual {
            work.release(admitted - actual).map_err(demand_watch_error)?;
        }
        Ok(())
    }

    /// Fences the actual native producers on the caller's original work.
    pub fn fence_with_native_work(
        &mut self, timeout: Duration, work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeWatchError> {
        let mut source = crate::demand::NativeSourceWork::from_native_materialization_work(work);
        self.fence_with_work(timeout, &mut source)
    }

    fn fence_with_work<W: crate::demand::SourceDemandReadWork>(
        &mut self, timeout: Duration, work: &mut W,
    ) -> Result<(), NativeWatchError> {
        work.check().map_err(demand_watch_error)?;
        let pending = match (&self.pending_source_watch, &self.watcher) {
            (Some(owner), _) | (_, NativeWatchDriver::StartupFailure(owner)) => Some(owner),
            _ => None,
        };
        if let Some(owner) = pending {
            let error = demand_watch_error(crate::demand::DemandError::SourceWatchStartup(Arc::clone(owner)));
            self.shared.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                .invalidation.get_or_insert(WatchInvalidationReason::NativeRescanRequired);
            self.fenced.notify_all();
            return Err(error);
        }
        let started = Instant::now();
        if timeout.is_zero() {
            self.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?
                .invalidation.get_or_insert(WatchInvalidationReason::NativeRescanRequired);
            self.fenced.notify_all();
            return Ok(());
        }
        if !self.root_unchanged_with_work(work).map_err(demand_watch_error)? {
            self.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?
                .invalidation = Some(WatchInvalidationReason::RootChanged);
            self.fenced.notify_all();
            return Ok(());
        }
        #[cfg(windows)]
        let mut children_delivered = true;
        #[cfg(windows)]
        for observed in &self.windows_directories {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })
                .map_err(demand_watch_error)?;
            children_delivered &= observed.watcher
                .fence_with_work(timeout.saturating_sub(started.elapsed()), work)
                .map_err(demand_watch_error)?;
        }
        let delivered = match &self.watcher {
            NativeWatchDriver::Source(watcher) => watcher
                .fence_with_work(timeout.saturating_sub(started.elapsed()), work)
                .map_err(demand_watch_error)?,
            NativeWatchDriver::StartupFailure(owner) => {
                return Err(demand_watch_error(crate::demand::DemandError::SourceWatchStartup(Arc::clone(owner))));
            }
            NativeWatchDriver::Adapter(_) => {
                #[cfg(target_os = "macos")]
                { false }
                #[cfg(not(target_os = "macos"))]
                { return Err(NativeWatchError::BoundedObservationUnavailable); }
            }
        };
        #[cfg(windows)]
        let delivered = delivered && children_delivered;
        let unchanged = self.root_unchanged_with_work(work).map_err(demand_watch_error)?;
        let mut state = self.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        if !unchanged {
            state.invalidation = Some(WatchInvalidationReason::RootChanged);
        } else if !delivered {
            state.invalidation.get_or_insert(WatchInvalidationReason::NativeRescanRequired);
        }
        drop(state);
        self.fenced.notify_all();
        Ok(())
    }

    fn root_unchanged_with_work<W: crate::demand::SourceDemandReadWork>(
        &self, work: &mut W,
    ) -> Result<bool, crate::demand::DemandError> {
        work.check()?;
        let Some(root) = self.held_root.root.as_ref() else {
            return Ok(false);
        };
        let held = root.stat(Path::new(""), work)?.identity();
        match crate::native_host::root_path_identity(&self.root, work) {
            Ok(identity) => Ok(held == self.root_identity && identity == self.root_identity),
            Err(crate::demand::DemandError::Io(error))
                if matches!(error.kind(), std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Waits until every host change completed before this call is queued.
    pub fn fence(&mut self, timeout: Duration) -> Result<(), NativeWatchError> {
        if let Some(work) = self.original_work.clone() {
            return self.fence_with_native_work(timeout, &work);
        }
        if !matches!(self.watcher, NativeWatchDriver::Adapter(_))
            || self.pending_source_watch.is_some()
        {
            let mut work = crate::demand::DemandWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new());
            return self.fence_with_work(timeout, &mut work);
        }
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
    }

    fn fence_cookie(&mut self) -> Result<PathBuf, NativeWatchError> {
        let host_directory = self.fence_directory()?;
        let relative = host_directory
            .strip_prefix(&self.root)
            .map_err(|_| NativeWatchError::UnrepresentablePath)?;
        let directory = crate::native_capture::host_path_to_namespace(relative, self.profile, self.limits)
            .map_err(|_| NativeWatchError::UnrepresentablePath)?;
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
            .map_err(NativeWatchError::Io)?;
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

    fn fence_directory(&self) -> Result<PathBuf, NativeWatchError> {
        let state = self.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
        for host in &state.excluded {
            match host.symlink_metadata() {
                Ok(metadata) if metadata.is_dir() => return Ok(host.clone()),
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(NativeWatchError::Io(error)),
            }
        }
        let host = state.excluded.first().cloned().ok_or(NativeWatchError::InvalidOptions)?;
        drop(state);
        std::fs::create_dir(&host).map_err(NativeWatchError::Io)?;
        Ok(host)
    }

    /// Moves at most `maximum_changes` contiguous hints from the native queue.
    pub fn poll(
        &mut self,
        maximum_changes: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<WatchBatch>, NativeWatchError> {
        let mut work = crate::demand::DemandWork::new(budget, cancellation);
        match self.poll_with_work(maximum_changes, &mut work, None, |_| Ok(())) {
            Ok((value, _)) => Ok(OperationReceipt { value, work: work.work }),
            Err(error) => Err(OperationFailure::new(error, work.work)),
        }
    }

    /// Runs the consumer's guarded publication before committing watch progress.
    pub fn poll_with_native_work_guarded<F>(
        &mut self, maximum_changes: u32, original: &crate::NativeMaterializationWork,
        before_publish: F,
    ) -> Result<(WatchBatch, crate::NativeAllocation), NativeWatchError>
    where
        F: FnMut(&WatchBatch) -> Result<(), crate::demand::DemandError>,
    {
        let mut work = crate::demand::NativeSourceWork::from_native_materialization_work(original);
        let (batch, allocation) =
            self.poll_with_work(maximum_changes, &mut work, Some(original), before_publish)?;
        Ok((batch, allocation.expect("original polling retains its actual output buffer")))
    }

    fn poll_with_work<W, F>(
        &mut self, maximum_changes: u32, work: &mut W,
        original: Option<&crate::NativeMaterializationWork>, mut before_publish: F,
    ) -> Result<(WatchBatch, Option<crate::NativeAllocation>), NativeWatchError>
    where
        W: crate::demand::SourceDemandReadWork,
        F: FnMut(&WatchBatch) -> Result<(), crate::demand::DemandError>,
    {
        work.check().map_err(demand_watch_error)?;
        if maximum_changes == 0 { return Err(NativeWatchError::ZeroPollLimit); }
        if self.rescan_in_progress { return Err(NativeWatchError::RescanInProgress); }
        // Data always drops before either its old payload or new buffer guards.
        let mut owned = (
            Vec::<WatchChange>::new(), WatchBatchOriginal::default(),
            original.map(|original| original.claim(0, 0, crate::WorkCounters::UNCHARGED)).transpose()?,
        );
        let mut ordinary_bytes = 0;
        let mut dequeued = false;
        let result = (|| {
            let mut reason = self.invalidation()?;
            if reason.is_none() {
                self.verify_current_root_with_work(work)?;
                reason = self.invalidation()?;
            }
            let mut batch = if let Some(reason) = reason {
                WatchBatch::RescanRequired { epoch: self.epoch, reason }
            } else {
                let queued = self.queued.load(Ordering::Acquire).min(maximum_changes);
                let capacity =
                    usize::try_from(queued).map_err(|_| NativeWatchError::InvalidOptions)?;
                let next = self
                    .next_sequence
                    .checked_add(u64::from(queued))
                    .ok_or(NativeWatchError::SequenceExhausted)?;
                let bytes = capacity
                    .checked_mul(size_of::<WatchChange>())
                    .map(crate::foundation::usize_to_u64)
                    .ok_or(WorkError::Overflow)?;
                if capacity != 0 {
                    if let Some(original) = original {
                        owned.2 = Some(original.claim(bytes, u64::from((bytes) != 0), crate::WorkCounters::UNCHARGED)?);
                    } else {
                        work.allocate(bytes).map_err(demand_watch_error)?;
                        ordinary_bytes = bytes;
                    }
                    owned.0.try_reserve_exact(capacity)
                        .map_err(|_| NativeWatchError::Io(std::io::ErrorKind::OutOfMemory.into()))?;
                    let actual = owned.0.capacity().checked_mul(size_of::<WatchChange>())
                        .map(crate::foundation::usize_to_u64).ok_or(WorkError::Overflow)?;
                    if let Some(allocation) = &mut owned.2 {
                        allocation.reconcile_capacity(actual)?;
                    } else {
                        work.with_original_leaf(|work| {
                            work.reconcile_native_allocation(&mut ordinary_bytes, actual)
                        }).map_err(demand_watch_error)?;
                    }
                }
                owned.1 = WatchBatchOriginal::reserve(
                    capacity, original.is_some() || self.original_work.is_some(), work,
                )?;
                owned.1.storage = owned.2.as_mut()
                    .map(|allocation| allocation.split_off(0)).transpose()?;
                while owned.0.len() < capacity {
                    // The queued count witnesses actual items before their sole consumer takes them; no post-dequeue quota check substitutes.
                    work.charge(WorkCounters {
                        items_examined: 1, items_returned: 1, ..WorkCounters::UNCHARGED
                    }).map_err(demand_watch_error)?;
                    let change = match self.receiver.get_mut().map_err(|_| NativeWatchError::StatePoisoned)?.try_recv() {
                        Ok(change) => change,
                        Err(TryRecvError::Empty) => {
                            self.invalidate_after_poll_failure(WatchInvalidationReason::BackendError);
                            return Err(NativeWatchError::NativeWork(
                                crate::NativeTreeMaterializationError::InvalidLayout,
                            ));
                        }
                        Err(TryRecvError::Disconnected) => {
                            self.invalidate_after_poll_failure(WatchInvalidationReason::BackendError);
                            return Err(NativeWatchError::SourceUnavailable);
                        }
                    };
                    dequeued = true;
                    self.queued.fetch_sub(1, Ordering::AcqRel);
                    let guarded = change.allocations.iter().any(Option::is_some);
                    if owned.1.allocations.capacity() != 0 {
                        if owned.1.allocations.len() == owned.1.allocations.capacity() {
                            return Err(NativeWatchError::NativeWork(
                                crate::NativeTreeMaterializationError::InvalidLayout,
                            ));
                        }
                        owned.1.allocations.push(change.allocations);
                    } else if guarded {
                        return Err(NativeWatchError::NativeWork(
                            crate::NativeTreeMaterializationError::InvalidLayout,
                        ));
                    }
                    owned.0.push(change.change);
                }
                self.forget_removed_directories_with_work(&owned.0, work, original)?;
                WatchBatch::Changes {
                    epoch: self.epoch,
                    first_sequence: self.next_sequence,
                    next_sequence: next,
                    changes: std::mem::take(&mut owned.0),
                    original: std::mem::take(&mut owned.1),
                }
            };
            // Serialize invalidation with publication, but never hold the work meter across arbitrary consumer code or physical data destruction.
            let shared = self.shared.clone();
            let mut state = shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?;
            if let Some(reason) = state.seal_invalidation() {
                drop(batch);
                batch = WatchBatch::RescanRequired { epoch: self.epoch, reason };
                if let Some(allocation) = &mut owned.2 {
                    allocation.resize(0)?;
                } else if ordinary_bytes != 0 {
                    work.release(ordinary_bytes).map_err(demand_watch_error)?;
                    ordinary_bytes = 0;
                }
            }
            before_publish(&batch).map_err(demand_watch_error)?;
            work.check().map_err(demand_watch_error)?;
            if let WatchBatch::Changes { next_sequence, .. } = &batch {
                self.next_sequence = *next_sequence;
            }
            Ok(batch)
        })();
        match result {
            Ok(batch) => Ok((batch, owned.2.take())),
            Err(error) => {
                drop(owned);
                if ordinary_bytes != 0 { let _ = work.release(ordinary_bytes); }
                if dequeued {
                    self.invalidate_after_poll_failure(WatchInvalidationReason::NativeRescanRequired);
                }
                Err(error)
            }
        }
    }

    fn verify_current_root_with_work<W: crate::demand::SourceDemandReadWork>(
        &self, work: &mut W,
    ) -> Result<(), NativeWatchError> {
        if self.held_root.root.is_none() { return Err(NativeWatchError::SourceUnavailable); }
        if self.root_unchanged_with_work(work).map_err(demand_watch_error)? {
            return Ok(());
        }
        self.shared.lock().map_err(|_| NativeWatchError::StatePoisoned)?
            .invalidation = Some(WatchInvalidationReason::RootChanged);
        Ok(())
    }

    fn invalidation(
        &self,
    ) -> Result<Option<WatchInvalidationReason>, NativeWatchError> {
        self.shared
            .lock()
            .map(|mut state| state.seal_invalidation())
            .map_err(|_| NativeWatchError::StatePoisoned)
    }

    fn invalidate_after_poll_failure(&self, reason: WatchInvalidationReason) {
        // Recovery must remain possible even after cancellation or poisoning.
        let mut state = self.shared.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.invalidation.get_or_insert(reason);
        self.fenced.notify_all();
    }
}

#[cfg(windows)]
fn watch_claim_allocation<W: crate::demand::SourceDemandReadWork>(
    bytes: u64, work: &mut W,
) -> Result<Option<crate::materializer::NativeAllocation>, crate::demand::DemandError> {
    work.check()?;
    if work.is_native_original() {
        let native = work.native_source_work()
            .ok_or(crate::demand::DemandError::NativeWorkUnavailable)?.materialization_work()?;
        return native.claim(bytes, u64::from((bytes) != 0), crate::WorkCounters::UNCHARGED).map(Some).map_err(|error| NativeWatchError::from(error).into());
    }
    work.allocate(bytes)?;
    Ok(None)
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
                .watch_directory_with_work(directory, &mut work)
        })();
        result
            .map(|value| OperationReceipt { value, work: work.work })
            .map_err(|error| OperationFailure::new(error, work.work))
    }

    #[cfg(feature = "native-mount")]
    fn observe_directory_with_native_work(
        &self, directory: &NamespacePath, mut work: crate::demand::NativeSourceWork<'_>,
    ) -> Result<(), crate::demand::DemandError> {
        crate::native_host::NativeHostWork::check(&work)?;
        self.lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?
            .watch_directory_with_work(directory, &mut work)
    }
}

#[cfg(target_os = "linux")]
/// Grows one owned path only after its logical capacity and copied bytes have been admitted.
fn grow_watch_path_with_work<W: crate::demand::SourceDemandReadWork>(
    candidate: &mut PathBuf,
    part: &Path,
    work: &mut W,
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
    work.admit(copied)?;
    work.allocate(bytes)?;
    let mut grown = PathBuf::with_capacity(capacity);
    grown.push(candidate.as_path());
    grown.push(part);
    if let Err(error) = work.record(copied) {
        drop(grown);
        work.release(bytes)?;
        return Err(error);
    }
    drop(std::mem::replace(candidate, grown));
    work.release(*owned_bytes)?;
    *owned_bytes = bytes;
    Ok(())
}

#[cfg(target_os = "linux")]
/// The same exact spelling conversion as native capture, with each temporary owned buffer admitted rather than hiding conversion behind a watch charge.
fn watch_host_name_with_work<W: crate::demand::SourceDemandReadWork>(
    component: &LogicalName,
    work: &mut W,
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
                work.admit(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() })?;
                work.allocate(bytes)?;
                let name = OsString::from_vec(source.to_vec());
                if let Err(error) = work.record(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() }) {
                    drop(name);
                    work.release(bytes)?;
                    return Err(error);
                }
                Ok((name, bytes))
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
            work.admit(copied)?;
            work.allocate(logical)?;
            let mut name = String::with_capacity(bytes);
            let result = (|| {
                for decoded in char::decode_utf16(units) {
                    name.push(decoded.map_err(|_| NativeWatchError::UnrepresentablePath)?);
                }
                work.record(copied)
            })();
            if let Err(error) = result {
                drop(name);
                work.release(logical)?;
                return Err(error);
            }
            Ok((OsString::from(name), logical))
        }
    }
}

fn accept_native_event(
    event: &notify::Result<Event>,
    context: &NativeEventContext,
    sender: &SyncSender<QueuedWatchChange>,
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
    let mapped = (|| {
        let event = event.as_ref().map_err(|_| WatchInvalidationReason::BackendError)?;
        validate_replayed_root_creation(event, &context.root, context.root_identity)?;
        #[cfg(target_os = "linux")]
        if let EventKind::Modify(ModifyKind::Name(mode)) = event.kind {
            if event.need_rescan() {
                return Err(WatchInvalidationReason::NativeRescanRequired);
            }
            let tracker = event.attrs.tracker().ok_or(WatchInvalidationReason::AmbiguousRename)?;
            if matches!(mode, RenameMode::From | RenameMode::To) {
                let [path] = event.paths.as_slice() else {
                    return Err(WatchInvalidationReason::AmbiguousRename);
                };
                let path = relative_namespace_path(&context.root, path, context.profile, context.limits)?;
                if path.is_root() { return Err(WatchInvalidationReason::RootChanged); }
                match mode {
                    RenameMode::From if state.pending_rename.is_none() => {
                        if queued.load(Ordering::Acquire) >= context.maximum_queued_changes {
                            return Err(WatchInvalidationReason::QueueOverflow);
                        }
                        state.pending_rename = Some(PendingRename {
                            tracker, from: path, to: None, allocation: None,
                        });
                    }
                    RenameMode::To => {
                        let pending = state.pending_rename.as_mut()
                            .ok_or(WatchInvalidationReason::AmbiguousRename)?;
                        if pending.tracker != tracker || pending.to.is_some() {
                            return Err(WatchInvalidationReason::AmbiguousRename);
                        }
                        pending.to = Some(path);
                    }
                    _ => return Err(WatchInvalidationReason::AmbiguousRename),
                }
                return Ok(Vec::new());
            }
            if mode == RenameMode::Both {
                let changes = map_event(event, &context.root, context.profile, context.limits)?;
                let [WatchChange::Renamed { from, to }] = changes.as_slice() else {
                    return Err(WatchInvalidationReason::AmbiguousRename);
                };
                let matched = state.pending_rename.as_ref().is_some_and(|pending| {
                    pending.tracker == tracker && &pending.from == from && pending.to.as_ref() == Some(to)
                });
                if !matched { return Err(WatchInvalidationReason::AmbiguousRename); }
                state.pending_rename = None;
                return Ok(changes);
            }
        }
        map_event(event, &context.root, context.profile, context.limits)
    })();
    let changes = match mapped {
        Ok(changes) => changes,
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
        match sender.try_send(QueuedWatchChange { change, allocations: [None, None] }) {
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
        // A replayed root creation is harmless; root removal and rename still invalidate continuity.
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
    // Coalesced root hints require a rescan because the changed descendant is unknown.
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
            // inotify pairs both halves by cookie before they reach here (see `accept_native_event`), so a lone half on Linux means the other half was lost.
            #[cfg(target_os = "linux")]
            _ => Err(WatchInvalidationReason::AmbiguousRename),
            // Windows identifies arrival and departure halves, so unpaired renames retain exact direction.
            #[cfg(windows)]
            (RenameMode::To, [to]) => Ok(vec![WatchChange::Arrived(to.clone())]),
            #[cfg(windows)]
            (RenameMode::From, [from]) => Ok(vec![WatchChange::Removed(from.clone())]),
            // FSEvents rename halves require final-state observation, including subtree removal when absent.
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
            // Darwin creation hints may be replayed; observe final state without replacing identity.
            #[cfg(target_os = "macos")]
            EventKind::Create(_) => WatchChange::Modified(path),
            #[cfg(not(target_os = "macos"))]
            EventKind::Create(_) => WatchChange::Created(path),
            EventKind::Remove(_) => WatchChange::Removed(path),
            // Darwin metadata hints may include coalesced content changes, so restage final state.
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
    /// A demanded directory traverses a symbolic link and cannot be mapped back to one exact namespace path.
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
    Io(#[source] std::io::Error),
    /// Native backend setup failed.
    #[error("native watcher backend failed: {0}")]
    Backend(#[source] notify::Error),
    /// Original partial source startup cause, already retained before failure.
    #[error(transparent)]
    SourceStartup(Arc<crate::source_watch::SourceWatchStartupFailure>),
    /// A partial source and the complete same root watcher remain owned.
    #[error("native source watcher startup failed: {cause}")]
    SourceStartupFailed {
        /// Original preallocated source failure custody.
        #[source]
        cause: Arc<crate::source_watch::SourceWatchStartupFailure>,
        /// Same actual watcher, retained for explicitly bounded shutdown.
        watch: NativeWatch,
    },
    /// Original native preparation admission or allocation cause.
    #[error(transparent)]
    NativeWork(#[from] crate::NativeTreeMaterializationError),
    /// A started watcher remains owned after a later preparation failure.
    #[error("native watcher startup failed: {cause}")]
    StartupFailed {
        /// Original typed preparation cause.
        #[source]
        cause: crate::NativeTreeMaterializationError,
        /// Same actual watcher, retained for explicitly bounded shutdown.
        watch: NativeWatch,
    },
    /// Original source reference is stale.
    #[error("source reference is stale")]
    StaleSource,
    /// Original source version is stale.
    #[error("source node changed during the request")]
    StaleVersion,
    /// Original directory cursor is stale.
    #[error("source directory cursor is expired or mismatched")]
    StaleCursor,
    /// Original directory cursor is busy.
    #[error("source directory cursor is in use")]
    CursorBusy,
    /// Original held source root is unavailable.
    #[error("source root is unavailable")]
    SourceUnavailable,
    /// Original requested source path is absent.
    #[error("source path is absent")]
    Absent,
    /// Original source node is not a regular file.
    #[error("source node is not a regular file")]
    NotRegularFile,
    /// Original source node is not a directory.
    #[error("source node is not a directory")]
    NotDirectory,
    /// Original source request is invalid.
    #[error("source request is invalid")]
    InvalidRequest,
    /// Original native source worker is unavailable.
    #[error("native source worker failed")]
    WorkerUnavailable,
    /// Original source does not support native custody.
    #[error("source does not support original native work custody")]
    NativeWorkUnavailable,
    /// Original mounted source cause, moved without formatting or boxing.
    #[cfg(feature = "native-mount")]
    #[error(transparent)]
    Mount(crate::native_mount::MountSourceError),
    /// Native metadata could not be represented.
    #[cfg(all(any(target_os = "linux", target_os = "macos", windows), any(feature = "native-mount", test)))]
    #[error(transparent)]
    Metadata(crate::native_host::NativeMetadataError),
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
    /// Polling was cooperatively cancelled.
    #[error(transparent)]
    Cancelled(#[from] CancellationError),
    /// Exact work overflowed or exceeded the admitted budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

impl From<std::io::Error> for NativeWatchError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

fn demand_watch_error(error: crate::demand::DemandError) -> NativeWatchError {
    match error {
        crate::demand::DemandError::Observation(error) => error,
        crate::demand::DemandError::Work(error) => NativeWatchError::Work(error),
        crate::demand::DemandError::Cancelled => NativeWatchError::Cancelled(CancellationError),
        crate::demand::DemandError::Io(error) => error.into(),
        crate::demand::DemandError::SourceWatchStartup(owner) => NativeWatchError::SourceStartup(owner),
        crate::demand::DemandError::StaleSource => NativeWatchError::StaleSource,
        crate::demand::DemandError::StaleVersion => NativeWatchError::StaleVersion,
        crate::demand::DemandError::StaleCursor => NativeWatchError::StaleCursor,
        crate::demand::DemandError::CursorBusy => NativeWatchError::CursorBusy,
        crate::demand::DemandError::SourceUnavailable => NativeWatchError::SourceUnavailable,
        crate::demand::DemandError::Absent => NativeWatchError::Absent,
        crate::demand::DemandError::NotRegularFile => NativeWatchError::NotRegularFile,
        crate::demand::DemandError::NotDirectory => NativeWatchError::NotDirectory,
        crate::demand::DemandError::InvalidRequest => NativeWatchError::InvalidRequest,
        crate::demand::DemandError::WorkerUnavailable => NativeWatchError::WorkerUnavailable,
        crate::demand::DemandError::OriginalWorkPoisoned => NativeWatchError::StatePoisoned,
        crate::demand::DemandError::Allocation(error) => NativeWatchError::NativeWork(crate::NativeTreeMaterializationError::AllocationFailed(error)),
        crate::demand::DemandError::NativeWorkUnavailable => NativeWatchError::NativeWorkUnavailable,
        #[cfg(feature = "native-mount")]
        crate::demand::DemandError::Mount(error) => NativeWatchError::Mount(error),
        #[cfg(all(target_os = "linux", any(feature = "native-mount", test)))]
        crate::demand::DemandError::LinuxMetadata(error) => NativeWatchError::Metadata(error),
        #[cfg(all(target_os = "macos", any(feature = "native-mount", test)))]
        crate::demand::DemandError::MacMetadata(error) => NativeWatchError::Metadata(error),
        #[cfg(all(windows, any(feature = "native-mount", test)))]
        crate::demand::DemandError::WindowsMetadata(error) => NativeWatchError::Metadata(error),
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
        watch.observe_directory(&namespace("/a/b")?, windows_budget(), &CancellationToken::new())?;
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
        std::fs::rename(root.path().join("a"), root.path().join("old"))?;
        std::fs::create_dir(root.path().join("a"))?;
        std::fs::write(root.path().join("a/f"), b"before")?;
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
        held.begin_rescan()?;
        assert!(held.windows_directories.is_empty());
        Ok(())
    }


}

#[cfg(test)]
#[path = "tests/watch/mod.rs"]
mod tests;
