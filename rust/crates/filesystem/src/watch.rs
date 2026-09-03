//! Bounded native change notifications for materialized checkouts.
//!
//! Watch events are acceleration hints, never filesystem truth. Consumers read
//! the affected paths from the host and produce ordinary authenticated checkout
//! mutations. Queue overflow, backend uncertainty, ambiguous rename, root
//! replacement, restart, or an unrepresentable path invalidates the hint stream
//! and requires a new bounded baseline scan.

use crate::cancellation::{CancellationError, CancellationToken};
use crate::kernel::{LogicalName, NameEncoding, NamespacePath, NamespacePathError};
use crate::model::VolumeLimits;
use crate::performance::{
    MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkError,
};
use notify::event::{ModifyKind, RenameMode};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::mem::size_of;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// Native notification mechanism compiled for this target.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeWatchBackend {
    /// Linux inotify through the reviewed `notify` adapter.
    LinuxInotify,
    /// macOS `FSEvents` through the reviewed `notify` adapter.
    MacosFsevents,
    /// Windows `ReadDirectoryChangesW` through the reviewed `notify` adapter.
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

/// Stable native identity of one held watcher/capture root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeRootIdentity {
    device: u64,
    object: u64,
}

impl NativeRootIdentity {
    /// Derives identity from an already-open root handle.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when this platform cannot report a stable root
    /// identity.
    pub fn from_file(file: &std::fs::File) -> std::io::Result<Self> {
        native_root_identity(file)
    }

    /// Returns the canonical platform-neutral 16-byte identity encoding.
    ///
    /// The first eight bytes are the native storage-device identity and the
    /// final eight are the directory-object identity, both little-endian.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 16] {
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&self.device.to_le_bytes());
        bytes[8..].copy_from_slice(&self.object.to_le_bytes());
        bytes
    }

    /// Decodes the exact canonical identity representation.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let mut device = [0_u8; 8];
        device.copy_from_slice(&bytes[..8]);
        let mut object = [0_u8; 8];
        object.copy_from_slice(&bytes[8..]);
        Self {
            device: u64::from_le_bytes(device),
            object: u64::from_le_bytes(object),
        }
    }
}

#[cfg(unix)]
fn native_root_identity(file: &std::fs::File) -> std::io::Result<NativeRootIdentity> {
    use std::os::unix::fs::MetadataExt;
    let metadata = file.metadata()?;
    Ok(NativeRootIdentity {
        device: metadata.dev(),
        object: metadata.ino(),
    })
}

#[cfg(windows)]
fn native_root_identity(file: &std::fs::File) -> std::io::Result<NativeRootIdentity> {
    use cap_primitives::fs::_WindowsByHandle;
    let metadata = cap_std::fs::File::from_std(file.try_clone()?).metadata()?;
    let device = metadata.volume_serial_number().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "root volume identity is unavailable",
        )
    })?;
    let object = metadata.file_index().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "root file identity is unavailable",
        )
    })?;
    Ok(NativeRootIdentity {
        device: u64::from(device),
        object,
    })
}

#[cfg(not(any(unix, windows)))]
fn native_root_identity(_: &std::fs::File) -> std::io::Result<NativeRootIdentity> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "stable root identity is unavailable on this platform",
    ))
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
}

/// One live native watcher over a materialized checkout root.
///
/// Opening starts invalidated. Call [`Self::begin_rescan`], build an exact
/// baseline while notifications continue to queue, and call
/// [`Self::finish_rescan`] before polling. The same handshake repairs overflow
/// without losing changes concurrent with the baseline scan.
pub struct NativeWatch {
    _watcher: RecommendedWatcher,
    root: PathBuf,
    receiver: Receiver<WatchChange>,
    queued: Arc<AtomicU32>,
    shared: Arc<Mutex<SharedState>>,
    epoch: WatchEpoch,
    next_sequence: WatchSequence,
    rescan_in_progress: bool,
    root_identity: NativeRootIdentity,
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
        let options = options.validate()?;
        let requested_root = root.as_ref();
        let admission = requested_root
            .symlink_metadata()
            .map_err(|error| NativeWatchError::Io(error.to_string()))?;
        if admission.file_type().is_symlink() {
            return Err(NativeWatchError::RootIsNotDirectory);
        }
        let root = requested_root
            .canonicalize()
            .map_err(|error| NativeWatchError::Io(error.to_string()))?;
        let root_file =
            open_native_root(&root).map_err(|error| NativeWatchError::Io(error.to_string()))?;
        let metadata = root_file
            .metadata()
            .map_err(|error| NativeWatchError::Io(error.to_string()))?;
        if !metadata.is_dir() {
            return Err(NativeWatchError::RootIsNotDirectory);
        }
        let root_identity = NativeRootIdentity::from_file(&root_file)
            .map_err(|error| NativeWatchError::Io(error.to_string()))?;
        let capacity = usize::try_from(options.maximum_queued_changes)
            .map_err(|_| NativeWatchError::InvalidOptions)?;
        let (sender, receiver) = sync_channel(capacity);
        let queued = Arc::new(AtomicU32::new(0));
        let shared = Arc::new(Mutex::new(SharedState {
            invalidation: Some(WatchInvalidationReason::InitialSnapshotRequired),
        }));
        let callback_root = root.clone();
        let callback_shared = Arc::clone(&shared);
        let callback_queued = Arc::clone(&queued);
        let mut watcher = notify::recommended_watcher(move |event| {
            accept_native_event(
                event,
                &callback_root,
                options.limits,
                &sender,
                &callback_shared,
                &callback_queued,
            );
        })
        .map_err(|error| NativeWatchError::Backend(error.to_string()))?;
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
        Ok(Self {
            _watcher: watcher,
            root,
            receiver,
            queued,
            shared,
            epoch: WatchEpoch(0),
            next_sequence: WatchSequence(0),
            rescan_in_progress: false,
            root_identity,
        })
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
        if self.rescan_in_progress {
            return Err(NativeWatchError::RescanAlreadyInProgress);
        }
        let mut shared = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        while self.receiver.try_recv().is_ok() {
            self.queued.fetch_sub(1, Ordering::AcqRel);
        }
        shared.invalidation = None;
        self.epoch = WatchEpoch(
            self.epoch
                .0
                .checked_add(1)
                .ok_or(NativeWatchError::SequenceExhausted)?,
        );
        self.next_sequence = WatchSequence(0);
        self.rescan_in_progress = true;
        Ok(self.epoch)
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
        if !self.rescan_in_progress {
            return Err(NativeWatchError::NoRescanInProgress);
        }
        self.rescan_in_progress = false;
        let shared = self
            .shared
            .lock()
            .map_err(|_| NativeWatchError::StatePoisoned)?;
        Ok(shared.invalidation.map_or(
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
            .map(|state| state.invalidation)
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

fn accept_native_event(
    event: notify::Result<Event>,
    root: &Path,
    limits: VolumeLimits,
    sender: &SyncSender<WatchChange>,
    shared: &Arc<Mutex<SharedState>>,
    queued: &Arc<AtomicU32>,
) {
    let mapped = event
        .map_err(|_| WatchInvalidationReason::BackendError)
        .and_then(|event| map_event(&event, root, limits));
    let Ok(mut state) = shared.lock() else {
        return;
    };
    if state.invalidation.is_some() {
        return;
    }
    match mapped {
        Ok(changes) => {
            for change in changes {
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
        Err(reason) => state.invalidation = Some(reason),
    }
}

fn map_event(
    event: &Event,
    root: &Path,
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
        paths.push(relative_namespace_path(root, path, limits)?);
    }
    if paths.iter().any(NamespacePath::is_root)
        && matches!(
            event.kind,
            EventKind::Remove(_) | EventKind::Modify(ModifyKind::Name(_))
        )
    {
        return Err(WatchInvalidationReason::RootChanged);
    }
    if let EventKind::Modify(ModifyKind::Name(mode)) = event.kind {
        return match (mode, paths.as_slice()) {
            (RenameMode::Both, [from, to]) => Ok(vec![WatchChange::Renamed {
                from: from.clone(),
                to: to.clone(),
            }]),
            _ => Err(WatchInvalidationReason::AmbiguousRename),
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
        let (encoding, bytes) = host_name(name)?;
        let logical = LogicalName::new(encoding, bytes, limits.maximum_component_bytes)
            .map_err(|_| WatchInvalidationReason::UnrepresentablePath)?;
        components.push(logical);
    }
    NamespacePath::new(components, limits)
        .map_err(|_error: NamespacePathError| WatchInvalidationReason::UnrepresentablePath)
}

#[cfg(unix)]
fn host_name(name: &std::ffi::OsStr) -> Result<(NameEncoding, Vec<u8>), WatchInvalidationReason> {
    use std::os::unix::ffi::OsStrExt;
    let source = name.as_bytes();
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(source.len())
        .map_err(|_| WatchInvalidationReason::UnrepresentablePath)?;
    bytes.extend_from_slice(source);
    Ok((NameEncoding::PosixBytes, bytes))
}

#[cfg(windows)]
fn host_name(name: &std::ffi::OsStr) -> Result<(NameEncoding, Vec<u8>), WatchInvalidationReason> {
    use std::os::windows::ffi::OsStrExt;
    let units = name.encode_wide();
    let length = units
        .clone()
        .count()
        .checked_mul(2)
        .ok_or(WatchInvalidationReason::UnrepresentablePath)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| WatchInvalidationReason::UnrepresentablePath)?;
    for unit in units {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    Ok((NameEncoding::WindowsUtf16Le, bytes))
}

#[cfg(not(any(unix, windows)))]
fn host_name(name: &std::ffi::OsStr) -> Result<(NameEncoding, Vec<u8>), WatchInvalidationReason> {
    let value = name
        .to_str()
        .ok_or(WatchInvalidationReason::UnrepresentablePath)?;
    let source = value.as_bytes();
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(source.len())
        .map_err(|_| WatchInvalidationReason::UnrepresentablePath)?;
    bytes.extend_from_slice(source);
    Ok((NameEncoding::Utf8, bytes))
}

/// Native watcher failures that are not recoverable through a baseline rescan.
#[derive(Debug, Error)]
pub enum NativeWatchError {
    /// Queue or volume bounds are invalid.
    #[error("native watcher options are invalid")]
    InvalidOptions,
    /// Watch root is not a directory.
    #[error("native watcher root is not a directory")]
    RootIsNotDirectory,
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

#[cfg(test)]
#[path = "tests/watch.rs"]
mod tests;
