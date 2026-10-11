//! Native file and owned-process primitives shared by SDK host consumers.
#![doc = include_str!("../README.md")]

use bytes::Bytes;
#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
use std::cell::Cell;
use std::fs::File;
use std::future::Future;
use std::io::{self, Read};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, OnceLock, Weak, mpsc};
use std::task::{Context, Poll, Waker};

type FileSequencer = Arc<Mutex<Option<Arc<OperationFence>>>>;

/// The existing shared file allocation also owns its original invocation
/// claim. All queued jobs and quarantine anchors retain this same payload.
pub(crate) struct NativeFilePayload {
    file: File,
    // Fields retire in declaration order: physical File before its claim.
    _allocation: Option<ReadAllocation>,
}

impl NativeFilePayload {
    fn unmetered(file: File) -> Self { Self { file, _allocation: None } }
}

impl std::ops::Deref for NativeFilePayload {
    type Target = File;
    fn deref(&self) -> &File { &self.file }
}

impl std::fmt::Debug for NativeFilePayload {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NativeFilePayload")
            .field("allocation", &self._allocation).finish_non_exhaustive()
    }
}

#[cfg(unix)]
impl std::os::fd::AsRawFd for NativeFilePayload {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.file)
    }
}

#[cfg(unix)]
impl std::os::fd::AsFd for NativeFilePayload {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        std::os::fd::AsFd::as_fd(&self.file)
    }
}

#[cfg(windows)]
impl std::os::windows::io::AsRawHandle for NativeFilePayload {
    fn as_raw_handle(&self) -> std::os::windows::io::RawHandle {
        std::os::windows::io::AsRawHandle::as_raw_handle(&self.file)
    }
}

#[cfg(windows)]
impl std::os::windows::io::AsHandle for NativeFilePayload {
    fn as_handle(&self) -> std::os::windows::io::BorrowedHandle<'_> {
        std::os::windows::io::AsHandle::as_handle(&self.file)
    }
}

#[cfg(windows)]
type FileIdentity = windows::FileIdentity;
#[cfg(target_os = "linux")]
type FileIdentity = linux::FileIdentity;
#[cfg(target_vendor = "apple")]
type FileIdentity = apple::FileIdentity;

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
struct FileHealth {
    tail: FileSequencer,
    uncertain: Arc<AtomicBool>,
    // Only an uncertain identity needs an open handle to prevent inode reuse.
    // Retaining healthy handles here would outlive their NativeFile owner and
    // can keep an exclusive Windows share lock open indefinitely.
    poison_anchor: Mutex<Option<Arc<NativeFilePayload>>>,
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
static FILE_HEALTH: OnceLock<Mutex<FileHealthRegistry>> = OnceLock::new();

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
struct FileHealthRegistry {
    entries: Vec<(FileIdentity, RegisteredFileHealth)>,
    capacity_limit: usize,
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
struct RegisteredFileHealth {
    health: Weak<FileHealth>,
    poisoned: Option<Arc<FileHealth>>,
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
impl FileHealthRegistry {
    fn new() -> Self {
        #[cfg(target_os = "linux")]
        let capacity_limit = linux::file_identity_capacity();
        #[cfg(target_vendor = "apple")]
        let capacity_limit = apple::file_identity_capacity();
        #[cfg(windows)]
        let capacity_limit = 4096;
        Self { entries: Vec::new(), capacity_limit }
    }

    fn find(&self, identity: FileIdentity, meter: Option<&dyn NativeReadMeter>)
        -> io::Result<Result<usize, usize>> {
        let mut low = 0;
        let mut high = self.entries.len();
        while low < high {
            if let Some(meter) = meter { meter.examine(1, 0)?; }
            let middle = low + (high - low) / 2;
            match self.entries[middle].0.cmp(&identity) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(Ok(middle)),
            }
        }
        Ok(Err(low))
    }

    fn sweep_idle(&mut self, meter: Option<&dyn NativeReadMeter>) -> io::Result<()> {
        let mut removed = 0;
        let mut shifted = 0;
        // All platform registration limits are at most 4096. Snapshot the
        // weak liveness decisions once: owners can drop concurrently while
        // the registry lock is held, so a second strong_count pass would not
        // describe the compaction whose moves were admitted.
        let mut live = [0_u64; 64];
        for (index, (_, entry)) in self.entries.iter().enumerate() {
            if let Some(meter) = meter { meter.examine(1, 0)?; }
            if entry.health.strong_count() == 0 { removed += 1; }
            else {
                live[index / 64] |= 1 << (index % 64);
                if removed != 0 { shifted += 1; }
            }
        }
        if let Some(meter) = meter {
            meter.examine(self.entries.len() as u64,
                (shifted * std::mem::size_of::<(FileIdentity, RegisteredFileHealth)>()) as u64)?;
        }
        let mut index = 0;
        self.entries.retain(|_| {
            let keep = live[index / 64] & (1 << (index % 64)) != 0;
            index += 1;
            keep
        });
        Ok(())
    }
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
fn file_identity(file: &File) -> io::Result<FileIdentity> {
    #[cfg(windows)]
    return windows::file_identity(file);
    #[cfg(target_os = "linux")]
    return linux::file_identity(file);
    #[cfg(target_vendor = "apple")]
    apple::file_identity(file)
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
thread_local! {
    static ACTIVE_FILES: Cell<*const ActiveFileScopeNode> = const { Cell::new(std::ptr::null()) };
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
struct ActiveFileScopeNode {
    identity: usize,
    previous: *const Self,
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
struct FileScopeRestore(*const ActiveFileScopeNode);

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
impl Drop for FileScopeRestore {
    fn drop(&mut self) {
        ACTIVE_FILES.with(|active| active.set(self.0));
    }
}

struct FileScope;

impl FileScope {
    /// Publishes only this synchronous invocation's pinned borrowed frame.
    /// No callback receives its address; this scope must never span async I/O
    /// or escape into a returned future. All three callers execute synchronously.
    fn with<T>(tail: Option<&FileSequencer>, operation: impl FnOnce() -> T) -> T {
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        {
            let Some(tail) = tail else { return operation(); };
            let previous = ACTIVE_FILES.with(Cell::get);
            let node = std::pin::pin!(ActiveFileScopeNode {
                identity: Arc::as_ptr(tail) as usize,
                previous,
            });
            // Declared after the pinned node: restore TLS before its frame
            // retires on every return or unwind, including nested scopes.
            let _restore = FileScopeRestore(previous);
            ACTIVE_FILES.with(|active| active.set(node.as_ref().get_ref()));
            operation()
        }
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        {
            let _ = tail;
            operation()
        }
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[allow(unsafe_code, reason = "TLS contains immutable pinned frames of live synchronous calls on this same thread")]
    fn contains(tail: &FileSequencer) -> bool {
        let identity = Arc::as_ptr(tail) as usize;
        let mut node = ACTIVE_FILES.with(Cell::get);
        while !node.is_null() {
            // SAFETY: only `with` publishes pointers derived from its pinned
            // stack node. Its immutable previous chain belongs to still-live
            // outer calls on this thread. RAII restores the prior head before
            // any node drops; no pointer is exposed to a callback or future.
            let active = unsafe { &*node };
            if active.identity == identity { return true; }
            node = active.previous;
        }
        false
    }
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
fn shared_file_health(file: &File) -> io::Result<Arc<FileHealth>> {
    shared_file_health_measured(file, None)
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
fn shared_file_health_measured(file: &File, meter: Option<&dyn NativeReadMeter>)
    -> io::Result<Arc<FileHealth>> {
    let registry_lock = match FILE_HEALTH.get() {
        Some(registry) => registry,
        None if meter.is_some() => return Err(io::ErrorKind::NotConnected.into()),
        None => FILE_HEALTH.get_or_init(|| Mutex::new(FileHealthRegistry::new())),
    };
    if let Some(meter) = meter { meter.admit_read(0)?; }
    let identity = file_identity(file)?;
    let mut registry = registry_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let old_bytes = registry.entries.capacity() * std::mem::size_of::<(FileIdentity, RegisteredFileHealth)>();
    let mut retained_registry_bytes = 0;
    let mut pending_registry_bytes = 0;
    let mut pending_health_bytes = 0usize;
    let result = (|| {
        if let Ok(index) = registry.find(identity, meter)? {
            if let Some(health) = registry.entries[index].1.health.upgrade() {
                return Ok(health);
            }
            if let Some(meter) = meter {
                meter.examine(0, ((registry.entries.len() - index - 1)
                    * std::mem::size_of::<(FileIdentity, RegisteredFileHealth)>()) as u64)?;
            }
            registry.entries.remove(index);
        }
        let capacity = registry.capacity_limit;
        if registry.entries.len() >= capacity { registry.sweep_idle(meter)?; }
        if registry.entries.len() >= capacity {
            return Err(io::Error::other("native file admission capacity exhausted"));
        }
        if registry.entries.len() == registry.entries.capacity() {
            let next = registry.entries.capacity().saturating_mul(2).max(1).min(capacity);
            let next_bytes = next * std::mem::size_of::<(FileIdentity, RegisteredFileHealth)>();
            if let Some(meter) = meter {
                meter.admit_examination(0, (registry.entries.len()
                    * std::mem::size_of::<(FileIdentity, RegisteredFileHealth)>()) as u64)?;
                meter.adopt(old_bytes as u64)?;
                retained_registry_bytes = old_bytes;
                meter.allocate(next_bytes as u64)?;
                pending_registry_bytes = next_bytes;
            }
            let mut replacement = Vec::new();
            replacement.try_reserve_exact(next).map_err(io::Error::other)?;
            // An explicit replacement makes old/new overlap and the copied
            // rows observable; no allocator-dependent in-place growth guess.
            replacement.append(&mut registry.entries);
            registry.entries = replacement;
            if let Some(meter) = meter { meter.release(old_bytes as u64); }
            if let Some(meter) = meter {
                retained_registry_bytes = next_bytes;
                pending_registry_bytes = 0;
                meter.record_examination(0, (registry.entries.len()
                    * std::mem::size_of::<(FileIdentity, RegisteredFileHealth)>()) as u64)?;
                let actual_bytes = registry.entries.capacity()
                    * std::mem::size_of::<(FileIdentity, RegisteredFileHealth)>();
                if actual_bytes > next_bytes {
                    meter.adopt((actual_bytes - next_bytes) as u64)?;
                    retained_registry_bytes = actual_bytes;
                }
            }
        }
        let index = registry.find(identity, meter)?.unwrap_or_else(|index| index);
        if let Some(meter) = meter {
            meter.allocate(arc_allocation_bytes::<Mutex<Option<Arc<OperationFence>>>>() as u64)?;
            pending_health_bytes += arc_allocation_bytes::<Mutex<Option<Arc<OperationFence>>>>();
        }
        let tail = Arc::new(Mutex::new(None));
        if let Some(meter) = meter {
            meter.allocate(arc_allocation_bytes::<AtomicBool>() as u64)?;
            pending_health_bytes += arc_allocation_bytes::<AtomicBool>();
        }
        let uncertain = Arc::new(AtomicBool::new(false));
        if let Some(meter) = meter {
            meter.allocate(arc_allocation_bytes::<FileHealth>() as u64)?;
            pending_health_bytes += arc_allocation_bytes::<FileHealth>();
        }
        let health = Arc::new(FileHealth { tail, uncertain, poison_anchor: Mutex::new(None) });
        if let Some(meter) = meter {
            meter.examine(0, ((registry.entries.len() - index)
                * std::mem::size_of::<(FileIdentity, RegisteredFileHealth)>()) as u64)?;
        }
        registry.entries.insert(index, (identity, RegisteredFileHealth {
            health: Arc::downgrade(&health), poisoned: None,
        }));
        Ok(health)
    })();
    if let Some(meter) = meter {
        meter.release((retained_registry_bytes + pending_registry_bytes) as u64);
        // Registration transfers these actual new allocations into the
        // canonical shared runtime control root. Aliases borrow that root;
        // they do not independently own its health or sequencing payloads.
        meter.release(pending_health_bytes as u64);
    }
    result
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
fn poison_file_health(file: &Arc<NativeFilePayload>, uncertain: &Arc<AtomicBool>) {
    let identity = file_identity(file).unwrap_or_else(|_| std::process::abort());
    let registry_lock = FILE_HEALTH.get().unwrap_or_else(|| std::process::abort());
    let mut registry = registry_lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let index = registry.find(identity, None).unwrap_or_else(|_| std::process::abort())
        .unwrap_or_else(|_| std::process::abort());
    let health = registry.entries[index].1.health.upgrade().unwrap_or_else(|| std::process::abort());
    if !Arc::ptr_eq(&health.uncertain, uncertain) { std::process::abort(); }
    *health.poison_anchor.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(file));
    registry.entries[index].1.poisoned = Some(health);
    uncertain.store(true, Ordering::Release);
    tracing::debug!(name: "acyclic.runtime.file_uncertain", "file health poisoned");
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
fn poison_borrowed_file_health(file: &File, uncertain: &Arc<AtomicBool>) {
    let anchor = Arc::new(NativeFilePayload::unmetered(
        file.try_clone().unwrap_or_else(|_| std::process::abort())));
    poison_file_health(&anchor, uncertain);
}

#[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
fn with_file_admission<T>(_: &File, operation: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
    operation()
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
fn with_file_admission<T>(file: &File, operation: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
    let health = shared_file_health(file)?;
    let tail = Arc::clone(&health.tail);
    let uncertain = Arc::clone(&health.uncertain);
    // A control/sync operation already running under this file's admitted
    // fence may call the direct API. It must inherit that fence, not enqueue
    // behind itself and deadlock the worker.
    if FileScope::contains(&tail) {
        if uncertain.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "prior native I/O completion on this file is uncertain",
            ));
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation))
            .unwrap_or_else(|_| std::process::abort());
        if result.as_ref().err().is_some_and(is_uncertain_io_error) {
            poison_borrowed_file_health(file, &uncertain);
        }
        return result;
    }
    let (predecessor, completion) = {
        let mut last = tail
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let predecessor = last.clone();
        let completion = Arc::new(OperationFence::default());
        *last = Some(Arc::clone(&completion));
        (predecessor, completion)
    };
    if let Some(predecessor) = predecessor {
        predecessor.wait_until_complete();
    }
    if uncertain.load(Ordering::Acquire) {
        completion.complete();
        return Err(io::Error::other(
            "prior native I/O completion on this file is uncertain",
        ));
    }
    let result = FileScope::with(Some(&tail), || {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation))
            .unwrap_or_else(|_| std::process::abort());
        if result.as_ref().err().is_some_and(is_uncertain_io_error) {
            poison_borrowed_file_health(file, &uncertain);
        }
        result
    });
    completion.complete();
    result
}

/// One file owned by the native I/O runtime.
///
/// Callers retain responsibility for capability-safe path resolution and pass
/// the resulting handle here exactly once. Operations submitted through this
/// handle execute in first-poll order. Creating an operation is lazy: an
/// unpolled future neither submits I/O nor delays later operations. Once an
/// operation has been polled, dropping its observer does not overtake earlier
/// admitted work.
pub struct NativeFile {
    file: Arc<NativeFilePayload>,
    tail: FileSequencer,
    uncertain: Arc<AtomicBool>,
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    _health: Arc<FileHealth>,
    #[cfg(windows)]
    overlapped: bool,
}

impl NativeFile {
    /// Transfers ownership of an already-authorized file handle to the runtime.
    pub fn from_file(file: File) -> io::Result<Self> {
        let file = Arc::new(NativeFilePayload::unmetered(file));
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let health = shared_file_health(&file)?;
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let (tail, uncertain) = (Arc::clone(&health.tail), Arc::clone(&health.uncertain));
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let (tail, uncertain) = (Arc::new(Mutex::new(None)), Arc::new(AtomicBool::new(false)));
        Ok(Self {
            file,
            tail,
            uncertain,
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            _health: health,
            #[cfg(windows)]
            overlapped: false,
        })
    }

    /// Imports a caller-held file into the same sequenced capability while
    /// admitting its actual identity probe and owned registration buffers.
    /// Initialize shared control with `initialize_native_io_services` first;
    /// this bounded entry never performs lazy resource-limit probes.
    pub fn from_file_measured(file: File, meter: &dyn NativeReadMeter) -> io::Result<Self> {
        meter.check()?;
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        if FILE_HEALTH.get().is_none() {
            return Err(io::ErrorKind::NotConnected.into());
        }
        let file_bytes = arc_allocation_bytes::<NativeFilePayload>();
        meter.allocate(file_bytes as u64)?;
        let file = Arc::new(NativeFilePayload::unmetered(file));
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let health = match shared_file_health_measured(&file, Some(meter)) {
            Ok(health) => health,
            Err(error) => {
                drop(file);
                meter.release(file_bytes as u64);
                return Err(error);
            }
        };
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let (tail, uncertain) = (Arc::clone(&health.tail), Arc::clone(&health.uncertain));
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let (tail, uncertain) = {
            meter.allocate(arc_allocation_bytes::<Mutex<Option<Arc<OperationFence>>>>() as u64)?;
            let tail = Arc::new(Mutex::new(None));
            meter.allocate(arc_allocation_bytes::<AtomicBool>() as u64)?;
            (tail, Arc::new(AtomicBool::new(false)))
        };
        Ok(Self {
            file, tail, uncertain,
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            _health: health,
            #[cfg(windows)]
            overlapped: false,
        })
    }

    /// Imports a finite native file with intrinsic original-ledger custody.
    /// The same physical Arc owns the claim through queued IO, observer Drop,
    /// and uncertain-IO quarantine; no caller performs a manual early release.
    /// Shared IO services must already have been explicitly initialized.
    pub fn from_file_measured_owned(file: File, meter: Arc<dyn NativeReadMeter>) -> io::Result<Self> {
        meter.check()?;
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        if FILE_HEALTH.get().is_none() {
            return Err(io::ErrorKind::NotConnected.into());
        }
        let allocation = ReadAllocation::new(&meter, arc_allocation_bytes::<NativeFilePayload>())?;
        let file = Arc::new(NativeFilePayload { file, _allocation: Some(allocation) });
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let health = shared_file_health_measured(&file, Some(meter.as_ref()))?;
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let (tail, uncertain) = (Arc::clone(&health.tail), Arc::clone(&health.uncertain));
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let (tail, uncertain) = {
            return Err(io::ErrorKind::Unsupported.into());
        };
        Ok(Self {
            file, tail, uncertain,
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            _health: health,
            #[cfg(windows)]
            overlapped: false,
        })
    }

    /// Logical handle storage owned by this capability, excluding the shared
    /// runtime health/sequencing control root. New registration allocations
    /// are still admitted and reported during their actual construction.
    #[must_use]
    pub fn logical_owned_bytes(&self) -> u64 {
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let health = 0;
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let health = arc_allocation_bytes::<Mutex<Option<Arc<OperationFence>>>>()
            + arc_allocation_bytes::<AtomicBool>();
        (arc_allocation_bytes::<NativeFilePayload>() + health) as u64
    }

    /// Transfers a Windows handle already opened for overlapped I/O.
    ///
    /// # Safety
    /// The handle must have been opened with `FILE_FLAG_OVERLAPPED`, must not
    /// already be associated with an I/O completion port, and must have no
    /// concurrent independent I/O outside this `NativeFile`. The caller must
    /// transfer the sole I/O ownership of the handle to this function.
    #[cfg(windows)]
    #[allow(
        unsafe_code,
        reason = "the caller must uphold the overlapped-handle ownership contract"
    )]
    pub unsafe fn from_overlapped_file_unchecked(file: File) -> io::Result<Self> {
        let mut native = Self::from_file(file)?;
        native.overlapped = true;
        Ok(native)
    }

    /// Metered adoption of an already-authorized overlapped Windows handle.
    ///
    /// # Safety
    /// The ownership and no-completion-port requirements of
    /// `from_overlapped_file_unchecked` also apply here.
    #[cfg(windows)]
    #[allow(unsafe_code, reason = "the caller transfers sole overlapped-handle ownership")]
    pub unsafe fn from_overlapped_file_measured_unchecked(
        file: File,
        meter: &dyn NativeReadMeter,
    ) -> io::Result<Self> {
        let mut native = Self::from_file_measured(file, meter)?;
        native.overlapped = true;
        Ok(native)
    }

    /// Imports an authorized overlapped handle with intrinsic original custody.
    ///
    /// # Safety
    /// The sole-ownership and no-completion-port requirements of
    /// `from_overlapped_file_unchecked` also apply here.
    #[cfg(windows)]
    #[allow(unsafe_code, reason = "the caller transfers sole overlapped-handle ownership")]
    pub unsafe fn from_overlapped_file_measured_owned_unchecked(
        file: File, meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<Self> {
        let mut native = Self::from_file_measured_owned(file, meter)?;
        native.overlapped = true;
        Ok(native)
    }

    /// Changes the logical file length without blocking the caller's executor.
    #[must_use]
    pub fn set_len_async(&self, size: u64) -> UnitCompletion {
        UnitCompletion::submit(
            Arc::clone(&self.file),
            NativeUnitOperation::SetLen(size),
            Some(Arc::clone(&self.tail)),
            Some(Arc::clone(&self.uncertain)),
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            Some(Arc::clone(&self._health)),
            obs::file_span!(DEBUG, "acyclic.runtime.set_len"),
            #[cfg(windows)]
            self.overlapped,
        )
    }

    /// Runs a platform-specific handle control in this file's operation order.
    /// The closure owns no borrowed handle after completion and executes off
    /// the caller's async executor when native completion is unavailable.
    #[must_use]
    pub fn control_async(
        &self,
        operation: impl FnOnce(&File) -> io::Result<()> + Send + 'static,
    ) -> UnitCompletion {
        UnitCompletion::submit(
            Arc::clone(&self.file),
            NativeUnitOperation::Control(Box::new(operation)),
            Some(Arc::clone(&self.tail)),
            Some(Arc::clone(&self.uncertain)),
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            Some(Arc::clone(&self._health)),
            obs::file_span!(DEBUG, "acyclic.runtime.control"),
            #[cfg(windows)]
            self.overlapped,
        )
    }

    /// Borrows the already-held file for measured scalar metadata operations.
    #[must_use]
    pub fn as_file(&self) -> &File {
        &self.file
    }

    /// Creates one metered positional read on this file's existing operation
    /// queue. OS calls are cooperatively cancellable only between requests.
    pub fn read_bounded_async(
        &self,
        offset: u64,
        length: usize,
        meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<NativeReadCompletion> {
        self.read_bounded_async_inner(offset, length, meter, false)
    }

    /// Reads an entire declared range or returns `UnexpectedEof` before
    /// constructing shared returned bytes. Partial physical work remains in
    /// the same invocation meter.
    pub fn read_bounded_exact_async(
        &self, offset: u64, length: usize, meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<NativeReadCompletion> {
        self.read_bounded_async_inner(offset, length, meter, true)
    }

    /// Reads into caller-prepared fixed, initialized storage on this file's
    /// existing queue. `storage.as_mut()` must expose the same writable slice
    /// without allocating. The caller retains its allocation custody in `B`;
    /// success returns that identical owner and its actual initialized prefix.
    /// All completion storage is admitted with the original meter before IO.
    /// Dropping an accepted observer does not cancel its physical operation.
    pub fn read_prepared_bounded_async<B>(
        &self, offset: u64, mut storage: B, length: usize, require_full: bool,
        meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<NativeCompletion<(B, usize)>>
    where B: AsMut<[u8]> + Send + 'static {
        meter.check()?;
        meter.admit_bytes(length)?;
        if storage.as_mut().len() < length {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        validate_prepared_read_range(offset, length)?;
        let allocation = ReadAllocation::new(
            &meter, arc_allocation_bytes::<Mutex<CompletionState<(B, usize)>>>(),
        )?;
        let state = Arc::new(Mutex::new(CompletionState {
            result: None, panic: None, waker: None, _allocation: Some(allocation),
        }));
        let output = Arc::clone(&state);
        #[cfg(windows)]
        let overlapped = self.overlapped;
        let operation = move |file: Arc<NativeFilePayload>, meter: Arc<dyn NativeReadMeter>,
                              completion: Option<Arc<OperationFence>>| {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let destination = storage.as_mut().get_mut(..length)
                    .ok_or(io::ErrorKind::InvalidInput)?;
                read_prepared_in_place(
                    &file, offset, destination, require_full, meter.as_ref(),
                    #[cfg(windows)]
                    overlapped,
                ).map(|filled| (storage, filled))
            }));
            // Retire invocation and handle aliases before exposing a terminal
            // result. Only caller-owned allocation custody travels with `B`.
            drop(file);
            drop(meter);
            match result {
                Ok(result) => finish_job(&output, result, completion),
                Err(payload) => finish_panicked_job(&output, payload, completion),
            }
        };
        let allocation = ReadAllocation::new(&meter, std::mem::size_of_val(&operation))?;
        Ok(NativeCompletion {
            pending: Some(NativeJob::PreparedRead {
                file: Arc::clone(&self.file), operation: Box::new(operation),
                meter, completion: None, _allocation: allocation,
                #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                _health: Some(Arc::clone(&self._health)),
            }),
            state, waiter: None, sequencer: Some(Arc::clone(&self.tail)),
            uncertain: Some(Arc::clone(&self.uncertain)), predecessor: None,
            completion: None, terminated: false,
        })
    }

    fn read_bounded_async_inner(
        &self, offset: u64, length: usize, meter: Arc<dyn NativeReadMeter>,
        require_full: bool,
    ) -> io::Result<NativeReadCompletion> {
        meter.check()?;
        meter.admit_bytes(length)?;
        if length == 0 {
            return Ok(NativeReadCompletion::ready(NativeReadResult {
                bytes: Bytes::new(), buffer_capacity: 0, _allocation: None,
            }));
        }
        offset.checked_add(length as u64)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        #[cfg(windows)]
        if length > u32::MAX as usize || offset == u64::MAX {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        let allocation = ReadAllocation::new(
            &meter, arc_allocation_bytes::<Mutex<CompletionState<NativeReadResult>>>(),
        )?;
        let state = Arc::new(Mutex::new(CompletionState {
            result: None,
            panic: None,
            waker: None,
            _allocation: Some(allocation),
        }));
        Ok(NativeReadCompletion::pending(NativeCompletion {
            pending: Some(NativeJob::BoundedRead {
                file: Arc::clone(&self.file),
                #[cfg(windows)]
                overlapped: self.overlapped,
                offset,
                length,
                require_full,
                meter,
                state: Arc::clone(&state),
                completion: None,
                #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                _health: Some(Arc::clone(&self._health)),
            }),
            state,
            waiter: None,
            sequencer: Some(Arc::clone(&self.tail)),
            uncertain: Some(Arc::clone(&self.uncertain)),
            predecessor: None,
            completion: None,
            terminated: false,
        }))
    }

    /// Submits owned positional reads while retaining this handle.
    #[must_use]
    pub fn read_batch_async(&self, reads: Vec<OwnedRead>) -> ReadBatch {
        ReadBatch::submit(
            Arc::clone(&self.file),
            reads,
            Some(Arc::clone(&self.tail)),
            Some(Arc::clone(&self.uncertain)),
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            Some(Arc::clone(&self._health)),
            obs::file_span!(DEBUG, "acyclic.runtime.read_batch"),
            #[cfg(windows)]
            self.overlapped,
        )
    }

    /// Submits owned writes while retaining this handle for later operations.
    #[must_use]
    pub fn write_all_batch_async(&self, writes: Vec<OwnedWrite>) -> UnitCompletion {
        UnitCompletion::submit(
            Arc::clone(&self.file),
            NativeUnitOperation::Write(writes),
            Some(Arc::clone(&self.tail)),
            Some(Arc::clone(&self.uncertain)),
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            Some(Arc::clone(&self._health)),
            obs::file_span!(DEBUG, "acyclic.runtime.write_batch"),
            #[cfg(windows)]
            self.overlapped,
        )
    }

    /// Flushes file contents and metadata without blocking the caller's executor.
    #[must_use]
    pub fn sync_async(&self, durability: Durability) -> UnitCompletion {
        UnitCompletion::submit(
            Arc::clone(&self.file),
            NativeUnitOperation::Sync(durability),
            Some(Arc::clone(&self.tail)),
            Some(Arc::clone(&self.uncertain)),
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            Some(Arc::clone(&self._health)),
            obs::file_span!(DEBUG, "acyclic.runtime.sync"),
            #[cfg(windows)]
            self.overlapped,
        )
    }

    /// Writes an already-accounted owned payload through this file's ordered
    /// native queue. The payload's allocation owner must live until completion.
    /// Physical prefixes are recorded before errors or cancellation are observed.
    pub fn write_bounded_async(
        &self,
        offset: u64,
        bytes: Bytes,
        meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<UnitCompletion> {
        self.validate_bounded_write(offset, bytes.len(), meter.as_ref())?;
        self.submit_bounded_unit(NativeBoundedUnitOperation::Write { offset, bytes }, meter)
    }

    /// Writes a caller-accounted payload on this same ordered native driver.
    /// `custody` must retain the original ledger's actual allocation claim;
    /// this method does not adopt `bytes.len()` or guess its owned capacity.
    /// The existing measured control closure owns both values. Physical data
    /// drops before custody, even if the observer is cancelled or dropped.
    pub fn write_bounded_async_with_custody<B: AsRef<[u8]> + Send + 'static, C: Send + 'static>(
        &self, offset: u64, bytes: B,
        meter: Arc<dyn NativeReadMeter>, custody: C,
    ) -> io::Result<UnitCompletion> {
        let owned = NativeOwnedWrite { bytes, _custody: custody };
        self.validate_bounded_write(offset, owned.bytes.as_ref().len(), meter.as_ref())?;
        self.control_bounded_async(move |file, meter| {
            let result = write_bounded_in_place(file, offset, owned.bytes.as_ref(), meter);
            drop(owned);
            result
        }, meter)
    }

    fn validate_bounded_write(
        &self, offset: u64, length: usize, meter: &dyn NativeReadMeter,
    ) -> io::Result<()> {
        meter.check()?;
        offset.checked_add(length as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write offset overflow"))?;
        #[cfg(windows)]
        if !self.overlapped {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                "metered writes require an admitted positional handle"));
        }
        Ok(())
    }

    /// Changes length with one actual, preadmitted native operation.
    pub fn set_len_bounded_async(
        &self, size: u64, meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<UnitCompletion> {
        self.submit_bounded_unit(NativeBoundedUnitOperation::SetLen(size), meter)
    }

    /// Flushes at the existing file fence, charging only a supported OS flush.
    pub fn sync_bounded_async(
        &self, durability: Durability, meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<UnitCompletion> {
        self.submit_bounded_unit(NativeBoundedUnitOperation::Sync(durability), meter)
    }

    /// Executes measured held-handle controls in this file's native order.
    /// The callback must admit each actual OS leaf using the supplied meter.
    pub fn control_bounded_async<F>(
        &self, operation: F, meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<UnitCompletion>
    where F: FnOnce(&File, &dyn NativeReadMeter) -> io::Result<()> + Send + 'static {
        meter.check()?;
        let allocation = ReadAllocation::new(&meter, std::mem::size_of::<F>())?;
        self.submit_bounded_unit(
            NativeBoundedUnitOperation::Control {
                operation: Box::new(operation), _allocation: allocation,
            }, meter,
        )
    }

    fn submit_bounded_unit(
        &self, operation: NativeBoundedUnitOperation, meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<UnitCompletion> {
        meter.check()?;
        let allocation = ReadAllocation::new(
            &meter, arc_allocation_bytes::<Mutex<CompletionState<()>>>(),
        )?;
        let state = Arc::new(Mutex::new(CompletionState {
            result: None, panic: None, waker: None, _allocation: Some(allocation),
        }));
        Ok(NativeCompletion {
            pending: Some(NativeJob::BoundedUnit {
                file: Arc::clone(&self.file), operation, meter,
                state: Arc::clone(&state), completion: None,
                tail: Some(Arc::clone(&self.tail)),
                uncertain: Some(Arc::clone(&self.uncertain)),
                #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                _health: Some(Arc::clone(&self._health)),
            }),
            state, waiter: None, sequencer: Some(Arc::clone(&self.tail)),
            uncertain: Some(Arc::clone(&self.uncertain)),
            predecessor: None, completion: None, terminated: false,
        })
    }
}

#[cfg(target_vendor = "apple")]
mod apple;
#[cfg(target_os = "linux")]
mod linux;
mod obs;
mod process_tree;
#[cfg(windows)]
mod windows;

pub use process_tree::{ProcessCaptureFailure, ProcessInput, ProcessTree};
#[cfg(unix)]
pub use process_tree::PreparedClosedProcess;

/// Whether a native operation may still have an unresolved kernel completion.
///
/// Such an error is not an ordinary retryable I/O failure: callers must not
/// assume the previous write was rolled back or safely publish another view.
pub fn is_uncertain_io_error(error: &io::Error) -> bool {
    #[cfg(target_os = "linux")]
    {
        linux::is_uncertain_completion(error)
    }
    #[cfg(windows)]
    {
        let _ = error;
        false
    }
    #[cfg(not(any(target_os = "linux", windows)))]
    {
        let _ = error;
        false
    }
}

/// Shared lifetime token for native resources that must not outlive an external owner.
///
/// Cloning the token extends the owner's lifetime. The owned value is released only after the
/// final token is dropped, allowing layered providers to carry a process-local ownership gate
/// alongside every independently cloned native handle.
#[derive(Clone)]
pub struct OwnershipAnchor {
    // Keep this field before `release`: Rust drops fields in declaration order, so the notifier
    // observes the owner count after this anchor has released its strong reference.
    _owner: Arc<dyn Send + Sync>,
    release: OwnershipReleaseNotifier,
}

#[derive(Clone)]
struct OwnershipReleaseNotifier {
    owner: Weak<dyn Send + Sync>,
    released: Arc<(Mutex<()>, Condvar)>,
}

impl Drop for OwnershipReleaseNotifier {
    fn drop(&mut self) {
        if self.owner.strong_count() == 0 {
            let (lock, released) = &*self.released;
            let _guard = lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            released.notify_all();
        }
    }
}

impl OwnershipAnchor {
    /// Erases and retains one ownership value behind a cloneable lifetime token.
    pub fn new(owner: impl Send + Sync + 'static) -> Self {
        let owner: Arc<dyn Send + Sync> = Arc::new(owner);
        Self {
            release: OwnershipReleaseNotifier {
                owner: Arc::downgrade(&owner),
                released: Arc::new((Mutex::new(()), Condvar::new())),
            },
            _owner: owner,
        }
    }

    /// Observes the exact boundary at which every clone has released the owner.
    #[must_use]
    pub fn release_barrier(&self) -> OwnershipReleaseBarrier {
        OwnershipReleaseBarrier {
            owner: self.release.owner.clone(),
            released: Arc::clone(&self.release.released),
        }
    }
}

/// A non-owning release condition for an [`OwnershipAnchor`] clone family.
///
/// The barrier never extends the protected owner's lifetime. Waiting completes only after the
/// final anchor clone has dropped the owner, making it suitable for explicit shutdown boundaries.
pub struct OwnershipReleaseBarrier {
    owner: Weak<dyn Send + Sync>,
    released: Arc<(Mutex<()>, Condvar)>,
}

impl OwnershipReleaseBarrier {
    /// Returns whether every ownership-anchor clone has released the owner.
    #[must_use]
    pub fn is_released(&self) -> bool {
        self.owner.strong_count() == 0
    }

    /// Blocks until every ownership-anchor clone has released the owner.
    pub fn wait(self) {
        let (lock, released) = &*self.released;
        let mut guard = lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while self.owner.strong_count() != 0 {
            guard = released
                .wait(guard)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

/// Required durability boundary for one file operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Durability {
    /// Flush file contents and metadata to stable storage.
    Full,
    /// Order earlier writes before later writes without forcing the device cache to drain.
    ///
    /// This capability is currently exposed only where the operating system provides that
    /// exact primitive. Other targets return [`io::ErrorKind::Unsupported`].
    Barrier,
}

/// Flushes file contents and metadata according to `durability`.
pub fn sync_file(file: &File, durability: Durability) -> io::Result<()> {
    let operation = obs::span!(
        DEBUG,
        "acyclic.runtime.sync_file",
        outcome = obs::Empty,
        error.kind = obs::Empty
    );
    operation.scope(|| {
        operation.finish(with_file_admission(file, || {
            sync_file_unsequenced(file, durability)
        }))
    })
}

fn sync_file_unsequenced(file: &File, durability: Durability) -> io::Result<()> {
    match durability {
        Durability::Full => file.sync_all(),
        Durability::Barrier => barrier_sync(file),
    }
}

/// Flushes file data according to `durability`.
pub fn sync_data(file: &File, durability: Durability) -> io::Result<()> {
    let operation = obs::span!(
        DEBUG,
        "acyclic.runtime.sync_data",
        outcome = obs::Empty,
        error.kind = obs::Empty
    );
    operation.scope(|| {
        operation.finish(with_file_admission(file, || {
            sync_data_unsequenced(file, durability)
        }))
    })
}

fn sync_data_unsequenced(file: &File, durability: Durability) -> io::Result<()> {
    match durability {
        Durability::Full => file.sync_data(),
        Durability::Barrier => barrier_sync(file),
    }
}

/// Flushes the directory entry namespace on platforms exposing directory flushes.
pub fn sync_parent(path: &Path, durability: Durability) -> io::Result<()> {
    let operation = obs::span!(
        DEBUG,
        "acyclic.runtime.sync_parent",
        outcome = obs::Empty,
        error.kind = obs::Empty
    );
    operation.scope(|| operation.finish(sync_parent_impl(path, durability)))
}

/// What recovery found after the last valid frame of an append-only log.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogTail {
    /// Nothing valid followed the invalid frame, so it was a torn append; the log now ends
    /// before it, durably.
    Torn,
    /// A valid frame follows the invalid one, so a committed frame was damaged. The log is
    /// left untouched.
    Corrupt,
}

impl LogTail {
    const fn name(self) -> &'static str {
        match self {
            Self::Torn => "torn",
            Self::Corrupt => "corrupt",
        }
    }
}

/// Recovers an append-only log whose frame at `invalid_start` is invalid.
///
/// A crash tears only the final append, and power loss can leave that append short,
/// zero-filled, or garbage once the file's size grew but its data never landed. So an
/// invalid frame is a torn tail exactly when no valid frame starts anywhere after it:
/// the log is then truncated before it and synchronized, and its cursor left at the new
/// end. Otherwise a committed frame was damaged, and recovery must fail closed rather than
/// discard the frames after it; the log is left untouched.
///
/// `valid_frame` reports whether a complete valid frame starts at the beginning of its
/// slice, which holds at most `maximum_frame_bytes` of the log's following bytes.
///
/// # Errors
///
/// Returns an I/O error when the log cannot be read, truncated, or synchronized.
pub fn recover_log_tail(
    file: &mut File,
    invalid_start: u64,
    maximum_frame_bytes: usize,
    durability: Durability,
    mut valid_frame: impl FnMut(&[u8]) -> bool,
) -> io::Result<LogTail> {
    let operation = obs::span!(
        DEBUG,
        "acyclic.runtime.recover_log_tail",
        bytes = obs::Empty,
        tail = obs::Empty,
        outcome = obs::Empty,
        error.kind = obs::Empty
    );
    operation.scope(|| {
        use std::io::{Read as _, Seek as _, SeekFrom};

        let tail = (|| {
            let end = file.metadata()?.len();
            operation.record("bytes", end.saturating_sub(invalid_start));
            let frame_bytes = maximum_frame_bytes.max(1);
            // Candidates are scanned through a window holding up to two frames' bytes, refilled
            // once fewer than a whole frame remain ahead of the candidate, so each byte is read
            // once.
            let first = invalid_start.saturating_add(1);
            let mut window = Vec::new();
            let mut window_start = first;
            let mut read_to = first;
            file.seek(SeekFrom::Start(first))?;
            for offset in first..end {
                let ahead = usize::try_from(read_to - offset).unwrap_or(usize::MAX);
                if ahead < frame_bytes && read_to < end {
                    window
                        .drain(..usize::try_from(offset - window_start).map_err(io::Error::other)?);
                    window_start = offset;
                    let wanted = u64::try_from(frame_bytes.saturating_mul(2)).unwrap_or(u64::MAX);
                    let fill_end = end.min(offset.saturating_add(wanted));
                    let mut buffer =
                        vec![0; usize::try_from(fill_end - read_to).map_err(io::Error::other)?];
                    file.read_exact(&mut buffer)?;
                    window.extend_from_slice(&buffer);
                    read_to = fill_end;
                }
                let skip = usize::try_from(offset - window_start).map_err(io::Error::other)?;
                let candidate = window.get(skip..).unwrap_or_default();
                let candidate = candidate
                    .get(..candidate.len().min(frame_bytes))
                    .unwrap_or_default();
                if valid_frame(candidate) {
                    return Ok(LogTail::Corrupt);
                }
            }
            file.set_len(invalid_start)?;
            sync_file(file, durability)?;
            file.seek(SeekFrom::Start(invalid_start))?;
            Ok(LogTail::Torn)
        })();
        if let Ok(tail) = &tail {
            operation.record("tail", tail.name());
        }
        operation.finish(tail)
    })
}

/// Whether an exclusive file-lock failure proves that another owner holds the lock.
/// Other failures must retain their original I/O error for recovery decisions.
#[must_use]
pub fn is_exclusive_lock_contention(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock || cfg!(windows) && error.raw_os_error() == Some(33)
}

/// Destination behavior for one durable rename.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenameMode {
    /// Fail atomically when the destination already exists.
    NoReplace,
    /// Atomically replace a compatible destination entry.
    Replace,
}

/// Renames one filesystem entry and durably publishes the affected namespace.
pub fn durable_rename(from: &Path, to: &Path, mode: RenameMode) -> io::Result<()> {
    let operation = obs::span!(
        DEBUG,
        "acyclic.runtime.durable_rename",
        outcome = obs::Empty,
        error.kind = obs::Empty
    );
    operation.scope(|| operation.finish(durable_rename_impl(from, to, mode)))
}

/// Reads at an absolute offset without changing the file cursor.
pub fn read_at(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    with_file_admission(file, || read_at_impl(file, offset, destination))
}

/// Reads one declared source prefix into caller-admitted storage without
/// changing the cursor or allocating. Actual work precedes late error delivery.
#[cfg(unix)]
pub fn read_declared_source_bytes(
    file: &std::fs::File,
    offset: u64,
    destination: &mut [u8],
    meter: &dyn NativeReadMeter,
) -> io::Result<usize> {
    meter.check()?;
    if destination.is_empty() {
        return Ok(0);
    }
    #[cfg(target_os = "linux")]
    linux::validate_io_range(offset, destination.len(), "read")?;
    #[cfg(not(target_os = "linux"))]
    {
        let length = u64::try_from(destination.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
        let end = offset.checked_add(length).ok_or(io::ErrorKind::InvalidInput)?;
        if offset > i64::MAX as u64 || end > i64::MAX as u64 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
    }
    let mut operation = NativeOperationReservation::new(meter, NativeOperationKind::Read, 1)?;
    let mut bytes = NativeByteReservation::new(meter, NativeOperationKind::Read, destination.len())?;
    meter.check()?;
    #[cfg(target_os = "linux")]
    let (result, recorded_operation) = operation.attempt(|| linux::read_at_validated(file, offset, destination));
    #[cfg(not(target_os = "linux"))]
    let (result, recorded_operation) = operation.attempt(|| read_at_impl(file, offset, destination));
    let recorded_bytes = bytes.record(result.as_ref().copied().unwrap_or(0));
    recorded_bytes?;
    recorded_operation?;
    let count = result?;
    meter.check()?;
    Ok(count)
}

/// Writes every byte at an absolute offset without changing the file cursor.
pub fn write_all_at(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    with_file_admission(file, || write_all_at_impl(file, offset, bytes))
}

/// Issues one device control request on `file` and waits for it on the
/// calling thread, whether the handle is synchronous or overlapped; an
/// overlapped handle's completion never reaches a port it is attached to.
/// Returns the output byte count.
///
/// # Errors
///
/// Returns the request's error, including a short output buffer.
#[cfg(windows)]
pub fn device_control_in_place(
    file: &impl std::os::windows::io::AsRawHandle,
    code: u32,
    input: &[u8],
    output: &mut [u8],
) -> io::Result<usize> {
    windows::control_in_place(file, code, input, output)
}

/// Applies one measured device request using the existing in-place completion
/// primitive. Cancellation cannot abandon an already-issued kernel request.
/// Metadata-control packets are not application/import source-body bytes.
/// Their actual request/completion attempts are metered here; the caller
/// admits its real input/output buffers and metadata examination/copies.
#[cfg(windows)]
pub fn device_control_in_place_measured(
    file: &impl std::os::windows::io::AsRawHandle,
    code: u32,
    input: &[u8],
    output: &mut [u8],
    meter: &dyn NativeReadMeter,
) -> io::Result<usize> {
    meter.check()?;
    windows::control_in_place_measured(file, code, input, output, meter)
}

/// Applies an actual mutating device request and completion on a held handle.
#[cfg(windows)]
pub fn device_control_in_place_measured_mutation(
    file: &impl std::os::windows::io::AsRawHandle,
    code: u32,
    input: &[u8],
    output: &mut [u8],
    meter: &dyn NativeReadMeter,
) -> io::Result<usize> {
    meter.check()?;
    windows::control_in_place_measured_mutation(file, code, input, output, meter)
}

/// Applies the exact native Windows file-attribute bitset.
#[cfg(windows)]
pub fn set_file_attributes(path: &Path, attributes: u32) -> io::Result<()> {
    windows::set_file_attributes(path, attributes)
}

/// One owned positional write whose buffer remains live through completion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedWrite {
    /// Absolute file offset for the first byte.
    pub offset: u64,
    /// Bytes written completely or reported as an error.
    pub bytes: Bytes,
}

/// One owned positional read request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnedRead {
    /// Absolute file offset for the first byte.
    pub offset: u64,
    /// Maximum bytes returned for this request.
    pub length: usize,
}

/// Submits an owned read batch to the shared native completion worker.
///
/// The first poll transfers ownership to the worker if capacity is available.
/// Dropping before admission discards the job; dropping after admission only
/// detaches the observer and the worker completes the operation.
pub fn read_batch_async(file: File, reads: Vec<OwnedRead>) -> ReadBatch {
    let file = Arc::new(NativeFilePayload::unmetered(file));
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    {
        match shared_file_health(&file) {
            Ok(health) => ReadBatch::submit(
                file,
                reads,
                Some(Arc::clone(&health.tail)),
                Some(Arc::clone(&health.uncertain)),
                Some(health),
                obs::file_span!(TRACE, "acyclic.runtime.read_batch"),
                #[cfg(windows)]
                false,
            ),
            Err(error) => ReadBatch::failed(error),
        }
    }
    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    ReadBatch::submit(
        file,
        reads,
        None,
        None,
        obs::file_span!(TRACE, "acyclic.runtime.read_batch"),
    )
}

/// Submits an owned write batch to the shared native completion workers.
///
/// Admission is irrevocable: after the first successful submission, dropping
/// the returned future does not roll back or cancel the write.
pub fn write_all_batch_async(file: File, writes: Vec<OwnedWrite>) -> UnitCompletion {
    let file = Arc::new(NativeFilePayload::unmetered(file));
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    {
        match shared_file_health(&file) {
            Ok(health) => UnitCompletion::submit(
                file,
                NativeUnitOperation::Write(writes),
                Some(Arc::clone(&health.tail)),
                Some(Arc::clone(&health.uncertain)),
                Some(health),
                obs::file_span!(TRACE, "acyclic.runtime.write_batch"),
                #[cfg(windows)]
                false,
            ),
            Err(error) => UnitCompletion::failed(error),
        }
    }
    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    UnitCompletion::submit(
        file,
        NativeUnitOperation::Write(writes),
        None,
        None,
        obs::file_span!(TRACE, "acyclic.runtime.write_batch"),
    )
}

/// The argument that tells a service started by [`spawn_service_process`]
/// that its standard output is the readiness channel.
pub const SERVICE_READY_ARGUMENT: &str = "--signal-ready";

/// The starter's end of a service's readiness channel.
#[derive(Debug)]
pub struct ServiceReadiness(File);

impl ServiceReadiness {
    /// Blocks until the service signals that it answers requests (`true`),
    /// or exits without signalling (`false`).
    ///
    /// # Errors
    ///
    /// Returns a failure to read the channel.
    pub fn wait(mut self) -> io::Result<bool> {
        let operation = obs::span!(
            INFO,
            "acyclic.runtime.service_ready",
            ready = obs::Empty,
            outcome = obs::Empty,
            error.kind = obs::Empty
        );
        operation.scope(|| {
            let mut signal = [0_u8; 1];
            let ready = loop {
                match self.0.read(&mut signal) {
                    Ok(read) => break Ok(read != 0),
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    // A closed Windows pipe reports that its writer is gone.
                    Err(error) if error.kind() == io::ErrorKind::BrokenPipe => break Ok(false),
                    Err(error) => break Err(error),
                }
            };
            if let Ok(ready) = ready {
                operation.record("ready", ready);
            }
            operation.finish(ready)
        })
    }
}

/// The service's end of its readiness channel.
#[derive(Debug)]
pub struct ServiceReadySignal(File);

impl ServiceReadySignal {
    /// Tells the starter that the service answers requests, and closes the
    /// channel.
    ///
    /// # Errors
    ///
    /// Returns a failure to write the channel, such as a starter that has
    /// already exited.
    pub fn signal(mut self) -> io::Result<()> {
        io::Write::write_all(
            &mut self.0,
            b"
",
        )
    }
}

/// Takes a service's readiness channel from its standard output, which then
/// discards everything written to it, so nothing the service prints can
/// reach, or fail on, a starter that has exited. Returns `None` when the
/// service was not started with [`SERVICE_READY_ARGUMENT`].
///
/// # Errors
///
/// Returns a failure to detach standard output.
pub fn take_service_ready_signal() -> io::Result<Option<ServiceReadySignal>> {
    if !std::env::args_os()
        .skip(2)
        .any(|argument| argument == SERVICE_READY_ARGUMENT)
    {
        return Ok(None);
    }
    #[cfg(windows)]
    {
        windows::take_standard_output().map(|file| file.map(ServiceReadySignal))
    }
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    {
        take_standard_output().map(|file| Some(ServiceReadySignal(file)))
    }
    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    {
        Ok(None)
    }
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
#[allow(
    unsafe_code,
    reason = "dup2 replaces one descriptor number with an open one"
)]
fn take_standard_output() -> io::Result<File> {
    use std::os::fd::{AsFd as _, AsRawFd as _};

    let taken = io::stdout().as_fd().try_clone_to_owned()?;
    let discard = std::fs::OpenOptions::new().write(true).open("/dev/null")?;
    // SAFETY: both descriptors are open for the duration of the call.
    if unsafe { libc::dup2(discard.as_raw_fd(), libc::STDOUT_FILENO) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(File::from(taken))
}

/// Starts the current executable's `__service` mode without inheriting host
/// standard-I/O handles, and returns the channel on which the service
/// signals that it answers requests.
///
/// On Windows this uses a detached process group and lets the service
/// inherit only its readiness channel, preventing a durable service from
/// keeping a short-lived hook runner's pipes open.
///
/// # Errors
///
/// Returns a failure to create the channel or the process.
pub fn spawn_service_process(executable: &Path) -> io::Result<ServiceReadiness> {
    let operation = obs::span!(
        INFO,
        "acyclic.runtime.spawn_service",
        outcome = obs::Empty,
        error.kind = obs::Empty
    );
    operation.scope(|| operation.finish(spawn_service_process_unobserved(executable)))
}

fn spawn_service_process_unobserved(executable: &Path) -> io::Result<ServiceReadiness> {
    #[cfg(windows)]
    {
        windows::spawn_service_process(executable).map(ServiceReadiness)
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;

        let mut command = std::process::Command::new(executable);
        command
            .args(["__service", SERVICE_READY_ARGUMENT])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        // A fresh process group keeps the independently durable service outside the caller's
        // process-tree containment so client teardown cannot substitute for service drain.
        command.process_group(0);
        let mut child = command.spawn()?;
        let readiness = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("service readiness channel is missing"))?;
        Ok(ServiceReadiness(File::from(std::os::fd::OwnedFd::from(
            readiness,
        ))))
    }
}

/// Starts a child in an owned operating-system process tree.
///
/// Descendants inherit the Windows Job or Unix process group. Explicit cleanup
/// reports unresolved termination; dropping the owner attempts best-effort
/// cleanup and cannot report failure. Unix
/// descendants can deliberately escape by creating another group or session,
/// so independently durable services require their own lifecycle ownership.
pub fn spawn_process_tree(command: &mut std::process::Command) -> io::Result<ProcessTree> {
    ProcessTree::spawn(command)
}

/// Starts the same owned process tree with the original invocation's meter.
///
/// The caller prepares and preclaims Command argument/environment/directory
/// storage and stream configuration before this call. This adapter borrows it
/// without copying. `Command::spawn` is one actual opaque standard-library SDK
/// attempt: its private allocator, hidden kernel calls and process RSS are not
/// reported as independently observed allocations, I/O or physical bytes.
/// Observable native Job/setup/control calls are admitted individually.
///
/// `custody` must initially be empty. On failure it retains any acquired Job,
/// setup handles and child, including a suspended or unreaped child. The caller
/// must retain that original owner and use explicit measured terminal control;
/// selecting measurement disables hidden unbounded Drop termination.
pub fn spawn_process_tree_with_meter_custody(
    command: &mut std::process::Command, meter: &dyn NativeReadMeter,
    custody: &mut Option<ProcessTree>,
) -> io::Result<()> {
    ProcessTree::spawn_with_meter_custody(command, meter, custody)
}

/// Runs a trusted host command with closed stdin and bounded captured output.
/// Environment and authority policy remain the command builder's responsibility.
/// Cleanup failure takes precedence; timeout never implies effects rolled back.
pub fn process_output(
    command: &mut std::process::Command,
    timeout: std::time::Duration,
    max_bytes: usize,
) -> io::Result<std::process::Output> {
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    spawn_process_tree(command)?.wait_with_output(timeout, max_bytes)
}

/// Runs a trusted host command with its configured streams and a bounded wait.
/// Cleanup errors take precedence over exit observation errors.
pub fn process_status(
    command: &mut std::process::Command,
    timeout: std::time::Duration,
) -> io::Result<std::process::ExitStatus> {
    let mut tree = spawn_process_tree(command)?;
    let result = tree.wait(timeout);
    tree.terminate()?;
    result
}

/// Runtime-independent completion of an owned native-file operation.
/// Once admitted, dropping the observer never cancels the underlying operation.
pub struct NativeCompletion<T> {
    state: Arc<Mutex<CompletionState<T>>>,
    pending: Option<NativeJob>,
    waiter: Option<Arc<CapacityWaiter>>,
    sequencer: Option<FileSequencer>,
    uncertain: Option<Arc<AtomicBool>>,
    predecessor: Option<Arc<OperationFence>>,
    completion: Option<Arc<OperationFence>>,
    terminated: bool,
}

/// Completion of one owned native read batch.
pub type ReadBatch = NativeCompletion<Vec<Bytes>>;

/// Completion of an owned write, resize, sync, or file-control operation.
pub type UnitCompletion = NativeCompletion<()>;

/// An owned native read and its actual retained data-buffer capacity.
pub struct NativeReadResult {
    /// Bytes actually returned, including an ordinary EOF-short prefix.
    pub bytes: Bytes,
    /// Actual retained vector capacity, excluding reference-count headers.
    pub buffer_capacity: usize,
    _allocation: Option<ReadAllocation>,
}

/// A bounded native read on the existing completion engine, or an inline
/// zero-length result with no request, buffer, fence, or wake allocation.
pub struct NativeReadCompletion(NativeReadCompletionState);

enum NativeReadCompletionState {
    Ready(Option<NativeReadResult>),
    Pending(NativeCompletion<NativeReadResult>),
}

impl NativeReadCompletion {
    fn ready(value: NativeReadResult) -> Self {
        Self(NativeReadCompletionState::Ready(Some(value)))
    }

    fn pending(completion: NativeCompletion<NativeReadResult>) -> Self {
        Self(NativeReadCompletionState::Pending(completion))
    }

    /// Waits through the same measured task engine. An inline empty result
    /// needs no waiter and does not create an independent deadline producer.
    pub fn wait_measured(
        self,
        deadline: Option<std::time::Instant>,
        cancellation: impl Future<Output = ()>,
        on_deadline: impl FnOnce(),
    ) -> io::Result<NativeReadResult> {
        match self.0 {
            NativeReadCompletionState::Ready(mut value) =>
                Ok(value.take().expect("native read polled after completion")),
            NativeReadCompletionState::Pending(completion) => {
                let meter = measured_completion_meter(&completion.state)?;
                wait_measured_completion(completion, meter, None, deadline, cancellation, on_deadline)
            }
        }
    }
}

impl Future for NativeReadCompletion {
    type Output = io::Result<NativeReadResult>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.get_mut().0 {
            NativeReadCompletionState::Ready(value) =>
                Poll::Ready(Ok(value.take().expect("native read polled after completion"))),
            NativeReadCompletionState::Pending(completion) =>
                Pin::new(completion).poll(context),
        }
    }
}

/// Filesystem-operation allowance owned by one accepted native request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeOperationKind {
    /// A native read, metadata query, or its completion retrieval.
    Read,
    /// A native write, mutating control, or its completion retrieval.
    Write,
    /// An actual durability attempt, reserved before its accepted commit point.
    /// It consumes the original durability counter, not a fictional write.
    Durability,
}

/// Stack-only custody of this request's unspent operation allowance.
///
/// Credits are withheld from the original invocation, not reported as spent.
/// Reentrant callbacks cannot consume them. Each physical operation consumes
/// exactly one credit; immediate failures/EOF refund only unused credits.
pub struct NativeOperationReservation<'a> {
    meter: &'a dyn NativeReadMeter,
    kind: NativeOperationKind,
    remaining: u64,
}

impl<'a> NativeOperationReservation<'a> {
    /// Admits this request's complete possible native operation sequence.
    pub fn new(
        meter: &'a dyn NativeReadMeter, kind: NativeOperationKind, count: u64,
    ) -> io::Result<Self> {
        meter.reserve_operations(kind, count)?;
        Ok(Self { meter, kind, remaining: count })
    }

    /// Invokes one actual native operation, even if recording its admitted
    /// attempt reports a late error. Accepted completion must still drain.
    pub fn attempt<T>(&mut self, operation: impl FnOnce() -> T) -> (T, io::Result<()>) {
        attempt_reserved_native_operation(self.meter, self.kind, &mut self.remaining, operation)
    }
}

impl Drop for NativeOperationReservation<'_> {
    fn drop(&mut self) {
        self.meter.cancel_reserved_operations(self.kind, self.remaining);
    }
}

fn attempt_reserved_native_operation<T>(
    meter: &dyn NativeReadMeter, kind: NativeOperationKind, remaining: &mut u64,
    operation: impl FnOnce() -> T,
) -> (T, io::Result<()>) {
    *remaining = remaining.checked_sub(1)
        .expect("a physical native operation owns an admitted request credit");
    let recorded = meter.record_reserved_operation(kind);
    (operation(), recorded)
}

/// Transferable custody of already admitted operations on the original meter.
/// The token has no heap allocation and can move into the existing native
/// worker closure. Unused credits alone are refunded on its physical drop.
pub struct OwnedNativeOperationReservation {
    meter: Arc<dyn NativeReadMeter>,
    kind: NativeOperationKind,
    remaining: u64,
}

impl OwnedNativeOperationReservation {
    /// Withholds possible operations without reporting a fabricated attempt.
    pub fn new(
        meter: Arc<dyn NativeReadMeter>, kind: NativeOperationKind, count: u64,
    ) -> io::Result<Self> {
        meter.reserve_operations(kind, count)?;
        Ok(Self { meter, kind, remaining: count })
    }

    /// Runs an already admitted physical operation despite late cancellation.
    /// Its recording error is separate, so it cannot suppress actual dispatch.
    pub fn attempt<T>(&mut self, operation: impl FnOnce() -> T) -> (T, io::Result<()>) {
        attempt_reserved_native_operation(self.meter.as_ref(), self.kind, &mut self.remaining, operation)
    }

    /// Returns this token's unused allowance, not global permission or work.
    #[must_use]
    pub const fn remaining(&self) -> u64 { self.remaining }
}

impl Drop for OwnedNativeOperationReservation {
    fn drop(&mut self) {
        self.meter.cancel_reserved_operations(self.kind, self.remaining);
    }
}


/// Stack-only custody of one request's possible physical byte transfer.
///
/// A live token withholds its maximum from the original invocation. Record
/// the actual transfer after terminal completion, including failed partial
/// transfers; dropping an unconsumed token refunds only its unused allowance.
pub struct NativeByteReservation<'a> {
    meter: &'a dyn NativeReadMeter,
    kind: NativeOperationKind,
    maximum: Option<u64>,
}

impl<'a> NativeByteReservation<'a> {
    /// Reserves the maximum transfer on the original request meter.
    ///
    /// # Errors
    /// Returns the original cancellation, quota, or arithmetic denial before
    /// the caller issues I/O. No allocation or transfer occurs here.
    pub fn new(
        meter: &'a dyn NativeReadMeter, kind: NativeOperationKind, maximum: usize,
    ) -> io::Result<Self> {
        let maximum = maximum as u64;
        meter.reserve_bytes(kind, maximum)?;
        Ok(Self { meter, kind, maximum: Some(maximum) })
    }

    /// Records the exact physical transfer and refunds its unused maximum.
    /// This must run before reporting late cancellation or completion errors.
    pub fn record(&mut self, actual: usize) -> io::Result<()> {
        record_reserved_native_bytes(self.meter, self.kind, &mut self.maximum, actual)
    }
}

impl Drop for NativeByteReservation<'_> {
    fn drop(&mut self) {
        if let Some(maximum) = self.maximum.take() {
            self.meter.cancel_reserved_bytes(self.kind, maximum);
        }
    }
}

fn record_reserved_native_bytes(
    meter: &dyn NativeReadMeter, kind: NativeOperationKind, maximum: &mut Option<u64>,
    actual: usize,
) -> io::Result<()> {
    let maximum = maximum.take().expect("a native byte reservation is consumed exactly once");
    meter.record_reserved_bytes(kind, maximum, actual as u64)
}

/// Transferable physical-byte allowance on the same original native meter.
/// Owned buffers stay separately accountable; this token grants no source
/// authority and neither creates another ledger nor allocates a control.
pub struct OwnedNativeByteReservation {
    meter: Arc<dyn NativeReadMeter>,
    kind: NativeOperationKind,
    maximum: Option<u64>,
}

impl OwnedNativeByteReservation {
    /// Withholds the real possible transfer before accepting an operation.
    pub fn new(
        meter: Arc<dyn NativeReadMeter>, kind: NativeOperationKind, maximum: usize,
    ) -> io::Result<Self> {
        let maximum = maximum as u64;
        meter.reserve_bytes(kind, maximum)?;
        Ok(Self { meter, kind, maximum: Some(maximum) })
    }

    /// Records a terminal physical transfer and refunds unused maximum.
    /// Call before exposing any late cancellation or physical I/O failure.
    pub fn record(&mut self, actual: usize) -> io::Result<()> {
        record_reserved_native_bytes(self.meter.as_ref(), self.kind, &mut self.maximum, actual)
    }

    /// Records a real completed prefix while retaining only its unused credit
    /// for subsequent already-admitted writes in this accepted operation.
    pub fn record_partial(&mut self, actual: usize) -> io::Result<()> {
        let maximum = self.maximum.as_mut().expect("native byte allowance is still owned");
        *maximum = maximum.checked_sub(actual as u64)
            .expect("physical prefix fits this request's owned byte allowance");
        self.meter.record_reserved_bytes(self.kind, actual as u64, actual as u64)
    }
}

impl Drop for OwnedNativeByteReservation {
    fn drop(&mut self) {
        if let Some(maximum) = self.maximum.take() {
            self.meter.cancel_reserved_bytes(self.kind, maximum);
        }
    }
}


/// Invocation-local accounting shared by bounded native I/O.
/// Admission precedes each allocation and each physical operation. Completed
/// bytes are recorded even if cancellation arrives during a blocking syscall.
///
/// Completing already-admitted shared sequencing control is mandatory even
/// after cancellation or exhaustion: settling fences, retiring a current tail,
/// taking existing wakers/links, and iteratively dropping skipped fences are
/// terminal ROOTCONTROL bookkeeping, not namespace/source examined items.
/// This does not exclude SDK I/O, filesystem cleanup effects, namespace visits,
/// or variable payload, path, proof, or cache buffers from invocation accounting.
/// Finite work does not bound opaque waker execution, runtime scheduling, or the
/// variable duration of terminal control draining; it is not a hard wall/CPU limit.
pub trait NativeReadMeter: Send + Sync {
    /// Checks the original request's cancellation and ownership conditions.
    fn check(&self) -> io::Result<()>;
    /// Returns a pre-existing allocation-lifetime owner when this request
    /// meter also retains terminal-only request custody. The returned owner
    /// must share the same invocation ledger, resource allowance, cancellation,
    /// and limits, but must not retain that request's retirement anchor.
    /// This is a pure Arc getter: no allocation, IO, or additional authority.
    fn allocation_owner(&self) -> Option<Arc<dyn NativeReadMeter>> { None }
    /// Withholds a concrete request's possible operations without inventing
    /// spent work; allowance remains unavailable to reentrant callbacks.
    fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> io::Result<()>;
    /// Refunds only a request token's unused withheld allowance.
    fn cancel_reserved_operations(&self, kind: NativeOperationKind, count: u64);
    /// Consumes one request-owned allowance and records its actual attempt,
    /// without cancellation checks that could abandon accepted completion.
    fn record_reserved_operation(&self, kind: NativeOperationKind) -> io::Result<()>;
    /// Preflights write bytes without reporting either bytes or an operation.
    fn admit_write_bytes(&self, maximum: usize) -> io::Result<()>;
    /// Withholds possible physical bytes without reporting a completed transfer.
    fn reserve_bytes(&self, kind: NativeOperationKind, maximum: u64) -> io::Result<()>;
    /// Refunds only this request's unconsumed physical-byte allowance.
    fn cancel_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64);
    /// Restores the request's maximum, then records its exact completed bytes,
    /// without cancellation or proof reconciliation before preserving work.
    fn record_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64, actual: u64) -> io::Result<()>;
    /// Admits and records a real allocation with this logical payload.
    fn allocate(&self, bytes: u64) -> io::Result<()>;
    /// Withholds one future allocation attempt and its known logical payload
    /// before accepting IO that may produce that allocation. No attempt or
    /// retained payload is reported until the reservation is consumed.
    fn reserve_allocation(&self, bytes: u64) -> io::Result<()>;
    /// Refunds only an unconsumed allocation reservation, even after cancellation.
    fn cancel_reserved_allocation(&self, bytes: u64);
    /// Converts the caller's withheld credit into one actual allocation attempt
    /// and retained logical payload immediately before the allocator boundary.
    /// Accepted completion does not repeat cancellation or quota admission.
    fn record_reserved_allocation(&self, bytes: u64) -> io::Result<()>;
    /// Retains the real allocator failure in the original meter's existing
    /// inline fatal-cause slot before returning a nonallocating IO relay.
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> io::Error;
    /// Admits one real reference-count/header allocation with no logical payload.
    fn allocate_refcount_header(&self) -> io::Result<()>;
    /// Retains an already allocated owned payload without another allocation.
    fn adopt(&self, bytes: u64) -> io::Result<()>;
    /// Records known SDK-owned capacity that already exists, before any late
    /// cancellation/budget check. Returns the bytes actually retained even
    /// when reporting an error, so physical-owner guards retire exact claims.
    fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>);
    /// Preflights the real codec byte counters. Unsupported providers do not
    /// silently grant finite codec work.
    fn admit_codec(&self, _encoded: u64, _copied: u64, _hashed: u64) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
    /// Records actual completed codec bytes, including failure prefixes.
    fn record_codec(&self, _encoded: u64, _copied: u64, _hashed: u64) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
    /// Admits and records examined items and actual owned-buffer copies.
    fn examine(&self, items: u64, bytes_copied: u64) -> io::Result<()>;
    /// Preflights examination without reporting work not yet performed.
    fn admit_examination(&self, items: u64, bytes_copied: u64) -> io::Result<()>;
    /// Records examination already admitted by the preceding preflight.
    fn record_examination(&self, items: u64, bytes_copied: u64) -> io::Result<()>;
    /// Retires owned capacity after its actual buffer is freed, even if cancelled.
    fn release(&self, bytes: u64);
    /// Preflights the requested read-byte allowance without reporting completion.
    fn admit_bytes(&self, maximum: usize) -> io::Result<()>;
    /// Admits one read attempt and its maximum byte transfer before dispatch.
    fn admit_read(&self, maximum: usize) -> io::Result<()>;
    /// Preflights additional read attempts before their native boundary.
    fn admit_operations(&self, additional: u64) -> io::Result<()>;
    /// Records the number of native read attempts actually made.
    fn record_operations(&self, completed: u64) -> io::Result<()>;
    /// Records bytes actually returned by the native read.
    fn record_read(&self, bytes: usize) -> io::Result<()>;
    /// Checks the completed logical read before constructing an owned chunk.
    fn finish_read(&self, _requested: usize, _completed: usize) -> io::Result<()> {
        self.check()
    }
    /// Admits one write attempt and its maximum byte transfer before dispatch.
    fn admit_write(&self, maximum: usize) -> io::Result<()>;
    /// Records bytes actually accepted by the native write.
    fn record_write(&self, bytes: usize) -> io::Result<()>;
    /// Preflights additional write attempts before their native boundary.
    fn admit_write_operations(&self, additional: u64) -> io::Result<()>;
    /// Records the number of native write attempts actually made.
    fn record_write_operations(&self, completed: u64) -> io::Result<()>;
    /// Admits and records one actual durability attempt before dispatch.
    fn admit_durability(&self) -> io::Result<()>;
    /// Reserves one mandatory native cleanup attempt before acquiring custody.
    /// Its token owns one withheld allowance, independent of other reservations.
    fn reserve_cleanup_write(&self) -> io::Result<u64> {
        Err(io::ErrorKind::Unsupported.into())
    }
    /// Cancels an unused reservation; unsupported meters cannot create one.
    fn cancel_cleanup_write(&self, _reservation: u64) {
        unreachable!("this meter cannot create native cleanup reservations");
    }
    /// Consumes a reservation without checking cancellation. The caller must
    /// invoke the actual cleanup even if recording its admitted attempt fails.
    fn record_cleanup_write(&self, _reservation: u64) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
}

/// One inline, original-meter allocation credit prepared before accepted IO.
///
/// This reserves quota, not resource access. Consume immediately before the
/// allocator call. Unused credits retire without fabricating spent work.
#[must_use]
pub struct ReadAllocationReservation {
    meter: Arc<dyn NativeReadMeter>,
    bytes: u64,
    pending: bool,
}

impl ReadAllocationReservation {
    /// Withholds one known positive allocation without allocating this token.
    pub fn new(meter: &Arc<dyn NativeReadMeter>, bytes: u64) -> io::Result<Self> {
        meter.reserve_allocation(bytes)?;
        Ok(Self { meter: Arc::clone(meter), bytes, pending: true })
    }

    /// Records the accepted allocation and transfers its exact original custody.
    /// This allocates nothing and does not readmit already accepted completion.
    pub fn allocate(mut self) -> io::Result<ReadAllocation> {
        self.meter.record_reserved_allocation(self.bytes)?;
        self.pending = false;
        Ok(ReadAllocation {
            meter: self.meter.allocation_owner().unwrap_or_else(|| Arc::clone(&self.meter)),
            bytes: self.bytes,
        })
    }
}

impl Drop for ReadAllocationReservation {
    fn drop(&mut self) {
        if self.pending { self.meter.cancel_reserved_allocation(self.bytes); }
    }
}

/// Custody of logical SDK-owned payload on the original invocation ledger.
///
/// Keep this guard after its physical payload in an owner struct, so payload
/// destruction precedes release. Transfers and aggregation allocate nothing.
#[must_use]
pub struct ReadAllocation {
    meter: Arc<dyn NativeReadMeter>,
    bytes: u64,
}

impl ReadAllocation {
    /// Admits one genuine positive allocation attempt before its allocator call.
    pub fn new(meter: &Arc<dyn NativeReadMeter>, bytes: usize) -> io::Result<Self> {
        let bytes = bytes as u64;
        if bytes == 0 { meter.check()?; } else { meter.allocate(bytes)?; }
        Ok(Self { meter: meter.allocation_owner().unwrap_or_else(|| Arc::clone(meter)), bytes })
    }

    /// Borrows the same original meter without an allocation or resource grant.
    pub fn empty(meter: &Arc<dyn NativeReadMeter>) -> Self {
        Self { meter: meter.allocation_owner().unwrap_or_else(|| Arc::clone(meter)), bytes: 0 }
    }

    /// Admits retention of an existing transferred payload, with zero
    /// allocation operations. No new allocator call is represented here.
    pub fn adopt(meter: &Arc<dyn NativeReadMeter>, bytes: usize) -> io::Result<Self> {
        let bytes = bytes as u64;
        if bytes == 0 { meter.check()?; } else { meter.adopt(bytes)?; }
        Ok(Self { meter: meter.allocation_owner().unwrap_or_else(|| Arc::clone(meter)), bytes })
    }

    /// Admits another genuine positive allocation attempt before it occurs.
    pub fn grow(&mut self, additional: usize) -> io::Result<()> {
        let additional = additional as u64;
        let bytes = self.bytes.checked_add(additional)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        if additional != 0 { self.meter.allocate(additional)?; }
        self.bytes = bytes;
        Ok(())
    }

    /// Admits logical retention before an allocation/transfer, without another
    /// allocation operation. A decrease requires prior physical retirement.
    pub fn resize_retained(&mut self, actual: usize) -> io::Result<()> {
        let actual = actual as u64;
        if actual > self.bytes {
            self.meter.adopt(actual - self.bytes)?;
        } else if actual < self.bytes {
            self.meter.release(self.bytes - actual);
        }
        self.bytes = actual;
        Ok(())
    }

    /// Records real capacity discovered after a controlled allocation.
    /// Actual excess remains in this guard even if a late quota check fails.
    /// A decrease requires that the physical capacity was already retired.
    pub fn reconcile_capacity(&mut self, actual: usize) -> io::Result<()> {
        Self::reconcile_capacity_recorded(&mut self.bytes, &self.meter, actual, self.meter.as_ref())
    }

    /// Reconciles an already allocated buffer through its accepted completion
    /// meter. The completion must project this exact original allocation owner;
    /// this identity check does not grant new allocation or source authority.
    /// Actual retained excess stays guarded even on a late quota failure.
    pub fn reconcile_capacity_with_meter(
        &mut self, actual: usize, completion: &dyn NativeReadMeter,
    ) -> io::Result<()> {
        let owner = completion.allocation_owner().ok_or(io::ErrorKind::InvalidInput)?;
        if !Arc::ptr_eq(&owner, &self.meter) { return Err(io::ErrorKind::InvalidInput.into()); }
        Self::reconcile_capacity_recorded(&mut self.bytes, &self.meter, actual, completion)
    }

    fn reconcile_capacity_recorded(
        bytes: &mut u64, original: &Arc<dyn NativeReadMeter>,
        actual: usize, completion: &dyn NativeReadMeter,
    ) -> io::Result<()> {
        let actual = actual as u64;
        if actual > *bytes {
            let additional = actual - *bytes;
            let (retained, result) = completion.record_completed_adoption(additional);
            assert!(retained <= additional, "completed adoption retains only actual excess");
            *bytes += retained;
            result?;
            assert_eq!(retained, additional, "successful adoption retains all actual excess");
        } else if actual < *bytes {
            original.release(*bytes - actual);
            *bytes = actual;
        }
        Ok(())
    }

    /// Logical capacity currently owned by this guard.
    pub const fn bytes(&self) -> u64 { self.bytes }

    /// The original shared invocation, not a newly constructed ledger.
    pub const fn meter(&self) -> &Arc<dyn NativeReadMeter> { &self.meter }

    /// Transfers an already-owned claim to its exact original meter owner.
    /// No admission, cancellation check, release or new allocation occurs.
    /// A mismatch returns the original guard intact.
    pub fn consume_same_meter_owned_bytes(
        mut self, expected: &Arc<dyn NativeReadMeter>,
    ) -> Result<u64, Self> {
        if !Arc::ptr_eq(&self.meter, expected) { return Err(self); }
        let bytes = self.bytes;
        self.bytes = 0;
        Ok(bytes)
    }

    /// Moves another same-ledger claim without allocating or releasing it.
    /// Both guards remain unchanged on identity mismatch or overflow.
    pub fn absorb(&mut self, other: &mut Self) -> io::Result<()> {
        if !Arc::ptr_eq(&self.meter, &other.meter) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let bytes = self.bytes.checked_add(other.bytes)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.bytes = bytes;
        other.bytes = 0;
        Ok(())
    }

    /// Splits custody already owned here, with no new quota grant or allocation.
    pub fn split(&mut self, bytes: usize) -> io::Result<Self> {
        let bytes = bytes as u64;
        let remaining = self.bytes.checked_sub(bytes)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.bytes = remaining;
        Ok(Self { meter: Arc::clone(&self.meter), bytes })
    }

    /// Releases only physically retired payload owned by this guard.
    pub fn release(&mut self, bytes: usize) -> io::Result<()> {
        let bytes = bytes as u64;
        let remaining = self.bytes.checked_sub(bytes)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.meter.release(bytes);
        self.bytes = remaining;
        Ok(())
    }
}

impl std::fmt::Debug for ReadAllocation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ReadAllocation").field("bytes", &self.bytes).finish_non_exhaustive()
    }
}

impl Drop for ReadAllocation {
    fn drop(&mut self) {
        self.meter.release(self.bytes);
    }
}

fn arc_allocation_bytes<T>() -> usize {
    // Logical SDK-owned payload; allocator and reference-count headers are
    // outside the canonical allocation ledger's byte policy.
    std::mem::size_of::<T>()
}

/// Runtime-independent bounded offload for host operations without a native
/// completion path, such as file locks and directory namespace transactions.
/// Dropping the observer after admission does not cancel the operation.
/// A panicking operation first unwinds its own locals, then the observing poll
/// resumes that same panic payload; it is not converted into an I/O error.
/// An unobserved terminal payload is dropped with the completion state;
/// dropping the observer never joins or drains an accepted operation.
pub struct BlockingIoTask<T> {
    state: Arc<Mutex<CompletionState<T>>>,
    pending: Option<NativeJob>,
    waiter: Option<Arc<CapacityWaiter>>,
    terminated: bool,
    prepared_wake: Option<Arc<BlockingWaitWake>>,
}

thread_local! {
    static INLINE_BLOCKING: Cell<bool> = const { Cell::new(false) };
}

/// Marks the current thread, while the guard lives, as one whose blocking
/// stalls only the operation it is running, such as a native filesystem
/// callback thread polling its own request. Host operations started from it
/// run on it directly instead of hopping to a worker thread and back.
pub struct InlineBlocking {
    previous: bool,
}

impl InlineBlocking {
    /// Admits inline host operations on this thread until the guard drops.
    #[must_use]
    pub fn enter() -> Self {
        Self {
            previous: INLINE_BLOCKING.replace(true),
        }
    }
}

impl Drop for InlineBlocking {
    fn drop(&mut self) {
        INLINE_BLOCKING.set(self.previous);
    }
}

/// Whether host operations started on this thread may block it directly.
#[must_use]
pub fn inline_blocking_allowed() -> bool {
    INLINE_BLOCKING.get()
}

/// Schedules one owned host operation on the bounded native worker pool, or
/// runs it in place on a thread that admits inline blocking.
#[must_use]
pub fn run_blocking_io<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
) -> BlockingIoTask<T> {
    blocking_io_task(operation, None).expect("unmetered task construction cannot fail")
}

/// Uses the same host-operation queue with admission for the actual owned
/// completion state, closure, and any capacity-waiter node.
pub fn run_blocking_io_measured<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
    meter: Arc<dyn NativeReadMeter>,
) -> io::Result<BlockingIoTask<T>> {
    blocking_io_task(operation, Some(meter))
}

/// Prepares one operation on the existing host-operation engine without
/// submitting or invoking it. Services must already be initialized.
///
/// Completion, exact closure storage, an unlinked capacity waiter, and the
/// synchronous wake owner are all allocated now. If `meter` projects an
/// original `allocation_owner`, those controls are admitted on that original
/// meter; submission and execution retain the supplied operative meter.
/// The caller must supply an authentic owned reservation for later cleanup,
/// not a borrowed loan or a replacement operation allowance.
///
/// First polling submits the same one-shot task. Observer drop after
/// submission still detaches rather than draining or undoing accepted work.
pub fn prepare_blocking_io_measured<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
    meter: Arc<dyn NativeReadMeter>,
) -> io::Result<BlockingIoTask<T>> {
    prepare_blocking_io_task(operation, meter)
}

fn blocking_io_task<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
    meter: Option<Arc<dyn NativeReadMeter>>,
) -> io::Result<BlockingIoTask<T>> {
    blocking_io_task_with_allocation_meter(operation, meter, None)
}

fn blocking_io_task_with_allocation_meter<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
    meter: Option<Arc<dyn NativeReadMeter>>,
    allocation_meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<BlockingIoTask<T>> {
    let allocation_meter = allocation_meter.or(meter.as_ref());
    if let Some(meter) = &meter { meter.check()?; }
    let allocation = allocation_meter.map(|meter| ReadAllocation::new(
        meter, arc_allocation_bytes::<Mutex<CompletionState<T>>>(),
    )).transpose()?;
    let state = Arc::new(Mutex::new(CompletionState {
        result: None, panic: None, waker: None, _allocation: allocation,
    }));
    let completion = Arc::clone(&state);
    let span = obs::span!(
        DEBUG, "acyclic.runtime.blocking_io",
        outcome = obs::Empty, error.kind = obs::Empty,
    );
    let observer = meter.clone();
    let operation = move || span.scope(|| {
        let admission = observer.as_ref().map_or(Ok(()), |meter| meter.check());
        if let Err(error) = admission {
            let result = Err(error);
            span.record_result(&result);
            return finish_job(&completion, result, None);
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation)) {
            Ok(value) => {
                let result = Ok(value);
                span.record_result(&result);
                finish_job(&completion, result, None)
            }
            Err(payload) => finish_panicked_job(&completion, payload, None),
        }
    });
    let allocation = allocation_meter.map(|meter| ReadAllocation::new(
        meter, std::mem::size_of_val(&operation),
    )).transpose()?;
    Ok(BlockingIoTask {
        pending: Some(NativeJob::Task {
            operation: Box::new(operation), meter, _allocation: allocation,
        }),
        state, waiter: None, terminated: false, prepared_wake: None,
    })
}

fn prepare_blocking_io_task<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
    meter: Arc<dyn NativeReadMeter>,
) -> io::Result<BlockingIoTask<T>> {
    // Preparation never starts services, queues a request, or invokes its
    // producer. All four known heap controls belong to this same original
    // meter before the caller starts a driver that may require retirement.
    ready_workers(&BLOCKING_WORKERS)?;
    let allocation_meter = meter.allocation_owner().unwrap_or_else(|| Arc::clone(&meter));
    let mut task = blocking_io_task_with_allocation_meter(
        operation, Some(Arc::clone(&meter)), Some(&allocation_meter),
    )?;
    task.waiter = Some(allocate_capacity_waiter(Some(&allocation_meter))?);
    task.prepared_wake = Some(allocate_blocking_wait_wake(&allocation_meter)?);
    Ok(task)
}

#[derive(Default)]
struct OperationFence {
    state: Mutex<OperationFenceState>,
    settled: Condvar,
    sequencer: Option<Weak<Mutex<Option<Arc<OperationFence>>>>>,
    _allocation: Option<ReadAllocation>,
}

#[derive(Default)]
struct OperationFenceState {
    complete: bool,
    successor: Option<Waker>,
    skipped_successor: Option<Arc<OperationFence>>,
}

impl OperationFence {
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    fn wait_until_complete(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.complete {
            tracing::trace!(name: "acyclic.runtime.fence_wait", "waiting for an earlier operation");
        }
        while !state.complete {
            state = self
                .settled
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    #[cfg(all(test, any(windows, target_os = "linux")))]
    fn is_complete(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .complete
    }

    fn complete_after(self: &Arc<Self>, predecessor: Option<Arc<Self>>) {
        let Some(predecessor) = predecessor else {
            self.complete();
            return;
        };
        let mut state = predecessor
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.complete {
            drop(state);
            self.complete();
        } else {
            // The waiting future has been dropped; its old task must not be
            // woken when the predecessor finishes.
            state.successor = None;
            state.skipped_successor = Some(Arc::clone(self));
        }
    }

    fn ready_or_register(&self, waker: &Waker) -> bool {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.complete {
            true
        } else {
            state.successor = Some(waker.clone());
            false
        }
    }

    // No cancellation check or new admission may interrupt this already-
    // admitted control drain. Keep it iterative and drop request custody only
    // outside fence-state and sequencer mutexes.
    fn complete(&self) {
        let mut next: Option<Arc<Self>> = None;
        loop {
            let current = next.as_deref().unwrap_or(self);
            let mut state = current
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.complete = true;
            let successor = state.successor.take();
            let following = state.skipped_successor.take();
            drop(state);
            // A completed tail is no longer an admission predecessor. Remove
            // only this fence: a concurrently reserved successor owns its own
            // tail and still retains any predecessor needed for FIFO ordering.
            // Retiring the actual root alias also retires its request meter.
            if let Some(sequencer) = current.sequencer.as_ref().and_then(Weak::upgrade) {
                let retired = {
                    let mut tail = sequencer.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    if tail.as_ref().is_some_and(|tail| std::ptr::eq(Arc::as_ptr(tail), current)) {
                        tail.take()
                    } else {
                        None
                    }
                };
                drop(retired);
            }
            current.settled.notify_all();
            wake_capacity_waiter(successor);
            let Some(following) = following else {
                break;
            };
            next = Some(following);
        }
    }
}

struct CompletionState<T> {
    result: Option<io::Result<T>>,
    panic: Option<Box<dyn std::any::Any + Send>>,
    waker: Option<Waker>,
    _allocation: Option<ReadAllocation>,
}

/// The only authority to finish an admitted file operation. Keeping the
/// fence and observer state together prevents a backend from publishing the
/// fence before it has reached a terminal native completion.
struct FileCompletion<T> {
    state: Arc<Mutex<CompletionState<T>>>,
    fence: Option<Arc<OperationFence>>,
    uncertain: Option<Arc<AtomicBool>>,
    _tail: Option<FileSequencer>,
    file: Option<Arc<NativeFilePayload>>,
    span: obs::OperationSpan,
    finished: bool,
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    _health: Option<Arc<FileHealth>>,
}

impl<T> FileCompletion<T> {
    fn new(
        state: Arc<Mutex<CompletionState<T>>>,
        fence: Option<Arc<OperationFence>>,
        uncertain: Option<Arc<AtomicBool>>,
        tail: Option<FileSequencer>,
        span: obs::OperationSpan,
    ) -> Self {
        Self {
            state,
            fence,
            uncertain,
            _tail: tail,
            file: None,
            span,
            finished: false,
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            _health: None,
        }
    }

    fn for_file(mut self, file: Arc<NativeFilePayload>) -> Self {
        self.file = Some(file);
        self
    }

    fn for_health(mut self,
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        health: Option<Arc<FileHealth>>,
    ) -> Self {
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        { self._health = health; }
        self
    }

    fn finish(mut self, result: io::Result<T>) -> Option<Waker> {
        let Self {
            state,
            fence,
            uncertain,
            file,
            span,
            finished,
            ..
        } = &mut self;
        span.scope(|| {
            span.record_result(&result);
            #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
            if result.as_ref().err().is_some_and(is_uncertain_io_error)
                && let (Some(file), Some(uncertain)) = (file.as_ref(), uncertain.as_ref())
            {
                poison_file_health(file, uncertain);
            }
            // Health/quarantine decisions need the real handle; once they are
            // settled, retire this accepted request's alias before publishing
            // Ready so its caller cannot retire logical custody prematurely.
            drop(file.take());
            let waker = finish_file_job(state, result, fence.take(), uncertain.as_deref());
            *finished = true;
            waker
        })
    }
}

impl<T> Drop for FileCompletion<T> {
    fn drop(&mut self) {
        if !self.finished {
            // A backend must never abandon an admitted operation: its native
            // request could still own buffers and the next file operation
            // cannot safely cross this fence.
            std::process::abort();
        }
    }
}

#[cfg(any(windows, target_os = "linux"))]
fn submit_file_io<T: Send + 'static>(
    completion: FileCompletion<T>,
    submit: impl FnOnce(Box<dyn FnOnce(io::Result<T>) + Send>) -> io::Result<()>,
) -> Option<Waker> {
    // A platform's Err means it accepted no I/O. This cell leaves the common
    // layer holding the completion authority until that decision is known;
    // Ok transfers it to the callback. Dropping an accepted callback without
    // terminal completion drops FileCompletion and fails stop.
    let handoff = Arc::new(Mutex::new(Some(completion)));
    let callback_handoff = Arc::clone(&handoff);
    let callback = Box::new(move |result| {
        let completion = callback_handoff
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| std::process::abort());
        wake_capacity_waiter(completion.finish(result));
    });
    match submit(callback) {
        Ok(()) => None,
        Err(error) => handoff
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| std::process::abort())
            .finish(Err(error)),
    }
}

enum NativeJob {
    Read {
        file: Arc<NativeFilePayload>,
        reads: Vec<OwnedRead>,
        span: obs::OperationSpan,
        #[cfg(windows)]
        overlapped: bool,
        tail: Option<FileSequencer>,
        uncertain: Option<Arc<AtomicBool>>,
        state: Arc<Mutex<CompletionState<Vec<Bytes>>>>,
        completion: Option<Arc<OperationFence>>,
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        _health: Option<Arc<FileHealth>>,
    },
    BoundedRead {
        file: Arc<NativeFilePayload>,
        #[cfg(windows)]
        overlapped: bool,
        offset: u64,
        length: usize,
        require_full: bool,
        meter: Arc<dyn NativeReadMeter>,
        state: Arc<Mutex<CompletionState<NativeReadResult>>>,
        completion: Option<Arc<OperationFence>>,
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        _health: Option<Arc<FileHealth>>,
    },
    PreparedRead {
        file: Arc<NativeFilePayload>,
        operation: Box<dyn FnOnce(
            Arc<NativeFilePayload>, Arc<dyn NativeReadMeter>, Option<Arc<OperationFence>>,
        ) -> Option<Waker> + Send>,
        meter: Arc<dyn NativeReadMeter>,
        completion: Option<Arc<OperationFence>>,
        _allocation: ReadAllocation,
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        _health: Option<Arc<FileHealth>>,
    },
    BoundedUnit {
        file: Arc<NativeFilePayload>,
        operation: NativeBoundedUnitOperation,
        meter: Arc<dyn NativeReadMeter>,
        state: Arc<Mutex<CompletionState<()>>>,
        completion: Option<Arc<OperationFence>>,
        tail: Option<FileSequencer>,
        uncertain: Option<Arc<AtomicBool>>,
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        _health: Option<Arc<FileHealth>>,
    },
    Unit {
        file: Arc<NativeFilePayload>,
        operation: NativeUnitOperation,
        span: obs::OperationSpan,
        #[cfg(windows)]
        overlapped: bool,
        tail: Option<FileSequencer>,
        uncertain: Option<Arc<AtomicBool>>,
        state: Arc<Mutex<CompletionState<()>>>,
        completion: Option<Arc<OperationFence>>,
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        _health: Option<Arc<FileHealth>>,
    },
    Task {
        operation: Box<dyn FnOnce() -> Option<Waker> + Send>,
        meter: Option<Arc<dyn NativeReadMeter>>,
        _allocation: Option<ReadAllocation>,
    },
}

enum NativeUnitOperation {
    Write(Vec<OwnedWrite>),
    SetLen(u64),
    Sync(Durability),
    Control(FileControl),
}

type FileControl = Box<dyn FnOnce(&File) -> io::Result<()> + Send>;

// Field order is the physical payload-before-original-ledger-custody contract.
struct NativeOwnedWrite<B, C> {
    bytes: B,
    _custody: C,
}

enum NativeBoundedUnitOperation {
    Write { offset: u64, bytes: Bytes },
    SetLen(u64),
    Sync(Durability),
    Control {
        operation: Box<dyn FnOnce(&File, &dyn NativeReadMeter) -> io::Result<()> + Send>,
        _allocation: ReadAllocation,
    },
}

impl NativeJob {
    fn set_completion(&mut self, fence: Arc<OperationFence>) {
        match self {
            Self::Read { completion, .. } | Self::BoundedRead { completion, .. }
            | Self::PreparedRead { completion, .. } | Self::BoundedUnit { completion, .. }
            | Self::Unit { completion, .. } => {
                *completion = Some(fence);
            }
            Self::Task { .. } => unreachable!("blocking tasks are not file-sequenced"),
        }
    }

    fn meter(&self) -> Option<&Arc<dyn NativeReadMeter>> {
        match self {
            Self::BoundedRead { meter, .. } | Self::PreparedRead { meter, .. }
            | Self::BoundedUnit { meter, .. } => Some(meter),
            Self::Task { meter, .. } => meter.as_ref(),
            _ => None,
        }
    }

    /// Whether this job may run on a thread that admits inline blocking:
    /// a positional read or write on an overlapped Windows handle, which the
    /// thread can issue and wait for itself without the driver.
    #[cfg(windows)]
    const fn runs_in_place(&self) -> bool {
        matches!(
            self,
            Self::Read {
                overlapped: true,
                ..
            } | Self::BoundedRead { .. } | Self::PreparedRead { .. } | Self::BoundedUnit { .. } | Self::Unit {
                overlapped: true,
                operation: NativeUnitOperation::Write(_),
                ..
            }
        )
    }

    /// Runs a job [`Self::runs_in_place`] admits on the calling thread,
    /// completing its fence exactly as the driver's completion would.
    #[cfg(windows)]
    fn run_in_place(self) -> Option<Waker> {
        match self {
            Self::Read {
                file,
                reads,
                span,
                overlapped: true,
                tail,
                uncertain,
                state,
                completion,
                _health,
            } => span.scope(|| {
                FileCompletion::new(state, completion, uncertain, tail, span.clone())
                    .for_health(_health)
                    .for_file(Arc::clone(&file))
                    .finish(windows::read_batch_in_place(&file, &reads))
            }),
            Self::Unit {
                file,
                operation: NativeUnitOperation::Write(writes),
                span,
                overlapped: true,
                tail,
                uncertain,
                state,
                completion,
                _health,
            } => span.scope(|| {
                let finish = FileCompletion::new(state, completion, uncertain, tail, span.clone())
                    .for_health(_health)
                    .for_file(Arc::clone(&file));
                let result = validate_write_batch(&writes)
                    .and_then(|()| windows::write_batch_in_place(&file, &writes));
                finish.finish(result)
            }),
            job => job.run(),
        }
    }

    fn run(self) -> Option<Waker> {
        match self {
            Self::PreparedRead { file, operation, meter, completion, _allocation,
                #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                _health,
            } => {
                #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                let _held_health = _health;
                let wake = operation(file, meter, completion);
                drop(_allocation);
                wake
            }
            Self::BoundedRead { file, offset, length, require_full, meter, state, completion,
                #[cfg(windows)]
                overlapped,
                #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                _health,
            } => {
                #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                let _held_health = _health;
                let result = read_bounded_in_place(
                    &file, offset, length, require_full, &meter,
                    #[cfg(windows)]
                    overlapped,
                );
                drop(file);
                finish_job(&state, result, completion)
            }
            Self::BoundedUnit { file, operation, meter, state, completion, tail, uncertain,
                #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                _health,
            } => {
                let finish = FileCompletion::new(
                    state, completion, uncertain, tail.clone(),
                    obs::file_span!(DEBUG, "acyclic.runtime.bounded_unit"),
                ).for_health(
                    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                    _health,
                ).for_file(Arc::clone(&file));
                FileScope::with(tail.as_ref(), || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        meter.check()?;
                        match operation {
                            NativeBoundedUnitOperation::Write { offset, bytes } =>
                                write_bounded_in_place(&file, offset, &bytes, meter.as_ref()),
                            NativeBoundedUnitOperation::SetLen(size) =>
                                set_len_measured(&file, size, meter.as_ref()),
                            NativeBoundedUnitOperation::Sync(durability) =>
                                sync_file_measured(&file, durability, meter.as_ref()),
                            NativeBoundedUnitOperation::Control { operation, _allocation } =>
                                operation(&file, meter.as_ref()),
                        }
                    })).unwrap_or_else(|_| std::process::abort());
                    // The completion keeps its own handle until health/quarantine
                    // decisions finish. Retire this operation alias before Ready.
                    drop(file);
                    finish.finish(result)
                })
            }
            Self::Read {
                file,
                reads,
                span,
                #[cfg(windows)]
                overlapped,
                tail,
                uncertain,
                state,
                completion,
                #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                _health,
            } => span.scope(|| {
                let finish = FileCompletion::new(state, completion, uncertain, tail, span.clone())
                    .for_health(#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))] _health)
                    .for_file(Arc::clone(&file));
                #[cfg(target_os = "linux")]
                return submit_file_io(finish, |callback| {
                    linux::submit_read(file, reads, callback)
                });
                #[cfg(windows)]
                return submit_file_io(finish, |callback| {
                    windows::submit_read(file, overlapped, reads, callback)
                });
                #[cfg(not(any(windows, target_os = "linux")))]
                {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        read_batch_impl(&file, &reads)
                    }))
                    // An unwind can happen after native submission, while the OS
                    // still owns an OVERLAPPED or io_uring buffer. Converting it
                    // to an ordinary error would release those allocations and
                    // publish the file fence before terminal completion.
                    .unwrap_or_else(|_| std::process::abort());
                    finish.finish(result)
                }
            }),
            Self::Unit {
                file,
                operation,
                span,
                #[cfg(windows)]
                overlapped,
                tail,
                uncertain,
                state,
                completion,
                #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                _health,
            } => span.scope(|| {
                let finish =
                    FileCompletion::new(state, completion, uncertain, tail.clone(), span.clone())
                        .for_health(#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))] _health)
                        .for_file(Arc::clone(&file));
                #[cfg(any(windows, target_os = "linux"))]
                let operation = match operation {
                    NativeUnitOperation::Write(writes) => {
                        if let Err(error) = validate_write_batch(&writes) {
                            return finish.finish(Err(error));
                        }
                        #[cfg(target_os = "linux")]
                        return submit_file_io(finish, |callback| {
                            linux::submit_write(file, writes, callback)
                        });
                        #[cfg(windows)]
                        return submit_file_io(finish, |callback| {
                            windows::submit_write(file, overlapped, writes, callback)
                        });
                    }
                    operation => operation,
                };
                FileScope::with(tail.as_ref(), || {
                    let result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match operation {
                            #[cfg(not(any(windows, target_os = "linux")))]
                            NativeUnitOperation::Write(writes) => write_all_batch_owned(&file, writes),
                            #[cfg(any(windows, target_os = "linux"))]
                            NativeUnitOperation::Write(_) => {
                                unreachable!("native writes are dispatched before bounded controls")
                            }
                            NativeUnitOperation::SetLen(size) => file.set_len(size),
                            NativeUnitOperation::Sync(durability) => sync_file(&file, durability),
                            NativeUnitOperation::Control(operation) => operation(&file),
                        }))
                        // A write may already be executing in the kernel when
                        // Rust unwinds. Fail-stop preserves buffer and fence
                        // soundness; durable service recovery can restart later.
                        .unwrap_or_else(|_| std::process::abort());
                    finish.finish(result)
                })
            }),
            Self::Task { operation, _allocation, .. } => {
                let wake = operation();
                drop(_allocation);
                wake
            }
        }
    }
}

fn finish_file_job<T>(
    state: &Mutex<CompletionState<T>>,
    result: io::Result<T>,
    completion: Option<Arc<OperationFence>>,
    uncertain: Option<&AtomicBool>,
) -> Option<Waker> {
    if result.as_ref().err().is_some_and(is_uncertain_io_error)
        && let Some(uncertain) = uncertain
    {
        uncertain.store(true, Ordering::Release);
    }
    finish_job(state, result, completion)
}

fn finish_job<T>(
    state: &Mutex<CompletionState<T>>,
    result: io::Result<T>,
    completion: Option<Arc<OperationFence>>,
) -> Option<Waker> {
    let mut state = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.result = Some(result);
    let waker = state.waker.take();
    drop(state);
    if let Some(completion) = completion {
        completion.complete();
    }
    waker
}

fn finish_panicked_job<T>(
    state: &Mutex<CompletionState<T>>,
    payload: Box<dyn std::any::Any + Send>,
    completion: Option<Arc<OperationFence>>,
) -> Option<Waker> {
    let mut state = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    state.panic = Some(payload);
    let waker = state.waker.take();
    drop(state);
    if let Some(completion) = completion {
        completion.complete();
    }
    // The same worker wakes outside the state lock, after retiring its job.
    waker
}

struct NativeWorkers {
    sender: mpsc::SyncSender<NativeJob>,
    admission: Arc<Mutex<AdmissionState>>,
}

struct AdmissionState {
    head: Option<Arc<CapacityWaiter>>,
    tail: Weak<CapacityWaiter>,
    waiter_count: usize,
    queued_jobs: usize,
}

struct CapacityWaiter {
    state: Mutex<CapacityWaiterState>,
    _allocation: Option<ReadAllocation>,
}

struct CapacityWaiterState {
    previous: Weak<CapacityWaiter>,
    next: Option<Arc<CapacityWaiter>>,
    waker: Waker,
    queued: bool,
}

impl AdmissionState {
    fn new() -> Self {
        Self { head: None, tail: Weak::new(), waiter_count: 0, queued_jobs: 0 }
    }

    fn register(
        &mut self,
        waker: &Waker,
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> io::Result<Arc<CapacityWaiter>> {
        let node = allocate_capacity_waiter(meter)?;
        self.register_existing(&node, waker);
        Ok(node)
    }

    fn register_existing(&mut self, node: &Arc<CapacityWaiter>, waker: &Waker) {
        {
            let mut state = node.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            debug_assert!(!state.queued);
            state.previous = self.tail.clone();
            state.next = None;
            state.waker.clone_from(waker);
            state.queued = true;
        }
        if let Some(tail) = self.tail.upgrade() {
            tail.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                .next = Some(Arc::clone(node));
        } else {
            self.head = Some(Arc::clone(node));
        }
        self.tail = Arc::downgrade(node);
        self.waiter_count += 1;
    }

    fn retire(&mut self, node: &Arc<CapacityWaiter>) -> Option<Waker> {
        let mut state = node.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.queued { return None; }
        state.queued = false;
        let previous = state.previous.upgrade();
        let next = state.next.clone();
        if let Some(previous) = &previous {
            previous.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                .next = next.clone();
        } else {
            self.head = next.clone();
        }
        if let Some(next) = next {
            next.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                .previous = previous.as_ref().map_or_else(Weak::new, Arc::downgrade);
        } else {
            self.tail = previous.as_ref().map_or_else(Weak::new, Arc::downgrade);
        }
        self.waiter_count -= 1;
        // Preserve the removed node's next link for an already-running wake
        // snapshot. It is no longer reachable from the active queue.
        drop(state);
        if previous.is_none() {
            self.head.as_ref().map(|head| head.state.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner).waker.clone())
        } else { None }
    }
}

fn allocate_capacity_waiter(
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<Arc<CapacityWaiter>> {
    let allocation = meter.map(|meter| ReadAllocation::new(
        meter, arc_allocation_bytes::<CapacityWaiter>(),
    )).transpose()?;
    Ok(Arc::new(CapacityWaiter {
        state: Mutex::new(CapacityWaiterState {
            previous: Weak::new(), next: None, waker: Waker::noop().clone(), queued: false,
        }),
        _allocation: allocation,
    }))
}

impl NativeCompletion<Vec<Bytes>> {
    fn submit(
        file: Arc<NativeFilePayload>,
        reads: Vec<OwnedRead>,
        sequencer: Option<FileSequencer>,
        uncertain: Option<Arc<AtomicBool>>,
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        health: Option<Arc<FileHealth>>,
        span: obs::OperationSpan,
        #[cfg(windows)] overlapped: bool,
    ) -> Self {
        let span = span.sized(reads.len(), || reads.iter().map(|read| read.length).sum());
        let state = Arc::new(Mutex::new(CompletionState {
            result: None,
            panic: None,
            waker: None,
            _allocation: None,
        }));
        Self {
            pending: Some(NativeJob::Read {
                file,
                reads,
                span,
                #[cfg(windows)]
                overlapped,
                tail: sequencer.clone(),
                uncertain: uncertain.clone(),
                state: Arc::clone(&state),
                completion: None,
                #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                _health: health,
            }),
            state,
            waiter: None,
            sequencer,
            uncertain,
            predecessor: None,
            completion: None,
            terminated: false,
        }
    }
}

impl<T> NativeCompletion<T> {
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    fn failed(error: io::Error) -> Self {
        Self {
            state: Arc::new(Mutex::new(CompletionState {
                result: Some(Err(error)),
                panic: None,
                waker: None,
                _allocation: None,
            })),
            pending: None,
            waiter: None,
            sequencer: None,
            uncertain: None,
            predecessor: None,
            completion: None,
            terminated: false,
        }
    }
}

impl NativeCompletion<()> {
    fn submit(
        file: Arc<NativeFilePayload>,
        operation: NativeUnitOperation,
        sequencer: Option<FileSequencer>,
        uncertain: Option<Arc<AtomicBool>>,
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        health: Option<Arc<FileHealth>>,
        span: obs::OperationSpan,
        #[cfg(windows)] overlapped: bool,
    ) -> Self {
        let mut span = span;
        if let NativeUnitOperation::Write(writes) = &operation {
            span = span.sized(writes.len(), || {
                writes.iter().map(|write| write.bytes.len()).sum()
            });
        }
        let state = Arc::new(Mutex::new(CompletionState {
            result: None,
            panic: None,
            waker: None,
            _allocation: None,
        }));
        Self {
            pending: Some(NativeJob::Unit {
                file,
                operation,
                span,
                #[cfg(windows)]
                overlapped,
                tail: sequencer.clone(),
                uncertain: uncertain.clone(),
                state: Arc::clone(&state),
                completion: None,
                #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                _health: health,
            }),
            state,
            waiter: None,
            sequencer,
            uncertain,
            predecessor: None,
            completion: None,
            terminated: false,
        }
    }
}

fn reserve_if_needed(
    sequencer: &mut Option<FileSequencer>,
    predecessor: &mut Option<Arc<OperationFence>>,
    completion: &mut Option<Arc<OperationFence>>,
    pending: &mut Option<NativeJob>,
) -> io::Result<()> {
    let Some(sequencer) = sequencer.take() else {
        return Ok(());
    };
    let mut tail = sequencer
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *predecessor = tail.clone();
    let allocation = match pending.as_ref() {
        Some(NativeJob::BoundedRead { meter, .. } | NativeJob::PreparedRead { meter, .. }
            | NativeJob::BoundedUnit { meter, .. }) => Some(ReadAllocation::new(
            meter, arc_allocation_bytes::<OperationFence>(),
        )?),
        _ => None,
    };
    let fence = Arc::new(OperationFence {
        sequencer: Some(Arc::downgrade(&sequencer)),
        _allocation: allocation,
        ..OperationFence::default()
    });
    if let Some(job) = pending {
        job.set_completion(Arc::clone(&fence));
    }
    *tail = Some(Arc::clone(&fence));
    *completion = Some(fence);
    Ok(())
}

impl<T> Future for NativeCompletion<T> {
    type Output = io::Result<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.terminated, "native I/O polled after completion");
        if let Err(error) = reserve_if_needed(
            &mut this.sequencer,
            &mut this.predecessor,
            &mut this.completion,
            &mut this.pending,
        ) {
            this.pending = None;
            this.terminated = true;
            return Poll::Ready(Err(error));
        }
        if let Some(predecessor) = &this.predecessor {
            if !predecessor.ready_or_register(context.waker()) {
                if let Some(NativeJob::Read { span, .. } | NativeJob::Unit { span, .. }) =
                    this.pending.as_ref()
                {
                    span.scope(|| {
                        tracing::trace!(name: "acyclic.runtime.fence_wait", "waiting for an earlier operation");
                    });
                }
                return Poll::Pending;
            }
            this.predecessor = None;
        }
        if this
            .uncertain
            .as_ref()
            .is_some_and(|uncertain| uncertain.load(Ordering::Acquire))
        {
            let result: io::Result<T> = Err(io::Error::other(
                "prior native I/O completion on this file is uncertain",
            ));
            if let Some(NativeJob::Read { span, .. } | NativeJob::Unit { span, .. }) =
                this.pending.as_ref()
            {
                span.record_result(&result);
            }
            this.pending = None;
            if let Some(completion) = &this.completion {
                completion.complete();
            }
            this.terminated = true;
            return Poll::Ready(result);
        }
        // A thread that admits inline blocking serves an overlapped handle's
        // positional I/O itself; the job records its result and fence.
        #[cfg(windows)]
        if inline_blocking_allowed()
            && let Some(job) = this.pending.take_if(|job| job.runs_in_place())
        {
            let _ = job.run_in_place();
        }
        match poll_submission(&mut this.pending, &mut this.waiter, context) {
            Poll::Ready(Err(error)) => {
                this.pending = None;
                if let Some(completion) = &this.completion {
                    completion.complete();
                }
                this.terminated = true;
                return Poll::Ready(Err(error));
            }
            Poll::Ready(Ok(())) => {}
            Poll::Pending => return Poll::Pending,
        }
        let mut state = this
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(payload) = state.panic.take() {
            this.terminated = true;
            let waker = state.waker.take();
            drop(state);
            drop(waker);
            drop(this.pending.take());
            if let Some(waiter) = this.waiter.take()
                && let Ok(workers) = ready_workers(&NATIVE_WORKERS)
            {
                remove_capacity_waiter(workers, Some(waiter));
            }
            drop(this.sequencer.take());
            drop(this.uncertain.take());
            drop(this.predecessor.take());
            drop(this.completion.take());
            std::panic::resume_unwind(payload);
        }
        if let Some(result) = state.result.take() {
            this.terminated = true;
            return Poll::Ready(result);
        }
        if !state
            .waker
            .as_ref()
            .is_some_and(|waker| waker.will_wake(context.waker()))
        {
            state.waker = Some(context.waker().clone());
        }
        Poll::Pending
    }
}

struct BlockingWaitWake {
    notified: Mutex<bool>,
    changed: Condvar,
    _allocation: ReadAllocation,
}

impl BlockingWaitWake {
    fn notify(&self) {
        *self.notified.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.changed.notify_one();
    }

    fn wait(&self, deadline: Option<std::time::Instant>) {
        let mut notified = self.notified.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*notified {
            if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() { break; }
                let (next, timeout) = self.changed.wait_timeout(notified, remaining)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                notified = next;
                if timeout.timed_out() { break; }
            } else {
                notified = self.changed.wait(notified)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
        *notified = false;
    }
}

impl std::task::Wake for BlockingWaitWake {
    fn wake(self: Arc<Self>) { self.notify(); }
    fn wake_by_ref(self: &Arc<Self>) { self.notify(); }
}

fn allocate_blocking_wait_wake(meter: &Arc<dyn NativeReadMeter>) -> io::Result<Arc<BlockingWaitWake>> {
    let allocation = ReadAllocation::new(meter, arc_allocation_bytes::<BlockingWaitWake>())?;
    Ok(Arc::new(BlockingWaitWake {
        notified: Mutex::new(false), changed: Condvar::new(), _allocation: allocation,
    }))
}

fn measured_completion_meter<T>(
    state: &Mutex<CompletionState<T>>,
) -> io::Result<Arc<dyn NativeReadMeter>> {
    state.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        ._allocation.as_ref().map(|allocation| Arc::clone(&allocation.meter))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput,
            "synchronous measured wait requires a measured task"))
}

fn wait_measured_completion<T>(
    task: impl Future<Output = io::Result<T>>,
    meter: Arc<dyn NativeReadMeter>,
    prepared_wake: Option<Arc<BlockingWaitWake>>,
    mut deadline: Option<std::time::Instant>,
    cancellation: impl Future<Output = ()>,
    on_deadline: impl FnOnce(),
) -> io::Result<T> {
    let wake = match prepared_wake {
        Some(wake) => wake,
        None => allocate_blocking_wait_wake(&meter)?,
    };
    let waker = Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    let mut task = std::pin::pin!(task);
    let mut cancellation = std::pin::pin!(cancellation);
    let mut cancelled = false;
    let mut on_deadline = Some(on_deadline);
    loop {
        if !cancelled && cancellation.as_mut().poll(&mut context).is_ready() {
            cancelled = true;
            deadline = None;
        }
        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            deadline = None;
            if let Some(on_deadline) = on_deadline.take() { on_deadline(); }
        }
        match task.as_mut().poll(&mut context) {
            Poll::Ready(result) => return result,
            Poll::Pending => wake.wait(deadline),
        }
    }
}

impl<T> BlockingIoTask<T> {
    /// Waits synchronously through this task's existing measured completion and
    /// capacity queue, admitting only its actual SDK-owned wake payload.
    ///
    /// An expired deadline invokes `on_deadline` once to request cooperative
    /// stop. Waiting then continues until the same task has actually terminated;
    /// this never reports a timeout while an accepted OS operation is running.
    /// The concrete cancellation future is stack-pinned and polled with the
    /// same wake context, so queued cancellation does not wait for an I/O wake.
    /// Its readiness disarms the deadline; the task/meter owns cancellation
    /// classification and accepted work still drains to terminal completion.
    /// Native services must already be initialized unless execution is inline.
    pub fn wait_measured(
        mut self,
        deadline: Option<std::time::Instant>,
        cancellation: impl Future<Output = ()>,
        on_deadline: impl FnOnce(),
    ) -> io::Result<T> {
        let meter = measured_completion_meter(&self.state)?;
        let wake = self.prepared_wake.take();
        wait_measured_completion(self, meter, wake, deadline, cancellation, on_deadline)
    }
}

impl<T> Future for BlockingIoTask<T> {
    type Output = io::Result<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.terminated, "host I/O task polled after completion");
        if let Some(job) = this.pending.take_if(|_| inline_blocking_allowed()) {
            // The job records its result before returning; there is no
            // observer to wake yet, and no worker is needed.
            let _ = job.run();
        } else if this.pending.is_some() {
            let measured = this.pending.as_ref().is_some_and(|job| job.meter().is_some());
            let workers = match if measured { ready_workers(&BLOCKING_WORKERS) } else { blocking_workers() } {
                Ok(workers) => workers,
                Err(error) => {
                    this.terminated = true;
                    return Poll::Ready(Err(error));
                }
            };
            match poll_submission_with(workers, &mut this.pending, &mut this.waiter, context) {
                Poll::Ready(Err(error)) => {
                    this.terminated = true;
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Ok(())) => {}
                Poll::Pending => return Poll::Pending,
            }
        }
        let mut state = this
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(payload) = state.panic.take() {
            this.terminated = true;
            let waker = state.waker.take();
            drop(state);
            drop(waker);
            drop(this.pending.take());
            if let Some(waiter) = this.waiter.take()
                && let Ok(workers) = ready_workers(&BLOCKING_WORKERS)
            {
                remove_capacity_waiter(workers, Some(waiter));
            }
            std::panic::resume_unwind(payload);
        }
        if let Some(result) = state.result.take() {
            this.terminated = true;
            return Poll::Ready(result);
        }
        if !state
            .waker
            .as_ref()
            .is_some_and(|waker| waker.will_wake(context.waker()))
        {
            state.waker = Some(context.waker().clone());
        }
        Poll::Pending
    }
}

fn poll_submission(
    pending: &mut Option<NativeJob>,
    waiter: &mut Option<Arc<CapacityWaiter>>,
    context: &Context<'_>,
) -> Poll<io::Result<()>> {
    if pending.is_none() {
        return Poll::Ready(Ok(()));
    }
    let measured = pending.as_ref().is_some_and(|job| job.meter().is_some());
    let workers = match if measured { ready_workers(&NATIVE_WORKERS) } else { native_workers() } {
        Ok(workers) => workers,
        Err(error) => return Poll::Ready(Err(error)),
    };
    poll_submission_with(workers, pending, waiter, context)
}

fn poll_submission_with(
    workers: &NativeWorkers,
    pending: &mut Option<NativeJob>,
    waiter: &mut Option<Arc<CapacityWaiter>>,
    context: &Context<'_>,
) -> Poll<io::Result<()>> {
    let Some(job) = pending.take() else { return Poll::Ready(Ok(())); };
    let meter = job.meter().cloned();
    if let Some(meter) = &meter {
        if let Err(error) = meter.check() {
            remove_capacity_waiter(workers, waiter.take());
            return Poll::Ready(Err(error));
        }
    }
    let mut admission = workers.admission.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(node) = waiter.as_ref() {
        let queued = node.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).queued;
        if !queued && admission.queued_jobs != 0 && admission.head.is_some() {
            admission.register_existing(node, context.waker());
        }
        node.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
            .waker.clone_from(context.waker());
        if admission.queued_jobs != 0 && admission.head.as_ref()
            .is_some_and(|head| !Arc::ptr_eq(head, node)) {
            *pending = Some(job);
            return Poll::Pending;
        }
    } else if admission.queued_jobs != 0 && admission.head.is_some() {
        match admission.register(context.waker(), meter.as_ref()) {
            Ok(node) => *waiter = Some(node),
            Err(error) => return Poll::Ready(Err(error)),
        }
        *pending = Some(job);
        return Poll::Pending;
    }
    match workers.sender.try_send(job) {
        Ok(()) => {
            admission.queued_jobs += 1;
            let next = waiter.take().and_then(|node| admission.retire(&node));
            drop(admission);
            wake_capacity_waiter(next);
            Poll::Ready(Ok(()))
        }
        Err(mpsc::TrySendError::Full(job)) => {
            if waiter.is_none() {
                match admission.register(context.waker(), meter.as_ref()) {
                    Ok(node) => *waiter = Some(node),
                    Err(error) => return Poll::Ready(Err(error)),
                }
            }
            else if let Some(node) = waiter.as_ref()
                && !node.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).queued
            {
                admission.register_existing(node, context.waker());
            }
            *pending = Some(job);
            Poll::Pending
        }
        Err(mpsc::TrySendError::Disconnected(_)) => {
            let next = waiter.take().and_then(|node| admission.retire(&node));
            drop(admission);
            wake_capacity_waiter(next);
            Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "native I/O workers stopped")))
        }
    }
}

fn remove_capacity_waiter(workers: &NativeWorkers, waiter: Option<Arc<CapacityWaiter>>) {
    let Some(waiter) = waiter else { return; };
    let mut admission = workers.admission.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let next = admission.retire(&waiter);
    drop(admission);
    wake_capacity_waiter(next);
}

fn wake_capacity_waiter(waiter: Option<Waker>) {
    if let Some(waiter) = waiter {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| waiter.wake()));
    }
}

fn notify_capacity_released(admission: &Mutex<AdmissionState>) {
    let (mut node, count) = {
        let mut admission = admission.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert_ne!(admission.queued_jobs, 0, "native queue accounting lost");
        admission.queued_jobs = admission.queued_jobs.saturating_sub(1);
        let count = if admission.queued_jobs == 0 { admission.waiter_count }
            else { usize::from(admission.head.is_some()) };
        (admission.head.clone(), count)
    };
    // Snapshot existing links, bounded by the initial queue length. Taking
    // next before waking permits reentrant retirement without losing peers.
    // No Vec<Waker> or other late completion allocation is needed.
    for _ in 0..count {
        let Some(current) = node else { break; };
        let (next, waker) = {
            let state = current.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            (state.next.clone(), state.waker.clone())
        };
        node = next;
        wake_capacity_waiter(Some(waker));
    }
}

static NATIVE_WORKERS: LazyLock<io::Result<NativeWorkers>> =
    LazyLock::new(|| start_workers("acyclic-native-io", |parallelism| parallelism.clamp(2, 4)));
static BLOCKING_WORKERS: LazyLock<io::Result<NativeWorkers>> =
    LazyLock::new(|| start_workers("acyclic-host-io", |parallelism| parallelism.saturating_mul(2).clamp(2, 16)));
static IO_SERVICES_INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Initializes the existing native and host I/O services before measured
/// requests are admitted. Thread/channel startup and host-parallelism probes
/// belong to this explicit service lifetime, never to a bounded request.
pub fn initialize_native_io_services() -> io::Result<()> {
    // Resource-limit probes are fixed shared control metadata. A measured
    // registration only borrows the already initialized limit and registry.
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    FILE_HEALTH.get_or_init(|| Mutex::new(FileHealthRegistry::new()));
    native_workers()?;
    blocking_workers()?;
    IO_SERVICES_INITIALIZED.store(true, Ordering::Release);
    Ok(())
}

fn ready_workers(
    slot: &'static LazyLock<io::Result<NativeWorkers>>,
) -> io::Result<&'static NativeWorkers> {
    if !IO_SERVICES_INITIALIZED.load(Ordering::Acquire) {
        return Err(io::Error::new(io::ErrorKind::NotConnected,
            "native I/O services must be initialized before measured requests"));
    }
    slot.as_ref().map_err(|error| io::Error::new(error.kind(), error.to_string()))
}

fn native_workers() -> io::Result<&'static NativeWorkers> {
    NATIVE_WORKERS.as_ref().map_err(|error| io::Error::new(error.kind(), error.to_string()))
}

fn blocking_workers() -> io::Result<&'static NativeWorkers> {
    BLOCKING_WORKERS.as_ref().map_err(|error| io::Error::new(error.kind(), error.to_string()))
}

/// Starts one existing service pool. Host parallelism is queried once here,
/// outside measured producer admission; it can read cgroup files.
fn start_workers(
    name: &str,
    worker_count: impl FnOnce(usize) -> usize,
) -> io::Result<NativeWorkers> {
        let worker_count =
            worker_count(std::thread::available_parallelism().map_or(1, std::num::NonZero::get));
        let (sender, receiver) = mpsc::sync_channel::<NativeJob>(worker_count * 4);
        let receiver = Arc::new(Mutex::new(receiver));
        let admission = Arc::new(Mutex::new(AdmissionState::new()));
        for worker_index in 0..worker_count {
            let receiver = Arc::clone(&receiver);
            let admission = Arc::clone(&admission);
            std::thread::Builder::new()
                .name(format!("{name}-{worker_index}"))
                .spawn(move || {
                    loop {
                        let job = {
                            let receiver = receiver
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            receiver.recv()
                        };
                        let Ok(job) = job else {
                            break;
                        };
                        notify_capacity_released(&admission);
                        let waker = job.run();
                        if let Some(waker) = waker {
                            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                waker.wake();
                            }));
                        }
                    }
                })?;
        }
        Ok(NativeWorkers { sender, admission })
}

impl<T> Drop for NativeCompletion<T> {
    fn drop(&mut self) {
        if self.pending.is_some()
            && let Some(completion) = &self.completion
        {
            completion.complete_after(self.predecessor.take());
        }
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .waker = None;
        let Some(waiter) = self.waiter.take() else {
            return;
        };
        let Ok(workers) = native_workers() else {
            return;
        };
        remove_capacity_waiter(workers, Some(waiter));
    }
}

impl<T> Drop for BlockingIoTask<T> {
    fn drop(&mut self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .waker = None;
        let Some(waiter) = self.waiter.take() else {
            return;
        };
        let Ok(workers) = ready_workers(&BLOCKING_WORKERS) else {
            return;
        };
        remove_capacity_waiter(workers, Some(waiter));
    }
}

/// Bounded positional source for one file range.
pub struct RangeReader<'a> {
    file: &'a File,
    offset: u64,
    remaining: u64,
}

/// Resizes an already-held, ordered handle. Each retry is separately admitted.
pub fn set_len_measured(file: &File, size: u64, meter: &dyn NativeReadMeter) -> io::Result<()> {
    i64::try_from(size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file length exceeds native range"))?;
    loop {
        meter.check()?;
        meter.admit_write(0)?;
        #[cfg(target_os = "linux")]
        let result = linux::set_len_once(file, size);
        #[cfg(target_vendor = "apple")]
        let result = apple::set_len_once(file, size);
        #[cfg(windows)]
        let result = windows::set_len_once(file, size);
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let result = file.set_len(size);
        match result {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result.and_then(|()| meter.check()),
        }
    }
}

/// Flushes an already-held, ordered handle without metadata queries or reopen.
/// Unsupported barriers perform no OS request and consume no I/O allowance.
pub fn sync_file_measured(
    file: &File, durability: Durability, meter: &dyn NativeReadMeter,
) -> io::Result<()> {
    #[cfg(not(target_vendor = "apple"))]
    if durability == Durability::Barrier { return barrier_sync(file); }
    loop {
        meter.check()?;
        meter.admit_durability()?;
        #[cfg(target_os = "linux")]
        let result = linux::sync_once(file);
        #[cfg(target_vendor = "apple")]
        let result = apple::sync_once(file, durability);
        #[cfg(windows)]
        let result = windows::sync_once(file);
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let result = sync_file_unsequenced(file, durability);
        match result {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result.and_then(|()| meter.check()),
        }
    }
}

/// Writes positionally through the same held completion engine, admitting any
/// Windows mode query and private overlapped reopen before their actual calls.
pub fn write_all_at_measured(
    file: &File, offset: u64, bytes: &[u8], meter: &dyn NativeReadMeter,
) -> io::Result<()> {
    offset.checked_add(bytes.len() as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write offset overflow"))?;
    #[cfg(windows)]
    {
        windows::with_overlapped_measured(file, 0x0012_0116, false, meter, |file| {
            write_bounded_in_place(file, offset, bytes, meter)
        })
    }
    #[cfg(not(windows))]
    { write_bounded_in_place(file, offset, bytes, meter) }
}

/// Writes through an already-held positional handle in its admitted file scope.
/// On Windows the caller must have admitted the handle as overlapped.
pub fn write_all_at_measured_held(
    file: &File, offset: u64, bytes: &[u8], meter: &dyn NativeReadMeter,
) -> io::Result<()> {
    offset.checked_add(bytes.len() as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write offset overflow"))?;
    write_bounded_in_place(file, offset, bytes, meter)
}

fn write_bounded_in_place(
    file: &File, mut offset: u64, mut bytes: &[u8], meter: &dyn NativeReadMeter,
) -> io::Result<()> {
    while !bytes.is_empty() {
        meter.check()?;
        // The same native worker owns each request through terminal completion.
        let length = bytes.len().min(1024 * 1024);
        let chunk = &bytes[..length];
        #[cfg(target_os = "linux")]
        let result = linux::write_at_measured(file, offset, chunk, meter);
        #[cfg(target_vendor = "apple")]
        let result = apple::write_at_measured(file, offset, chunk, meter);
        #[cfg(windows)]
        let result = windows::write_at_known_overlapped(file, offset, chunk, meter);
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let result: io::Result<usize> = Err(io::Error::new(
            io::ErrorKind::Unsupported, "bounded positional writes unavailable",
        ));
        let count = match result {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count > length { std::process::abort(); }
        meter.check()?;
        if count == 0 { return Err(io::ErrorKind::WriteZero.into()); }
        offset += count as u64;
        bytes = &bytes[count..];
    }
    Ok(())
}

fn read_bounded_in_place(
    file: &File,
    offset: u64,
    length: usize,
    require_full: bool,
    meter: &Arc<dyn NativeReadMeter>,
    #[cfg(windows)]
    known_overlapped: bool,
) -> io::Result<NativeReadResult> {
    meter.check()?;
    meter.admit_bytes(length)?;
    if length == 0 {
        return Ok(NativeReadResult { bytes: Bytes::new(), buffer_capacity: 0, _allocation: None });
    }
    let read = |file: &File| {
    let mut allocation = ReadAllocation::new(meter, length)?;
    let mut data = Vec::new();
    data.try_reserve_exact(length).map_err(|error| meter.allocation_failed(error))?;
    allocation.reconcile_capacity(data.capacity())?;
    data.resize(length, 0);
    let filled = read_bounded_slice(file, offset, &mut data, require_full, meter.as_ref())?;
    if filled == 0 {
        return Ok(NativeReadResult { bytes: Bytes::new(), buffer_capacity: 0, _allocation: None });
    }
    let buffer_capacity = data.capacity();
    if filled != buffer_capacity { meter.allocate_refcount_header()?; }
    data.truncate(filled);
    Ok(NativeReadResult {
        bytes: Bytes::from(data), buffer_capacity, _allocation: Some(allocation),
    })
    };
    #[cfg(windows)]
    {
        windows::with_overlapped_measured(
            file,
            windows_sys::Win32::Storage::FileSystem::FILE_GENERIC_READ,
            known_overlapped,
            meter.as_ref(),
            read,
        )
    }
    #[cfg(not(windows))]
    read(file)
}

fn validate_prepared_read_range(offset: u64, length: usize) -> io::Result<()> {
    let end = offset.checked_add(length as u64).ok_or(io::ErrorKind::InvalidInput)?;
    #[cfg(windows)]
    if length != 0 && (length > u32::MAX as usize || offset == u64::MAX) {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    #[cfg(unix)]
    if length != 0 && (offset > i64::MAX as u64 || end > i64::MAX as u64) {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    #[cfg(not(unix))]
    let _ = end;
    Ok(())
}

fn read_prepared_in_place(
    file: &File, offset: u64, destination: &mut [u8], require_full: bool,
    meter: &dyn NativeReadMeter,
    #[cfg(windows)]
    known_overlapped: bool,
) -> io::Result<usize> {
    meter.check()?;
    meter.admit_bytes(destination.len())?;
    validate_prepared_read_range(offset, destination.len())?;
    if destination.is_empty() { return Ok(0); }
    #[cfg(windows)]
    {
        windows::with_overlapped_measured(
            file, windows_sys::Win32::Storage::FileSystem::FILE_GENERIC_READ,
            known_overlapped, meter,
            |file| read_bounded_slice(file, offset, destination, require_full, meter),
        )
    }
    #[cfg(not(windows))]
    read_bounded_slice(file, offset, destination, require_full, meter)
}

// The sole positional bounded-read loop. Both allocating and prepared owners
// use these same retries, byte/operation reservations and EOF accounting.
fn read_bounded_slice(
    file: &File, offset: u64, data: &mut [u8], require_full: bool,
    meter: &dyn NativeReadMeter,
) -> io::Result<usize> {
    let length = data.len();
    let mut filled = 0;
    while filled < length {
        meter.check()?;
        let destination = &mut data[filled..];
        #[cfg(windows)]
        let result = windows::read_at_known_overlapped(
            file, offset + filled as u64, destination, meter,
        );
        #[cfg(unix)]
        let result = read_declared_source_bytes(file, offset + filled as u64, destination, meter);
        let count = match result {
            Ok(count) => count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        meter.check()?;
        if count == 0 { break; }
        filled = filled.checked_add(count)
            .filter(|count| *count <= length)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;
    }
    meter.finish_read(length, filled)?;
    if require_full && filled != length {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(filled)
}

/// Bounded asynchronous positional source for one range of a native file,
/// read through the file's own ordered operations.
pub struct AsyncRangeReader {
    file: Arc<NativeFile>,
    offset: u64,
    remaining: u64,
}

impl AsyncRangeReader {
    /// Creates a source for at most `length` bytes of `file` starting at
    /// `offset`.
    #[must_use]
    pub const fn new(file: Arc<NativeFile>, offset: u64, length: u64) -> Self {
        Self {
            file,
            offset,
            remaining: length,
        }
    }

    /// Bytes not yet returned from this original bounded range.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Reads using this file's existing ordered worker machinery, with actual
    /// request, buffer, retry, and completed-byte accounting on every exit.
    pub async fn read_measured(
        &mut self,
        maximum: usize,
        meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<NativeReadResult> {
        self.read_measured_inner(maximum, meter, false).await
    }

    /// Reads a complete declared chunk using the same finite native queue.
    /// Short physical input is rejected before any shared-byte header is built.
    pub async fn read_exact_measured(
        &mut self, maximum: usize, meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<NativeReadResult> {
        self.read_measured_inner(maximum, meter, true).await
    }

    async fn read_measured_inner(
        &mut self, maximum: usize, meter: Arc<dyn NativeReadMeter>, require_full: bool,
    ) -> io::Result<NativeReadResult> {
        meter.check()?;
        let length = usize::try_from(self.remaining.min(maximum as u64))
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        if length == 0 {
            return Ok(NativeReadResult { bytes: Bytes::new(), buffer_capacity: 0, _allocation: None });
        }
        meter.admit_bytes(length)?;
        let result = self.file.read_bounded_async_inner(self.offset, length, meter, require_full)?.await?;
        self.offset = self.offset.checked_add(result.bytes.len() as u64)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.remaining -= result.bytes.len() as u64;
        Ok(result)
    }

    /// Reads the next bounded chunk without blocking the caller's executor.
    pub async fn read(&mut self, maximum: usize) -> io::Result<Bytes> {
        let length = usize::try_from(self.remaining.min(maximum as u64))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "range too large"))?;
        if length == 0 {
            return Ok(Bytes::new());
        }
        let mut result = self
            .file
            .read_batch_async(vec![OwnedRead {
                offset: self.offset,
                length,
            }])
            .await?;
        let bytes = result
            .pop()
            .ok_or_else(|| io::Error::other("native read returned no result"))?;
        self.offset = self
            .offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "read offset overflow"))?;
        self.remaining -= bytes.len() as u64;
        Ok(bytes)
    }
}

impl<'a> RangeReader<'a> {
    /// Creates a source for at most `length` bytes starting at `offset`.
    pub const fn new(file: &'a File, offset: u64, length: u64) -> Self {
        Self {
            file,
            offset,
            remaining: length,
        }
    }
}

impl Read for RangeReader<'_> {
    fn read(&mut self, destination: &mut [u8]) -> io::Result<usize> {
        let maximum = usize::try_from(self.remaining.min(destination.len() as u64))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "range too large"))?;
        if maximum == 0 {
            return Ok(0);
        }
        let selected = destination.get_mut(..maximum).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "range exceeds destination")
        })?;
        let count = read_at(self.file, self.offset, selected)?;
        self.offset = self
            .offset
            .checked_add(count as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "read offset overflow"))?;
        self.remaining -= count as u64;
        Ok(count)
    }
}

#[cfg(target_os = "linux")]
fn read_at_impl(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    linux::read_at(file, offset, destination)
}

#[cfg(target_os = "linux")]
fn write_all_at_impl(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    linux::write_all_at(file, offset, bytes)
}

#[cfg(not(any(windows, target_os = "linux")))]
fn write_all_batch_owned(file: &Arc<NativeFilePayload>, writes: Vec<OwnedWrite>) -> io::Result<()> {
    validate_write_batch(&writes)?;
    write_all_batch_impl(file, writes)
}

fn validate_write_batch(writes: &[OwnedWrite]) -> io::Result<()> {
    let mut ranges = Vec::new();
    ranges.try_reserve_exact(writes.len())?;
    for write in writes {
        if write.offset == u64::MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "batch write offset aliases the shared file cursor",
            ));
        }
        let length = u64::try_from(write.bytes.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "batch write length is invalid")
        })?;
        let end = write.offset.checked_add(length).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "batch write range overflows")
        })?;
        if !write.bytes.is_empty() {
            ranges.push((write.offset, end));
        }
    }
    ranges.sort_unstable();
    if ranges
        .windows(2)
        .any(|pair| matches!(pair, [first, second] if first.1 > second.0))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "overlapping batch write ranges",
        ));
    }
    Ok(())
}

#[cfg(target_vendor = "apple")]
#[allow(unsafe_code, reason = "one positional read on a live descriptor and borrowed destination")]
fn read_at_impl(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    use std::os::fd::AsRawFd as _;
    let offset = libc::off_t::try_from(offset).map_err(|_| io::ErrorKind::InvalidInput)?;
    // SAFETY: the held file and writable destination live through this syscall.
    let result = unsafe {
        libc::pread(file.as_raw_fd(), destination.as_mut_ptr().cast(), destination.len(), offset)
    };
    if result < 0 { Err(io::Error::last_os_error()) } else { Ok(result as usize) }
}

#[cfg(all(unix, not(target_os = "linux"), not(target_vendor = "apple")))]
fn read_at_impl(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    use std::os::unix::fs::FileExt as _;
    file.read_at(destination, offset)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn write_all_at_impl(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt as _;
    file.write_all_at(bytes, offset)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn write_all_batch_impl(file: &File, writes: Vec<OwnedWrite>) -> io::Result<()> {
    apple::write_all_batch_owned(file, writes)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn read_batch_impl(file: &File, reads: &[OwnedRead]) -> io::Result<Vec<Bytes>> {
    apple::read_batch(file, reads)
}

#[cfg(windows)]
fn read_at_impl(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    windows::read_at(file, offset, destination)
}

#[cfg(windows)]
fn write_all_at_impl(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    windows::write_all_at(file, offset, bytes)
}

#[cfg(target_vendor = "apple")]
#[allow(unsafe_code, reason = "F_BARRIERFSYNC is reachable only through fcntl")]
fn barrier_sync(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;
    // SAFETY: `F_BARRIERFSYNC` acts only on the live descriptor.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_BARRIERFSYNC) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_vendor = "apple"))]
fn barrier_sync(_file: &File) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "ordered write barrier is unavailable on this platform",
    ))
}

#[cfg(unix)]
fn sync_parent_impl(path: &Path, durability: Durability) -> io::Result<()> {
    sync_file(&File::open(path)?, durability)
}

#[cfg(windows)]
fn sync_parent_impl(path: &Path, durability: Durability) -> io::Result<()> {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt as _;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    const FILE_SHARE_READ_WRITE_DELETE: u32 = 0x0000_0001 | 0x0000_0002 | 0x0000_0004;
    let directory = OpenOptions::new()
        .read(true)
        .write(true)
        .share_mode(FILE_SHARE_READ_WRITE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    sync_file(&directory, durability)
}

#[cfg(unix)]
type NamespacePathUnit = u8;
#[cfg(windows)]
type NamespacePathUnit = u16;

/// Buffer storage precedes its claim in drop order, including failed effects.
#[cfg(any(unix, windows))]
struct NamespacePath {
    buffer: Vec<NamespacePathUnit>,
    _allocation: Option<ReadAllocation>,
}

#[cfg(any(unix, windows))]
fn namespace_path(path: &Path, meter: Option<&Arc<dyn NativeReadMeter>>) -> io::Result<NamespacePath> {
    if let Some(meter) = meter {
        meter.check()?;
        meter.admit_examination(1, 0)?;
    }
    #[cfg(unix)]
    let length = {
        use std::os::unix::ffi::OsStrExt as _;
        let bytes = path.as_os_str().as_bytes();
        if bytes.contains(&0) { return Err(io::ErrorKind::InvalidInput.into()); }
        bytes.len()
    };
    #[cfg(windows)]
    let length = {
        use std::os::windows::ffi::OsStrExt as _;
        let mut length = 0_usize;
        for unit in path.as_os_str().encode_wide() {
            if unit == 0 { return Err(io::ErrorKind::InvalidInput.into()); }
            length = length.checked_add(1).ok_or(io::ErrorKind::InvalidInput)?;
        }
        length
    };
    if let Some(meter) = meter { meter.record_examination(1, 0)?; }
    let capacity = length.checked_add(1).ok_or(io::ErrorKind::InvalidInput)?;
    let bytes = capacity.checked_mul(std::mem::size_of::<NamespacePathUnit>())
        .ok_or(io::ErrorKind::InvalidInput)?;
    let mut allocation = meter.map(|meter| ReadAllocation::new(meter, bytes)).transpose()?;
    let mut buffer = Vec::new();
    if let Err(error) = buffer.try_reserve_exact(capacity) {
        return Err(match meter {
            Some(meter) => meter.allocation_failed(error),
            None => io::ErrorKind::OutOfMemory.into(),
        });
    }
    if let Some(allocation) = &mut allocation {
        allocation.reconcile_capacity(buffer.capacity() * std::mem::size_of::<NamespacePathUnit>())?;
    }
    #[cfg(unix)]
    let (encoded, copied) = (0, length);
    #[cfg(windows)]
    let (encoded, copied) = (length * 2, 0);
    if let Some(meter) = meter { meter.admit_codec(encoded as u64, copied as u64, 0)?; }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        buffer.extend_from_slice(path.as_os_str().as_bytes());
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        buffer.extend(path.as_os_str().encode_wide());
    }
    buffer.push(0);
    if let Some(meter) = meter { meter.record_codec(encoded as u64, copied as u64, 0)?; }
    Ok(NamespacePath { buffer, _allocation: allocation })
}

/// Publishes a new entry without replacing an existing one.
pub fn publish_new(from: &Path, to: &Path) -> io::Result<()> {
    publish_new_engine(from, to, None)
}

/// Uses the ordinary no-replace namespace engine with original path custody.
pub fn publish_new_measured(
    from: &Path, to: &Path, meter: &Arc<dyn NativeReadMeter>,
) -> io::Result<()> {
    publish_new_engine(from, to, Some(meter))
}

#[allow(unsafe_code, reason = "native links borrow two retained terminated pathname buffers")]
fn publish_new_engine(
    from: &Path, to: &Path, meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<()> {
    #[cfg(any(unix, windows))]
    {
        let from_path = namespace_path(from, meter)?;
        let to_path = namespace_path(to, meter)?;
        loop {
            if let Some(meter) = meter { meter.check()?; meter.admit_write(0)?; }
            #[cfg(unix)]
            let failed = unsafe {
                libc::link(from_path.buffer.as_ptr().cast(), to_path.buffer.as_ptr().cast())
            } != 0;
            #[cfg(windows)]
            let failed = unsafe {
                windows_sys::Win32::Storage::FileSystem::CreateHardLinkW(
                    to_path.buffer.as_ptr(), from_path.buffer.as_ptr(), std::ptr::null(),
                )
            } == 0;
            if failed {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted { continue; }
                return Err(error);
            }
            return Ok(());
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        if let Some(meter) = meter { meter.check()?; meter.admit_write(0)?; }
        std::fs::hard_link(from, to)
    }
}

fn durable_rename_impl(from: &Path, to: &Path, mode: RenameMode) -> io::Result<()> {
    durable_rename_engine(from, to, mode, None)
}

/// Retains the actual platform pathname controls through durable publication.
pub fn durable_rename_measured(
    from: &Path, to: &Path, mode: RenameMode, meter: &Arc<dyn NativeReadMeter>,
) -> io::Result<()> {
    durable_rename_engine(from, to, mode, Some(meter))
}

#[allow(unsafe_code, reason = "rename borrows live retained native path buffers")]
fn durable_rename_engine(
    from: &Path, to: &Path, mode: RenameMode, meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<()> {
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    {
        let from_path = namespace_path(from, meter)?;
        let to_path = namespace_path(to, meter)?;
        loop {
            if let Some(meter) = meter {
                meter.check()?;
                meter.admit_write(0)?;
                #[cfg(windows)]
                meter.admit_durability()?;
            }
            #[cfg(target_os = "linux")]
            let failed = unsafe {
                libc::syscall(libc::SYS_renameat2, libc::AT_FDCWD, from_path.buffer.as_ptr(),
                    libc::AT_FDCWD, to_path.buffer.as_ptr(),
                    if mode == RenameMode::NoReplace { libc::RENAME_NOREPLACE } else { 0 })
            } != 0;
            #[cfg(target_os = "macos")]
            let failed = unsafe {
                libc::renamex_np(from_path.buffer.as_ptr().cast(), to_path.buffer.as_ptr().cast(),
                    if mode == RenameMode::NoReplace { libc::RENAME_EXCL } else { 0 })
            } != 0;
            #[cfg(windows)]
            let failed = unsafe {
                use windows_sys::Win32::Storage::FileSystem::{
                    MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
                };
                MoveFileExW(from_path.buffer.as_ptr(), to_path.buffer.as_ptr(),
                    MOVEFILE_WRITE_THROUGH | if mode == RenameMode::Replace {
                        MOVEFILE_REPLACE_EXISTING
                    } else { 0 })
            } == 0;
            if !failed { break; }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted { return Err(error); }
        }
        #[cfg(unix)]
        sync_rename_parents(from, to, meter)?;
        Ok(())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        let _ = (from, to, mode, meter);
        Err(io::ErrorKind::Unsupported.into())
    }
}

#[cfg(unix)]
fn sync_rename_parents(
    from: &Path, to: &Path, meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<()> {
    fn namespace_parent(path: &Path) -> &Path {
        path.parent().filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
    }
    let synchronize = |parent: &Path| -> io::Result<()> {
        if let Some(meter) = meter {
            meter.check()?;
            meter.admit_read(0)?;
            let directory = File::open(parent)?;
            sync_file_measured(&directory, Durability::Full, meter.as_ref())
        } else {
            sync_parent(parent, Durability::Full)
        }
    };
    let to_parent = namespace_parent(to);
    synchronize(to_parent)?;
    let from_parent = namespace_parent(from);
    if from_parent != to_parent { synchronize(from_parent)?; }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::{Seek as _, SeekFrom};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::task::{Poll, Wake};
    use std::thread::Thread;
    use std::time::{Duration, Instant};

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn file_scope_nested_a_b_a_preserves_original_alias_detection() {
        let a: FileSequencer = Arc::new(Mutex::new(None));
        let b: FileSequencer = Arc::new(Mutex::new(None));
        assert!(!FileScope::contains(&a));
        FileScope::with(Some(&a), || {
            assert!(FileScope::contains(&a));
            assert!(!FileScope::contains(&b));
            FileScope::with(Some(&b), || {
                assert!(FileScope::contains(&a));
                assert!(FileScope::contains(&b));
                FileScope::with(Some(&a), || {
                    assert!(FileScope::contains(&a));
                    assert!(FileScope::contains(&b));
                });
                assert!(FileScope::contains(&a));
                assert!(FileScope::contains(&b));
            });
            assert!(FileScope::contains(&a));
            assert!(!FileScope::contains(&b));
        });
        assert!(!FileScope::contains(&a));
        assert!(!FileScope::contains(&b));
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn file_scope_unwind_restores_live_outer_frame_before_node_retirement() {
        let a: FileSequencer = Arc::new(Mutex::new(None));
        let b: FileSequencer = Arc::new(Mutex::new(None));
        FileScope::with(Some(&a), || {
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                FileScope::with(Some(&b), || {
                    assert!(FileScope::contains(&a));
                    assert!(FileScope::contains(&b));
                    std::panic::panic_any(41_u32);
                });
            })).expect_err("inner synchronous scope unwinds");
            assert_eq!(panic.downcast_ref::<u32>(), Some(&41));
            assert!(FileScope::contains(&a));
            assert!(!FileScope::contains(&b));
        });
        assert!(!FileScope::contains(&a));
        assert!(!FileScope::contains(&b));
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn file_scope_borrowed_chain_never_crosses_calling_threads() {
        let a: FileSequencer = Arc::new(Mutex::new(None));
        FileScope::with(Some(&a), || {
            std::thread::scope(|threads| {
                threads.spawn(|| {
                    assert!(!FileScope::contains(&a));
                    FileScope::with(Some(&a), || assert!(FileScope::contains(&a)));
                    assert!(!FileScope::contains(&a));
                }).join().expect("scope isolation worker");
            });
            assert!(FileScope::contains(&a));
        });
        assert!(!FileScope::contains(&a));
    }

    #[test]
    fn a_service_that_exits_without_signalling_closes_its_readiness_channel() -> io::Result<()> {
        // This test binary rejects the service arguments and exits, writing
        // only to its discarded standard error.
        let readiness = spawn_service_process(&std::env::current_exe()?)?;
        assert!(!readiness.wait()?);
        Ok(())
    }

    #[test]
    fn a_process_not_started_as_a_signalled_service_has_no_readiness_channel() -> io::Result<()> {
        assert!(take_service_ready_signal()?.is_none());
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    fn platform_submission_transfers_completion_only_after_acceptance() -> io::Result<()> {
        let state = Arc::new(Mutex::new(CompletionState::<usize> {
            result: None,
            panic: None,
            waker: None,
            _allocation: None,
        }));
        let fence = Arc::new(OperationFence::default());
        let uncertain = Arc::new(AtomicBool::new(false));
        let mut callback = None;
        let wake = submit_file_io(
            FileCompletion::new(
                Arc::clone(&state),
                Some(Arc::clone(&fence)),
                Some(Arc::clone(&uncertain)),
                None,
                obs::OperationSpan::new(tracing::Span::none()),
            ),
            |finish| {
                callback = Some(finish);
                Ok(())
            },
        );
        assert!(wake.is_none());
        assert!(!fence.is_complete());
        callback
            .take()
            .ok_or_else(|| io::Error::other("accepted callback missing"))?(Ok(7));
        assert!(fence.is_complete());
        let actual = state
            .lock()
            .map_err(|_| io::Error::other("completion state poisoned"))?
            .result
            .take()
            .ok_or_else(|| io::Error::other("accepted result missing"))??;
        assert_eq!(actual, 7);
        assert!(!uncertain.load(Ordering::Acquire));

        let rejected = Arc::new(Mutex::new(CompletionState::<usize> {
            result: None,
            panic: None,
            waker: None,
            _allocation: None,
        }));
        let rejected_fence = Arc::new(OperationFence::default());
        assert!(
            submit_file_io(
                FileCompletion::new(
                    Arc::clone(&rejected),
                    Some(Arc::clone(&rejected_fence)),
                    None,
                    None,
                    obs::OperationSpan::new(tracing::Span::none())
                ),
                |_finish| Err(io::Error::other("not submitted")),
            )
            .is_none()
        );
        assert!(rejected_fence.is_complete());
        let rejected_result = rejected
            .lock()
            .map_err(|_| io::Error::other("rejected completion state poisoned"))?
            .result
            .take()
            .ok_or_else(|| io::Error::other("rejected result missing"))?;
        assert!(matches!(rejected_result, Err(error) if error.to_string() == "not submitted"));
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    fn accepted_callback_cannot_disappear_without_terminal_completion() -> io::Result<()> {
        const MARKER: &str = "ACYCLIC_DROPPED_COMPLETION_CHILD";
        if std::env::var_os(MARKER).is_some() {
            let state = Arc::new(Mutex::new(CompletionState::<()> {
                result: None,
                panic: None,
                waker: None,
                _allocation: None,
            }));
            eprintln!("dropping accepted native callback");
            let _ = submit_file_io(
                FileCompletion::new(
                    state,
                    None,
                    None,
                    None,
                    obs::OperationSpan::new(tracing::Span::none()),
                ),
                |callback| {
                    drop(callback);
                    Ok(())
                },
            );
            return Err(io::Error::other("missing terminal callback was accepted"));
        }
        let output = crate::process_output(
            std::process::Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("tests::accepted_callback_cannot_disappear_without_terminal_completion")
                .arg("--nocapture")
                .env(MARKER, "1"),
            std::time::Duration::from_secs(120),
            8 * 1024 * 1024,
        )?;
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("dropping accepted native callback")
        );
        Ok(())
    }

    #[test]
    fn native_file_worker_unwind_is_fail_stop() -> io::Result<()> {
        const CHILD_MARKER: &str = "ACYCLIC_NATIVE_UNWIND_CHILD_TEST";
        if std::env::var_os(CHILD_MARKER).is_some() {
            let directory = tempfile::tempdir()?;
            let file = NativeFile::from_file(
                OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(directory.path().join("panic-worker"))?,
            )?;
            let _ = complete_write(file.control_async(|_| {
                eprintln!("injected native worker unwind");
                std::panic::resume_unwind(Box::new("injected native worker unwind"));
            }));
            return Err(io::Error::other(
                "native worker unwind returned to observer",
            ));
        }
        let output = crate::process_output(
            std::process::Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("tests::native_file_worker_unwind_is_fail_stop")
                .arg("--nocapture")
                .env(CHILD_MARKER, "1"),
            std::time::Duration::from_secs(120),
            8 * 1024 * 1024,
        )?;
        assert!(
            !output.status.success(),
            "native worker unwind escaped fail-stop"
        );
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("injected native worker unwind"),
            "child did not reach the injected worker panic"
        );
        Ok(())
    }

    struct ThreadWake(Thread);

    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    fn completion_waker() -> Waker {
        Waker::from(Arc::new(ThreadWake(std::thread::current())))
    }

    #[cfg(unix)]
    fn direct_write_at(file: &File, bytes: &[u8], offset: u64) -> io::Result<()> {
        use std::os::unix::fs::FileExt as _;
        file.write_all_at(bytes, offset)
    }

    #[cfg(windows)]
    fn direct_write_at(file: &File, mut bytes: &[u8], mut offset: u64) -> io::Result<()> {
        use std::os::windows::fs::FileExt as _;
        while !bytes.is_empty() {
            let count = file.seek_write(bytes, offset)?;
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "direct write returned zero",
                ));
            }
            offset = offset.checked_add(count as u64).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "write offset overflow")
            })?;
            bytes = bytes
                .get(count..)
                .ok_or_else(|| io::Error::other("overlong direct write"))?;
        }
        Ok(())
    }

    #[cfg(unix)]
    fn direct_read_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::unix::fs::FileExt as _;
        file.read_at(bytes, offset)
    }

    #[cfg(windows)]
    fn direct_read_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::windows::fs::FileExt as _;
        file.seek_read(bytes, offset)
    }

    #[test]
    fn exclusive_lock_contention_is_not_a_generic_io_failure() {
        assert!(is_exclusive_lock_contention(&io::Error::from(
            io::ErrorKind::WouldBlock
        )));
        assert!(!is_exclusive_lock_contention(&io::Error::from(
            io::ErrorKind::PermissionDenied
        )));
        assert!(!is_exclusive_lock_contention(&io::Error::from(
            io::ErrorKind::Unsupported
        )));
        #[cfg(windows)]
        assert!(is_exclusive_lock_contention(&io::Error::from_raw_os_error(
            33
        )));
    }

    #[test]
    fn ownership_release_barrier_waits_for_the_final_clone() {
        let anchor = OwnershipAnchor::new(());
        let clone = anchor.clone();
        let barrier = anchor.release_barrier();
        drop(anchor);
        assert!(!barrier.is_released());
        drop(clone);
        barrier.wait();
    }

    #[test]
    fn durable_rename_moves_and_replaces_entries() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        let destination = directory.path().join("destination");
        std::fs::write(&source, b"first")?;
        durable_rename(&source, &destination, RenameMode::NoReplace)?;
        assert!(!source.exists());
        assert_eq!(std::fs::read(&destination)?, b"first");

        std::fs::write(&source, b"second")?;
        durable_rename(&source, &destination, RenameMode::Replace)?;
        assert!(!source.exists());
        assert_eq!(std::fs::read(destination)?, b"second");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn durable_rename_accepts_relative_sibling_paths() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let original = std::env::current_dir()?;
        std::env::set_current_dir(temporary.path())?;
        let result = (|| {
            std::fs::write("from", b"relative")?;
            durable_rename(Path::new("from"), Path::new("to"), RenameMode::NoReplace)?;
            assert_eq!(std::fs::read("to")?, b"relative");
            Ok(())
        })();
        std::env::set_current_dir(original)?;
        result
    }

    #[test]
    fn no_replace_preserves_both_entries() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let source = directory.path().join("source");
        let destination = directory.path().join("destination");
        std::fs::write(&source, b"source")?;
        std::fs::write(&destination, b"destination")?;
        let Err(error) = durable_rename(&source, &destination, RenameMode::NoReplace) else {
            return Err(io::Error::other(
                "existing destination accepted no-replace rename",
            ));
        };
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(publish_new(&source, &destination).unwrap_err().kind(), io::ErrorKind::AlreadyExists);
        let mut limits = ReadBudgetMeter::new(0, 0);
        limits.memory_limit = 0;
        let observer = Arc::new(limits);
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        assert_eq!(publish_new_measured(&source, &destination, &meter).unwrap_err().kind(), io::ErrorKind::Other);
        assert_eq!(durable_rename_measured(
            &source, &destination, RenameMode::Replace, &meter,
        ).unwrap_err().kind(), io::ErrorKind::Other);
        assert_eq!(observer.operations.load(Ordering::Acquire), 0);
        assert_eq!(observer.allocations.load(Ordering::Acquire), 0);
        assert_eq!(observer.memory.load(Ordering::Acquire), 0);
        assert_eq!(std::fs::read(source)?, b"source");
        assert_eq!(std::fs::read(destination)?, b"destination");
        Ok(())
    }

    fn complete_read(mut read: ReadBatch) -> io::Result<Vec<Bytes>> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let waker = completion_waker();
        loop {
            match Pin::new(&mut read).poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(result) => return result,
                Poll::Pending if Instant::now() < deadline => {
                    std::thread::park_timeout(deadline.saturating_duration_since(Instant::now()));
                }
                Poll::Pending => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "owned native read did not complete",
                    ));
                }
            }
        }
    }

    fn complete_write(mut write: UnitCompletion) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let waker = completion_waker();
        loop {
            match Pin::new(&mut write).poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(result) => return result,
                Poll::Pending if Instant::now() < deadline => {
                    std::thread::park_timeout(deadline.saturating_duration_since(Instant::now()));
                }
                Poll::Pending => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "owned native write did not complete",
                    ));
                }
            }
        }
    }

    fn complete_measured<T>(future: impl Future<Output = io::Result<T>>) -> io::Result<T> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let waker = completion_waker();
        let mut future = std::pin::pin!(future);
        loop {
            match future.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(result) => return result,
                Poll::Pending if Instant::now() < deadline =>
                    std::thread::park_timeout(deadline.saturating_duration_since(Instant::now())),
                Poll::Pending => return Err(io::ErrorKind::TimedOut.into()),
            }
        }
    }

    pub(super) struct ReadBudgetMeter {
        operations: AtomicU64,
        withheld_operations: std::sync::Mutex<u64>,
        read: AtomicU64,
        withheld_bytes: std::sync::Mutex<u64>,
        allocations: AtomicU64,
        pending_allocations: parking_lot::Mutex<(u64, u64)>,
        allocation_limit: u64,
        headers: AtomicU64,
        memory: AtomicU64,
        peak: AtomicU64,
        operation_limit: u64,
        byte_limit: u64,
        memory_limit: u64,
        cancelled: AtomicBool,
        cancel_after_read: bool,
        panic_after_read: bool,
        allocation_failure: parking_lot::Mutex<Option<std::collections::TryReserveError>>,
    }

    impl ReadBudgetMeter {
        pub(super) fn new(operation_limit: u64, byte_limit: u64) -> Self {
            Self {
                operations: AtomicU64::new(0), read: AtomicU64::new(0),
                withheld_operations: std::sync::Mutex::new(0),
                withheld_bytes: std::sync::Mutex::new(0),
                allocations: AtomicU64::new(0), headers: AtomicU64::new(0),
                pending_allocations: parking_lot::Mutex::new((0, 0)),
                allocation_limit: u64::MAX,
                memory: AtomicU64::new(0), peak: AtomicU64::new(0),
                operation_limit, byte_limit, memory_limit: 1024 * 1024,
                cancelled: AtomicBool::new(false), cancel_after_read: false,
                panic_after_read: false,
                allocation_failure: parking_lot::Mutex::new(None),
            }
        }
        fn refuse() -> io::Error { io::ErrorKind::Other.into() }
        fn held_operations(&self) -> std::sync::MutexGuard<'_, u64> {
            match self.withheld_operations.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            }
        }
        fn held_bytes(&self) -> std::sync::MutexGuard<'_, u64> {
            match self.withheld_bytes.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            }
        }
    }

    impl NativeReadMeter for ReadBudgetMeter {
        fn check(&self) -> io::Result<()> {
            if self.cancelled.load(Ordering::Acquire) {
                Err(io::ErrorKind::Interrupted.into())
            } else { Ok(()) }
        }
        fn allocate(&self, bytes: u64) -> io::Result<()> {
            self.reserve_allocation(bytes)?;
            self.record_reserved_allocation(bytes)
        }
        fn reserve_allocation(&self, bytes: u64) -> io::Result<()> {
            let mut pending = self.pending_allocations.lock();
            self.check()?;
            self.allocations.load(Ordering::Acquire).checked_add(pending.0)
                .and_then(|total| total.checked_add(1))
                .filter(|total| *total <= self.allocation_limit).ok_or_else(Self::refuse)?;
            self.memory.load(Ordering::Acquire).checked_add(pending.1)
                .and_then(|total| total.checked_add(bytes))
                .filter(|total| *total <= self.memory_limit).ok_or_else(Self::refuse)?;
            pending.0 += 1;
            pending.1 += bytes;
            Ok(())
        }
        fn cancel_reserved_allocation(&self, bytes: u64) {
            let mut pending = self.pending_allocations.lock();
            pending.0 = pending.0.checked_sub(1).expect("refund owns an allocation credit");
            pending.1 = pending.1.checked_sub(bytes).expect("refund owns its retained-byte credit");
        }
        fn record_reserved_allocation(&self, bytes: u64) -> io::Result<()> {
            let mut pending = self.pending_allocations.lock();
            let next_operations = pending.0.checked_sub(1).ok_or_else(Self::refuse)?;
            let next_bytes = pending.1.checked_sub(bytes).ok_or_else(Self::refuse)?;
            let allocations = self.allocations.load(Ordering::Acquire).checked_add(1)
                .ok_or_else(Self::refuse)?;
            let memory = self.memory.load(Ordering::Acquire).checked_add(bytes).ok_or_else(Self::refuse)?;
            pending.0 = next_operations;
            pending.1 = next_bytes;
            self.allocations.store(allocations, Ordering::Release);
            self.memory.store(memory, Ordering::Release);
            self.peak.fetch_max(memory, Ordering::AcqRel);
            Ok(())
        }
        fn allocation_failed(&self, error: std::collections::TryReserveError) -> io::Error {
            let mut failure = self.allocation_failure.lock();
            if failure.is_none() { *failure = Some(error); }
            io::ErrorKind::OutOfMemory.into()
        }
        fn allocate_refcount_header(&self) -> io::Result<()> {
            self.allocate(0)?;
            self.headers.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
        fn adopt(&self, bytes: u64) -> io::Result<()> {
            let pending = self.pending_allocations.lock();
            self.check()?;
            let previous = self.memory.fetch_update(Ordering::AcqRel, Ordering::Acquire,
                |current| current.checked_add(bytes).filter(|next|
                    next.checked_add(pending.1).is_some_and(|total| total <= self.memory_limit)))
                .map_err(|_| Self::refuse())?;
            self.peak.fetch_max(previous + bytes, Ordering::AcqRel);
            Ok(())
        }
        fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>) {
            let pending = self.pending_allocations.lock();
            let previous = match self.memory.fetch_update(Ordering::AcqRel, Ordering::Acquire,
                |current| current.checked_add(bytes)) {
                Ok(previous) => previous,
                Err(_) => return (0, Err(Self::refuse())),
            };
            let actual = previous + bytes;
            self.peak.fetch_max(actual, Ordering::AcqRel);
            let result = if actual.checked_add(pending.1).is_none_or(|total| total > self.memory_limit) {
                Err(Self::refuse())
            } else { self.check() };
            (bytes, result)
        }
        fn release(&self, bytes: u64) {
            let _pending = self.pending_allocations.lock();
            self.memory.fetch_sub(bytes, Ordering::AcqRel);
        }
        fn examine(&self, _: u64, _: u64) -> io::Result<()> { self.check() }
        fn admit_examination(&self, _: u64, _: u64) -> io::Result<()> { self.check() }
        fn record_examination(&self, _: u64, _: u64) -> io::Result<()> { Ok(()) }
        fn admit_bytes(&self, maximum: usize) -> io::Result<()> {
            let held = self.held_bytes();
            self.check()?;
            self.read.load(Ordering::Acquire).checked_add(*held)
                .and_then(|total| total.checked_add(maximum as u64))
                .filter(|total| *total <= self.byte_limit)
                .map(|_| ()).ok_or_else(Self::refuse)
        }
        fn reserve_bytes(&self, kind: NativeOperationKind, maximum: u64) -> io::Result<()> {
            if kind != NativeOperationKind::Read { return Err(io::ErrorKind::Unsupported.into()); }
            let mut held = self.held_bytes();
            self.check()?;
            self.read.load(Ordering::Acquire).checked_add(*held)
                .and_then(|total| total.checked_add(maximum))
                .filter(|total| *total <= self.byte_limit)
                .ok_or_else(Self::refuse)?;
            *held += maximum;
            Ok(())
        }
        fn cancel_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64) {
            assert_eq!(kind, NativeOperationKind::Read);
            let mut held = self.held_bytes();
            *held = held.checked_sub(maximum).expect("refund owns read byte allowance");
        }
        fn record_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64, actual: u64) -> io::Result<()> {
            assert_eq!(kind, NativeOperationKind::Read);
            assert!(actual <= maximum);
            let mut held = self.held_bytes();
            *held = held.checked_sub(maximum).expect("transfer owns read byte allowance");
            self.read.fetch_add(actual, Ordering::AcqRel);
            if self.cancel_after_read && actual != 0 {
                self.cancelled.store(true, Ordering::Release);
            }
            Ok(())
        }
        fn admit_operations(&self, additional: u64) -> io::Result<()> {
            let held = self.held_operations();
            self.check()?;
            self.operations.load(Ordering::Acquire).checked_add(*held)
                .and_then(|total| total.checked_add(additional))
                .filter(|total| *total <= self.operation_limit)
                .map(|_| ()).ok_or_else(Self::refuse)
        }
        fn record_operations(&self, completed: u64) -> io::Result<()> {
            let _held = self.held_operations();
            self.operations.fetch_add(completed, Ordering::AcqRel);
            Ok(())
        }
        fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> io::Result<()> {
            if kind != NativeOperationKind::Read { return Err(io::ErrorKind::Unsupported.into()); }
            let mut held = self.held_operations();
            self.check()?;
            self.operations.load(Ordering::Acquire).checked_add(*held)
                .and_then(|total| total.checked_add(count))
                .filter(|total| *total <= self.operation_limit)
                .ok_or_else(Self::refuse)?;
            *held += count;
            Ok(())
        }
        fn cancel_reserved_operations(&self, kind: NativeOperationKind, count: u64) {
            assert_eq!(kind, NativeOperationKind::Read);
            let mut held = self.held_operations();
            *held = held.checked_sub(count).expect("refund owns unspent read credits");
        }
        fn record_reserved_operation(&self, kind: NativeOperationKind) -> io::Result<()> {
            assert_eq!(kind, NativeOperationKind::Read);
            let mut held = self.held_operations();
            *held = held.checked_sub(1).expect("native attempt owns a read credit");
            self.operations.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
        fn admit_write_bytes(&self, _: usize) -> io::Result<()> { Err(Self::refuse()) }
        fn admit_read(&self, maximum: usize) -> io::Result<()> {
            let held = self.held_operations();
            self.admit_bytes(maximum)?;
            self.operations.load(Ordering::Acquire).checked_add(*held)
                .and_then(|total| total.checked_add(1))
                .filter(|total| *total <= self.operation_limit)
                .ok_or_else(Self::refuse)?;
            self.operations.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
        fn record_read(&self, bytes: usize) -> io::Result<()> {
            let _held = self.held_bytes();
            self.read.fetch_add(bytes as u64, Ordering::AcqRel);
            if self.cancel_after_read && bytes != 0 {
                self.cancelled.store(true, Ordering::Release);
            }
            Ok(())
        }
        fn finish_read(&self, _: usize, completed: usize) -> io::Result<()> {
            if self.panic_after_read && completed != 0 {
                std::panic::panic_any(0x7072_6570_6172_6564_u64);
            }
            self.check()
        }
        fn admit_write(&self, _: usize) -> io::Result<()> { Err(Self::refuse()) }
        fn record_write(&self, _: usize) -> io::Result<()> { Err(Self::refuse()) }
        fn admit_write_operations(&self, _: u64) -> io::Result<()> { Err(Self::refuse()) }
        fn record_write_operations(&self, _: u64) -> io::Result<()> { Err(Self::refuse()) }
        fn admit_durability(&self) -> io::Result<()> { Err(Self::refuse()) }
    }

    fn prepared_metadata_task(
        file: File, meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<BlockingIoTask<io::Result<u64>>> {
        let mut operation = OwnedNativeOperationReservation::new(
            Arc::clone(&meter), NativeOperationKind::Read, 1,
        )?;
        prepare_blocking_io_measured(move || {
            let (result, recorded) = operation.attempt(|| file.metadata().map(|metadata| metadata.len()));
            result.and_then(|length| recorded.map(|()| length))
        }, meter)
    }

    #[test]
    fn prepared_blocking_controls_are_paid_before_submit_and_wait_needs_no_new_allocation()
    -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempfile()?;
        temporary.set_len(17)?;
        let sizing = Arc::new(ReadBudgetMeter::new(1, 0));
        let task = prepared_metadata_task(temporary.try_clone()?, sizing.clone())?;
        assert_eq!(sizing.operations.load(Ordering::Acquire), 0);
        assert_eq!(sizing.allocations.load(Ordering::Acquire), 4);
        let exact_bytes = sizing.memory.load(Ordering::Acquire);
        assert!(exact_bytes > 0);
        assert!(!task.waiter.as_ref().expect("preowned capacity node").state.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner).queued);
        drop(task);
        assert_eq!(sizing.memory.load(Ordering::Acquire), 0);
        assert_eq!(*sizing.held_operations(), 0, "unused operation refunded without physical metadata I/O");
        let mut limits = ReadBudgetMeter::new(1, 0);
        limits.allocation_limit = 4;
        limits.memory_limit = exact_bytes;
        let observed = Arc::new(limits);
        let task = prepared_metadata_task(temporary, observed.clone())?;
        assert_eq!(observed.memory.load(Ordering::Acquire), exact_bytes);
        assert!(observed.allocate(1).is_err(), "all four actual control attempts exhausted the allocation quota");
        let _inline = InlineBlocking::enter();
        assert_eq!(task.wait_measured(None, std::future::pending(), || {})??, 17);
        assert_eq!(observed.operations.load(Ordering::Acquire), 1);
        assert_eq!(observed.allocations.load(Ordering::Acquire), 4);
        assert_eq!(observed.peak.load(Ordering::Acquire), exact_bytes);
        assert_eq!(observed.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    fn prepared_blocking_control_denial_retires_only_actual_reached_allocations()
    -> io::Result<()> {
        initialize_native_io_services()?;
        for accepted in 0..4 {
            let mut limits = ReadBudgetMeter::new(1, 0);
            limits.allocation_limit = accepted;
            let observed = Arc::new(limits);
            assert!(prepared_metadata_task(tempfile::tempfile()?, observed.clone()).is_err());
            assert_eq!(observed.operations.load(Ordering::Acquire), 0);
            assert_eq!(observed.allocations.load(Ordering::Acquire), accepted);
            assert_eq!(observed.memory.load(Ordering::Acquire), 0);
            assert_eq!(*observed.held_operations(), 0);
        }
        Ok(())
    }

    #[test]
    fn prepared_capacity_pressure_and_abandoned_observer_keep_original_job_custody()
    -> io::Result<()> {
        initialize_native_io_services()?;
        let mut limits = ReadBudgetMeter::new(1, 0);
        limits.allocation_limit = 4;
        let observed = Arc::new(limits);
        let file = tempfile::tempfile()?;
        file.set_len(23)?;
        let mut task = prepared_metadata_task(file, observed.clone())?;
        let prepared_bytes = observed.memory.load(Ordering::Acquire);
        let node = Arc::clone(task.waiter.as_ref().expect("prepared original node"));
        let (sender, receiver) = mpsc::sync_channel(1);
        let workers = NativeWorkers { sender, admission: Arc::new(Mutex::new(AdmissionState::new())) };
        workers.sender.try_send(NativeJob::Task {
            operation: Box::new(|| None), meter: None, _allocation: None,
        }).map_err(|_| io::Error::other("failed to saturate canonical test queue"))?;
        workers.admission.lock().unwrap_or_else(std::sync::PoisonError::into_inner).queued_jobs = 1;
        let context = Context::from_waker(Waker::noop());
        assert!(matches!(poll_submission_with(
            &workers, &mut task.pending, &mut task.waiter, &context,
        ), Poll::Pending));
        assert!(Arc::ptr_eq(task.waiter.as_ref().expect("same preowned node"), &node));
        assert!(node.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).queued);
        assert_eq!(observed.allocations.load(Ordering::Acquire), 4);
        assert_eq!(observed.memory.load(Ordering::Acquire), prepared_bytes);
        let filler = receiver.recv().map_err(io::Error::other)?;
        notify_capacity_released(&workers.admission);
        let _ = filler.run();
        assert!(matches!(poll_submission_with(
            &workers, &mut task.pending, &mut task.waiter, &context,
        ), Poll::Ready(Ok(()))));
        assert!(!node.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner).queued);
        drop(node);
        drop(task);
        assert!(observed.memory.load(Ordering::Acquire) > 0,
            "accepted job owns original completion and exact closure beyond observer drop");
        assert_eq!(observed.operations.load(Ordering::Acquire), 0);
        let accepted = receiver.recv().map_err(io::Error::other)?;
        notify_capacity_released(&workers.admission);
        let _ = accepted.run();
        assert_eq!(observed.operations.load(Ordering::Acquire), 1);
        assert_eq!(observed.allocations.load(Ordering::Acquire), 4);
        assert_eq!(observed.memory.load(Ordering::Acquire), 0);
        assert_eq!(workers.admission.lock().unwrap_or_else(std::sync::PoisonError::into_inner).waiter_count, 0);
        Ok(())
    }

    #[test]
    fn prepared_blocking_panic_preserves_original_payload_and_retires_actual_controls()
    -> io::Result<()> {
        initialize_native_io_services()?;
        let mut limits = ReadBudgetMeter::new(0, 0);
        limits.allocation_limit = 4;
        let observed = Arc::new(limits);
        let task = prepare_blocking_io_measured(|| -> () { std::panic::panic_any(0x5a17_u64) }, observed.clone())?;
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _inline = InlineBlocking::enter();
            task.wait_measured(None, std::future::pending(), || {})
        })).expect_err("same actual producer panic must resume");
        assert_eq!(panic.downcast_ref::<u64>(), Some(&0x5a17_u64));
        assert_eq!(observed.allocations.load(Ordering::Acquire), 4);
        assert_eq!(observed.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    fn allocation_capacity_reconciliation_retains_real_excess_on_late_refusal() -> io::Result<()> {
        let mut configured = ReadBudgetMeter::new(0, 0);
        configured.memory_limit = 3;
        let observer = Arc::new(configured);
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let mut allocation = ReadAllocation::new(&meter, 3)?;
        let mut payload = Vec::<u8>::new();
        // Deliberate real over-capacity fault: the completed buffer is larger
        // than its original admitted request, not an inferred allocator model.
        payload.try_reserve_exact(64).map_err(io::Error::other)?;
        let actual = payload.capacity();
        assert!(actual > 3);
        assert_eq!(allocation.reconcile_capacity(actual).unwrap_err().kind(), io::ErrorKind::Other);
        assert_eq!(allocation.bytes(), actual as u64);
        assert_eq!(observer.memory.load(Ordering::Acquire), actual as u64);
        assert_eq!(observer.peak.load(Ordering::Acquire), actual as u64);
        assert_eq!(observer.allocations.load(Ordering::Acquire), 1);
        assert_eq!(observer.operations.load(Ordering::Acquire), 0);
        drop(payload);
        drop(allocation);
        assert_eq!(observer.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    fn allocation_split_absorb_and_mismatch_move_only_original_custody() -> io::Result<()> {
        let observer = Arc::new(ReadBudgetMeter::new(0, 0));
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let mut payload = Vec::<u8>::new();
        payload.try_reserve_exact(23).map_err(io::Error::other)?;
        let actual = payload.capacity();
        let mut allocation = ReadAllocation::adopt(&meter, actual)?;
        let mut split = allocation.split(7)?;
        assert_eq!(split.bytes(), 7);
        assert_eq!(observer.memory.load(Ordering::Acquire), actual as u64);
        assert_eq!(observer.allocations.load(Ordering::Acquire), 0);
        let other_meter: Arc<dyn NativeReadMeter> = Arc::new(ReadBudgetMeter::new(0, 0));
        let mut other = ReadAllocation::empty(&other_meter);
        assert_eq!(other.absorb(&mut split).unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(split.bytes(), 7);
        assert_eq!(other.bytes(), 0);
        allocation.absorb(&mut split)?;
        assert_eq!(split.bytes(), 0);
        assert_eq!(allocation.bytes(), actual as u64);
        assert_eq!(allocation.split(actual + 1).unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(allocation.bytes(), actual as u64);
        drop(payload);
        allocation.release(actual)?;
        assert_eq!(observer.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    fn allocation_reconciliation_records_actual_capacity_before_cancellation() -> io::Result<()> {
        let observer = Arc::new(ReadBudgetMeter::new(0, 0));
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let mut allocation = ReadAllocation::new(&meter, 3)?;
        let mut payload = Vec::<u8>::new();
        payload.try_reserve_exact(17).map_err(io::Error::other)?;
        observer.cancelled.store(true, Ordering::Release);
        let actual = payload.capacity();
        assert_eq!(allocation.reconcile_capacity(actual).unwrap_err().kind(), io::ErrorKind::Interrupted);
        assert_eq!(allocation.bytes(), actual as u64);
        assert_eq!(observer.memory.load(Ordering::Acquire), actual as u64);
        assert_eq!(observer.allocations.load(Ordering::Acquire), 1);
        drop(payload);
        drop(allocation);
        assert_eq!(observer.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn owned_native_file_claim_follows_physical_last_arc_not_capability_drop() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("owned-file-custody");
        std::fs::write(&path, b"x")?;
        let observer = Arc::new(ReadBudgetMeter::new(16, 1));
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let native = NativeFile::from_file_measured_owned(File::open(&path)?, meter)?;
        let bytes = native.logical_owned_bytes();
        let retained = Arc::clone(&native.file);
        let weak = Arc::downgrade(&retained);
        assert_eq!(observer.memory.load(Ordering::Acquire), bytes);
        drop(native);
        assert!(weak.upgrade().is_some());
        assert_eq!(observer.memory.load(Ordering::Acquire), bytes);
        drop(retained);
        assert!(weak.upgrade().is_none());
        assert_eq!(observer.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn dropped_queued_native_observer_keeps_file_claim_through_real_read() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("queued-file-custody");
        std::fs::write(&path, b"x")?;
        let observer = Arc::new(ReadBudgetMeter::new(16, 1));
        let meter: Arc<dyn NativeReadMeter> = observer.clone();
        let native = NativeFile::from_file_measured_owned(File::open(&path)?, meter.clone())?;
        let file_bytes = native.logical_owned_bytes();
        let weak = Arc::downgrade(&native.file);
        let (started, start) = mpsc::sync_channel(1);
        let (release, released) = mpsc::sync_channel(1);
        let mut operation = native.control_bounded_async(meter, move |file, meter| {
            started.send(()).map_err(io::Error::other)?;
            released.recv().map_err(io::Error::other)?;
            let mut byte = [0_u8; 1];
            assert_eq!(read_declared_source_bytes(file, 0, &mut byte, meter)?, 1);
            assert_eq!(byte, *b"x");
            Ok(())
        })?;
        let waker = completion_waker();
        assert!(Pin::new(&mut operation).poll(&mut Context::from_waker(&waker)).is_pending());
        start.recv_timeout(Duration::from_secs(10)).map_err(io::Error::other)?;
        drop(native);
        drop(operation);
        assert!(weak.upgrade().is_some());
        assert!(observer.memory.load(Ordering::Acquire) >= file_bytes);
        release.send(()).map_err(io::Error::other)?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while observer.memory.load(Ordering::Acquire) != 0 {
            if Instant::now() >= deadline { return Err(io::ErrorKind::TimedOut.into()); }
            std::thread::yield_now();
        }
        assert!(weak.upgrade().is_none());
        assert_eq!(observer.read.load(Ordering::Acquire), 1);
        assert_eq!(observer.operations.load(Ordering::Acquire), 2);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn declared_source_invalid_range_records_no_physical_attempt() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("declared-invalid-range");
        std::fs::write(&path, b"x")?;
        let mut file = File::open(path)?;
        for offset in [u64::MAX, i64::MAX as u64] {
            let meter = ReadBudgetMeter::new(1, 1);
            let mut destination = [0x55];
            assert_eq!(
                read_declared_source_bytes(&file, offset, &mut destination, &meter).unwrap_err().kind(),
                io::ErrorKind::InvalidInput,
            );
            assert_eq!(destination, [0x55]);
            assert_eq!(file.stream_position()?, 0);
            assert_eq!(meter.operations.load(Ordering::Acquire), 0);
            assert_eq!(meter.read.load(Ordering::Acquire), 0);
            assert_eq!(*meter.held_operations(), 0);
            assert_eq!(*meter.held_bytes(), 0);
            assert_eq!(meter.allocations.load(Ordering::Acquire), 0);
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn declared_source_refusal_precedes_io_and_preserves_cursor() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("declared-refusal");
        std::fs::write(&path, b"abcdef")?;
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(3))?;
        for meter in [ReadBudgetMeter::new(0, 8), ReadBudgetMeter::new(1, 0)] {
            let mut destination = [0x55; 8];
            assert!(read_declared_source_bytes(&file, 0, &mut destination, &meter).is_err());
            assert_eq!(destination, [0x55; 8]);
            assert_eq!(file.stream_position()?, 3);
            assert_eq!(meter.operations.load(Ordering::Acquire), 0);
            assert_eq!(meter.read.load(Ordering::Acquire), 0);
            assert_eq!(*meter.held_operations(), 0);
            assert_eq!(*meter.held_bytes(), 0);
            assert_eq!(meter.allocations.load(Ordering::Acquire), 0);
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn declared_source_records_short_prefix_before_late_cancellation() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("declared-prefix");
        std::fs::write(&path, b"abcdef")?;
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(1))?;
        for cancel_after_read in [false, true] {
            let mut meter = ReadBudgetMeter::new(1, 8);
            meter.cancel_after_read = cancel_after_read;
            let mut destination = [0x55; 8];
            let result = read_declared_source_bytes(&file, 4, &mut destination, &meter);
            if cancel_after_read {
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
            } else {
                assert_eq!(result?, 2);
            }
            assert_eq!(&destination[..2], b"ef");
            assert_eq!(&destination[2..], &[0x55; 6]);
            assert_eq!(file.stream_position()?, 1);
            assert_eq!(meter.operations.load(Ordering::Acquire), 1);
            assert_eq!(meter.read.load(Ordering::Acquire), 2);
            assert_eq!(*meter.held_operations(), 0);
            assert_eq!(*meter.held_bytes(), 0);
            assert_eq!(meter.allocations.load(Ordering::Acquire), 0);
            assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn declared_source_empty_destination_only_checks_original_request() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        // A directory cannot supply file bytes: success proves no read syscall.
        let file = File::open(temporary.path())?;
        let meter = ReadBudgetMeter::new(0, 0);
        assert_eq!(read_declared_source_bytes(&file, u64::MAX, &mut [], &meter)?, 0);
        meter.cancelled.store(true, Ordering::Release);
        assert_eq!(
            read_declared_source_bytes(&file, 0, &mut [], &meter).unwrap_err().kind(),
            io::ErrorKind::Interrupted,
        );
        assert_eq!(meter.operations.load(Ordering::Acquire), 0);
        assert_eq!(meter.read.load(Ordering::Acquire), 0);
        assert_eq!(*meter.held_operations(), 0);
        assert_eq!(*meter.held_bytes(), 0);
        assert_eq!(meter.allocations.load(Ordering::Acquire), 0);
        assert_eq!(meter.headers.load(Ordering::Acquire), 0);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn declared_source_os_failure_preserves_attempt_and_refunds_body() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = File::open(temporary.path())?;
        let meter = ReadBudgetMeter::new(1, 8);
        assert!(read_declared_source_bytes(&file, 0, &mut [0; 8], &meter).is_err());
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        assert_eq!(meter.read.load(Ordering::Acquire), 0);
        assert_eq!(*meter.held_operations(), 0);
        assert_eq!(*meter.held_bytes(), 0);
        Ok(())
    }

    #[test]
    fn read_request_credits_protect_completion_from_reentrant_admission() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("reserved-read");
        std::fs::write(&path, b"x")?;
        let mut file = std::fs::File::open(&path)?;
        let meter = ReadBudgetMeter::new(2, 2);
        let mut request = NativeOperationReservation::new(&meter, NativeOperationKind::Read, 2)?;
        assert_eq!(meter.operations.load(Ordering::Acquire), 0);
        let mut byte = [0; 1];
        let (first, recorded) = request.attempt(|| {
            let result = std::io::Read::read(&mut file, &mut byte);
            let callback = || meter.admit_read(1);
            assert!(callback().is_err());
            result
        });
        recorded?;
        assert_eq!(first?, 1);
        meter.record_read(1)?;
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        meter.cancelled.store(true, Ordering::Release);
        let (completion, recorded) = request.attempt(|| std::io::Read::read(&mut file, &mut byte));
        recorded?;
        assert_eq!(completion?, 0);
        drop(request);
        assert_eq!(meter.operations.load(Ordering::Acquire), 2);
        assert_eq!(*meter.held_operations(), 0);
        Ok(())
    }

    #[test]
    fn immediate_read_eof_refunds_unused_request_credit() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("empty-reserved-read");
        std::fs::write(&path, b"")?;
        let mut file = std::fs::File::open(&path)?;
        let meter = ReadBudgetMeter::new(2, 1);
        let mut request = NativeOperationReservation::new(&meter, NativeOperationKind::Read, 2)?;
        let mut byte = [0; 1];
        let (read, recorded) = request.attempt(|| std::io::Read::read(&mut file, &mut byte));
        recorded?;
        assert_eq!(read?, 0);
        drop(request);
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        assert_eq!(*meter.held_operations(), 0);
        meter.admit_read(1)?;
        let bytes = std::io::Read::read(&mut file, &mut byte)?;
        meter.record_read(bytes)?;
        assert_eq!(bytes, 0);
        assert_eq!(meter.operations.load(Ordering::Acquire), 2);
        Ok(())
    }

    #[test]
    fn read_byte_reservation_protects_transfer_and_refunds_short_read() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("reserved-bytes");
        std::fs::write(&path, b"abc")?;
        let mut file = std::fs::File::open(&path)?;
        let meter = ReadBudgetMeter::new(2, 8);
        let mut bytes = NativeByteReservation::new(&meter, NativeOperationKind::Read, 8)?;
        assert_eq!(meter.read.load(Ordering::Acquire), 0);
        let mut request = NativeOperationReservation::new(&meter, NativeOperationKind::Read, 1)?;
        let mut buffer = [0; 8];
        let (read, recorded) = request.attempt(|| {
            let result = std::io::Read::read(&mut file, &mut buffer);
            let callback = || meter.admit_read(1);
            assert!(callback().is_err());
            result
        });
        recorded?;
        let actual = read?;
        assert_eq!(actual, 3);
        bytes.record(actual)?;
        drop(bytes);
        drop(request);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        assert_eq!(*meter.held_bytes(), 0);
        assert!(meter.admit_bytes(6).is_err());
        let unused = NativeByteReservation::new(&meter, NativeOperationKind::Read, 5)?;
        assert!(meter.admit_bytes(1).is_err());
        drop(unused);
        meter.admit_read(5)?;
        let actual = std::io::Read::read(&mut file, &mut buffer[..5])?;
        meter.record_read(actual)?;
        assert_eq!(actual, 0);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        assert_eq!(meter.operations.load(Ordering::Acquire), 2);
        Ok(())
    }

    #[test]
    fn allocation_reservation_withholds_both_limits_without_phantom_work() -> io::Result<()> {
        let mut policy = ReadBudgetMeter::new(0, 0);
        policy.allocation_limit = 1;
        policy.memory_limit = 8;
        let observed = Arc::new(policy);
        let meter: Arc<dyn NativeReadMeter> = observed.clone();
        let reserved = ReadAllocationReservation::new(&meter, 8)?;
        assert_eq!(observed.allocations.load(Ordering::Acquire), 0);
        assert_eq!(observed.memory.load(Ordering::Acquire), 0);
        assert_eq!(observed.peak.load(Ordering::Acquire), 0);
        assert!(ReadAllocationReservation::new(&meter, 0).is_err());
        assert!(ReadAllocation::adopt(&meter, 1).is_err());
        drop(reserved);
        assert_eq!(*observed.pending_allocations.lock(), (0, 0));
        let allocation = ReadAllocationReservation::new(&meter, 8)?.allocate()?;
        struct FundedBox {
            payload: Box<[u8; 8]>,
            _allocation: ReadAllocation,
        }
        let owner = FundedBox { payload: Box::new(*b"reserved"), _allocation: allocation };
        assert_eq!(&*owner.payload, b"reserved");
        assert_eq!(observed.allocations.load(Ordering::Acquire), 1);
        assert_eq!(observed.memory.load(Ordering::Acquire), 8);
        drop(owner);
        assert_eq!(observed.memory.load(Ordering::Acquire), 0);
        assert_eq!(observed.peak.load(Ordering::Acquire), 8);
        Ok(())
    }

    #[test]
    fn allocation_reservation_consumes_accepted_credit_after_cancellation() -> io::Result<()> {
        let mut policy = ReadBudgetMeter::new(0, 0);
        policy.allocation_limit = 1;
        policy.memory_limit = 8;
        let observed = Arc::new(policy);
        let meter: Arc<dyn NativeReadMeter> = observed.clone();
        let reserved = ReadAllocationReservation::new(&meter, 8)?;
        observed.cancelled.store(true, Ordering::Release);
        let allocation = reserved.allocate()?;
        struct FundedBox {
            payload: Box<[u8; 8]>,
            _allocation: ReadAllocation,
        }
        let owner = FundedBox { payload: Box::new(*b"accepted"), _allocation: allocation };
        assert_eq!(&*owner.payload, b"accepted");
        assert_eq!(*observed.pending_allocations.lock(), (0, 0));
        assert_eq!(observed.allocations.load(Ordering::Acquire), 1);
        assert!(ReadAllocationReservation::new(&meter, 1).is_err());
        drop(owner);
        assert_eq!(observed.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_read_refusal_retains_actual_partial_io() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("partial");
        std::fs::write(&path, b"abc")?;
        let file = bounded_write_file(&path, false)?;
        let physical_calls = if cfg!(windows) { 2 } else { 1 };
        let meter = Arc::new(ReadBudgetMeter::new(physical_calls, 8));
        let read = file.read_bounded_async(0, 8, meter.clone())?;
        assert!({
            let _inline = InlineBlocking::enter();
            complete_measured(read)
        }.is_err());
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        assert_eq!(meter.operations.load(Ordering::Acquire), physical_calls);
        assert!(meter.allocations.load(Ordering::Acquire) >= 3);
        assert_eq!(meter.headers.load(Ordering::Acquire), 0);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        assert!(meter.peak.load(Ordering::Acquire) >= 8);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_read_cancellation_preserves_completed_prefix() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("cancel");
        std::fs::write(&path, b"abc")?;
        let file = bounded_write_file(&path, false)?;
        let mut limits = ReadBudgetMeter::new(8, 8);
        limits.cancel_after_read = true;
        let meter = Arc::new(limits);
        let read = file.read_bounded_async(0, 8, meter.clone())?;
        let error = match {
            let _inline = InlineBlocking::enter();
            complete_measured(read)
        } {
            Err(error) => error,
            Ok(_) => panic!("cancelled read returned success"),
        };
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        assert_eq!(meter.operations.load(Ordering::Acquire), if cfg!(windows) { 2 } else { 1 });
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_short_read_transfers_actual_capacity_and_header_operation() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("eof");
        std::fs::write(&path, b"abc")?;
        let file = bounded_write_file(&path, false)?;
        let meter = Arc::new(ReadBudgetMeter::new(8, 8));
        let read = file.read_bounded_async(0, 8, meter.clone())?;
        let result = {
            let _inline = InlineBlocking::enter();
            complete_measured(read)
        }?;
        assert_eq!(result.bytes.as_ref(), b"abc");
        assert_eq!(result.buffer_capacity, 8);
        assert_eq!(meter.memory.load(Ordering::Acquire), result.buffer_capacity as u64);
        assert_eq!(meter.headers.load(Ordering::Acquire), 1);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        assert!(file.tail.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_none());
        assert_eq!(meter.operations.load(Ordering::Acquire), if cfg!(windows) { 3 } else { 2 });
        drop(result);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn prepared_read_returns_same_owner_short_prefix_and_eof_without_header() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("prepared-prefix");
        std::fs::write(&path, b"abc")?;
        let file = bounded_write_file(&path, false)?;
        for (offset, expected) in [(0, 3), (3, 0)] {
            let meter = Arc::new(ReadBudgetMeter::new(8, 8));
            let storage = vec![0x55; 8];
            let identity = storage.as_ptr();
            let read = file.read_prepared_bounded_async(offset, storage, 8, false, meter.clone())?;
            let (storage, filled) = {
                let _blocking = InlineBlocking::enter();
                complete_measured(read)
            }?;
            assert_eq!(filled, expected);
            assert_eq!(storage.as_ptr(), identity);
            assert_eq!(&storage[..filled], &b"abc"[..expected]);
            assert!(storage[filled..].iter().all(|byte| *byte == 0x55));
            assert_eq!(meter.read.load(Ordering::Acquire), expected as u64);
            assert_eq!(meter.headers.load(Ordering::Acquire), 0);
            assert!(file.tail.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_none());
            let weak = Arc::downgrade(&meter);
            drop(meter);
            let deadline = Instant::now() + Duration::from_secs(10);
            while weak.upgrade().is_some() && Instant::now() < deadline {
                std::thread::yield_now();
            }
            assert!(weak.upgrade().is_none(), "returned owner must not retain invocation custody");
            drop(storage);
        }
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn prepared_read_exact_eof_and_late_cancel_preserve_physical_work() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("prepared-refusal");
        std::fs::write(&path, b"abc")?;
        let file = bounded_write_file(&path, false)?;
        for late_cancel in [false, true] {
            let mut limits = ReadBudgetMeter::new(8, 8);
            limits.cancel_after_read = late_cancel;
            let meter = Arc::new(limits);
            let read = file.read_prepared_bounded_async(0, [0x55; 8], 8, true, meter.clone())?;
            let error = {
                let _blocking = InlineBlocking::enter();
                complete_measured(read)
            }.unwrap_err();
            assert_eq!(error.kind(), if late_cancel {
                io::ErrorKind::Interrupted
            } else { io::ErrorKind::UnexpectedEof });
            assert_eq!(meter.read.load(Ordering::Acquire), 3);
            assert_eq!(meter.headers.load(Ordering::Acquire), 0);
            let deadline = Instant::now() + Duration::from_secs(10);
            while meter.memory.load(Ordering::Acquire) != 0 && Instant::now() < deadline {
                std::thread::yield_now();
            }
            assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        }
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn prepared_read_panic_preserves_original_payload_and_actual_work() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("prepared-panic");
        std::fs::write(&path, b"abc")?;
        let file = bounded_write_file(&path, false)?;
        let mut limits = ReadBudgetMeter::new(8, 3);
        limits.panic_after_read = true;
        let meter = Arc::new(limits);
        let mut read = file.read_prepared_bounded_async(0, [0; 3], 3, true, meter.clone())?;
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _blocking = InlineBlocking::enter();
            complete_measured(&mut read)
        })).expect_err("prepared observer resumes original terminal panic");
        assert_eq!(payload.downcast_ref::<u64>(), Some(&0x7072_6570_6172_6564_u64));
        assert!(read.terminated);
        assert!(!read.state.is_poisoned());
        assert!(read.state.try_lock().expect("observer released state").panic.is_none());
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        assert_eq!(meter.operations.load(Ordering::Acquire), if cfg!(windows) { 2 } else { 1 });
        assert_eq!(meter.headers.load(Ordering::Acquire), 0);
        drop(payload);
        drop(read);
        let deadline = Instant::now() + Duration::from_secs(10);
        while meter.memory.load(Ordering::Acquire) != 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        assert!(file.tail.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_none());
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn prepared_read_bounds_and_header_denial_precede_io() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("prepared-bounds");
        std::fs::write(&path, b"abc")?;
        let file = bounded_write_file(&path, false)?;
        for (offset, length) in [(0, 9), (u64::MAX, 1)] {
            let meter = Arc::new(ReadBudgetMeter::new(8, 9));
            assert!(file.read_prepared_bounded_async(offset, [0; 8], length, false, meter.clone()).is_err());
            assert_eq!(meter.operations.load(Ordering::Acquire), 0);
            assert_eq!(meter.allocations.load(Ordering::Acquire), 0);
        }
        let mut limits = ReadBudgetMeter::new(8, 8);
        limits.memory_limit = 0;
        let meter = Arc::new(limits);
        assert!(file.read_prepared_bounded_async(0, [0; 8], 8, false, meter.clone()).is_err());
        assert_eq!(meter.operations.load(Ordering::Acquire), 0);
        assert_eq!(meter.allocations.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn prepared_read_dropped_accepted_observer_keeps_original_meter_until_terminal() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("prepared-observer-drop");
        std::fs::write(&path, b"abc")?;
        let file = bounded_write_file(&path, false)?;
        struct PreparedProbe {
            data: [u8; 3],
            calls: usize,
            started: mpsc::SyncSender<()>,
            released: mpsc::Receiver<()>,
            dropped_prefix: Arc<AtomicBool>,
        }
        impl AsMut<[u8]> for PreparedProbe {
            fn as_mut(&mut self) -> &mut [u8] {
                self.calls += 1;
                if self.calls == 2 {
                    self.started.send(()).expect("observer receives accepted job");
                    self.released.recv().expect("observer releases accepted job");
                }
                &mut self.data
            }
        }
        impl Drop for PreparedProbe {
            fn drop(&mut self) {
                self.dropped_prefix.store(self.data == *b"abc", Ordering::Release);
            }
        }
        let (started, start) = mpsc::sync_channel(1);
        let (release, released) = mpsc::sync_channel(1);
        let dropped_prefix = Arc::new(AtomicBool::new(false));
        let meter = Arc::new(ReadBudgetMeter::new(1, 3));
        let weak = Arc::downgrade(&meter);
        let storage = PreparedProbe { data: [0; 3], calls: 0, started, released,
            dropped_prefix: Arc::clone(&dropped_prefix) };
        let mut read = file.read_prepared_bounded_async(0, storage, 3, true, meter.clone())?;
        let waker = completion_waker();
        assert!(Pin::new(&mut read).poll(&mut Context::from_waker(&waker)).is_pending());
        start.recv_timeout(Duration::from_secs(10)).map_err(io::Error::other)?;
        drop(read);
        drop(meter);
        assert!(weak.upgrade().is_some());
        assert!(!dropped_prefix.load(Ordering::Acquire));
        release.send(()).map_err(io::Error::other)?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while weak.upgrade().is_some() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(weak.upgrade().is_none());
        assert!(dropped_prefix.load(Ordering::Acquire));
        assert!(file.tail.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_none());
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_read_byte_and_memory_refusals_precede_io() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("admission");
        std::fs::write(&path, b"abc")?;
        let file = bounded_write_file(&path, false)?;
        let byte_meter = Arc::new(ReadBudgetMeter::new(8, 2));
        assert!(file.read_bounded_async(0, 3, byte_meter.clone()).is_err());
        assert_eq!(byte_meter.operations.load(Ordering::Acquire), 0);
        assert_eq!(byte_meter.allocations.load(Ordering::Acquire), 0);
        let mut limits = ReadBudgetMeter::new(8, 3);
        limits.memory_limit = 0;
        let memory_meter = Arc::new(limits);
        assert!(file.read_bounded_async(0, 3, memory_meter.clone()).is_err());
        assert_eq!(memory_meter.operations.load(Ordering::Acquire), 0);
        assert_eq!(memory_meter.allocations.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn completed_bounded_tail_retires_original_meter_without_another_request() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("retired-tail");
        std::fs::write(&path, b"abc")?;
        let file = bounded_write_file(&path, false)?;
        let meter = Arc::new(ReadBudgetMeter::new(8, 3));
        let borrowed = Arc::downgrade(&meter);
        let read = file.read_bounded_async(0, 3, meter.clone())?;
        let result = {
            let _blocking = InlineBlocking::enter();
            complete_measured(read)
        }?;
        assert_eq!(result.bytes.as_ref(), b"abc");
        assert!(file.tail.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_none());
        drop(result);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        drop(meter);
        assert!(borrowed.upgrade().is_none());
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn zero_bounded_request_has_no_state_fence_buffer_or_waiter() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("zero-request");
        std::fs::write(&path, b"real file")?;
        let file = NativeFile::from_file(File::open(&path)?)?;
        let mut limits = ReadBudgetMeter::new(0, 0);
        limits.memory_limit = 0;
        let meter = Arc::new(limits);
        for synchronous in [false, true] {
            let completion = file.read_bounded_async(u64::MAX, 0, meter.clone())?;
            let result = if synchronous {
                completion.wait_measured(
                    Some(std::time::Instant::now()),
                    std::future::pending(),
                    || panic!("an inline zero read must not start a deadline waiter"),
                )?
            } else {
                complete_measured(completion)?
            };
            assert!(result.bytes.is_empty());
            assert_eq!(result.buffer_capacity, 0);
            assert_eq!(meter.operations.load(Ordering::Acquire), 0);
            assert_eq!(meter.read.load(Ordering::Acquire), 0);
            assert_eq!(meter.allocations.load(Ordering::Acquire), 0);
            assert_eq!(meter.headers.load(Ordering::Acquire), 0);
            assert_eq!(meter.memory.load(Ordering::Acquire), 0);
            assert_eq!(meter.peak.load(Ordering::Acquire), 0);
        }
        meter.cancelled.store(true, Ordering::Release);
        let error = file.read_bounded_async(0, 0, meter.clone()).err()
            .expect("zero reads retain the original cancellation boundary");
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(meter.operations.load(Ordering::Acquire), 0);
        assert_eq!(meter.allocations.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn ordinary_handle_bounded_read_counts_real_preparation_and_preserves_cursor() -> io::Result<()> {
        use std::io::{Seek, SeekFrom};
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("ordinary-cursor");
        std::fs::write(&path, b"abc")?;
        let mut input = File::open(&path)?;
        input.seek(SeekFrom::Start(2))?;
        let file = NativeFile::from_file(input)?;
        let meter = Arc::new(ReadBudgetMeter::new(4, 3));
        let result = {
            let _inline = InlineBlocking::enter();
            complete_measured(file.read_bounded_async(0, 3, meter.clone())?)
        }?;
        assert_eq!(result.bytes.as_ref(), b"abc");
        assert_eq!(meter.operations.load(Ordering::Acquire), 4);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        let mut cursor = file.as_file();
        assert_eq!(cursor.stream_position()?, 2);
        drop(result);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn ordinary_handle_refusal_keeps_real_query_and_reopen_prefix() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("ordinary-refusal");
        std::fs::write(&path, b"abc")?;
        let file = NativeFile::from_file(File::open(&path)?)?;
        let meter = Arc::new(ReadBudgetMeter::new(2, 3));
        let result = {
            let _inline = InlineBlocking::enter();
            complete_measured(file.read_bounded_async(0, 3, meter.clone())?)
        };
        assert!(result.is_err());
        assert_eq!(meter.operations.load(Ordering::Acquire), 2);
        assert_eq!(meter.read.load(Ordering::Acquire), 0);
        assert!(meter.allocations.load(Ordering::Acquire) >= 3);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn empty_measured_range_has_no_io_or_allocation() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("empty");
        File::create(&path)?;
        let file = Arc::new(bounded_write_file(&path, false)?);
        let mut source = AsyncRangeReader::new(file, 0, 0);
        let mut limits = ReadBudgetMeter::new(0, 0);
        limits.memory_limit = 0;
        let meter = Arc::new(limits);
        let result = complete_measured(source.read_measured(8, meter.clone()))?;
        assert!(result.bytes.is_empty());
        assert_eq!(result.buffer_capacity, 0);
        assert_eq!(meter.operations.load(Ordering::Acquire), 0);
        assert_eq!(meter.allocations.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    #[allow(unsafe_code, reason = "Windows test opens its own overlapped attributes-only handle")]
    fn bounded_read_os_failure_records_actual_request_without_bytes() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("denied");
        std::fs::write(&path, b"abc")?;
        let mut options = OpenOptions::new();
        #[cfg(windows)]
        let file = {
            use std::os::windows::fs::OpenOptionsExt as _;
            use windows_sys::Win32::Storage::FileSystem::{FILE_FLAG_OVERLAPPED, FILE_READ_ATTRIBUTES};
            options.access_mode(FILE_READ_ATTRIBUTES).custom_flags(FILE_FLAG_OVERLAPPED);
            // SAFETY: this fresh handle was opened with FILE_FLAG_OVERLAPPED.
            unsafe { NativeFile::from_overlapped_file_unchecked(options.open(&path)?)? }
        };
        #[cfg(not(windows))]
        let file = {
            options.write(true);
            NativeFile::from_file(options.open(&path)?)?
        };
        let meter = Arc::new(ReadBudgetMeter::new(8, 8));
        let read = file.read_bounded_async(0, 3, meter.clone())?;
        assert!({
            let _inline = InlineBlocking::enter();
            complete_measured(read)
        }.is_err());
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        assert_eq!(meter.read.load(Ordering::Acquire), 0);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    fn measured_host_task_memory_refusal_precedes_body() {
        let mut limits = ReadBudgetMeter::new(0, 0);
        limits.memory_limit = 0;
        let meter = Arc::new(limits);
        let called = Arc::new(AtomicBool::new(false));
        let body = called.clone();
        assert!(run_blocking_io_measured(
            move || body.store(true, Ordering::Release), meter.clone(),
        ).is_err());
        assert!(!called.load(Ordering::Acquire));
        assert_eq!(meter.allocations.load(Ordering::Acquire), 0);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    }

    #[test]
    fn measured_host_task_cancelled_after_admission_retains_envelope_work() {
        let meter = Arc::new(ReadBudgetMeter::new(0, 0));
        let called = Arc::new(AtomicBool::new(false));
        let body = called.clone();
        let task = run_blocking_io_measured(
            move || body.store(true, Ordering::Release), meter.clone(),
        ).expect("finite envelope budget");
        meter.cancelled.store(true, Ordering::Release);
        let error = {
            let _inline = InlineBlocking::enter();
            complete_measured(task)
        }
            .expect_err("cancelled envelope must not execute its body");
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert!(!called.load(Ordering::Acquire));
        assert_eq!(meter.allocations.load(Ordering::Acquire), 2);
        assert!(meter.peak.load(Ordering::Acquire) > 0);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    }

    #[test]
    fn measured_host_task_panic_preserves_real_envelope_allocations() {
        let meter = Arc::new(ReadBudgetMeter::new(0, 0));
        let task = run_blocking_io_measured(
            || -> () { panic!("controlled native host task panic") }, meter.clone(),
        ).expect("finite envelope budget");
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _inline = InlineBlocking::enter();
            complete_measured(task)
        })).expect_err("the observing poll must resume the original panic");
        assert_eq!(payload.downcast_ref::<&'static str>(),
            Some(&"controlled native host task panic"));
        assert_eq!(meter.allocations.load(Ordering::Acquire), 2);
        assert!(meter.peak.load(Ordering::Acquire) > 0);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
    }

    #[derive(Debug)]
    struct OriginalHostTaskPanic {
        marker: u64,
        tag: String,
    }

    #[test]
    fn measured_host_task_panic_retains_physical_prefix_and_original_box() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("panic-prefix");
        std::fs::write(&path, b"abc")?;
        let mut file = File::open(path)?;
        let meter = Arc::new(ReadBudgetMeter::new(1, 3));
        let body_meter = Arc::clone(&meter);
        let address = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let body_address = Arc::clone(&address);
        let operation_dropped = Arc::new(AtomicBool::new(false));
        let body_dropped = Arc::clone(&operation_dropped);
        struct PhysicalScope(Arc<AtomicBool>);
        impl Drop for PhysicalScope {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let mut task = run_blocking_io_measured(move || -> io::Result<()> {
            let _scope = PhysicalScope(body_dropped);
            let mut buffer = [0_u8; 3];
            body_meter.admit_read(buffer.len())?;
            let completed = file.read(&mut buffer)?;
            body_meter.record_read(completed)?;
            assert_eq!(completed, 3);
            assert_eq!(&buffer, b"abc");
            let payload = std::panic::catch_unwind(|| -> () {
                std::panic::panic_any(OriginalHostTaskPanic {
                    marker: 0x6e61_7469_7665,
                    tag: "owned physical-read panic".to_owned(),
                });
            }).expect_err("physical operation must panic after its real read");
            body_address.store(
                payload.downcast_ref::<OriginalHostTaskPanic>()
                    .expect("original typed payload") as *const OriginalHostTaskPanic as usize,
                Ordering::Release,
            );
            std::panic::resume_unwind(payload);
        }, meter.clone())?;
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            complete_measured(&mut task)
        })).expect_err("observer must resume the operation's original panic");
        let original = payload.downcast_ref::<OriginalHostTaskPanic>()
            .expect("panic payload must not be erased or boxed again");
        assert_eq!(original.marker, 0x6e61_7469_7665);
        assert_eq!(original.tag, "owned physical-read panic");
        assert_eq!(original as *const OriginalHostTaskPanic as usize,
            address.load(Ordering::Acquire));
        assert!(operation_dropped.load(Ordering::Acquire));
        assert!(task.terminated);
        assert!(!task.state.is_poisoned());
        assert!(task.state.try_lock().expect("poll releases completion lock").panic.is_none());
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        drop(payload);
        drop(task);
        // Publication can race the worker's final closure-allocation retirement.
        let deadline = Instant::now() + Duration::from_secs(10);
        while meter.memory.load(Ordering::Acquire) != 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        assert!(meter.allocations.load(Ordering::Acquire) >= 2);
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        Ok(())
    }

    #[test]
    fn host_task_observer_resumes_original_owned_panic() {
        let mut task = run_blocking_io(|| -> () {
            std::panic::panic_any(OriginalHostTaskPanic {
                marker: 19,
                tag: "ordinary host task".to_owned(),
            });
        });
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            complete_measured(&mut task)
        })).expect_err("ordinary task panic must reach its observer");
        let original = payload.downcast_ref::<OriginalHostTaskPanic>()
            .expect("original owned payload type");
        assert_eq!(original.marker, 19);
        assert_eq!(original.tag, "ordinary host task");
        assert!(task.terminated);
        assert!(!task.state.is_poisoned());
    }

    #[test]
    fn native_completion_resumes_original_task_panic() {
        let mut task = run_blocking_io(|| -> () {
            std::panic::panic_any(OriginalHostTaskPanic {
                marker: 23,
                tag: "generic native completion".to_owned(),
            });
        });
        let mut completion = NativeCompletion {
            state: Arc::clone(&task.state),
            pending: task.pending.take(),
            waiter: None,
            sequencer: None,
            uncertain: None,
            predecessor: None,
            completion: None,
            terminated: false,
        };
        drop(task);
        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            complete_measured(&mut completion)
        })).expect_err("generic native completion must resume the same task panic");
        let original = payload.downcast_ref::<OriginalHostTaskPanic>()
            .expect("original owned payload type");
        assert_eq!(original.marker, 23);
        assert_eq!(original.tag, "generic native completion");
        assert!(completion.terminated);
        assert!(!completion.state.is_poisoned());
        assert!(completion.pending.is_none());
        assert!(completion.waiter.is_none());
        assert!(completion.predecessor.is_none());
        assert!(completion.completion.is_none());
    }

    #[test]
    fn dropped_host_observer_retires_retained_panic_after_actual_read() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("unobserved-panic-prefix");
        std::fs::write(&path, b"abc")?;
        let mut file = File::open(path)?;
        let meter = Arc::new(ReadBudgetMeter::new(1, 3));
        let body_meter = Arc::clone(&meter);
        let payload_dropped = Arc::new(AtomicBool::new(false));
        let body_dropped = Arc::clone(&payload_dropped);
        struct RetainedHostPanic {
            tag: String,
            dropped: Arc<AtomicBool>,
        }
        impl Drop for RetainedHostPanic {
            fn drop(&mut self) {
                self.dropped.store(true, Ordering::Release);
            }
        }
        let (prefix_sender, prefix_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let mut task = run_blocking_io_measured(move || -> io::Result<()> {
            let mut buffer = [0_u8; 3];
            body_meter.admit_read(buffer.len())?;
            let completed = file.read(&mut buffer)?;
            body_meter.record_read(completed)?;
            assert_eq!(completed, 3);
            assert_eq!(&buffer, b"abc");
            prefix_sender.send(()).map_err(io::Error::other)?;
            release_receiver.recv().map_err(io::Error::other)?;
            std::panic::panic_any(RetainedHostPanic {
                tag: "retained after actual OS completion".to_owned(),
                dropped: body_dropped,
            });
        }, meter.clone())?;
        let waker = completion_waker();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(Pin::new(&mut task).poll(&mut Context::from_waker(&waker)).is_pending());
            match prefix_receiver.try_recv() {
                Ok(()) => break,
                Err(mpsc::TryRecvError::Empty) if Instant::now() < deadline =>
                    std::thread::park_timeout(Duration::from_millis(1)),
                Err(error) => return Err(io::Error::other(error)),
            }
        }
        release_sender.send(()).map_err(io::Error::other)?;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let state = task.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(payload) = state.panic.as_ref() {
                let original = payload.downcast_ref::<RetainedHostPanic>()
                    .expect("unobserved completion retains the original owned payload");
                assert_eq!(original.tag, "retained after actual OS completion");
                break;
            }
            drop(state);
            assert!(Instant::now() < deadline, "physical operation must reach terminal panic");
            std::thread::yield_now();
        }
        assert!(!task.terminated);
        assert!(!payload_dropped.load(Ordering::Acquire));
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        drop(task);
        let deadline = Instant::now() + Duration::from_secs(10);
        while (!payload_dropped.load(Ordering::Acquire)
            || meter.memory.load(Ordering::Acquire) != 0) && Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        assert!(payload_dropped.load(Ordering::Acquire));
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        Ok(())
    }

    #[test]
    fn measured_sync_wait_uses_real_file_work_on_existing_pool() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("sync-wait");
        std::fs::write(&path, b"abc")?;
        let mut file = File::open(path)?;
        let meter = Arc::new(ReadBudgetMeter::new(1, 3));
        let body_meter = meter.clone();
        let task = run_blocking_io_measured(move || -> io::Result<[u8; 3]> {
            let mut buffer = [0_u8; 3];
            body_meter.admit_read(buffer.len())?;
            let completed = file.read(&mut buffer)?;
            body_meter.record_read(completed)?;
            Ok(buffer)
        }, meter.clone())?;
        let result = task.wait_measured(None, std::future::pending(), || panic!("unexpected deadline"))??;
        assert_eq!(&result, b"abc");
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        assert!(meter.allocations.load(Ordering::Acquire) >= 3);
        Ok(())
    }

    #[test]
    fn measured_sync_wait_expired_deadline_stops_before_body() -> io::Result<()> {
        initialize_native_io_services()?;
        let meter = Arc::new(ReadBudgetMeter::new(0, 0));
        let called = Arc::new(AtomicBool::new(false));
        let body_called = called.clone();
        let task = run_blocking_io_measured(
            move || body_called.store(true, Ordering::Release), meter.clone(),
        )?;
        let mut deadline_calls = 0;
        let error = task.wait_measured(Some(Instant::now()), std::future::pending(), || {
            deadline_calls += 1;
            meter.cancelled.store(true, Ordering::Release);
        }).expect_err("expired deadline must stop the unsubmitted body");
        assert_eq!(deadline_calls, 1);
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert!(!called.load(Ordering::Acquire));
        assert_eq!(meter.operations.load(Ordering::Acquire), 0);
        assert_eq!(meter.allocations.load(Ordering::Acquire), 3);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    fn measured_sync_wait_deadline_drains_the_actual_terminal_body() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("sync-deadline");
        std::fs::write(&path, b"abc")?;
        let mut file = File::open(path)?;
        let meter = Arc::new(ReadBudgetMeter::new(1, 3));
        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let terminal = Arc::new(AtomicBool::new(false));
        let body_gate = gate.clone();
        let body_terminal = terminal.clone();
        let body_meter = meter.clone();
        let mut task = run_blocking_io_measured(move || -> io::Result<usize> {
            let mut buffer = [0_u8; 3];
            body_meter.admit_read(buffer.len())?;
            let completed = file.read(&mut buffer)?;
            body_meter.record_read(completed)?;
            let (lock, changed) = &*body_gate;
            let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.0 = true;
            changed.notify_all();
            while !state.1 {
                state = changed.wait(state).unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            body_terminal.store(true, Ordering::Release);
            Ok(completed)
        }, meter.clone())?;
        // Admit the real queued body before making the deadline immediately
        // expired; the deadline callback must observe an actual read prefix.
        let waker = completion_waker();
        let mut context = Context::from_waker(&waker);
        loop {
            assert!(Pin::new(&mut task).poll(&mut context).is_pending());
            if task.pending.is_none() { break; }
            std::thread::park_timeout(Duration::from_millis(1));
        }
        let mut deadline_calls = 0;
        let completed = task.wait_measured(
            Some(Instant::now()),
            std::future::pending(),
            || {
                deadline_calls += 1;
                let (lock, changed) = &*gate;
                let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                while !state.0 {
                    state = changed.wait(state).unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                meter.cancelled.store(true, Ordering::Release);
                state.1 = true;
                changed.notify_all();
            },
        )??;
        assert_eq!(deadline_calls, 1);
        assert!(terminal.load(Ordering::Acquire));
        assert_eq!(completed, 3);
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        Ok(())
    }

    #[test]
    fn measured_sync_wait_cancellation_wake_drains_real_read_prefix() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("sync-cancel-wake");
        std::fs::write(&path, b"abc")?;
        let mut file = File::open(path)?;
        let meter = Arc::new(ReadBudgetMeter::new(1, 3));
        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let terminal = Arc::new(AtomicBool::new(false));
        let body_gate = gate.clone();
        let body_terminal = terminal.clone();
        let body_meter = meter.clone();
        let mut task = run_blocking_io_measured(move || -> io::Result<usize> {
            let mut buffer = [0_u8; 3];
            body_meter.admit_read(buffer.len())?;
            let completed = file.read(&mut buffer)?;
            body_meter.record_read(completed)?;
            let (lock, changed) = &*body_gate;
            let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.0 = true;
            changed.notify_all();
            while !state.1 {
                state = changed.wait(state).unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            body_terminal.store(true, Ordering::Release);
            Ok(completed)
        }, meter.clone())?;
        let waker = completion_waker();
        let mut context = Context::from_waker(&waker);
        loop {
            assert!(Pin::new(&mut task).poll(&mut context).is_pending());
            if task.pending.is_none() { break; }
            std::thread::park_timeout(Duration::from_millis(1));
        }
        let mut cancellation_polls = 0;
        let cancellation = std::future::poll_fn(|context| {
            cancellation_polls += 1;
            if cancellation_polls == 1 {
                // This wake must survive the following task poll/park, even
                // though no I/O completion or deadline can wake the caller.
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            let (lock, changed) = &*gate;
            let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            while !state.0 {
                state = changed.wait(state).unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            meter.cancelled.store(true, Ordering::Release);
            state.1 = true;
            changed.notify_all();
            Poll::Ready(())
        });
        let completed = task.wait_measured(None, cancellation, || panic!("unexpected deadline"))??;
        assert_eq!(cancellation_polls, 2);
        assert!(terminal.load(Ordering::Acquire));
        assert_eq!(completed, 3);
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        Ok(())
    }

    struct WriteBudgetMeter {
        allocation_failure: parking_lot::Mutex<Option<std::collections::TryReserveError>>,
        operations: AtomicU64,
        held_operations: parking_lot::Mutex<u64>,
        held_bytes: parking_lot::Mutex<u64>,
        written: AtomicU64,
        durability: AtomicU64,
        memory: AtomicU64,
        allocations: AtomicU64,
        pending_allocations: parking_lot::Mutex<(u64, u64)>,
        operation_limit: u64,
        byte_limit: u64,
        memory_limit: u64,
        cancel_after_write: bool,
        cancelled: AtomicBool,
    }

    impl WriteBudgetMeter {
        fn new(operation_limit: u64, byte_limit: u64) -> Self {
            Self {
                operations: AtomicU64::new(0), written: AtomicU64::new(0),
                held_operations: parking_lot::Mutex::new(0),
                held_bytes: parking_lot::Mutex::new(0),
                durability: AtomicU64::new(0), memory: AtomicU64::new(0),
                allocations: AtomicU64::new(0),
                pending_allocations: parking_lot::Mutex::new((0, 0)),
                operation_limit, byte_limit, memory_limit: u64::MAX,
                cancel_after_write: false, cancelled: AtomicBool::new(false),
                allocation_failure: parking_lot::Mutex::new(None),
            }
        }

        fn refuse() -> io::Error { io::ErrorKind::Other.into() }
    }

    impl NativeReadMeter for WriteBudgetMeter {
        fn check(&self) -> io::Result<()> {
            if self.cancelled.load(Ordering::Acquire) {
                Err(io::ErrorKind::Interrupted.into())
            } else { Ok(()) }
        }
        fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> io::Result<()> {
            if kind == NativeOperationKind::Read { return Err(io::ErrorKind::Unsupported.into()); }
            let mut held = self.held_operations.lock();
            self.check()?;
            self.operations.load(Ordering::Acquire).checked_add(*held)
                .and_then(|total| total.checked_add(count))
                .filter(|total| *total <= self.operation_limit).ok_or_else(Self::refuse)?;
            *held += count;
            Ok(())
        }
        fn cancel_reserved_operations(&self, kind: NativeOperationKind, count: u64) {
            assert!(matches!(kind, NativeOperationKind::Write | NativeOperationKind::Durability));
            let mut held = self.held_operations.lock();
            *held = held.checked_sub(count).expect("refund owns unused write allowance");
        }
        fn record_reserved_operation(&self, kind: NativeOperationKind) -> io::Result<()> {
            assert!(matches!(kind, NativeOperationKind::Write | NativeOperationKind::Durability));
            let mut held = self.held_operations.lock();
            *held = held.checked_sub(1).expect("write attempt owns reserved allowance");
            self.operations.fetch_add(1, Ordering::AcqRel);
            if kind == NativeOperationKind::Durability {
                self.durability.fetch_add(1, Ordering::AcqRel);
            }
            Ok(())
        }
        fn reserve_bytes(&self, kind: NativeOperationKind, maximum: u64) -> io::Result<()> {
            if kind != NativeOperationKind::Write { return Err(io::ErrorKind::Unsupported.into()); }
            let mut held = self.held_bytes.lock();
            self.check()?;
            self.written.load(Ordering::Acquire).checked_add(*held)
                .and_then(|total| total.checked_add(maximum))
                .filter(|total| *total <= self.byte_limit).ok_or_else(Self::refuse)?;
            *held += maximum;
            Ok(())
        }
        fn cancel_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64) {
            assert_eq!(kind, NativeOperationKind::Write);
            let mut held = self.held_bytes.lock();
            *held = held.checked_sub(maximum).expect("refund owns unused write bytes");
        }
        fn record_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64, actual: u64) -> io::Result<()> {
            assert_eq!(kind, NativeOperationKind::Write);
            assert!(actual <= maximum);
            let mut held = self.held_bytes.lock();
            *held = held.checked_sub(maximum).expect("completion owns reserved write bytes");
            self.written.fetch_add(actual, Ordering::AcqRel);
            if self.cancel_after_write { self.cancelled.store(true, Ordering::Release); }
            Ok(())
        }
        fn admit_write_bytes(&self, maximum: usize) -> io::Result<()> {
            let held = self.held_bytes.lock();
            self.check()?;
            self.written.load(Ordering::Acquire).checked_add(*held)
                .and_then(|total| total.checked_add(maximum as u64))
                .filter(|total| *total <= self.byte_limit)
                .map(|_| ()).ok_or_else(Self::refuse)
        }
        fn allocate(&self, bytes: u64) -> io::Result<()> {
            self.reserve_allocation(bytes)?;
            self.record_reserved_allocation(bytes)
        }
        fn reserve_allocation(&self, bytes: u64) -> io::Result<()> {
            let mut pending = self.pending_allocations.lock();
            self.check()?;
            self.allocations.load(Ordering::Acquire).checked_add(pending.0)
                .and_then(|total| total.checked_add(1)).ok_or_else(Self::refuse)?;
            self.memory.load(Ordering::Acquire).checked_add(pending.1)
                .and_then(|total| total.checked_add(bytes))
                .filter(|total| *total <= self.memory_limit).ok_or_else(Self::refuse)?;
            pending.0 += 1;
            pending.1 += bytes;
            Ok(())
        }
        fn cancel_reserved_allocation(&self, bytes: u64) {
            let mut pending = self.pending_allocations.lock();
            pending.0 = pending.0.checked_sub(1).expect("refund owns one allocation credit");
            pending.1 = pending.1.checked_sub(bytes).expect("refund owns its allocation bytes");
        }
        fn record_reserved_allocation(&self, bytes: u64) -> io::Result<()> {
            let mut pending = self.pending_allocations.lock();
            let next_operations = pending.0.checked_sub(1).ok_or_else(Self::refuse)?;
            let next_bytes = pending.1.checked_sub(bytes).ok_or_else(Self::refuse)?;
            let allocations = self.allocations.load(Ordering::Acquire).checked_add(1)
                .ok_or_else(Self::refuse)?;
            let memory = self.memory.load(Ordering::Acquire).checked_add(bytes).ok_or_else(Self::refuse)?;
            pending.0 = next_operations;
            pending.1 = next_bytes;
            self.allocations.store(allocations, Ordering::Release);
            self.memory.store(memory, Ordering::Release);
            Ok(())
        }
        fn adopt(&self, bytes: u64) -> io::Result<()> {
            let pending = self.pending_allocations.lock();
            self.check()?;
            self.memory.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current|
                current.checked_add(bytes).filter(|next|
                    next.checked_add(pending.1).is_some_and(|total| total <= self.memory_limit)))
                .map(|_| ()).map_err(|_| Self::refuse())
        }
        fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>) {
            let pending = self.pending_allocations.lock();
            let previous = match self.memory.fetch_update(Ordering::AcqRel, Ordering::Acquire,
                |current| current.checked_add(bytes)) {
                Ok(previous) => previous,
                Err(_) => return (0, Err(Self::refuse())),
            };
            let result = if (previous + bytes).checked_add(pending.1)
                .is_none_or(|total| total > self.memory_limit) {
                Err(Self::refuse())
            } else { self.check() };
            (bytes, result)
        }
        fn allocation_failed(&self, error: std::collections::TryReserveError) -> io::Error {
            let mut failure = self.allocation_failure.lock();
            if failure.is_none() { *failure = Some(error); }
            io::ErrorKind::OutOfMemory.into()
        }
        fn allocate_refcount_header(&self) -> io::Result<()> { Err(Self::refuse()) }
        fn release(&self, bytes: u64) {
            let _pending = self.pending_allocations.lock();
            self.memory.fetch_sub(bytes, Ordering::AcqRel);
        }
        fn examine(&self, _: u64, _: u64) -> io::Result<()> { self.check() }
        fn admit_examination(&self, _: u64, _: u64) -> io::Result<()> { self.check() }
        fn record_examination(&self, _: u64, _: u64) -> io::Result<()> { Ok(()) }
        fn admit_bytes(&self, _: usize) -> io::Result<()> { self.check() }
        fn admit_read(&self, _: usize) -> io::Result<()> { Err(Self::refuse()) }
        fn record_read(&self, _: usize) -> io::Result<()> { Err(Self::refuse()) }
        fn admit_operations(&self, _: u64) -> io::Result<()> { Err(Self::refuse()) }
        fn record_operations(&self, _: u64) -> io::Result<()> { Err(Self::refuse()) }
        fn admit_write_operations(&self, additional: u64) -> io::Result<()> {
            let held = self.held_operations.lock();
            self.check()?;
            self.operations.load(Ordering::Acquire).checked_add(*held)
                .and_then(|total| total.checked_add(additional))
                .filter(|total| *total <= self.operation_limit)
                .map(|_| ()).ok_or_else(Self::refuse)
        }
        fn record_write_operations(&self, completed: u64) -> io::Result<()> {
            let held = self.held_operations.lock();
            self.check()?;
            self.operations.load(Ordering::Acquire).checked_add(*held)
                .and_then(|total| total.checked_add(completed))
                .filter(|total| *total <= self.operation_limit).ok_or_else(Self::refuse)?;
            self.operations.fetch_add(completed, Ordering::AcqRel);
            Ok(())
        }
        fn admit_write(&self, maximum: usize) -> io::Result<()> {
            let held = self.held_operations.lock();
            self.admit_write_bytes(maximum)?;
            self.operations.load(Ordering::Acquire).checked_add(*held)
                .and_then(|total| total.checked_add(1))
                .filter(|total| *total <= self.operation_limit).ok_or_else(Self::refuse)?;
            self.operations.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
        fn record_write(&self, bytes: usize) -> io::Result<()> {
            let held = self.held_bytes.lock();
            self.written.load(Ordering::Acquire).checked_add(*held)
                .and_then(|total| total.checked_add(bytes as u64))
                .filter(|total| *total <= self.byte_limit).ok_or_else(Self::refuse)?;
            self.written.fetch_add(bytes as u64, Ordering::AcqRel);
            if self.cancel_after_write { self.cancelled.store(true, Ordering::Release); }
            Ok(())
        }
        fn admit_durability(&self) -> io::Result<()> {
            let held = self.held_operations.lock();
            self.check()?;
            self.operations.load(Ordering::Acquire).checked_add(*held)
                .and_then(|total| total.checked_add(1))
                .filter(|total| *total <= self.operation_limit).ok_or_else(Self::refuse)?;
            self.operations.fetch_add(1, Ordering::AcqRel);
            self.durability.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    #[test]
    fn write_byte_reservation_preserves_actual_prefix_and_refunds_unused_maximum() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("reserved-write-prefix");
        let mut file = File::create(&path)?;
        let meter = Arc::new(WriteBudgetMeter::new(4, 8));
        let second_io_alias = meter.clone();
        let mut bytes = NativeByteReservation::new(&*meter, NativeOperationKind::Write, 8)?;
        assert_eq!(meter.written.load(Ordering::Acquire), 0);
        assert!(second_io_alias.admit_write_bytes(1).is_err());
        assert!(second_io_alias.admit_write(1).is_err());
        let mut operation = NativeOperationReservation::new(&*meter, NativeOperationKind::Write, 1)?;
        let (written, recorded) = operation.attempt(|| std::io::Write::write(&mut file, b"abc"));
        recorded?;
        let written = written?;
        bytes.record(written)?;
        assert_eq!(written, 3);
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        assert_eq!(meter.written.load(Ordering::Acquire), 3);
        second_io_alias.admit_write_bytes(5)?;
        assert!(second_io_alias.admit_write_bytes(6).is_err());
        assert_eq!(std::fs::read(&path)?, b"abc");
        {
            let _unused = NativeByteReservation::new(&*meter, NativeOperationKind::Write, 5)?;
            assert!(second_io_alias.admit_write_bytes(1).is_err());
        }
        second_io_alias.admit_write_bytes(5)?;
        meter.cancelled.store(true, Ordering::Release);
        assert!(NativeByteReservation::new(&*meter, NativeOperationKind::Write, 1).is_err());
        meter.cancelled.store(false, Ordering::Release);
        let mut completion = NativeByteReservation::new(&*meter, NativeOperationKind::Write, 5)?;
        meter.cancelled.store(true, Ordering::Release);
        completion.record(0)?;
        assert_eq!(meter.written.load(Ordering::Acquire), 3);
        Ok(())
    }

    #[test]
    fn write_reservation_withholds_completion_from_direct_callback() -> io::Result<()> {
        let meter = WriteBudgetMeter::new(2, 8);
        let mut request = NativeOperationReservation::new(&meter, NativeOperationKind::Write, 2)?;
        assert_eq!(meter.operations.load(Ordering::Acquire), 0);
        let (_, recorded) = request.attempt(|| {
            assert!(meter.admit_write(1).is_err());
            assert!(meter.record_write_operations(1).is_err());
            assert!(meter.admit_durability().is_err());
            meter.cancelled.store(true, Ordering::Release);
        });
        recorded?;
        let (_, recorded) = request.attempt(|| ());
        recorded?;
        assert_eq!(meter.operations.load(Ordering::Acquire), 2);
        assert_eq!(meter.durability.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    fn write_reservation_refunds_unused_immediate_failure_allowance() -> io::Result<()> {
        let meter = WriteBudgetMeter::new(2, 8);
        {
            let mut request = NativeOperationReservation::new(&meter, NativeOperationKind::Write, 2)?;
            let (failure, recorded) = request.attempt(|| Err::<(), _>(io::ErrorKind::PermissionDenied));
            recorded?;
            assert!(failure.is_err());
            assert_eq!(meter.operations.load(Ordering::Acquire), 1);
            assert!(meter.admit_write(1).is_err());
        }
        meter.admit_write_bytes(8)?;
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        meter.admit_write(1)?;
        meter.record_write(1)?;
        assert_eq!(meter.operations.load(Ordering::Acquire), 2);
        assert_eq!(meter.written.load(Ordering::Acquire), 1);
        Ok(())
    }

    #[test]
    fn owned_request_tokens_cross_worker_and_drain_write_flush_after_cancellation() -> io::Result<()> {
        use std::io::Write as _;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("owned-credits");
        let mut file = OpenOptions::new().create_new(true).read(true).write(true).open(&path)?;
        let meter = Arc::new(WriteBudgetMeter::new(2, 3));
        let mut write = OwnedNativeOperationReservation::new(
            meter.clone(), NativeOperationKind::Write, 1,
        )?;
        let mut flush = OwnedNativeOperationReservation::new(
            meter.clone(), NativeOperationKind::Durability, 1,
        )?;
        let mut bytes = OwnedNativeByteReservation::new(
            meter.clone(), NativeOperationKind::Write, 3,
        )?;
        assert_eq!(meter.operations.load(Ordering::Acquire), 0);
        assert_eq!(meter.durability.load(Ordering::Acquire), 0);
        assert_eq!(meter.written.load(Ordering::Acquire), 0);
        meter.cancelled.store(true, Ordering::Release);
        let result = std::thread::spawn(move || -> io::Result<()> {
            let (physical, recorded_write) = write.attempt(|| file.write(b"x"));
            let count = physical?;
            let recorded_bytes = bytes.record_partial(count);
            let (physical_flush, recorded_flush) = flush.attempt(|| file.sync_data());
            physical_flush?;
            recorded_write?;
            recorded_bytes?;
            recorded_flush?;
            Ok(())
        }).join().expect("actual admitted worker does not panic");
        result?;
        assert_eq!(std::fs::read(&path)?, b"x");
        assert_eq!(meter.operations.load(Ordering::Acquire), 2);
        assert_eq!(meter.durability.load(Ordering::Acquire), 1);
        assert_eq!(meter.written.load(Ordering::Acquire), 1);
        assert_eq!(*meter.held_operations.lock(), 0);
        assert_eq!(*meter.held_bytes.lock(), 0);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    fn owned_request_failure_refunds_only_unused_physical_allowance() -> io::Result<()> {
        use std::io::Write as _;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("owned-readonly");
        std::fs::write(&path, b"original")?;
        let mut file = File::open(&path)?;
        let meter = Arc::new(WriteBudgetMeter::new(2, 3));
        let mut operation = OwnedNativeOperationReservation::new(
            meter.clone(), NativeOperationKind::Write, 2,
        )?;
        let mut bytes = OwnedNativeByteReservation::new(
            meter.clone(), NativeOperationKind::Write, 3,
        )?;
        let (physical, recorded) = operation.attempt(|| file.write(b"x"));
        assert!(physical.is_err());
        bytes.record(0)?;
        recorded?;
        assert_eq!(operation.remaining(), 1);
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        assert!(meter.admit_write_operations(1).is_err());
        drop(operation);
        meter.admit_write_operations(1)?;
        meter.admit_write_bytes(3)?;
        assert_eq!(meter.written.load(Ordering::Acquire), 0);
        assert_eq!(std::fs::read(&path)?, b"original");
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[allow(unsafe_code, reason = "Windows test opens its handle with FILE_FLAG_OVERLAPPED")]
    fn bounded_write_file(path: &Path, writable: bool) -> io::Result<NativeFile> {
        initialize_native_io_services()?;
        let mut options = OpenOptions::new();
        options.read(true).write(writable);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt as _;
            options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED);
            // SAFETY: the exact open above supplies the required flag.
            return unsafe { NativeFile::from_overlapped_file_unchecked(options.open(path)?) };
        }
        #[cfg(not(windows))]
        NativeFile::from_file(options.open(path)?)
    }

    struct ObservedWriteBytes {
        bytes: Vec<u8>,
        dropped: Arc<AtomicBool>,
    }

    impl AsRef<[u8]> for ObservedWriteBytes {
        fn as_ref(&self) -> &[u8] { &self.bytes }
    }

    impl Drop for ObservedWriteBytes {
        fn drop(&mut self) { self.dropped.store(true, Ordering::Release); }
    }

    struct ObservedWriteCustody {
        bytes_dropped: Arc<AtomicBool>,
        retired: Arc<AtomicBool>,
        meter: Arc<WriteBudgetMeter>,
        actual_prefix: u64,
    }

    impl Drop for ObservedWriteCustody {
        fn drop(&mut self) {
            assert!(self.bytes_dropped.load(Ordering::Acquire));
            assert_eq!(self.meter.written.load(Ordering::Acquire), self.actual_prefix);
            self.retired.store(true, Ordering::Release);
        }
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_write_custody_retires_after_actual_partial_prefix() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("custodied-prefix");
        File::create(&path)?;
        let file = bounded_write_file(&path, true)?;
        let per_request = if cfg!(windows) { 2 } else { 1 };
        let meter = Arc::new(WriteBudgetMeter::new(per_request, 1024 * 1024 + 7));
        let bytes_dropped = Arc::new(AtomicBool::new(false));
        let retired = Arc::new(AtomicBool::new(false));
        let bytes = Bytes::from_owner(ObservedWriteBytes {
            bytes: vec![0x5a; 1024 * 1024 + 7],
            dropped: bytes_dropped.clone(),
        });
        let custody = ObservedWriteCustody {
            bytes_dropped: bytes_dropped.clone(), retired: retired.clone(),
            meter: meter.clone(), actual_prefix: 1024 * 1024,
        };
        assert!(complete_write(file.write_bounded_async_with_custody(
            0, bytes, meter.clone(), custody,
        )?).is_err());
        assert!(retired.load(Ordering::Acquire));
        assert_eq!(meter.operations.load(Ordering::Acquire), per_request);
        assert_eq!(std::fs::read(&path)?, vec![0x5a; 1024 * 1024]);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_write_custody_remains_with_admitted_job_after_observer_drop() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("custodied-detached");
        File::create(&path)?;
        let file = bounded_write_file(&path, true)?;
        let per_request = if cfg!(windows) { 2 } else { 1 };
        let meter = Arc::new(WriteBudgetMeter::new(per_request, 9));
        let bytes_dropped = Arc::new(AtomicBool::new(false));
        let retired = Arc::new(AtomicBool::new(false));
        let bytes = Bytes::from_owner(ObservedWriteBytes {
            bytes: b"persisted".to_vec(), dropped: bytes_dropped.clone(),
        });
        let custody = ObservedWriteCustody {
            bytes_dropped: bytes_dropped.clone(), retired: retired.clone(),
            meter: meter.clone(), actual_prefix: 9,
        };
        let mut write = file.write_bounded_async_with_custody(
            0, bytes, meter.clone(), custody,
        )?;
        let completed = write.state.clone();
        let (sender, receiver) = mpsc::sync_channel(1);
        sender.try_send(write.pending.take().expect("the real write is pending"))
            .map_err(|_| io::Error::other("write admission failed"))?;
        drop(write);
        assert!(!retired.load(Ordering::Acquire));
        assert!(!bytes_dropped.load(Ordering::Acquire));
        let _ = receiver.recv().map_err(|_| io::Error::other("write disappeared"))?.run();
        completed.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
            .result.take().expect("real write completes")?;
        assert!(retired.load(Ordering::Acquire));
        assert_eq!(std::fs::read(&path)?, b"persisted");
        assert_eq!(meter.operations.load(Ordering::Acquire), per_request);
        drop(completed);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_write_cold_admission_drops_payload_before_custody() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("custodied-denied");
        File::create(&path)?;
        let file = bounded_write_file(&path, true)?;
        let mut budget = WriteBudgetMeter::new(2, 9);
        budget.memory_limit = 0;
        let meter = Arc::new(budget);
        let bytes_dropped = Arc::new(AtomicBool::new(false));
        let retired = Arc::new(AtomicBool::new(false));
        let bytes = Bytes::from_owner(ObservedWriteBytes {
            bytes: b"persisted".to_vec(), dropped: bytes_dropped.clone(),
        });
        let custody = ObservedWriteCustody {
            bytes_dropped: bytes_dropped.clone(), retired: retired.clone(),
            meter: meter.clone(), actual_prefix: 0,
        };
        assert!(file.write_bounded_async_with_custody(0, bytes, meter.clone(), custody).is_err());
        assert!(retired.load(Ordering::Acquire));
        assert_eq!(meter.operations.load(Ordering::Acquire), 0);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        assert_eq!(std::fs::read(&path)?, b"");
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_write_cancelled_custody_records_physical_prefix_before_retirement() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("custodied-cancel");
        File::create(&path)?;
        let file = bounded_write_file(&path, true)?;
        let per_request = if cfg!(windows) { 2 } else { 1 };
        let mut budget = WriteBudgetMeter::new(per_request, 9);
        budget.cancel_after_write = true;
        let meter = Arc::new(budget);
        let bytes_dropped = Arc::new(AtomicBool::new(false));
        let retired = Arc::new(AtomicBool::new(false));
        let bytes = Bytes::from_owner(ObservedWriteBytes {
            bytes: b"persisted".to_vec(), dropped: bytes_dropped.clone(),
        });
        let custody = ObservedWriteCustody {
            bytes_dropped: bytes_dropped.clone(), retired: retired.clone(),
            meter: meter.clone(), actual_prefix: 9,
        };
        let error = complete_write(file.write_bounded_async_with_custody(
            0, bytes, meter.clone(), custody,
        )?).expect_err("actual transfer sets the cancellation control");
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert!(retired.load(Ordering::Acquire));
        assert_eq!(meter.operations.load(Ordering::Acquire), per_request);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        assert_eq!(std::fs::read(&path)?, b"persisted");
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_write_refusal_preserves_actual_physical_prefix() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("prefix");
        File::create(&path)?;
        let file = bounded_write_file(&path, true)?;
        let per_request = if cfg!(windows) { 2 } else { 1 };
        let meter = Arc::new(WriteBudgetMeter::new(per_request, 1024 * 1024 + 7));
        let bytes = Bytes::from(vec![0x5a; 1024 * 1024 + 7]);
        let failure = complete_write(file.write_bounded_async(0, bytes, meter.clone())?);
        assert!(failure.is_err());
        assert_eq!(meter.written.load(Ordering::Acquire), 1024 * 1024);
        assert_eq!(meter.operations.load(Ordering::Acquire), per_request);
        assert_eq!(std::fs::read(&path)?, vec![0x5a; 1024 * 1024]);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_write_cancellation_records_completed_bytes() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("cancel");
        File::create(&path)?;
        let file = bounded_write_file(&path, true)?;
        let mut budget = WriteBudgetMeter::new(10, 10);
        budget.cancel_after_write = true;
        let meter = Arc::new(budget);
        let error = complete_write(file.write_bounded_async(
            0, Bytes::from_static(b"prefix"), meter.clone(),
        )?).expect_err("observer must see cancellation");
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(meter.written.load(Ordering::Acquire), 6);
        assert_eq!(std::fs::read(path)?, b"prefix");
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_write_os_failure_records_only_issued_request() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("readonly");
        std::fs::write(&path, b"unchanged")?;
        let file = bounded_write_file(&path, false)?;
        let meter = Arc::new(WriteBudgetMeter::new(2, 10));
        assert!(complete_write(file.write_bounded_async(
            0, Bytes::from_static(b"change"), meter.clone(),
        )?).is_err());
        assert_eq!(meter.written.load(Ordering::Acquire), 0);
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        assert_eq!(std::fs::read(path)?, b"unchanged");
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_sync_counts_actual_flush_and_unsupported_barrier() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("durable");
        File::create(&path)?;
        let file = bounded_write_file(&path, true)?;
        let meter = Arc::new(WriteBudgetMeter::new(1, 0));
        complete_write(file.sync_bounded_async(Durability::Full, meter.clone())?)?;
        assert_eq!(meter.durability.load(Ordering::Acquire), 1);
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        #[cfg(not(target_vendor = "apple"))]
        {
            let error = complete_write(file.sync_bounded_async(
                Durability::Barrier, meter.clone(),
            )?).expect_err("barrier capability is absent");
            assert_eq!(error.kind(), io::ErrorKind::Unsupported);
            assert_eq!(meter.durability.load(Ordering::Acquire), 1);
        }
        let error = complete_write(file.sync_bounded_async(
            Durability::Full, meter.clone(),
        )?).expect_err("a second actual flush exceeds the one-call budget");
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(meter.operations.load(Ordering::Acquire), 1);
        assert_eq!(meter.durability.load(Ordering::Acquire), 1);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_completion_memory_refusal_precedes_mutation() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("memory");
        File::create(&path)?;
        let file = bounded_write_file(&path, true)?;
        let mut budget = WriteBudgetMeter::new(10, 10);
        budget.memory_limit = 0;
        let meter = Arc::new(budget);
        assert!(file.write_bounded_async(
            0, Bytes::from_static(b"x"), meter.clone(),
        ).is_err());
        assert_eq!(meter.operations.load(Ordering::Acquire), 0);
        assert_eq!(std::fs::metadata(path)?.len(), 0);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_control_uses_exact_held_handle_leaves() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("control");
        File::create(&path)?;
        let file = bounded_write_file(&path, true)?;
        let meter = Arc::new(WriteBudgetMeter::new(2, 0));
        complete_write(file.control_bounded_async(|held, meter| {
            set_len_measured(held, 17, meter)?;
            sync_file_measured(held, Durability::Full, meter)
        }, meter.clone())?)?;
        assert_eq!(meter.operations.load(Ordering::Acquire), 2);
        assert_eq!(meter.durability.load(Ordering::Acquire), 1);
        assert_eq!(std::fs::metadata(path)?.len(), 17);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_invalid_resize_performs_no_native_operation() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("invalid");
        File::create(&path)?;
        let file = bounded_write_file(&path, true)?;
        let meter = Arc::new(WriteBudgetMeter::new(0, 0));
        let error = complete_write(file.set_len_bounded_async(u64::MAX, meter.clone())?)
            .expect_err("invalid native length");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(meter.operations.load(Ordering::Acquire), 0);
        assert_eq!(std::fs::metadata(path)?.len(), 0);
        Ok(())
    }

    #[test]
    fn completed_native_futures_reject_repoll() -> io::Result<()> {
        fn finish<T>(future: &mut (impl Future<Output = io::Result<T>> + Unpin)) -> io::Result<T> {
            let deadline = Instant::now() + Duration::from_secs(10);
            let waker = completion_waker();
            loop {
                match Pin::new(&mut *future).poll(&mut Context::from_waker(&waker)) {
                    Poll::Ready(result) => return result,
                    Poll::Pending if Instant::now() < deadline => {
                        std::thread::park_timeout(
                            deadline.saturating_duration_since(Instant::now()),
                        );
                    }
                    Poll::Pending => {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "native future stalled",
                        ));
                    }
                }
            }
        }

        fn repoll_panics(future: &mut (impl Future + Unpin)) {
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _ = Pin::new(future).poll(&mut Context::from_waker(Waker::noop()));
                }))
                .is_err()
            );
        }

        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("completed-native-futures"))?;
        let mut read = read_batch_async(file.try_clone()?, vec![]);
        assert!(finish(&mut read)?.is_empty());
        repoll_panics(&mut read);

        let mut write = write_all_batch_async(file, vec![]);
        finish(&mut write)?;
        repoll_panics(&mut write);

        let mut host_task = run_blocking_io(|| 7);
        assert_eq!(finish(&mut host_task)?, 7);
        repoll_panics(&mut host_task);
        Ok(())
    }

    #[test]
    fn positional_io_preserves_cursor_and_bounds_ranges() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("positional");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        write_all_at(&file, 2, &[0x22; 3])?;
        write_all_at(&file, 16, &[0x33; 5])?;
        sync_file(&file, Durability::Full)?;

        let mut bytes = [0_u8; 24];
        assert_eq!(read_at(&file, 0, &mut bytes)?, 21);
        assert_eq!(&bytes[..8], &[0, 0, 0x22, 0x22, 0x22, 0, 0, 0]);
        assert_eq!(&bytes[8..16], &[0; 8]);
        assert_eq!(&bytes[16..21], &[0x33; 5]);
        assert_eq!(read_at(&file, 21, &mut bytes)?, 0);

        let mut range = RangeReader::new(&file, 15, 8);
        let mut selected = Vec::new();
        range.read_to_end(&mut selected)?;
        assert_eq!(selected, [0, 0x33, 0x33, 0x33, 0x33, 0x33]);

        complete_write(write_all_batch_async(
            file.try_clone()?,
            vec![
                OwnedWrite {
                    offset: 32,
                    bytes: Bytes::from_static(&[0x44; 5]),
                },
                OwnedWrite {
                    offset: 48,
                    bytes: Bytes::from_static(&[0x55; 7]),
                },
            ],
        ))?;
        let mut batch = [0_u8; 23];
        assert_eq!(read_at(&file, 32, &mut batch)?, 23);
        assert_eq!(&batch[..5], &[0x44; 5]);
        assert_eq!(&batch[5..16], &[0; 11]);
        assert_eq!(&batch[16..], &[0x55; 7]);

        // Native batch reads are positional even after another handle sharing
        // the open file description has moved its cursor to EOF.
        let mut cursor = file.try_clone()?;
        cursor.seek(SeekFrom::End(0))?;
        let selected = complete_read(read_batch_async(
            file.try_clone()?,
            vec![OwnedRead {
                offset: 16,
                length: 5,
            }],
        ))?;
        assert_eq!(selected, [Bytes::from_static(&[0x33; 5])]);
        assert_eq!(cursor.stream_position()?, 55);

        // Batch writes must be independent of the shared file cursor too.
        complete_write(write_all_batch_async(
            file.try_clone()?,
            vec![OwnedWrite {
                offset: 4,
                bytes: Bytes::from_static(b"cursor-independent"),
            }],
        ))?;
        let mut written = [0_u8; 18];
        assert_eq!(read_at(&file, 4, &mut written)?, written.len());
        assert_eq!(&written, b"cursor-independent");
        assert_eq!(cursor.stream_position()?, 55);
        Ok(())
    }

    #[test]
    fn batch_larger_than_native_queue_preserves_every_write() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("large-batch"))?;
        let writes: Vec<_> = (0_u8..17)
            .map(|value| OwnedWrite {
                offset: u64::from(value) * 4,
                bytes: Bytes::from(vec![value; 3]),
            })
            .collect();
        complete_write(write_all_batch_async(file.try_clone()?, writes))?;

        let mut actual = [0_u8; 67];
        assert_eq!(read_at(&file, 0, &mut actual)?, actual.len());
        for value in 0_u8..17 {
            let start = usize::from(value) * 4;
            assert_eq!(actual.get(start..start + 3), Some([value; 3].as_slice()));
            if value != 16 {
                assert_eq!(actual.get(start + 3), Some(&0));
            }
        }
        Ok(())
    }

    #[test]
    fn overlapping_batch_writes_fail_before_any_write() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("overlapping-batch");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        std::fs::write(&path, b"unchanged")?;
        let result = complete_write(write_all_batch_async(
            file,
            vec![
                OwnedWrite {
                    offset: 0,
                    bytes: Bytes::from_static(b"first"),
                },
                OwnedWrite {
                    offset: 3,
                    bytes: Bytes::from_static(b"second"),
                },
            ],
        ));
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::InvalidInput));
        assert_eq!(std::fs::read(&path)?, b"unchanged");
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn completed_batches_preserve_ring_reuse() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("ring-reuse"))?;
        for value in 0_u16..256 {
            complete_write(write_all_batch_async(
                file.try_clone()?,
                vec![OwnedWrite {
                    offset: u64::from(value),
                    bytes: Bytes::copy_from_slice(&[value.to_le_bytes()[0]]),
                }],
            ))?;
        }
        let contents = complete_read(read_batch_async(
            file,
            vec![OwnedRead {
                offset: 0,
                length: 256,
            }],
        ))?;
        assert_eq!(contents.first().map(Bytes::len), Some(256));
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn native_write_batch_preserves_empty_writes_and_port_reuse() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("write-batch-reuse"))?;
        for value in 0_u8..8 {
            complete_write(write_all_batch_async(
                file.try_clone()?,
                vec![
                    OwnedWrite {
                        offset: u64::from(value) * 2,
                        bytes: Bytes::copy_from_slice(&[value]),
                    },
                    OwnedWrite {
                        offset: u64::from(value) * 2 + 1,
                        bytes: Bytes::new(),
                    },
                ],
            ))?;
        }
        let mut actual = [0_u8; 15];
        assert_eq!(read_at(&file, 0, &mut actual)?, actual.len());
        for value in 0_u8..8 {
            assert_eq!(actual.get(usize::from(value) * 2), Some(&value));
        }
        Ok(())
    }

    #[test]
    fn owned_write_outlives_the_callers_file_and_buffers() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("owned-write");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let write = write_all_batch_async(
            file,
            vec![OwnedWrite {
                offset: 3,
                bytes: Bytes::from_static(b"owned"),
            }],
        );
        complete_write(write)?;
        let mut actual = Vec::new();
        File::open(path)?.read_to_end(&mut actual)?;
        assert_eq!(actual, [0, 0, 0, b'o', b'w', b'n', b'e', b'd']);
        Ok(())
    }

    #[test]
    fn owned_native_file_preserves_positional_contract() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("owned-native-file");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let native = NativeFile::from_file(file)?;
        complete_write(native.set_len_async(32))?;
        complete_write(native.write_all_batch_async(vec![OwnedWrite {
            offset: 7,
            bytes: Bytes::from_static(b"native"),
        }]))?;
        complete_write(native.write_all_batch_async(vec![
            OwnedWrite {
                offset: 0,
                bytes: Bytes::from_static(b"one"),
            },
            OwnedWrite {
                offset: 20,
                bytes: Bytes::from_static(b"two"),
            },
        ]))?;
        complete_write(native.sync_async(Durability::Full))?;

        let actual = complete_read(native.read_batch_async(vec![OwnedRead {
            offset: 0,
            length: 32,
        }]))?
        .remove(0);
        assert_eq!(actual.len(), 32);
        assert_eq!(actual.get(..3), Some(b"one".as_slice()));
        assert_eq!(actual.get(7..13), Some(b"native".as_slice()));
        assert_eq!(actual.get(20..23), Some(b"two".as_slice()));
        Ok(())
    }

    /// An inline thread serves an overlapped handle's positional I/O in
    /// place with exactly the driver's results, before and after the
    /// driver has attached that handle, and neither moves the other.
    #[cfg(windows)]
    #[test]
    #[allow(
        unsafe_code,
        reason = "the test drives a raw overlapped handle through the unsafe constructor"
    )]
    fn inline_overlapped_io_matches_the_driver() -> io::Result<()> {
        use std::os::windows::fs::OpenOptionsExt as _;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED;

        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .custom_flags(FILE_FLAG_OVERLAPPED)
            .open(temporary.path().join("in-place"))?;
        // SAFETY: the handle was just opened overlapped, is attached to no
        // port, and is transferred once.
        let native = unsafe { NativeFile::from_overlapped_file_unchecked(file)? };
        let reads = || {
            [(5, 8), (1, 3), (0, 0), (12, 4), (30, 4)]
                .map(|(offset, length)| OwnedRead { offset, length })
                .to_vec()
        };
        let expected = [
            Bytes::from_static(b"fgh\0\0xy"),
            Bytes::from_static(b"bcd"),
            Bytes::new(),
            Bytes::new(),
            Bytes::new(),
        ];
        let write = |offset, bytes| {
            complete_write(native.write_all_batch_async(vec![OwnedWrite { offset, bytes }]))
        };
        {
            let _inline = InlineBlocking::enter();
            write(0, Bytes::from_static(b"abcdefgh"))?;
            write(10, Bytes::from_static(b"xy"))?;
            assert_eq!(complete_read(native.read_batch_async(reads()))?, expected);
        }
        // The driver attaches the handle and sees exactly the same file.
        assert_eq!(complete_read(native.read_batch_async(reads()))?, expected);
        write(0, Bytes::from_static(b"A"))?;
        {
            // In-place requests on an attached handle post nothing to its port.
            let _inline = InlineBlocking::enter();
            write(1, Bytes::from_static(b"B"))?;
            let read = complete_read(native.read_batch_async(vec![OwnedRead {
                offset: 0,
                length: 3,
            }]))?;
            assert_eq!(read, [Bytes::from_static(b"ABc")]);
        }
        assert_eq!(
            complete_read(native.read_batch_async(vec![OwnedRead {
                offset: 0,
                length: 3,
            }]))?,
            [Bytes::from_static(b"ABc")]
        );
        Ok(())
    }

    #[test]
    fn owned_file_operation_keeps_its_handle_after_owner_drop() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("owned-file-lifetime");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let native = NativeFile::from_file(file)?;
        let resize = native.set_len_async(64);
        drop(native);
        complete_write(resize)?;
        assert_eq!(std::fs::metadata(&path)?.len(), 64);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn healthy_registry_does_not_retain_exclusive_handle() -> io::Result<()> {
        use std::os::windows::fs::OpenOptionsExt as _;

        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("exclusive-native-file");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .share_mode(0)
            .open(&path)?;
        let native = NativeFile::from_file(file)?;
        drop(native);
        // No unrelated admission or registry sweep is required to release
        // the caller's exclusive Windows share lock.
        File::open(&path)?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn uncertain_completion_retains_identity_after_owner_drop() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("uncertain-identity");
        let native = NativeFile::from_file(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)?,
        )?;
        let completion = FileCompletion::new(
            Arc::new(Mutex::new(CompletionState::<()> {
                result: None,
                panic: None,
                waker: None,
                _allocation: None,
            })),
            None,
            Some(Arc::clone(&native.uncertain)),
            Some(Arc::clone(&native.tail)),
            obs::OperationSpan::new(tracing::Span::none()),
        )
        .for_file(Arc::clone(&native.file));
        completion.finish(Err(linux::uncertain_completion(
            io::ErrorKind::TimedOut.into(),
        )));
        drop(native);
        let alias = NativeFile::from_file(OpenOptions::new().read(true).open(&path)?)?;
        assert!(alias.uncertain.load(Ordering::Acquire));
        assert!(
            complete_read(alias.read_batch_async(vec![OwnedRead {
                offset: 0,
                length: 1,
            }]))
            .is_err()
        );
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn concurrent_alias_registration_uses_one_gate() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("concurrent-identity");
        std::fs::write(&path, b"shared")?;
        let start = Arc::new(std::sync::Barrier::new(16));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let path = path.clone();
            let start = Arc::clone(&start);
            handles.push(std::thread::spawn(move || {
                let file = OpenOptions::new().read(true).write(true).open(path)?;
                start.wait();
                NativeFile::from_file(file)
            }));
        }
        let mut files = Vec::new();
        for handle in handles {
            files.push(
                handle
                    .join()
                    .map_err(|_| io::Error::other("registration panicked"))??,
            );
        }
        assert_eq!(files.len(), 16);
        if let Some(first) = files.first() {
            for file in files.iter().skip(1) {
                assert!(Arc::ptr_eq(&first.tail, &file.tail));
                assert!(Arc::ptr_eq(&first.uncertain, &file.uncertain));
            }
        }
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn independently_opened_aliases_share_fencing_and_uncertainty() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("identity");
        let alias = temporary.path().join("hard-link");
        let first = NativeFile::from_file(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)?,
        )?;
        std::fs::hard_link(&path, &alias)?;
        let second =
            NativeFile::from_file(OpenOptions::new().read(true).write(true).open(&alias)?)?;
        assert!(Arc::ptr_eq(&first.tail, &second.tail));
        assert!(Arc::ptr_eq(&first.uncertain, &second.uncertain));
        poison_file_health(&first.file, &first.uncertain);
        drop(first);
        let result = complete_write(second.set_len_async(1));
        assert!(result.is_err(), "an alias bypassed an uncertain completion");
        drop(second);
        let bare_result = complete_read(read_batch_async(
            OpenOptions::new().read(true).open(&path)?,
            vec![OwnedRead {
                offset: 0,
                length: 1,
            }],
        ));
        assert!(
            bare_result.is_err(),
            "the free read API bypassed the file fence"
        );
        let direct_alias = OpenOptions::new().read(true).write(true).open(&alias)?;
        let mut byte = [0];
        assert!(read_at(&direct_alias, 0, &mut byte).is_err());
        assert!(write_all_at(&direct_alias, 0, b"x").is_err());
        assert!(sync_file(&direct_alias, Durability::Full).is_err());
        assert!(sync_data(&direct_alias, Durability::Full).is_err());
        assert_eq!(std::fs::metadata(&path)?.len(), 0);

        let independent = NativeFile::from_file(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(temporary.path().join("clean"))?,
        )?;
        complete_write(independent.set_len_async(1))?;
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn admitted_job_keeps_alias_registry_alive_after_owner_drop() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("outstanding");
        let alias_path = temporary.path().join("outstanding-link");
        let native = NativeFile::from_file(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)?,
        )?;

        std::fs::hard_link(&path, &alias_path)?;
        let tail = Arc::downgrade(&native.tail);
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel::<()>(1);
        let mut pending = native.control_async(move |_| {
            started_sender.send(()).map_err(io::Error::other)?;
            release_receiver.recv().map_err(io::Error::other)?;
            Ok(())
        });
        let waker = completion_waker();
        assert!(matches!(
            Pin::new(&mut pending).poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        started_receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)?;
        drop(native);
        let alias = NativeFile::from_file(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&alias_path)?,
        )?;
        assert!(Arc::ptr_eq(
            &tail
                .upgrade()
                .ok_or_else(|| io::Error::other("admitted job lost its alias tail"))?,
            &alias.tail
        ));
        release_sender.send(()).map_err(io::Error::other)?;
        complete_write(pending)?;
        Ok(())
    }

    #[test]
    fn owned_file_operations_follow_first_poll_order() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("owned-file-order");
        let native = NativeFile::from_file(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)?,
        )?;
        let write = native.write_all_batch_async(vec![OwnedWrite {
            offset: 0,
            bytes: Bytes::from_static(b"ordered"),
        }]);
        let mut sync = native.sync_async(Durability::Full);
        let waker = completion_waker();
        // Polled first, the sync is admitted ahead of the unpolled write and
        // completes without it. A fast worker may finish it before this first
        // poll returns, so either outcome of that poll is correct.
        match Pin::new(&mut sync).poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(result) => result?,
            Poll::Pending => complete_write(sync)?,
        }
        complete_write(write)?;
        assert_eq!(std::fs::read(&path)?, b"ordered");

        let discarded = native.write_all_batch_async(vec![OwnedWrite {
            offset: 0,
            bytes: Bytes::from_static(b"discarded"),
        }]);
        let resize = native.set_len_async(16);
        drop(discarded);
        complete_write(resize)?;
        assert_eq!(std::fs::metadata(&path)?.len(), 16);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn uncertain_completion_poison_is_file_local_and_fences_successors() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let poisoned_path = temporary.path().join("poisoned");
        let poisoned = NativeFile::from_file(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&poisoned_path)?,
        )?;
        let prior = Arc::new(OperationFence::default());
        *poisoned
            .tail
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&prior));
        let state = Mutex::new(CompletionState::<()> {
            result: None,
            panic: None,
            waker: None,
            _allocation: None,
        });
        poison_file_health(&poisoned.file, &poisoned.uncertain);
        finish_file_job(
            &state,
            Err(linux::uncertain_completion(io::Error::other(
                "injected ambiguous completion",
            ))),
            Some(prior),
            Some(&poisoned.uncertain),
        );
        let Err(error) = complete_write(poisoned.set_len_async(4)) else {
            return Err(io::Error::other(
                "an uncertain prior operation did not fence its successor",
            ));
        };
        assert!(error.to_string().contains("prior native I/O completion"));
        assert_eq!(std::fs::metadata(poisoned_path)?.len(), 0);

        let unrelated_path = temporary.path().join("unrelated");
        let unrelated = NativeFile::from_file(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&unrelated_path)?,
        )?;
        complete_write(unrelated.set_len_async(4))?;
        assert_eq!(std::fs::metadata(unrelated_path)?.len(), 4);
        Ok(())
    }

    #[test]
    fn platform_control_is_offloaded_and_file_sequenced() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let native = NativeFile::from_file(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(temporary.path().join("ordered-control"))?,
        )?;
        let mut write = native.write_all_batch_async(vec![OwnedWrite {
            offset: 0,
            bytes: Bytes::from_static(b"ready"),
        }]);
        let waker = completion_waker();
        let deadline = Instant::now() + Duration::from_secs(10);
        let write_completed = loop {
            match Pin::new(&mut write).poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(result) => {
                    result?;
                    break true;
                }
                Poll::Pending if write.pending.is_none() => break false,
                Poll::Pending if Instant::now() < deadline => {
                    std::thread::park_timeout(deadline.saturating_duration_since(Instant::now()));
                }
                Poll::Pending => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "write admission stalled",
                    ));
                }
            }
        };
        let (sender, receiver) = mpsc::channel();
        let control = native.control_async(move |file| {
            let mut bytes = [0; 5];
            read_at(file, 0, &mut bytes)?;
            sender
                .send(bytes)
                .map_err(|error| io::Error::other(error.to_string()))
        });
        complete_write(control)?;
        assert_eq!(receiver.recv().map_err(io::Error::other)?, *b"ready");
        if !write_completed {
            complete_write(write)?;
        }
        Ok(())
    }

    #[test]
    fn unpolled_operation_does_not_delay_a_later_awaited_operation() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("skipped-operation-order");
        let native = NativeFile::from_file(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)?,
        )?;
        let never_polled = native.set_len_async(8);
        let last = native.set_len_async(32);
        complete_write(last)?;
        assert_eq!(std::fs::metadata(&path)?.len(), 32);
        drop(never_polled);
        assert_eq!(std::fs::metadata(&path)?.len(), 32);
        Ok(())
    }

    #[test]
    fn skipped_fence_chain_completes_iteratively() -> io::Result<()> {
        let first = Arc::new(OperationFence::default());
        let mut prior = Arc::clone(&first);
        for _ in 0..100_000 {
            let skipped = Arc::new(OperationFence::default());
            skipped.complete_after(Some(prior));
            prior = skipped;
        }
        first.complete();
        assert!(prior.ready_or_register(Waker::noop()));
        Ok(())
    }

    #[test]
    fn skipped_fence_does_not_wake_a_dropped_waiter() {
        struct CountWake(AtomicU64);
        impl Wake for CountWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let predecessor = Arc::new(OperationFence::default());
        let skipped = Arc::new(OperationFence::default());
        let count = Arc::new(CountWake(AtomicU64::new(0)));
        let waker = Waker::from(Arc::clone(&count));
        assert!(!predecessor.ready_or_register(&waker));
        skipped.complete_after(Some(Arc::clone(&predecessor)));
        predecessor.complete();
        assert_eq!(count.0.load(Ordering::Relaxed), 0);
        assert!(skipped.ready_or_register(Waker::noop()));
    }

    #[test]
    fn saturated_host_transactions_do_not_starve_native_file_io() -> io::Result<()> {
        struct GateRelease(Arc<(Mutex<bool>, Condvar)>);
        impl Drop for GateRelease {
            fn drop(&mut self) {
                let (lock, ready) = &*self.0;
                *lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
                ready.notify_all();
            }
        }

        let parallelism = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        let count = parallelism.saturating_mul(2).clamp(2, 16);
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let release = GateRelease(Arc::clone(&gate));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = mpsc::channel();
        let mut observers = Vec::new();
        let waker = completion_waker();
        for _ in 0..count {
            let gate = Arc::clone(&gate);
            let entered_tx = entered_tx.clone();
            let finished_tx = finished_tx.clone();
            let mut task = run_blocking_io(move || {
                let _ = entered_tx.send(());
                let (lock, ready) = &*gate;
                let mut released = lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                while !*released {
                    released = ready
                        .wait(released)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                let _ = finished_tx.send(());
            });
            assert!(matches!(
                Pin::new(&mut task).poll(&mut Context::from_waker(&waker)),
                Poll::Pending
            ));
            observers.push(task);
        }
        for _ in 0..count {
            entered_rx
                .recv_timeout(Duration::from_secs(10))
                .map_err(|error| io::Error::other(error.to_string()))?;
        }
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("independent-native-io"))?;
        complete_read(read_batch_async(
            file,
            vec![OwnedRead {
                offset: 0,
                length: 1,
            }],
        ))?;
        drop(release);
        for _ in 0..count {
            finished_rx
                .recv_timeout(Duration::from_secs(10))
                .map_err(|error| io::Error::other(error.to_string()))?;
        }
        drop(observers);
        Ok(())
    }

    #[cfg(not(target_vendor = "apple"))]
    #[test]
    fn barrier_durability_rejects_targets_without_an_exact_primitive() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = File::create(temporary.path().join("barrier"))?;
        assert!(matches!(
            sync_file(&file, Durability::Barrier),
            Err(error) if error.kind() == io::ErrorKind::Unsupported
        ));
        assert!(matches!(
            sync_data(&file, Durability::Barrier),
            Err(error) if error.kind() == io::ErrorKind::Unsupported
        ));
        Ok(())
    }

    #[test]
    fn native_read_batch_preserves_order_and_short_reads() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("read-batch"))?;
        write_all_at(&file, 0, b"abcdefgh")?;
        let reads = complete_read(read_batch_async(
            file,
            vec![
                OwnedRead {
                    offset: 5,
                    length: 8,
                },
                OwnedRead {
                    offset: 1,
                    length: 3,
                },
            ],
        ))?;
        assert_eq!(
            reads,
            [Bytes::from_static(b"fgh"), Bytes::from_static(b"bcd")]
        );
        Ok(())
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn native_batches_reject_all_invalid_offsets_before_submission() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("invalid-offset-batch");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        write_all_at(&file, 0, b"stable")?;

        let read = complete_read(read_batch_async(
            file.try_clone()?,
            vec![
                OwnedRead {
                    offset: 0,
                    length: 1,
                },
                OwnedRead {
                    offset: u64::MAX,
                    length: 1,
                },
            ],
        ));
        assert!(matches!(read, Err(error) if error.kind() == io::ErrorKind::InvalidInput));

        let write = complete_write(write_all_batch_async(
            file,
            vec![
                OwnedWrite {
                    offset: 0,
                    bytes: Bytes::from_static(b"changed"),
                },
                OwnedWrite {
                    offset: u64::MAX,
                    bytes: Bytes::from_static(b"x"),
                },
            ],
        ));
        assert!(matches!(write, Err(error) if error.kind() == io::ErrorKind::InvalidInput));
        assert_eq!(std::fs::read(path)?, b"stable");
        Ok(())
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn apple_batches_preserve_unlinked_open_files() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("unlinked-open-file");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        std::fs::remove_file(path)?;
        complete_write(write_all_batch_async(
            file.try_clone()?,
            vec![OwnedWrite {
                offset: 3,
                bytes: Bytes::from_static(b"kept"),
            }],
        ))?;
        assert_eq!(
            complete_read(read_batch_async(
                file,
                vec![OwnedRead {
                    offset: 3,
                    length: 4,
                }],
            ))?,
            [Bytes::from_static(b"kept")]
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn io_uring_rejects_cursor_aliases_before_any_submission() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("invalid-io-uring-offset");
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        write_all_at(&file, 0, b"stable")?;
        file.seek(SeekFrom::Start(3))?;

        let mut byte = [0_u8; 1];
        assert!(matches!(
            read_at(&file, u64::MAX, &mut byte),
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
        assert!(matches!(
            write_all_at(&file, u64::MAX, b"x"),
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
        assert!(matches!(
            complete_read(read_batch_async(
                file.try_clone()?,
                vec![
                    OwnedRead {
                        offset: 0,
                        length: 1,
                    },
                    OwnedRead {
                        offset: u64::MAX,
                        length: 1,
                    },
                ],
            )),
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
        assert!(matches!(
            complete_write(write_all_batch_async(
                file.try_clone()?,
                vec![
                    OwnedWrite {
                        offset: 0,
                        bytes: Bytes::from_static(b"changed"),
                    },
                    OwnedWrite {
                        offset: u64::MAX,
                        bytes: Bytes::from_static(b"x"),
                    },
                ],
            )),
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
        assert_eq!(file.stream_position()?, 3);
        assert_eq!(std::fs::read(path)?, b"stable");
        Ok(())
    }

    #[test]
    fn owned_read_outlives_the_callers_file_and_request() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("owned-read");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        write_all_at(&file, 0, b"abcdefgh")?;
        let read = read_batch_async(
            file,
            vec![OwnedRead {
                offset: 2,
                length: 4,
            }],
        );
        let actual = complete_read(read)?;
        assert_eq!(actual, [Bytes::from_static(b"cdef")]);
        Ok(())
    }

    #[test]
    fn admitted_write_outlives_dropped_observer() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("detached-write");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let mut write = write_all_batch_async(
            file,
            vec![OwnedWrite {
                offset: 0,
                bytes: Bytes::from_static(b"persisted"),
            }],
        );
        let (sender, receiver) = mpsc::sync_channel(1);
        let admitted = write
            .pending
            .take()
            .ok_or_else(|| io::Error::other("write was not pending admission"))?;
        let completed = Arc::clone(&write.state);
        sender
            .try_send(admitted)
            .map_err(|_| io::Error::other("write admission failed"))?;

        drop(write);
        let job = receiver
            .recv()
            .map_err(|_| io::Error::other("admitted write disappeared"))?;
        let _ = job.run();

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(result) = completed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .result
                .take()
            {
                result?;
                break;
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "detached admitted write did not complete",
                ));
            }
            std::thread::park_timeout(Duration::from_millis(1));
        }

        assert_eq!(std::fs::read(path)?, b"persisted");
        Ok(())
    }

    #[test]
    fn dropped_admission_waiters_do_not_block_later_reads() -> io::Result<()> {
        enum FirstPoll {
            Ready(io::Result<Vec<Bytes>>),
            Pending(Box<ReadBatch>),
        }

        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("admission-drop");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(path)?;
        write_all_at(&file, 0, b"x")?;
        let mut reads = Vec::new();
        for _ in 0..64 {
            let mut read = read_batch_async(
                file.try_clone()?,
                vec![OwnedRead {
                    offset: 0,
                    length: 1,
                }],
            );
            reads.push(
                match Pin::new(&mut read).poll(&mut Context::from_waker(Waker::noop())) {
                    Poll::Ready(result) => FirstPoll::Ready(result),
                    Poll::Pending => FirstPoll::Pending(Box::new(read)),
                },
            );
        }
        for read in &reads {
            if let FirstPoll::Ready(result) = read {
                match result {
                    Ok(completed) => assert_eq!(completed, &[Bytes::from_static(b"x")]),
                    Err(error) => {
                        return Err(io::Error::new(
                            error.kind(),
                            format!("initial native read failed: {error}"),
                        ));
                    }
                }
            }
        }
        reads.truncate(8);
        for read in reads {
            let completed = match read {
                FirstPoll::Ready(result) => result?,
                FirstPoll::Pending(read) => complete_read(*read)?,
            };
            assert_eq!(completed, [Bytes::from_static(b"x")]);
        }
        let final_read = read_batch_async(
            file,
            vec![OwnedRead {
                offset: 0,
                length: 1,
            }],
        );
        assert_eq!(complete_read(final_read)?, [Bytes::from_static(b"x")]);
        Ok(())
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one ordered scenario keeps each step next to the state it checks"
    )]
    fn bounded_native_admission_is_work_conserving_and_cancellation_wakes_the_successor()
    -> io::Result<()> {
        use std::sync::atomic::AtomicUsize;

        struct CountWake(AtomicUsize);

        impl Wake for CountWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        fn job(file: &File) -> io::Result<NativeJob> {
            Ok(NativeJob::Read {
                file: Arc::new(NativeFilePayload::unmetered(file.try_clone()?)),
                reads: vec![OwnedRead {
                    offset: 0,
                    length: 1,
                }],
                span: obs::OperationSpan::new(tracing::Span::none()),
                #[cfg(windows)]
                overlapped: false,
                tail: None,
                uncertain: None,
                state: Arc::new(Mutex::new(CompletionState {
                    result: None,
                    panic: None,
                    waker: None,
                    _allocation: None,
                })),
                completion: None,
                #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                _health: None,
            })
        }

        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("fifo-admission"))?;
        write_all_at(&file, 0, b"x")?;
        let (sender, receiver) = mpsc::sync_channel(1);
        let workers = NativeWorkers {
            sender,
            admission: Arc::new(Mutex::new(AdmissionState::new())),
        };
        workers
            .sender
            .try_send(job(&file)?)
            .map_err(|_| io::Error::other("failed to saturate test queue"))?;
        workers
            .admission
            .lock()
            .map_err(|_| io::Error::other("admission mutex poisoned"))?
            .queued_jobs += 1;

        let mut first = Some(job(&file)?);
        let mut first_waiter = None;
        let mut second = Some(job(&file)?);
        let mut second_waiter = None;
        let noop = Waker::noop();
        let context = Context::from_waker(noop);
        assert!(matches!(
            poll_submission_with(&workers, &mut first, &mut first_waiter, &context),
            Poll::Pending
        ));
        assert!(matches!(
            poll_submission_with(&workers, &mut second, &mut second_waiter, &context),
            Poll::Pending
        ));
        let _ = receiver
            .recv()
            .map_err(|_| io::Error::other("queue closed"))?;
        notify_capacity_released(&workers.admission);
        assert!(matches!(
            poll_submission_with(&workers, &mut second, &mut second_waiter, &context),
            Poll::Ready(Ok(()))
        ));
        assert!(matches!(
            poll_submission_with(&workers, &mut first, &mut first_waiter, &context),
            Poll::Pending
        ));
        let _ = receiver
            .recv()
            .map_err(|_| io::Error::other("queue closed"))?;
        notify_capacity_released(&workers.admission);
        assert!(matches!(
            poll_submission_with(&workers, &mut first, &mut first_waiter, &context),
            Poll::Ready(Ok(()))
        ));
        let _ = receiver
            .recv()
            .map_err(|_| io::Error::other("queue closed"))?;
        notify_capacity_released(&workers.admission);

        workers
            .sender
            .try_send(job(&file)?)
            .map_err(|_| io::Error::other("failed to saturate cancellation queue"))?;
        workers
            .admission
            .lock()
            .map_err(|_| io::Error::other("admission mutex poisoned"))?
            .queued_jobs += 1;
        let mut cancelled = Some(job(&file)?);
        let mut cancelled_waiter = None;
        assert!(matches!(
            poll_submission_with(&workers, &mut cancelled, &mut cancelled_waiter, &context),
            Poll::Pending
        ));
        let wake_count = Arc::new(CountWake(AtomicUsize::new(0)));
        let successor_waker = Waker::from(Arc::clone(&wake_count));
        let successor_context = Context::from_waker(&successor_waker);
        let mut successor = Some(job(&file)?);
        let mut successor_waiter = None;
        assert!(matches!(
            poll_submission_with(
                &workers,
                &mut successor,
                &mut successor_waiter,
                &successor_context
            ),
            Poll::Pending
        ));
        remove_capacity_waiter(&workers, cancelled_waiter.take());
        assert_eq!(wake_count.0.load(Ordering::Relaxed), 1);
        assert!(workers.admission.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner).head.as_ref()
            .is_some_and(|head| successor_waiter.as_ref()
                .is_some_and(|successor| Arc::ptr_eq(head, successor))));
        Ok(())
    }

    #[test]
    fn inactive_admission_waiter_cannot_strand_other_files() -> io::Result<()> {
        use std::sync::atomic::AtomicUsize;

        struct CountWake(AtomicUsize);
        impl Wake for CountWake {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let (sender, receiver) = mpsc::sync_channel(1);
        let workers = NativeWorkers {
            sender,
            admission: Arc::new(Mutex::new(AdmissionState::new())),
        };
        let job = || NativeJob::Task {
            operation: Box::new(|| None), meter: None, _allocation: None,
        };
        workers
            .sender
            .try_send(job())
            .map_err(|_| io::Error::other("failed to saturate admission queue"))?;
        workers
            .admission
            .lock()
            .map_err(|_| io::Error::other("admission mutex poisoned"))?
            .queued_jobs += 1;

        let mut inactive = Some(job());
        let mut inactive_waiter = None;
        assert!(matches!(
            poll_submission_with(
                &workers,
                &mut inactive,
                &mut inactive_waiter,
                &Context::from_waker(Waker::noop()),
            ),
            Poll::Pending
        ));
        let wakes = Arc::new(CountWake(AtomicUsize::new(0)));
        let active_waker = Waker::from(Arc::clone(&wakes));
        let context = Context::from_waker(&active_waker);
        let mut active = Some(job());
        let mut active_waiter = None;
        assert!(matches!(
            poll_submission_with(&workers, &mut active, &mut active_waiter, &context),
            Poll::Pending
        ));

        let _ = receiver.recv().map_err(io::Error::other)?;
        notify_capacity_released(&workers.admission);
        assert!(wakes.0.load(Ordering::Relaxed) > 0);
        assert!(matches!(
            poll_submission_with(&workers, &mut active, &mut active_waiter, &context),
            Poll::Ready(Ok(()))
        ));
        let _ = receiver.recv().map_err(io::Error::other)?;
        notify_capacity_released(&workers.admission);
        assert!(matches!(
            poll_submission_with(
                &workers,
                &mut inactive,
                &mut inactive_waiter,
                &Context::from_waker(Waker::noop()),
            ),
            Poll::Ready(Ok(()))
        ));
        let _ = receiver.recv().map_err(io::Error::other)?;
        notify_capacity_released(&workers.admission);
        assert_eq!(
            workers
                .admission
                .lock()
                .map_err(|_| io::Error::other("admission mutex poisoned"))?
                .queued_jobs,
            0
        );
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn native_read_batch_preserves_empty_and_eof_ranges() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("read-batch-eof"))?;
        write_all_at(&file, 0, b"abc")?;
        let reads = complete_read(read_batch_async(
            file.try_clone()?,
            vec![
                OwnedRead {
                    offset: 0,
                    length: 0,
                },
                OwnedRead {
                    offset: 3,
                    length: 4,
                },
                OwnedRead {
                    offset: 30,
                    length: 4,
                },
            ],
        ))?;
        assert_eq!(reads, [Bytes::new(), Bytes::new(), Bytes::new()]);
        for (offset, expected) in b"abc".iter().copied().enumerate() {
            let repeated = complete_read(read_batch_async(
                file.try_clone()?,
                vec![OwnedRead {
                    offset: offset as u64,
                    length: 1,
                }],
            ))?;
            assert_eq!(repeated, [Bytes::copy_from_slice(&[expected])]);
        }
        Ok(())
    }

    #[test]
    #[ignore = "local backend comparison; run with --ignored --nocapture on each target"]
    fn owned_io_backend_baseline() -> io::Result<()> {
        const BLOCK_BYTES: usize = 64 * 1024;
        const BLOCKS: usize = 256;
        let directory = tempfile::tempdir()?;
        let payload = Bytes::from(vec![0x5a; BLOCK_BYTES]);
        let baseline = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(directory.path().join("baseline"))?;
        let native = NativeFile::from_file(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(directory.path().join("native"))?,
        )?;

        let copy_start = Instant::now();
        for _ in 0..BLOCKS {
            std::hint::black_box(Bytes::copy_from_slice(&payload));
        }
        let copy_elapsed = copy_start.elapsed();

        let baseline_start = Instant::now();
        for block in 0..BLOCKS {
            direct_write_at(&baseline, &payload, (block * BLOCK_BYTES) as u64)?;
        }
        let baseline_write = baseline_start.elapsed();
        baseline.sync_all()?;
        let baseline_sync = baseline_start.elapsed();
        let mut buffer = vec![0_u8; BLOCK_BYTES];
        for block in 0..BLOCKS {
            assert_eq!(
                direct_read_at(&baseline, &mut buffer, (block * BLOCK_BYTES) as u64)?,
                BLOCK_BYTES
            );
            assert_eq!(buffer, payload);
        }
        let baseline_elapsed = baseline_start.elapsed();

        complete_read(native.read_batch_async(Vec::new()))?;
        let native_start = Instant::now();
        complete_write(
            native.write_all_batch_async(
                (0..BLOCKS)
                    .map(|block| OwnedWrite {
                        offset: (block * BLOCK_BYTES) as u64,
                        bytes: payload.clone(),
                    })
                    .collect(),
            ),
        )?;
        let native_write = native_start.elapsed();
        complete_write(native.sync_async(Durability::Full))?;
        let native_sync = native_start.elapsed();
        let reads = complete_read(
            native.read_batch_async(
                (0..BLOCKS)
                    .map(|block| OwnedRead {
                        offset: (block * BLOCK_BYTES) as u64,
                        length: BLOCK_BYTES,
                    })
                    .collect(),
            ),
        )?;
        assert_eq!(reads.len(), BLOCKS);
        assert!(reads.iter().all(|bytes| bytes == &payload));
        let native_elapsed = native_start.elapsed();
        let serial_start = Instant::now();
        for block in 0..BLOCKS {
            let read = complete_read(native.read_batch_async(vec![OwnedRead {
                offset: (block * BLOCK_BYTES) as u64,
                length: BLOCK_BYTES,
            }]))?;
            assert_eq!(read.as_slice(), std::slice::from_ref(&payload));
        }
        let native_serial_read = serial_start.elapsed();
        eprintln!(
            "owned-io-baseline os={} arch={} bytes={} copy_us={} baseline_us={}/{}/{} native_us={}/{}/{} native_serial_read_us={}",
            std::env::consts::OS,
            std::env::consts::ARCH,
            BLOCKS * BLOCK_BYTES * 2,
            copy_elapsed.as_micros(),
            baseline_write.as_micros(),
            baseline_sync.as_micros() - baseline_write.as_micros(),
            baseline_elapsed.as_micros() - baseline_sync.as_micros(),
            native_write.as_micros(),
            native_sync.as_micros() - native_write.as_micros(),
            native_elapsed.as_micros() - native_sync.as_micros(),
            native_serial_read.as_micros(),
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn native_read_batch_chunks_large_ranges() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("large-read-batch"))?;
        let expected = Bytes::from(vec![0x5a; 1024 * 1024 + 1]);
        write_all_at(&file, 7, &expected)?;
        let actual = complete_read(read_batch_async(
            file,
            vec![OwnedRead {
                offset: 7,
                length: expected.len(),
            }],
        ))?;
        assert_eq!(actual, [expected]);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn native_read_batch_windows_requests_beyond_the_ring() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("windowed-read-batch"))?;
        let expected: Vec<u8> = (0..17).collect();
        write_all_at(&file, 0, &expected)?;
        let requests = (0_u64..17)
            .map(|offset| OwnedRead { offset, length: 1 })
            .collect();
        let actual = complete_read(read_batch_async(file, requests))?;
        assert_eq!(
            actual,
            expected
                .into_iter()
                .map(|value| Bytes::copy_from_slice(&[value]))
                .collect::<Vec<_>>()
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn native_read_batch_windows_one_range_beyond_the_ring() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(temporary.path().join("windowed-large-read"))?;
        let expected = Bytes::from(vec![0x69; 16 * 1024 * 1024 + 1]);
        write_all_at(&file, 3, &expected)?;
        let actual = complete_read(read_batch_async(
            file,
            vec![OwnedRead {
                offset: 3,
                length: expected.len(),
            }],
        ))?;
        assert_eq!(actual, [expected]);
        Ok(())
    }
}
