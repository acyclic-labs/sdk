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

/// The existing shared file allocation also owns its original invocation claim.
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
        // Snapshot liveness once so concurrent drops cannot change admitted compaction.
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
    #[allow(unsafe_code)]
    fn contains(tail: &FileSequencer) -> bool {
        let identity = Arc::as_ptr(tail) as usize;
        let mut node = ACTIVE_FILES.with(Cell::get);
        while !node.is_null() {
            // SAFETY: `with` publishes live, pinned frames on this thread and restores TLS before they drop.
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

    /// Imports a caller-held file into the same sequenced capability while admitting its actual identity probe and owned registration buffers.
    pub fn from_file_measured(
        file: File, meter: &dyn NativeReadMeter,
    ) -> Result<Self, (io::Error, File)> {
        Self::from_file_measured_inner(file, meter, None)
    }

    fn from_file_measured_inner(
        file: File, meter: &dyn NativeReadMeter, owner: Option<&Arc<dyn NativeReadMeter>>,
    ) -> Result<Self, (io::Error, File)> {
        if let Err(error) = meter.check() { return Err((error, file)); }
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        if FILE_HEALTH.get().is_none() {
            return Err((io::ErrorKind::NotConnected.into(), file));
        }
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        if owner.is_some() { return Err((io::ErrorKind::Unsupported.into(), file)); }
        let file_bytes = arc_allocation_bytes::<NativeFilePayload>();
        let allocation = if let Some(owner) = owner {
            match ReadAllocation::new(owner, file_bytes) {
                Ok(allocation) => Some(allocation),
                Err(error) => return Err((error, file)),
            }
        } else {
            if let Err(error) = meter.allocate(file_bytes as u64) { return Err((error, file)); }
            None
        };
        let file = Arc::new(NativeFilePayload { file, _allocation: allocation });
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let health = match shared_file_health_measured(&file, Some(meter)) {
            Ok(health) => health,
            Err(error) => {
                let payload = match Arc::try_unwrap(file) {
                    Ok(payload) => payload,
                    Err(_) => unreachable!("unpublished native file import is uniquely owned"),
                };
                let NativeFilePayload { file, _allocation } = payload;
                if _allocation.is_none() { meter.release(file_bytes as u64); }
                drop(_allocation);
                return Err((error, file));
            }
        };
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let (tail, uncertain) = (Arc::clone(&health.tail), Arc::clone(&health.uncertain));
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let (tail, uncertain) = {
            if let Err(error) = meter.allocate(arc_allocation_bytes::<Mutex<Option<Arc<OperationFence>>>>() as u64) {
                let payload = Arc::try_unwrap(file).ok().expect("unpublished native file import");
                meter.release(file_bytes as u64);
                return Err((error, payload.file));
            }
            let tail = Arc::new(Mutex::new(None));
            if let Err(error) = meter.allocate(arc_allocation_bytes::<AtomicBool>() as u64) {
                drop(tail);
                meter.release(arc_allocation_bytes::<Mutex<Option<Arc<OperationFence>>>>() as u64);
                let payload = Arc::try_unwrap(file).ok().expect("unpublished native file import");
                meter.release(file_bytes as u64);
                return Err((error, payload.file));
            }
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
    pub fn from_file_measured_owned(
        file: File, meter: Arc<dyn NativeReadMeter>,
    ) -> Result<Self, (io::Error, File)> {
        Self::from_file_measured_inner(file, meter.as_ref(), Some(&meter))
    }

    /// Logical handle storage owned by this capability, excluding the shared runtime health/sequencing control root.
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
    #[allow(unsafe_code)]
    pub unsafe fn from_overlapped_file_measured_unchecked(
        file: File,
        meter: &dyn NativeReadMeter,
    ) -> Result<Self, (io::Error, File)> {
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
    #[allow(unsafe_code)]
    pub unsafe fn from_overlapped_file_measured_owned_unchecked(
        file: File, meter: Arc<dyn NativeReadMeter>,
    ) -> Result<Self, (io::Error, File)> {
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

    /// Creates one metered positional read on this file's existing operation queue.
    pub fn read_bounded_async(
        &self,
        offset: u64,
        length: usize,
        meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<NativeReadCompletion> {
        self.read_bounded_async_inner(offset, length, meter, false)
    }

    /// Reads caller-owned storage; `AsMut` must expose the same initialized slice without allocating.
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
        let state = Arc::new(Mutex::new(CompletionState { _allocation: Some(allocation), ..CompletionState::default() }));
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
        let state = Arc::new(Mutex::new(CompletionState { _allocation: Some(allocation), ..CompletionState::default() }));
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

    /// Writes an already-accounted owned payload through this file's ordered native queue.
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
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
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
        let state = Arc::new(Mutex::new(CompletionState { _allocation: Some(allocation), ..CompletionState::default() }));
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
fn is_uncertain_io_error(error: &io::Error) -> bool {
    #[cfg(target_os = "linux")]
    {
        linux::is_uncertain_completion(error)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = error;
        false
    }
}

/// Shared lifetime token for native resources that must not outlive an external owner.
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
    /// Nothing valid followed the invalid frame, so it was a torn append; the log now ends before it, durably.
    Torn,
    /// A valid frame follows the invalid one, so a committed frame was damaged.
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
    operation.scope(|| operation.finish(durable_rename_engine(from, to, mode, None)))
}

/// Reads at an absolute offset without changing the file cursor.
pub fn read_at(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    with_file_admission(file, || read_at_impl(file, offset, destination))
}

/// Reads one declared source prefix into caller-admitted storage without changing the cursor or allocating.
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

/// Issues one device control request on `file` and waits for it on the calling thread, whether the handle is synchronous or overlapped; an overlapped handle's completion never reaches a port it is attached to.
#[cfg(windows)]
pub fn device_control_in_place(
    file: &impl std::os::windows::io::AsRawHandle,
    code: u32,
    input: &[u8],
    output: &mut [u8],
) -> io::Result<usize> {
    windows::control_in_place(file, code, input, output)
}

/// Applies one measured device request using the existing in-place completion primitive.
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

/// The argument that tells a service started by [`spawn_service_process`] that its standard output is the readiness channel.
pub const SERVICE_READY_ARGUMENT: &str = "--signal-ready";

/// The starter's end of a service's readiness channel.
#[derive(Debug)]
pub struct ServiceReadiness(File);

impl ServiceReadiness {
    /// Blocks until the service signals that it answers requests (`true`), or exits without signalling (`false`).
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
    /// Tells the starter that the service answers requests, and closes the channel.
    pub fn signal(mut self) -> io::Result<()> {
        io::Write::write_all(
            &mut self.0,
            b"
",
        )
    }
}

/// Takes a service's readiness channel from its standard output, which then discards everything written to it, so nothing the service prints can reach, or fail on, a starter that has exited.
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

/// Starts the current executable's `__service` mode without inheriting host standard-I/O handles, and returns the channel on which the service signals that it answers requests.
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
pub fn spawn_process_tree(command: &mut std::process::Command) -> io::Result<ProcessTree> {
    ProcessTree::spawn(command)
}

/// Starts the same owned process tree with the original invocation's meter.
pub fn spawn_process_tree_with_meter_custody(
    command: &mut std::process::Command, meter: &dyn NativeReadMeter,
    custody: &mut Option<ProcessTree>,
) -> io::Result<()> {
    ProcessTree::spawn_with_meter_custody(command, meter, custody)
}

/// Runs a trusted host command with closed stdin and bounded captured output.
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

/// A bounded native read on the existing completion engine, or an inline zero-length result with no request, buffer, fence, or wake allocation.
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

    /// Waits through the same measured task engine.
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
    Durability,
}

/// Stack-only custody of this request's unspent operation allowance.
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

    /// Invokes one actual native operation, even if recording its admitted attempt reports a late error.
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
pub struct NativeByteReservation<'a> {
    meter: &'a dyn NativeReadMeter,
    kind: NativeOperationKind,
    maximum: Option<u64>,
}

impl<'a> NativeByteReservation<'a> {
    /// Reserves the maximum physical transfer for one request.
    pub fn new(
        meter: &'a dyn NativeReadMeter, kind: NativeOperationKind, maximum: usize,
    ) -> io::Result<Self> {
        let maximum = maximum as u64;
        meter.reserve_bytes(kind, maximum)?;
        Ok(Self { meter, kind, maximum: Some(maximum) })
    }

    /// Records the exact physical transfer and refunds its unused maximum.
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
    pub fn record(&mut self, actual: usize) -> io::Result<()> {
        record_reserved_native_bytes(self.meter.as_ref(), self.kind, &mut self.maximum, actual)
    }

    /// Records a real completed prefix while retaining only its unused credit for subsequent already-admitted writes in this accepted operation.
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
pub trait NativeReadMeter: Send + Sync {
    /// Checks the original request's cancellation and ownership conditions.
    fn check(&self) -> io::Result<()>;
    /// Returns a pre-existing owner of the same ledger, allowance, cancellation and limits, without request retirement custody.
    fn allocation_owner(&self) -> Option<Arc<dyn NativeReadMeter>> { None }
    /// Withholds a concrete request's possible operations without inventing spent work; allowance remains unavailable to reentrant callbacks.
    fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> io::Result<()>;
    /// Refunds only a request token's unused withheld allowance.
    fn cancel_reserved_operations(&self, kind: NativeOperationKind, count: u64);
    /// Consumes one request-owned allowance and records its actual attempt, without cancellation checks that could abandon accepted completion.
    fn record_reserved_operation(&self, kind: NativeOperationKind) -> io::Result<()>;
    /// Preflights write bytes without reporting either bytes or an operation.
    fn admit_write_bytes(&self, maximum: usize) -> io::Result<()>;
    /// Withholds possible physical bytes without reporting a completed transfer.
    fn reserve_bytes(&self, kind: NativeOperationKind, maximum: u64) -> io::Result<()>;
    /// Refunds only this request's unconsumed physical-byte allowance.
    fn cancel_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64);
    /// Restores the request's maximum, then records its exact completed bytes, without cancellation or proof reconciliation before preserving work.
    fn record_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64, actual: u64) -> io::Result<()>;
    /// Admits and records a real allocation with this logical payload.
    fn allocate(&self, bytes: u64) -> io::Result<()>;
    /// Withholds one future allocation attempt and its known logical payload before accepting IO that may produce that allocation.
    fn reserve_allocation(&self, bytes: u64) -> io::Result<()>;
    /// Refunds only an unconsumed allocation reservation, even after cancellation.
    fn cancel_reserved_allocation(&self, bytes: u64);
    /// Converts the caller's withheld credit into one actual allocation attempt and retained logical payload immediately before the allocator boundary.
    fn record_reserved_allocation(&self, bytes: u64) -> io::Result<()>;
    /// Retains the real allocator failure in the original meter's existing inline fatal-cause slot before returning a nonallocating IO relay.
    fn allocation_failed(&self, error: std::collections::TryReserveError) -> io::Error;
    /// Admits one real reference-count/header allocation with no logical payload.
    fn allocate_refcount_header(&self) -> io::Result<()>;
    /// Retains an already allocated owned payload without another allocation.
    fn adopt(&self, bytes: u64) -> io::Result<()>;
    /// Records known SDK-owned capacity that already exists, before any late cancellation/budget check.
    fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>);
    /// Preflights the real codec byte counters.
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
    fn reserve_cleanup_write(&self) -> io::Result<u64> {
        Err(io::ErrorKind::Unsupported.into())
    }
    /// Cancels an unused reservation; unsupported meters cannot create one.
    fn cancel_cleanup_write(&self, _reservation: u64) {
        unreachable!("this meter cannot create native cleanup reservations");
    }
    /// Consumes a reservation without checking cancellation.
    fn record_cleanup_write(&self, _reservation: u64) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }
}

/// One inline, original-meter allocation credit prepared before accepted IO.
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

    /// Admits retention of an existing transferred payload, with zero allocation operations.
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

    /// Admits logical retention before an allocation/transfer, without another allocation operation.
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
    pub fn reconcile_capacity(&mut self, actual: usize) -> io::Result<()> {
        Self::reconcile_capacity_recorded(&mut self.bytes, &self.meter, actual, self.meter.as_ref())
    }

    /// Reconciles an already allocated buffer through its accepted completion meter.
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
    pub fn consume_same_meter_owned_bytes(
        mut self, expected: &Arc<dyn NativeReadMeter>,
    ) -> Result<u64, Self> {
        if !Arc::ptr_eq(&self.meter, expected) { return Err(self); }
        let bytes = self.bytes;
        self.bytes = 0;
        Ok(bytes)
    }

    /// Moves another same-ledger claim without allocating or releasing it.
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

/// Runtime-independent bounded offload for host operations without a native completion path, such as file locks and directory namespace transactions.
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

/// Marks the current thread, while the guard lives, as one whose blocking stalls only the operation it is running, such as a native filesystem callback thread polling its own request.
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
fn inline_blocking_allowed() -> bool {
    INLINE_BLOCKING.get()
}

/// Schedules one owned host operation on the bounded native worker pool, or runs it in place on a thread that admits inline blocking.
#[must_use]
pub fn run_blocking_io<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
) -> BlockingIoTask<T> {
    blocking_io_task(operation, None, None).expect("unmetered task construction cannot fail")
}

/// Uses the same host-operation queue with admission for the actual owned completion state, closure, and any capacity-waiter node.
pub fn run_blocking_io_measured<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
    meter: Arc<dyn NativeReadMeter>,
) -> io::Result<BlockingIoTask<T>> {
    blocking_io_task(operation, Some(meter), None)
}

/// Prepares one operation on the existing host-operation engine without submitting or invoking it.
pub fn prepare_blocking_io_measured<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
    meter: Arc<dyn NativeReadMeter>,
) -> io::Result<BlockingIoTask<T>> {
    ready_workers(&BLOCKING_WORKERS)?;
    let allocation_meter = meter.allocation_owner().unwrap_or_else(|| Arc::clone(&meter));
    let mut task = blocking_io_task(
        operation, Some(meter), Some(&allocation_meter),
    )?;
    task.waiter = Some(allocate_capacity_waiter(Some(&allocation_meter))?);
    task.prepared_wake = Some(allocate_blocking_wait_wake(&allocation_meter)?);
    Ok(task)
}

fn blocking_io_task<T: Send + 'static>(
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
        task_pending: true,
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
        let result = match admission {
            Ok(()) => Ok(operation()),
            Err(error) => Err(error),
        };
        span.record_result(&result);
        // Staged data is not observable as Ready. In particular, the uncalled
        // operation still owns its accepted payload on worker admission error.
        completion.lock().unwrap_or_else(std::sync::PoisonError::into_inner).result = Some(result);
    });
    let allocation = allocation_meter.map(|meter| ReadAllocation::new(
        meter, std::mem::size_of_val(&operation),
    )).transpose()?;
    Ok(BlockingIoTask {
        pending: Some(NativeJob::Task {
            operation: Box::new(operation), meter, completion: Arc::clone(&state) as Arc<dyn TaskTerminal>,
            _allocation: allocation,
        }),
        state, waiter: None, terminated: false, prepared_wake: None,
    })
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

    // Drain without admission checks; retire custody outside state/sequencer locks.
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
            // Remove only this completed tail; a concurrent successor owns its own predecessor.
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
    task_pending: bool,
    result: Option<io::Result<T>>,
    panic: Option<Box<dyn std::any::Any + Send>>,
    waker: Option<Waker>,
    _allocation: Option<ReadAllocation>,
}

/// Publication authority for the existing task's already-funded state Arc.
impl<T> Default for CompletionState<T> {
    fn default() -> Self {
        Self { task_pending: false, result: None, panic: None, waker: None, _allocation: None }
    }
}

impl<T> CompletionState<T> {
    fn poll_result(&mut self, waker: &Waker) -> Poll<io::Result<T>> {
        if !self.task_pending && let Some(result) = self.result.take() {
            return Poll::Ready(result);
        }
        if !self.waker.as_ref().is_some_and(|current| current.will_wake(waker)) {
            self.waker = Some(waker.clone());
        }
        Poll::Pending
    }
}

trait TaskTerminal: Send + Sync {
    fn stage_panic(&self, payload: Box<dyn std::any::Any + Send>);
    fn publish(&self) -> Option<Waker>;
}

impl<T: Send> TaskTerminal for Mutex<CompletionState<T>> {
    fn stage_panic(&self, payload: Box<dyn std::any::Any + Send>) {
        self.lock().unwrap_or_else(std::sync::PoisonError::into_inner).panic = Some(payload);
    }
    fn publish(&self) -> Option<Waker> {
        let mut state = self.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.task_pending = false;
        state.waker.take()
    }
}

/// The only authority to finish an admitted file operation.
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
            // Retire the handle after quarantine decisions and before publishing Ready.
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
            std::process::abort();
        }
    }
}

#[cfg(any(windows, target_os = "linux"))]
fn submit_file_io<T: Send + 'static>(
    completion: FileCompletion<T>,
    submit: impl FnOnce(Box<dyn FnOnce(io::Result<T>) + Send>) -> io::Result<()>,
) -> Option<Waker> {
    // Transfer completion only on acceptance; an abandoned accepted callback aborts.
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
        operation: Box<dyn FnOnce() + Send>,
        meter: Option<Arc<dyn NativeReadMeter>>,
        completion: Arc<dyn TaskTerminal>,
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

    /// Whether this job may run on a thread that admits inline blocking: a positional read or write on an overlapped Windows handle, which the thread can issue and wait for itself without the driver.
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

    /// Runs a job [`Self::runs_in_place`] admits on the calling thread, completing its fence exactly as the driver's completion would.
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
                        .unwrap_or_else(|_| std::process::abort());
                    finish.finish(result)
                })
            }),
            Self::Task { operation, meter, completion, _allocation } => {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation));
                drop(_allocation);
                drop(meter);
                if let Err(panic) = result { completion.stage_panic(panic); }
                completion.publish()
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
        let state = Arc::new(Mutex::new(CompletionState::default()));
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
                task_pending: false,
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
        let state = Arc::new(Mutex::new(CompletionState::default()));
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
        if !state.task_pending && let Some(payload) = state.panic.take() {
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
        let result = state.poll_result(context.waker());
        if result.is_ready() { this.terminated = true; }
        result
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
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))
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
    /// Waits synchronously through this task's existing measured completion and capacity queue, admitting only its actual SDK-owned wake payload.
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
        if !state.task_pending && let Some(payload) = state.panic.take() {
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
        let result = state.poll_result(context.waker());
        if result.is_ready() { this.terminated = true; }
        result
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
            Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
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
    // Snapshot next before waking so reentrant retirement cannot lose peers.
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

/// Initializes the existing native and host I/O services before measured requests are admitted.
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
        return Err(io::Error::from(io::ErrorKind::NotConnected));
    }
    slot.as_ref().map_err(|error| io::Error::new(error.kind(), error.to_string()))
}

fn native_workers() -> io::Result<&'static NativeWorkers> {
    NATIVE_WORKERS.as_ref().map_err(|error| io::Error::new(error.kind(), error.to_string()))
}

fn blocking_workers() -> io::Result<&'static NativeWorkers> {
    BLOCKING_WORKERS.as_ref().map_err(|error| io::Error::new(error.kind(), error.to_string()))
}

/// Starts one existing service pool.
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

/// Resizes an already-held, ordered handle. Each retry is separately admitted.
pub fn set_len_measured(file: &File, size: u64, meter: &dyn NativeReadMeter) -> io::Result<()> {
    i64::try_from(size)
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
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

/// Writes positionally through the same held completion engine, admitting any Windows mode query and private overlapped reopen before their actual calls.
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
        let result: io::Result<usize> = Err(io::Error::from(io::ErrorKind::Unsupported));
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

/// Bounded asynchronous positional source for one range of a native file, read through the file's own ordered operations.
pub struct AsyncRangeReader {
    file: Arc<NativeFile>,
    offset: u64,
    remaining: u64,
}

impl AsyncRangeReader {
    /// Creates a source for at most `length` bytes of `file` starting at `offset`.
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

    /// Reads using this file's existing ordered worker machinery, with actual request, buffer, retry, and completed-byte accounting on every exit.
    pub async fn read_measured(
        &mut self,
        maximum: usize,
        meter: Arc<dyn NativeReadMeter>,
    ) -> io::Result<NativeReadResult> {
        self.read_measured_inner(maximum, meter, false).await
    }

    /// Reads a complete declared chunk using the same finite native queue.
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
#[allow(unsafe_code)]
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

#[allow(unsafe_code)]
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

/// Retains the actual platform pathname controls through durable publication.
pub fn durable_rename_measured(
    from: &Path, to: &Path, mode: RenameMode, meter: &Arc<dyn NativeReadMeter>,
) -> io::Result<()> {
    durable_rename_engine(from, to, mode, Some(meter))
}

#[allow(unsafe_code)]
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

    #[test]
    fn a_service_that_exits_without_signalling_closes_its_readiness_channel() -> io::Result<()> {
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
    fn accepted_callback_cannot_disappear_without_terminal_completion() -> io::Result<()> {
        const MARKER: &str = "ACYCLIC_DROPPED_COMPLETION_CHILD";
        if std::env::var_os(MARKER).is_some() {
            let state = Arc::new(Mutex::new(CompletionState::<()> {
                task_pending: false,
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
        used_operations: AtomicU64,
        used_bytes: AtomicU64,
        used_allocations: AtomicU64,
        read: AtomicU64,
        pub(super) memory: AtomicU64,
        operation_limit: u64,
        byte_limit: u64,
        allocation_limit: u64,
        pub(super) deny_allocations: AtomicBool,
        pub(super) operation_denial: Option<NativeOperationKind>,
        pub(super) cancel_after_operation: bool,
        memory_limit: u64,
        pub(super) cancelled: AtomicBool,
        cancel_after_read: bool,
        panic_after_read: bool,
        allocation_failure: parking_lot::Mutex<Option<std::collections::TryReserveError>>,
    }

    impl ReadBudgetMeter {
        pub(super) fn new(operation_limit: u64, byte_limit: u64) -> Self {
            Self {
                used_operations: AtomicU64::new(0), used_bytes: AtomicU64::new(0),
                used_allocations: AtomicU64::new(0), read: AtomicU64::new(0), memory: AtomicU64::new(0),
                operation_limit, byte_limit, allocation_limit: u64::MAX, memory_limit: 1024 * 1024,
                deny_allocations: AtomicBool::new(false), operation_denial: None, cancel_after_operation: false,
                cancelled: AtomicBool::new(false), cancel_after_read: false, panic_after_read: false,
                allocation_failure: parking_lot::Mutex::new(None),
            }
        }
        fn refuse() -> io::Error { io::ErrorKind::Other.into() }
    }

    impl NativeReadMeter for ReadBudgetMeter {
        fn check(&self) -> io::Result<()> {
            if self.cancelled.load(Ordering::Acquire) { Err(io::ErrorKind::Interrupted.into()) }
            else { Ok(()) }
        }
        fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> io::Result<()> {
            self.check()?;
            if self.operation_denial == Some(kind) { return Err(Self::refuse()); }
            self.used_operations.fetch_update(Ordering::AcqRel, Ordering::Acquire,
                |used| used.checked_add(count).filter(|total| *total <= self.operation_limit))
                .map(|_| ()).map_err(|_| Self::refuse())
        }
        fn cancel_reserved_operations(&self, _: NativeOperationKind, count: u64) {
            assert!(self.used_operations.fetch_sub(count, Ordering::AcqRel) >= count);
        }
        fn record_reserved_operation(&self, _: NativeOperationKind) -> io::Result<()> {
            if self.cancel_after_operation { self.cancelled.store(true, Ordering::Release); }
            Ok(())
        }
        fn reserve_bytes(&self, kind: NativeOperationKind, maximum: u64) -> io::Result<()> {
            assert_ne!(kind, NativeOperationKind::Durability);
            self.check()?;
            self.used_bytes.fetch_update(Ordering::AcqRel, Ordering::Acquire,
                |used| used.checked_add(maximum).filter(|total| *total <= self.byte_limit))
                .map(|_| ()).map_err(|_| Self::refuse())
        }
        fn cancel_reserved_bytes(&self, _: NativeOperationKind, maximum: u64) {
            assert!(self.used_bytes.fetch_sub(maximum, Ordering::AcqRel) >= maximum);
        }
        fn record_reserved_bytes(&self, _: NativeOperationKind, maximum: u64, actual: u64) -> io::Result<()> {
            assert!(actual <= maximum);
            assert!(self.used_bytes.fetch_sub(maximum - actual, Ordering::AcqRel) >= maximum);
            self.read.fetch_add(actual, Ordering::AcqRel);
            if self.cancel_after_read && actual != 0 { self.cancelled.store(true, Ordering::Release); }
            Ok(())
        }
        fn allocate(&self, bytes: u64) -> io::Result<()> {
            self.reserve_allocation(bytes)?;
            self.record_reserved_allocation(bytes)
        }
        fn reserve_allocation(&self, bytes: u64) -> io::Result<()> {
            self.check()?;
            if self.deny_allocations.load(Ordering::Acquire) { return Err(Self::refuse()); }
            self.used_allocations.fetch_update(Ordering::AcqRel, Ordering::Acquire,
                |used| used.checked_add(1).filter(|total| *total <= self.allocation_limit))
                .map_err(|_| Self::refuse())?;
            if let Err(error) = self.adopt(bytes) {
                self.used_allocations.fetch_sub(1, Ordering::AcqRel);
                return Err(error);
            }
            Ok(())
        }
        fn cancel_reserved_allocation(&self, bytes: u64) {
            assert!(self.used_allocations.fetch_sub(1, Ordering::AcqRel) != 0);
            self.release(bytes);
        }
        fn record_reserved_allocation(&self, _: u64) -> io::Result<()> { Ok(()) }
        fn allocation_failed(&self, error: std::collections::TryReserveError) -> io::Error {
            let mut failure = self.allocation_failure.lock();
            if failure.is_none() { *failure = Some(error); }
            io::ErrorKind::OutOfMemory.into()
        }
        fn allocate_refcount_header(&self) -> io::Result<()> { self.allocate(0) }
        fn adopt(&self, bytes: u64) -> io::Result<()> {
            self.check()?;
            self.memory.fetch_update(Ordering::AcqRel, Ordering::Acquire,
                |used| used.checked_add(bytes).filter(|total| *total <= self.memory_limit))
                .map(|_| ()).map_err(|_| Self::refuse())
        }
        fn record_completed_adoption(&self, bytes: u64) -> (u64, io::Result<()>) {
            let previous = match self.memory.fetch_update(Ordering::AcqRel, Ordering::Acquire,
                |used| used.checked_add(bytes)) {
                Ok(previous) => previous,
                Err(_) => return (0, Err(Self::refuse())),
            };
            (bytes, if previous + bytes > self.memory_limit { Err(Self::refuse()) } else { self.check() })
        }
        fn release(&self, bytes: u64) { assert!(self.memory.fetch_sub(bytes, Ordering::AcqRel) >= bytes); }
        fn examine(&self, _: u64, _: u64) -> io::Result<()> { self.check() }
        fn admit_examination(&self, _: u64, _: u64) -> io::Result<()> { self.check() }
        fn record_examination(&self, _: u64, _: u64) -> io::Result<()> { Ok(()) }
        fn admit_bytes(&self, maximum: usize) -> io::Result<()> {
            self.check()?;
            self.used_bytes.load(Ordering::Acquire).checked_add(maximum as u64)
                .filter(|total| *total <= self.byte_limit).map(|_| ()).ok_or_else(Self::refuse)
        }
        fn admit_read(&self, maximum: usize) -> io::Result<()> {
            self.admit_bytes(maximum)?;
            self.reserve_operations(NativeOperationKind::Read, 1)
        }
        fn admit_operations(&self, count: u64) -> io::Result<()> {
            self.check()?;
            self.used_operations.load(Ordering::Acquire).checked_add(count)
                .filter(|total| *total <= self.operation_limit).map(|_| ()).ok_or_else(Self::refuse)
        }
        fn record_operations(&self, count: u64) -> io::Result<()> {
            self.used_operations.fetch_add(count, Ordering::AcqRel);
            Ok(())
        }
        fn record_read(&self, bytes: usize) -> io::Result<()> {
            self.used_bytes.fetch_add(bytes as u64, Ordering::AcqRel);
            self.read.fetch_add(bytes as u64, Ordering::AcqRel);
            if self.cancel_after_read && bytes != 0 { self.cancelled.store(true, Ordering::Release); }
            Ok(())
        }
        fn finish_read(&self, _: usize, completed: usize) -> io::Result<()> {
            if self.panic_after_read && completed != 0 { std::panic::panic_any(0x7072_6570_6172_6564_u64); }
            self.check()
        }
        fn admit_write_bytes(&self, maximum: usize) -> io::Result<()> { self.admit_bytes(maximum) }
        fn admit_write(&self, maximum: usize) -> io::Result<()> { self.admit_read(maximum) }
        fn record_write(&self, bytes: usize) -> io::Result<()> { self.record_read(bytes) }
        fn admit_write_operations(&self, count: u64) -> io::Result<()> { self.admit_operations(count) }
        fn record_write_operations(&self, count: u64) -> io::Result<()> { self.record_operations(count) }
        fn admit_durability(&self) -> io::Result<()> { self.admit_write(0) }
        fn reserve_cleanup_write(&self) -> io::Result<u64> {
            self.reserve_operations(NativeOperationKind::Write, 1)?;
            Ok(1)
        }
        fn cancel_cleanup_write(&self, token: u64) { self.cancel_reserved_operations(NativeOperationKind::Write, token); }
        fn record_cleanup_write(&self, _: u64) -> io::Result<()> { self.record_reserved_operation(NativeOperationKind::Write) }

    }

    #[cfg(unix)]
    #[test]
    fn callback_cancellation_retains_captured_prefix_and_original_owner() -> io::Result<()> {
        let mut command = std::process::Command::new("/bin/sh");
        command.args(["-c", "printf out; printf err >&2; sleep 60"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut tree = ProcessTree::spawn(&mut command)?;
        let mut meter = ReadBudgetMeter::new(1024, 32);
        meter.memory_limit = 0;
        meter.allocation_limit = 0;
        let failure = tree.wait_with_exchange_observed_with_meter(
            std::time::Duration::from_secs(5), 16, 1, b"",
            [Vec::with_capacity(16), Vec::with_capacity(16), Vec::with_capacity(1)],
            &meter, || Ok(()), |_| {
                meter.cancelled.store(true, std::sync::atomic::Ordering::Release);
                Err(io::ErrorKind::Interrupted.into())
            },
        ).expect_err("caller cancels after physical stdout delivery");
        assert_eq!(failure.error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(failure.stdout, b"out");
        assert!(b"err".starts_with(&failure.stderr));
        assert_eq!(meter.read.load(Ordering::Acquire), (failure.stdout.len() + failure.stderr.len()) as u64);
        assert!(!failure.cleanup_completed);
        assert!(!tree.is_reaped());
        tree.terminate()?;
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn callback_panic_preserves_original_meter_work_and_pipe_owner() -> io::Result<()> {
        let mut command = std::process::Command::new("/bin/sh");
        command.args(["-c", "printf out; sleep 60"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut tree = ProcessTree::spawn(&mut command)?;
        let mut meter = ReadBudgetMeter::new(1024, 32);
        meter.memory_limit = 0;
        meter.allocation_limit = 0;
        let stopped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tree.wait_with_exchange_observed_with_meter(
                std::time::Duration::from_secs(5), 16, 1, b"",
                [Vec::with_capacity(16), Vec::with_capacity(16), Vec::with_capacity(1)],
                &meter, || Ok(()), |_| panic!("real protocol observer panic"),
            )
        }));
        assert!(stopped.is_err());
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        assert!(!tree.is_reaped());
        tree.terminate()?;
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

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn denied_file_import_returns_original_handle_without_fabricated_io() -> io::Result<()> {
        initialize_native_io_services()?;
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("retained-import-denial");
        std::fs::write(&path, b"original")?;
        for owned in [false, true] {
            let mut policy = ReadBudgetMeter::new(16, 8);
            policy.memory_limit = 0;
            let observer = Arc::new(policy);
            let file = File::open(&path)?;
            let result = if owned {
                let meter: Arc<dyn NativeReadMeter> = observer.clone();
                NativeFile::from_file_measured_owned(file, meter)
            } else { NativeFile::from_file_measured(file, observer.as_ref()) };
            let (error, mut file) = match result {
                Err(failure) => failure,
                Ok(_) => panic!("actual zero-memory import admission must refuse"),
            };
            assert_eq!(error.kind(), io::ErrorKind::Other);
            let mut original = [0_u8; 8];
            std::io::Read::read_exact(&mut file, &mut original)?;
            assert_eq!(&original, b"original");
            assert_eq!(observer.memory.load(Ordering::Acquire), 0);
            drop(file);
        }
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
            let _blocking = InlineBlocking::enter();
            complete_measured(read)
        } {
            Err(error) => error,
            Ok(_) => panic!("cancelled read returned success"),
        };
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
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
    fn bounded_read_byte_and_memory_refusals_precede_io() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("admission");
        std::fs::write(&path, b"abc")?;
        let file = bounded_write_file(&path, false)?;
        let byte_meter = Arc::new(ReadBudgetMeter::new(8, 2));
        assert!(file.read_bounded_async(0, 3, byte_meter.clone()).is_err());
        let mut limits = ReadBudgetMeter::new(8, 3);
        limits.memory_limit = 0;
        let memory_meter = Arc::new(limits);
        assert!(file.read_bounded_async(0, 3, memory_meter.clone()).is_err());
        Ok(())
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
        drop(task);
        release_sender.send(()).map_err(io::Error::other)?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while (!payload_dropped.load(Ordering::Acquire)
            || meter.memory.load(Ordering::Acquire) != 0) && Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        assert!(payload_dropped.load(Ordering::Acquire));
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
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
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
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
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[allow(unsafe_code)]
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
        meter: Arc<ReadBudgetMeter>,
        actual_prefix: u64,
    }

    impl Drop for ObservedWriteCustody {
        fn drop(&mut self) {
            assert!(self.bytes_dropped.load(Ordering::Acquire));
            assert_eq!(self.meter.read.load(Ordering::Acquire), self.actual_prefix);
            self.retired.store(true, Ordering::Release);
        }
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_write_custody_remains_with_admitted_job_after_observer_drop() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("custodied-detached");
        File::create(&path)?;
        let file = bounded_write_file(&path, true)?;
        let per_request = if cfg!(windows) { 2 } else { 1 };
        let meter = Arc::new(ReadBudgetMeter { memory_limit: u64::MAX, ..ReadBudgetMeter::new(per_request, 9) });
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
        let _ = receiver.recv().map_err(|_| io::Error::from(io::ErrorKind::Other))?.run();
        completed.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
            .result.take().expect("real write completes")?;
        assert!(retired.load(Ordering::Acquire));
        assert_eq!(std::fs::read(&path)?, b"persisted");
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
        let mut budget = ReadBudgetMeter { memory_limit: u64::MAX, ..ReadBudgetMeter::new(2, 9) };
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
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        assert_eq!(std::fs::read(&path)?, b"");
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_completion_memory_refusal_precedes_mutation() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("memory");
        File::create(&path)?;
        let file = bounded_write_file(&path, true)?;
        let mut budget = ReadBudgetMeter { memory_limit: u64::MAX, ..ReadBudgetMeter::new(10, 10) };
        budget.memory_limit = 0;
        let meter = Arc::new(budget);
        assert!(file.write_bounded_async(
            0, Bytes::from_static(b"x"), meter.clone(),
        ).is_err());
        assert_eq!(std::fs::metadata(path)?.len(), 0);
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

        complete_write(NativeFile::from_file(file.try_clone()?)?.write_all_batch_async(vec![
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

        complete_write(NativeFile::from_file(file.try_clone()?)?.write_all_batch_async(vec![OwnedWrite {
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
        complete_write(NativeFile::from_file(file.try_clone()?)?.write_all_batch_async(writes))?;

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
        let result = complete_write(NativeFile::from_file(file)?.write_all_batch_async(vec![
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
            complete_write(NativeFile::from_file(file.try_clone()?)?.write_all_batch_async(vec![
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

    /// An inline thread serves an overlapped handle's positional I/O in place with exactly the driver's results, before and after the driver has attached that handle, and neither moves the other.
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
        assert_eq!(complete_read(native.read_batch_async(reads()))?, expected);
        write(0, Bytes::from_static(b"A"))?;
        {
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
                task_pending: false,
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
            task_pending: false,
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

        let write = complete_write(NativeFile::from_file(file)?.write_all_batch_async(vec![
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
        complete_write(NativeFile::from_file(file.try_clone()?)?.write_all_batch_async(vec![OwnedWrite {
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
            complete_write(NativeFile::from_file(file.try_clone()?)?.write_all_batch_async(vec![
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
    fn admitted_write_outlives_dropped_observer() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("detached-write");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let mut write = NativeFile::from_file(file)?.write_all_batch_async(vec![OwnedWrite {
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
    fn prepared_blocking_controls_are_paid_before_submit_and_wait_needs_no_new_allocation() -> io::Result<()> {
        initialize_native_io_services()?;
        let file = tempfile::tempfile()?;
        file.set_len(17)?;
        let meter = Arc::new(ReadBudgetMeter::new(1, 0));
        let task = prepared_metadata_task(file, meter.clone())?;
        assert!(meter.memory.load(Ordering::Acquire) > 0);
        let stages = meter.used_allocations.load(Ordering::Acquire);
        meter.deny_allocations.store(true, Ordering::Release);
        assert_eq!(task.wait_measured(None, std::future::pending(), || {})??, 17);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        for accepted in 0..stages {
            let mut limits = ReadBudgetMeter::new(1, 0);
            limits.allocation_limit = accepted;
            let denied = Arc::new(limits);
            assert!(prepared_metadata_task(tempfile::tempfile()?, denied.clone()).is_err());
            assert_eq!(denied.memory.load(Ordering::Acquire), 0);
        }
        Ok(())
    }

    #[test]
    fn prepared_capacity_pressure_and_abandoned_observer_keep_original_job_custody()
    -> io::Result<()> {
        initialize_native_io_services()?;
        let limits = ReadBudgetMeter::new(1, 0);
        let observed = Arc::new(limits);
        let file = tempfile::tempfile()?;
        file.set_len(23)?;
        let mut task = prepared_metadata_task(file, observed.clone())?;
        let prepared_bytes = observed.memory.load(Ordering::Acquire);
        observed.deny_allocations.store(true, Ordering::Release);
        let (sender, receiver) = mpsc::sync_channel(1);
        let workers = NativeWorkers { sender, admission: Arc::new(Mutex::new(AdmissionState::new())) };
        workers.sender.try_send(NativeJob::Task {
            operation: Box::new(|| ()), meter: None,
            completion: Arc::new(Mutex::new(CompletionState::<()> {
                task_pending: true, result: None, panic: None, waker: None, _allocation: None,
            })),
            _allocation: None,
        }).map_err(|_| io::Error::other("failed to saturate canonical test queue"))?;
        workers.admission.lock().unwrap_or_else(std::sync::PoisonError::into_inner).queued_jobs = 1;
        let context = Context::from_waker(Waker::noop());
        assert!(matches!(poll_submission_with(
            &workers, &mut task.pending, &mut task.waiter, &context,
        ), Poll::Pending));
        assert_eq!(observed.memory.load(Ordering::Acquire), prepared_bytes);
        let filler = receiver.recv().map_err(io::Error::other)?;
        notify_capacity_released(&workers.admission);
        let _ = filler.run();
        assert!(matches!(poll_submission_with(
            &workers, &mut task.pending, &mut task.waiter, &context,
        ), Poll::Ready(Ok(()))));
        drop(task);
        assert!(observed.memory.load(Ordering::Acquire) > 0,
            "accepted job owns original completion and exact closure beyond observer drop");
        let accepted = receiver.recv().map_err(io::Error::other)?;
        notify_capacity_released(&workers.admission);
        let _ = accepted.run();
        assert_eq!(observed.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    #[allow(unsafe_code)]
    fn staged_admission_failure_waits_for_actual_payload_and_job_retirement() -> io::Result<()> {
        use std::os::fd::AsRawFd;
        struct HeldFile {
            file: Option<File>,
            entered: mpsc::SyncSender<()>,
            release: mpsc::Receiver<()>,
        }
        impl Drop for HeldFile {
            fn drop(&mut self) {
                self.entered.send(()).expect("actual payload-drop observer remains owned");
                self.release.recv().expect("actual destructor release remains owned");
                drop(self.file.take());
            }
        }
        initialize_native_io_services()?;
        let file = tempfile::tempfile()?;
        let fd = file.as_raw_fd();
        let (entered, entered_drop) = mpsc::sync_channel(1);
        let (release, released) = mpsc::sync_channel(1);
        let held = HeldFile { file: Some(file), entered, release: released };
        let policy = ReadBudgetMeter::new(0, 0);
        let observer = Arc::new(policy);
        let mut task = prepare_blocking_io_measured(move || { drop(held); }, observer.clone())?;
        let prepared = observer.memory.load(Ordering::Acquire);
        let job = task.pending.take().expect("same canonical prepared ingress");
        observer.cancelled.store(true, Ordering::Release);
        let worker = std::thread::spawn(move || wake_capacity_waiter(job.run()));
        entered_drop.recv_timeout(Duration::from_secs(10)).map_err(io::Error::other)?;
        assert!(Pin::new(&mut task).poll(&mut Context::from_waker(Waker::noop())).is_pending());
        assert_eq!(observer.memory.load(Ordering::Acquire), prepared,
            "the real closure allocation remains owned while its payload destructor is pending");
        assert_ne!(unsafe { libc::fcntl(fd, libc::F_GETFD) }, -1,
            "the original held file remains physically open before destructor release");
        release.send(()).map_err(io::Error::other)?;
        worker.join().map_err(|_| io::Error::from(io::ErrorKind::Other))?;
        match Pin::new(&mut task).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Err(error)) => assert_eq!(error.kind(), io::ErrorKind::Interrupted),
            _ => panic!("the actual terminal admission error must now be observable"),
        }
        assert!(observer.memory.load(Ordering::Acquire) < prepared,
            "job backing retires before terminal publication");
        drop(task);
        assert_eq!(observer.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    fn observed_host_task_panic_preserves_payload_and_retires_controls() -> io::Result<()> {
        initialize_native_io_services()?;
        let meter = Arc::new(ReadBudgetMeter::new(0, 0));
        let payload: Box<dyn std::any::Any + Send> = Box::new(0x5a5a_u64);
        let address = payload.as_ref() as *const dyn std::any::Any as *const () as usize;
        let task = prepare_blocking_io_measured(move || -> () { std::panic::resume_unwind(payload) }, meter.clone())?;
        let observed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            task.wait_measured(None, std::future::pending(), || {})
        })).expect_err("original panic reaches observer");
        assert_eq!(observed.downcast_ref::<u64>(), Some(&0x5a5a));
        assert_eq!(observed.as_ref() as *const dyn std::any::Any as *const () as usize, address);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
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
            operation: Box::new(|| ()), meter: None,
            completion: Arc::new(Mutex::new(CompletionState::<()> {
                task_pending: true, result: None, panic: None, waker: None, _allocation: None,
            })),
            _allocation: None,
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
        Ok(())
    }

    #[test]
    fn skipped_fence_chain_completes_iteratively() -> io::Result<()> {
        std::thread::Builder::new().stack_size(64 * 1024).spawn(|| {
            let first = Arc::new(OperationFence::default());
            let mut prior = first.clone();
            for _ in 0..100_000 {
                let next = Arc::new(OperationFence::default());
                next.complete_after(Some(prior));
                prior = next;
            }
            first.complete();
            assert!(prior.ready_or_register(Waker::noop()));
        })?.join().map_err(|_| io::Error::from(io::ErrorKind::Other))?;
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn file_scope_nested_unwind_and_thread_isolation() {
        let a = Arc::new(Mutex::new(None));
        let b = Arc::new(Mutex::new(None));
        FileScope::with(Some(&a), || {
            FileScope::with(Some(&b), || FileScope::with(Some(&a), || {
                assert!(FileScope::contains(&a) && FileScope::contains(&b));
            }));
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                FileScope::with(Some(&b), || panic!("unwind"));
            })).is_err());
            assert!(FileScope::contains(&a) && !FileScope::contains(&b));
            let other = a.clone();
            assert!(!std::thread::spawn(move || FileScope::contains(&other)).join().unwrap());
        });
        assert!(!FileScope::contains(&a) && !FileScope::contains(&b));
    }

    #[test]
    fn allocation_reconciliation_retains_capacity_on_refusal_and_cancellation() -> io::Result<()> {
        for cancelled in [false, true] {
            let mut limits = ReadBudgetMeter::new(0, 0);
            if !cancelled { limits.memory_limit = 3; }
            let observer = Arc::new(limits);
            let meter: Arc<dyn NativeReadMeter> = observer.clone();
            let mut allocation = ReadAllocation::new(&meter, 3)?;
            let mut payload = Vec::<u8>::new();
            payload.try_reserve_exact(if cancelled { 17 } else { 64 }).map_err(io::Error::other)?;
            observer.cancelled.store(cancelled, Ordering::Release);
            let actual = payload.capacity() as u64;
            assert_eq!(allocation.reconcile_capacity(payload.capacity()).unwrap_err().kind(),
                if cancelled { io::ErrorKind::Interrupted } else { io::ErrorKind::Other });
            assert_eq!(allocation.bytes(), actual);
            assert_eq!(observer.memory.load(Ordering::Acquire), actual);
            drop(payload);
            drop(allocation);
            assert_eq!(observer.memory.load(Ordering::Acquire), 0);
        }
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn zero_bounded_read_needs_no_quota() -> io::Result<()> {
        let file = NativeFile::from_file(tempfile::tempfile()?)?;
        let mut limits = ReadBudgetMeter::new(0, 0);
        limits.memory_limit = 0;
        limits.allocation_limit = 0;
        let meter = Arc::new(limits);
        let output = complete_measured(file.read_bounded_async(u64::MAX, 0, meter.clone())?)?;
        assert!(output.bytes.is_empty());
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
        assert_eq!(meter.read.load(Ordering::Acquire), 3);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_write_custody_retires_after_actual_partial_prefix() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("custodied-prefix");
        File::create(&path)?;
        let file = bounded_write_file(&path, true)?;
        let mut limits = ReadBudgetMeter::new(u64::MAX, 1024 * 1024 + 7);
        limits.cancel_after_read = true;
        let meter = Arc::new(limits);
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
        assert_eq!(std::fs::read(&path)?, vec![0x5a; 1024 * 1024]);
        Ok(())
    }

    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    #[test]
    fn bounded_write_cancelled_custody_records_physical_prefix_before_retirement() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("custodied-cancel");
        File::create(&path)?;
        let file = bounded_write_file(&path, true)?;
        let mut budget = ReadBudgetMeter::new(u64::MAX, 9);
        budget.cancel_after_read = true;
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
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        assert_eq!(std::fs::read(&path)?, b"persisted");
        Ok(())
    }

}
