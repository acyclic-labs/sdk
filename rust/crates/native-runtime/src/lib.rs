//! Private native file primitives shared by durable local providers.

use bytes::Bytes;
#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
use std::cell::{Cell, RefCell};
#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs::File;
use std::future::Future;
use std::io::{self, Read};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak, mpsc};
use std::task::{Context, Poll, Waker};

type FileSequencer = Arc<Mutex<Option<Arc<OperationFence>>>>;

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
    poison_anchor: Option<Arc<File>>,
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
static FILE_HEALTH: OnceLock<Mutex<FileHealthRegistry>> = OnceLock::new();

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
#[derive(Default)]
struct FileHealthRegistry {
    entries: HashMap<FileIdentity, FileHealth>,
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
impl FileHealthRegistry {
    fn sweep_idle(&mut self) {
        self.entries.retain(|_, health| {
            health.poison_anchor.is_some()
                || Arc::strong_count(&health.tail) > 1
                || Arc::strong_count(&health.uncertain) > 1
        });
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
    static ACTIVE_FILES: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
struct FileScope(usize);

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
impl FileScope {
    fn enter(tail: &FileSequencer) -> Self {
        let identity = Arc::as_ptr(tail) as usize;
        ACTIVE_FILES.with_borrow_mut(|active| active.push(identity));
        Self(identity)
    }

    fn contains(tail: &FileSequencer) -> bool {
        let identity = Arc::as_ptr(tail) as usize;
        ACTIVE_FILES.with_borrow(|active| active.contains(&identity))
    }
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
impl Drop for FileScope {
    fn drop(&mut self) {
        ACTIVE_FILES.with_borrow_mut(|active| {
            assert_eq!(
                active.pop(),
                Some(self.0),
                "native file scope order changed"
            );
        });
    }
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
fn shared_file_health(file: &File) -> io::Result<(FileSequencer, Arc<AtomicBool>)> {
    let identity = file_identity(file)?;
    let registry_lock = FILE_HEALTH.get_or_init(|| Mutex::new(FileHealthRegistry::default()));
    let mut registry = registry_lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(health) = registry.entries.get(&identity) {
        if health.poison_anchor.is_some()
            || Arc::strong_count(&health.tail) > 1
            || Arc::strong_count(&health.uncertain) > 1
        {
            return Ok((Arc::clone(&health.tail), Arc::clone(&health.uncertain)));
        }
        // No owner can still refer to this healthy gate. An OS identity may
        // have been reused since the last admission, so start a fresh gate.
        registry.entries.remove(&identity);
    }
    #[cfg(target_os = "linux")]
    let capacity = linux::file_identity_capacity();
    #[cfg(target_vendor = "apple")]
    let capacity = apple::file_identity_capacity();
    #[cfg(windows)]
    let capacity: usize = 4096;
    // Sweep metadata only near capacity. A queue of historical identities
    // would grow without bound when one healthy file is repeatedly reopened.
    if registry.entries.len() >= capacity {
        registry.sweep_idle();
    }
    if registry.entries.len() >= capacity {
        return Err(io::Error::other("native file admission capacity exhausted"));
    }
    let tail = Arc::new(Mutex::new(None));
    let uncertain = Arc::new(AtomicBool::new(false));
    registry.entries.insert(
        identity,
        FileHealth {
            tail: Arc::clone(&tail),
            uncertain: Arc::clone(&uncertain),
            poison_anchor: None,
        },
    );
    Ok((tail, uncertain))
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
fn poison_file_health(file: &Arc<File>, uncertain: &Arc<AtomicBool>) {
    let identity = file_identity(file).unwrap_or_else(|_| std::process::abort());
    let registry_lock = FILE_HEALTH.get().unwrap_or_else(|| std::process::abort());
    let mut registry = registry_lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let health = registry
        .entries
        .get_mut(&identity)
        .unwrap_or_else(|| std::process::abort());
    if !Arc::ptr_eq(&health.uncertain, uncertain) {
        std::process::abort();
    }
    health.poison_anchor.get_or_insert_with(|| Arc::clone(file));
    uncertain.store(true, Ordering::Release);
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
fn poison_borrowed_file_health(file: &File, uncertain: &Arc<AtomicBool>) {
    let anchor = Arc::new(file.try_clone().unwrap_or_else(|_| std::process::abort()));
    poison_file_health(&anchor, uncertain);
}

#[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
fn with_file_admission<T>(file: &File, operation: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
    let (tail, uncertain) = shared_file_health(file)?;
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
    let scope = FileScope::enter(&tail);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation))
        .unwrap_or_else(|_| std::process::abort());
    if result.as_ref().err().is_some_and(is_uncertain_io_error) {
        poison_borrowed_file_health(file, &uncertain);
    }
    drop(scope);
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
    file: Arc<File>,
    tail: FileSequencer,
    uncertain: Arc<AtomicBool>,
    #[cfg(windows)]
    overlapped: bool,
}

impl NativeFile {
    /// Transfers ownership of an already-authorized file handle to the runtime.
    pub fn from_file(file: File) -> io::Result<Self> {
        let file = Arc::new(file);
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        let (tail, uncertain) = shared_file_health(&file)?;
        #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
        let (tail, uncertain) = (Arc::new(Mutex::new(None)), Arc::new(AtomicBool::new(false)));
        Ok(Self {
            file,
            tail,
            uncertain,
            #[cfg(windows)]
            overlapped: false,
        })
    }

    /// Transfers a Windows handle already opened for overlapped I/O.
    ///
    /// # Safety
    /// The handle must have been opened with `FILE_FLAG_OVERLAPPED`, must not
    /// already be associated with an I/O completion port, and must have no
    /// concurrent independent I/O outside this `NativeFile`. The caller must
    /// transfer the sole I/O ownership of the handle to this function.
    #[cfg(windows)]
    #[allow(unsafe_code)]
    pub unsafe fn from_overlapped_file_unchecked(file: File) -> io::Result<Self> {
        let mut native = Self::from_file(file)?;
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
            #[cfg(windows)]
            self.overlapped,
        )
    }

    /// Submits owned positional reads while retaining this handle.
    #[must_use]
    pub fn read_batch_async(&self, reads: Vec<OwnedRead>) -> ReadBatch {
        ReadBatch::submit(
            Arc::clone(&self.file),
            reads,
            Some(Arc::clone(&self.tail)),
            Some(Arc::clone(&self.uncertain)),
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
            #[cfg(windows)]
            self.overlapped,
        )
    }
}

#[cfg(target_vendor = "apple")]
mod apple;
#[cfg(target_os = "linux")]
mod linux;
mod process_tree;
#[cfg(windows)]
mod windows;

pub use process_tree::ProcessTree;

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
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    return with_file_admission(file, || sync_file_unsequenced(file, durability));
    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    sync_file_unsequenced(file, durability)
}

fn sync_file_unsequenced(file: &File, durability: Durability) -> io::Result<()> {
    match durability {
        Durability::Full => file.sync_all(),
        Durability::Barrier => barrier_sync(file),
    }
}

/// Flushes file data according to `durability`.
pub fn sync_data(file: &File, durability: Durability) -> io::Result<()> {
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    return with_file_admission(file, || sync_data_unsequenced(file, durability));
    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    sync_data_unsequenced(file, durability)
}

fn sync_data_unsequenced(file: &File, durability: Durability) -> io::Result<()> {
    match durability {
        Durability::Full => file.sync_data(),
        Durability::Barrier => barrier_sync(file),
    }
}

/// Flushes the directory entry namespace on platforms exposing directory flushes.
pub fn sync_parent(path: &Path, durability: Durability) -> io::Result<()> {
    sync_parent_impl(path, durability)
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
    durable_rename_impl(from, to, mode)
}

/// Reads at an absolute offset without changing the file cursor.
pub fn read_at(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    return with_file_admission(file, || read_at_impl(file, offset, destination));
    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    read_at_impl(file, offset, destination)
}

/// Writes every byte at an absolute offset without changing the file cursor.
pub fn write_all_at(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    return with_file_admission(file, || write_all_at_impl(file, offset, bytes));
    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    write_all_at_impl(file, offset, bytes)
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
    let file = Arc::new(file);
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    {
        match shared_file_health(&file) {
            Ok((tail, uncertain)) => ReadBatch::submit(
                file,
                reads,
                Some(tail),
                Some(uncertain),
                #[cfg(windows)]
                false,
            ),
            Err(error) => ReadBatch::failed(error),
        }
    }
    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    ReadBatch::submit(file, reads, None, None)
}

/// Submits an owned write batch to the shared native completion workers.
///
/// Admission is irrevocable: after the first successful submission, dropping
/// the returned future does not roll back or cancel the write.
pub fn write_all_batch_async(file: File, writes: Vec<OwnedWrite>) -> UnitCompletion {
    let file = Arc::new(file);
    #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
    {
        match shared_file_health(&file) {
            Ok((tail, uncertain)) => UnitCompletion::submit(
                file,
                NativeUnitOperation::Write(writes),
                Some(tail),
                Some(uncertain),
                #[cfg(windows)]
                false,
            ),
            Err(error) => UnitCompletion::failed(error),
        }
    }
    #[cfg(not(any(windows, target_os = "linux", target_vendor = "apple")))]
    UnitCompletion::submit(file, NativeUnitOperation::Write(writes), None, None)
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
        let mut signal = [0_u8; 1];
        loop {
            match self.0.read(&mut signal) {
                Ok(read) => return Ok(read != 0),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                // A closed Windows pipe reports that its writer is gone.
                Err(error) if error.kind() == io::ErrorKind::BrokenPipe => return Ok(false),
                Err(error) => return Err(error),
            }
        }
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
/// Descendants inherit the Windows Job or Unix process group. Dropping the
/// returned guard, or explicitly terminating it, kills that containment. Unix
/// descendants can deliberately escape by creating another group or session,
/// so independently durable services require their own lifecycle ownership.
pub fn spawn_process_tree(command: &mut std::process::Command) -> io::Result<ProcessTree> {
    ProcessTree::spawn(command)
}

/// Runtime-independent completion of an owned native-file operation.
/// Once admitted, dropping the observer never cancels the underlying operation.
pub struct NativeCompletion<T> {
    state: Arc<Mutex<CompletionState<T>>>,
    pending: Option<NativeJob>,
    waiter: Option<u64>,
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

/// Runtime-independent bounded offload for host operations without a native
/// completion path, such as file locks and directory namespace transactions.
/// Dropping the observer after admission does not cancel the operation.
pub struct BlockingIoTask<T> {
    state: Arc<Mutex<CompletionState<T>>>,
    pending: Option<NativeJob>,
    waiter: Option<u64>,
    terminated: bool,
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
/// runs it in place on a thread that admits inline blocking. Creating the
/// future is lazy; polling it admits the operation.
#[must_use]
pub fn run_blocking_io<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
) -> BlockingIoTask<T> {
    let state = Arc::new(Mutex::new(CompletionState {
        result: None,
        waker: None,
    }));
    let completion = Arc::clone(&state);
    BlockingIoTask {
        pending: Some(NativeJob::Task(Box::new(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation))
                .map_err(|_| io::Error::other("native blocking I/O task panicked"));
            finish_job(&completion, result, None)
        }))),
        state,
        waiter: None,
        terminated: false,
    }
}

#[derive(Default)]
struct OperationFence {
    state: Mutex<OperationFenceState>,
    settled: Condvar,
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
    waker: Option<Waker>,
}

/// The only authority to finish an admitted file operation. Keeping the
/// fence and observer state together prevents a backend from publishing the
/// fence before it has reached a terminal native completion.
struct FileCompletion<T> {
    state: Arc<Mutex<CompletionState<T>>>,
    fence: Option<Arc<OperationFence>>,
    uncertain: Option<Arc<AtomicBool>>,
    _tail: Option<FileSequencer>,
    file: Option<Arc<File>>,
    finished: bool,
}

impl<T> FileCompletion<T> {
    fn new(
        state: Arc<Mutex<CompletionState<T>>>,
        fence: Option<Arc<OperationFence>>,
        uncertain: Option<Arc<AtomicBool>>,
        tail: Option<FileSequencer>,
    ) -> Self {
        Self {
            state,
            fence,
            uncertain,
            _tail: tail,
            file: None,
            finished: false,
        }
    }

    fn for_file(mut self, file: Arc<File>) -> Self {
        self.file = Some(file);
        self
    }

    fn finish(mut self, result: io::Result<T>) -> Option<Waker> {
        #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
        if result.as_ref().err().is_some_and(is_uncertain_io_error)
            && let (Some(file), Some(uncertain)) = (&self.file, &self.uncertain)
        {
            poison_file_health(file, uncertain);
        }
        let waker = finish_file_job(
            &self.state,
            result,
            self.fence.take(),
            self.uncertain.as_deref(),
        );
        self.finished = true;
        waker
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
        file: Arc<File>,
        reads: Vec<OwnedRead>,
        #[cfg(windows)]
        overlapped: bool,
        tail: Option<FileSequencer>,
        uncertain: Option<Arc<AtomicBool>>,
        state: Arc<Mutex<CompletionState<Vec<Bytes>>>>,
        completion: Option<Arc<OperationFence>>,
    },
    Unit {
        file: Arc<File>,
        operation: NativeUnitOperation,
        #[cfg(windows)]
        overlapped: bool,
        tail: Option<FileSequencer>,
        uncertain: Option<Arc<AtomicBool>>,
        state: Arc<Mutex<CompletionState<()>>>,
        completion: Option<Arc<OperationFence>>,
    },
    Task(Box<dyn FnOnce() -> Option<Waker> + Send>),
}

enum NativeUnitOperation {
    Write(Vec<OwnedWrite>),
    SetLen(u64),
    Sync(Durability),
    Control(FileControl),
}

type FileControl = Box<dyn FnOnce(&File) -> io::Result<()> + Send>;

impl NativeJob {
    fn set_completion(&mut self, fence: Arc<OperationFence>) {
        match self {
            Self::Read { completion, .. } | Self::Unit { completion, .. } => {
                *completion = Some(fence);
            }
            Self::Task(_) => unreachable!("blocking tasks are not file-sequenced"),
        }
    }

    fn run(self) -> Option<Waker> {
        match self {
            Self::Read {
                file,
                reads,
                #[cfg(windows)]
                overlapped,
                tail,
                uncertain,
                state,
                completion,
            } => {
                let finish = FileCompletion::new(state, completion, uncertain, tail)
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
            }
            Self::Unit {
                file,
                operation,
                #[cfg(windows)]
                overlapped,
                tail,
                uncertain,
                state,
                completion,
            } => {
                let finish = FileCompletion::new(state, completion, uncertain, tail.clone())
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
                #[cfg(any(windows, target_os = "linux", target_vendor = "apple"))]
                let _scope = tail.as_ref().map(FileScope::enter);
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
            }
            Self::Task(operation) => operation(),
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

struct NativeWorkers {
    sender: mpsc::SyncSender<NativeJob>,
    admission: Arc<Mutex<AdmissionState>>,
}

struct AdmissionState {
    waiters: VecDeque<(u64, Waker)>,
    queued_jobs: usize,
    next_waiter: u64,
}

impl AdmissionState {
    fn new() -> Self {
        Self {
            waiters: VecDeque::new(),
            queued_jobs: 0,
            next_waiter: 1,
        }
    }

    fn next_waiter(&mut self) -> u64 {
        let id = self.next_waiter;
        self.next_waiter = id.wrapping_add(1).max(1);
        id
    }
}

impl NativeCompletion<Vec<Bytes>> {
    fn submit(
        file: Arc<File>,
        reads: Vec<OwnedRead>,
        sequencer: Option<FileSequencer>,
        uncertain: Option<Arc<AtomicBool>>,
        #[cfg(windows)] overlapped: bool,
    ) -> Self {
        let state = Arc::new(Mutex::new(CompletionState {
            result: None,
            waker: None,
        }));
        Self {
            pending: Some(NativeJob::Read {
                file,
                reads,
                #[cfg(windows)]
                overlapped,
                tail: sequencer.clone(),
                uncertain: uncertain.clone(),
                state: Arc::clone(&state),
                completion: None,
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
                waker: None,
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
        file: Arc<File>,
        operation: NativeUnitOperation,
        sequencer: Option<FileSequencer>,
        uncertain: Option<Arc<AtomicBool>>,
        #[cfg(windows)] overlapped: bool,
    ) -> Self {
        let state = Arc::new(Mutex::new(CompletionState {
            result: None,
            waker: None,
        }));
        Self {
            pending: Some(NativeJob::Unit {
                file,
                operation,
                #[cfg(windows)]
                overlapped,
                tail: sequencer.clone(),
                uncertain: uncertain.clone(),
                state: Arc::clone(&state),
                completion: None,
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
) {
    let Some(sequencer) = sequencer.take() else {
        return;
    };
    let mut tail = sequencer
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *predecessor = tail.clone();
    let fence = Arc::new(OperationFence::default());
    if let Some(job) = pending {
        job.set_completion(Arc::clone(&fence));
    }
    *tail = Some(Arc::clone(&fence));
    *completion = Some(fence);
}

impl<T> Future for NativeCompletion<T> {
    type Output = io::Result<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.terminated, "native I/O polled after completion");
        reserve_if_needed(
            &mut this.sequencer,
            &mut this.predecessor,
            &mut this.completion,
            &mut this.pending,
        );
        if let Some(predecessor) = &this.predecessor {
            if !predecessor.ready_or_register(context.waker()) {
                return Poll::Pending;
            }
            this.predecessor = None;
        }
        if this
            .uncertain
            .as_ref()
            .is_some_and(|uncertain| uncertain.load(Ordering::Acquire))
        {
            this.pending = None;
            if let Some(completion) = &this.completion {
                completion.complete();
            }
            this.terminated = true;
            return Poll::Ready(Err(io::Error::other(
                "prior native I/O completion on this file is uncertain",
            )));
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

impl<T> Future for BlockingIoTask<T> {
    type Output = io::Result<T>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        assert!(!this.terminated, "host I/O task polled after completion");
        if inline_blocking_allowed()
            && let Some(job) = this.pending.take()
        {
            // The job records its result before returning; there is no
            // observer to wake yet.
            let _ = job.run();
        }
        let workers = match blocking_workers() {
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
        let mut state = this
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    waiter: &mut Option<u64>,
    context: &Context<'_>,
) -> Poll<io::Result<()>> {
    if pending.is_none() {
        return Poll::Ready(Ok(()));
    }
    let workers = match native_workers() {
        Ok(workers) => workers,
        Err(error) => return Poll::Ready(Err(error)),
    };
    poll_submission_with(workers, pending, waiter, context)
}

fn poll_submission_with(
    workers: &NativeWorkers,
    pending: &mut Option<NativeJob>,
    waiter: &mut Option<u64>,
    context: &Context<'_>,
) -> Poll<io::Result<()>> {
    let Some(job) = pending.take() else {
        return Poll::Ready(Ok(()));
    };
    let mut admission = workers
        .admission
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut identity = *waiter;
    if let Some(identity) = identity {
        if let Some((_, waker)) = admission
            .waiters
            .iter_mut()
            .find(|(candidate, _)| *candidate == identity)
        {
            waker.clone_from(context.waker());
        } else {
            admission
                .waiters
                .push_back((identity, context.waker().clone()));
        }
        if admission.queued_jobs != 0
            && admission
                .waiters
                .front()
                .is_some_and(|(front, _)| *front != identity)
        {
            *pending = Some(job);
            return Poll::Pending;
        }
    } else if admission.queued_jobs != 0 && !admission.waiters.is_empty() {
        let queued = admission.next_waiter();
        *waiter = Some(queued);
        admission
            .waiters
            .push_back((queued, context.waker().clone()));
        *pending = Some(job);
        return Poll::Pending;
    }
    match workers.sender.try_send(job) {
        Ok(()) => {
            // The receiver synchronizes through this same mutex before it
            // decrements, so a fast dequeue cannot race this increment.
            admission.queued_jobs += 1;
            let next = identity.and_then(|submitted| {
                waiter.take();
                let was_front = admission
                    .waiters
                    .front()
                    .is_some_and(|(front, _)| *front == submitted);
                admission
                    .waiters
                    .retain(|(candidate, _)| *candidate != submitted);
                was_front
                    .then(|| admission.waiters.front().map(|(_, waker)| waker.clone()))
                    .flatten()
            });
            drop(admission);
            wake_capacity_waiter(next);
            Poll::Ready(Ok(()))
        }
        Err(mpsc::TrySendError::Full(job)) => {
            if identity.is_none() {
                let queued = admission.next_waiter();
                *waiter = Some(queued);
                identity = Some(queued);
                admission
                    .waiters
                    .push_back((queued, context.waker().clone()));
            }
            debug_assert!(identity.is_some());
            *pending = Some(job);
            Poll::Pending
        }
        Err(mpsc::TrySendError::Disconnected(_)) => {
            let next = identity.and_then(|submitted| {
                waiter.take();
                let was_front = admission
                    .waiters
                    .front()
                    .is_some_and(|(front, _)| *front == submitted);
                admission
                    .waiters
                    .retain(|(candidate, _)| *candidate != submitted);
                was_front
                    .then(|| admission.waiters.front().map(|(_, waker)| waker.clone()))
                    .flatten()
            });
            drop(admission);
            wake_capacity_waiter(next);
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "native I/O workers stopped",
            )))
        }
    }
}

fn remove_capacity_waiter(workers: &NativeWorkers, waiter: Option<u64>) {
    let Some(waiter) = waiter else {
        return;
    };
    let mut admission = workers
        .admission
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let was_front = admission
        .waiters
        .front()
        .is_some_and(|(identity, _)| *identity == waiter);
    admission
        .waiters
        .retain(|(identity, _)| *identity != waiter);
    let next = was_front
        .then(|| admission.waiters.front().map(|(_, waker)| waker.clone()))
        .flatten();
    drop(admission);
    wake_capacity_waiter(next);
}

fn wake_capacity_waiter(waiter: Option<Waker>) {
    if let Some(waiter) = waiter {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| waiter.wake()));
    }
}

fn notify_capacity_released(admission: &Mutex<AdmissionState>) {
    let (first, others) = {
        let mut admission = admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert_ne!(admission.queued_jobs, 0, "native queue accounting lost");
        admission.queued_jobs = admission.queued_jobs.saturating_sub(1);
        let first = admission.waiters.front().map(|(_, waker)| waker.clone());
        // A waiter can be retained without ever being polled again. When the
        // channel drains, wake every other waiter so one inactive head cannot
        // strand unrelated work while all native workers are idle.
        let others = if admission.queued_jobs == 0 {
            admission
                .waiters
                .iter()
                .skip(1)
                .map(|(_, waker)| waker.clone())
                .collect()
        } else {
            Vec::new()
        };
        (first, others)
    };
    wake_capacity_waiter(first);
    for waiter in others {
        wake_capacity_waiter(Some(waiter));
    }
}

fn native_workers() -> io::Result<&'static NativeWorkers> {
    static WORKERS: OnceLock<io::Result<NativeWorkers>> = OnceLock::new();
    workers(&WORKERS, "acyclic-native-io", |parallelism| {
        parallelism.min(4)
    })
}

fn blocking_workers() -> io::Result<&'static NativeWorkers> {
    static WORKERS: OnceLock<io::Result<NativeWorkers>> = OnceLock::new();
    workers(&WORKERS, "acyclic-host-io", |parallelism| {
        parallelism.saturating_mul(2).clamp(2, 16)
    })
}

/// The pool in `slot`, started on first use with `worker_count` of the
/// host's parallelism. Parallelism is queried once: it reads cgroup files.
fn workers(
    slot: &'static OnceLock<io::Result<NativeWorkers>>,
    name: &str,
    worker_count: impl FnOnce(usize) -> usize,
) -> io::Result<&'static NativeWorkers> {
    slot.get_or_init(|| {
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
    })
    .as_ref()
    .map_err(|error| io::Error::new(error.kind(), error.to_string()))
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
        let Some(waiter) = self.waiter else {
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
        let Some(waiter) = self.waiter else {
            return;
        };
        let Ok(workers) = blocking_workers() else {
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

/// Bounded asynchronous positional source backed by the native completion path.
pub struct AsyncRangeReader {
    file: NativeFile,
    offset: u64,
    remaining: u64,
}

impl AsyncRangeReader {
    /// Creates a source for at most `length` bytes starting at `offset`.
    pub fn new(file: File, offset: u64, length: u64) -> io::Result<Self> {
        Ok(Self {
            file: NativeFile::from_file(file)?,
            offset,
            remaining: length,
        })
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
fn write_all_batch_owned(file: &Arc<File>, writes: Vec<OwnedWrite>) -> io::Result<()> {
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

#[cfg(all(unix, not(target_os = "linux")))]
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
fn wait_for_windows_io<T: Send + 'static>(
    submit: impl FnOnce(Box<dyn FnOnce(io::Result<T>) + Send>) -> io::Result<()>,
) -> io::Result<T> {
    let (sender, receiver) = mpsc::sync_channel(1);
    submit(Box::new(move |result| {
        let _ = sender.send(result);
    }))?;
    receiver.recv().map_err(|_| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            "completion owner stopped before terminal I/O result",
        )
    })?
}

#[cfg(windows)]
fn read_at_impl(file: &File, offset: u64, destination: &mut [u8]) -> io::Result<usize> {
    let file = Arc::new(file.try_clone()?);
    let mut reads = wait_for_windows_io(|finish| {
        windows::submit_read(
            file,
            false,
            vec![OwnedRead {
                offset,
                length: destination.len(),
            }],
            finish,
        )
    })?;
    let bytes = reads
        .pop()
        .ok_or_else(|| io::Error::other("native read returned no result"))?;
    destination
        .get_mut(..bytes.len())
        .ok_or_else(|| io::Error::other("native read exceeded submitted length"))?
        .copy_from_slice(&bytes);
    Ok(bytes.len())
}

#[cfg(windows)]
fn write_all_at_impl(file: &File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    let file = Arc::new(file.try_clone()?);
    wait_for_windows_io(|finish| {
        windows::submit_write(
            file,
            false,
            vec![OwnedWrite {
                offset,
                bytes: Bytes::copy_from_slice(bytes),
            }],
            finish,
        )
    })
}

#[cfg(target_vendor = "apple")]
#[allow(unsafe_code)]
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

#[cfg(target_os = "linux")]
#[allow(unsafe_code, reason = "renameat2 receives two live C paths")]
fn durable_rename_impl(from: &Path, to: &Path, mode: RenameMode) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt as _;
    let from_path = std::ffi::CString::new(from.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "rename path contains NUL"))?;
    let to_path = std::ffi::CString::new(to.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "rename path contains NUL"))?;
    let flags = match mode {
        RenameMode::NoReplace => libc::RENAME_NOREPLACE,
        RenameMode::Replace => 0,
    };
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            from_path.as_ptr(),
            libc::AT_FDCWD,
            to_path.as_ptr(),
            flags,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    sync_rename_parents(from, to)
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code, reason = "renamex_np receives two live C paths")]
fn durable_rename_impl(from: &Path, to: &Path, mode: RenameMode) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt as _;
    let from_path = std::ffi::CString::new(from.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "rename path contains NUL"))?;
    let to_path = std::ffi::CString::new(to.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "rename path contains NUL"))?;
    let flags = match mode {
        RenameMode::NoReplace => libc::RENAME_EXCL,
        RenameMode::Replace => 0,
    };
    if unsafe { libc::renamex_np(from_path.as_ptr(), to_path.as_ptr(), flags) } != 0 {
        return Err(io::Error::last_os_error());
    }
    sync_rename_parents(from, to)
}

#[cfg(unix)]
fn sync_rename_parents(from: &Path, to: &Path) -> io::Result<()> {
    fn namespace_parent(path: &Path) -> &Path {
        path.parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
    }
    let to_parent = namespace_parent(to);
    sync_parent(to_parent, Durability::Full)?;
    let from_parent = namespace_parent(from);
    if from_parent != to_parent {
        sync_parent(from_parent, Durability::Full)?;
    }
    Ok(())
}

#[cfg(windows)]
#[allow(unsafe_code, reason = "MoveFileExW receives terminated UTF-16 paths")]
fn durable_rename_impl(from: &Path, to: &Path, mode: RenameMode) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt as _;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };
    let mut from: Vec<u16> = from.as_os_str().encode_wide().collect();
    let mut to: Vec<u16> = to.as_os_str().encode_wide().collect();
    if from.contains(&0) || to.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "rename path contains NUL",
        ));
    }
    from.push(0);
    to.push(0);
    let flags = MOVEFILE_WRITE_THROUGH
        | if mode == RenameMode::Replace {
            MOVEFILE_REPLACE_EXISTING
        } else {
            0
        };
    // SAFETY: both arguments are live, NUL-terminated UTF-16 path buffers.
    if unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), flags) } == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
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
            waker: None,
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
            waker: None,
        }));
        let rejected_fence = Arc::new(OperationFence::default());
        assert!(
            submit_file_io(
                FileCompletion::new(
                    Arc::clone(&rejected),
                    Some(Arc::clone(&rejected_fence)),
                    None,
                    None,
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
                waker: None,
            }));
            eprintln!("dropping accepted native callback");
            let _ = submit_file_io(FileCompletion::new(state, None, None, None), |callback| {
                drop(callback);
                Ok(())
            });
            return Err(io::Error::other("missing terminal callback was accepted"));
        }
        let output = std::process::Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("tests::accepted_callback_cannot_disappear_without_terminal_completion")
            .arg("--nocapture")
            .env(MARKER, "1")
            .output()?;
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
        let output = std::process::Command::new(std::env::current_exe()?)
            .arg("--exact")
            .arg("tests::native_file_worker_unwind_is_fail_stop")
            .arg("--nocapture")
            .env(CHILD_MARKER, "1")
            .output()?;
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
                waker: None,
            })),
            None,
            Some(Arc::clone(&native.uncertain)),
            Some(Arc::clone(&native.tail)),
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
        assert!(matches!(
            Pin::new(&mut sync).poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        complete_write(sync)?;
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
            waker: None,
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
            Pending(ReadBatch),
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
                    Poll::Pending => FirstPoll::Pending(read),
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
                FirstPoll::Pending(read) => complete_read(read)?,
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
    #[allow(clippy::too_many_lines)]
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
                file: Arc::new(file.try_clone()?),
                reads: vec![OwnedRead {
                    offset: 0,
                    length: 1,
                }],
                #[cfg(windows)]
                overlapped: false,
                tail: None,
                uncertain: None,
                state: Arc::new(Mutex::new(CompletionState {
                    result: None,
                    waker: None,
                })),
                completion: None,
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
        assert_eq!(
            workers
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .waiters
                .front()
                .map(|(identity, _)| *identity),
            successor_waiter
        );
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
        let job = || NativeJob::Task(Box::new(|| None));
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
