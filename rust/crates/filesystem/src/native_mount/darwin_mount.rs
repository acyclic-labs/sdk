//! Self-contained macOS mount projection over loopback `NFSv4`.
//!
//! The embedded C transport translates NFS requests into this module's bounded
//! callbacks; the canonical Rust checkout remains the only filesystem state.

#![allow(unsafe_code)]

use super::{
    DriverStartFailure, MountAttributeWriteMode, MountDirectoryEntry, MountFilesystem, MountLookup,
    MountNodeKind, MountOpenFile, MountPath, MountRangeAllocation, MountSeekTarget,
    MountSourceError, NativeMountError, NativeMountRequest, metadata_or, system_time_ns,
};
use crate::FileId;
use crate::kernel::{FileMetadata, MetadataField};
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::fs::Metadata;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime};

const ROOT_INODE: u64 = 1;
const DIRECTORY_PAGE_SIZE: u32 = 256;
const ATTRIBUTE_PAGE_SIZE: u32 = 256;
const MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES: usize = 1024 * 1024;
const MAXIMUM_CALLBACK_BYTES: usize = i32::MAX as usize;
const RENAME_NOREPLACE: u32 = 1;
const FALLOC_FL_KEEP_SIZE: c_int = 0x01;
const FALLOC_FL_PUNCH_HOLE: c_int = 0x02;
const FALLOC_FL_ZERO_RANGE: c_int = 0x10;
const DISKUTIL_UNMOUNT_TIMEOUT: Duration = Duration::from_secs(4);
const MOUNT_LOOP_EXIT_TIMEOUT: Duration = Duration::from_secs(4);
const DISKUTIL_VISIBILITY_TIMEOUT: Duration = Duration::from_secs(2);
const DIRECT_UNMOUNT_TIMEOUT: Duration = Duration::from_secs(3);
mod mode {
    pub(super) const IFMT: u32 = libc::S_IFMT as u32;
    pub(super) const IFIFO: u32 = libc::S_IFIFO as u32;
    pub(super) const IFSOCK: u32 = libc::S_IFSOCK as u32;
    pub(super) const IFCHR: u32 = libc::S_IFCHR as u32;
    pub(super) const IFBLK: u32 = libc::S_IFBLK as u32;
    pub(super) const IFDIR: u32 = libc::S_IFDIR as u32;
    pub(super) const IFLNK: u32 = libc::S_IFLNK as u32;
    pub(super) const IFREG: u32 = libc::S_IFREG as u32;
}

#[repr(C)]
struct NativeStat {
    inode: u64,
    logical_bytes: u64,
    blocks: u64,
    accessed_seconds: i64,
    accessed_nanoseconds: u32,
    modified_seconds: i64,
    modified_nanoseconds: u32,
    changed_seconds: i64,
    changed_nanoseconds: u32,
    created_seconds: i64,
    created_nanoseconds: u32,
    mode: u32,
    link_count: u32,
    uid: u32,
    gid: u32,
    device: u64,
    block_size: u32,
    flags: u32,
}

#[repr(C)]
struct NativeTimes {
    accessed_seconds: i64,
    accessed_nanoseconds: i64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
}

type DirectoryFiller = unsafe extern "C" fn(
    *mut c_void,
    *const c_char,
    *const libc::stat,
    libc::off_t,
    c_int,
) -> c_int;

unsafe extern "C" {
    fn acyclic_fs_darwin_mount_session_new() -> *mut c_void;
    fn acyclic_fs_darwin_mount_session_free(session: *mut c_void);
    fn acyclic_fs_darwin_mount_run(
        session: *mut c_void,
        argc: c_int,
        argv: *const *const c_char,
        mountpoint: *const c_char,
        context: usize,
    ) -> c_int;
    fn acyclic_fs_darwin_mount_interrupt(session: *mut c_void);
    fn acyclic_fs_darwin_mount_invalidate(session: *mut c_void, path: *const c_char) -> c_int;
    fn acyclic_fs_darwin_mount_fill_directory(
        buffer: *mut c_void,
        filler: DirectoryFiller,
        name: *const c_char,
        attributes: *const NativeStat,
        next_offset: i64,
    ) -> c_int;
}

struct FileHandle {
    file_id: FileId,
    file: Arc<dyn MountOpenFile>,
}

#[derive(Clone)]
struct DirectoryHandle {
    path: MountPath,
    view_epoch: Option<u64>,
    cursor: Option<Vec<u8>>,
    entries: VecDeque<MountDirectoryEntry>,
    exhausted: bool,
    emitted: i64,
    revision: Option<u64>,
}

struct DirectoryCheckpoint {
    revision: u64,
    handle: DirectoryHandle,
}

impl DirectoryHandle {
    fn new(path: MountPath, view_epoch: Option<u64>) -> Self {
        Self {
            path,
            view_epoch,
            cursor: None,
            entries: VecDeque::new(),
            exhausted: false,
            emitted: 0,
            revision: None,
        }
    }

    fn rewind(&mut self) {
        self.cursor = None;
        self.entries.clear();
        self.exhausted = false;
        self.emitted = 0;
        self.revision = None;
    }
}

struct DarwinMountContext {
    source: Arc<dyn MountFilesystem>,
    writable: bool,
    mount_uid: u32,
    mount_gid: u32,
    next_handle: AtomicU64,
    next_inode: AtomicU64,
    inodes: Mutex<HashMap<FileId, u64>>,
    files: RwLock<HashMap<u64, FileHandle>>,
    directories: Mutex<HashMap<u64, Arc<Mutex<DirectoryHandle>>>>,
    directory_checkpoints: Mutex<HashMap<MountPath, DirectoryCheckpoint>>,
    namespace_revision: AtomicU64,
}

impl DarwinMountContext {
    fn new(
        source: Arc<dyn MountFilesystem>,
        writable: bool,
        metadata: &Metadata,
        root_file_id: FileId,
    ) -> Self {
        Self {
            source,
            writable,
            mount_uid: metadata.uid(),
            mount_gid: metadata.gid(),
            next_handle: AtomicU64::new(1),
            next_inode: AtomicU64::new(ROOT_INODE + 1),
            inodes: Mutex::new(HashMap::from([(root_file_id, ROOT_INODE)])),
            files: RwLock::new(HashMap::new()),
            directories: Mutex::new(HashMap::new()),
            directory_checkpoints: Mutex::new(HashMap::new()),
            namespace_revision: AtomicU64::new(0),
        }
    }

    fn allocate_handle(&self) -> Result<u64, i32> {
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed);
        if handle == 0 || handle == u64::MAX {
            return Err(libc::EMFILE);
        }
        Ok(handle)
    }

    fn namespace_changed(&self) {
        self.namespace_revision.fetch_add(1, Ordering::Release);
    }

    fn inode(&self, file_id: FileId) -> Result<u64, i32> {
        let mut inodes = self.inodes.lock().map_err(|_| libc::EIO)?;
        if let Some(inode) = inodes.get(&file_id) {
            return Ok(*inode);
        }
        let inode = self.next_inode.fetch_add(1, Ordering::Relaxed);
        if inode <= ROOT_INODE || inode == u64::MAX {
            return Err(libc::EOVERFLOW);
        }
        inodes.try_reserve(1).map_err(|_| libc::ENOMEM)?;
        inodes.insert(file_id, inode);
        Ok(inode)
    }

    fn lookup(&self, path: &MountPath) -> Result<MountLookup, i32> {
        self.source
            .lookup(path)
            .map_err(|error| errno(&error))?
            .ok_or(libc::ENOENT)
    }

    fn open(&self, path: &MountPath) -> Result<u64, i32> {
        let file = self.source.open_file(path).map_err(|error| errno(&error))?;
        let file_id = file.lookup().map_err(|error| errno(&error))?.node.file_id;
        let handle = self.allocate_handle()?;
        let mut files = self.files.write().map_err(|_| libc::EIO)?;
        files.try_reserve(1).map_err(|_| libc::ENOMEM)?;
        files.insert(handle, FileHandle { file_id, file });
        Ok(handle)
    }

    fn file(&self, handle: u64) -> Result<Arc<dyn MountOpenFile>, i32> {
        self.files
            .read()
            .map_err(|_| libc::EIO)?
            .get(&handle)
            .map(|entry| Arc::clone(&entry.file))
            .ok_or(libc::ESTALE)
    }

    fn file_or_open(&self, path: &MountPath, handle: u64) -> Result<Arc<dyn MountOpenFile>, i32> {
        if handle == 0 {
            self.source.open_file(path).map_err(|error| errno(&error))
        } else {
            self.file(handle)
        }
    }

    fn lookup_handle(&self, path: &MountPath, handle: u64) -> Result<MountLookup, i32> {
        if handle == 0 {
            return self.lookup(path);
        }
        self.file(handle)?.lookup().map_err(|error| errno(&error))
    }

    fn attributes(&self, path: &MountPath, handle: u64) -> Result<NativeStat, i32> {
        let lookup = self.lookup_handle(path, handle)?;
        let node = lookup.node;
        let file_kind = match node.kind {
            MountNodeKind::Regular => mode::IFREG,
            MountNodeKind::Directory => mode::IFDIR,
            MountNodeKind::SymbolicLink => mode::IFLNK,
            MountNodeKind::Fifo => mode::IFIFO,
            MountNodeKind::Socket => mode::IFSOCK,
            MountNodeKind::CharacterDevice => mode::IFCHR,
            MountNodeKind::BlockDevice => mode::IFBLK,
            MountNodeKind::Unsupported => return Err(libc::EOPNOTSUPP),
        };
        let default_mode = if self.writable { 0o755 } else { 0o555 };
        let mode = metadata_or(lookup.metadata.posix_mode, default_mode) & 0o7777;
        let (accessed_seconds, accessed_nanoseconds) = metadata_time(lookup.metadata.accessed_ns);
        let (modified_seconds, modified_nanoseconds) = metadata_time(lookup.metadata.modified_ns);
        let (changed_seconds, changed_nanoseconds) = metadata_time(lookup.metadata.changed_ns);
        let (created_seconds, created_nanoseconds) = metadata_time(lookup.metadata.created_ns);
        Ok(NativeStat {
            inode: self.inode(node.file_id)?,
            logical_bytes: node.logical_bytes,
            blocks: node.logical_bytes.div_ceil(512),
            accessed_seconds,
            accessed_nanoseconds,
            modified_seconds,
            modified_nanoseconds,
            changed_seconds,
            changed_nanoseconds,
            created_seconds,
            created_nanoseconds,
            mode: file_kind | mode,
            link_count: u32::try_from(node.link_count).unwrap_or(u32::MAX),
            uid: metadata_or(lookup.metadata.posix_uid, self.mount_uid),
            gid: metadata_or(lookup.metadata.posix_gid, self.mount_gid),
            device: node
                .device
                .map(|(major, minor)| {
                    super::device::join_device(major, minor)
                        .map(|device| u64::from(u32::from_ne_bytes(device.to_ne_bytes())))
                })
                .transpose()?
                .unwrap_or(0),
            block_size: 4096,
            flags: u32::try_from(metadata_or(lookup.metadata.posix_flags, 0)).unwrap_or(u32::MAX),
        })
    }

    fn admit_write(&self) -> Result<(), i32> {
        self.writable.then_some(()).ok_or(libc::EROFS)
    }

    fn mutate_metadata(
        &self,
        path: &MountPath,
        handle: u64,
        mutate: impl FnOnce(&mut FileMetadata) -> Result<(), i32>,
    ) -> Result<(), i32> {
        self.admit_write()?;
        let current = self.lookup_handle(path, handle)?;
        let mut metadata = current.metadata;
        mutate(&mut metadata)?;
        if handle == 0 {
            self.source
                .set_attributes(path, metadata, None)
                .map_err(|error| errno(&error))
        } else {
            self.file(handle)?
                .set_attributes(metadata, None)
                .map_err(|error| errno(&error))
        }
    }
}

/// One process-owned high-level Darwin mount session.
pub(super) struct DarwinMountSession {
    resources: Option<Arc<DriverSessionResources>>,
    destination: PathBuf,
    thread: Option<JoinHandle<c_int>>,
    loop_finished_before_teardown: Option<bool>,
    loop_result: Option<MountLoopResult>,
    teardown_complete: bool,
}

#[derive(Debug)]
enum MountLoopResult {
    Exited(c_int),
    Panicked,
    TimedOut,
}

/// Both the session and mount loop own these pointers. A timed-out loop may
/// outlive the session, but neither pointer is freed until that loop exits.
struct DriverSessionResources {
    source_context: usize,
    driver_session: usize,
}

impl Drop for DriverSessionResources {
    fn drop(&mut self) {
        // SAFETY: the last owner has released these exact allocations, and
        // the mount loop retains an owner through every native callback.
        unsafe {
            acyclic_fs_darwin_mount_session_free(self.driver_session as *mut c_void);
            drop(Arc::from_raw(
                self.source_context as *const DarwinMountContext,
            ));
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnmountEvidence {
    AlreadyUnmounted,
    DiskutilUnmounted,
}

impl DarwinMountSession {
    pub(super) fn start(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
    ) -> Result<Self, DriverStartFailure> {
        static STARTUP: OnceLock<Mutex<()>> = OnceLock::new();
        let _startup = STARTUP.get_or_init(|| Mutex::new(())).lock().map_err(|_| {
            NativeMountError::Driver("Darwin mount startup lock is poisoned".to_owned())
        })?;
        let root = source
            .lookup(&MountPath::root())
            .map_err(|error| source_error(&error))?
            .ok_or_else(|| NativeMountError::Driver("volume root is absent".to_owned()))?;
        if root.node.kind != MountNodeKind::Directory {
            return Err(
                NativeMountError::Driver("volume root is not a directory".to_owned()).into(),
            );
        }
        let destination_metadata = request
            .destination
            .metadata()
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        let parent_metadata = request
            .destination
            .parent()
            .and_then(|parent| parent.metadata().ok())
            .ok_or_else(|| NativeMountError::Driver("mount parent is unavailable".to_owned()))?;
        let supports_named_attributes = source.supports_posix_named_attributes();
        let context = Arc::new(DarwinMountContext::new(
            source,
            request.writable,
            &destination_metadata,
            root.node.file_id,
        ));
        let destination = request.destination.clone();
        let destination_c = path_cstring(&destination).map_err(driver_errno)?;
        let volume_name = format!(
            "acyclic-fs-{}-{}",
            std::process::id(),
            hex::encode(request.mount_id.into_bytes())
        );
        let options = mount_options(request.writable, &volume_name, supports_named_attributes);
        let arguments = fuse_arguments(&options).map_err(driver_errno)?;
        let argument_count = c_int::try_from(arguments.len())
            .map_err(|_| NativeMountError::Driver("too many Darwin mount arguments".to_owned()))?;
        // Transfer the Rust callback context only after every fallible argument
        // conversion has completed, then bind one C interrupt handle to this
        // exact loop. The bridge registry keeps native control state scoped to
        // this session, so independent mounts do not share an interrupt target.
        let source_context = Arc::into_raw(context) as usize;
        let driver_session = unsafe { acyclic_fs_darwin_mount_session_new() } as usize;
        if driver_session == 0 {
            unsafe { drop(Arc::from_raw(source_context as *const DarwinMountContext)) };
            return Err(NativeMountError::Driver(
                "Darwin mount session allocation failed".to_owned(),
            )
            .into());
        }
        let resources = Arc::new(DriverSessionResources {
            source_context,
            driver_session,
        });
        let loop_resources = Arc::clone(&resources);
        let thread = std::thread::Builder::new()
            .name("acyclic-fs-darwin-nfs".to_owned())
            .spawn(move || {
                let pointers = arguments
                    .iter()
                    .map(|argument| argument.as_ptr())
                    .collect::<Vec<_>>();
                unsafe {
                    acyclic_fs_darwin_mount_run(
                        loop_resources.driver_session as *mut c_void,
                        argument_count,
                        pointers.as_ptr(),
                        destination_c.as_ptr(),
                        loop_resources.source_context,
                    )
                }
            })
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        Self {
            resources: Some(resources),
            destination,
            thread: Some(thread),
            loop_finished_before_teardown: None,
            loop_result: None,
            teardown_complete: false,
        }
        .await_mount(&parent_metadata)
    }

    fn await_mount(mut self, parent_metadata: &Metadata) -> Result<Self, DriverStartFailure> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match is_mounted(&self.destination, parent_metadata) {
                Ok(true) => break,
                Ok(false) => {}
                Err(error) => {
                    return Err(self.failed_start(NativeMountError::Driver(error)));
                }
            }
            if self.thread.as_ref().is_some_and(JoinHandle::is_finished) {
                let status = self.finish_thread();
                self.loop_finished_before_teardown = Some(true);
                let description = format!("{status:?}");
                self.loop_result = Some(status);
                return Err(self.failed_start(NativeMountError::Driver(format!(
                    "Darwin mount exited before the mount became visible: {description}"
                ))));
            }
            if Instant::now() >= deadline {
                return Err(self.failed_start(NativeMountError::Driver(
                    "Darwin mount did not become visible within 10 seconds".to_owned(),
                )));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        // A successful NFS mount does not prove the caller can use it: macOS
        // privacy policy can allow metadata lookups while denying every open
        // on the mounted network volume. Opening the root reads no entries or
        // content, and fails before exposing an unusable child workspace.
        if let Err(error) = std::fs::File::open(&self.destination) {
            let message = if error.raw_os_error() == Some(libc::EPERM) {
                format!(
                    "macOS denied opening the mounted network volume: {error}; for SSH sessions, enable Remote Login → Allow full disk access for remote users"
                )
            } else {
                format!("Darwin mount root could not be opened: {error}")
            };
            return Err(self.failed_start(NativeMountError::Driver(message)));
        }
        Ok(self)
    }

    fn failed_start(mut self, error: NativeMountError) -> DriverStartFailure {
        match self.stop() {
            Ok(()) => error.into(),
            Err(cleanup) => {
                // The caller will preserve the destination fence. Do not let
                // Drop retry teardown without owning that fence: its result
                // could otherwise contradict the caller's ownership decision.
                std::mem::forget(self);
                DriverStartFailure::preserving_destination_fence(NativeMountError::Driver(format!(
                    "{error}; teardown failed: {cleanup}"
                )))
            }
        }
    }

    /// Drops the kernel's cached entry/attributes for one mount-relative
    /// path (leading `/` optional), so a projection change — such as a
    /// removed route — becomes visible before any cache timeout. Best
    /// effort: transports that cannot invalidate report a driver error.
    pub(super) fn invalidate(&self, path: &[u8]) -> Result<(), NativeMountError> {
        let resources = self.resources.as_ref().ok_or_else(|| {
            NativeMountError::Driver("Darwin mount session has stopped".to_owned())
        })?;
        let mut bytes = Vec::with_capacity(path.len() + 1);
        if path.first() != Some(&b'/') {
            bytes.push(b'/');
        }
        bytes.extend_from_slice(path);
        let path = CString::new(bytes)
            .map_err(|_| NativeMountError::Driver("path contains NUL".to_owned()))?;
        // SAFETY: `driver_session` is the live bridge session this struct
        // owns until teardown, and `path` is a NUL-terminated string.
        // Reject continuation snapshots before the external invalidation can
        // overlap a directory read.
        unsafe {
            (&*(resources.source_context as *const DarwinMountContext)).namespace_changed();
        }
        let status = unsafe {
            acyclic_fs_darwin_mount_invalidate(
                resources.driver_session as *mut c_void,
                path.as_ptr(),
            )
        };
        if status == 0 {
            Ok(())
        } else {
            Err(NativeMountError::Driver(format!(
                "invalidate returned {status}"
            )))
        }
    }

    #[allow(clippy::unnecessary_wraps)]
    pub(super) fn stop(&mut self) -> Result<(), NativeMountError> {
        if self.teardown_complete {
            return Ok(());
        }
        // Observe whether the provider loop failed independently before any
        // teardown action can make it exit. `diskutil unmount` waits for the
        // transport to close on some hosts, so sampling after that call races
        // a successful detach and misclassifies it as a pre-existing failure.
        if self.loop_result.is_none() {
            self.loop_finished_before_teardown =
                Some(self.thread.as_ref().is_none_or(JoinHandle::is_finished));
        }
        let unmount = bounded_diskutil_unmount(&self.destination);
        if self
            .loop_result
            .as_ref()
            .is_none_or(|result| matches!(result, MountLoopResult::TimedOut))
        {
            if let Some(resources) = &self.resources {
                unsafe {
                    acyclic_fs_darwin_mount_interrupt(resources.driver_session as *mut c_void);
                }
            }
            let join = self.finish_thread();
            self.loop_result = Some(join);
        }
        let unmount = unmount.map_err(NativeMountError::Driver)?;
        let loop_finished_before_teardown =
            self.loop_finished_before_teardown.ok_or_else(|| {
                NativeMountError::Driver("Darwin mount teardown state is absent".to_owned())
            })?;
        let join = self.loop_result.as_ref().ok_or_else(|| {
            NativeMountError::Driver("Darwin mount loop result is absent".to_owned())
        })?;
        let result = classify_detached_loop(join, unmount, loop_finished_before_teardown);
        if result.is_ok() {
            self.teardown_complete = true;
        }
        result
    }

    fn finish_thread(&mut self) -> MountLoopResult {
        self.finish_thread_within(MOUNT_LOOP_EXIT_TIMEOUT)
    }

    fn finish_thread_within(&mut self, timeout: Duration) -> MountLoopResult {
        let Some(thread) = self.thread.as_ref() else {
            return MountLoopResult::Exited(0);
        };
        let deadline = Instant::now() + timeout;
        while !thread.is_finished() {
            if Instant::now() >= deadline {
                // Keep the join handle, callback resources, and destination
                // fence so a later stop can retry once callbacks have exited.
                return MountLoopResult::TimedOut;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let Some(thread) = self.thread.take() else {
            return MountLoopResult::Panicked;
        };
        let status = match thread.join() {
            Ok(status) => MountLoopResult::Exited(status),
            Err(_) => MountLoopResult::Panicked,
        };
        self.resources.take();
        status
    }
}

fn classify_detached_loop(
    join: &MountLoopResult,
    unmount: UnmountEvidence,
    loop_finished_before_teardown: bool,
) -> Result<(), NativeMountError> {
    match (join, unmount, loop_finished_before_teardown) {
        (MountLoopResult::Exited(0), _, false)
        | (MountLoopResult::Exited(0), UnmountEvidence::AlreadyUnmounted, true) => Ok(()),
        // An external forced detach is a mount-loss event, not a provider
        // failure.  The destination evidence distinguishes it from a
        // provider loop that died while its stale mount was still present.
        (MountLoopResult::Exited(0), _, true) => Err(NativeMountError::Driver(
            "Darwin mount loop exited before teardown began".to_owned(),
        )),
        // A detached namespace does not prove that in-flight callbacks have
        // finished. Publishing this destination now could race a late write.
        (MountLoopResult::TimedOut, _, _) => Err(NativeMountError::Driver(
            "Darwin mount callbacks have not finished; destination remains fenced".to_owned(),
        )),
        (MountLoopResult::Exited(status), _, _) => Err(NativeMountError::Driver(format!(
            "Darwin mount loop exited with status {status}"
        ))),
        (MountLoopResult::Panicked, _, _) => Err(NativeMountError::Driver(
            "Darwin mount loop thread panicked".to_owned(),
        )),
    }
}

impl Drop for DarwinMountSession {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn mount_options(
    writable: bool,
    volume_name: &str,
    supports_named_attributes: bool,
) -> Vec<String> {
    let mut options = vec![
        if writable { "rw" } else { "ro" }.to_owned(),
        "default_permissions".to_owned(),
        "noatime".to_owned(),
        "noforget".to_owned(),
        "nobrowse".to_owned(),
        format!("volname={volume_name}"),
    ];
    if supports_named_attributes {
        options.push("namedattr".to_owned());
    }
    options
}

fn fuse_arguments(options: &[String]) -> Result<Vec<CString>, i32> {
    let mut arguments = Vec::with_capacity(options.len().saturating_mul(2).saturating_add(1));
    arguments.push(CString::new("acyclic-fs").map_err(|_| libc::EINVAL)?);
    for option in options {
        arguments.push(CString::new("-o").map_err(|_| libc::EINVAL)?);
        arguments.push(CString::new(option.as_str()).map_err(|_| libc::EINVAL)?);
    }
    Ok(arguments)
}

fn is_mounted(destination: &Path, _parent: &Metadata) -> Result<bool, String> {
    use std::ffi::CStr;
    use std::os::unix::ffi::OsStrExt as _;

    let mut mounts = std::ptr::null_mut::<libc::statfs>();
    // SAFETY: `getmntinfo` initializes `mounts` to an OS-owned array retained
    // until the next call; this function consumes it before returning.
    let count = unsafe { libc::getmntinfo(&raw mut mounts, libc::MNT_NOWAIT) };
    if count <= 0 || mounts.is_null() {
        return Err(format!(
            "getmntinfo failed while querying {}: {}",
            destination.display(),
            std::io::Error::last_os_error()
        ));
    }
    let count = usize::try_from(count)
        .map_err(|_| "getmntinfo returned an invalid mount count".to_owned())?;
    // SAFETY: a positive return value is the exact initialized array length.
    let mounts = unsafe { std::slice::from_raw_parts(mounts, count) };
    let normalized = destination
        .parent()
        .and_then(|parent| parent.canonicalize().ok())
        .and_then(|parent| destination.file_name().map(|name| parent.join(name)));
    Ok(mounts.iter().any(|mount| {
        // SAFETY: Darwin guarantees that `f_mntonname` is NUL terminated.
        let mounted_at = unsafe { CStr::from_ptr(mount.f_mntonname.as_ptr()) };
        mounted_at.to_bytes() == destination.as_os_str().as_bytes()
            || normalized
                .as_deref()
                .is_some_and(|path| mounted_at.to_bytes() == path.as_os_str().as_bytes())
    }))
}

fn bounded_diskutil_unmount(destination: &Path) -> Result<UnmountEvidence, String> {
    let parent = destination
        .parent()
        .and_then(|path| path.metadata().ok())
        .ok_or_else(|| "mount parent disappeared during teardown".to_owned())?;
    if !is_mounted(destination, &parent)? {
        return Ok(UnmountEvidence::AlreadyUnmounted);
    }
    if bounded_direct_unmount(destination, &parent)? {
        return Ok(UnmountEvidence::DiskutilUnmounted);
    }
    let mut child = Command::new("/usr/sbin/diskutil")
        .arg("unmount")
        .arg("force")
        .arg(destination)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| error.to_string())?;
    let deadline = Instant::now() + DISKUTIL_UNMOUNT_TIMEOUT;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|error| error.to_string())? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            if !is_mounted(destination, &parent)? {
                return Ok(UnmountEvidence::DiskutilUnmounted);
            }
            return Err(format!(
                "diskutil unmount exceeded its {}-second bound",
                DISKUTIL_UNMOUNT_TIMEOUT.as_secs()
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    if !is_mounted(destination, &parent)? {
        return Ok(UnmountEvidence::DiskutilUnmounted);
    }
    if !status.success() {
        return Err(format!("diskutil unmount failed with status {status}"));
    }
    let verify_deadline = Instant::now() + DISKUTIL_VISIBILITY_TIMEOUT;
    while is_mounted(destination, &parent)? {
        if Instant::now() >= verify_deadline {
            return Err("Darwin mount remained visible after diskutil unmount".to_owned());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(UnmountEvidence::DiskutilUnmounted)
}

pub(super) fn recover_destination(destination: &Path) -> Result<(), NativeMountError> {
    bounded_diskutil_unmount(destination)
        .map(|_| ())
        .map_err(NativeMountError::Driver)
}

fn bounded_direct_unmount(destination: &Path, parent: &Metadata) -> Result<bool, String> {
    let mut child = Command::new("/sbin/umount")
        .arg("-f")
        .arg(destination)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| error.to_string())?;
    let deadline = Instant::now() + DIRECT_UNMOUNT_TIMEOUT;
    loop {
        if child
            .try_wait()
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return is_mounted(destination, parent).map(|mounted| !mounted);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return is_mounted(destination, parent).map(|mounted| !mounted);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn context(address: usize) -> Result<&'static DarwinMountContext, i32> {
    if address == 0 {
        return Err(libc::ESTALE);
    }
    Ok(unsafe { &*(address as *const DarwinMountContext) })
}

fn ffi_status(operation: impl FnOnce() -> Result<c_int, i32>) -> c_int {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation)) {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => -error,
        Err(_) => -libc::EIO,
    }
}

fn ffi_offset(operation: impl FnOnce() -> Result<i64, i32>) -> i64 {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation)) {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => -i64::from(error),
        Err(_) => -i64::from(libc::EIO),
    }
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_getattr(
    address: usize,
    path: *const c_char,
    handle: u64,
    result: *mut NativeStat,
) -> c_int {
    ffi_status(|| {
        let attributes = context(address)?.attributes(&mount_path(path)?, handle)?;
        unsafe { result.write(attributes) };
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_access(
    address: usize,
    path: *const c_char,
    _mask: c_int,
) -> c_int {
    ffi_status(|| {
        context(address)?.lookup(&mount_path(path)?)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_open(
    address: usize,
    path: *const c_char,
    flags: c_int,
    handle: *mut u64,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        if flags & libc::O_ACCMODE != libc::O_RDONLY {
            context.admit_write()?;
        }
        unsafe { handle.write(context.open(&mount_path(path)?)?) };
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_create(
    address: usize,
    path: *const c_char,
    mode: u32,
    uid: u32,
    gid: u32,
    _flags: c_int,
    handle: *mut u64,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        context.admit_write()?;
        let path = mount_path(path)?;
        context
            .source
            .create_file(&path, create_metadata(mode, mode::IFREG, uid, gid))
            .map_err(|error| errno(&error))?;
        context.namespace_changed();
        unsafe { handle.write(context.open(&path)?) };
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_release(
    address: usize,
    _path: *const c_char,
    handle: u64,
) -> c_int {
    ffi_status(|| {
        if handle == 0 {
            return Ok(0);
        }
        context(address)?
            .files
            .write()
            .map_err(|_| libc::EIO)?
            .remove(&handle)
            .ok_or(libc::ESTALE)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_read(
    address: usize,
    path: *const c_char,
    handle: u64,
    buffer: *mut c_char,
    length: usize,
    offset: i64,
) -> c_int {
    ffi_status(|| {
        let requested_length = bounded_length(length)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let context = context(address)?;
        let file = context.file_or_open(&mount_path(path)?, handle)?;
        let logical_bytes = file
            .lookup()
            .map_err(|error| errno(&error))?
            .node
            .logical_bytes;
        let length = logical_bytes
            .saturating_sub(offset)
            .min(u64::from(requested_length));
        if length == 0 {
            return Ok(0);
        }
        let length = u32::try_from(length).map_err(|_| libc::EOVERFLOW)?;
        let bytes = file
            .read_range(offset, length)
            .map_err(|error| errno(&error))?;
        if bytes.len() > usize::try_from(length).unwrap_or(usize::MAX) {
            return Err(libc::EIO);
        }
        unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.cast(), bytes.len()) };
        c_int::try_from(bytes.len()).map_err(|_| libc::EOVERFLOW)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_write(
    address: usize,
    path: *const c_char,
    handle: u64,
    buffer: *const c_char,
    length: usize,
    offset: i64,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        context.admit_write()?;
        let length = bounded_length(length)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let bytes = unsafe { std::slice::from_raw_parts(buffer.cast::<u8>(), length as usize) };
        context
            .file_or_open(&mount_path(path)?, handle)?
            .write_range(offset, Bytes::copy_from_slice(bytes))
            .map_err(|error| errno(&error))?;
        c_int::try_from(length).map_err(|_| libc::EOVERFLOW)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_truncate(
    address: usize,
    path: *const c_char,
    handle: u64,
    length: i64,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        context.admit_write()?;
        let length = u64::try_from(length).map_err(|_| libc::EINVAL)?;
        if handle == 0 {
            context
                .source
                .resize(&mount_path(path)?, length)
                .map_err(|error| errno(&error))?;
        } else {
            context
                .file(handle)?
                .resize(length)
                .map_err(|error| errno(&error))?;
        }
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_flush(address: usize, _handle: u64) -> c_int {
    ffi_status(|| {
        context(address)?
            .source
            .flush()
            .map_err(|error| errno(&error))?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_opendir(
    address: usize,
    path: *const c_char,
    handle: *mut u64,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        let path = mount_path(path)?;
        let view_epoch = context.source.view_epoch();
        let _view_lease = context
            .source
            .acquire_view_lease(view_epoch)
            .map_err(|error| errno(&error))?;
        if context.lookup(&path)?.node.kind != MountNodeKind::Directory {
            return Err(libc::ENOTDIR);
        }
        let allocated = context.allocate_handle()?;
        let mut directories = context.directories.lock().map_err(|_| libc::EIO)?;
        directories.try_reserve(1).map_err(|_| libc::ENOMEM)?;
        directories.insert(
            allocated,
            Arc::new(Mutex::new(DirectoryHandle::new(path, view_epoch))),
        );
        unsafe { handle.write(allocated) };
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_readdir(
    address: usize,
    _path: *const c_char,
    buffer: *mut c_void,
    filler: DirectoryFiller,
    offset: i64,
    handle: u64,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        let directory = context
            .directories
            .lock()
            .map_err(|_| libc::EIO)?
            .get(&handle)
            .cloned()
            .ok_or(libc::ESTALE)?;
        let mut directory = directory.lock().map_err(|_| libc::EIO)?;
        let _view_lease = context
            .source
            .acquire_view_lease(directory.view_epoch)
            .map_err(|error| errno(&error))?;
        if offset < 0 {
            return Err(libc::EINVAL);
        }
        if offset > 0 && directory.emitted == 0 {
            let checkpoints = context
                .directory_checkpoints
                .lock()
                .map_err(|_| libc::EIO)?;
            if let Some(checkpoint) = checkpoints.get(&directory.path)
                && checkpoint.revision == context.namespace_revision.load(Ordering::Acquire)
                && checkpoint.handle.emitted == offset
            {
                *directory = checkpoint.handle.clone();
            }
        }
        if offset < directory.emitted {
            directory.rewind();
        }
        advance_directory_to_offset(context, &mut directory, offset)?;
        while directory.emitted < 2 {
            let name = if directory.emitted == 0 { c"." } else { c".." };
            let next = directory.emitted + 1;
            let buffer_full = unsafe {
                acyclic_fs_darwin_mount_fill_directory(
                    buffer,
                    filler,
                    name.as_ptr(),
                    ptr::null(),
                    next,
                )
            };
            if buffer_full != 0 {
                checkpoint_directory(context, &directory)?;
                return Ok(0);
            }
            directory.emitted = next;
        }
        loop {
            ensure_directory_page(context, &mut directory)?;
            let Some(entry) = directory.entries.front() else {
                context
                    .directory_checkpoints
                    .lock()
                    .map_err(|_| libc::EIO)?
                    .remove(&directory.path);
                return Ok(0);
            };
            let name = CString::new(entry.name.as_slice()).map_err(|_| libc::EIO)?;
            let child = directory.path.child(entry.name.clone());
            let attributes = context.attributes(&child, 0)?;
            let next = directory.emitted.checked_add(1).ok_or(libc::EOVERFLOW)?;
            let buffer_full = unsafe {
                acyclic_fs_darwin_mount_fill_directory(
                    buffer,
                    filler,
                    name.as_ptr(),
                    &raw const attributes,
                    next,
                )
            };
            if buffer_full != 0 {
                checkpoint_directory(context, &directory)?;
                return Ok(0);
            }
            directory.entries.pop_front();
            directory.emitted = next;
        }
    })
}

fn advance_directory_to_offset(
    context: &DarwinMountContext,
    directory: &mut DirectoryHandle,
    offset: i64,
) -> Result<(), i32> {
    while directory.emitted < offset {
        if directory.emitted < 2 {
            directory.emitted += 1;
            continue;
        }
        ensure_directory_page(context, directory)?;
        if directory.entries.pop_front().is_none() {
            return Err(libc::EINVAL);
        }
        directory.emitted += 1;
    }
    Ok(())
}

fn ensure_directory_page(
    context: &DarwinMountContext,
    directory: &mut DirectoryHandle,
) -> Result<(), i32> {
    if directory.entries.is_empty() && !directory.exhausted {
        directory
            .revision
            .get_or_insert_with(|| context.namespace_revision.load(Ordering::Acquire));
        let page = context
            .source
            .read_directory(
                &directory.path,
                directory.cursor.as_deref(),
                DIRECTORY_PAGE_SIZE,
            )
            .map_err(|error| errno(&error))?;
        if page.entries.is_empty() && page.next_cursor.is_some() {
            return Err(libc::EIO);
        }
        directory.cursor = page.next_cursor;
        directory.exhausted = directory.cursor.is_none();
        directory.entries = page.entries.into();
    }
    Ok(())
}

fn checkpoint_directory(
    context: &DarwinMountContext,
    directory: &DirectoryHandle,
) -> Result<(), i32> {
    const MAXIMUM_DIRECTORY_CHECKPOINTS: usize = 64;
    let mut checkpoints = context
        .directory_checkpoints
        .lock()
        .map_err(|_| libc::EIO)?;
    let current_revision = context.namespace_revision.load(Ordering::Acquire);
    let revision = directory.revision.unwrap_or(current_revision);
    if revision != current_revision {
        checkpoints.remove(&directory.path);
        return Ok(());
    }
    if checkpoints.len() >= MAXIMUM_DIRECTORY_CHECKPOINTS
        && !checkpoints.contains_key(&directory.path)
    {
        checkpoints.clear();
    }
    checkpoints.insert(
        directory.path.clone(),
        DirectoryCheckpoint {
            revision,
            handle: directory.clone(),
        },
    );
    Ok(())
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_releasedir(address: usize, handle: u64) -> c_int {
    ffi_status(|| {
        context(address)?
            .directories
            .lock()
            .map_err(|_| libc::EIO)?
            .remove(&handle)
            .ok_or(libc::ESTALE)?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_mkdir(
    address: usize,
    path: *const c_char,
    mode: u32,
    uid: u32,
    gid: u32,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        context.admit_write()?;
        context
            .source
            .create_directory(
                &mount_path(path)?,
                create_metadata(mode, mode::IFDIR, uid, gid),
            )
            .map_err(|error| errno(&error))?;
        context.namespace_changed();
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_remove(
    address: usize,
    path: *const c_char,
    directory: c_int,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        context.admit_write()?;
        let path = mount_path(path)?;
        let lookup = context.lookup(&path)?;
        if (directory != 0) != (lookup.node.kind == MountNodeKind::Directory) {
            return Err(if directory == 0 {
                libc::EISDIR
            } else {
                libc::ENOTDIR
            });
        }
        let detached = if lookup.node.kind == MountNodeKind::Regular && lookup.node.link_count == 1
        {
            let has_open = context
                .files
                .read()
                .map_err(|_| libc::EIO)?
                .values()
                .any(|entry| entry.file_id == lookup.node.file_id);
            has_open
                .then(|| {
                    context
                        .source
                        .detach_file(&path)
                        .map_err(|error| errno(&error))
                })
                .transpose()?
        } else {
            None
        };
        context
            .source
            .remove(&path, Some(lookup.node.file_id))
            .map_err(|error| errno(&error))?;
        context.namespace_changed();
        if let Some(detached) = detached {
            for entry in context
                .files
                .write()
                .map_err(|_| libc::EIO)?
                .values_mut()
                .filter(|entry| entry.file_id == lookup.node.file_id)
            {
                entry.file = Arc::clone(&detached);
            }
        }
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_rename(
    address: usize,
    source: *const c_char,
    destination: *const c_char,
    flags: u32,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        context.admit_write()?;
        if flags & !RENAME_NOREPLACE != 0 {
            return Err(libc::EOPNOTSUPP);
        }
        context
            .source
            .rename(
                &mount_path(source)?,
                &mount_path(destination)?,
                flags & RENAME_NOREPLACE == 0,
            )
            .map_err(|error| errno(&error))?;
        context.namespace_changed();
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_link(
    address: usize,
    source: *const c_char,
    destination: *const c_char,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        context.admit_write()?;
        context
            .source
            .hard_link(&mount_path(source)?, &mount_path(destination)?)
            .map_err(|error| errno(&error))?;
        context.namespace_changed();
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_symlink(
    address: usize,
    target: *const c_char,
    destination: *const c_char,
    uid: u32,
    gid: u32,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        context.admit_write()?;
        let target = unsafe { CStr::from_ptr(target) }.to_bytes();
        context
            .source
            .create_symbolic_link(
                &mount_path(destination)?,
                Bytes::copy_from_slice(target),
                create_metadata(0o777, mode::IFLNK, uid, gid),
            )
            .map_err(|error| errno(&error))?;
        context.namespace_changed();
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_readlink(
    address: usize,
    path: *const c_char,
    buffer: *mut c_char,
    length: usize,
) -> c_int {
    ffi_status(|| {
        if length == 0 {
            return Err(libc::ERANGE);
        }
        let target = context(address)?
            .source
            .read_link(&mount_path(path)?)
            .map_err(|error| errno(&error))?;
        if target.len() >= length {
            return Err(libc::ENAMETOOLONG);
        }
        unsafe {
            ptr::copy_nonoverlapping(target.as_ptr(), buffer.cast(), target.len());
            buffer.add(target.len()).write(0);
        }
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_mknod(
    address: usize,
    path: *const c_char,
    mode: u32,
    device: u64,
    uid: u32,
    gid: u32,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        context.admit_write()?;
        let kind = match mode & mode::IFMT {
            mode::IFIFO => MountNodeKind::Fifo,
            mode::IFSOCK => MountNodeKind::Socket,
            mode::IFCHR => MountNodeKind::CharacterDevice,
            mode::IFBLK => MountNodeKind::BlockDevice,
            _ => return Err(libc::EOPNOTSUPP),
        };
        let device = matches!(
            kind,
            MountNodeKind::CharacterDevice | MountNodeKind::BlockDevice
        )
        .then(|| super::device::split_device(device));
        context
            .source
            .create_special(
                &mount_path(path)?,
                kind,
                device,
                create_metadata(mode, mode & mode::IFMT, uid, gid),
            )
            .map_err(|error| errno(&error))?;
        context.namespace_changed();
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_chmod(
    address: usize,
    path: *const c_char,
    mode: u32,
    handle: u64,
) -> c_int {
    ffi_status(|| {
        context(address)?.mutate_metadata(&mount_path(path)?, handle, |metadata| {
            let kind = metadata_or(metadata.posix_mode, 0) & mode::IFMT;
            metadata.posix_mode = MetadataField::Value(kind | (mode & 0o7777));
            Ok(())
        })?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_chown(
    address: usize,
    path: *const c_char,
    uid: u32,
    gid: u32,
    handle: u64,
) -> c_int {
    ffi_status(|| {
        context(address)?.mutate_metadata(&mount_path(path)?, handle, |metadata| {
            if uid != u32::MAX {
                metadata.posix_uid = MetadataField::Value(uid);
            }
            if gid != u32::MAX {
                metadata.posix_gid = MetadataField::Value(gid);
            }
            Ok(())
        })?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_utimens(
    address: usize,
    path: *const c_char,
    times: *const NativeTimes,
    handle: u64,
) -> c_int {
    ffi_status(|| {
        let times = unsafe { times.as_ref() }.ok_or(libc::EINVAL)?;
        context(address)?.mutate_metadata(&mount_path(path)?, handle, |metadata| {
            update_time(
                &mut metadata.accessed_ns,
                times.accessed_seconds,
                times.accessed_nanoseconds,
            )?;
            update_time(
                &mut metadata.modified_ns,
                times.modified_seconds,
                times.modified_nanoseconds,
            )?;
            Ok(())
        })?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_getxattr(
    address: usize,
    path: *const c_char,
    name: *const c_char,
    value: *mut c_char,
    length: usize,
) -> c_int {
    ffi_status(|| {
        let bytes = context(address)?
            .source
            .read_attribute(&mount_path(path)?, c_bytes(name)?)
            .map_err(|error| errno(&error))?
            .ok_or(libc::ENOATTR)?;
        copy_variable_result(&bytes, value, length)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_setxattr(
    address: usize,
    path: *const c_char,
    name: *const c_char,
    value: *const c_char,
    length: usize,
    flags: c_int,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        context.admit_write()?;
        if length > MAXIMUM_CALLBACK_BYTES {
            return Err(libc::E2BIG);
        }
        let mode = match flags {
            0 => MountAttributeWriteMode::Upsert,
            libc::XATTR_CREATE => MountAttributeWriteMode::Create,
            libc::XATTR_REPLACE => MountAttributeWriteMode::Replace,
            _ => return Err(libc::EINVAL),
        };
        let bytes = unsafe { std::slice::from_raw_parts(value.cast::<u8>(), length) };
        context
            .source
            .write_attribute(
                &mount_path(path)?,
                c_bytes(name)?,
                Bytes::copy_from_slice(bytes),
                mode,
            )
            .map_err(|error| errno(&error))?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_listxattr(
    address: usize,
    path: *const c_char,
    list: *mut c_char,
    length: usize,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        let path = mount_path(path)?;
        let mut cursor = None;
        let mut encoded = Vec::new();
        loop {
            let page = context
                .source
                .list_attributes(&path, cursor.as_deref(), ATTRIBUTE_PAGE_SIZE)
                .map_err(|error| errno(&error))?;
            for name in page.names {
                if name.contains(&0) {
                    return Err(libc::EIO);
                }
                let required = encoded
                    .len()
                    .checked_add(name.len())
                    .and_then(|value| value.checked_add(1))
                    .ok_or(libc::EOVERFLOW)?;
                if required > MAXIMUM_NATIVE_ATTRIBUTE_LIST_BYTES {
                    return Err(libc::E2BIG);
                }
                encoded.extend_from_slice(&name);
                encoded.push(0);
            }
            match page.next_cursor {
                Some(next) if cursor.as_ref() != Some(&next) => cursor = Some(next),
                Some(_) => return Err(libc::EIO),
                None => break,
            }
        }
        copy_variable_result(&encoded, list, length)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_removexattr(
    address: usize,
    path: *const c_char,
    name: *const c_char,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        context.admit_write()?;
        context
            .source
            .remove_attribute(&mount_path(path)?, c_bytes(name)?)
            .map_err(|error| errno(&error))?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_lseek(
    address: usize,
    path: *const c_char,
    handle: u64,
    offset: i64,
    whence: c_int,
) -> i64 {
    ffi_offset(|| {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let target = match whence {
            libc::SEEK_DATA => MountSeekTarget::Data,
            libc::SEEK_HOLE => MountSeekTarget::Hole,
            _ => return Err(libc::EINVAL),
        };
        let context = context(address)?;
        let result = context
            .file_or_open(&mount_path(path)?, handle)?
            .seek(offset, target)
            .map_err(|error| errno(&error))?
            .ok_or(libc::ENXIO)?;
        i64::try_from(result).map_err(|_| libc::EOVERFLOW)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_fallocate(
    address: usize,
    path: *const c_char,
    handle: u64,
    mode: c_int,
    offset: i64,
    length: i64,
) -> c_int {
    ffi_status(|| {
        let context = context(address)?;
        context.admit_write()?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let length = u64::try_from(length).map_err(|_| libc::EINVAL)?;
        let (operation, permitted_flags) = if mode & FALLOC_FL_PUNCH_HOLE != 0 {
            (
                MountRangeAllocation::PunchHole,
                FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
            )
        } else if mode & FALLOC_FL_ZERO_RANGE != 0 {
            (
                MountRangeAllocation::ZeroRange {
                    extend: mode & FALLOC_FL_KEEP_SIZE == 0,
                },
                FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE,
            )
        } else {
            (
                MountRangeAllocation::Preallocate {
                    keep_size: mode & FALLOC_FL_KEEP_SIZE != 0,
                },
                FALLOC_FL_KEEP_SIZE,
            )
        };
        if mode & !permitted_flags != 0 {
            return Err(libc::EOPNOTSUPP);
        }
        let file = context.file_or_open(&mount_path(path)?, handle)?;
        file.allocate_range(offset, length, operation)
            .map_err(|error| errno(&error))?;
        Ok(0)
    })
}

#[unsafe(no_mangle)]
unsafe extern "C" fn acyclic_fs_darwin_mount_copy_file_range(
    address: usize,
    source_path: *const c_char,
    source_handle: u64,
    source_offset: i64,
    destination_path: *const c_char,
    destination_handle: u64,
    destination_offset: i64,
    length: usize,
    flags: c_int,
) -> i64 {
    ffi_offset(|| {
        if flags != 0 {
            return Err(libc::EINVAL);
        }
        let context = context(address)?;
        context.admit_write()?;
        let source_offset = u64::try_from(source_offset).map_err(|_| libc::EINVAL)?;
        let destination_offset = u64::try_from(destination_offset).map_err(|_| libc::EINVAL)?;
        let length = u64::try_from(length).map_err(|_| libc::EOVERFLOW)?;
        let source = context.file_or_open(&mount_path(source_path)?, source_handle)?;
        let destination =
            context.file_or_open(&mount_path(destination_path)?, destination_handle)?;
        let source_lookup = source.lookup().map_err(|error| errno(&error))?;
        let destination_lookup = destination.lookup().map_err(|error| errno(&error))?;
        let length = length.min(
            source_lookup
                .node
                .logical_bytes
                .saturating_sub(source_offset),
        );
        context
            .source
            .clone_range_by_id(
                source_lookup.node.file_id,
                source_offset,
                destination_lookup.node.file_id,
                destination_offset,
                length,
            )
            .map_err(|error| errno(&error))?;
        i64::try_from(length).map_err(|_| libc::EOVERFLOW)
    })
}

fn mount_path(path: *const c_char) -> Result<MountPath, i32> {
    let bytes = c_bytes(path)?;
    if bytes.first().copied() != Some(b'/') {
        return Err(libc::EINVAL);
    }
    let mut result = MountPath::root();
    for component in bytes.split(|byte| *byte == b'/').skip(1) {
        if component.is_empty() {
            continue;
        }
        if component == b"." || component == b".." {
            return Err(libc::EPERM);
        }
        result = result.child(component.to_vec());
    }
    Ok(result)
}

fn c_bytes<'a>(value: *const c_char) -> Result<&'a [u8], i32> {
    if value.is_null() {
        return Err(libc::EINVAL);
    }
    Ok(unsafe { CStr::from_ptr(value) }.to_bytes())
}

fn path_cstring(path: &Path) -> Result<CString, i32> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| libc::EINVAL)
}

fn bounded_length(length: usize) -> Result<u32, i32> {
    if length > MAXIMUM_CALLBACK_BYTES {
        return Err(libc::E2BIG);
    }
    u32::try_from(length).map_err(|_| libc::EOVERFLOW)
}

fn copy_variable_result(bytes: &[u8], output: *mut c_char, length: usize) -> Result<c_int, i32> {
    if length == 0 {
        return c_int::try_from(bytes.len()).map_err(|_| libc::EOVERFLOW);
    }
    if output.is_null() || bytes.len() > length {
        return Err(libc::ERANGE);
    }
    unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), output.cast(), bytes.len()) };
    c_int::try_from(bytes.len()).map_err(|_| libc::EOVERFLOW)
}

fn create_metadata(mode: u32, kind: u32, uid: u32, gid: u32) -> FileMetadata {
    let now = system_time_ns(SystemTime::now()).unwrap_or(i64::MAX);
    FileMetadata {
        posix_mode: MetadataField::Value((mode & 0o7777) | kind),
        posix_uid: MetadataField::Value(uid),
        posix_gid: MetadataField::Value(gid),
        posix_flags: MetadataField::Value(0),
        windows_attributes: MetadataField::Unavailable,
        created_ns: MetadataField::Value(now),
        modified_ns: MetadataField::Value(now),
        accessed_ns: MetadataField::Value(now),
        changed_ns: MetadataField::Value(now),
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Unavailable,
        security_descriptor: MetadataField::Unavailable,
    }
}

fn update_time(field: &mut MetadataField<i64>, seconds: i64, nanoseconds: i64) -> Result<(), i32> {
    if nanoseconds == libc::UTIME_OMIT {
        return Ok(());
    }
    if nanoseconds == libc::UTIME_NOW {
        *field = MetadataField::Value(system_time_ns(SystemTime::now())?);
        return Ok(());
    }
    if !(0..1_000_000_000).contains(&nanoseconds) {
        return Err(libc::EINVAL);
    }
    let nanos = i128::from(seconds)
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i128::from(nanoseconds)))
        .ok_or(libc::EOVERFLOW)?;
    *field = MetadataField::Value(i64::try_from(nanos).map_err(|_| libc::EOVERFLOW)?);
    Ok(())
}

fn metadata_time(field: MetadataField<i64>) -> (i64, u32) {
    let nanos = match field {
        MetadataField::Unavailable => 0,
        MetadataField::Value(value) => value,
    };
    (
        nanos.div_euclid(1_000_000_000),
        u32::try_from(nanos.rem_euclid(1_000_000_000)).unwrap_or(0),
    )
}

fn errno(error: &MountSourceError) -> i32 {
    if std::env::var_os("ACYCLIC_FS_DARWIN_MOUNT_DEBUG").is_some() {
        eprintln!("acyclic-fs Darwin mount callback error: {error}");
    }
    match error {
        MountSourceError::NotFound => libc::ENOENT,
        MountSourceError::AlreadyExists => libc::EEXIST,
        MountSourceError::Invalid(_) => libc::EINVAL,
        MountSourceError::Unsupported(_) => libc::EOPNOTSUPP,
        MountSourceError::Engine(_) => libc::EIO,
        MountSourceError::Stale => libc::ESTALE,
    }
}

fn source_error(error: &MountSourceError) -> NativeMountError {
    NativeMountError::Driver(error.to_string())
}

fn driver_errno(error: i32) -> NativeMountError {
    NativeMountError::Driver(std::io::Error::from_raw_os_error(error).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stalled_mount_loop_keeps_destination_fenced_until_callbacks_finish() {
        let (release, waiting) = std::sync::mpsc::channel::<()>();
        let (exited, complete) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            let _ = waiting.recv();
            let _ = exited.send(());
            0
        });
        let mut session = DarwinMountSession {
            resources: None,
            destination: PathBuf::new(),
            thread: Some(thread),
            loop_finished_before_teardown: None,
            loop_result: None,
            teardown_complete: false,
        };
        let start = Instant::now();
        let result = session.finish_thread_within(Duration::from_millis(20));
        assert!(matches!(result, MountLoopResult::TimedOut));
        assert!(
            classify_detached_loop(&result, UnmountEvidence::DiskutilUnmounted, false).is_err()
        );
        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(session.thread.is_some());
        let _ = release.send(());
        assert!(complete.recv_timeout(Duration::from_secs(1)).is_ok());
        let result = session.finish_thread_within(Duration::from_secs(1));
        assert!(matches!(result, MountLoopResult::Exited(0)));
        assert!(classify_detached_loop(&result, UnmountEvidence::DiskutilUnmounted, false).is_ok());
        assert!(session.thread.is_none());
        // This synthetic session has no destination or native driver to stop.
        session.teardown_complete = true;
    }
}
