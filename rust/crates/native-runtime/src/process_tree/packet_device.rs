//! Exclusive physical packet-device custody outside every consuming owner.
//!
//! The host is shared by this OS user within its mount and network namespaces. Its
//! readiness queue watches a mount-free thread's pidfd, rather than waiting
//! for the entire owner process (which may contain a blocked device caller).

use super::packet_device_receipt::Receipt;
use super::runner;
use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, OnceLock, PoisonError};

#[cfg(test)]
#[path = "packet_device/tests.rs"]
mod tests;

#[cfg(test)]
thread_local! {
    static BEFORE_ADMISSION: std::cell::RefCell<Option<Box<dyn FnOnce(&File)>>> = const { std::cell::RefCell::new(None) };
    static BEFORE_CONNECT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static BEFORE_LISTEN: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static WITHOUT_ADMISSION_LOCK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A host-owned physical device and the consuming owner's stream endpoint.
/// Closing it joins the host's actual device closure before returning.
pub struct PacketDevice {
    stream: File,
    retirement: PacketDeviceRetirement,
}

impl PacketDevice {
    /// Admit an unopened namespace: only the host can mount this device.
    pub fn prepare(device: File, capacity: u32) -> io::Result<Self> {
        Self::admit(device, capacity, 2)
    }

    /// Receive a mount helper's descriptor directly into the host process.
    pub fn receive_device(socket: UnixStream, capacity: u32) -> io::Result<Self> {
        Self::admit(File::from(OwnedFd::from(socket)), capacity, 3)
    }

    /// Anonymous mapping bytes: request, reply and notification envelopes,
    /// plus one metadata page.
    /// The host checks its fixed metadata against Linux's minimum page size.
    pub fn storage_bytes(capacity: u32) -> io::Result<u64> {
        // SAFETY: sysconf has no pointer arguments.
        let page = u64::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })
            .map_err(io::Error::other)?;
        if page == 0 {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let envelope = u64::from(capacity)
            .div_ceil(page)
            .checked_mul(page)
            .ok_or(io::ErrorKind::InvalidInput)?;
        envelope
            .checked_mul(3)
            .and_then(|bytes| bytes.checked_add(page))
            .ok_or_else(|| io::ErrorKind::InvalidInput.into())
    }

    fn admit(device: File, capacity: u32, mode: u32) -> io::Result<Self> {
        if capacity < 4 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let owner = owner_exit()?;
        let control = connect()?;
        let (stream, theirs) = UnixStream::pair()?;
        let mut message = [0; 12];
        message[..4].copy_from_slice(&capacity.to_ne_bytes());
        message[4..8].copy_from_slice(&super::packet_device_receipt::VERSION.to_ne_bytes());
        message[8..12].copy_from_slice(&mode.to_ne_bytes());
        runner::send_message(
            &control,
            &[device.as_raw_fd(), theirs.as_raw_fd(), owner.as_raw_fd()],
            &message,
        )?;
        let mut accepted = [0; 4];
        (&control).read_exact(&mut accepted)?;
        let host = u32::from_ne_bytes(accepted);
        if host == 0 {
            return Err(io::ErrorKind::InvalidData.into());
        }
        // No consumer ever retains /dev/fuse after acknowledged transfer.
        drop(device);
        drop(theirs);
        Ok(Self {
            stream: File::from(OwnedFd::from(stream)),
            retirement: PacketDeviceRetirement {
                control,
                retired: AtomicBool::new(false),
                receipts: Mutex::new(PublicationReceipts::default()),
                receipt_ready: Condvar::new(),
                host,
            },
        })
    }

    /// Install FUSE from the external device owner. The caller never holds
    /// the device while its mounted namespace can accept a request.
    pub fn mount_fuse(
        &self,
        directory: &File,
        mode: u32,
        uid: u32,
        gid: u32,
        writable: bool,
    ) -> io::Result<()> {
        let mut request = [0; 20];
        for (slot, word) in
            request
                .chunks_exact_mut(4)
                .zip([1, mode, uid, gid, u32::from(writable)])
        {
            slot.copy_from_slice(&word.to_ne_bytes());
        }
        runner::send_message(&self.retirement.control, &[directory.as_raw_fd()], &request)?;
        self.wait_mounted()
    }
    /// Join actual device receipt from a helper admitted before its spawn.
    pub fn wait_mounted(&self) -> io::Result<()> {
        let mut status = [0; 4];
        (&self.retirement.control).read_exact(&mut status)?;
        let status = i32::from_ne_bytes(status);
        operation_status(status)
    }
    /// Original mount-free thread identity for real process-death proofs.
    #[cfg(feature = "test-support")]
    pub fn owner_thread_for_test() -> io::Result<u32> {
        let original = owner_exit()?;
        let info = std::fs::read_to_string(format!("/proc/self/fdinfo/{}", original.as_raw_fd()))?;
        info.lines()
            .find_map(|line| line.strip_prefix("Pid:"))
            .and_then(|value| value.trim().parse().ok())
            .ok_or_else(|| io::Error::other("thread pidfd did not expose its original identity"))
    }
    /// Split the endpoint from the original physical-retirement obligation.
    pub fn into_parts(self) -> (File, PacketDeviceRetirement) {
        (self.stream, self.retirement)
    }
}

/// Original host custody. A retirement receipt acknowledges actual device closure.
pub struct PacketDeviceRetirement {
    control: UnixStream,
    retired: AtomicBool,
    receipts: Mutex<PublicationReceipts>,
    receipt_ready: Condvar,
    host: u32,
}

#[derive(Default)]
struct PublicationReceipts {
    reading: bool,
    // Each producer holds its own class's publication claim through receipt.
    // Replies and notifications therefore need one slot each, not a growing
    // map or an additional receiver thread for every device.
    completed: [Option<PacketDevicePublication>; 2],
    failure: Option<(io::ErrorKind, Option<i32>)>,
}

/// An acknowledged physical device-write outcome, distinct from transport loss.
/// A rejected frame published no bytes and does not retire its connection.
#[derive(Debug, PartialEq, Eq)]
#[must_use]
pub enum PacketDevicePublication {
    /// The complete frame reached the physical device.
    Published,
    /// The device rejected the complete frame with this original errno.
    Rejected(i32),
}

impl PacketDeviceRetirement {
    /// The original device host process, shared by all admitted owners.
    pub fn host_id(&self) -> u32 {
        self.host
    }

    /// Starts kernel negotiation in the external host. Success acknowledges
    /// actual INIT publication, without running a kernel session in the caller.
    /// An optional memfd receives pairs of original request/reply lengths
    /// (native-endian u64) followed by their actual bytes, after publication.
    /// The host closes this writer before acknowledging initialization.
    pub fn start_fuse(
        &mut self,
        configuration: crate::fuse_protocol::Configuration,
        initialization_trace: Option<&File>,
    ) -> io::Result<()> {
        let configuration = configuration
            .validate()
            .map_err(io::Error::from_raw_os_error)?;
        let mut request = [0; 20];
        request[..4].copy_from_slice(&2_u32.to_ne_bytes());
        request[4..8].copy_from_slice(&configuration.maximum_write.to_ne_bytes());
        request[8..12].copy_from_slice(&configuration.page_size.to_ne_bytes());
        request[12..].copy_from_slice(&configuration.cache_capabilities.to_ne_bytes());
        let descriptors = initialization_trace.map(|file| [file.as_raw_fd()]);
        runner::send_message(
            &self.control,
            descriptors.as_ref().map_or(&[], |files| &files[..]),
            &request,
        )?;
        let mut response = [0; 12];
        (&self.control).read_exact(&mut response)?;
        let status = i32::from_ne_bytes(response[..4].try_into().map_err(io::Error::other)?);
        let missing = u64::from_ne_bytes(response[4..].try_into().map_err(io::Error::other)?);
        if missing != 0 {
            return Err(io::Error::other(
                crate::fuse_protocol::Error::UnsupportedCapabilities(missing),
            ));
        }
        operation_status(status)
    }

    /// Joins one physical reply publication. Callers retain the original
    /// payload and work until this receipt, not merely until socket acceptance.
    /// `notification` selects the FUSE unique-zero publication class. The
    /// caller serializes publications within each class, but can await one
    /// notification concurrently with a reply. No receipt is socket acceptance.
    pub fn publication(&self, notification: bool) -> io::Result<PacketDevicePublication> {
        let mut state = self.receipts.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if let Some(receipt) = state.completed[usize::from(notification)].take() {
                return Ok(receipt);
            }
            if self.retired.load(Ordering::Acquire) {
                return Err(io::Error::from_raw_os_error(libc::ENODEV));
            }
            if let Some((kind, errno)) = state.failure {
                return Err(errno.map_or_else(|| kind.into(), io::Error::from_raw_os_error));
            }
            if state.reading {
                state = self
                    .receipt_ready
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
                continue;
            }
            state.reading = true;
            drop(state);
            let mut bytes = [0; Receipt::SIZE];
            let receipt = (&self.control).read_exact(&mut bytes).and_then(|()| {
                Receipt::try_from(bytes).map_err(|()| io::ErrorKind::InvalidData.into())
            });
            state = self.receipts.lock().unwrap_or_else(PoisonError::into_inner);
            state.reading = false;
            let completed = match receipt {
                Ok(Receipt::Published(class)) => Some((class, PacketDevicePublication::Published)),
                Ok(Receipt::Rejected(class, errno)) => {
                    Some((class, PacketDevicePublication::Rejected(errno)))
                }
                Ok(Receipt::Retired) => {
                    self.retired.store(true, Ordering::Release);
                    None
                }
                Err(error) => {
                    state.failure = Some((error.kind(), error.raw_os_error()));
                    None
                }
            };
            if let Some((class, receipt)) = completed {
                if state.completed[usize::from(class)]
                    .replace(receipt)
                    .is_some()
                {
                    state.failure = Some((io::ErrorKind::InvalidData, None));
                }
            }
            self.receipt_ready.notify_all();
        }
    }

    /// Closes admission without closing the receipt reader. This OS half-close
    /// interrupts a concurrent publication wait through the host's retirement
    /// receipt, after its physical device and packet storage have closed.
    pub fn request_stop(&self) -> io::Result<()> {
        match self.control.shutdown(std::net::Shutdown::Write) {
            Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
            result => result,
        }
    }

    /// Stop admission and join the receipt published after device closure.
    pub fn retire(&mut self) -> io::Result<()> {
        if self.retired.load(Ordering::Acquire) {
            return Ok(());
        }
        self.request_stop()?;
        loop {
            let mut receipt = [0; Receipt::SIZE];
            self.control.read_exact(&mut receipt)?;
            match Receipt::try_from(receipt).map_err(|()| io::ErrorKind::InvalidData)? {
                Receipt::Retired => break,
                // Stop may interrupt the publisher after the host has already
                // completed its reply. Drain that receipt before retirement.
                Receipt::Published(_) | Receipt::Rejected(_, _) => {}
            }
        }
        self.retired.store(true, Ordering::Release);
        Ok(())
    }
}

impl Drop for PacketDeviceRetirement {
    fn drop(&mut self) {
        if let Err(error) = self.retire() {
            tracing::error!(%error, "packet device retirement failed");
        }
    }
}

fn operation_status(status: i32) -> io::Result<()> {
    match status.checked_neg() {
        Some(0) => Ok(()),
        Some(errno) if errno > 0 => Err(io::Error::from_raw_os_error(errno)),
        _ => Err(io::ErrorKind::InvalidData.into()),
    }
}

fn owner_exit() -> io::Result<&'static OwnedFd> {
    static OWNER: OnceLock<OwnedFd> = OnceLock::new();
    static MAKING: Mutex<()> = Mutex::new(());
    let _making = MAKING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(owner) = OWNER.get() {
        return Ok(owner);
    }
    let (started, ready) = std::sync::mpsc::sync_channel(1);
    let (keep, parked) = std::sync::mpsc::channel::<()>();
    let thread = std::thread::Builder::new()
        .name("acyclic-device-owner".into())
        .spawn(move || {
            // This thread never accesses a volume. SIGKILL therefore ends it even
            // while another thread is waiting uninterruptibly on its own mount.
            // SAFETY: gettid has no arguments; pidfd_open transfers a fresh fd.
            let tid = unsafe { libc::syscall(libc::SYS_gettid) };
            let file = unsafe { libc::syscall(libc::SYS_pidfd_open, tid, libc::PIDFD_THREAD) };
            let result = if file < 0 {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "thread pidfd required for packet-device owner death: {}",
                        io::Error::last_os_error()
                    ),
                ))
            } else {
                // SAFETY: pidfd_open returned this fresh descriptor.
                Ok(unsafe { OwnedFd::from_raw_fd(file as i32) })
            };
            if started.send(result).is_ok() {
                let _ = parked.recv();
            }
        })?;
    let descriptor = ready
        .recv()
        .map_err(|_| io::Error::other("device owner thread exited during admission"))??;
    // Process lifetime owns these two handles, once per owner, never per view.
    static LIFETIME: OnceLock<(std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>)> =
        OnceLock::new();
    let _ = LIFETIME.set((keep, thread));
    Ok(OWNER.get_or_init(|| descriptor))
}

fn address(path: &std::path::Path) -> io::Result<(libc::sockaddr_un, libc::socklen_t)> {
    use std::os::unix::ffi::OsStrExt;
    let name = path.as_os_str().as_bytes();
    // SAFETY: all-zero is valid address storage, including its trailing NUL.
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    address.sun_family = libc::AF_UNIX as _;
    if name.len() + 1 > address.sun_path.len() {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    for (target, byte) in address.sun_path.iter_mut().zip(name) {
        *target = *byte as _;
    }
    let length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + 1 + name.len();
    Ok((address, length as _))
}

fn socket() -> io::Result<OwnedFd> {
    // SOCK_SEQPACKET makes each descriptor-admission envelope atomic.
    // SAFETY: socket transfers a fresh descriptor on success.
    let file = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC, 0) };
    if file < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: socket returned this fresh owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(file) })
}

struct Admission {
    lock: crate::FileLock,
    path: std::path::PathBuf,
    identity: (u64, u64),
    writable: bool,
}

fn admissions() -> io::Result<Vec<Admission>> {
    use std::os::unix::fs::MetadataExt;
    let namespace =
        |name| std::fs::metadata(format!("/proc/self/ns/{name}")).map(|file| file.ino());
    let name = format!(
        "v{}-{}-{}.lock",
        super::packet_device_receipt::VERSION,
        namespace("mnt")?,
        namespace("net")?
    );
    let runtime = std::path::PathBuf::from(format!(
        "/run/user/{}",
        crate::private_file::effective_user()
    ));
    let home = match crate::private_file::user_home_directory() {
        Ok(home) => Some(home),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let mut admissions: Vec<Admission> = Vec::new();
    for root in std::iter::once(runtime).chain(home) {
        let directory = match admission_directory(&root) {
            Ok(directory) => directory,
            Err(error) if unavailable(&error) => continue,
            Err(error) => return Err(error),
        };
        let path = directory.join(&name);
        // Never unlink an authority inode. Even a read-only home can retain
        // an original live host, so acquire its original lock before fallback.
        let (file, writable) = match crate::private_file::open_or_create(&path) {
            Ok(file) => {
                if file.metadata()?.len() == 0 {
                    (&file).write_all(&[0])?;
                }
                (file, true)
            }
            Err(error) if unavailable(&error) => {
                let file = match crate::private_file::open(&path, false) {
                    Ok(file) => file,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(error),
                };
                // A host is never published before its wake byte exists.
                if file.metadata()?.len() == 0 {
                    continue;
                }
                (file, false)
            }
            Err(error) => return Err(error),
        };
        let identity = crate::file_identity(&file)?;
        let admission = Admission {
            lock: crate::FileLock::new(file),
            path: path.with_extension("sock"),
            identity,
            writable,
        };
        if let Some(old) = admissions.iter_mut().find(|old| old.identity == identity) {
            if admission.writable && !old.writable {
                *old = admission;
            }
        } else {
            admissions.push(admission);
        }
    }
    if admissions.is_empty() {
        return Err(io::Error::other(
            "mount host requires a trusted writable OS runtime directory or account home",
        ));
    }
    Ok(admissions)
}

fn unavailable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::NotFound
            | io::ErrorKind::PermissionDenied
            | io::ErrorKind::ReadOnlyFilesystem
    )
}

fn with_admissions<T>(
    admissions: &[Admission],
    operation: &mut dyn FnMut() -> io::Result<T>,
) -> io::Result<T> {
    match admissions.split_first() {
        Some((first, rest)) => first.lock.with_lock(crate::FileLockMode::Exclusive, || {
            with_admissions(rest, operation)
        }),
        None => operation(),
    }
}

fn admission_directory(root: &std::path::Path) -> io::Result<std::path::PathBuf> {
    use std::os::unix::fs::MetadataExt;
    // A checked child in /tmp is insufficient: another user can reserve its
    // name first. The parent itself must already belong to this user and
    // exclude changes by other users before we create any shared authority.
    let metadata = std::fs::symlink_metadata(root)?;
    if !metadata.is_dir()
        || metadata.uid() != crate::private_file::effective_user()
        || metadata.mode() & 0o022 != 0
    {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    let directory = root.join(".acyclic-mount-host");
    match crate::private_file::validate_directory(&directory) {
        Ok(()) => return Ok(directory),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    match crate::private_file::create_directory(&directory) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    crate::private_file::validate_directory(&directory)?;
    Ok(directory)
}

fn admission_events(file: &File) -> io::Result<OwnedFd> {
    // Explicit unlock is followed by a one-byte access event, including on a
    // read-only filesystem. The initialized wake byte makes vfs_read publish
    // IN_ACCESS (it does not publish that event for EOF). Close
    // also covers process death. Neither event is a timed readiness recheck.
    // SAFETY: inotify_init1 has no pointer arguments and returns a fresh fd.
    let descriptor = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: inotify_init1 returned this fresh owned descriptor.
    let descriptor = unsafe { OwnedFd::from_raw_fd(descriptor) };
    let path = std::ffi::CString::new(format!("/proc/self/fd/{}", file.as_raw_fd()))?;
    // SAFETY: the path is NUL terminated and descriptor remains owned here.
    if unsafe {
        libc::inotify_add_watch(
            descriptor.as_raw_fd(),
            path.as_ptr(),
            libc::IN_CLOSE | libc::IN_ACCESS,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(descriptor)
}

fn connect() -> io::Result<UnixStream> {
    let admissions = admissions()?;
    #[cfg(test)]
    if let Some(first) = admissions.first() {
        BEFORE_ADMISSION.with_borrow_mut(|probe| {
            if let Some(probe) = probe.take() {
                probe(&first.lock);
            }
        });
    }
    #[cfg(test)]
    if WITHOUT_ADMISSION_LOCK.get() {
        return connect_locked(&admissions);
    }
    // Lock every usable canonical root in fixed runtime/home order. If a
    // login directory appears while a home-backed host is live, its home lock
    // still excludes this client and discovery joins the original endpoint.
    let connected = with_admissions(&admissions, &mut || connect_locked(&admissions));
    // Signal after explicit unlock, not just last-close: an unrelated fork
    // can temporarily retain a duplicate open description across its exec.
    // Read the initialized byte after unlock: IN_ACCESS is a kernel wake even
    // on a read-only filesystem, and needs no mutation or durability fsync.
    use std::os::unix::fs::FileExt;
    let mut notified = Ok(());
    for admission in &admissions {
        if let Err(error) = admission.lock.read_exact_at(&mut [0], 0) {
            notified = Err(error);
        }
    }
    match connected {
        Ok(control) => notified.map(|()| control),
        Err(error) => Err(error),
    }
}

fn connect_locked(admissions: &[Admission]) -> io::Result<UnixStream> {
    #[cfg(test)]
    BEFORE_CONNECT.with_borrow_mut(|probe| {
        if let Some(probe) = probe.take() {
            probe();
        }
    });
    for admission in admissions {
        if let Some(control) = connect_existing(&admission.path)? {
            return authenticate(control);
        }
    }
    let admission = admissions
        .iter()
        .find(|admission| admission.writable)
        .ok_or(io::ErrorKind::ReadOnlyFilesystem)?;
    let path = &admission.path;
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            use std::os::unix::fs::FileTypeExt;
            if !metadata.file_type().is_socket() {
                return Err(io::ErrorKind::PermissionDenied.into());
            }
            // Every candidate was probed under its original lock. Only the
            // selected writable endpoint needs its closed inode removed.
            std::fs::remove_file(path)?;
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    // Reuse the held-directory IPC adapter, so long account homes need no
    // public abstract name that another user could reserve before this owner.
    let (_directory, socket_path) = crate::ipc::socket_path(path)?;
    let (address, length) = address(&socket_path)?;
    let listener = socket()?;
    // SAFETY: the held directory and address remain live throughout bind.
    if unsafe { libc::bind(listener.as_raw_fd(), (&raw const address).cast(), length) } < 0 {
        return Err(io::Error::last_os_error());
    }
    #[cfg(test)]
    BEFORE_LISTEN.with_borrow_mut(|probe| {
        if let Some(probe) = probe.take() {
            probe();
        }
    });
    // The backlog is kernel backpressure, not a limit on admitted owners.
    // SAFETY: listener is the fresh bound socket.
    if unsafe { libc::listen(listener.as_raw_fd(), libc::SOMAXCONN) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // This must be a separate open description, not a duplicate of the
    // contender's held flock. The event watch precedes its first release.
    let authority = crate::private_file::open(&path.with_extension("lock"), true)?;
    let events = admission_events(&authority)?;
    runner::start_packet_device_host(listener, authority, events)?;
    let stream = socket()?;
    // Reconnect only after this call has installed a listening endpoint;
    // this is a state transition, never a readiness retry.
    // SAFETY: live socket and initialized pathname address.
    if unsafe { libc::connect(stream.as_raw_fd(), (&raw const address).cast(), length) } < 0 {
        return Err(io::Error::last_os_error());
    }
    authenticate(UnixStream::from(stream))
}

fn connect_existing(path: &std::path::Path) -> io::Result<Option<UnixStream>> {
    let (_directory, short) = crate::ipc::socket_path(path)?;
    let (address, length) = address(&short)?;
    let stream = socket()?;
    // SAFETY: the held directory and initialized address outlive the call.
    if unsafe { libc::connect(stream.as_raw_fd(), (&raw const address).cast(), length) } == 0 {
        return Ok(Some(UnixStream::from(stream)));
    }
    let error = io::Error::last_os_error();
    #[cfg(test)]
    if WITHOUT_ADMISSION_LOCK.get() {
        return Err(error);
    }
    match error.raw_os_error() {
        Some(libc::ENOENT) => Ok(None),
        Some(libc::ECONNREFUSED) => {
            use std::os::unix::fs::FileTypeExt;
            // Every candidate authority is held: this is an original host's
            // closed socket, never a bind-before-listen contender.
            if !std::fs::symlink_metadata(path)?.file_type().is_socket() {
                return Err(io::ErrorKind::PermissionDenied.into());
            }
            Ok(None)
        }
        _ => Err(error),
    }
}

fn authenticate(control: UnixStream) -> io::Result<UnixStream> {
    let mut peer: libc::ucred = unsafe { std::mem::zeroed() };
    let mut size = std::mem::size_of_val(&peer) as libc::socklen_t;
    // SAFETY: SO_PEERCRED fills the initialized peer structure.
    if unsafe {
        libc::getsockopt(
            control.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&raw mut peer).cast(),
            &raw mut size,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    // A listener created by this client retains its creator's credentials;
    // ownership is authenticated by UID, not the listener's recorded PID.
    if peer.uid != unsafe { libc::geteuid() } {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    Ok(control)
}
