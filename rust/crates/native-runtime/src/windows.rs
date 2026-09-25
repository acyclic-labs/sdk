//! Windows file I/O is owned by one completion driver, not by the service executor.

#![allow(unsafe_code)]

use crate::{OwnedRead, OwnedWrite};
use bytes::Bytes;
use compio_buf::IntoInner as _;
use compio_driver::{
    BorrowedFd, Key, OwnedFd, Proactor, PushEntry, SharedFd,
    op::{ReadAt, WriteAt},
};
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::mem::zeroed;
use std::os::windows::ffi::OsStrExt as _;
use std::os::windows::io::{AsHandle, AsRawHandle, FromRawHandle, RawHandle};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock, Weak, mpsc};
use std::task::{Wake, Waker};
use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Foundation::{HANDLE_FLAG_INHERIT, SetHandleInformation};
use windows_sys::Win32::Security::SECURITY_ATTRIBUTES;
use windows_sys::Win32::Storage::FileSystem::{
    FILE_FLAG_OVERLAPPED, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_ID_INFO, FILE_SHARE_DELETE,
    FILE_SHARE_READ, FILE_SHARE_WRITE, FileIdInfo, GetFileInformationByHandleEx, ReOpenFile,
    SetFileAttributesW,
};
use windows_sys::Win32::System::Console::{GetStdHandle, STD_OUTPUT_HANDLE, SetStdHandle};
use windows_sys::Win32::System::Pipes::CreatePipe;
use windows_sys::Win32::System::Threading::{
    CREATE_NEW_PROCESS_GROUP, CreateProcessW, DETACHED_PROCESS, DeleteProcThreadAttributeList,
    EXTENDED_STARTUPINFO_PRESENT, InitializeProcThreadAttributeList, LPPROC_THREAD_ATTRIBUTE_LIST,
    PROC_THREAD_ATTRIBUTE_HANDLE_LIST, PROCESS_INFORMATION, STARTF_USESTDHANDLES, STARTUPINFOEXW,
    UpdateProcThreadAttribute,
};

#[derive(Clone, Debug)]
struct ArcFile(Arc<File>);

impl compio_driver::AsFd for ArcFile {
    fn as_fd(&self) -> BorrowedFd<'_> {
        BorrowedFd::from(self.0.as_handle())
    }
}

#[derive(Clone, Debug)]
enum DriverFile {
    Reopened(SharedFd<OwnedFd>),
    Original(SharedFd<ArcFile>),
}

impl compio_driver::AsFd for DriverFile {
    fn as_fd(&self) -> BorrowedFd<'_> {
        match self {
            Self::Reopened(file) => file.as_fd(),
            Self::Original(file) => file.as_fd(),
        }
    }
}
type ReadOp = ReadAt<Vec<u8>, DriverFile>;
type WriteOp = WriteAt<Bytes, DriverFile>;
type ReadFinish = Box<dyn FnOnce(io::Result<Vec<Bytes>>) + Send + 'static>;
type WriteFinish = Box<dyn FnOnce(io::Result<()>) + Send + 'static>;

const DRIVER_QUEUE: usize = 1024;
const INTAKE_QUANTUM: usize = 64;
const COMPLETION_QUANTUM: usize = 256;
const MAX_READ_WINDOW: usize = 512 * 1024;

struct ReadWindow {
    offset: u64,
    length: usize,
    parts: Vec<(usize, usize)>,
}

fn read_windows(reads: &[OwnedRead]) -> io::Result<Vec<ReadWindow>> {
    let mut windows: Vec<ReadWindow> = Vec::new();
    windows.try_reserve_exact(reads.len())?;
    for (index, read) in reads.iter().enumerate() {
        if read.offset == u64::MAX
            || read.length > u32::MAX as usize
            || read.offset.checked_add(read.length as u64).is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid overlapped read range",
            ));
        }
        if read.length == 0 {
            continue;
        }
        if let Some(window) = windows.last_mut()
            && window.offset.checked_add(window.length as u64) == Some(read.offset)
            && window
                .length
                .checked_add(read.length)
                .is_some_and(|length| length <= MAX_READ_WINDOW)
        {
            window.parts.push((index, read.length));
            window.length += read.length;
            continue;
        }
        windows.push(ReadWindow {
            offset: read.offset,
            length: read.length,
            parts: vec![(index, read.length)],
        });
    }
    Ok(windows)
}

fn fail_stop(message: &str) -> ! {
    eprintln!("Acyclic Windows completion invariant failed: {message}");
    std::process::abort()
}

fn poll_proactor(proactor: &mut Proactor, timeout: Option<std::time::Duration>) -> io::Result<()> {
    match proactor.poll(timeout) {
        Ok(()) => Ok(()),
        Err(error)
            if timeout == Some(std::time::Duration::ZERO)
                && error.kind() == io::ErrorKind::TimedOut =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(super) struct FileIdentity {
    volume: u64,
    id: [u8; 16],
}

pub(super) fn file_identity(file: &File) -> io::Result<FileIdentity> {
    // SAFETY: FILE_ID_INFO is an initialized POD output buffer and the file
    // handle remains live for the synchronous metadata query.
    let mut info: FILE_ID_INFO = unsafe { zeroed() };
    if unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle() as HANDLE,
            FileIdInfo,
            (&raw mut info).cast(),
            u32::try_from(std::mem::size_of::<FILE_ID_INFO>())
                .map_err(|_| io::Error::other("FILE_ID_INFO size overflow"))?,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    if info.FileId.Identifier == [0; 16] {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "filesystem did not provide a stable file identity",
        ));
    }
    Ok(FileIdentity {
        volume: info.VolumeSerialNumber,
        id: info.FileId.Identifier,
    })
}

/// Starts `executable __service --signal-ready` detached, with the write end
/// of a fresh pipe as its standard output and the only handle it inherits,
/// and returns the read end.
pub(super) fn spawn_service_process(executable: &Path) -> io::Result<File> {
    let executable_argument = executable.as_os_str().encode_wide().collect::<Vec<_>>();
    if executable_argument.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "service executable path contains NUL",
        ));
    }
    if executable_argument.contains(&u16::from(b'"')) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "service executable path contains a quote",
        ));
    }
    let mut executable_wide = executable_argument.clone();
    executable_wide.push(0);
    let mut command_line = Vec::with_capacity(executable_argument.len() + 32);
    command_line.push(u16::from(b'"'));
    command_line.extend(executable_argument);
    command_line.extend(format!("\" __service {}\0", crate::SERVICE_READY_ARGUMENT).encode_utf16());

    let (reader, writer) = readiness_pipe()?;
    let writer_handle: HANDLE = writer.as_raw_handle();
    let mut attributes = HandleListAttribute::new(&writer_handle)?;
    // SAFETY: zero is the documented initial state for these Win32 outputs.
    let mut startup: STARTUPINFOEXW = unsafe { zeroed() };
    startup.StartupInfo.cb = u32::try_from(std::mem::size_of::<STARTUPINFOEXW>())
        .map_err(|_| io::Error::other("STARTUPINFOEXW size does not fit u32"))?;
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdOutput = writer_handle;
    startup.lpAttributeList = attributes.list();
    // SAFETY: CreateProcessW initializes every returned handle on success.
    let mut process: PROCESS_INFORMATION = unsafe { zeroed() };
    // SAFETY: both buffers are live and NUL-terminated. Inheritance is limited
    // to the attribute list's single handle, which outlives the call.
    let created = unsafe {
        CreateProcessW(
            executable_wide.as_ptr(),
            command_line.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            1,
            CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS | EXTENDED_STARTUPINFO_PRESENT,
            std::ptr::null(),
            std::ptr::null(),
            &raw const startup.StartupInfo,
            &mut process,
        )
    };
    if created == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful creation transfers both owned handles to this process.
    unsafe {
        CloseHandle(process.hThread);
        CloseHandle(process.hProcess);
    }
    // Only the service may hold the write end, so the reader sees the pipe
    // close when the service exits.
    drop(attributes);
    drop(writer);
    Ok(reader)
}

/// An anonymous pipe whose write end alone is inheritable.
fn readiness_pipe() -> io::Result<(File, File)> {
    let attributes = SECURITY_ATTRIBUTES {
        nLength: u32::try_from(std::mem::size_of::<SECURITY_ATTRIBUTES>())
            .map_err(|_| io::Error::other("SECURITY_ATTRIBUTES size does not fit u32"))?,
        lpSecurityDescriptor: std::ptr::null_mut(),
        bInheritHandle: 1,
    };
    let mut reader: HANDLE = std::ptr::null_mut();
    let mut writer: HANDLE = std::ptr::null_mut();
    // SAFETY: both outputs are writable and the attributes are live.
    if unsafe { CreatePipe(&mut reader, &mut writer, &raw const attributes, 0) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreatePipe returned two owned handles.
    let (reader, writer) =
        unsafe { (File::from_raw_handle(reader), File::from_raw_handle(writer)) };
    // SAFETY: the reader handle is live.
    if unsafe { SetHandleInformation(reader.as_raw_handle(), HANDLE_FLAG_INHERIT, 0) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((reader, writer))
}

/// A process attribute list naming the only handle a child inherits.
struct HandleListAttribute {
    buffer: Vec<u8>,
}

impl HandleListAttribute {
    fn new(handle: &HANDLE) -> io::Result<Self> {
        let mut size = 0;
        // SAFETY: a null list with a size output queries the required size;
        // this documented probe fails with ERROR_INSUFFICIENT_BUFFER.
        unsafe { InitializeProcThreadAttributeList(std::ptr::null_mut(), 1, 0, &mut size) };
        let mut buffer = vec![0; size];
        // SAFETY: the buffer holds exactly the size the probe requested.
        if unsafe { InitializeProcThreadAttributeList(buffer.as_mut_ptr().cast(), 1, 0, &mut size) }
            == 0
        {
            return Err(io::Error::last_os_error());
        }
        let mut attribute = Self { buffer };
        // SAFETY: the list is initialized, and `handle` outlives it because
        // the caller keeps both alive across process creation.
        if unsafe {
            UpdateProcThreadAttribute(
                attribute.list(),
                0,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                std::ptr::from_ref(handle).cast(),
                std::mem::size_of::<HANDLE>(),
                std::ptr::null_mut(),
                std::ptr::null(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(attribute)
    }

    fn list(&mut self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
        self.buffer.as_mut_ptr().cast()
    }
}

impl Drop for HandleListAttribute {
    fn drop(&mut self) {
        // SAFETY: a constructed attribute always holds an initialized list.
        unsafe { DeleteProcThreadAttributeList(self.list()) };
    }
}

/// Takes the process's standard output handle, leaving no standard output,
/// which the standard library then treats as a sink.
pub(super) fn take_standard_output() -> io::Result<Option<File>> {
    // SAFETY: querying a standard handle has no preconditions.
    let handle = unsafe { GetStdHandle(STD_OUTPUT_HANDLE) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Ok(None);
    }
    // SAFETY: clearing a standard handle has no preconditions; this process
    // now solely owns the handle it held.
    if unsafe { SetStdHandle(STD_OUTPUT_HANDLE, std::ptr::null_mut()) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the handle is live and no longer reachable as standard output.
    Ok(Some(unsafe { File::from_raw_handle(handle) }))
}

pub(super) fn set_file_attributes(path: &Path, attributes: u32) -> io::Result<()> {
    let mut encoded: Vec<_> = path.as_os_str().encode_wide().collect();
    if encoded.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "attribute path contains NUL",
        ));
    }
    encoded.push(0);
    // SAFETY: `encoded` is a live NUL-terminated UTF-16 path for this call.
    if unsafe { SetFileAttributesW(encoded.as_ptr(), attributes) } == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

enum Job {
    Read(Arc<File>, bool, Vec<OwnedRead>, ReadFinish),
    Write(Arc<File>, bool, Vec<OwnedWrite>, WriteFinish),
}

#[derive(Clone)]
struct Driver {
    sender: mpsc::SyncSender<Job>,
    wake: Waker,
}

static DRIVER: OnceLock<Mutex<Option<Driver>>> = OnceLock::new();

fn driver() -> io::Result<Driver> {
    let slot = DRIVER.get_or_init(|| Mutex::new(None));
    let mut slot = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(driver) = slot.as_ref() {
        return Ok(driver.clone());
    }
    // Producers are dedicated native workers. Backpressure can block them,
    // but not the service executor, and cannot fail an admitted operation.
    let (sender, receiver) = mpsc::sync_channel(DRIVER_QUEUE);
    let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("acyclic-compio-windows".to_owned())
        .spawn(move || {
            let proactor = match Proactor::new() {
                Ok(proactor) => proactor,
                Err(error) => {
                    let _ = ready_sender.send(Err(error));
                    return;
                }
            };
            if ready_sender.send(Ok(proactor.waker())).is_err() {
                return;
            }
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                DriverState::new(proactor, receiver).run()
            }));
            // A driver error or panic may strand kernel-owned buffers. Never
            // unwind its owner thread or release an admitted caller's fence.
            eprintln!("Acyclic Windows completion owner stopped: {result:?}");
            std::process::abort();
        })
        .map_err(|error| io::Error::other(format!("cannot start completion owner: {error}")))?;
    let wake = ready_receiver
        .recv()
        .map_err(|_| io::Error::other("completion owner exited during startup"))??;
    let driver = Driver { sender, wake };
    *slot = Some(driver.clone());
    Ok(driver)
}

pub(super) fn submit_read(
    file: Arc<File>,
    overlapped: bool,
    reads: Vec<OwnedRead>,
    finish: ReadFinish,
) -> io::Result<()> {
    let driver = driver()?;
    driver
        .sender
        .send(Job::Read(file, overlapped, reads, finish))
        .map_err(|_| io::Error::other("completion owner disconnected"))?;
    driver.wake.wake_by_ref();
    Ok(())
}

pub(super) fn submit_write(
    file: Arc<File>,
    overlapped: bool,
    writes: Vec<OwnedWrite>,
    finish: WriteFinish,
) -> io::Result<()> {
    let driver = driver()?;
    driver
        .sender
        .send(Job::Write(file, overlapped, writes, finish))
        .map_err(|_| io::Error::other("completion owner disconnected"))?;
    driver.wake.wake_by_ref();
    Ok(())
}

enum Pending {
    Read {
        batch: u64,
        key: Key<ReadOp>,
        requested: usize,
        parts: Vec<(usize, usize)>,
    },
    Write {
        batch: u64,
        index: usize,
        key: Key<WriteOp>,
        offset: u64,
        file: DriverFile,
    },
}

enum BatchResult {
    Reads(Vec<Option<Bytes>>, ReadFinish),
    Writes(WriteFinish),
}

struct Batch {
    remaining: usize,
    first_error: Option<io::Error>,
    result: BatchResult,
}

struct CompletionWake {
    id: u64,
    ready: mpsc::Sender<u64>,
}

impl Wake for CompletionWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.ready.send(self.id);
    }
}

struct DriverState {
    proactor: Proactor,
    incoming: mpsc::Receiver<Job>,
    ready_sender: mpsc::Sender<u64>,
    ready: mpsc::Receiver<u64>,
    pending: HashMap<u64, Pending>,
    batches: HashMap<u64, Batch>,
    attached_original: HashMap<usize, Weak<File>>,
    attached_insertions: usize,
    next_id: u64,
}

impl DriverState {
    fn new(proactor: Proactor, incoming: mpsc::Receiver<Job>) -> Self {
        let (ready_sender, ready) = mpsc::channel();
        Self {
            proactor,
            incoming,
            ready_sender,
            ready,
            pending: HashMap::new(),
            batches: HashMap::new(),
            attached_original: HashMap::new(),
            attached_insertions: 0,
            next_id: 1,
        }
    }

    fn id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .unwrap_or_else(|| fail_stop("key space exhausted"));
        id
    }

    fn run(mut self) -> io::Result<()> {
        loop {
            let mut completed = 0;
            while completed < COMPLETION_QUANTUM {
                let Ok(id) = self.ready.try_recv() else {
                    break;
                };
                self.complete(id);
                completed += 1;
            }
            let mut admitted = 0;
            while admitted < INTAKE_QUANTUM {
                let Ok(job) = self.incoming.try_recv() else {
                    break;
                };
                self.submit(job);
                admitted += 1;
            }
            // A continuously refilled mailbox must not starve terminal keys;
            // do not block when either bounded drain may have more work.
            let timeout = (completed == COMPLETION_QUANTUM || admitted == INTAKE_QUANTUM)
                .then_some(std::time::Duration::ZERO);
            poll_proactor(&mut self.proactor, timeout)?;
        }
    }

    fn submit(&mut self, job: Job) {
        match job {
            Job::Read(file, overlapped, reads, finish) => {
                self.submit_read_job(&file, overlapped, &reads, finish);
            }
            Job::Write(file, overlapped, writes, finish) => {
                self.submit_write_job(&file, overlapped, writes, finish);
            }
        }
    }

    fn submit_read_job(
        &mut self,
        file: &Arc<File>,
        overlapped: bool,
        reads: &[OwnedRead],
        finish: ReadFinish,
    ) {
        if reads.is_empty() {
            finish(Ok(Vec::new()));
            return;
        }
        let windows = match read_windows(reads) {
            Ok(windows) => windows,
            Err(error) => {
                finish(Err(error));
                return;
            }
        };
        if windows.is_empty() {
            finish(Ok(vec![Bytes::new(); reads.len()]));
            return;
        }
        let mut buffers = Vec::with_capacity(windows.len());
        for window in &windows {
            let mut buffer = Vec::new();
            if let Err(error) = buffer.try_reserve_exact(window.length) {
                finish(Err(io::Error::other(error)));
                return;
            }
            buffers.push(buffer);
        }
        let fd = match self.attach(file, overlapped, FILE_GENERIC_READ) {
            Ok(fd) => fd,
            Err(error) => {
                finish(Err(error));
                return;
            }
        };
        let batch = self.id();
        self.batches.insert(
            batch,
            Batch {
                remaining: reads.len(),
                first_error: None,
                result: BatchResult::Reads(vec![None; reads.len()], finish),
            },
        );
        for (window, buffer) in windows.into_iter().zip(buffers) {
            let requested = window.length;
            let op = ReadAt::new(fd.clone(), window.offset, buffer);
            match self.proactor.push(op) {
                PushEntry::Ready(result) => self.read_done(batch, window.parts, requested, result),
                PushEntry::Pending(mut key) => {
                    let id = self.id();
                    let wake = Waker::from(Arc::new(CompletionWake {
                        id,
                        ready: self.ready_sender.clone(),
                    }));
                    self.proactor.update_waker(&mut key, &wake);
                    self.pending.insert(
                        id,
                        Pending::Read {
                            batch,
                            key,
                            requested,
                            parts: window.parts,
                        },
                    );
                }
            }
        }
        for (index, _) in reads
            .iter()
            .enumerate()
            .filter(|(_, read)| read.length == 0)
        {
            self.done(batch, index, Ok(Some(Bytes::new())));
        }
    }

    fn submit_write_job(
        &mut self,
        file: &Arc<File>,
        overlapped: bool,
        writes: Vec<OwnedWrite>,
        finish: WriteFinish,
    ) {
        if writes.is_empty() {
            finish(Ok(()));
            return;
        }
        for write in &writes {
            if write.offset == u64::MAX
                || write.bytes.len() > u32::MAX as usize
                || write.offset.checked_add(write.bytes.len() as u64).is_none()
            {
                finish(Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid overlapped write range",
                )));
                return;
            }
        }
        let fd = match self.attach(file, overlapped, FILE_GENERIC_WRITE) {
            Ok(fd) => fd,
            Err(error) => {
                finish(Err(error));
                return;
            }
        };
        let batch = self.id();
        self.batches.insert(
            batch,
            Batch {
                remaining: writes.len(),
                first_error: None,
                result: BatchResult::Writes(finish),
            },
        );
        for (index, write) in writes.into_iter().enumerate() {
            self.write_piece(batch, index, fd.clone(), write.offset, write.bytes);
        }
    }

    fn attach(
        &mut self,
        file: &Arc<File>,
        overlapped: bool,
        access: u32,
    ) -> io::Result<DriverFile> {
        if overlapped {
            let identity = Arc::as_ptr(file) as usize;
            match self
                .attached_original
                .get(&identity)
                .and_then(Weak::upgrade)
            {
                Some(attached) if Arc::ptr_eq(&attached, file) => {}
                Some(_) => fail_stop("overlapped file identity collision"),
                None => {
                    // This mode is available only through the caller's unsafe
                    // constructor, which proves the original handle was opened
                    // with FILE_FLAG_OVERLAPPED and has not been attached yet.
                    // Compio may associate a handle before an attach error is
                    // returned, so reusing that original handle after failure
                    // would be indeterminate; fail stop instead.
                    self.proactor
                        .attach(file.as_raw_handle().cast())
                        .unwrap_or_else(|error| {
                            eprintln!("Acyclic overlapped-handle attach failed: {error}");
                            std::process::abort()
                        });
                    self.attached_original
                        .insert(identity, Arc::downgrade(file));
                    self.attached_insertions = self.attached_insertions.wrapping_add(1);
                    if self.attached_insertions.is_multiple_of(4096) {
                        self.attached_original
                            .retain(|_, weak| weak.strong_count() > 0);
                    }
                }
            }
            return Ok(DriverFile::Original(SharedFd::new(ArcFile(Arc::clone(
                file,
            )))));
        }
        let reopened = reopen_overlapped(file, access)?;
        self.proactor.attach(reopened.as_raw_handle().cast())?;
        Ok(DriverFile::Reopened(SharedFd::new(OwnedFd::from(reopened))))
    }

    fn write_piece(
        &mut self,
        batch: u64,
        index: usize,
        file: DriverFile,
        mut offset: u64,
        mut bytes: Bytes,
    ) {
        loop {
            let op = WriteAt::new(file.clone(), offset, bytes);
            match self.proactor.push(op) {
                PushEntry::Pending(mut key) => {
                    let id = self.id();
                    let wake = Waker::from(Arc::new(CompletionWake {
                        id,
                        ready: self.ready_sender.clone(),
                    }));
                    self.proactor.update_waker(&mut key, &wake);
                    self.pending.insert(
                        id,
                        Pending::Write {
                            batch,
                            index,
                            key,
                            offset,
                            file,
                        },
                    );
                    return;
                }
                PushEntry::Ready(result) => {
                    let (count, op) = result.into_parts();
                    bytes = op.into_inner();
                    match count {
                        Ok(count) if count == bytes.len() => {
                            self.done(batch, index, Ok(None));
                            return;
                        }
                        Ok(0) => {
                            self.done(batch, index, Err(io::Error::from(io::ErrorKind::WriteZero)));
                            return;
                        }
                        Ok(count) if count < bytes.len() => {
                            offset += count as u64;
                            bytes = bytes.slice(count..);
                        }
                        Ok(_) => {
                            self.done(
                                batch,
                                index,
                                Err(io::Error::other("write completion exceeded buffer")),
                            );
                            return;
                        }
                        Err(error) => {
                            self.done(batch, index, Err(error));
                            return;
                        }
                    }
                }
            }
        }
    }

    fn complete(&mut self, id: u64) {
        let Some(pending) = self.pending.remove(&id) else {
            return;
        };
        match pending {
            Pending::Read {
                batch,
                key,
                requested,
                parts,
            } => match self.proactor.pop(key) {
                PushEntry::Pending(key) => {
                    self.pending.insert(
                        id,
                        Pending::Read {
                            batch,
                            key,
                            requested,
                            parts,
                        },
                    );
                }
                PushEntry::Ready(result) => self.read_done(batch, parts, requested, result),
            },
            Pending::Write {
                batch,
                index,
                key,
                offset,
                file,
            } => match self.proactor.pop(key) {
                PushEntry::Pending(key) => {
                    self.pending.insert(
                        id,
                        Pending::Write {
                            batch,
                            index,
                            key,
                            offset,
                            file,
                        },
                    );
                }
                PushEntry::Ready(result) => {
                    let (count, op) = result.into_parts();
                    let bytes = op.into_inner();
                    match count {
                        Ok(count) if count == bytes.len() => self.done(batch, index, Ok(None)),
                        Ok(0) => {
                            self.done(batch, index, Err(io::Error::from(io::ErrorKind::WriteZero)));
                        }
                        Ok(count) if count < bytes.len() => self.write_piece(
                            batch,
                            index,
                            file,
                            offset + count as u64,
                            bytes.slice(count..),
                        ),
                        Ok(_) => self.done(
                            batch,
                            index,
                            Err(io::Error::other("write completion exceeded buffer")),
                        ),
                        Err(error) => self.done(batch, index, Err(error)),
                    }
                }
            },
        }
    }

    fn read_done(
        &mut self,
        batch: u64,
        parts: Vec<(usize, usize)>,
        requested: usize,
        result: compio_buf::BufResult<usize, ReadOp>,
    ) {
        let (count, op) = result.into_parts();
        let mut buffer = op.into_inner();
        let result = count.and_then(|count| {
            if count > requested || count > buffer.capacity() {
                return Err(io::Error::other("read completion exceeded buffer"));
            }
            // SAFETY: the completed kernel read initialized exactly `count`
            // bytes in the owned spare capacity, which remains live here.
            unsafe {
                buffer.set_len(count);
            }
            Ok(Bytes::from(buffer))
        });
        match result {
            Ok(bytes) => {
                let mut start = 0;
                for (index, length) in parts {
                    let end = start + length;
                    let value = if start < bytes.len() {
                        bytes.slice(start..end.min(bytes.len()))
                    } else {
                        Bytes::new()
                    };
                    self.done(batch, index, Ok(Some(value)));
                    start = end;
                }
            }
            Err(error) => {
                let mut parts = parts.into_iter();
                if let Some((index, _)) = parts.next() {
                    self.done(batch, index, Err(error));
                }
                for (index, _) in parts {
                    self.done(batch, index, Ok(Some(Bytes::new())));
                }
            }
        }
    }

    fn done(&mut self, batch: u64, index: usize, result: io::Result<Option<Bytes>>) {
        let Some(entry) = self.batches.get_mut(&batch) else {
            fail_stop("completion without a batch");
        };
        match result {
            Ok(Some(bytes)) => match &mut entry.result {
                BatchResult::Reads(values, _) => {
                    let Some(slot) = values.get_mut(index) else {
                        fail_stop("read completion index out of range");
                    };
                    *slot = Some(bytes);
                }
                BatchResult::Writes(_) => unreachable!("read result in write batch"),
            },
            Err(error) if entry.first_error.is_none() => entry.first_error = Some(error),
            Ok(None) | Err(_) => {}
        }
        entry.remaining -= 1;
        if entry.remaining != 0 {
            return;
        }
        let Some(entry) = self.batches.remove(&batch) else {
            fail_stop("completed batch disappeared");
        };
        match entry.result {
            BatchResult::Reads(values, finish) => {
                if let Some(error) = entry.first_error {
                    finish(Err(error));
                } else {
                    finish(
                        values
                            .into_iter()
                            .collect::<Option<Vec<_>>>()
                            .ok_or_else(|| io::Error::other("completed read omitted a value")),
                    );
                }
            }
            BatchResult::Writes(finish) => finish(entry.first_error.map_or(Ok(()), Err)),
        }
    }
}

fn reopen_overlapped(file: &File, access: u32) -> io::Result<File> {
    // SAFETY: ReOpenFile opens the same live file object without path traversal.
    let handle = unsafe {
        ReOpenFile(
            file.as_raw_handle() as HANDLE,
            access,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            FILE_FLAG_OVERLAPPED,
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful ReOpenFile returns one owned handle.
    Ok(unsafe { File::from_raw_handle(handle as RawHandle) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::{Seek as _, SeekFrom, Write as _};
    use std::os::windows::fs::OpenOptionsExt as _;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use windows_sys::Win32::Foundation::{ERROR_PIPE_CONNECTED, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::Storage::FileSystem::PIPE_ACCESS_INBOUND;
    use windows_sys::Win32::System::Pipes::{ConnectNamedPipe, CreateNamedPipeW, PIPE_TYPE_BYTE};

    #[test]
    fn empty_nonblocking_poll_is_not_a_driver_failure() -> io::Result<()> {
        let mut proactor = Proactor::new()?;
        poll_proactor(&mut proactor, Some(Duration::ZERO))
    }

    #[test]
    fn compio_keeps_pending_kernel_buffer_until_terminal_pop() -> io::Result<()> {
        static NEXT_PIPE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let name = format!(
            r"\\.\pipe\acyclic-compio-{}-{}",
            std::process::id(),
            NEXT_PIPE.fetch_add(1, Ordering::Relaxed)
        );
        let encoded = name
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        // SAFETY: the name is live and NUL terminated; the returned handle is
        // transferred exactly once to File below.
        let handle = unsafe {
            CreateNamedPipeW(
                encoded.as_ptr(),
                PIPE_ACCESS_INBOUND | FILE_FLAG_OVERLAPPED,
                PIPE_TYPE_BYTE,
                1,
                4096,
                4096,
                0,
                std::ptr::null(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful CreateNamedPipeW transferred one owned handle.
        let server = unsafe { File::from_raw_handle(handle as RawHandle) };
        let mut client = OpenOptions::new().write(true).open(&name)?;
        // SAFETY: a client that connected first produces ERROR_PIPE_CONNECTED.
        if unsafe { ConnectNamedPipe(handle, std::ptr::null_mut()) } == 0
            && io::Error::last_os_error().raw_os_error() != Some(ERROR_PIPE_CONNECTED.cast_signed())
        {
            return Err(io::Error::last_os_error());
        }
        let mut proactor = Proactor::new()?;
        proactor.attach(handle)?;
        let fd = SharedFd::new(OwnedFd::from(server));
        let mut buffer = Vec::new();
        buffer.try_reserve_exact(1).map_err(io::Error::other)?;
        let key = match proactor.push(ReadAt::new(fd, 0, buffer)) {
            PushEntry::Pending(key) => key,
            PushEntry::Ready(_) => return Err(io::Error::other("empty pipe read did not pend")),
        };
        let write_result = client.write_all(b"x");
        if write_result.is_err() {
            drop(client);
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut key = key;
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                fail_stop("test pending pipe read did not reach terminal completion");
            }
            proactor
                .poll(Some(remaining))
                .unwrap_or_else(|_| fail_stop("test proactor failed with pending kernel I/O"));
            match proactor.pop(key) {
                PushEntry::Pending(still_pending) => key = still_pending,
                PushEntry::Ready(result) => {
                    write_result?;
                    let (count, op) = result.into_parts();
                    assert_eq!(count?, 1);
                    let mut buffer = op.into_inner();
                    // SAFETY: the terminal read initialized the first byte.
                    unsafe {
                        buffer.set_len(1);
                    }
                    assert_eq!(buffer, b"x");
                    return Ok(());
                }
            }
        }
    }

    #[test]
    fn positional_batches_complete_without_changing_source_cursor() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(directory.path().join("positional"))?;
        file.write_all(b"abcdefghijklmnop")?;
        file.seek(SeekFrom::Start(13))?;
        let file = Arc::new(file);
        let (sender, receiver) = mpsc::sync_channel(1);
        submit_write(
            Arc::clone(&file),
            false,
            vec![OwnedWrite {
                offset: 4,
                bytes: Bytes::from_static(b"WXYZ"),
            }],
            Box::new(move |result| {
                let _ = sender.send(result);
            }),
        )?;
        receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)??;
        let requests = vec![
            OwnedRead {
                offset: 2,
                length: 2,
            },
            OwnedRead {
                offset: 4,
                length: 0,
            },
            OwnedRead {
                offset: 4,
                length: 6,
            },
            OwnedRead {
                offset: 14,
                length: 8,
            },
        ];
        assert_eq!(read_windows(&requests)?.len(), 2);
        let (sender, receiver) = mpsc::sync_channel(1);
        submit_read(
            Arc::clone(&file),
            false,
            requests,
            Box::new(move |result| {
                let _ = sender.send(result);
            }),
        )?;
        let result = receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)??;
        assert_eq!(
            result,
            [
                Bytes::from_static(b"cd"),
                Bytes::new(),
                Bytes::from_static(b"WXYZij"),
                Bytes::from_static(b"op"),
            ]
        );
        assert_eq!((&*file).stream_position()?, 13);
        Ok(())
    }

    #[test]
    fn sdk_owned_overlapped_handle_attaches_once_and_reuses_original() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let file = Arc::new(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .custom_flags(FILE_FLAG_OVERLAPPED)
                .open(directory.path().join("overlapped"))?,
        );
        let (sender, receiver) = mpsc::sync_channel(1);
        submit_write(
            Arc::clone(&file),
            true,
            vec![OwnedWrite {
                offset: 7,
                bytes: Bytes::from_static(b"owned"),
            }],
            Box::new(move |result| {
                let _ = sender.send(result);
            }),
        )?;
        receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)??;
        file.set_len(20)?;
        file.sync_all()?;
        for _ in 0..2 {
            let (sender, receiver) = mpsc::sync_channel(1);
            submit_read(
                Arc::clone(&file),
                true,
                vec![OwnedRead {
                    offset: 7,
                    length: 5,
                }],
                Box::new(move |result| {
                    let _ = sender.send(result);
                }),
            )?;
            assert_eq!(
                receiver
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(io::Error::other)??,
                [Bytes::from_static(b"owned")],
            );
        }
        let (sender, receiver) = mpsc::sync_channel(1);
        submit_read(
            file,
            true,
            vec![OwnedRead {
                offset: 0,
                length: 20,
            }],
            Box::new(move |result| {
                let _ = sender.send(result);
            }),
        )?;
        let whole = receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)??;
        assert_eq!(
            whole,
            [Bytes::from_static(b"\0\0\0\0\0\0\0owned\0\0\0\0\0\0\0\0")]
        );
        Ok(())
    }

    #[test]
    fn invalid_batch_rejects_without_partial_write() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let file = Arc::new(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(directory.path().join("invalid"))?,
        );
        let (sender, receiver) = mpsc::sync_channel(1);
        submit_write(
            Arc::clone(&file),
            false,
            vec![
                OwnedWrite {
                    offset: 0,
                    bytes: Bytes::from_static(b"must-not-write"),
                },
                OwnedWrite {
                    offset: u64::MAX,
                    bytes: Bytes::from_static(b"invalid"),
                },
            ],
            Box::new(move |result| {
                let _ = sender.send(result);
            }),
        )?;
        let error = match receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)?
        {
            Ok(()) => return Err(io::Error::other("invalid batch was accepted")),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(file.metadata()?.len(), 0);
        Ok(())
    }

    #[test]
    fn detached_observer_does_not_cancel_accepted_write() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let file = Arc::new(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(directory.path().join("detached"))?,
        );
        let (sender, receiver) = mpsc::sync_channel(1);
        let completed = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&completed);
        submit_write(
            Arc::clone(&file),
            false,
            vec![OwnedWrite {
                offset: 0,
                bytes: Bytes::from_static(b"persist"),
            }],
            Box::new(move |result| {
                let _ = sender.send(result);
                signal.store(true, Ordering::Release);
            }),
        )?;
        drop(receiver);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !completed.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(
            completed.load(Ordering::Acquire),
            "accepted write did not finish"
        );
        let (sender, receiver) = mpsc::sync_channel(1);
        submit_read(
            file,
            false,
            vec![OwnedRead {
                offset: 0,
                length: 7,
            }],
            Box::new(move |result| {
                let _ = sender.send(result);
            }),
        )?;
        let read = receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)??;
        assert_eq!(read, [Bytes::from_static(b"persist")]);
        Ok(())
    }
}
