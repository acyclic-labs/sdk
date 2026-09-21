//! Windows positional I/O through overlapped handles.

#![allow(unsafe_code)]

use crate::{OwnedRead, OwnedWrite};
use bytes::Bytes;
use std::cell::RefCell;
use std::fs::File;
use std::io;
use std::mem::{ManuallyDrop, zeroed};
use std::os::windows::ffi::OsStrExt as _;
use std::os::windows::io::{AsRawHandle, FromRawHandle, RawHandle};
use std::path::Path;
use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_HANDLE_EOF, ERROR_IO_PENDING, HANDLE, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_FLAG_OVERLAPPED, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_SHARE_DELETE,
    FILE_SHARE_READ, FILE_SHARE_WRITE, ReOpenFile, ReadFile, SetFileAttributesW, WriteFile,
};
use windows_sys::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED,
};
use windows_sys::Win32::System::Threading::{
    CREATE_NEW_PROCESS_GROUP, CreateProcessW, DETACHED_PROCESS, INFINITE, PROCESS_INFORMATION,
    STARTUPINFOW,
};

const MAXIMUM_BATCH: usize = 16;

pub(super) fn spawn_service_process(executable: &Path) -> io::Result<()> {
    let executable_wide = executable
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let executable_argument = executable.as_os_str().encode_wide().collect::<Vec<_>>();
    if executable_argument.contains(&u16::from(b'"')) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "service executable path contains a quote",
        ));
    }
    let mut command_line = Vec::with_capacity(executable_argument.len() + 16);
    command_line.push(u16::from(b'"'));
    command_line.extend(executable_argument);
    command_line.extend("\" __service\0".encode_utf16());
    // SAFETY: zero is the documented initial state for these Win32 output
    // structures; `cb` is set before the structure is passed to the API.
    let mut startup: STARTUPINFOW = unsafe { zeroed() };
    startup.cb = u32::try_from(std::mem::size_of::<STARTUPINFOW>())
        .map_err(|_| io::Error::other("STARTUPINFOW size does not fit u32"))?;
    // SAFETY: CreateProcessW initializes every returned handle on success.
    let mut process: PROCESS_INFORMATION = unsafe { zeroed() };
    // SAFETY: the application and command-line buffers are live and
    // NUL-terminated for the call. Handle inheritance is explicitly disabled;
    // null environment and directory pointers inherit the current values.
    let created = unsafe {
        CreateProcessW(
            executable_wide.as_ptr(),
            command_line.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            0,
            CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS,
            std::ptr::null(),
            std::ptr::null(),
            &startup,
            &mut process,
        )
    };
    if created == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful process creation transfers one owned reference for
    // each returned handle; the child remains alive after both are closed.
    unsafe {
        CloseHandle(process.hThread);
        CloseHandle(process.hProcess);
    }
    Ok(())
}

pub(super) fn set_file_attributes(path: &std::path::Path, attributes: u32) -> io::Result<()> {
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

struct CompletionPort(HANDLE);

impl CompletionPort {
    fn new() -> io::Result<Self> {
        // SAFETY: INVALID_HANDLE_VALUE creates a standalone completion port.
        let handle =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, std::ptr::null_mut(), 0, 0) };
        if handle.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(Self(handle))
        }
    }
}

impl Drop for CompletionPort {
    fn drop(&mut self) {
        // SAFETY: this type exclusively owns the valid completion port.
        unsafe { CloseHandle(self.0) };
    }
}

struct PendingRead {
    overlapped: Box<OVERLAPPED>,
    buffer: Vec<u8>,
    completed: Option<usize>,
}

struct PendingWrite {
    overlapped: Box<OVERLAPPED>,
    bytes: Bytes,
    completed: bool,
}

enum QuarantinedPending {
    Reads { _requests: Vec<PendingRead> },
    Writes { _requests: Vec<PendingWrite> },
}

struct QuarantinedIo {
    _port: CompletionPort,
    _file: File,
    _pending: QuarantinedPending,
}

struct PortState {
    port: Option<CompletionPort>,
    next_key: usize,
    // An uncertain overlapped operation may still dereference its OVERLAPPED
    // and buffer after the owning thread exits. Keep the entire batch alive
    // until process teardown and fail closed instead of ever replacing it.
    quarantine: Option<ManuallyDrop<QuarantinedIo>>,
}

thread_local! {
    static PORT: RefCell<PortState> = const { RefCell::new(PortState {
        port: None,
        next_key: 1,
        quarantine: None,
    }) };
}

pub(super) fn read_batch(file: &File, reads: &[OwnedRead]) -> io::Result<Vec<Bytes>> {
    if reads.is_empty() {
        return Ok(Vec::new());
    }
    PORT.with_borrow_mut(|state| {
        let mut results = Vec::with_capacity(reads.len());
        for chunk in reads.chunks(MAXIMUM_BATCH) {
            results.extend(read_batch_on_port(state, file, chunk)?);
        }
        Ok(results)
    })
}

#[allow(clippy::needless_pass_by_value)]
pub(super) fn write_all_batch_owned(file: &File, writes: Vec<OwnedWrite>) -> io::Result<()> {
    if writes.is_empty() {
        return Ok(());
    }
    PORT.with_borrow_mut(|state| {
        for chunk in writes.chunks(MAXIMUM_BATCH) {
            write_batch_on_port(state, file, chunk)?;
        }
        Ok(())
    })
}

fn write_batch_on_port(
    state: &mut PortState,
    file: &File,
    writes: &[OwnedWrite],
) -> io::Result<()> {
    if state.quarantine.is_some() {
        return Err(io::Error::other(
            "Windows overlapped I/O is quarantined after an uncertain completion",
        ));
    }
    if state.port.is_none() {
        state.port = Some(CompletionPort::new()?);
    }
    let port = state
        .port
        .as_ref()
        .ok_or_else(|| io::Error::other("completion port is unavailable"))?;
    let key = state.next_key;
    state.next_key = state.next_key.wrapping_add(1).max(1);
    let overlapped = reopen_overlapped(file, FILE_GENERIC_WRITE)?;
    let handle = overlapped.as_raw_handle() as HANDLE;
    // SAFETY: the fresh overlapped handle has not been associated with another port.
    if unsafe { CreateIoCompletionPort(handle, port.0, key, 0) }.is_null() {
        return Err(io::Error::last_os_error());
    }
    let mut pending = Vec::new();
    pending.try_reserve_exact(writes.len())?;
    let mut accepted = 0;
    let mut first_error = None;
    for write in writes {
        match submit_write(handle, write) {
            Ok((write, queued)) => {
                accepted += usize::from(queued);
                pending.push(write);
            }
            Err(error) => {
                first_error = Some(error);
                break;
            }
        }
    }
    match complete_writes(port.0, key, accepted, &mut pending) {
        Ok(()) => {}
        Err(CompletionError::Completed(error)) => {
            first_error.get_or_insert(error);
        }
        Err(CompletionError::Uncertain(error)) => {
            first_error.get_or_insert(error);
            let failed_port = state
                .port
                .take()
                .ok_or_else(|| io::Error::other("completion port disappeared during quarantine"))?;
            state.quarantine = Some(ManuallyDrop::new(QuarantinedIo {
                _port: failed_port,
                _file: overlapped,
                _pending: QuarantinedPending::Writes { _requests: pending },
            }));
            return first_error.map_or(Ok(()), Err);
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn read_batch_on_port(
    state: &mut PortState,
    file: &File,
    reads: &[OwnedRead],
) -> io::Result<Vec<Bytes>> {
    if state.quarantine.is_some() {
        return Err(io::Error::other(
            "Windows overlapped I/O is quarantined after an uncertain completion",
        ));
    }
    if state.port.is_none() {
        state.port = Some(CompletionPort::new()?);
    }
    let port = state
        .port
        .as_ref()
        .ok_or_else(|| io::Error::other("completion port is unavailable"))?;
    let key = state.next_key;
    state.next_key = state.next_key.wrapping_add(1).max(1);
    let overlapped = reopen_overlapped(file, FILE_GENERIC_READ)?;
    let handle = overlapped.as_raw_handle() as HANDLE;
    // SAFETY: the fresh overlapped handle has not been associated with another port.
    if unsafe { CreateIoCompletionPort(handle, port.0, key, 0) }.is_null() {
        return Err(io::Error::last_os_error());
    }
    let mut pending = Vec::new();
    pending.try_reserve_exact(reads.len())?;
    let mut accepted = 0;
    let mut first_error = None;
    for read in reads {
        match submit_read(handle, *read) {
            Ok((read, queued)) => {
                accepted += usize::from(queued);
                pending.push(read);
            }
            Err(error) => {
                first_error = Some(error);
                break;
            }
        }
    }
    match complete_reads(port.0, key, accepted, &mut pending) {
        Ok(()) => {}
        Err(CompletionError::Completed(error)) => {
            first_error.get_or_insert(error);
        }
        Err(CompletionError::Uncertain(error)) => {
            first_error.get_or_insert(error);
            let failed_port = state
                .port
                .take()
                .ok_or_else(|| io::Error::other("completion port disappeared during quarantine"))?;
            state.quarantine = Some(ManuallyDrop::new(QuarantinedIo {
                _port: failed_port,
                _file: overlapped,
                _pending: QuarantinedPending::Reads { _requests: pending },
            }));
            return first_error.map_or(Ok(Vec::new()), Err);
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok(pending
        .into_iter()
        .map(|mut read| {
            read.buffer.truncate(read.completed.unwrap_or_default());
            Bytes::from(read.buffer)
        })
        .collect())
}

fn reopen_overlapped(file: &File, access: u32) -> io::Result<File> {
    // SAFETY: ReOpenFile duplicates the live authorized handle without path traversal.
    let handle = unsafe {
        ReOpenFile(
            file.as_raw_handle() as HANDLE,
            access,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            FILE_FLAG_OVERLAPPED,
        )
    };
    if handle.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: ownership of the newly returned handle transfers to File.
    Ok(unsafe { File::from_raw_handle(handle as RawHandle) })
}

fn submit_write(handle: HANDLE, write: &OwnedWrite) -> io::Result<(PendingWrite, bool)> {
    let length = u32::try_from(write.bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "batch write is too large"))?;
    // SAFETY: zero is the documented initial OVERLAPPED representation.
    let mut overlapped: Box<OVERLAPPED> = Box::new(unsafe { zeroed() });
    let [a, b, c, d, e, f, g, h] = write.offset.to_le_bytes();
    overlapped.Anonymous.Anonymous.Offset = u32::from_le_bytes([a, b, c, d]);
    overlapped.Anonymous.Anonymous.OffsetHigh = u32::from_le_bytes([e, f, g, h]);
    if write.bytes.is_empty() {
        return Ok((
            PendingWrite {
                overlapped,
                bytes: Bytes::new(),
                completed: true,
            },
            false,
        ));
    }
    // SAFETY: handle is overlapped; bytes and OVERLAPPED remain live until completion.
    let started = unsafe {
        WriteFile(
            handle,
            write.bytes.as_ptr(),
            length,
            std::ptr::null_mut(),
            &mut *overlapped,
        )
    };
    if started == 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(ERROR_IO_PENDING.cast_signed()) {
            return Err(error);
        }
    }
    Ok((
        PendingWrite {
            overlapped,
            bytes: write.bytes.clone(),
            completed: false,
        },
        true,
    ))
}

fn submit_read(handle: HANDLE, read: OwnedRead) -> io::Result<(PendingRead, bool)> {
    let length = u32::try_from(read.length)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "batch read is too large"))?;
    // SAFETY: zero is the documented initial OVERLAPPED representation.
    let mut overlapped: Box<OVERLAPPED> = Box::new(unsafe { zeroed() });
    let [a, b, c, d, e, f, g, h] = read.offset.to_le_bytes();
    overlapped.Anonymous.Anonymous.Offset = u32::from_le_bytes([a, b, c, d]);
    overlapped.Anonymous.Anonymous.OffsetHigh = u32::from_le_bytes([e, f, g, h]);
    let mut buffer = vec![0_u8; read.length];
    if buffer.is_empty() {
        return Ok((
            PendingRead {
                overlapped,
                buffer,
                completed: Some(0),
            },
            false,
        ));
    }
    // SAFETY: handle is overlapped; buffer and OVERLAPPED remain pinned by owned allocations.
    let started = unsafe {
        ReadFile(
            handle,
            buffer.as_mut_ptr(),
            length,
            std::ptr::null_mut(),
            &mut *overlapped,
        )
    };
    if started == 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(ERROR_HANDLE_EOF.cast_signed()) {
            return Ok((
                PendingRead {
                    overlapped,
                    buffer,
                    completed: Some(0),
                },
                false,
            ));
        }
        if error.raw_os_error() != Some(ERROR_IO_PENDING.cast_signed()) {
            return Err(error);
        }
    }
    Ok((
        PendingRead {
            overlapped,
            buffer,
            completed: None,
        },
        true,
    ))
}

fn complete_reads(
    port: HANDLE,
    expected_key: usize,
    mut remaining: usize,
    pending: &mut [PendingRead],
) -> Result<(), CompletionError> {
    let mut first_error = None;
    while remaining != 0 {
        let mut transferred = 0;
        let mut key = 0;
        let mut overlapped = std::ptr::null_mut();
        // SAFETY: the completion port is live and output pointers are valid for this call.
        let succeeded = unsafe {
            GetQueuedCompletionStatus(port, &mut transferred, &mut key, &mut overlapped, INFINITE)
        };
        if overlapped.is_null() {
            return Err(CompletionError::Uncertain(io::Error::last_os_error()));
        }
        if key != expected_key {
            return Err(CompletionError::Uncertain(io::Error::other(
                "unknown completion batch",
            )));
        }
        let Some(read) = pending
            .iter_mut()
            .find(|read| std::ptr::eq(&*read.overlapped, overlapped))
        else {
            return Err(CompletionError::Uncertain(io::Error::other(
                "unknown completion identity",
            )));
        };
        if read.completed.is_some() {
            return Err(CompletionError::Uncertain(io::Error::other(
                "duplicate completion identity",
            )));
        }
        remaining -= 1;
        if transferred as usize > read.buffer.len() {
            first_error.get_or_insert_with(|| io::Error::other("read exceeded submitted length"));
        } else if succeeded == 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_HANDLE_EOF.cast_signed()) {
                first_error.get_or_insert(error);
            }
        }
        read.completed = Some(transferred as usize);
    }
    first_error.map_or(Ok(()), |error| Err(CompletionError::Completed(error)))
}

fn complete_writes(
    port: HANDLE,
    expected_key: usize,
    mut remaining: usize,
    pending: &mut [PendingWrite],
) -> Result<(), CompletionError> {
    let mut first_error = None;
    while remaining != 0 {
        let mut transferred = 0;
        let mut key = 0;
        let mut overlapped = std::ptr::null_mut();
        // SAFETY: the completion port is live and output pointers are valid for this call.
        let succeeded = unsafe {
            GetQueuedCompletionStatus(port, &mut transferred, &mut key, &mut overlapped, INFINITE)
        };
        if overlapped.is_null() {
            return Err(CompletionError::Uncertain(io::Error::last_os_error()));
        }
        if key != expected_key {
            return Err(CompletionError::Uncertain(io::Error::other(
                "unknown completion batch",
            )));
        }
        let Some(write) = pending
            .iter_mut()
            .find(|write| std::ptr::eq(&*write.overlapped, overlapped))
        else {
            return Err(CompletionError::Uncertain(io::Error::other(
                "unknown completion identity",
            )));
        };
        if write.completed {
            return Err(CompletionError::Uncertain(io::Error::other(
                "duplicate completion identity",
            )));
        }
        remaining -= 1;
        if succeeded == 0 {
            first_error.get_or_insert_with(io::Error::last_os_error);
        } else if transferred as usize != write.bytes.len() {
            first_error.get_or_insert_with(|| {
                io::Error::new(io::ErrorKind::WriteZero, "short overlapped write")
            });
        }
        write.completed = true;
    }
    first_error.map_or(Ok(()), |error| Err(CompletionError::Completed(error)))
}

enum CompletionError {
    Completed(io::Error),
    Uncertain(io::Error),
}
