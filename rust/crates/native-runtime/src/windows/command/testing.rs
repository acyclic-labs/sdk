//! Test support for observing which processes hold this process's output,
//! and for tying the helper processes a test starts to the test.
//!
//! A [`Tether`] is the test's end of a pipe to one helper; the helper holds
//! the other end as a [`Tethered`]. The helper reports over it, and learns
//! it was released when the test's end closes, however the test ends, so a
//! helper that waits on [`Tethered::released`] never outlives its test.
//! A test waits on its tethers and its helpers' process handles together,
//! with one deadline, through [`Tether::wait`]: a report, a helper's end and a
//! helper's exit are each an event, so a failing helper is seen at once.

use std::ffi::{OsStr, OsString};
use std::io;
use std::os::windows::ffi::OsStrExt as _;
use std::os::windows::io::{AsRawHandle as _, BorrowedHandle, FromRawHandle as _, OwnedHandle};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_BROKEN_PIPE, ERROR_IO_PENDING, ERROR_PIPE_CONNECTED, GENERIC_READ,
    GENERIC_WRITE, HANDLE, INVALID_HANDLE_VALUE, WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, OPEN_EXISTING,
    PIPE_ACCESS_DUPLEX, ReadFile, WriteFile,
};
use windows_sys::Win32::System::Console::{GetStdHandle, STD_OUTPUT_HANDLE};
use windows_sys::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};
use windows_sys::Win32::System::Pipes::{
    ConnectNamedPipe, CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS,
    PIPE_TYPE_BYTE, PIPE_WAIT,
};
use windows_sys::Win32::System::Threading::{CreateEventW, INFINITE, WaitForMultipleObjects};

/// Closes this process's standard output handle, so a reader of it sees
/// the end once no other process holds it. Writes to standard output fail
/// afterwards; the caller must hold no borrow of the handle.
pub fn close_standard_output() -> io::Result<()> {
    // SAFETY: reads this process's standard output slot.
    let handle = unsafe { GetStdHandle(STD_OUTPUT_HANDLE) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "no standard output to close",
        ));
    }
    // SAFETY: the slot's open handle; the contract above leaves no borrow.
    if unsafe { CloseHandle(handle) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

const CAPACITY: u32 = 4096;

fn wide(text: &OsStr) -> Vec<u16> {
    text.encode_wide().chain(Some(0)).collect()
}

fn event() -> io::Result<OwnedHandle> {
    // SAFETY: an unnamed manual-reset event, owned below.
    let event = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
    if event.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreateEventW returned a handle this process owns.
    Ok(unsafe { OwnedHandle::from_raw_handle(event) })
}

fn overlapped(event: &OwnedHandle) -> OVERLAPPED {
    OVERLAPPED {
        hEvent: event.as_raw_handle(),
        ..OVERLAPPED::default()
    }
}

/// One read or write on an overlapped `pipe`, waited to its end. Reads and
/// writes on such a handle do not wait for each other, so one thread may
/// report while another waits to read. A closed far end reads as zero.
fn transfer(pipe: HANDLE, read: bool, buffer: &mut [u8]) -> io::Result<usize> {
    let done = event()?;
    let mut operation = overlapped(&done);
    let length = u32::try_from(buffer.len()).unwrap_or(u32::MAX);
    let mut count = 0;
    // SAFETY: `buffer` and `operation` outlive the operation, which is
    // waited to its end below before either is released.
    let started = unsafe {
        if read {
            ReadFile(
                pipe,
                buffer.as_mut_ptr(),
                length,
                &raw mut count,
                &raw mut operation,
            )
        } else {
            WriteFile(
                pipe,
                buffer.as_ptr(),
                length,
                &raw mut count,
                &raw mut operation,
            )
        }
    };
    if started == 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(ERROR_IO_PENDING.cast_signed()) {
            return closed_as_zero(error);
        }
    }
    // SAFETY: waits for the operation started above.
    if unsafe { GetOverlappedResult(pipe, &raw const operation, &raw mut count, 1) } == 0 {
        return closed_as_zero(io::Error::last_os_error());
    }
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn closed_as_zero(error: io::Error) -> io::Result<usize> {
    if error.raw_os_error() == Some(ERROR_BROKEN_PIPE.cast_signed()) {
        Ok(0)
    } else {
        Err(error)
    }
}

/// A test's end of a pipe to one helper process.
pub struct Tether {
    name: OsString,
    pipe: OwnedHandle,
    event: OwnedHandle,
    /// The connection or read in flight; boxed so its address is stable.
    operation: Box<OVERLAPPED>,
    buffer: Box<[u8; CAPACITY as usize]>,
    pending: bool,
    connected: bool,
    closed: bool,
    received: Vec<u8>,
}

/// What [`Tether::wait`] saw first, by position in the slices it was given.
#[derive(Debug, PartialEq, Eq)]
pub enum TetherEvent {
    /// The tether's helper reported this line.
    Line(usize, String),
    /// The tether's helper closed its end: it exited.
    Closed(usize),
    /// The process exited.
    Exited(usize),
}

impl Tether {
    /// Opens a tether under a name no other tether takes, ready for its
    /// helper to connect.
    pub fn new() -> io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let name = OsString::from(format!(
            "\\\\.\\pipe\\acyclic-tether-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        // SAFETY: a NUL-terminated name; the one instance is ours, and the
        // handle is not inheritable.
        let pipe = unsafe {
            CreateNamedPipeW(
                wide(&name).as_ptr(),
                PIPE_ACCESS_DUPLEX | FILE_FLAG_FIRST_PIPE_INSTANCE | FILE_FLAG_OVERLAPPED,
                PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
                1,
                CAPACITY,
                CAPACITY,
                0,
                std::ptr::null(),
            )
        };
        if pipe == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateNamedPipeW returned a handle this process owns.
        let pipe = unsafe { OwnedHandle::from_raw_handle(pipe) };
        let event = event()?;
        let operation = Box::new(overlapped(&event));
        let mut tether = Self {
            name,
            pipe,
            event,
            operation,
            buffer: Box::new([0; CAPACITY as usize]),
            pending: false,
            connected: false,
            closed: false,
            received: Vec::new(),
        };
        tether.arm()?;
        Ok(tether)
    }

    /// The name a helper connects to.
    pub fn name(&self) -> &OsStr {
        &self.name
    }

    /// Sends the helper one line. The helper must have connected.
    pub fn send(&self, line: &str) -> io::Result<()> {
        let mut bytes = format!("{line}\n").into_bytes();
        let written = transfer(self.pipe.as_raw_handle(), false, &mut bytes)?;
        if written == bytes.len() {
            Ok(())
        } else {
            Err(io::ErrorKind::BrokenPipe.into())
        }
    }

    fn line(&mut self) -> Option<String> {
        let end = self.received.iter().position(|&byte| byte == b'\n')?;
        let line: Vec<u8> = self.received.drain(..=end).collect();
        Some(
            String::from_utf8_lossy(&line)
                .trim_end_matches(['\r', '\n'])
                .to_owned(),
        )
    }

    /// Starts the connection or next read, unless one is in flight. Its
    /// completion signals the event, synchronous completion included.
    fn arm(&mut self) -> io::Result<()> {
        if self.pending || self.closed {
            return Ok(());
        }
        *self.operation = overlapped(&self.event);
        let pipe = self.pipe.as_raw_handle();
        if !self.connected {
            // SAFETY: the boxed operation stays in place until it completes
            // or `drop` cancels it and waits.
            if unsafe { ConnectNamedPipe(pipe, &raw mut *self.operation) } == 0 {
                let error = io::Error::last_os_error();
                match error.raw_os_error() {
                    Some(code) if code == ERROR_IO_PENDING.cast_signed() => {
                        self.pending = true;
                        return Ok(());
                    }
                    Some(code) if code == ERROR_PIPE_CONNECTED.cast_signed() => {}
                    _ => return Err(error),
                }
            }
            self.connected = true;
            *self.operation = overlapped(&self.event);
        }
        // SAFETY: as above, for the boxed buffer too.
        let started = unsafe {
            ReadFile(
                pipe,
                self.buffer.as_mut_ptr(),
                CAPACITY,
                std::ptr::null_mut(),
                &raw mut *self.operation,
            )
        };
        if started == 0 {
            let error = io::Error::last_os_error();
            match error.raw_os_error() {
                Some(code) if code == ERROR_IO_PENDING.cast_signed() => {}
                Some(code) if code == ERROR_BROKEN_PIPE.cast_signed() => {
                    self.closed = true;
                    return Ok(());
                }
                _ => return Err(error),
            }
        }
        self.pending = true;
        Ok(())
    }

    /// Takes the result of the operation whose event signalled.
    fn complete(&mut self) -> io::Result<()> {
        let mut count = 0;
        self.pending = false;
        // SAFETY: the operation in flight, which has completed.
        let done = unsafe {
            GetOverlappedResult(
                self.pipe.as_raw_handle(),
                &raw const *self.operation,
                &raw mut count,
                0,
            )
        };
        if done == 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(ERROR_BROKEN_PIPE.cast_signed()) {
                self.closed = true;
                return Ok(());
            }
            return Err(error);
        }
        if !self.connected {
            self.connected = true;
            return Ok(());
        }
        let count = usize::try_from(count).unwrap_or(usize::MAX);
        if count == 0 {
            self.closed = true;
        }
        self.received
            .extend_from_slice(self.buffer.get(..count).unwrap_or_default());
        Ok(())
    }
}

impl Tether {
    /// Waits until one of `tethers` has a line or has closed, or one of
    /// `processes` has exited, and reports which; a tether's line comes before
    /// its close and before any exit. Fails as timed out at `deadline`.
    pub fn wait(
        tethers: &mut [&mut Tether],
        processes: &[BorrowedHandle<'_>],
        deadline: Instant,
    ) -> io::Result<TetherEvent> {
        loop {
            for (index, tether) in tethers.iter_mut().enumerate() {
                if let Some(line) = tether.line() {
                    return Ok(TetherEvent::Line(index, line));
                }
                tether.arm()?;
                if tether.closed {
                    return Ok(TetherEvent::Closed(index));
                }
            }
            let handles: Vec<HANDLE> = tethers
                .iter()
                .map(|tether| tether.event.as_raw_handle())
                .chain(processes.iter().map(|process| process.as_raw_handle()))
                .collect();
            let remaining = deadline.saturating_duration_since(Instant::now());
            let milliseconds = u32::try_from(remaining.as_millis())
                .map_or(INFINITE - 1, |ms| ms.min(INFINITE - 1));
            let count = u32::try_from(handles.len()).unwrap_or(u32::MAX);
            // SAFETY: open event and process handles, borrowed for the call.
            let signalled =
                unsafe { WaitForMultipleObjects(count, handles.as_ptr(), 0, milliseconds) };
            if signalled == WAIT_TIMEOUT {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "the test's deadline passed",
                ));
            }
            if signalled == WAIT_FAILED {
                return Err(io::Error::last_os_error());
            }
            let index = usize::try_from(signalled - WAIT_OBJECT_0).unwrap_or(usize::MAX);
            match tethers.get_mut(index) {
                Some(tether) => tether.complete()?,
                None => return Ok(TetherEvent::Exited(index.saturating_sub(tethers.len()))),
            }
        }
    }
}

impl Drop for Tether {
    fn drop(&mut self) {
        if self.pending {
            let mut count = 0;
            // SAFETY: cancels the operation in flight and waits for it to
            // end before its box is released.
            unsafe {
                CancelIoEx(self.pipe.as_raw_handle(), &raw const *self.operation);
                GetOverlappedResult(
                    self.pipe.as_raw_handle(),
                    &raw const *self.operation,
                    &raw mut count,
                    1,
                );
            }
        }
    }
}

/// A helper's end of a [`Tether`]. Shared between threads, it reports from
/// one while another waits to read.
pub struct Tethered(OwnedHandle);

impl Tethered {
    /// Connects to the tether named `name`. The handle is not inheritable.
    pub fn connect(name: &OsStr) -> io::Result<Self> {
        // SAFETY: a NUL-terminated name; the handle is owned below.
        let pipe = unsafe {
            CreateFileW(
                wide(name).as_ptr(),
                GENERIC_READ | GENERIC_WRITE,
                0,
                std::ptr::null(),
                OPEN_EXISTING,
                FILE_FLAG_OVERLAPPED,
                std::ptr::null_mut(),
            )
        };
        if pipe == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateFileW returned a handle this process owns.
        Ok(Self(unsafe { OwnedHandle::from_raw_handle(pipe) }))
    }

    /// Reports one line to the test; a line break inside it becomes a space.
    pub fn send(&self, line: &str) -> io::Result<()> {
        let mut bytes = format!("{}\n", line.replace(['\r', '\n'], " ")).into_bytes();
        let written = transfer(self.0.as_raw_handle(), false, &mut bytes)?;
        if written == bytes.len() {
            Ok(())
        } else {
            Err(io::ErrorKind::BrokenPipe.into())
        }
    }

    /// The test's next line, or `None` once the test released this helper.
    pub fn receive(&self) -> io::Result<Option<String>> {
        let mut line = Vec::new();
        let mut byte = [0];
        loop {
            if transfer(self.0.as_raw_handle(), true, &mut byte)? == 0 {
                return Ok(None);
            }
            if byte == *b"\n" {
                return Ok(Some(String::from_utf8_lossy(&line).into_owned()));
            }
            line.extend_from_slice(&byte);
        }
    }

    /// Waits until the test releases this helper, by closing its end or
    /// ending.
    pub fn released(&self) {
        while let Ok(Some(_)) = self.receive() {}
    }
}
