//! Checked console operations. The terminal owner controls worker lifetimes.
use std::io;
use std::mem::zeroed;
use std::os::windows::io::{AsRawHandle, OwnedHandle};
use std::sync::OnceLock;
use std::time::Instant;
use windows_sys::Win32::{
    Foundation::{ERROR_NOT_FOUND, HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT},
    Storage::FileSystem::{FILE_TYPE_CHAR, FILE_TYPE_PIPE, GetFileType, ReadFile},
    System::{
        Console::{
            CONSOLE_READCONSOLE_CONTROL, CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent, GetConsoleCP,
            GetConsoleMode, GetConsoleOutputCP, INPUT_RECORD, KEY_EVENT, PeekConsoleInputW,
            ReadConsoleInputW, ReadConsoleW, SetConsoleCP, SetConsoleCtrlHandler, SetConsoleMode,
            SetConsoleOutputCP, WINDOW_BUFFER_SIZE_EVENT,
        },
        IO::CancelSynchronousIo,
        Threading::{INFINITE, SetEvent, WaitForMultipleObjects, WaitForSingleObject},
    },
};

/// Original input/output modes and encoding pages.
#[derive(Clone, Copy)]
pub struct ConsoleState {
    /// Input mode bits.
    pub input_mode: u32,
    /// Output mode bits.
    pub output_mode: u32,
    /// Input code page.
    pub input_page: u32,
    /// Output code page.
    pub output_page: u32,
}

/// Capture modes from live, caller-owned console handles.
pub fn console_state(input: usize, output: usize) -> io::Result<ConsoleState> {
    let mut state = ConsoleState {
        input_mode: 0,
        output_mode: 0,
        input_page: 0,
        output_page: 0,
    };
    // SAFETY: pointers refer to initialized writable fields; handles are checked
    // by the OS and remain owned by the caller for the duration of this call.
    unsafe {
        if GetConsoleMode(input as HANDLE, &mut state.input_mode) == 0
            || GetConsoleMode(output as HANDLE, &mut state.output_mode) == 0
        {
            return Err(io::Error::last_os_error());
        }
        state.input_page = GetConsoleCP();
        state.output_page = GetConsoleOutputCP();
    }
    if state.input_page == 0 || state.output_page == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(state)
}

/// Apply every field even if an earlier restoration fails.
pub fn console_apply(input: usize, output: usize, state: ConsoleState) -> io::Result<()> {
    fn checked(result: i32) -> io::Result<()> {
        if result == 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
    // SAFETY: no borrowed buffers escape these calls; the OS validates handles.
    unsafe {
        let input = checked(SetConsoleMode(input as HANDLE, state.input_mode));
        let output = checked(SetConsoleMode(output as HANDLE, state.output_mode));
        let input_page = checked(SetConsoleCP(state.input_page));
        let output_page = checked(SetConsoleOutputCP(state.output_page));
        input.and(output).and(input_page).and(output_page)
    }
}

static HANDLER: OnceLock<fn(u32) -> bool> = OnceLock::new();
unsafe extern "system" fn control(event: u32) -> i32 {
    i32::from(HANDLER.get().is_some_and(|handler| handler(event)))
}

/// Register synchronous restoration before the default OS control handler.
pub fn console_register(handler: fn(u32) -> bool) -> io::Result<()> {
    HANDLER
        .set(handler)
        .map_err(|_| io::ErrorKind::AlreadyExists)?;
    // SAFETY: the callback and its published handler live for the process.
    if unsafe { SetConsoleCtrlHandler(Some(control), 1) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Generate an actual `CTRL_BREAK` event in this console.
pub fn console_break() -> io::Result<()> {
    // SAFETY: this function has no pointer arguments.
    if unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, 0) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Request cancellation on a live worker. The owner still waits for its ack;
/// `ERROR_NOT_FOUND` alone does not fence a submission that has not started yet.
pub fn cancel_thread(worker: &std::thread::JoinHandle<()>) {
    // SAFETY: JoinHandle owns a live thread handle throughout this call.
    let result = unsafe { CancelSynchronousIo(worker.as_raw_handle() as HANDLE) };
    let code = io::Error::last_os_error().raw_os_error();
    if result == 0 && code.and_then(|code| u32::try_from(code).ok()) != Some(ERROR_NOT_FOUND) {
        // Cancellation is retried until the worker acknowledges completion.
        std::thread::yield_now();
    }
}

/// How a process input handle can be awaited without polling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputKind {
    /// A pipe opened for overlapped I/O: completions arrive on the IOCP.
    Overlapped,
    /// Any other pipe, file or device: a synchronous read may wait on a
    /// peer, a disk or a network share, and `CancelSynchronousIo` releases
    /// it. The file type cannot tell NUL from a serial port, so no device
    /// is assumed to answer at once.
    Blocking,
    /// A console input buffer: waitable, and its text is UTF-16.
    Console,
}

/// Classify a live input handle.
pub fn input_kind(file: &std::fs::File) -> InputKind {
    let handle = file.as_raw_handle() as HANDLE;
    let mut mode = 0;
    // SAFETY: the borrowed file keeps the handle live for both queries, and
    // the mode output outlives the console query.
    let (kind, console) = unsafe { (GetFileType(handle), GetConsoleMode(handle, &raw mut mode)) };
    match kind {
        FILE_TYPE_PIPE if super::io::is_overlapped(file) => InputKind::Overlapped,
        FILE_TYPE_CHAR if console != 0 => InputKind::Console,
        _ => InputKind::Blocking,
    }
}

/// Process input, opened so it can be awaited without polling.
#[cfg(feature = "async-stdin")]
pub enum AsyncInput {
    /// An overlapped pipe registered with the runtime's IOCP.
    Overlapped(tokio::net::windows::named_pipe::NamedPipeClient),
    /// Anything else but a console, for a dedicated reader that
    /// `cancel_thread` releases.
    Blocking(std::fs::File),
    /// A console input buffer, for a dedicated reader that waits with
    /// `console_wait` and reads text with `console_read_text`.
    Console(std::fs::File),
}

/// Classify an owned input handle and open it accordingly. An overlapped pipe
/// is registered with the current Tokio runtime, so this must run inside one.
#[cfg(feature = "async-stdin")]
pub fn async_input(file: std::fs::File) -> io::Result<AsyncInput> {
    use std::os::windows::io::IntoRawHandle as _;
    Ok(match input_kind(&file) {
        // SAFETY: the file owned the handle and gives up ownership to the
        // client, and the classification confirmed a pipe opened for
        // overlapped I/O, which is what the client requires.
        InputKind::Overlapped => AsyncInput::Overlapped(unsafe {
            tokio::net::windows::named_pipe::NamedPipeClient::from_raw_handle(
                file.into_raw_handle(),
            )
        }?),
        InputKind::Blocking => AsyncInput::Blocking(file),
        InputKind::Console => AsyncInput::Console(file),
    })
}

/// Bytes queued in a pipe and not yet read by anyone.
pub fn pipe_bytes_available(pipe: &std::fs::File) -> io::Result<u32> {
    let mut available = 0;
    // SAFETY: the borrowed file keeps the handle live, the optional buffers
    // are null, and the count output outlives the call.
    let peeked = unsafe {
        windows_sys::Win32::System::Pipes::PeekNamedPipe(
            pipe.as_raw_handle() as HANDLE,
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            &raw mut available,
            std::ptr::null_mut(),
        )
    };
    if peeked == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(available)
}

/// Manual-reset event that every unbounded console input wait includes, so
/// closing the terminal releases a waiting reader without input arriving.
pub struct ConsoleWake(OwnedHandle);

impl ConsoleWake {
    /// Create an unsignaled wake event.
    pub fn new() -> io::Result<Self> {
        super::io::create_event()
            .map(Self)
            .map_err(io::Error::from_raw_os_error)
    }

    /// Signal permanently; waits that include this event return `NotConnected`.
    pub fn signal(&self) -> io::Result<()> {
        // SAFETY: the owned event handle is live for this call.
        if unsafe { SetEvent(self.0.as_raw_handle() as HANDLE) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl ConsoleWake {
    /// Whether `signal` has been called.
    pub fn is_signalled(&self) -> bool {
        // SAFETY: the owned event handle is live for this zero-timeout wait.
        unsafe { WaitForSingleObject(self.0.as_raw_handle() as HANDLE, 0) == WAIT_OBJECT_0 }
    }
}

/// Wait until console input is pending (true) or `wake` is signalled
/// (false). When both are, `wake` wins.
pub fn console_wait(input: &std::fs::File, wake: &ConsoleWake) -> io::Result<bool> {
    let handles = [
        wake.0.as_raw_handle() as HANDLE,
        input.as_raw_handle() as HANDLE,
    ];
    // SAFETY: both handles are borrowed live for the call and the array
    // outlives it.
    match unsafe { WaitForMultipleObjects(2, handles.as_ptr(), 0, INFINITE) } {
        WAIT_OBJECT_0 => Ok(false),
        result if result == WAIT_OBJECT_0 + 1 => Ok(true),
        _ => Err(io::Error::last_os_error()),
    }
}

/// UTF-16 code units of console text, read as the console's input mode
/// delivers them (a line, in line input mode). A Ctrl+Z typed at any point
/// ends the read with the 0x1A unit last, which the caller treats as end of
/// input. `CancelSynchronousIo` on the reading thread ends a pending read.
pub fn console_read_text(input: &std::fs::File, units: &mut [u16]) -> io::Result<usize> {
    let control = CONSOLE_READCONSOLE_CONTROL {
        nLength: size_of::<CONSOLE_READCONSOLE_CONTROL>() as u32,
        nInitialChars: 0,
        dwCtrlWakeupMask: 1 << 0x1a,
        dwControlKeyState: 0,
    };
    let mut count = 0;
    // SAFETY: the borrowed file keeps the handle live; the slice, count and
    // control outlive the call, and the length is the slice's own.
    let read = unsafe {
        ReadConsoleW(
            input.as_raw_handle() as HANDLE,
            units.as_mut_ptr().cast(),
            u32::try_from(units.len()).unwrap_or(u32::MAX),
            &raw mut count,
            &raw const control,
        )
    };
    if read == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(count as usize)
}

impl AsRawHandle for ConsoleWake {
    fn as_raw_handle(&self) -> std::os::windows::io::RawHandle {
        self.0.as_raw_handle()
    }
}

/// A native input record is either VT bytes or a window-size notification.
pub enum ConsoleRead {
    /// Bytes written to the supplied buffer; zero at a deadline.
    Bytes(usize),
    /// The console window dimensions may have changed.
    Resized,
}

/// Read VT bytes into `bytes` without consuming beyond its length. Without a
/// deadline the wait ends only on input or `wake`; with one, zero reports the
/// deadline. When input and `wake` are both signalled, `wake` wins.
pub fn console_read(
    input: usize,
    bytes: &mut [u8],
    deadline: Option<Instant>,
    wake: Option<&ConsoleWake>,
) -> io::Result<ConsoleRead> {
    if bytes.is_empty() {
        return Ok(ConsoleRead::Bytes(0));
    }
    let (handles, waits, first_input) = match wake {
        Some(wake) => (
            [wake.0.as_raw_handle() as HANDLE, input as HANDLE],
            2,
            WAIT_OBJECT_0 + 1,
        ),
        None => ([input as HANDLE, std::ptr::null_mut()], 1, WAIT_OBJECT_0),
    };
    loop {
        let timeout = match deadline {
            None => INFINITE,
            Some(deadline) => {
                let now = Instant::now();
                if now >= deadline {
                    return Ok(ConsoleRead::Bytes(0));
                }
                u32::try_from((deadline - now).as_millis()).unwrap_or(INFINITE - 1)
            }
        };
        // SAFETY: the OS checks the handles; the first `waits` entries are live
        // for this call. Event and count are writable stack storage, and
        // ReadFile receives the caller's exact slice length.
        unsafe {
            match WaitForMultipleObjects(waits, handles.as_ptr(), 0, timeout) {
                WAIT_TIMEOUT => return Ok(ConsoleRead::Bytes(0)),
                result if result == first_input => {}
                WAIT_OBJECT_0 => return Err(io::ErrorKind::NotConnected.into()),
                _ => return Err(io::Error::last_os_error()),
            }
            let mut event: INPUT_RECORD = zeroed();
            let mut count = 0;
            if PeekConsoleInputW(input as HANDLE, &mut event, 1, &mut count) == 0 {
                return Err(io::Error::last_os_error());
            }
            if count == 0 {
                continue;
            }
            let key = event.Event.KeyEvent;
            let vk = key.wVirtualKeyCode;
            if u32::from(event.EventType) == KEY_EVENT
                && key.bKeyDown != 0
                && (key.uChar.UnicodeChar != 0
                    || matches!(vk, 0x20 | 0x1b | 0x21..=0x28 | 0x2d | 0x2e | 0x70..=0x87))
            {
                if ReadFile(
                    input as HANDLE,
                    bytes.as_mut_ptr(),
                    u32::try_from(bytes.len()).unwrap_or(u32::MAX),
                    &mut count,
                    std::ptr::null_mut(),
                ) == 0
                {
                    return Err(io::Error::last_os_error());
                }
                return Ok(ConsoleRead::Bytes(count as usize));
            }
            if ReadConsoleInputW(input as HANDLE, &mut event, 1, &mut count) == 0 {
                return Err(io::Error::last_os_error());
            }
            if count != 0 && u32::from(event.EventType) == WINDOW_BUFFER_SIZE_EVENT {
                return Ok(ConsoleRead::Resized);
            }
        }
    }
}
