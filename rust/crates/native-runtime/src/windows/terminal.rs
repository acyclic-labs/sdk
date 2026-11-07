//! Checked console operations. The terminal owner controls worker lifetimes.
use std::io;
use std::mem::zeroed;
use std::os::windows::io::{AsRawHandle, OwnedHandle};
use std::sync::OnceLock;
use std::time::Instant;
use windows_sys::Win32::{
    Foundation::{HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT},
    Storage::FileSystem::ReadFile,
    System::{
        Console::{
            CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent, GetConsoleCP, GetConsoleMode,
            GetConsoleOutputCP, INPUT_RECORD, KEY_EVENT, PeekConsoleInputW, ReadConsoleInputW,
            SetConsoleCP, SetConsoleCtrlHandler, SetConsoleMode, SetConsoleOutputCP,
        },
        IO::CancelSynchronousIo,
        Threading::{SetEvent, WaitForMultipleObjects},
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

/// Read the visible window of the caller-owned output handle.
pub fn console_size(output: usize) -> io::Result<(u16, u16)> {
    use windows_sys::Win32::System::Console::{
        CONSOLE_SCREEN_BUFFER_INFO, GetConsoleScreenBufferInfo,
    };
    // SAFETY: all fields of this Win32 output structure admit the zero value.
    let mut info: CONSOLE_SCREEN_BUFFER_INFO = unsafe { zeroed() };
    // SAFETY: info is a live writable output structure; the OS validates the caller-owned handle.
    if unsafe { GetConsoleScreenBufferInfo(output as HANDLE, &mut info) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let width = i32::from(info.srWindow.Right) - i32::from(info.srWindow.Left) + 1;
    let height = i32::from(info.srWindow.Bottom) - i32::from(info.srWindow.Top) + 1;
    Ok((
        u16::try_from(width).map_err(|_| io::ErrorKind::InvalidData)?,
        u16::try_from(height).map_err(|_| io::ErrorKind::InvalidData)?,
    ))
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

/// Request cancellation of a worker's synchronous I/O. The caller must retain
/// the original worker and wait for its completion acknowledgement: this request
/// alone does not fence a submission that has not reached the OS yet.
pub fn cancel_thread(worker: &std::thread::JoinHandle<()>) {
    // SAFETY: JoinHandle retains the original thread handle throughout this call.
    // Completion, including a raced or unsuccessful request, belongs to the
    // owner's acknowledgement path rather than the cancellation return value.
    let _ = unsafe { CancelSynchronousIo(worker.as_raw_handle() as HANDLE) };
}

/// Read VT bytes directly into the caller's buffer. A native call consumes at
/// most u32::MAX bytes; larger buffers continue in the caller's next read.
pub fn console_read(
    input: usize,
    closed: &ConsoleClose,
    bytes: &mut [u8],
    deadline: Instant,
) -> io::Result<usize> {
    let length = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    if bytes.is_empty() {
        return Ok(0);
    }
    loop {
        if !closed.wait(input, deadline)? {
            return Ok(0);
        }
        // SAFETY: the OS checks the handle; event and count are writable stack
        // storage, and ReadFile receives at most the borrowed slice capacity.
        unsafe {
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
                    length,
                    &mut count,
                    std::ptr::null_mut(),
                ) == 0
                {
                    return Err(io::Error::last_os_error());
                }
                return usize::try_from(count).map_err(|_| io::ErrorKind::InvalidData.into());
            }
            if ReadConsoleInputW(input as HANDLE, &mut event, 1, &mut count) == 0 {
                return Err(io::Error::last_os_error());
            }
        }
    }
}

/// Retained close event for one console owner and its original input reader.
/// Closing is permanent: an event published before wait arming stays visible.
pub struct ConsoleClose {
    event: OwnedHandle,
}

impl ConsoleClose {
    /// Allocate one initially unsignalled event for the owning console.
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            event: super::io::create_event().map_err(io::Error::from_raw_os_error)?,
        })
    }

    /// Publish the original owner's terminal close fence.
    pub fn signal(&self) -> io::Result<()> {
        // SAFETY: the owned manual-reset event remains live throughout the call.
        if unsafe { SetEvent(self.event.as_raw_handle() as HANDLE) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn wait(&self, input: usize, deadline: Instant) -> io::Result<bool> {
        let handles = [self.event.as_raw_handle() as HANDLE, input as HANDLE];
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            let remaining = deadline - now;
            // Round up to the OS millisecond unit; never turn a remaining
            // fraction into an immediate readiness recheck. The OS maximum
            // is a wait chunk, not an earlier caller deadline.
            let milliseconds =
                remaining.as_millis() + u128::from(remaining.subsec_nanos() % 1_000_000 != 0);
            let timeout = milliseconds.min(u128::from(u32::MAX - 1)) as u32;
            // SAFETY: both original handles are retained by their owners;
            // this fixed array lives through the wait. Close has priority
            // when both sources are ready.
            match unsafe { WaitForMultipleObjects(2, handles.as_ptr(), 0, timeout) } {
                WAIT_OBJECT_0 => return Err(io::ErrorKind::NotConnected.into()),
                ready if ready == WAIT_OBJECT_0 + 1 => return Ok(true),
                WAIT_TIMEOUT => {}
                _ => return Err(io::Error::last_os_error()),
            }
        }
    }
}
