//! Checked console operations. The terminal owner controls worker lifetimes.
use std::io;
use std::mem::zeroed;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::sync::OnceLock;
use std::time::Instant;
use windows_sys::Win32::{
    Foundation::{ERROR_NOT_FOUND, HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT},
    Storage::FileSystem::ReadFile,
    System::{
        Console::{
            CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent, GetConsoleCP, GetConsoleMode,
            GetConsoleOutputCP, INPUT_RECORD, KEY_EVENT, PeekConsoleInputW, ReadConsoleInputW,
            SetConsoleCP, SetConsoleCtrlHandler, SetConsoleMode, SetConsoleOutputCP,
        },
        IO::CancelSynchronousIo,
        Threading::{CreateEventW, SetEvent, WaitForMultipleObjects},
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

/// Monotonic close notification shared by the console owner and its reader.
pub struct ConsoleClose(OwnedHandle);

impl ConsoleClose {
    /// Allocate the manual-reset event before publishing either console half.
    pub fn new() -> io::Result<Self> {
        // SAFETY: unnamed event, default security, initially nonsignalled.
        let event = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
        if event.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful CreateEventW transfers one owned kernel handle.
        Ok(Self(unsafe { OwnedHandle::from_raw_handle(event) }))
    }

    /// Publish the owner's close fence to every pending input wait.
    pub fn close(&self) -> io::Result<()> {
        // SAFETY: this object owns the event for the duration of the call.
        if unsafe { SetEvent(self.0.as_raw_handle() as HANDLE) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

/// Read VT bytes directly into caller storage, waiting on input or owner close.
pub fn console_read(
    input: usize,
    closed: &ConsoleClose,
    bytes: &mut [u8],
    deadline: Option<Instant>,
) -> io::Result<usize> {
    // One native read is a chunk; the caller retains the unconsumed input.
    let length = u16::try_from(bytes.len()).unwrap_or(u16::MAX).min(1024);
    if bytes.is_empty() {
        return Ok(0);
    }
    // Close precedes input so simultaneous readiness respects the close fence.
    let handles = [closed.0.as_raw_handle() as HANDLE, input as HANDLE];
    loop {
        let timeout = match deadline {
            Some(deadline) => {
                let now = Instant::now();
                if now >= deadline {
                    return Ok(0);
                }
                let remaining = deadline - now;
                let millis =
                    remaining.as_millis() + u128::from(remaining.subsec_nanos() % 1_000_000 != 0);
                // Finite deadlines may need successive native wait chunks;
                // the reserved INFINITE value belongs only to None.
                u32::try_from(millis)
                    .unwrap_or(u32::MAX - 1)
                    .min(u32::MAX - 1)
            }
            None => u32::MAX,
        };
        // SAFETY: the OS checks the handle; event and count are writable stack
        // storage, and ReadFile receives the exact allocated slice capacity.
        unsafe {
            match WaitForMultipleObjects(2, handles.as_ptr(), 0, timeout) {
                WAIT_TIMEOUT => continue,
                WAIT_OBJECT_0 => return Err(io::ErrorKind::NotConnected.into()),
                ready if ready == WAIT_OBJECT_0 + 1 => {}
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
                    u32::from(length),
                    &mut count,
                    std::ptr::null_mut(),
                ) == 0
                {
                    return Err(io::Error::last_os_error());
                }
                return Ok(count as usize);
            }
            if ReadConsoleInputW(input as HANDLE, &mut event, 1, &mut count) == 0 {
                return Err(io::Error::last_os_error());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, mpsc};

    #[test]
    fn owner_close_wakes_input_and_precedes_simultaneous_readiness() -> io::Result<()> {
        let input = Arc::new(ConsoleClose::new()?);
        let closed = Arc::new(ConsoleClose::new()?);
        let reader_input = Arc::clone(&input);
        let reader_closed = Arc::clone(&closed);
        let (started, ready) = mpsc::sync_channel(0);
        let worker = std::thread::spawn(move || {
            started.send(()).map_err(io::Error::other)?;
            console_read(
                reader_input.0.as_raw_handle() as usize,
                &reader_closed,
                &mut [0; 1],
                None,
            )
        });
        ready.recv().map_err(io::Error::other)?;
        closed.close()?;
        assert_eq!(
            worker
                .join()
                .map_err(|_| io::Error::other("input worker panicked"))?
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotConnected,
        );
        // A signalled input event is deliberately not a console handle. If
        // close loses priority the attempted console read fails differently.
        input.close()?;
        assert_eq!(
            console_read(input.0.as_raw_handle() as usize, &closed, &mut [0; 1], None)
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotConnected,
        );
        Ok(())
    }
}
