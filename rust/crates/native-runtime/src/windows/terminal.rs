//! Checked console operations. The terminal owner controls worker lifetimes.
use std::io;
use std::mem::zeroed;
use std::os::windows::io::AsRawHandle;
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
        Threading::WaitForSingleObject,
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

/// Generate an actual CTRL_BREAK event in this console.
pub fn console_break() -> io::Result<()> {
    // SAFETY: this function has no pointer arguments.
    if unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, 0) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Request cancellation on a live worker. The owner still waits for its ack;
/// ERROR_NOT_FOUND alone does not fence a submission that has not started yet.
pub fn cancel_thread(worker: &std::thread::JoinHandle<()>) {
    // SAFETY: JoinHandle owns a live thread handle throughout this call.
    let result = unsafe { CancelSynchronousIo(worker.as_raw_handle() as HANDLE) };
    if result == 0 && io::Error::last_os_error().raw_os_error() != Some(ERROR_NOT_FOUND as i32) {
        // Cancellation is retried until the worker acknowledges completion.
        std::thread::yield_now();
    }
}

/// Read VT bytes without consuming beyond the caller's capacity.
pub fn console_read(input: usize, capacity: usize, deadline: Instant) -> io::Result<Vec<u8>> {
    let mut bytes = vec![0; capacity.min(1024)];
    if bytes.is_empty() {
        return Ok(bytes);
    }
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Ok(Vec::new());
        }
        let timeout = u32::try_from((deadline - now).as_millis()).unwrap_or(u32::MAX - 1);
        // SAFETY: the OS checks the handle; event and count are writable stack
        // storage, and ReadFile receives the exact allocated slice capacity.
        unsafe {
            match WaitForSingleObject(input as HANDLE, timeout) {
                WAIT_TIMEOUT => return Ok(Vec::new()),
                WAIT_OBJECT_0 => {}
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
            if event.EventType == KEY_EVENT as u16
                && key.bKeyDown != 0
                && (key.uChar.UnicodeChar != 0
                    || matches!(vk, 0x20 | 0x1b | 0x21..=0x28 | 0x2d | 0x2e | 0x70..=0x87))
            {
                if ReadFile(
                    input as HANDLE,
                    bytes.as_mut_ptr(),
                    bytes.len() as u32,
                    &mut count,
                    std::ptr::null_mut(),
                ) == 0
                {
                    return Err(io::Error::last_os_error());
                }
                bytes.truncate(count as usize);
                return Ok(bytes);
            }
            if ReadConsoleInputW(input as HANDLE, &mut event, 1, &mut count) == 0 {
                return Err(io::Error::last_os_error());
            }
        }
    }
}
