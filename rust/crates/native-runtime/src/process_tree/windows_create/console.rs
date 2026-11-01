//! The one pseudoconsole every tree's first member is attached to.
//!
//! A console program started without a console gets a console host of its
//! own (about 10 ms and one `conhost.exe` per child, or a window when it is
//! detached and starts a console grandchild). This owner instead creates one
//! pseudoconsole, the first time it starts a tree, and attaches every child
//! to it. Its host has no window, so neither a child nor any console
//! descendant that inherits the console shows one.
//!
//! What holds for children sharing it:
//!
//! - **Streams.** A child's standard handles are the ones its command gave
//!   it (pipes, files, the null device), never the console, so its captured
//!   output is exactly what it wrote. Console state (modes, title, cursor,
//!   screen contents) lives in the host and changes no pipe's bytes.
//! - **Input.** Nothing is ever typed into the console: this process holds
//!   the input pipe's write end and never writes to it, so a child that
//!   reads the console reads no other tree's input. Such a read waits until
//!   the child's Job ends it, exactly as it would in the hidden console of a
//!   `CREATE_NO_WINDOW` child, whose input nothing writes either. Closing
//!   that write end instead is not an option: the host then sends every
//!   attached process a close event, and each exits with
//!   `STATUS_CONTROL_C_EXIT` at once. Programs that read input read their
//!   standard input, which is whatever the command gave them.
//! - **Output.** What children write to the console itself is rendered by
//!   the host and read and discarded by one thread for the life of this
//!   process, so console writes never block on a full pipe.
//! - **Lifetime.** The console lives until this process exits; its host
//!   then exits too. Ending a tree is always its Job's kill, never the
//!   console's.
use std::io;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use std::sync::Mutex;
use windows_sys::Win32::Storage::FileSystem::ReadFile;
use windows_sys::Win32::System::Console::{COORD, ClosePseudoConsole, CreatePseudoConsole, HPCON};
use windows_sys::Win32::System::Pipes::CreatePipe;

/// This process's console, once created, and the input write end that keeps
/// it open. Neither is ever closed: the host exits when this process does.
static CONSOLE: Mutex<Option<(Console, OwnedHandle)>> = Mutex::new(None);

struct Console(HPCON);

impl Drop for Console {
    fn drop(&mut self) {
        // SAFETY: this guard owns the successfully created pseudoconsole.
        unsafe { ClosePseudoConsole(self.0) };
    }
}

fn anonymous_pipe() -> io::Result<(OwnedHandle, OwnedHandle)> {
    let mut read = std::ptr::null_mut();
    let mut write = std::ptr::null_mut();
    // SAFETY: two outputs and no borrowed security attributes; neither end
    // is inheritable.
    if unsafe { CreatePipe(&raw mut read, &raw mut write, std::ptr::null(), 0) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful creation returns two new owned handles.
    Ok(unsafe {
        (
            OwnedHandle::from_raw_handle(read),
            OwnedHandle::from_raw_handle(write),
        )
    })
}

/// This process's console, created by the first call.
pub(super) fn shared() -> io::Result<HPCON> {
    let mut console = CONSOLE
        .lock()
        .map_err(|_| io::Error::other("the shared console lock is poisoned"))?;
    if let Some((console, _)) = console.as_ref() {
        return Ok(console.0);
    }
    let (input, typed) = anonymous_pipe()?;
    let (rendered, output) = anonymous_pipe()?;
    let mut created: HPCON = 0;
    // SAFETY: live pipe ends, which the host duplicates, and one output.
    let result = unsafe {
        CreatePseudoConsole(
            COORD { X: 80, Y: 25 },
            input.as_raw_handle(),
            output.as_raw_handle(),
            0,
            &raw mut created,
        )
    };
    if result < 0 {
        return Err(io::Error::from_raw_os_error(result));
    }
    let created = Console(created);
    // The host holds its own ends of both pipes.
    drop((input, output));
    std::thread::Builder::new()
        .name("acyclic-console".to_owned())
        .spawn(move || {
            let mut buffer = [0_u8; 4096];
            loop {
                let mut read = 0;
                // SAFETY: the owned pipe end and a live buffer of this size.
                let ok = unsafe {
                    ReadFile(
                        rendered.as_raw_handle(),
                        buffer.as_mut_ptr(),
                        4096,
                        &raw mut read,
                        std::ptr::null_mut(),
                    )
                };
                if ok == 0 || read == 0 {
                    return;
                }
            }
        })?;
    let handle = created.0;
    *console = Some((created, typed));
    Ok(handle)
}
