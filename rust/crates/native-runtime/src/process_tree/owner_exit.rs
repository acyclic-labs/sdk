//! A process-lifetime event that no filesystem request can hold open.

use std::io;
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::{Mutex, OnceLock};

pub(super) fn event() -> io::Result<&'static OwnedFd> {
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
        .name("acyclic-process-owner".into())
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
                        "thread pidfd required for owner death: {}",
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
        .map_err(|_| io::Error::other("process owner thread exited during admission"))??;
    // Process lifetime owns these two handles, once per owner, never per view.
    static LIFETIME: OnceLock<(std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>)> =
        OnceLock::new();
    let _ = LIFETIME.set((keep, thread));
    Ok(OWNER.get_or_init(|| descriptor))
}
