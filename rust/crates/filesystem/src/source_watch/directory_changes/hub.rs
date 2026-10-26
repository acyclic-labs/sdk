//! One completion port and one thread for every Windows source watch.
//!
//! Each watch's directory handle is associated with the port under the
//! address of the watch's shared state. Every packet on the port, a read's
//! completion or a wake, owns one strong count of that state, taken before
//! the read or wake was issued. The state therefore outlives every packet
//! that names it, and the thread never touches a packet's `OVERLAPPED`.
use super::*;
use std::sync::OnceLock;
use windows::Win32::Foundation::{ERROR_NOT_ENOUGH_MEMORY, INVALID_HANDLE_VALUE};
use windows::Win32::System::IO::{
    CreateIoCompletionPort, GetQueuedCompletionStatusEx, OVERLAPPED_ENTRY,
    PostQueuedCompletionStatus,
};
use windows::Win32::System::Threading::INFINITE;

std::thread_local! {
    /// Whether this thread is the port's.
    static SERVING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Whether the caller runs on the port's thread, which collects every
/// completion and so must never wait for one.
pub(super) fn serving() -> bool {
    SERVING.with(std::cell::Cell::get)
}

/// Packets the thread removes from the port per wait.
const BATCH: usize = 64;

struct Port(HANDLE);
// SAFETY: a completion port handle may be used from any thread.
unsafe impl Send for Port {}
// SAFETY: as above; the handle is never closed once the thread serves it.
unsafe impl Sync for Port {}

/// The process's port, created with its thread on first use.
static PORT: OnceLock<Result<Port, i32>> = OnceLock::new();

fn port() -> Result<HANDLE, DemandError> {
    match PORT.get_or_init(start) {
        Ok(port) => Ok(port.0),
        Err(code) => Err(io::Error::from_raw_os_error(*code).into()),
    }
}

fn start() -> Result<Port, i32> {
    // SAFETY: creates a port with no file associated yet.
    let port = unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, None, 0, 1) }
        .map_err(|error| error.code().0)?;
    let raw = port.0 as usize;
    std::thread::Builder::new()
        .name("acyclic-source-watch".to_owned())
        .spawn(move || serve(HANDLE(raw as *mut std::ffi::c_void)))
        .map(|_| Port(port))
        .map_err(|error| {
            // SAFETY: no thread serves this port, so this is its only owner.
            let _ = unsafe { CloseHandle(port) };
            error
                .raw_os_error()
                .unwrap_or(ERROR_NOT_ENOUGH_MEMORY.to_hresult().0)
        })
}

/// Makes sure the port and its thread exist, before a watch relies on them.
pub(super) fn ensure() -> Result<(), DemandError> {
    port().map(|_| ())
}

fn key(shared: &Shared) -> usize {
    std::ptr::from_ref(shared) as usize
}

/// Routes `directory`'s read completions to the port, keyed by `shared`.
pub(super) fn attach(shared: &Shared, directory: HANDLE) -> Result<(), DemandError> {
    let port = port()?;
    // SAFETY: the port and the open directory handle are both live.
    unsafe { CreateIoCompletionPort(directory, Some(port), key(shared), 0) }
        .map_err(io::Error::from)?;
    Ok(())
}

/// Takes the strong count that the packet of the read or wake about to be
/// issued owns.
pub(super) fn retain_for_packet(shared: &Shared) {
    // SAFETY: every `Shared` lives in an `Arc`, which the caller keeps.
    unsafe { Arc::increment_strong_count(std::ptr::from_ref(shared)) };
}

/// Returns the count `retain_for_packet` took, for a read or wake that
/// failed and so queued no packet.
pub(super) fn release_unqueued(shared: &Shared) {
    // SAFETY: the count taken above, which no packet owns; the caller's own
    // `Arc` keeps the state alive.
    unsafe { Arc::decrement_strong_count(std::ptr::from_ref(shared)) };
}

/// Queues a wake that has the thread serve `shared`.
pub(super) fn wake(shared: &Shared) -> Result<(), DemandError> {
    let port = port()?;
    retain_for_packet(shared);
    // SAFETY: a packet without an `OVERLAPPED`; it owns the count just taken.
    if let Err(error) = unsafe { PostQueuedCompletionStatus(port, 0, key(shared), None) } {
        release_unqueued(shared);
        return Err(io::Error::from(error).into());
    }
    Ok(())
}

fn serve(port: HANDLE) {
    SERVING.with(|serving| serving.set(true));
    let mut entries = [OVERLAPPED_ENTRY::default(); BATCH];
    loop {
        let mut removed = 0;
        // SAFETY: the kernel writes at most `entries.len()` entries.
        let waited = unsafe {
            GetQueuedCompletionStatusEx(port, &mut entries, &mut removed, INFINITE, false)
        };
        if waited.is_err() {
            continue;
        }
        for entry in entries.iter().take(removed as usize) {
            if entry.lpCompletionKey == 0 {
                continue;
            }
            // SAFETY: each packet owns one strong count of the state its key
            // names, taken by `retain_for_packet`.
            let shared = unsafe { Arc::from_raw(entry.lpCompletionKey as *const Shared) };
            // Removing the packet completed its read; waiters may go on.
            shared.collected();
            Shared::serve(shared);
        }
    }
}
