//! Process-wide terminal device I/O counts at the device layer, read by
//! real-terminal measurements: every write system call or console write
//! (including attempts a full queue refused) and every readiness wait.
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

static WRITES: AtomicU64 = AtomicU64::new(0);
static WAITS: AtomicU64 = AtomicU64::new(0);

/// Device calls since the process started.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeviceIo {
    /// Write calls into the kernel or console, refused attempts included.
    pub writes: u64,
    /// Waits for a full output queue to accept more.
    pub waits: u64,
}

/// Current device call counts.
pub fn device_io() -> DeviceIo {
    DeviceIo {
        writes: WRITES.load(Relaxed),
        waits: WAITS.load(Relaxed),
    }
}

pub(crate) fn wrote() {
    WRITES.fetch_add(1, Relaxed);
}

#[cfg(unix)]
pub(crate) fn waited() {
    WAITS.fetch_add(1, Relaxed);
}
