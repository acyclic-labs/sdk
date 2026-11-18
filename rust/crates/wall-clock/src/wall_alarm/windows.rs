//! Native windows wall-clock timer and rearm signal.

use std::io;
use std::os::windows::io::{AsRawHandle as _, FromRawHandle as _, OwnedHandle};
use windows_sys::Win32::Foundation::{HANDLE, WAIT_FAILED};
use windows_sys::Win32::System::Threading::{
    CancelWaitableTimer, CreateEventW, CreateWaitableTimerExW, INFINITE,
    SYNCHRONIZATION_SYNCHRONIZE, SetEvent, SetWaitableTimerEx, TIMER_MODIFY_STATE,
    WaitForMultipleObjects,
};

/// 100-nanosecond intervals from 1601 to the Unix epoch.
const UNIX_EPOCH_TICKS: i64 = 116_444_736_000_000_000;

/// A retained native clock timer and its rearm signal.
pub struct Waiter {
    timer: OwnedHandle,
    rearm: OwnedHandle,
}

fn owned(handle: HANDLE) -> io::Result<OwnedHandle> {
    if handle.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful creation transfers one fresh owned handle.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
}

impl Waiter {
    /// Creates the original native wait objects.
    pub fn new() -> io::Result<Self> {
        // SAFETY: an unnamed timer with default security.
        let timer = owned(unsafe {
            CreateWaitableTimerExW(
                std::ptr::null(),
                std::ptr::null(),
                0,
                SYNCHRONIZATION_SYNCHRONIZE | TIMER_MODIFY_STATE,
            )
        })?;
        // SAFETY: an unnamed, initially clear, auto-reset event.
        let rearm = owned(unsafe { CreateEventW(std::ptr::null(), 0, 0, std::ptr::null()) })?;
        Ok(Self { timer, rearm })
    }

    /// Wakes the driver to arm its current deadline.
    pub fn rearm(&self) -> io::Result<()> {
        // SAFETY: signals an owned event.
        if unsafe { SetEvent(self.rearm.as_raw_handle()) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Waits until the deadline, a clock change or a rearm signal.
    pub fn wait(&self, deadline: Option<u64>) -> io::Result<()> {
        if let Some(deadline) = deadline {
            // A positive due time is absolute UTC; the system adjusts
            // absolute timers when its time changes.
            let due = i64::try_from(deadline)
                .ok()
                .and_then(|deadline| deadline.checked_mul(10_000))
                .and_then(|ticks| ticks.checked_add(UNIX_EPOCH_TICKS))
                .unwrap_or(i64::MAX);
            // SAFETY: an owned timer and a live due time, without a callback.
            if unsafe {
                SetWaitableTimerEx(
                    self.timer.as_raw_handle(),
                    &raw const due,
                    0,
                    None,
                    std::ptr::null(),
                    std::ptr::null(),
                    0,
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
        } else {
            // SAFETY: an owned timer; cancelling an unarmed timer is a no-op.
            unsafe { CancelWaitableTimer(self.timer.as_raw_handle()) };
        }
        let handles = [self.timer.as_raw_handle(), self.rearm.as_raw_handle()];
        // SAFETY: two owned handles that outlive the wait.
        if unsafe { WaitForMultipleObjects(2, handles.as_ptr(), 0, INFINITE) } == WAIT_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}
