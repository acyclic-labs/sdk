//! Native apple wall-clock timer and rearm signal.

use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd, RawFd};

unsafe extern "C" {
    fn notify_register_file_descriptor(
        name: *const libc::c_char,
        notify_fd: *mut libc::c_int,
        flags: libc::c_int,
        out_token: *mut libc::c_int,
    ) -> u32;
    fn notify_cancel(token: libc::c_int) -> u32;
}

const REARM: usize = 1;
const TIMER: usize = 2;

/// A retained native clock timer and its rearm signal.
pub struct Waiter {
    queue: OwnedFd,
    // libnotify retains this descriptor and closes it with the token.
    clock_set: RawFd,
    token: libc::c_int,
}

fn event(ident: usize, filter: i16, flags: u16, fflags: u32, data: isize) -> libc::kevent {
    libc::kevent {
        ident,
        filter,
        flags,
        fflags,
        data,
        udata: std::ptr::null_mut(),
    }
}

impl Waiter {
    /// Creates the original native wait objects.
    pub fn new() -> io::Result<Self> {
        // SAFETY: creates one fresh kqueue descriptor.
        let queue = unsafe { libc::kqueue() };
        if queue < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a successful call returned this fresh descriptor.
        let queue = unsafe { OwnedFd::from_raw_fd(queue) };
        let (mut fd, mut token) = (-1, 0);
        // SAFETY: a NUL-terminated name and two live out-parameters.
        let status = unsafe {
            notify_register_file_descriptor(
                c"com.apple.system.clock_set".as_ptr(),
                &raw mut fd,
                0,
                &raw mut token,
            )
        };
        if status != 0 || fd < 0 {
            return Err(io::Error::other(format!(
                "clock-change notification registration failed: {status}"
            )));
        }
        let waiter = Self {
            queue,
            clock_set: fd,
            token,
        };
        // SAFETY: an owned descriptor; only its status flags change.
        if unsafe { libc::fcntl(waiter.clock_set, libc::F_SETFL, libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        waiter.change(&[
            event(
                REARM,
                libc::EVFILT_USER,
                libc::EV_ADD | libc::EV_CLEAR,
                0,
                0,
            ),
            event(
                usize::try_from(waiter.clock_set).unwrap_or(usize::MAX),
                libc::EVFILT_READ,
                libc::EV_ADD,
                0,
                0,
            ),
        ])?;
        Ok(waiter)
    }

    fn change(&self, changes: &[libc::kevent]) -> io::Result<()> {
        let count = libc::c_int::try_from(changes.len()).unwrap_or(libc::c_int::MAX);
        // SAFETY: a live change list and no event list on an owned kqueue.
        if unsafe {
            libc::kevent(
                self.queue.as_raw_fd(),
                changes.as_ptr(),
                count,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Wakes the driver to arm its current deadline.
    pub fn rearm(&self) -> io::Result<()> {
        self.change(&[event(REARM, libc::EVFILT_USER, 0, libc::NOTE_TRIGGER, 0)])
    }

    fn arm(&self, deadline: Option<u64>) -> io::Result<()> {
        match deadline {
            Some(deadline) => {
                // XNU forbids changing an existing timer between absolute
                // and relative modes. Use one relative representation for
                // every deadline, chunked to fit its nanosecond conversion.
                // Clock-set notifications re-read the wall-clock deadline;
                // continuous time also includes time spent asleep.
                let milliseconds = deadline
                    .saturating_sub(super::now_unix_ms())
                    .min((u64::MAX / 2) / 1_000_000);
                self.change(&[event(
                    TIMER,
                    libc::EVFILT_TIMER,
                    libc::EV_ADD | libc::EV_ONESHOT,
                    libc::NOTE_MACH_CONTINUOUS_TIME,
                    isize::try_from(milliseconds).unwrap_or(isize::MAX),
                )])?;
            }
            None => {
                // Absent when it already fired or was never armed.
                let _ = self.change(&[event(TIMER, libc::EVFILT_TIMER, libc::EV_DELETE, 0, 0)]);
            }
        }
        Ok(())
    }

    /// Waits until the deadline, a clock change or a rearm signal.
    pub fn wait(&self, deadline: Option<u64>) -> io::Result<()> {
        self.arm(deadline)?;
        let mut ready = [event(0, 0, 0, 0, 0)];
        // SAFETY: one live event slot on an owned kqueue; no timeout.
        if unsafe {
            libc::kevent(
                self.queue.as_raw_fd(),
                std::ptr::null(),
                0,
                ready.as_mut_ptr(),
                1,
                std::ptr::null(),
            )
        } < 0
        {
            let error = io::Error::last_os_error();
            return if error.kind() == io::ErrorKind::Interrupted {
                Ok(())
            } else {
                Err(error)
            };
        }
        let mut tokens = [0_u8; 64];
        // SAFETY: drains posted tokens into a live buffer.
        while unsafe { libc::read(self.clock_set, tokens.as_mut_ptr().cast(), tokens.len()) } > 0 {}
        Ok(())
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        // SAFETY: cancels this waiter's own registration once.
        unsafe { notify_cancel(self.token) };
    }
}

#[test]
fn native_timer_accepts_deadlines_across_its_calendar_range() -> io::Result<()> {
    let waiter = Waiter::new()
        .map_err(|error| io::Error::new(error.kind(), format!("create wall alarm: {error}")))?;
    let calendar_end = u64::MAX / 1_000_000;
    for deadline in [
        0,
        1,
        calendar_end - 1,
        calendar_end,
        calendar_end + 1,
        u64::MAX,
        1,
        u64::MAX,
    ] {
        waiter.arm(Some(deadline)).map_err(|error| {
            io::Error::new(error.kind(), format!("arm deadline {deadline}: {error}"))
        })?;
    }
    waiter.arm(None)
}
