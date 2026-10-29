//! macOS readiness: one kqueue with one-shot pipe filters, `EVFILT_PROC`
//! `NOTE_EXIT` for each awaited child, and an `EVFILT_USER` wake.

use super::Source;
use std::io;
use std::os::fd::{AsRawFd as _, FromRawFd as _, OwnedFd};
use std::time::Duration;

const WAKE: usize = 0;

pub(super) struct Poller {
    queue: OwnedFd,
}

/// The source remains borrowed until its one-shot filter is removed.
pub(super) struct Registration {
    queue: std::os::fd::RawFd,
    event: Option<libc::kevent>,
}

impl Registration {
    pub(super) fn disarm(&mut self) -> io::Result<()> {
        let Some(mut event) = self.event.take() else {
            return Ok(());
        };
        event.flags = libc::EV_DELETE;
        loop {
            // SAFETY: the hub and source outlive this registration.
            if unsafe {
                libc::kevent(
                    self.queue,
                    &raw const event,
                    1,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                )
            } == 0
            {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            match error.kind() {
                io::ErrorKind::Interrupted => {}
                _ if error.raw_os_error() == Some(libc::ENOENT) => return Ok(()),
                _ => return Err(error),
            }
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        let _ = self.disarm();
    }
}

// SAFETY: udata carries only a numeric token; no pointer is dereferenced.
unsafe impl Send for Registration {}
// SAFETY: only exclusive Drop accesses the registration.
unsafe impl Sync for Registration {}

fn change(ident: usize, filter: i16, flags: u16, fflags: u32, token: u64) -> libc::kevent {
    libc::kevent {
        ident,
        filter,
        flags,
        fflags,
        data: 0,
        udata: std::ptr::without_provenance_mut(usize::try_from(token).unwrap_or(usize::MAX)),
    }
}

/// Whether this unreaped child has exited, without reaping it.
fn exited(pid: usize) -> io::Result<bool> {
    let id = libc::id_t::try_from(pid).map_err(io::Error::other)?;
    loop {
        // SAFETY: plain fields; all-zero is a valid output value.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: writable status storage owned by this frame; WNOWAIT leaves
        // the child unreaped for its owner.
        if unsafe {
            libc::waitid(
                libc::P_PID,
                id,
                &raw mut info,
                libc::WEXITED | libc::WNOWAIT | libc::WNOHANG,
            )
        } == 0
        {
            // SAFETY: a successful WEXITED observation initialized the union.
            return Ok(unsafe { info.si_pid() } != 0);
        }
        let error = io::Error::last_os_error();
        match error.kind() {
            io::ErrorKind::Interrupted => {}
            // Not this process's child (or already reaped): it has no exit
            // left to await.
            _ if error.raw_os_error() == Some(libc::ECHILD) => return Ok(true),
            _ => return Err(error),
        }
    }
}

impl Poller {
    pub(super) fn new() -> io::Result<Self> {
        // SAFETY: kqueue has no preconditions and returns a new fd.
        let queue = unsafe { libc::kqueue() };
        if queue < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a successful kqueue call returns one owned fd.
        let queue = unsafe { OwnedFd::from_raw_fd(queue) };
        let poller = Self { queue };
        poller.apply(&change(
            WAKE,
            libc::EVFILT_USER,
            libc::EV_ADD | libc::EV_CLEAR,
            0,
            0,
        ))?;
        Ok(poller)
    }

    fn apply(&self, change: &libc::kevent) -> io::Result<()> {
        // SAFETY: one initialized change and no event output.
        if unsafe {
            libc::kevent(
                self.queue.as_raw_fd(),
                change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        } == 0
        {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    pub(super) fn register(
        &self,
        source: Source<'_>,
        token: u64,
    ) -> io::Result<Option<Registration>> {
        let flags = libc::EV_ADD | libc::EV_ONESHOT;
        let event = match source {
            Source::Readable(fd) => change(
                usize::try_from(fd.as_raw_fd()).map_err(io::Error::other)?,
                libc::EVFILT_READ,
                flags,
                0,
                token,
            ),
            Source::Writable(fd) => change(
                usize::try_from(fd.as_raw_fd()).map_err(io::Error::other)?,
                libc::EVFILT_WRITE,
                flags,
                0,
                token,
            ),
            Source::Exit(pid, _) => change(
                usize::try_from(pid).map_err(io::Error::other)?,
                libc::EVFILT_PROC,
                flags,
                libc::NOTE_EXIT,
                token,
            ),
        };
        match self.apply(&event) {
            // XNU attaches NOTE_EXIT to a process that has already become a
            // zombie without ever firing it, so recheck after attaching.
            Ok(()) => {
                let mut registration = Registration {
                    queue: self.queue.as_raw_fd(),
                    event: Some(event),
                };
                if matches!(source, Source::Exit(..)) && exited(event.ident)? {
                    registration.disarm()?;
                    Ok(None)
                } else {
                    Ok(Some(registration))
                }
            }
            // The process already exited: the waiter observes it now.
            Err(error)
                if matches!(source, Source::Exit(..))
                    && error.raw_os_error() == Some(libc::ESRCH) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn notify(&self) -> io::Result<()> {
        self.apply(&change(WAKE, libc::EVFILT_USER, 0, libc::NOTE_TRIGGER, 0))
    }

    pub(super) fn wait(&self, timeout: Option<Duration>, ready: &mut Vec<u64>) -> io::Result<()> {
        let timeout = timeout.map(|timeout| libc::timespec {
            tv_sec: libc::time_t::try_from(timeout.as_secs()).unwrap_or(libc::time_t::MAX),
            tv_nsec: libc::c_long::from(timeout.subsec_nanos()),
        });
        // SAFETY: plain fields; all-zero is a valid output value.
        let mut events: [libc::kevent; 256] = unsafe { std::mem::zeroed() };
        // SAFETY: no changes, the initialized output array and an optional
        // timeout that outlives the call.
        let count = unsafe {
            libc::kevent(
                self.queue.as_raw_fd(),
                std::ptr::null(),
                0,
                events.as_mut_ptr(),
                libc::c_int::try_from(events.len()).unwrap_or(libc::c_int::MAX),
                timeout
                    .as_ref()
                    .map_or(std::ptr::null(), std::ptr::from_ref),
            )
        };
        if count < 0 {
            let error = io::Error::last_os_error();
            return if error.kind() == io::ErrorKind::Interrupted {
                Ok(())
            } else {
                Err(error)
            };
        }
        for event in events.iter().take(usize::try_from(count).unwrap_or(0)) {
            if event.flags & libc::EV_ERROR != 0 && event.data != 0 {
                return Err(io::Error::from_raw_os_error(
                    i32::try_from(event.data).map_err(io::Error::other)?,
                ));
            }
            if event.filter != libc::EVFILT_USER {
                ready.push(event.udata.addr() as u64);
            }
        }
        Ok(())
    }
}
