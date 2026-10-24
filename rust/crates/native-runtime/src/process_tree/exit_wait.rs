//! Direct-child exit notification leaves the leader unreaped for tree cleanup.
use super::*;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

#[cfg(test)]
mod tests;

#[cfg(test)]
thread_local! {
    static OBSERVATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn observed() {
    OBSERVATIONS.with(|count| count.set(count.get() + 1));
}

fn timeout() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "process exit deadline exceeded; termination may be unresolved",
    )
}

pub(super) fn wait(id: libc::id_t, deadline: Instant) -> io::Result<ExitStatus> {
    wait_metered(id, deadline, None)
}

pub(super) fn wait_metered(
    id: libc::id_t,
    deadline: Instant,
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<ExitStatus> {
    if let Some(status) = platform::observe_exit_id(id, meter)? {
        return Ok(status);
    }
    if Instant::now() >= deadline {
        return Err(timeout());
    }
    let watch = open(id, meter)?;
    // Close registration's race with exit; no wait/reap releases the PID.
    if let Some(status) = platform::observe_exit_id(id, meter)? {
        return Ok(status);
    }
    wait_ready(&watch, id, deadline)
}

pub(super) struct ExitWatch<'a> {
    fd: Option<OwnedFd>,
    meter: Option<&'a dyn crate::NativeReadMeter>,
    cleanup: Option<u64>,
}

impl AsRawFd for ExitWatch<'_> {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.fd.as_ref().map_or(-1, AsRawFd::as_raw_fd)
    }
}

impl Drop for ExitWatch<'_> {
    fn drop(&mut self) {
        if let (Some(meter), Some(cleanup)) = (self.meter, self.cleanup.take()) {
            if self.fd.is_some() {
                let _ = meter.record_cleanup_write(cleanup);
            } else {
                meter.cancel_cleanup_write(cleanup);
            }
        }
    }
}

pub(super) fn open(
    id: libc::id_t,
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<ExitWatch<'_>> {
    let watch = open_watch(id, meter)?;
    register(&watch, id)?;
    Ok(watch)
}

#[cfg(target_vendor = "apple")]
pub(super) fn open_member(
    id: libc::id_t,
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<ExitWatch<'_>> {
    let watch = open_watch(id, meter)?;
    register_event(&watch, id, libc::NOTE_EXIT)?;
    Ok(watch)
}

fn open_watch(
    id: libc::id_t,
    meter: Option<&dyn crate::NativeReadMeter>,
) -> io::Result<ExitWatch<'_>> {
    let mut watch = ExitWatch {
        fd: None,
        meter,
        cleanup: meter
            .map(crate::NativeReadMeter::reserve_cleanup_write)
            .transpose()?,
    };
    let (opened, recorded) =
        pipe_operation_attempt(meter, crate::NativeOperationKind::Read, || {
            open_descriptor_for(id)
        })?;
    watch.fd = Some(opened?);
    // Retain the descriptor and its prepaid close before late accounting can
    // fail. Every successful open has one physical release on this ledger.
    recorded?;
    Ok(watch)
}

#[cfg(target_os = "linux")]
fn open_descriptor_for(id: libc::id_t) -> io::Result<OwnedFd> {
    // SAFETY: the unreaped child owns this PID; pidfd_open sets close-on-exec.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, id, 0) };
    let fd = libc::c_int::try_from(fd).map_err(io::Error::other)?;
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful pidfd_open transferred this fresh owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(target_vendor = "apple")]
fn open_descriptor_for(_id: libc::id_t) -> io::Result<OwnedFd> {
    // SAFETY: kqueue has no arguments and creates a close-on-exec descriptor.
    let fd = unsafe { libc::kqueue() };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful kqueue transferred this fresh owned descriptor.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(target_os = "linux")]
fn register(_watch: &ExitWatch<'_>, _id: libc::id_t) -> io::Result<()> {
    Ok(())
}

#[cfg(target_vendor = "apple")]
fn register(watch: &ExitWatch<'_>, id: libc::id_t) -> io::Result<()> {
    complete_registration(
        register_event(watch, id, libc::NOTE_EXIT | libc::NOTE_EXITSTATUS)?,
        || platform::await_exiting_id(id, watch.meter).map(|_| ()),
    )
}

// Darwin removes an exiting process from registration before waitid can
// observe its zombie. ESRCH therefore cannot leave an empty queue to wait on.
// The exclusive, unreaped child ID permits a kernel terminal wait instead.
#[cfg(any(target_vendor = "apple", test))]
fn complete_registration(
    registered: bool,
    settle_exiting: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    if registered { Ok(()) } else { settle_exiting() }
}

#[cfg(target_vendor = "apple")]
fn register_event(watch: &ExitWatch<'_>, id: libc::id_t, flags: u32) -> io::Result<bool> {
    let change = libc::kevent {
        ident: libc::uintptr_t::try_from(id).map_err(io::Error::other)?,
        filter: libc::EVFILT_PROC,
        flags: libc::EV_ADD | libc::EV_ONESHOT,
        fflags: flags,
        data: 0,
        udata: std::ptr::null_mut(),
    };
    process_control_attempt(watch.meter, crate::NativeOperationKind::Read, || {
        // SAFETY: one initialized change, a live queue and no output event.
        let result = unsafe {
            libc::kevent(
                watch.as_raw_fd(),
                &raw const change,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if result != 0 {
            let cause = io::Error::last_os_error();
            if cause.raw_os_error() != Some(libc::ESRCH) {
                return Err(cause);
            }
            return Ok(false);
        }
        Ok(true)
    })
}

#[cfg(target_os = "linux")]
fn wait_ready(watch: &ExitWatch<'_>, id: libc::id_t, deadline: Instant) -> io::Result<ExitStatus> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return platform::observe_exit_id(id, watch.meter)?.ok_or_else(timeout);
        }
        let mut ready = libc::pollfd {
            fd: watch.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let milliseconds =
            i32::try_from(remaining.as_nanos().div_ceil(1_000_000)).unwrap_or(i32::MAX);
        // SAFETY: one writable pollfd whose descriptor remains owned here.
        let result = process_control_attempt(watch.meter, crate::NativeOperationKind::Read, || {
            // SAFETY: the retained descriptor and writable pollfd outlive this call.
            let count = unsafe { libc::poll(&raw mut ready, 1, milliseconds) };
            if count < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(count)
            }
        });
        let result = match result {
            Ok(count) => count,
            Err(cause) if cause.kind() == io::ErrorKind::Interrupted => continue,
            Err(cause) => return Err(cause),
        };
        if result > 0 {
            if ready.revents & (libc::POLLERR | libc::POLLNVAL) != 0 {
                return Err(io::Error::other("invalid process exit notification"));
            }
            return platform::observe_exit_id(id, watch.meter)?
                .ok_or_else(|| io::Error::other("process exit notification has no status"));
        }
    }
}

#[cfg(target_vendor = "apple")]
fn wait_ready(watch: &ExitWatch<'_>, id: libc::id_t, deadline: Instant) -> io::Result<ExitStatus> {
    use std::os::unix::process::ExitStatusExt;
    match member_event(watch, id, deadline, libc::NOTE_EXIT | libc::NOTE_EXITSTATUS)? {
        Some(event) => Ok(ExitStatus::from_raw(
            event.data.try_into().map_err(io::Error::other)?,
        )),
        None => platform::observe_exit_id(id, watch.meter)?.ok_or_else(timeout),
    }
}

#[cfg(target_vendor = "apple")]
pub(super) fn wait_member(
    watch: &ExitWatch<'_>,
    id: libc::id_t,
    deadline: Instant,
) -> io::Result<bool> {
    Ok(member_event(watch, id, deadline, libc::NOTE_EXIT)?.is_some())
}

#[cfg(target_vendor = "apple")]
fn member_event(
    watch: &ExitWatch<'_>,
    id: libc::id_t,
    deadline: Instant,
    flags: u32,
) -> io::Result<Option<libc::kevent>> {
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(None);
        }
        let remaining = libc::timespec {
            tv_sec: remaining.as_secs().try_into().map_err(io::Error::other)?,
            tv_nsec: remaining.subsec_nanos().into(),
        };
        // SAFETY: all integer and pointer fields accept zero as output storage.
        let mut event: libc::kevent = unsafe { std::mem::zeroed() };
        // SAFETY: the owned queue, writable event and relative timeout live through this call.
        let result = process_control_attempt(watch.meter, crate::NativeOperationKind::Read, || {
            // SAFETY: the queue, writable event and timeout live through this call.
            let count = unsafe {
                libc::kevent(
                    watch.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    &raw mut event,
                    1,
                    &raw const remaining,
                )
            };
            if count < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(count)
            }
        });
        let result = match result {
            Ok(count) => count,
            Err(cause) if cause.kind() == io::ErrorKind::Interrupted => continue,
            Err(cause) => return Err(cause),
        };
        if result > 0 {
            if event.ident != libc::uintptr_t::try_from(id).map_err(io::Error::other)?
                || event.filter != libc::EVFILT_PROC
                || event.flags & libc::EV_ERROR != 0
                || event.fflags & flags != flags
            {
                return Err(io::Error::other("invalid process exit notification"));
            }
            return Ok(Some(event));
        }
    }
}
