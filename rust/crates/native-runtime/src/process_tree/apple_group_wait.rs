//! An unreaped leader reserves the group while member exit events settle it.
use super::super::{exit_wait, process_control_attempt};
use crate::{NativeOperationKind, NativeReadMeter};
use std::io;

pub(super) fn wait(group: libc::pid_t, meter: Option<&dyn NativeReadMeter>) -> io::Result<bool> {
    let group = u32::try_from(group).map_err(io::Error::other)?;
    loop {
        let Some(member) = live_member(group, meter)? else {
            return Ok(true);
        };
        let id = libc::id_t::try_from(member.pid).map_err(io::Error::other)?;
        let watch = exit_wait::open_member(id, meter)?;
        // Registration can race exit and PID reuse. Only wait when both the
        // group and the full creation identity still describe this member.
        if identity(member.pid, group, meter)? != Some(member) {
            continue;
        }
        exit_wait::wait_member(&watch, id)?;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Member {
    pid: libc::pid_t,
    seconds: u64,
    microseconds: u64,
}

fn identity(
    pid: libc::pid_t,
    group: u32,
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<Option<Member>> {
    if let Some(meter) = meter {
        meter.examine(1, 0)?;
    }
    // SAFETY: proc_bsdinfo is a plain C output structure with integer fields.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = i32::try_from(std::mem::size_of_val(&info)).map_err(io::Error::other)?;
    let result = process_control_attempt(meter, NativeOperationKind::Read, || {
        // SAFETY: the correctly sized writable structure lives through the call.
        let count = unsafe {
            libc::proc_pidinfo(pid, libc::PROC_PIDTBSDINFO, 0, (&raw mut info).cast(), size)
        };
        if count <= 0 {
            Err(io::Error::last_os_error())
        } else if count != size {
            Err(io::Error::other("incomplete process group member identity"))
        } else {
            Ok(())
        }
    });
    match result {
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => return Ok(None),
        result => result?,
    }
    // Darwin sys/proc.h defines SZOMB as 5. Zombies need no exit notification.
    if info.pbi_pgid != group || info.pbi_status == 5 {
        return Ok(None);
    }
    Ok(Some(Member {
        pid,
        seconds: info.pbi_start_tvsec,
        microseconds: info.pbi_start_tvusec,
    }))
}

fn live_member(group: u32, meter: Option<&dyn NativeReadMeter>) -> io::Result<Option<Member>> {
    let mut minimum = 1;
    loop {
        let hint = list(group, std::ptr::null_mut(), 0, meter)?;
        let width = std::mem::size_of::<libc::pid_t>();
        let count = hint
            .div_ceil(width)
            .checked_add(1)
            .ok_or(io::ErrorKind::OutOfMemory)?;
        let mut members = Members::new(count.max(minimum), meter)?;
        let bytes = members
            .ids
            .len()
            .checked_mul(width)
            .ok_or(io::ErrorKind::OutOfMemory)?;
        let filled = list(group, members.ids.as_mut_ptr().cast(), bytes, meter)?;
        if filled > bytes || filled % width != 0 {
            return Err(io::Error::other("invalid process group snapshot size"));
        }
        for pid in members
            .ids
            .iter()
            .take(filled / width)
            .copied()
            .filter(|pid| *pid > 0)
        {
            if let Some(member) = identity(pid, group, meter)? {
                return Ok(Some(member));
            }
        }
        if filled < bytes {
            return Ok(None);
        }
        // A full prefix cannot prove emptiness. Grow on actual truncation,
        // under the original allocation ledger.
        minimum = members
            .ids
            .len()
            .checked_add(1)
            .ok_or(io::ErrorKind::OutOfMemory)?;
    }
}

fn list(
    group: u32,
    buffer: *mut libc::c_void,
    bytes: usize,
    meter: Option<&dyn NativeReadMeter>,
) -> io::Result<usize> {
    let size = i32::try_from(bytes).map_err(io::Error::other)?;
    process_control_attempt(meter, NativeOperationKind::Read, || {
        // SAFETY: the caller supplies a null sizing query or a live writable
        // buffer of exactly size bytes. The errno slot is thread-local; clear
        // and read it around this call because zero can also mean failure.
        let (count, code) = unsafe {
            *libc::__error() = 0;
            let count = libc::proc_listpids(2, group, buffer, size);
            (count, *libc::__error())
        };
        if count < 0 || (count == 0 && code != 0) {
            Err(io::Error::from_raw_os_error(code))
        } else {
            usize::try_from(count).map_err(io::Error::other)
        }
    })
}

struct Members<'a> {
    ids: Vec<libc::pid_t>,
    meter: Option<&'a dyn NativeReadMeter>,
    retained: u64,
}

impl<'a> Members<'a> {
    fn new(count: usize, meter: Option<&'a dyn NativeReadMeter>) -> io::Result<Self> {
        let bytes = count
            .checked_mul(std::mem::size_of::<libc::pid_t>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(io::ErrorKind::OutOfMemory)?;
        if let Some(meter) = meter {
            meter.allocate(bytes)?;
        }
        let mut members = Self {
            ids: Vec::new(),
            meter,
            retained: bytes,
        };
        members
            .ids
            .try_reserve_exact(count)
            .map_err(|error| match meter {
                Some(meter) => meter.allocation_failed(error),
                None => io::Error::other(error),
            })?;
        let actual = u64::try_from(
            members
                .ids
                .capacity()
                .checked_mul(std::mem::size_of::<libc::pid_t>())
                .ok_or(io::ErrorKind::OutOfMemory)?,
        )
        .map_err(io::Error::other)?;
        if let Some(meter) = meter {
            if actual > bytes {
                let (retained, recorded) = meter.record_completed_adoption(actual - bytes);
                members.retained += retained;
                recorded?;
            } else {
                meter.release(bytes - actual);
                members.retained = actual;
            }
        }
        members.ids.resize(count, 0);
        Ok(members)
    }
}

impl Drop for Members<'_> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.ids));
        if let Some(meter) = self.meter {
            meter.release(self.retained);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn member_storage_is_funded_until_the_buffer_is_freed() -> io::Result<()> {
        let meter = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
        let members = Members::new(32, Some(&meter))?;
        assert!(meter.memory.load(Ordering::Acquire) >= 128);
        drop(members);
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        meter.deny_allocations.store(true, Ordering::Release);
        assert!(Members::new(32, Some(&meter)).is_err());
        assert_eq!(meter.memory.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[test]
    fn a_process_outside_the_owned_group_is_not_a_wait_target() -> io::Result<()> {
        let pid = libc::pid_t::try_from(std::process::id()).map_err(io::Error::other)?;
        assert!(identity(pid, u32::MAX, None)?.is_none());
        Ok(())
    }

    #[test]
    fn a_failed_group_query_cannot_prove_the_group_empty() -> io::Result<()> {
        // SAFETY: getpgrp has no arguments or borrowed memory.
        let group = u32::try_from(unsafe { libc::getpgrp() }).map_err(io::Error::other)?;
        let mut byte = 0_u8;
        // A real libproc query with less than one PID of storage fails with
        // ENOMEM and returns zero. Zero alone is not an emptiness witness.
        let result = list(group, (&raw mut byte).cast(), 1, None);
        eprintln!("APPLE_GROUP_QUERY_FAILURE result={result:?}");
        let error = result.expect_err("a failed process query must never prove the group empty");
        assert_eq!(error.raw_os_error(), Some(libc::ENOMEM));
        Ok(())
    }
}
