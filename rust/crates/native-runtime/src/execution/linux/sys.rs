//! C-library syscall adapter for the shared final-child entry. Errors remain
//! errno values until the original launch owner returns to the host.

#![allow(
    unsafe_code,
    reason = "native final-child syscalls over already-borrowed policy buffers"
)]

use super::entry::{Capabilities, CapabilityHeader, Program};

pub(super) use libc::{EINVAL, EPERM};

fn status(result: libc::c_long) -> Result<(), i32> {
    if result < 0 {
        Err(std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO))
    } else {
        Ok(())
    }
}

pub(super) fn seal_descriptors() -> Result<(), i32> {
    // SAFETY: CLOEXEC changes only descriptor flags, including the exec pipe.
    status(unsafe { libc::syscall(libc::SYS_close_range, 3_u32, u32::MAX, 4_u32) })
}

pub(super) fn computation_stdio(fd: i32) -> Result<bool, i32> {
    // SAFETY: all-zero stat is valid output storage for fstat.
    let mut metadata: libc::stat = unsafe { core::mem::zeroed() };
    // SAFETY: fstat accepts any descriptor number and initializes its output.
    status(unsafe { libc::fstat(fd, &raw mut metadata) }.into())?;
    let kind = metadata.st_mode & libc::S_IFMT;
    Ok(kind == libc::S_IFIFO
        || (kind == libc::S_IFCHR
            && libc::major(metadata.st_rdev) == 1
            && libc::minor(metadata.st_rdev) == 3))
}

pub(super) fn clear_ambient_capabilities() -> Result<(), i32> {
    // SAFETY: PR_CAP_AMBIENT_CLEAR_ALL has no pointer arguments.
    status(unsafe { libc::prctl(libc::PR_CAP_AMBIENT, 4, 0, 0, 0) }.into())
}

pub(super) fn capset(
    header: &CapabilityHeader,
    capabilities: &[Capabilities; 2],
) -> Result<(), i32> {
    // SAFETY: capability version 3 uses exactly these two initialized words.
    status(unsafe { libc::syscall(libc::SYS_capset, header, capabilities.as_ptr()) })
}

pub(super) fn no_new_privileges() -> Result<(), i32> {
    // SAFETY: irreversible attenuation of this child, with no pointer arguments.
    status(unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) }.into())
}

pub(super) fn restrict_filesystem(ruleset: i32) -> Result<(), i32> {
    // SAFETY: the launch retains this owned Landlock ruleset until exec.
    status(unsafe { libc::syscall(libc::SYS_landlock_restrict_self, ruleset, 0) })
}

pub(super) fn restrict_syscalls(program: &Program) -> Result<(), i32> {
    // SAFETY: enter borrows the complete instruction buffer for this call.
    status(unsafe { libc::syscall(libc::SYS_seccomp, 1, 0, program) })
}
