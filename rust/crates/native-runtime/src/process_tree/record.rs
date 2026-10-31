//! What a custody record names: its warden's PID, the boot, and the
//! warden's start. Wardens write these and fences compare them, so both use
//! this one implementation; on Linux it also runs in the runner, without the
//! standard library.

use super::sys::{self, Errno, Pid};
use core::ffi::CStr;

/// Bytes of boot identity a record holds.
pub(super) const BOOT_BYTES: usize = 64;

/// Bytes of process start identity a record holds.
pub(super) const START_BYTES: usize = 32;

/// Formats `pid` as the NUL-terminated decimal name of its record.
pub(super) fn name(pid: Pid, buffer: &mut [u8; 24]) -> Option<&CStr> {
    let mut value = u32::try_from(pid).ok()?;
    let mut digits = [0_u8; 10];
    let mut count = 0;
    for digit in &mut digits {
        *digit = b'0' + u8::try_from(value % 10).ok()?;
        value /= 10;
        count += 1;
        if value == 0 {
            break;
        }
    }
    let (name, _) = buffer.split_at_mut(count + 1);
    for (slot, digit) in name.iter_mut().zip(digits.iter().take(count).rev()) {
        *slot = *digit;
    }
    *name.last_mut()? = 0;
    CStr::from_bytes_with_nul(name).ok()
}

/// Reads `file` until `buffer` is full or the file ends.
#[cfg(target_os = "linux")]
pub(super) fn read_full(file: sys::Fd, buffer: &mut [u8]) -> Result<usize, Errno> {
    let mut filled = 0;
    while let Some(rest) = buffer.get_mut(filled..).filter(|rest| !rest.is_empty()) {
        match sys::read(file, rest) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(Errno(sys::EINTR)) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(filled)
}

/// Reads the whole of the file at `path` into `buffer`, up to its size.
#[cfg(target_os = "linux")]
fn read_path(path: &CStr, buffer: &mut [u8]) -> Result<usize, Errno> {
    let file = sys::open(path, sys::O_RDONLY | sys::O_CLOEXEC)?;
    let read = read_full(file, buffer);
    sys::close(file);
    read
}

/// The identity of the current boot, so a record never outlives its boot.
pub(super) fn boot_identity(buffer: &mut [u8; BOOT_BYTES]) -> Result<usize, Errno> {
    #[cfg(target_os = "linux")]
    {
        read_path(c"/proc/sys/kernel/random/boot_id", buffer)
    }
    #[cfg(target_vendor = "apple")]
    {
        let mut names = [libc::CTL_KERN, libc::KERN_BOOTTIME];
        // SAFETY: plain integer fields; all-zero is a valid value.
        let mut boot: libc::timeval = unsafe { core::mem::zeroed() };
        let mut size = core::mem::size_of::<libc::timeval>();
        // SAFETY: a two-name MIB and writable storage of the stated size.
        if unsafe {
            libc::sysctl(
                names.as_mut_ptr(),
                2,
                (&raw mut boot).cast(),
                &raw mut size,
                core::ptr::null_mut(),
                0,
            )
        } != 0
        {
            return Err(Errno(
                std::io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO),
            ));
        }
        Ok(copy(
            buffer,
            &[&boot.tv_sec.to_le_bytes(), &boot.tv_usec.to_le_bytes()],
        ))
    }
}

/// Copies `parts` into the start of `buffer`, returning the length copied.
fn copy(buffer: &mut [u8], parts: &[&[u8]]) -> usize {
    let mut written = 0;
    for (slot, byte) in buffer
        .iter_mut()
        .zip(parts.iter().flat_map(|part| part.iter()))
    {
        *slot = *byte;
        written += 1;
    }
    written
}

/// The start identity of process `pid`, zombie or not. No later process that
/// reuses the PID shares it. `ENOENT` when no such process exists.
pub(super) fn start_identity(pid: Pid, buffer: &mut [u8; START_BYTES]) -> Result<usize, Errno> {
    #[cfg(target_os = "linux")]
    {
        let mut digits = [0; 24];
        let digits = name(pid, &mut digits).ok_or(Errno(sys::EINVAL))?.to_bytes();
        let mut path = [0_u8; 40];
        let length = copy(&mut path, &[b"/proc/", digits, b"/stat\0"]);
        let path = CStr::from_bytes_with_nul(path.get(..length).unwrap_or_default())
            .map_err(|_| Errno(sys::EINVAL))?;
        let mut stat = [0_u8; 1024];
        let read = read_path(path, &mut stat)?;
        let stat = stat.get(..read).unwrap_or_default();
        // After the parenthesized command name: field 3 (state) onwards;
        // field 22 is the start time in clock ticks since boot.
        let field = stat
            .iter()
            .rposition(|byte| *byte == b')')
            .and_then(|end| stat.get(end + 1..))
            .and_then(|after| {
                after
                    .split(|byte| *byte == b' ')
                    .filter(|field| !field.is_empty())
                    .nth(19)
            })
            .ok_or(Errno(sys::EINVAL))?;
        Ok(copy(buffer, &[field]))
    }
    #[cfg(target_vendor = "apple")]
    {
        // SAFETY: plain integer fields; all-zero is a valid value.
        let mut info: libc::proc_bsdinfo = unsafe { core::mem::zeroed() };
        let size = libc::c_int::try_from(core::mem::size_of::<libc::proc_bsdinfo>())
            .map_err(|_| Errno(sys::EINVAL))?;
        // SAFETY: writable storage of the stated size. BSD info is reported
        // for zombies too.
        let filled = unsafe {
            libc::proc_pidinfo(pid, libc::PROC_PIDTBSDINFO, 0, (&raw mut info).cast(), size)
        };
        if filled != size {
            let error = std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EIO);
            return Err(Errno(if error == libc::ESRCH {
                sys::ENOENT
            } else {
                error
            }));
        }
        Ok(copy(
            buffer,
            &[
                &info.pbi_start_tvsec.to_le_bytes(),
                &info.pbi_start_tvusec.to_le_bytes(),
            ],
        ))
    }
}
