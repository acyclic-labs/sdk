//! Portable host device-number packing.
//!
//! Linux and Darwin disagree on the width of `dev_t` and on the signatures of
//! `libc::major`, `libc::minor`, and `libc::makedev`. Every split or join of a
//! host device number goes through these shims so call sites stay
//! platform-free.

/// Splits a host `dev_t` into `(major, minor)`.
#[cfg(target_os = "linux")]
pub(crate) fn split_device(device: u64) -> (u32, u32) {
    (libc::major(device), libc::minor(device))
}

/// Splits a host `dev_t` into `(major, minor)`.
///
/// Darwin's `dev_t` is a signed 32-bit value; the low 32 bits of the wider
/// host representation are reinterpreted without loss.
#[cfg(target_os = "macos")]
pub(crate) fn split_device(device: u64) -> (u32, u32) {
    let low = u32::try_from(device & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    let native = i32::from_ne_bytes(low.to_ne_bytes());
    (
        u32::try_from(libc::major(native)).unwrap_or(u32::MAX),
        u32::try_from(libc::minor(native)).unwrap_or(u32::MAX),
    )
}

/// Joins `(major, minor)` into a host `dev_t`, failing with `EOVERFLOW` when
/// the pair cannot be represented on this platform.
#[cfg(target_os = "linux")]
pub(crate) fn join_device(major: u32, minor: u32) -> Result<libc::dev_t, i32> {
    let device = libc::makedev(major, minor);
    if libc::major(device) != major || libc::minor(device) != minor {
        return Err(libc::EOVERFLOW);
    }
    Ok(device)
}

/// Joins `(major, minor)` into a host `dev_t`, failing with `EOVERFLOW` when
/// the pair cannot be represented on this platform.
///
/// Darwin packs the major number into 8 bits and the minor number into 24;
/// out-of-range components are rejected instead of silently aliasing.
#[cfg(target_os = "macos")]
pub(crate) fn join_device(major: u32, minor: u32) -> Result<libc::dev_t, i32> {
    if major > 0xFF || minor > 0xFF_FFFF {
        return Err(libc::EOVERFLOW);
    }
    let major = i32::try_from(major).map_err(|_| libc::EOVERFLOW)?;
    let minor = i32::try_from(minor).map_err(|_| libc::EOVERFLOW)?;
    Ok(libc::makedev(major, minor))
}

#[cfg(test)]
mod tests {
    use super::{join_device, split_device};

    #[cfg(target_os = "linux")]
    fn widen(device: libc::dev_t) -> u64 {
        device
    }

    #[cfg(target_os = "macos")]
    fn widen(device: libc::dev_t) -> u64 {
        u64::from(u32::from_ne_bytes(device.to_ne_bytes()))
    }

    #[test]
    fn split_and_join_round_trip() -> Result<(), i32> {
        for (major, minor) in [(0, 0), (1, 3), (0xFF, 0xFF_FFFF)] {
            let device = join_device(major, minor)?;
            assert_eq!(split_device(widen(device)), (major, minor));
        }
        Ok(())
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn join_rejects_unrepresentable_components() {
        assert_eq!(join_device(0x100, 0), Err(libc::EOVERFLOW));
        assert_eq!(join_device(0, 0x100_0000), Err(libc::EOVERFLOW));
    }
}
