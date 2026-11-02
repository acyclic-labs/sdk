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
    let native = device as libc::dev_t;
    (
        libc::major(native).cast_unsigned(),
        libc::minor(native).cast_unsigned(),
    )
}

/// Joins `(major, minor)` into a host `dev_t`, failing with `EOVERFLOW` when
/// the pair cannot be represented on this platform.
#[cfg(target_os = "linux")]
#[cfg(any(test, feature = "native-mount"))]
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
#[cfg(any(test, feature = "native-mount"))]
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
        device as u64
    }

    #[test]
    fn split_and_join_round_trip() -> Result<(), i32> {
        for (major, minor) in [(0, 0), (1, 3), (0xFF, 0xFF_FFFF)] {
            let device = join_device(major, minor)?;
            assert_eq!(split_device(widen(device)), (major, minor));
            #[cfg(target_os = "macos")]
            if device < 0 {
                // A checked narrowing loses valid device numbers after the
                // host's signed dev_t was widened by a metadata consumer.
                assert!(libc::dev_t::try_from(widen(device)).is_err());
            }
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
