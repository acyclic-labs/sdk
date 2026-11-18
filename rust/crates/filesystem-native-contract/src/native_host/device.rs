//! Portable host device-number packing.
//!
//! Linux and Darwin disagree on the width of `dev_t` and on the signatures of
//! `libc::major`, `libc::minor`, and `libc::makedev`. Every split or join of a
//! host device number uses this codec so call sites stay
//! platform-free.

/// Splits a host `dev_t` into `(major, minor)`.
#[cfg(target_os = "linux")]
pub fn split_device(device: u64) -> (u32, u32) {
    (libc::major(device), libc::minor(device))
}

/// Splits a host `dev_t` into `(major, minor)`.
///
/// Darwin's `dev_t` is a signed 32-bit value; the low 32 bits of the wider
/// host representation are reinterpreted without loss.
#[cfg(target_os = "macos")]
pub fn split_device(device: u64) -> (u32, u32) {
    let native = device as libc::dev_t;
    (
        libc::major(native).cast_unsigned(),
        libc::minor(native).cast_unsigned(),
    )
}

/// Joins `(major, minor)` into a host `dev_t`, failing with `EOVERFLOW` when
/// the pair cannot be represented on this platform.
#[cfg(target_os = "linux")]
pub fn join_device(major: u32, minor: u32) -> Result<libc::dev_t, i32> {
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
pub fn join_device(major: u32, minor: u32) -> Result<libc::dev_t, i32> {
    if major > 0xFF || minor > 0xFF_FFFF {
        return Err(libc::EOVERFLOW);
    }
    let major = i32::try_from(major).map_err(|_| libc::EOVERFLOW)?;
    let minor = i32::try_from(minor).map_err(|_| libc::EOVERFLOW)?;
    Ok(libc::makedev(major, minor))
}

#[cfg(all(target_os = "linux", any(test, feature = "fuzzing")))]
fn widen(device: libc::dev_t) -> u64 {
    device
}

#[cfg(all(target_os = "macos", any(test, feature = "fuzzing")))]
fn widen(device: libc::dev_t) -> u64 {
    device as u64
}

/// Checks arbitrary native bit patterns and component pairs against the OS codec.
///
/// # Panics
///
/// Panics if the native codec disagrees with the OS or accepts an unrepresentable pair.
#[cfg(all(feature = "fuzzing", any(target_os = "linux", target_os = "macos")))]
pub fn check_device_codec(input: &[u8]) {
    let Some((native, input)) = input.split_first_chunk::<8>() else {
        return;
    };
    let Some((major, input)) = input.split_first_chunk::<4>() else {
        return;
    };
    let Some((minor, _)) = input.split_first_chunk::<4>() else {
        return;
    };
    let native = u64::from_le_bytes(*native);
    let components = split_device(native);
    #[cfg(target_os = "linux")]
    let expected = (libc::major(native), libc::minor(native));
    #[cfg(target_os = "macos")]
    let expected = (
        libc::major(native as libc::dev_t).cast_unsigned(),
        libc::minor(native as libc::dev_t).cast_unsigned(),
    );
    assert_eq!(components, expected);
    assert!(matches!(join_device(components.0, components.1), Ok(joined)
        if split_device(widen(joined)) == components));

    let components = (u32::from_le_bytes(*major), u32::from_le_bytes(*minor));
    match join_device(components.0, components.1) {
        Ok(joined) => assert_eq!(split_device(widen(joined)), components),
        Err(error) => {
            assert_eq!(error, libc::EOVERFLOW);
            #[cfg(target_os = "linux")]
            assert_ne!(
                error,
                libc::EOVERFLOW,
                "Linux dev_t must retain both complete u32 components"
            );
            #[cfg(target_os = "macos")]
            assert!(components.0 > 0xFF || components.1 > 0xFF_FFFF);
        }
    }
}

#[cfg(test)]
#[path = "tests/device.rs"]
mod tests;

/// Decodes a native special node request into its kind and optional device number.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub fn special_mount_node(
    mode: libc::mode_t,
    device: u64,
) -> Result<(crate::MountNodeKind, Option<(u32, u32)>), i32> {
    let kind = match mode & libc::S_IFMT {
        libc::S_IFIFO => crate::MountNodeKind::Fifo,
        libc::S_IFSOCK => crate::MountNodeKind::Socket,
        libc::S_IFCHR => crate::MountNodeKind::CharacterDevice,
        libc::S_IFBLK => crate::MountNodeKind::BlockDevice,
        _ => return Err(libc::EOPNOTSUPP),
    };
    let device = matches!(
        kind,
        crate::MountNodeKind::CharacterDevice | crate::MountNodeKind::BlockDevice
    )
    .then(|| split_device(device));
    Ok((kind, device))
}
