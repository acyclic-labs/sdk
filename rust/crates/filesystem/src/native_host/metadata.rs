#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[cfg(any(feature = "native-mount", test))]
#[derive(Debug, thiserror::Error)]
#[error("native metadata cannot represent {0}")]
/// Metadata the host cannot represent.
pub struct NativeMetadataError(pub(crate) &'static str);

#[cfg(all(unix, any(feature = "native-mount", test)))]
pub(super) fn unix_metadata_timespec(
    field: crate::kernel::MetadataField<i64>,
) -> Result<libc::timespec, NativeMetadataError> {
    let crate::kernel::MetadataField::Value(ns) = field else {
        return Ok(libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        });
    };
    Ok(libc::timespec {
        tv_sec: libc::time_t::try_from(ns.div_euclid(1_000_000_000))
            .map_err(|_| NativeMetadataError("timestamp range"))?,
        tv_nsec: libc::c_long::try_from(ns.rem_euclid(1_000_000_000))
            .map_err(|_| NativeMetadataError("timestamp range"))?,
    })
}
#[cfg(all(unix, any(feature = "native-mount", test)))]
pub(super) fn unix_metadata_nanos(seconds: libc::time_t, nanos: libc::c_long) -> Option<i64> {
    i64::try_from(i128::from(seconds) * 1_000_000_000 + i128::from(nanos)).ok()
}
