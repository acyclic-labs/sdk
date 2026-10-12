#[cfg(any(target_os = "linux", target_os = "macos", windows))]
#[cfg(any(feature = "native-mount", test))]
#[derive(Debug, thiserror::Error)]
#[error("native metadata cannot represent {0}")]
/// Metadata the host cannot represent.
pub struct NativeMetadataError(pub(crate) &'static str);

#[cfg(all(any(unix, windows), any(feature = "native-mount", test)))]
pub(super) fn reject_metadata(fields: &[(bool, &'static str)]) -> Result<(), NativeMetadataError> {
    for &(present, field) in fields {
        if present {
            return Err(NativeMetadataError(field));
        }
    }
    Ok(())
}
