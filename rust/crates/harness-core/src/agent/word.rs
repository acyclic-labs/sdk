//! Shared target and workspace labels.
pub(super) const MAX_NAME_BYTES: usize = 64;

/// Names are lowercase ASCII words, so `user:` can never name a device.
pub(super) fn check_name(name: &str) -> Result<(), super::Error> {
    let mut bytes = name.bytes();
    if name.len() <= MAX_NAME_BYTES
        && bytes
            .next()
            .is_some_and(|first| first.is_ascii_lowercase() || first.is_ascii_digit())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
    {
        Ok(())
    } else {
        Err(super::Error::Record("target name is not a lowercase word"))
    }
}
