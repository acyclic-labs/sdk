use crate::kernel::NameEncoding;
use crate::model::FilesystemProfile;
use std::ffi::OsStr;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HostNameEncodingError;

fn copy_bytes(source: &[u8], maximum_bytes: u32) -> Result<Vec<u8>, HostNameEncodingError> {
    let maximum_bytes = usize::try_from(maximum_bytes).map_err(|_| HostNameEncodingError)?;
    if source.len() > maximum_bytes {
        return Err(HostNameEncodingError);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(source.len())
        .map_err(|_| HostNameEncodingError)?;
    bytes.extend_from_slice(source);
    Ok(bytes)
}

fn utf16_bytes<I>(units: I, maximum_bytes: u32) -> Result<Vec<u8>, HostNameEncodingError>
where
    I: Iterator<Item = u16> + Clone,
{
    let length = units
        .clone()
        .count()
        .checked_mul(2)
        .ok_or(HostNameEncodingError)?;
    let maximum_bytes = usize::try_from(maximum_bytes).map_err(|_| HostNameEncodingError)?;
    if length > maximum_bytes {
        return Err(HostNameEncodingError);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| HostNameEncodingError)?;
    for unit in units {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    Ok(bytes)
}

#[cfg(unix)]
pub(crate) fn host_name_bytes(
    name: &OsStr,
    profile: FilesystemProfile,
    maximum_bytes: u32,
) -> Result<(NameEncoding, Vec<u8>), HostNameEncodingError> {
    use std::os::unix::ffi::OsStrExt;
    let raw = name.as_bytes();
    match profile {
        FilesystemProfile::Posix => Ok((NameEncoding::PosixBytes, copy_bytes(raw, maximum_bytes)?)),
        FilesystemProfile::Windows => {
            let text = std::str::from_utf8(raw).map_err(|_| HostNameEncodingError)?;
            Ok((
                NameEncoding::WindowsUtf16Le,
                utf16_bytes(text.encode_utf16(), maximum_bytes)?,
            ))
        }
        FilesystemProfile::Portable | FilesystemProfile::Browser => {
            std::str::from_utf8(raw).map_err(|_| HostNameEncodingError)?;
            Ok((NameEncoding::Utf8, copy_bytes(raw, maximum_bytes)?))
        }
    }
}

#[cfg(windows)]
pub(crate) fn host_name_bytes(
    name: &OsStr,
    profile: FilesystemProfile,
    maximum_bytes: u32,
) -> Result<(NameEncoding, Vec<u8>), HostNameEncodingError> {
    use std::os::windows::ffi::OsStrExt;
    match profile {
        FilesystemProfile::Windows => Ok((
            NameEncoding::WindowsUtf16Le,
            utf16_bytes(name.encode_wide(), maximum_bytes)?,
        )),
        FilesystemProfile::Posix => Ok((
            NameEncoding::PosixBytes,
            copy_bytes(
                name.to_str().ok_or(HostNameEncodingError)?.as_bytes(),
                maximum_bytes,
            )?,
        )),
        FilesystemProfile::Portable | FilesystemProfile::Browser => Ok((
            NameEncoding::Utf8,
            copy_bytes(
                name.to_str().ok_or(HostNameEncodingError)?.as_bytes(),
                maximum_bytes,
            )?,
        )),
    }
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn host_name_bytes(
    name: &OsStr,
    _profile: FilesystemProfile,
    maximum_bytes: u32,
) -> Result<(NameEncoding, Vec<u8>), HostNameEncodingError> {
    Ok((
        NameEncoding::Utf8,
        copy_bytes(
            name.to_str().ok_or(HostNameEncodingError)?.as_bytes(),
            maximum_bytes,
        )?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_rejects_a_component_before_allocating_beyond_its_bound() {
        assert!(host_name_bytes(OsStr::new("ab"), FilesystemProfile::Portable, 1).is_err());
        assert!(host_name_bytes(OsStr::new("ab"), FilesystemProfile::Windows, 3).is_err());
    }
}
