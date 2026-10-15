//! User-private payload protection with caller-owned copying.
use std::io;

/// Protects a nonempty payload for the current user. The callback owns any copy;
/// Windows' foreign DPAPI buffer is cleared and released after the callback.
pub fn protect_with_copy<T, E: From<io::Error>>(
    bytes: &[u8],
    maximum_bytes: u64,
    copy: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<T, E> {
    transform(bytes, maximum_bytes, true, copy)
}

/// Opens a user-private payload without allocating an SDK-owned intermediate.
/// On Unix, private file permissions protect the bytes and the callback borrows them.
pub fn unprotect_with_copy<T, E: From<io::Error>>(
    bytes: &[u8],
    maximum_bytes: u64,
    copy: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<T, E> {
    transform(bytes, maximum_bytes, false, copy)
}

fn transform<T, E: From<io::Error>>(
    bytes: &[u8],
    maximum_bytes: u64,
    protect: bool,
    copy: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<T, E> {
    if bytes.is_empty() || maximum_bytes == 0 || bytes.len() as u64 > maximum_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "private payload exceeds its bound",
        )
        .into());
    }
    #[cfg(windows)]
    {
        crate::windows::protect_with_copy(bytes, maximum_bytes, protect, copy)
    }
    #[cfg(not(windows))]
    {
        let _ = protect;
        copy(bytes)
    }
}

#[cfg(test)]
mod tests;
