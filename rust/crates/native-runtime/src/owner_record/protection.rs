use std::io;

/// The managed copy belongs to the original caller's guarded SDK producer.
#[cfg(unix)]
pub fn unprotect_with_copy<T, E: From<io::Error>>(
    bytes: &[u8],
    maximum: u64,
    copy: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<T, E> {
    if bytes.len() as u64 > maximum {
        return Err(io::Error::from(io::ErrorKind::InvalidData).into());
    }
    copy(bytes)
}

#[cfg(unix)]
pub fn protect_with_copy<T, E: From<io::Error>>(
    bytes: &[u8],
    maximum: u64,
    copy: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<T, E> {
    if bytes.len() as u64 > maximum {
        return Err(io::Error::from(io::ErrorKind::InvalidData).into());
    }
    copy(bytes)
}

/// DPAPI owns its opaque foreign allocation.
#[cfg(windows)]
pub fn unprotect_with_copy<T, E: From<io::Error>>(
    bytes: &[u8],
    maximum: u64,
    copy: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<T, E> {
    windows_protect_with_copy(bytes, false, maximum, copy)
}

#[cfg(windows)]
pub fn protect_with_copy<T, E: From<io::Error>>(
    bytes: &[u8],
    maximum: u64,
    copy: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<T, E> {
    windows_protect_with_copy(bytes, true, maximum, copy)
}

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "DPAPI protects owner credentials for the current Windows user; all output allocations are cleared and freed"
)]
fn windows_protect_with_copy<T, E: From<io::Error>>(
    bytes: &[u8],
    protect: bool,
    maximum: u64,
    copy: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<T, E> {
    use std::{ptr, slice};
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::Cryptography::{
            CRYPT_INTEGER_BLOB, CRYPTPROTECT_UI_FORBIDDEN, CryptProtectData, CryptUnprotectData,
        },
    };
    if bytes.len() as u64 > maximum {
        return Err(io::Error::from(io::ErrorKind::InvalidData).into());
    }
    let input = CRYPT_INTEGER_BLOB {
        cbData: u32::try_from(bytes.len())
            .map_err(|_| io::Error::other("owner payload is too large"))?,
        pbData: bytes.as_ptr().cast_mut(),
    };
    let mut output = CRYPT_INTEGER_BLOB {
        cbData: 0,
        pbData: ptr::null_mut(),
    };
    // SAFETY: input points to a live read-only buffer.
    let success = unsafe {
        if protect {
            CryptProtectData(
                &input,
                ptr::null(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                CRYPTPROTECT_UI_FORBIDDEN,
                &mut output,
            )
        } else {
            CryptUnprotectData(
                &input,
                ptr::null_mut(),
                ptr::null(),
                ptr::null(),
                ptr::null(),
                CRYPTPROTECT_UI_FORBIDDEN,
                &mut output,
            )
        }
    };
    if success == 0 {
        return Err(io::Error::last_os_error().into());
    }
    // Foreign secret storage must be cleared even if the admitted consumer unwinds.
    struct ProtectedOutput(CRYPT_INTEGER_BLOB);
    impl Drop for ProtectedOutput {
        fn drop(&mut self) {
            if !self.0.pbData.is_null() {
                // SAFETY: successful DPAPI transferred this live allocation.
                unsafe {
                    super::clear_secret(slice::from_raw_parts_mut(
                        self.0.pbData,
                        self.0.cbData as usize,
                    ));
                    LocalFree(self.0.pbData.cast());
                }
            }
        }
    }
    let retained = ProtectedOutput(output);
    let output = &retained.0;
    if output.cbData == 0 || output.pbData.is_null() || u64::from(output.cbData) > maximum {
        Err(io::Error::other("invalid protected owner payload size").into())
    } else {
        // SAFETY: successful DPAPI returned an allocation of cbData bytes.
        copy(unsafe { slice::from_raw_parts(output.pbData, output.cbData as usize) })
    }
}
