//! Small platform boundary for private Pi owner files and system randomness.

use std::io;

#[cfg(unix)]
pub(super) fn new_key() -> io::Result<[u8; 32]> {
    use std::io::Read as _;
    let mut key = [0; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut key)?;
    Ok(key)
}

/// The managed copy belongs to the original caller's guarded SDK producer.
#[cfg(unix)]
pub(super) fn unprotect_with_copy<T, E>(
    bytes: &[u8],
    copy: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<T, E> {
    copy(bytes)
}

#[cfg(unix)]
pub(super) fn protect_with_copy<T, E>(
    bytes: &[u8],
    copy: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<T, E> {
    copy(bytes)
}

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "BCryptGenRandom fills one fixed host signing key using the system CSPRNG"
)]
pub(super) fn new_key() -> io::Result<[u8; 32]> {
    use windows_sys::Win32::Security::Cryptography::{
        BCRYPT_USE_SYSTEM_PREFERRED_RNG, BCryptGenRandom,
    };
    let mut key = [0; 32];
    // SAFETY: system-preferred RNG requires no algorithm handle; the live writable buffer is exactly 32 bytes and no pointers are retained.
    let status = unsafe {
        BCryptGenRandom(
            std::ptr::null_mut(),
            key.as_mut_ptr(),
            32,
            BCRYPT_USE_SYSTEM_PREFERRED_RNG,
        )
    };
    if status < 0 {
        return Err(io::Error::other("system signing-key generation failed"));
    }
    Ok(key)
}

/// DPAPI owns its opaque foreign allocation.
#[cfg(windows)]
pub(super) fn unprotect_with_copy<T, E: From<io::Error>>(
    bytes: &[u8],
    copy: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<T, E> {
    windows_protect_with_copy(bytes, false, copy)
}

#[cfg(windows)]
pub(super) fn protect_with_copy<T, E: From<io::Error>>(
    bytes: &[u8],
    copy: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<T, E> {
    windows_protect_with_copy(bytes, true, copy)
}

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "DPAPI protects owner credentials for the current Windows user; all output allocations are cleared and freed"
)]
fn windows_protect_with_copy<T, E: From<io::Error>>(
    bytes: &[u8],
    protect: bool,
    copy: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<T, E> {
    use std::{ptr, slice};
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::Cryptography::{
            CRYPT_INTEGER_BLOB, CRYPTPROTECT_UI_FORBIDDEN, CryptProtectData, CryptUnprotectData,
        },
    };
    let input = CRYPT_INTEGER_BLOB {
        cbData: u32::try_from(bytes.len())
            .map_err(|_| io::Error::other("Pi owner payload is too large"))?,
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
    let result = if output.cbData == 0
        || output.pbData.is_null()
        || u64::from(output.cbData) > super::MAXIMUM_OWNER_BYTES
    {
        Err(io::Error::other("invalid protected Pi owner payload size").into())
    } else {
        // SAFETY: successful DPAPI returned an allocation of cbData bytes.
        copy(unsafe { slice::from_raw_parts(output.pbData, output.cbData as usize) })
    };
    if !output.pbData.is_null() {
        // SAFETY: output is the live DPAPI allocation above, cleared before release, including error paths that reject an oversized result.
        unsafe {
            ptr::write_bytes(output.pbData, 0, output.cbData as usize);
            LocalFree(output.pbData.cast());
        }
    }
    result
}
