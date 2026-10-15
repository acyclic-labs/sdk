//! Current-user DPAPI boundary; foreign buffers never escape the callback.
use std::{io, ptr, slice};
use windows_sys::Win32::{
    Foundation::LocalFree,
    Security::Cryptography::{
        CRYPT_INTEGER_BLOB, CRYPTPROTECT_UI_FORBIDDEN, CryptProtectData, CryptUnprotectData,
    },
};

struct ProtectedBlob(CRYPT_INTEGER_BLOB);
impl Drop for ProtectedBlob {
    fn drop(&mut self) {
        if !self.0.pbData.is_null() {
            // SAFETY: successful DPAPI output owns exactly cbData live bytes. Clear
            // and free that allocation on success, error and callback unwinding.
            unsafe {
                ptr::write_bytes(self.0.pbData, 0, self.0.cbData as usize);
                LocalFree(self.0.pbData.cast());
            }
        }
    }
}

pub(crate) fn protect_with_copy<T, E: From<io::Error>>(
    bytes: &[u8],
    maximum_bytes: u64,
    protect: bool,
    copy: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<T, E> {
    let input = CRYPT_INTEGER_BLOB {
        cbData: u32::try_from(bytes.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "private payload exceeds u32")
        })?,
        pbData: bytes.as_ptr().cast_mut(),
    };
    let mut output = CRYPT_INTEGER_BLOB {
        cbData: 0,
        pbData: ptr::null_mut(),
    };
    // SAFETY: input borrows live read-only bytes. DPAPI writes its owned output
    // descriptor, and no input pointer is retained after the synchronous call.
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
    let output = ProtectedBlob(output);
    if output.0.cbData == 0
        || output.0.pbData.is_null()
        || u64::from(output.0.cbData) > maximum_bytes
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "protected payload exceeds its bound",
        )
        .into());
    }
    // SAFETY: DPAPI returned cbData live bytes owned by output until after copy.
    copy(unsafe { slice::from_raw_parts(output.0.pbData, output.0.cbData as usize) })
}
