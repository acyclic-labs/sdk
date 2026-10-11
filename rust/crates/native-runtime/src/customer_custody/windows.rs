#![allow(
    unsafe_code,
    reason = "Windows Credential Manager exposes owned credential buffers through native FFI"
)]

use super::CustodyError;
use std::ptr;
use windows_sys::Win32::Foundation::{ERROR_NOT_FOUND, GetLastError};
use windows_sys::Win32::Security::Credentials::{
    CRED_MAX_CREDENTIAL_BLOB_SIZE, CRED_PERSIST_LOCAL_MACHINE, CRED_TYPE_GENERIC, CREDENTIALW,
    CredDeleteW, CredFree, CredReadW, CredWriteW,
};
use zeroize::{Zeroize, Zeroizing};

fn target(name: &str) -> Vec<u16> {
    format!("acyclic.customer-leaf.v1/{name}").encode_utf16().chain(Some(0)).collect()
}

fn failure() -> CustodyError {
    // SAFETY: GetLastError reads only the calling thread's native error status.
    match unsafe { GetLastError() } {
        ERROR_NOT_FOUND => CustodyError::NotFound,
        code => CustodyError::Platform(code as i64),
    }
}

pub(super) fn read(name: &str) -> Result<Zeroizing<Vec<u8>>, CustodyError> {
    let target = target(name);
    let mut credential: *mut CREDENTIALW = ptr::null_mut();
    // The API owns the returned allocation; scrub its blob before releasing it.
    // SAFETY: target is terminated UTF-16 and the output pointer is valid for this call.
    if unsafe { CredReadW(target.as_ptr(), CRED_TYPE_GENERIC, 0, &mut credential) } == 0 {
        return Err(failure());
    }
    if credential.is_null() {
        return Err(CustodyError::Corrupt);
    }
    // SAFETY: successful CredReadW owns a nonnull CREDENTIALW and its sized blob until
    // CredFree below; the blob is uniquely borrowed here and scrubbed before release.
    let result = unsafe {
        let value = &*credential;
        if value.CredentialBlobSize == 0 || value.CredentialBlob.is_null() {
            Err(CustodyError::Corrupt)
        } else {
            let blob = std::slice::from_raw_parts_mut(value.CredentialBlob, value.CredentialBlobSize as usize);
            let result = Zeroizing::new(blob.to_vec());
            blob.zeroize();
            Ok(result)
        }
    };
    // SAFETY: this is the allocation returned by CredReadW, released exactly once.
    unsafe { CredFree(credential.cast()) };
    result
}

pub(super) fn write(name: &str, secret: &[u8]) -> Result<(), CustodyError> {
    if secret.len() > CRED_MAX_CREDENTIAL_BLOB_SIZE as usize {
        return Err(CustodyError::TooLarge);
    }
    let mut target = target(name);
    let mut user: Vec<u16> = "customer-leaf".encode_utf16().chain(Some(0)).collect();
    let credential = CREDENTIALW {
        Type: CRED_TYPE_GENERIC,
        TargetName: target.as_mut_ptr(),
        CredentialBlobSize: secret.len() as u32,
        // CredWriteW reads this input buffer synchronously and does not mutate it.
        CredentialBlob: secret.as_ptr().cast_mut(),
        Persist: CRED_PERSIST_LOCAL_MACHINE,
        UserName: user.as_mut_ptr(),
        // SAFETY: CREDENTIALW contains only scalar fields and nullable raw pointers.
        ..unsafe { std::mem::zeroed() }
    };
    // One CredWrite replaces the complete key/session item. Never delete first.
    // SAFETY: credential and its terminated metadata buffers remain live for the call;
    // CredWriteW synchronously reads, but does not mutate, the borrowed secret bytes.
    if unsafe { CredWriteW(&credential, 0) } == 0 {
        return Err(failure());
    }
    Ok(())
}

pub(super) fn delete(name: &str) -> Result<(), CustodyError> {
    let target = target(name);
    // SAFETY: target is terminated UTF-16 and remains live throughout CredDeleteW.
    if unsafe { CredDeleteW(target.as_ptr(), CRED_TYPE_GENERIC, 0) } == 0 {
        return Err(failure());
    }
    Ok(())
}
