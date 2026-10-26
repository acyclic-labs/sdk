//! One protected, integrity-checked owner-record format for all native hosts.
//! The host supplies payload encoding and admitted copy storage. Windows DPAPI
//! always protects payloads before publication; Unix relies on descriptor-checked
//! owner permissions. Neither path accepts a legacy plaintext fallback.
use std::{io, sync::Arc};
use subtle::ConstantTimeEq as _;
use zeroize::Zeroize as _;
mod protection;

/// Owned secret bytes cleared on every return and unwind path.
pub struct SecretBytes(pub Vec<u8>);

impl Drop for SecretBytes {
    fn drop(&mut self) {
        clear_secret(&mut self.0);
    }
}

/// Canonical owner-record envelope version, independent of payload schema.
pub const MAGIC: &[u8; 8] = b"ACYPIOW1";

/// Shared entropy seam for persisted signing keys and fresh native identities.
pub fn fill_random(bytes: &mut [u8]) -> Result<(), getrandom::Error> {
    #[cfg(feature = "test-support")]
    if crate::testing::fill_seeded(bytes) {
        return Ok(());
    }
    getrandom::fill(bytes)
}

/// Create one nonzero signing key through the host's shared entropy seam.
pub fn new_key() -> io::Result<[u8; 32]> {
    let mut key = [0; 32];
    fill_random(&mut key).map_err(io::Error::other)?;
    if key == [0; 32] {
        return Err(io::ErrorKind::InvalidData.into());
    }
    Ok(key)
}

fn digest(
    bytes: &[u8],
    meter: Option<&Arc<dyn crate::NativeReadMeter>>,
) -> io::Result<blake3::Hash> {
    if let Some(meter) = meter {
        meter.admit_codec(0, 0, bytes.len() as u64)?;
    }
    let digest = blake3::hash(bytes);
    if let Some(meter) = meter {
        meter.record_codec(0, 0, bytes.len() as u64)?;
    }
    Ok(digest)
}

/// Protect and frame a payload. The admitted producer copies these three parts
/// once into its original owned allocation; no intermediate managed copy exists.
pub fn encode_with<T, E: From<io::Error>>(
    plain: &[u8],
    maximum: u64,
    meter: Option<&Arc<dyn crate::NativeReadMeter>>,
    copy: impl FnOnce([&[u8]; 3]) -> Result<T, E>,
) -> Result<T, E> {
    protection::protect_with_copy(
        plain,
        maximum
            .checked_sub(40)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?,
        |protected| {
            let digest = digest(protected, meter)?;
            copy([MAGIC, digest.as_bytes(), protected])
        },
    )
}

/// Verify the complete retained envelope before decrypting or allocating the
/// caller's plaintext. A fresh digest does not bypass payload schema validation.
pub fn decode_with<T, E: From<io::Error>>(
    record: &[u8],
    maximum: u64,
    meter: Option<&Arc<dyn crate::NativeReadMeter>>,
    copy: impl FnOnce(&[u8]) -> Result<T, E>,
) -> Result<T, E> {
    if record.len() as u64 > maximum {
        return Err(io::Error::from(io::ErrorKind::InvalidData).into());
    }
    let (magic, rest) = record
        .split_first_chunk::<8>()
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;
    let (expected, protected) = rest
        .split_first_chunk::<32>()
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?;
    if magic != MAGIC || !bool::from(expected.ct_eq(digest(protected, meter)?.as_bytes())) {
        return Err(
            io::Error::new(io::ErrorKind::InvalidData, "owner record integrity differs").into(),
        );
    }
    protection::unprotect_with_copy(protected, maximum, copy)
}

#[cfg(test)]
mod tests;

/// Clear a managed secret using writes that cannot be optimized away.
pub fn clear_secret(bytes: &mut [u8]) {
    bytes.zeroize();
}
