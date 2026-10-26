//! One OS keychain entry owns a signing seed; persisted host metadata contains
//! only its public opaque reference. Missing or locked custody never regenerates it.
use crate::owner_record::{SecretBytes, new_key};
use serde::{Deserialize, Serialize};
use std::io;
use subtle::ConstantTimeEq as _;
use zeroize::Zeroizing;

const SERVICE: &str = "acyclic.authority.signing-seed.v1";

/// Non-secret, content-bound reference to OS-owned signing custody.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SigningKeyReference([u8; 32]);

fn unavailable() -> io::Error {
    io::Error::other(
        "OS signing-key custody failed; recovery: unlock the user's OS credential store and reopen the original state directory",
    )
}

impl SigningKeyReference {
    /// Generate a root seed once using the shared entropy seam and retain it in
    /// the OS credential store before the host publishes its reference.
    pub fn create() -> io::Result<Self> {
        let seed = Zeroizing::new(new_key()?);
        Self::retain(&seed)
    }

    /// Migrate an original seed read from an authenticated private descriptor.
    /// This preserves its authority rather than generating replacement credentials.
    pub fn retain(seed: &[u8; 32]) -> io::Result<Self> {
        if bool::from(seed.ct_eq(&[0; 32])) {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let reference = Self(*blake3::hash(seed).as_bytes());
        let entry = reference.entry()?;
        match entry.get_secret() {
            Ok(existing) => {
                let existing = SecretBytes(existing);
                if !bool::from(existing.0.as_slice().ct_eq(seed)) {
                    return Err(unavailable());
                }
            }
            Err(keyring::Error::NoEntry) => entry.set_secret(seed).map_err(|_| unavailable())?,
            Err(_) => return Err(unavailable()),
        }
        // Observe the actual stored seed before publishing any reference. A
        // competing replacement or an unavailable backend cannot appear successful.
        reference.with_seed(|stored| {
            if bool::from(stored.ct_eq(seed)) {
                Ok(reference)
            } else {
                Err(unavailable())
            }
        })?
    }

    /// Borrow secret material only for bootstrap, then clear the OS output buffer
    /// on success, refusal or unwind. The reference never carries seed bytes.
    pub fn with_seed<T>(&self, consume: impl FnOnce(&[u8; 32]) -> T) -> io::Result<T> {
        let secret = SecretBytes(self.entry()?.get_secret().map_err(|_| unavailable())?);
        let seed: &[u8; 32] = secret.0.as_slice().try_into().map_err(|_| unavailable())?;
        if !bool::from(blake3::hash(seed).as_bytes().ct_eq(&self.0)) {
            return Err(unavailable());
        }
        Ok(consume(seed))
    }

    fn entry(&self) -> io::Result<keyring::Entry> {
        let name = blake3::Hash::from_bytes(self.0).to_hex();
        keyring::Entry::new(SERVICE, name.as_str()).map_err(|_| unavailable())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Uses the actual OS credential service, launched in an isolated user
    /// session by scripts/with-owner-keychain.sh on headless Linux hosts.
    #[test]
    fn os_keychain_retains_original_seed_and_rejects_missing_or_replaced_custody() -> io::Result<()>
    {
        let seed = Zeroizing::new(new_key()?);
        let reference = SigningKeyReference::retain(&seed)?;
        let entry = reference.entry()?;
        assert!(reference.with_seed(|loaded| bool::from(loaded.ct_eq(&*seed)))?);
        // Negative control: the former config writer exposes the planted seed.
        let plaintext_config = serde_json::to_vec(&*seed).map_err(io::Error::other)?;
        let metadata = serde_json::to_vec(&reference).map_err(io::Error::other)?;
        assert_ne!(metadata, plaintext_config);
        assert!(
            !metadata
                .windows(seed.len())
                .any(|window| window == seed.as_slice())
        );
        let replacement = Zeroizing::new(new_key()?);
        entry.set_secret(&*replacement).map_err(|_| unavailable())?;
        assert!(
            reference
                .with_seed(|_| panic!("replaced seed reached bootstrap"))
                .is_err()
        );
        entry.delete_credential().map_err(|_| unavailable())?;
        assert!(
            reference
                .with_seed(|_| panic!("missing seed was regenerated"))
                .is_err()
        );
        assert!(matches!(entry.get_secret(), Err(keyring::Error::NoEntry)));
        Ok(())
    }
}
