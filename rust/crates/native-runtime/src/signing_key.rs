//! One OS keychain entry owns a signing seed; persisted host metadata contains
//! only its public opaque reference. Missing or locked custody never regenerates it.
use crate::owner_record::{SecretBytes, new_key};
use serde::{Deserialize, Serialize};
use std::{
    io,
    sync::atomic::{AtomicBool, Ordering},
};
use subtle::ConstantTimeEq as _;
use zeroize::Zeroizing;

const SERVICE: &str = "acyclic.authority.signing-seed.v1";

/// Non-secret, content-bound reference to OS-owned signing custody.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SigningKeyReference([u8; 32]);

/// A newly created entry belongs to its unpublished owner. Abandonment removes
/// that entry; an existing entry and a published owner's entry are never retired.
pub struct ProvisionalSigningKey {
    reference: SigningKeyReference,
    cleanup: AtomicBool,
}

impl ProvisionalSigningKey {
    /// Non-secret reference serialized into the owner's private record.
    pub fn reference(&self) -> SigningKeyReference {
        self.reference
    }

    /// Transfer custody at the actual namespace rename, including a later lost
    /// durability acknowledgement. Removing the key then would destroy the owner.
    pub fn observe_publication(&self, stage: crate::private_file::PublicationStage) {
        if stage == crate::private_file::PublicationStage::Installed {
            self.cleanup.store(false, Ordering::Release);
        }
    }
}

impl Drop for ProvisionalSigningKey {
    fn drop(&mut self) {
        if self.cleanup.load(Ordering::Acquire) {
            // Drop cannot report a locked/unavailable store. Never replace or
            // recreate custody during cleanup, and only remove our original seed.
            let _ = self.reference.with_seed(|_| {
                self.reference
                    .entry()?
                    .delete_credential()
                    .map_err(|_| unavailable())
            });
        }
    }
}

fn unavailable() -> io::Error {
    io::Error::other(
        "OS signing-key custody failed; recovery: unlock the user's OS credential store and reopen the original state directory",
    )
}

impl SigningKeyReference {
    /// Generate a root seed once using the shared entropy seam and retain it in
    /// the OS credential store before the host publishes its reference.
    pub fn create() -> io::Result<ProvisionalSigningKey> {
        let seed = Zeroizing::new(new_key()?);
        let (reference, created) = Self::retain_entry(&seed)?;
        Ok(ProvisionalSigningKey {
            reference,
            cleanup: AtomicBool::new(created),
        })
    }

    /// Migrate an original seed read from an authenticated private descriptor.
    /// This preserves its authority rather than generating replacement credentials.
    pub fn retain(seed: &[u8; 32]) -> io::Result<Self> {
        Self::retain_entry(seed).map(|(reference, _)| reference)
    }

    fn retain_entry(seed: &[u8; 32]) -> io::Result<(Self, bool)> {
        if bool::from(seed.ct_eq(&[0; 32])) {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let reference = Self(*blake3::hash(seed).as_bytes());
        let entry = reference.entry()?;
        let created = match entry.get_secret() {
            Ok(existing) => {
                let existing = SecretBytes(existing);
                if !bool::from(existing.0.as_slice().ct_eq(seed)) {
                    return Err(unavailable());
                }
                false
            }
            Err(keyring::Error::NoEntry) => {
                entry.set_secret(seed).map_err(|_| unavailable())?;
                true
            }
            Err(_) => return Err(unavailable()),
        };
        // Observe the actual stored seed before publishing any reference. A
        // competing replacement or an unavailable backend cannot appear successful.
        reference.with_seed(|stored| {
            if bool::from(stored.ct_eq(seed)) {
                Ok((reference, created))
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

    /// This key's entry in its one store: the first store holding it, else
    /// the first reachable store, where it is created. An entry is never
    /// copied between stores, and an unreachable store is never written.
    fn entry(&self) -> io::Result<keyring::Entry> {
        let name = blake3::Hash::from_bytes(self.0).to_hex();
        let mut stores = stores(name.as_str())?;
        if stores.len() == 1 {
            return stores.pop().ok_or_else(unavailable);
        }
        let mut reachable = None;
        for entry in stores {
            match entry.get_secret().map(SecretBytes) {
                Ok(_held) => return Ok(entry),
                Err(keyring::Error::NoEntry) => {
                    reachable.get_or_insert(entry);
                }
                Err(_) => {}
            }
        }
        reachable.ok_or_else(unavailable)
    }
}

/// The OS credential stores for `name`, in preference order: the one native
/// store on Windows and Apple platforms. On Linux this is the Secret Service,
/// only when a D-Bus session exists, then the kernel keyring
/// ([`crate::kernel_keyring`]), which needs no daemon,
/// so headless hosts keep custody without any plaintext fallback.
fn stores(name: &str) -> io::Result<Vec<keyring::Entry>> {
    #[cfg(target_os = "linux")]
    {
        let mut stores = Vec::with_capacity(2);
        if std::env::var_os("DBUS_SESSION_BUS_ADDRESS").is_some()
            && let Ok(service) =
                keyring::secret_service::SsCredential::new_with_target(None, SERVICE, name)
        {
            stores.push(keyring::Entry::new_with_credential(Box::new(service)));
        }
        let kernel = crate::kernel_keyring::KernelKey::new(SERVICE, name);
        stores.push(keyring::Entry::new_with_credential(Box::new(kernel)));
        Ok(stores)
    }
    #[cfg(not(target_os = "linux"))]
    Ok(vec![
        keyring::Entry::new(SERVICE, name).map_err(|_| unavailable())?,
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::private_file::{self, PublicationStage};
    use std::io::Write as _;

    #[test]
    fn provisional_keys_follow_actual_owner_publication_at_every_cut() -> io::Result<()> {
        for failure in [
            Some(PublicationStage::Created),
            Some(PublicationStage::Synced),
            Some(PublicationStage::Installed),
            Some(PublicationStage::Published),
            None,
        ] {
            let root = tempfile::tempdir()?;
            let key = SigningKeyReference::create()?;
            let reference = key.reference();
            let path = root.path().join("owner");
            let bytes = serde_json::to_vec(&reference).map_err(io::Error::other)?;
            let result = private_file::publish_new(
                &path,
                &root.path().join("next"),
                |file| file.write_all(&bytes),
                |stage| {
                    key.observe_publication(stage);
                    if Some(stage) == failure {
                        Err(io::ErrorKind::Interrupted.into())
                    } else {
                        Ok(())
                    }
                },
            );
            assert_eq!(result.is_err(), failure.is_some());
            drop(key);
            if path.exists() {
                reference.with_seed(|_| ())?;
                reference
                    .entry()?
                    .delete_credential()
                    .map_err(|_| unavailable())?;
            } else {
                assert!(matches!(
                    reference.entry()?.get_secret(),
                    Err(keyring::Error::NoEntry)
                ));
            }
        }
        Ok(())
    }

    #[test]
    fn losing_or_abandoned_creation_removes_only_its_own_new_entry() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("owner");
        let winner = SigningKeyReference::create()?;
        let winner_ref = winner.reference();
        assert!(private_file::publish_new(
            &path,
            &root.path().join("winner"),
            |file| file.write_all(b"winner"),
            |stage| {
                winner.observe_publication(stage);
                Ok(())
            }
        )?);
        let loser = SigningKeyReference::create()?;
        let loser_ref = loser.reference();
        assert!(!private_file::publish_new(
            &path,
            &root.path().join("loser"),
            |file| file.write_all(b"loser"),
            |stage| {
                loser.observe_publication(stage);
                Ok(())
            }
        )?);
        drop(loser);
        drop(winner);
        assert!(matches!(
            loser_ref.entry()?.get_secret(),
            Err(keyring::Error::NoEntry)
        ));
        winner_ref.with_seed(|_| ())?;
        // Reopening existing custody does not give the new owner cleanup rights.
        let (existing, created) = winner_ref.with_seed(SigningKeyReference::retain_entry)??;
        assert!(!created);
        drop(ProvisionalSigningKey {
            reference: existing,
            cleanup: AtomicBool::new(created),
        });
        winner_ref.with_seed(|_| ())?;
        winner_ref
            .entry()?
            .delete_credential()
            .map_err(|_| unavailable())?;

        // Actual former create path retained an entry with no publication owner.
        let seed = Zeroizing::new(new_key()?);
        let leaked = SigningKeyReference::retain(&seed)?;
        assert!(leaked.with_seed(|_| ()).is_ok());
        leaked
            .entry()?
            .delete_credential()
            .map_err(|_| unavailable())?;
        let abandoned = SigningKeyReference::create()?;
        let abandoned_ref = abandoned.reference();
        drop(abandoned);
        assert!(matches!(
            abandoned_ref.entry()?.get_secret(),
            Err(keyring::Error::NoEntry)
        ));
        Ok(())
    }

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
