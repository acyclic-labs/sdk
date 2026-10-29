//! One credential entry owns a signing seed; persisted host metadata contains
//! only its public opaque reference. Production selects native custody unless
//! the owner explicitly requests private-file custody. Test and fixture
//! builds use process-local custody.
//!
//! A freshly generated seed needs no shared lock namespace: its identity is
//! new entropy that no other process can name before publication. Only an
//! import of an existing seed, whose identity other owners may already hold,
//! serializes on the OS account's own lock namespace (`credential_lock`).
use crate::owner_record::{SecretBytes, new_key};
use serde::{Deserialize, Serialize};
use std::{
    io,
    sync::atomic::{AtomicBool, Ordering},
};
use subtle::ConstantTimeEq as _;
use zeroize::Zeroizing;

#[cfg(any(test, not(feature = "test-support")))]
mod file;

#[cfg(not(any(test, feature = "test-support")))]
const SERVICE: &str = "acyclic.authority.signing-seed.v1";

#[cfg(any(test, feature = "test-support"))]
pub(crate) mod fixture;
#[cfg(any(test, feature = "test-support"))]
pub use fixture::FixtureCustody;

/// Non-secret, content-bound reference to OS-owned signing custody.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SigningKeyReference([u8; 32]);

/// A newly created entry belongs to its unpublished owner. Abandonment removes
/// that entry; an existing entry and a published owner's entry are never retired.
pub struct ProvisionalSigningKey {
    reference: SigningKeyReference,
    cleanup: AtomicBool,
    entry: keyring::Entry,
    // Declared after the entry: Drop deletes abandoned imported custody while
    // its shared credential identity remains exclusively held across processes.
    // A generated identity is exclusive by construction and holds no lock.
    _transaction: Option<crate::FileLock>,
}

enum Selection {
    Held(keyring::Entry, SecretBytes),
    Vacant(keyring::Entry),
}

mod error;
use error::failure;
pub use error::{CustodyCause, CustodyFailure};

#[cfg(test)]
thread_local! {
    static TRY_TRANSACTION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
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
            // Keep the original backend handle: cleanup must not require another
            // successful read or select a different store after a failed write.
            let _ = self.entry.delete_credential();
        }
    }
}

fn unavailable() -> io::Error {
    failure("validate", CustodyCause::InvalidStoredSeed)
}

impl SigningKeyReference {
    /// Generate a root seed once using the shared entropy seam and retain it in
    /// the explicitly selected custody store before publishing its reference.
    /// The fresh identity is unknown to every other process until this owner
    /// publishes its reference, so no cross-process identity lock is needed.
    /// Native-store failure never selects a different custody policy.
    pub fn create() -> io::Result<ProvisionalSigningKey> {
        let seed = Zeroizing::new(new_key()?);
        Self::admit(&seed, None)
    }

    /// Migrate an original seed read from an authenticated private descriptor.
    /// This preserves its authority rather than generating replacement credentials.
    /// The caller must keep this guard until actual owner publication. An
    /// unsuccessful publication retires newly acquired custody on every exit.
    pub fn retain(seed: &[u8; 32]) -> io::Result<ProvisionalSigningKey> {
        if bool::from(seed.ct_eq(&[0; 32])) {
            return Err(io::ErrorKind::InvalidData.into());
        }
        // Copied owner records can carry one seed into independent state
        // directories, so an import locks in the OS account's namespace.
        let name = blake3::hash(seed).to_hex();
        let file = crate::private_file::credential_lock(name.as_str())?;
        let acquire: fn(std::fs::File, crate::FileLockMode) -> io::Result<crate::FileLock> =
            crate::FileLock::acquire;
        #[cfg(test)]
        let acquire = if TRY_TRANSACTION.get() {
            crate::FileLock::try_acquire
        } else {
            acquire
        };
        let transaction = acquire(file, crate::FileLockMode::Exclusive)?;
        Self::admit(seed, Some(transaction))
    }

    /// Store `seed`, or adopt its identical existing entry, and observe the
    /// stored value before any reference can be published.
    fn admit(
        seed: &[u8; 32],
        transaction: Option<crate::FileLock>,
    ) -> io::Result<ProvisionalSigningKey> {
        let reference = Self(*blake3::hash(seed).as_bytes());
        let entry = match reference.select()? {
            Selection::Held(entry, existing) => {
                if !bool::from(existing.0.as_slice().ct_eq(seed)) {
                    return Err(unavailable());
                }
                return Ok(ProvisionalSigningKey {
                    reference,
                    cleanup: AtomicBool::new(false),
                    entry,
                    _transaction: transaction,
                });
            }
            Selection::Vacant(entry) => entry,
        };
        // Retain the selected handle and arm cleanup before a possibly lost
        // write acknowledgement. No failure selects another backend.
        let mut key = ProvisionalSigningKey {
            reference,
            cleanup: AtomicBool::new(true),
            entry,
            _transaction: None,
        };
        key.entry
            .set_secret(seed)
            .map_err(|error| error::backend("write", error))?;
        let stored = SecretBytes(
            key.entry
                .get_secret()
                .map_err(|error| error::backend("verify", error))?,
        );
        if !bool::from(stored.0.as_slice().ct_eq(seed)) {
            return Err(unavailable());
        }
        key._transaction = transaction;
        Ok(key)
    }

    /// Borrow secret material only for bootstrap, then clear the OS output buffer
    /// on success, refusal or unwind. The reference never carries seed bytes.
    pub fn with_seed<T>(&self, consume: impl FnOnce(&[u8; 32]) -> T) -> io::Result<T> {
        let Selection::Held(_entry, secret) = self.select()? else {
            return Err(failure("read", CustodyCause::MissingEntry));
        };
        let seed: &[u8; 32] = secret.0.as_slice().try_into().map_err(|_| unavailable())?;
        if !bool::from(blake3::hash(seed).as_bytes().ct_eq(&self.0)) {
            return Err(unavailable());
        }
        Ok(consume(seed))
    }

    /// Probe exactly the selected store. A refusal is never treated as absence.
    fn select(&self) -> io::Result<Selection> {
        let name = blake3::Hash::from_bytes(self.0).to_hex();
        let entry = store(name.as_str())?;
        match entry.get_secret().map(SecretBytes) {
            Ok(held) => Ok(Selection::Held(entry, held)),
            Err(keyring::Error::NoEntry) => Ok(Selection::Vacant(entry)),
            Err(error) => Err(error::backend("read", error)),
        }
    }

    #[cfg(test)]
    fn entry(&self) -> io::Result<keyring::Entry> {
        match self.select()? {
            Selection::Held(entry, _held) => Ok(entry),
            Selection::Vacant(entry) => Ok(entry),
        }
    }
}

/// One explicit process custody policy, shared by every native consumer.
/// Linux selects Secret Service with a session bus, otherwise the kernel store.
/// Private files require an explicit owner choice; failures never change policy.
#[cfg(not(any(test, feature = "test-support")))]
fn store(name: &str) -> io::Result<keyring::Entry> {
    match std::env::var_os("ACYCLIC_SIGNING_CUSTODY").as_deref() {
        None => {}
        Some(value) if value == "native" => {}
        Some(value) if value == "private-file" => return file::entry(name),
        Some(_) => return Err(failure("select", CustodyCause::InvalidPolicy)),
    }
    if std::env::var_os("ACYCLIC_SIGNING_CUSTODY_DIRECTORY").is_some() {
        return Err(failure("select", CustodyCause::InvalidPolicy));
    }
    #[cfg(target_os = "linux")]
    {
        if std::env::var_os("DBUS_SESSION_BUS_ADDRESS").is_some() {
            let service =
                keyring::secret_service::SsCredential::new_with_target(None, SERVICE, name)
                    .map_err(|error| error::backend("open", error))?;
            Ok(keyring::Entry::new_with_credential(Box::new(service)))
        } else {
            Ok(keyring::Entry::new_with_credential(Box::new(
                crate::kernel_keyring::KernelKey::new(SERVICE, name),
            )))
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        keyring::Entry::new(SERVICE, name).map_err(|error| error::backend("open", error))
    }
}

#[cfg(any(test, feature = "test-support"))]
fn store(name: &str) -> io::Result<keyring::Entry> {
    Ok(fixture::entry(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::private_file::{self, PublicationStage};
    use std::io::Write as _;

    #[test]
    fn shared_seed_publication_survives_another_owner_abandoning() -> io::Result<()> {
        for former_unlocked in [true, false] {
            let root = tempfile::tempdir()?;
            let seed = Zeroizing::new(new_key()?);
            let first = SigningKeyReference::retain(&seed)?;
            let reference = first.reference();
            if former_unlocked {
                // Actual former transaction: its cleanup guard outlived the
                // acquisition, so another independently located owner entered.
                first
                    ._transaction
                    .as_ref()
                    .ok_or_else(|| io::Error::other("an import holds its identity lock"))?
                    .unlock()?;
            }
            TRY_TRANSACTION.set(true);
            let contender = SigningKeyReference::retain(&seed);
            TRY_TRANSACTION.set(false);
            let publish = |key: &ProvisionalSigningKey| -> io::Result<()> {
                let bytes = serde_json::to_vec(&key.reference()).map_err(io::Error::other)?;
                assert!(private_file::publish_new(
                    &root.path().join("copied-owner"),
                    &root.path().join("copied-next"),
                    |file| file.write_all(&bytes),
                    |stage| {
                        key.observe_publication(stage);
                        Ok(())
                    },
                )?);
                Ok(())
            };
            if former_unlocked {
                let contender = contender?;
                publish(&contender)?;
            } else {
                assert!(
                    matches!(contender, Err(error) if error.kind() == io::ErrorKind::WouldBlock)
                );
            }
            let failed = private_file::publish_new(
                &root.path().join("original-owner"),
                &root.path().join("original-next"),
                |_| Err(io::ErrorKind::Interrupted.into()),
                |stage| {
                    first.observe_publication(stage);
                    Ok(())
                },
            );
            assert!(failed.is_err());
            drop(first);
            if !former_unlocked {
                let contender = SigningKeyReference::retain(&seed)?;
                publish(&contender)?;
            }
            assert!(root.path().join("copied-owner").is_file());
            let reopened = reference.with_seed(|stored| bool::from(stored.ct_eq(&*seed)));
            assert_eq!(
                reopened.is_ok(),
                !former_unlocked,
                "abandonment deleted another published owner's credential"
            );
            if !former_unlocked {
                assert!(reopened?);
                reference
                    .entry()?
                    .delete_credential()
                    .map_err(|_| unavailable())?;
            }
        }
        Ok(())
    }

    /// GraphCoder startup creates custody under a read-only account profile.
    /// A generated identity opens no lock path; an import of that same seed,
    /// the negative control, still creates its account-namespace lock.
    #[test]
    fn generated_custody_opens_no_identity_lock_namespace() -> io::Result<()> {
        let key = SigningKeyReference::create()?;
        let suffix = format!(
            "-{}.lock",
            blake3::Hash::from_bytes(key.reference().0).to_hex()
        );
        let locks = || -> io::Result<usize> {
            let mut count = 0;
            for entry in std::fs::read_dir(std::env::temp_dir())? {
                count += usize::from(entry?.file_name().to_string_lossy().ends_with(&suffix));
            }
            Ok(count)
        };
        assert_eq!(locks()?, 0, "generated custody opened an identity lock");
        let imported = key.reference().with_seed(SigningKeyReference::retain)??;
        assert_eq!(locks()?, 1, "an import must serialize on its identity lock");
        drop(imported);
        drop(key);
        Ok(())
    }

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
        let existing = winner_ref.with_seed(SigningKeyReference::retain)??;
        assert!(!existing.cleanup.load(Ordering::Acquire));
        drop(existing);
        winner_ref.with_seed(|_| ())?;
        winner_ref
            .entry()?
            .delete_credential()
            .map_err(|_| unavailable())?;

        // Actual former create path retained an entry with no publication owner.
        let seed = Zeroizing::new(new_key()?);
        let former = SigningKeyReference::retain(&seed)?;
        let leaked = former.reference();
        // The former bare-reference API had no abandonment owner.
        former.cleanup.store(false, Ordering::Release);
        drop(former);
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

    /// The custody contract runs against the same in-process fixture backend as
    /// integration tests, never the user's real OS credential service.
    #[test]
    fn custody_retains_original_seed_and_rejects_missing_or_replaced_entries() -> io::Result<()> {
        let seed = Zeroizing::new(new_key()?);
        let key = SigningKeyReference::retain(&seed)?;
        let reference = key.reference();
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

    #[test]
    fn abandoned_keys_and_failed_custody_admission_leave_zero_entries() -> io::Result<()> {
        let mut references = Vec::new();
        for _ in 0..256 {
            let key = SigningKeyReference::create()?;
            references.push(key.reference());
            drop(key);

            for fault in [
                fixture::Fault::StoredRead,
                fixture::Fault::WriteAcknowledgement,
            ] {
                let seed = Zeroizing::new(new_key()?);
                let reference = SigningKeyReference(*blake3::hash(&*seed).as_bytes());
                let name = blake3::Hash::from_bytes(reference.0).to_hex();
                fixture::fail_once(name.as_str(), fault);
                assert!(SigningKeyReference::retain(&seed).is_err());
                references.push(reference);
            }
        }
        let retained = references.iter().try_fold(0, |count, reference| {
            Ok::<_, io::Error>(count + usize::from(reference.entry()?.get_secret().is_ok()))
        })?;
        assert_eq!(retained, 0, "abandoned or refused custody leaked an entry");
        Ok(())
    }
}
