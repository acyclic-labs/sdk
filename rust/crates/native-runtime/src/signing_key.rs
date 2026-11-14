//! Store-owned signing seeds. Only public, content-bound references reach owner
//! metadata. An unpublished seed is retired on abandonment; deleting a store
//! retires its exact entry. Credential adapters are injected at the boundary.
use crate::owner_record::new_key;
use serde::{Deserialize, Serialize};
use std::{
    io,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
};
use subtle::ConstantTimeEq as _;
use zeroize::Zeroizing;

mod file;
mod memory;
pub use memory::MemorySigningStore;
mod error;
use error::failure;
pub use error::{CustodyCause, CustodyFailure};

#[cfg(not(any(test, feature = "test-support")))]
const SERVICE: &str = "acyclic.authority.signing-seed.v2";

#[cfg(any(test, feature = "test-support"))]
pub(crate) mod fixture;
#[cfg(any(test, feature = "test-support"))]
pub use fixture::FixtureCustody;

/// A host-owned credential namespace. Entries implement the ordinary keyring
/// contract; tests inject an isolated memory store through the same boundary.
pub trait SigningStore: Send + Sync {
    /// Stable public identity of this custody namespace, never a secret.
    fn identity(&self) -> [u8; 32];
    /// Select an opaque entry without reading or creating its value.
    fn entry(&self, name: &str) -> io::Result<keyring::Entry>;
}

/// Explicit custody never falls back to an ambient credential service.
#[derive(Clone, Copy)]
pub enum SigningCustody<'a> {
    /// The application's configured native or explicit private-file policy.
    Configured,
    /// Private entries in this absolute directory.
    PrivateFile(&'a Path),
    /// Host-injected custody; no native credential API is consulted.
    Store(&'a dyn SigningStore),
}

/// A non-secret seed commitment bound to its selected credential namespace.
/// Version two does not derive store keys from an account-wide signing root.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SigningKeyReference {
    v2: ([u8; 32], [u8; 32]),
}

/// A new entry belongs to its unpublished store. Publication transfers custody
/// at the actual namespace installation, including a lost durability response.
pub struct ProvisionalSigningKey {
    reference: SigningKeyReference,
    entry: keyring::Entry,
    cleanup: AtomicBool,
}

impl ProvisionalSigningKey {
    /// Identifies the unpublished credential without exposing its secret.
    pub fn reference(&self) -> SigningKeyReference {
        self.reference
    }

    /// Transfers cleanup custody once namespace installation is observed.
    pub fn observe_publication(&self, stage: crate::private_file::PublicationStage) {
        if stage == crate::private_file::PublicationStage::Installed {
            self.cleanup.store(false, Ordering::Release);
        }
    }

    /// Temporary stores retire published custody with their existing resource
    /// lifetime. Unpublished entries still retire immediately on abandonment.
    pub fn retire_with(self, owner: crate::OwnershipAnchor) {
        if !self.cleanup.load(Ordering::Acquire) {
            self.cleanup.store(true, Ordering::Release);
            crate::OwnershipAnchor::fenced(owner, self);
        }
    }
}

impl Drop for ProvisionalSigningKey {
    fn drop(&mut self) {
        if self.cleanup.load(Ordering::Acquire) {
            let _ = self.entry.delete_credential();
        }
    }
}

enum Store<'a> {
    Native { session_bus: bool },
    PrivateFile(std::path::PathBuf),
    Injected(&'a dyn SigningStore),
}

impl<'a> SigningCustody<'a> {
    /// Validate policy without opening or creating a credential entry.
    pub fn validate(self) -> io::Result<()> {
        self.resolve().map(drop)
    }

    fn resolve(self) -> io::Result<Store<'a>> {
        match self {
            Self::Store(store) => Ok(Store::Injected(store)),
            Self::PrivateFile(directory) if directory.is_absolute() => {
                Ok(Store::PrivateFile(directory.to_owned()))
            }
            Self::PrivateFile(_) => Err(failure("select", CustodyCause::InvalidPolicy)),
            Self::Configured => {
                match std::env::var_os("ACYCLIC_SIGNING_CUSTODY").as_deref() {
                    Some(value) if value == "private-file" => {
                        return file::directory().map(Store::PrivateFile);
                    }
                    None => {}
                    Some(value) if value == "native" => {}
                    Some(_) => return Err(failure("select", CustodyCause::InvalidPolicy)),
                }
                if std::env::var_os("ACYCLIC_SIGNING_CUSTODY_DIRECTORY").is_some() {
                    return Err(failure("select", CustodyCause::InvalidPolicy));
                }
                Ok(Store::Native {
                    session_bus: cfg!(target_os = "linux")
                        && std::env::var_os("DBUS_SESSION_BUS_ADDRESS").is_some(),
                })
            }
        }
    }
}

impl Store<'_> {
    fn identity(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic.signing-custody/v2\0");
        match self {
            Self::Native { session_bus } => {
                hash.update(b"native\0");
                hash.update(std::env::consts::OS.as_bytes());
                hash.update(&[u8::from(*session_bus)]);
            }
            Self::PrivateFile(directory) => {
                hash.update(b"private-file\0");
                for component in directory.components() {
                    let bytes = component.as_os_str().as_encoded_bytes();
                    hash.update(&(bytes.len() as u64).to_le_bytes());
                    hash.update(bytes);
                }
            }
            Self::Injected(store) => return store.identity(),
        }
        *hash.finalize().as_bytes()
    }

    fn entry(&self, name: &str) -> io::Result<keyring::Entry> {
        match self {
            Self::Native { session_bus } => native(*session_bus, name),
            Self::PrivateFile(directory) => file::entry_in(directory.clone(), name.to_owned()),
            Self::Injected(store) => store.entry(name),
        }
    }
}

impl SigningKeyReference {
    /// Create one seed for a new store, retaining cleanup until its owner
    /// record is installed. Existing stores reopen their persisted reference.
    pub fn create(custody: SigningCustody<'_>) -> io::Result<ProvisionalSigningKey> {
        let store = custody.resolve()?;
        let seed = Zeroizing::new(new_key()?);
        let reference = Self {
            v2: (*blake3::hash(&seed[..]).as_bytes(), store.identity()),
        };
        let entry = store.entry(blake3::Hash::from_bytes(reference.v2.0).to_hex().as_str())?;
        // Native writes overwrite. Never arm cleanup for an already-owned
        // entry, even when a deterministic entropy source repeats an identity.
        match entry.get_secret() {
            Ok(secret) => {
                let _secret = Zeroizing::new(secret);
                return Err(failure("create", CustodyCause::InvalidEntry));
            }
            Err(keyring::Error::NoEntry) => {}
            Err(error) => return Err(error::backend("read", error)),
        }
        // The seed's cryptographic identity is fresh before publication; no
        // account-wide creation lock or shared root is needed.
        let provisional = ProvisionalSigningKey {
            reference,
            entry,
            cleanup: AtomicBool::new(true),
        };
        provisional
            .entry
            .set_secret(&seed[..])
            .map_err(|error| error::backend("write", error))?;
        Ok(provisional)
    }

    /// Rejects custody from a store other than the credential's original store.
    pub fn validate_custody(&self, custody: SigningCustody<'_>) -> io::Result<()> {
        self.validate_store(&custody.resolve()?)
    }

    fn validate_store(&self, store: &Store<'_>) -> io::Result<()> {
        if self.v2.1 != store.identity() {
            return Err(failure("select", CustodyCause::StoreMismatch));
        }
        Ok(())
    }

    /// The exact persisted seed is zeroized on success, refusal and unwind.
    pub fn with_seed<T>(
        &self,
        custody: SigningCustody<'_>,
        consume: impl FnOnce(&[u8; 32]) -> T,
    ) -> io::Result<T> {
        let store = custody.resolve()?;
        self.validate_store(&store)?;
        let entry = store.entry(blake3::Hash::from_bytes(self.v2.0).to_hex().as_str())?;
        let secret = Zeroizing::new(
            entry
                .get_secret()
                .map_err(|error| error::backend("read", error))?,
        );
        let seed: &[u8; 32] = secret
            .as_slice()
            .try_into()
            .map_err(|_| failure("read", CustodyCause::InvalidStoredSeed))?;
        if !bool::from(blake3::hash(seed).as_bytes().ct_eq(&self.v2.0)) {
            return Err(failure("read", CustodyCause::InvalidStoredSeed));
        }
        Ok(consume(seed))
    }

    /// Remove only this store's entry, without creating or regenerating it.
    /// Repeating an interrupted store deletion is safe when it is already gone.
    pub fn delete(self, custody: SigningCustody<'_>) -> io::Result<()> {
        let store = custody.resolve()?;
        self.validate_store(&store)?;
        let entry = store.entry(blake3::Hash::from_bytes(self.v2.0).to_hex().as_str())?;
        match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(error) => Err(error::backend("delete", error)),
        }
    }
}

#[cfg(not(any(test, feature = "test-support")))]
fn native(_session_bus: bool, name: &str) -> io::Result<keyring::Entry> {
    #[cfg(target_os = "linux")]
    {
        if _session_bus {
            let credential =
                keyring::secret_service::SsCredential::new_with_target(None, SERVICE, name)
                    .map_err(|error| error::backend("open", error))?;
            Ok(keyring::Entry::new_with_credential(Box::new(credential)))
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

// Fixture builds cannot link this selection to the real native credential API.
#[cfg(any(test, feature = "test-support"))]
fn native(_session_bus: bool, name: &str) -> io::Result<keyring::Entry> {
    fixture::entry(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "test-support")]
    #[test]
    fn repeated_entropy_never_retires_a_published_seed() -> io::Result<()> {
        let store = MemorySigningStore::new()?;
        let custody = SigningCustody::Store(&store);
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let (first, _) = crate::testing::Schedule::seeded(71);
        let signing =
            runtime.block_on(first.scope(async { SigningKeyReference::create(custody) }))?;
        let reference = signing.reference();
        signing.observe_publication(crate::private_file::PublicationStage::Installed);
        drop(signing);
        let seed = Zeroizing::new(reference.with_seed(custody, |seed| *seed)?);
        let (repeated, _) = crate::testing::Schedule::seeded(71);
        assert!(
            runtime
                .block_on(repeated.scope(async { SigningKeyReference::create(custody) }))
                .is_err()
        );
        assert_eq!(store.len(), 1);
        assert_eq!(reference.with_seed(custody, |seed| *seed)?, *seed);
        Ok(())
    }

    #[test]
    fn owner_installation_is_the_custody_transfer_boundary() -> io::Result<()> {
        use crate::private_file::{self, PublicationStage};
        for cut in [
            PublicationStage::Created,
            PublicationStage::Synced,
            PublicationStage::Installed,
            PublicationStage::Published,
        ] {
            let directory = tempfile::tempdir()?;
            let store = MemorySigningStore::new()?;
            let signing = SigningKeyReference::create(SigningCustody::Store(&store))?;
            let reference = signing.reference();
            let record = serde_json::to_vec(&reference)?;
            let result = private_file::publish_new(
                &directory.path().join("owner"),
                &directory.path().join("next"),
                |file| crate::write_file_all(file, &record),
                |stage| {
                    signing.observe_publication(stage);
                    if stage == cut {
                        Err(io::ErrorKind::Interrupted.into())
                    } else {
                        Ok(())
                    }
                },
            );
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
            drop(signing);
            let installed = matches!(
                cut,
                PublicationStage::Installed | PublicationStage::Published
            );
            assert_eq!(directory.path().join("owner").exists(), installed);
            assert_eq!(
                reference
                    .with_seed(SigningCustody::Store(&store), |_| ())
                    .is_ok(),
                installed
            );
        }
        Ok(())
    }

    #[test]
    fn store_custody_isolated_reopen_abandon_and_delete() -> io::Result<()> {
        let store = MemorySigningStore::new()?;
        let other = MemorySigningStore::new()?;
        let custody = SigningCustody::Store(&store);
        let first = SigningKeyReference::create(custody)?;
        let first_ref = first.reference();
        first.observe_publication(crate::private_file::PublicationStage::Installed);
        drop(first);
        let second = SigningKeyReference::create(custody)?;
        let second_ref = second.reference();
        second.observe_publication(crate::private_file::PublicationStage::Installed);
        drop(second);
        let first_seed = Zeroizing::new(first_ref.with_seed(custody, |seed| *seed)?);
        assert_ne!(*first_seed, second_ref.with_seed(custody, |seed| *seed)?);
        assert_eq!(*first_seed, first_ref.with_seed(custody, |seed| *seed)?);
        assert!(
            first_ref
                .with_seed(SigningCustody::Store(&other), |_| ())
                .is_err()
        );
        let abandoned = SigningKeyReference::create(custody)?;
        let abandoned_ref = abandoned.reference();
        drop(abandoned);
        assert!(abandoned_ref.with_seed(custody, |_| ()).is_err());
        first_ref.delete(custody)?;
        first_ref.delete(custody)?;
        assert!(first_ref.with_seed(custody, |_| ()).is_err());
        second_ref.with_seed(custody, |_| ())?;
        second_ref.delete(custody)?;
        assert_eq!(store.len(), 0);
        Ok(())
    }
}
