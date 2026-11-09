//! One credential entry per OS account holds a random signing root. Every
//! owner's signing seed is derived from that root and the owner's public
//! reference, so creating owners never adds entries and abandoning one leaves
//! nothing to retire. Persisted host metadata contains only the reference.
//! Production selects native custody unless the owner explicitly requests
//! private-file custody. Test and fixture builds replace only the native store.
//!
//! The root is created once, if absent, and adopted when present. A native
//! store overwrites on write, so creators serialize on the store's own lock
//! namespace; a private-file entry is installed without replacement, so a
//! losing creator adopts the winner's root. Reopening takes no lock.
use crate::owner_record::new_key;
use serde::{Deserialize, Serialize};
use std::{io, path::Path};
use subtle::ConstantTimeEq as _;
use zeroize::Zeroizing;

mod file;

#[cfg(not(any(test, feature = "test-support")))]
const SERVICE: &str = "acyclic.authority.signing-root.v2";
/// The account's only entry name, in every store.
const ROOT: &str = "root";
/// Derives an owner's signing seed from (root, owner nonce).
const SEED_CONTEXT: &str = "acyclic.authority 2026-11-06 owner signing seed v2";
/// Derives the public commitment that binds a reference to its root.
const CHECK_CONTEXT: &str = "acyclic.authority 2026-11-06 signing root check v2";

#[cfg(any(test, feature = "test-support"))]
pub(crate) mod fixture;
#[cfg(any(test, feature = "test-support"))]
pub use fixture::FixtureCustody;

/// Host-selected credential custody. The explicit file selection never consults
/// process settings or a native credential service and never falls back to one.
/// References bind the effective store without retaining its path or secrets.
#[derive(Clone, Copy)]
pub enum SigningCustody<'a> {
    /// Use the application's configured native or explicit file policy.
    Configured,
    /// Use the same one-root-entry protocol in this absolute directory.
    PrivateFile(&'a Path),
}

/// Non-secret owner reference: a random 16-byte owner nonce, then a 16-byte
/// commitment to the root it was derived from. A replaced or recreated root
/// is refused instead of silently deriving a different signing seed. Versioned
/// metadata also binds the effective custody store before any store access.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SigningKeyReference {
    // Owner nonce and root commitment, bound to the effective store.
    v1: ([u8; 32], [u8; 32]),
}

enum Store {
    Native { session_bus: bool },
    PrivateFile(std::path::PathBuf),
}

impl SigningCustody<'_> {
    /// Validate the configured policy without opening or creating any store.
    pub fn validate(self) -> io::Result<()> {
        self.resolve().map(drop)
    }

    fn resolve(self) -> io::Result<Store> {
        match self {
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

impl Store {
    fn identity(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"acyclic.signing-custody/v1\0");
        match self {
            Self::Native { session_bus } => {
                hash.update(b"native\0");
                hash.update(std::env::consts::OS.as_bytes());
                hash.update(&[u8::from(*session_bus)]);
            }
            Self::PrivateFile(directory) => {
                hash.update(b"private-file\0");
                // Lexical normalization requires no filesystem access, including
                // before a newly selected directory exists. Parent components
                // remain significant: resolving them could cross a symlink.
                for component in directory.components() {
                    let bytes = component.as_os_str().as_encoded_bytes();
                    hash.update(&(bytes.len() as u64).to_le_bytes());
                    hash.update(bytes);
                }
            }
        }
        *hash.finalize().as_bytes()
    }

    fn entry(self) -> io::Result<(keyring::Entry, bool)> {
        match self {
            Self::Native { session_bus } => Ok((native(session_bus)?, false)),
            Self::PrivateFile(directory) => Ok((file::entry_in(directory)?, true)),
        }
    }
}

mod error;
use error::failure;
pub use error::{CustodyCause, CustodyFailure};

fn unavailable() -> io::Error {
    failure("validate", CustodyCause::InvalidStoredSeed)
}

type Root = Zeroizing<[u8; 32]>;

fn check(root: &Root) -> [u8; 16] {
    let mut check = [0; 16];
    check.copy_from_slice(&blake3::derive_key(CHECK_CONTEXT, &root[..])[..16]);
    check
}

impl SigningKeyReference {
    /// Reference a new owner's signing seed in the selected store, creating
    /// the account's root entry only if it is absent. Store failure never
    /// selects a different custody policy.
    pub fn create(custody: SigningCustody<'_>) -> io::Result<Self> {
        let store = custody.resolve()?;
        let identity = store.identity();
        let root = open_or_create(store)?;
        let mut reference = [0; 32];
        crate::owner_record::fill_random(&mut reference[..16]).map_err(io::Error::other)?;
        reference[16..].copy_from_slice(&check(&root));
        Ok(Self {
            v1: (reference, identity),
        })
    }

    /// Check the bound policy without opening or creating a custody store.
    pub fn validate_custody(&self, custody: SigningCustody<'_>) -> io::Result<()> {
        self.validate_store(&custody.resolve()?)
    }

    fn validate_store(&self, store: &Store) -> io::Result<()> {
        if self.v1.1 != store.identity() {
            return Err(failure("select", CustodyCause::StoreMismatch));
        }
        Ok(())
    }

    /// Borrow the derived seed only for bootstrap; it and the root are cleared
    /// on success, refusal or unwind. A missing root is never regenerated.
    pub fn with_seed<T>(
        &self,
        custody: SigningCustody<'_>,
        consume: impl FnOnce(&[u8; 32]) -> T,
    ) -> io::Result<T> {
        let store = custody.resolve()?;
        self.validate_store(&store)?;
        let root =
            read(&store.entry()?.0)?.ok_or_else(|| failure("read", CustodyCause::MissingEntry))?;
        if !bool::from(check(&root)[..].ct_eq(&self.v1.0[16..])) {
            return Err(unavailable());
        }
        let mut material = Zeroizing::new([0; 48]);
        let (root_material, nonce_material) = material.split_at_mut(32);
        root_material.copy_from_slice(&root[..]);
        nonce_material.copy_from_slice(&self.v1.0[..16]);
        let seed = Zeroizing::new(blake3::derive_key(SEED_CONTEXT, &material[..]));
        Ok(consume(&seed))
    }
}

/// Read the root, or create it once under the store's creation discipline.
fn open_or_create(store: Store) -> io::Result<Root> {
    let (entry, exclusive) = store.entry()?;
    if let Some(root) = read(&entry)? {
        return Ok(root);
    }
    let _creation = (!exclusive).then(creation_lock).transpose()?;
    if !exclusive && let Some(root) = read(&entry)? {
        return Ok(root);
    }
    let root = Zeroizing::new(new_key()?);
    if let Err(error) = entry.set_secret(&root[..]) {
        // A visible root does not acknowledge publication's durability. Only
        // an actual no-replace collision establishes a competing creator.
        if exclusive
            && matches!(&error, keyring::Error::PlatformFailure(source)
                if source.downcast_ref::<io::Error>()
                    .is_some_and(|error| error.kind() == io::ErrorKind::AlreadyExists))
        {
            return read(&entry)?.ok_or_else(unavailable);
        }
        return Err(error::backend("write", error));
    }
    // Observe the stored value before any reference to it can be published.
    match read(&entry)? {
        Some(stored) if bool::from(stored[..].ct_eq(&root[..])) => Ok(root),
        _ => Err(unavailable()),
    }
}

/// Probe exactly the selected store. A refusal is never treated as absence.
fn read(entry: &keyring::Entry) -> io::Result<Option<Root>> {
    match entry.get_secret() {
        Ok(secret) => {
            let secret = Zeroizing::new(secret);
            let root: [u8; 32] = secret.as_slice().try_into().map_err(|_| unavailable())?;
            Ok(Some(Zeroizing::new(root)))
        }
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(error) => Err(error::backend("read", error)),
    }
}

/// Linux selects Secret Service with a session bus, otherwise the kernel store.
#[cfg(not(any(test, feature = "test-support")))]
fn native(_session_bus: bool) -> io::Result<keyring::Entry> {
    #[cfg(target_os = "linux")]
    {
        if _session_bus {
            let service =
                keyring::secret_service::SsCredential::new_with_target(None, SERVICE, ROOT)
                    .map_err(|error| error::backend("open", error))?;
            Ok(keyring::Entry::new_with_credential(Box::new(service)))
        } else {
            Ok(keyring::Entry::new_with_credential(Box::new(
                crate::kernel_keyring::KernelKey::new(SERVICE, ROOT),
            )))
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        keyring::Entry::new(SERVICE, ROOT).map_err(|error| error::backend("open", error))
    }
}

/// Native root creation serializes on the OS account's lock namespace.
#[cfg(not(any(test, feature = "test-support")))]
fn creation_lock() -> io::Result<crate::FileLock> {
    crate::FileLock::acquire(
        crate::private_file::credential_lock()?,
        crate::FileLockMode::Exclusive,
    )
}

/// Test and fixture builds stand the in-memory fixture store in for the OS
/// store only; an explicit private-file selection reopens across processes.
#[cfg(any(test, feature = "test-support"))]
fn native(_session_bus: bool) -> io::Result<keyring::Entry> {
    Ok(fixture::entry())
}

#[cfg(any(test, feature = "test-support"))]
use fixture::creation_lock;

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "test-support")]
    #[test]
    fn private_root_creation_preserves_the_original_publication_failure() -> io::Result<()> {
        use crate::testing::{DurableFault, Schedule};
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        for fault in [DurableFault::Before, DurableFault::After] {
            let root = tempfile::tempdir()?;
            let directory = root.path().join("custody");
            let (schedule, _) = Schedule::seeded(71);
            // The original file sync is followed by the no-replace rename.
            schedule.cut_at(1, fault);
            let failure = runtime
                .block_on(schedule.scope(async {
                    SigningKeyReference::create(SigningCustody::PrivateFile(&directory))
                }))
                .expect_err("a visible root cannot acknowledge failed publication");
            let cause = failure
                .get_ref()
                .and_then(|error| error.downcast_ref::<CustodyFailure>())
                .expect("publication retains its typed custody failure");
            assert_eq!(cause.operation, "write");
            assert_eq!(directory.join(ROOT).is_file(), fault == DurableFault::After);
            let original = schedule.storage_fault().expect("the physical cut ran");
            assert_eq!(original.operation, "namespace:rename");
            assert_eq!(original.ordinal, 1);
            assert_eq!(original.fault, fault);
        }
        Ok(())
    }

    /// Growth guard: owners in any number of fresh state directories share one
    /// entry per store. The former per-owner protocol added one entry each.
    #[test]
    fn owners_share_one_root_entry_and_derive_distinct_seeds() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().join("custody");
        for custody in [
            SigningCustody::Configured,
            SigningCustody::PrivateFile(&directory),
        ] {
            let references = (0..8)
                .map(|_| SigningKeyReference::create(custody))
                .collect::<io::Result<Vec<_>>>()?;
            let mut seeds = references
                .iter()
                .map(|reference| reference.with_seed(custody, |seed| *seed))
                .collect::<io::Result<Vec<_>>>()?;
            for (reference, seed) in references.iter().zip(&seeds) {
                assert_eq!(
                    reference.with_seed(custody, |seed| *seed)?,
                    *seed,
                    "reopening changed an owner's seed"
                );
            }
            seeds.sort_unstable();
            seeds.dedup();
            assert_eq!(seeds.len(), references.len(), "owners shared a seed");
        }
        assert_eq!(std::fs::read_dir(&directory)?.count(), 1);
        Ok(())
    }

    /// Concurrent first creators converge on one root in the no-replace store
    /// and in the overwriting store guarded by the creation lock.
    #[test]
    fn concurrent_first_creation_adopts_one_root() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().join("custody");
        for custody in [
            SigningCustody::Configured,
            SigningCustody::PrivateFile(&directory),
        ] {
            let start = std::sync::Barrier::new(8);
            let references = std::thread::scope(|scope| {
                let workers = (0..8)
                    .map(|_| {
                        scope.spawn(|| {
                            start.wait();
                            SigningKeyReference::create(custody)
                        })
                    })
                    .collect::<Vec<_>>();
                workers
                    .into_iter()
                    .map(|worker| {
                        worker
                            .join()
                            .map_err(|_| io::Error::other("creator panicked"))?
                    })
                    .collect::<io::Result<Vec<_>>>()
            })?;
            for reference in references {
                reference.with_seed(custody, |_| ())?;
            }
        }
        assert_eq!(std::fs::read_dir(&directory)?.count(), 1);
        Ok(())
    }

    /// Preserve the selected custody policy: metadata never carries the root
    /// or seed, another store never stands in, and a replaced or missing root
    /// is refused rather than regenerated.
    #[test]
    fn custody_refuses_other_stores_and_replaced_or_missing_roots() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let directory = root.path().join("custody");
        let custody = SigningCustody::PrivateFile(&directory);
        let reference = SigningKeyReference::create(custody)?;
        let seed = Zeroizing::new(reference.with_seed(custody, |seed| *seed)?);
        let (entry, _) = custody.resolve()?.entry()?;
        let stored = read(&entry)?.ok_or_else(unavailable)?;
        assert_ne!(reference.v1.0, *stored, "the reference carries the root");
        assert_ne!(reference.v1.0, *seed, "the reference carries the seed");
        let other = root.path().join("other");
        for wrong in [
            SigningCustody::Configured,
            SigningCustody::PrivateFile(&other),
        ] {
            assert!(
                reference
                    .with_seed(wrong, |_| panic!("custody fell back to another store"))
                    .is_err()
            );
        }
        // File custody refuses overwrites. Replace the entry through deletion
        // and creation, as an external custodian could.
        entry.delete_credential().map_err(|_| unavailable())?;
        entry.set_secret(&new_key()?).map_err(|_| unavailable())?;
        assert!(
            reference
                .with_seed(custody, |_| panic!("replaced root reached bootstrap"))
                .is_err()
        );
        entry.delete_credential().map_err(|_| unavailable())?;
        assert!(
            reference
                .with_seed(custody, |_| panic!("missing root was regenerated"))
                .is_err()
        );
        assert!(read(&entry)?.is_none());
        assert!(
            SigningKeyReference::create(SigningCustody::PrivateFile(Path::new("relative")))
                .is_err()
        );
        Ok(())
    }
}
