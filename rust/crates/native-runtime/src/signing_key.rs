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
/// This choice is not serialized into the public signing-key reference.
#[derive(Clone, Copy)]
pub enum SigningCustody<'a> {
    /// Use the application's configured native or explicit file policy.
    Configured,
    /// Use the same one-root-entry protocol in this absolute directory.
    PrivateFile(&'a Path),
}

/// Non-secret owner reference: a random 16-byte owner nonce, then a 16-byte
/// commitment to the root it was derived from. A replaced or recreated root
/// is refused instead of silently deriving a different signing seed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SigningKeyReference([u8; 32]);

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
        let root = open_or_create(custody)?;
        let mut reference = [0; 32];
        crate::owner_record::fill_random(&mut reference[..16]).map_err(io::Error::other)?;
        reference[16..].copy_from_slice(&check(&root));
        Ok(Self(reference))
    }

    /// Borrow the derived seed only for bootstrap; it and the root are cleared
    /// on success, refusal or unwind. A missing root is never regenerated.
    pub fn with_seed<T>(
        &self,
        custody: SigningCustody<'_>,
        consume: impl FnOnce(&[u8; 32]) -> T,
    ) -> io::Result<T> {
        let root = read(&entry(custody)?.0)?
            .ok_or_else(|| failure("read", CustodyCause::MissingEntry))?;
        if !bool::from(check(&root)[..].ct_eq(&self.0[16..])) {
            return Err(unavailable());
        }
        let mut material = Zeroizing::new([0; 48]);
        material[..32].copy_from_slice(&root[..]);
        material[32..].copy_from_slice(&self.0[..16]);
        let seed = Zeroizing::new(blake3::derive_key(SEED_CONTEXT, &material[..]));
        Ok(consume(&seed))
    }
}

/// Read the root, or create it once under the store's creation discipline.
fn open_or_create(custody: SigningCustody<'_>) -> io::Result<Root> {
    let (entry, exclusive) = entry(custody)?;
    if let Some(root) = read(&entry)? {
        return Ok(root);
    }
    let _creation = (!exclusive).then(creation_lock).transpose()?;
    if let Some(root) = read(&entry)? {
        return Ok(root);
    }
    let root = Zeroizing::new(new_key()?);
    if let Err(error) = entry.set_secret(&root[..]) {
        // Only a no-replace store can lose a creation race; adopt its winner.
        return match exclusive.then(|| read(&entry)).transpose()?.flatten() {
            Some(winner) => Ok(winner),
            None => Err(error::backend("write", error)),
        };
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

/// The selected store's root entry, and whether that store installs it
/// without replacement (private files) rather than overwriting (native).
fn entry(custody: SigningCustody<'_>) -> io::Result<(keyring::Entry, bool)> {
    match custody {
        SigningCustody::Configured => store(),
        SigningCustody::PrivateFile(directory) => {
            Ok((file::entry_in(directory.to_owned())?, true))
        }
    }
}

/// One explicit process custody policy, shared by every native consumer and
/// every build. Private files require an explicit owner choice; failures
/// never change policy.
fn store() -> io::Result<(keyring::Entry, bool)> {
    match std::env::var_os("ACYCLIC_SIGNING_CUSTODY").as_deref() {
        None => {}
        Some(value) if value == "native" => {}
        Some(value) if value == "private-file" => return Ok((file::entry()?, true)),
        Some(_) => return Err(failure("select", CustodyCause::InvalidPolicy)),
    }
    if std::env::var_os("ACYCLIC_SIGNING_CUSTODY_DIRECTORY").is_some() {
        return Err(failure("select", CustodyCause::InvalidPolicy));
    }
    Ok((native()?, false))
}

/// Linux selects Secret Service with a session bus, otherwise the kernel store.
#[cfg(not(any(test, feature = "test-support")))]
fn native() -> io::Result<keyring::Entry> {
    #[cfg(target_os = "linux")]
    {
        if std::env::var_os("DBUS_SESSION_BUS_ADDRESS").is_some() {
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
fn native() -> io::Result<keyring::Entry> {
    Ok(fixture::entry())
}

#[cfg(any(test, feature = "test-support"))]
use fixture::creation_lock;

#[cfg(test)]
mod tests {
    use super::*;

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
            assert_eq!(
                references[0].with_seed(custody, |seed| *seed)?,
                seeds[0],
                "reopening changed an owner's seed"
            );
            seeds.sort_unstable();
            seeds.dedup();
            assert_eq!(seeds.len(), references.len(), "owners shared a seed");
        }
        assert_eq!(fixture::local_entries(), 1);
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
                    .map(|worker| worker.join().map_err(|_| io::Error::other("creator panicked"))?)
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
        let (entry, _) = entry(custody)?;
        let stored = read(&entry)?.ok_or_else(unavailable)?;
        assert_ne!(reference.0, *stored, "the reference carries the root");
        assert_ne!(reference.0, *seed, "the reference carries the seed");
        let other = root.path().join("other");
        for wrong in [SigningCustody::Configured, SigningCustody::PrivateFile(&other)] {
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
        assert!(SigningKeyReference::create(SigningCustody::PrivateFile(Path::new("relative"))).is_err());
        Ok(())
    }
}
