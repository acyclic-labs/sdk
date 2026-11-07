//! Shared process-local credential backend for every test/fixture caller. Entry
//! handles reopen the same zeroizing value, just as the production stores do.
use keyring::{Error, credential::CredentialApi};
use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
};
use zeroize::Zeroizing;

mod host;
pub use host::FixtureCustody;

type Secrets = BTreeMap<String, Zeroizing<Vec<u8>>>;

fn secrets() -> &'static Mutex<Secrets> {
    static SECRETS: OnceLock<Mutex<Secrets>> = OnceLock::new();
    SECRETS.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// Entries of this process's own store, excluding hosted fixture children.
#[cfg(test)]
pub(super) fn local_entries() -> usize {
    secrets()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .keys()
        .filter(|name| !name.contains(':'))
        .count()
}

/// The fixture store's creation lock follows its custody boundary: a hosted
/// store's children share its directory; a process-local store is one process.
pub(super) struct CreationLock {
    _hosted: Option<crate::FileLock>,
    _local: Option<std::sync::MutexGuard<'static, ()>>,
}

pub(super) fn creation_lock() -> std::io::Result<CreationLock> {
    if let Some(directory) = host::lock_directory()? {
        let file = crate::private_file::open_or_create(&directory.join("root.lock"))?;
        return Ok(CreationLock {
            _hosted: Some(crate::FileLock::acquire(file, crate::FileLockMode::Exclusive)?),
            _local: None,
        });
    }
    static LOCAL: Mutex<()> = Mutex::new(());
    Ok(CreationLock {
        _hosted: None,
        _local: Some(LOCAL.lock().unwrap_or_else(|error| error.into_inner())),
    })
}

#[derive(Debug)]
struct Credential {
    name: String,
    remote: Option<host::Remote>,
}

pub(super) fn entry() -> keyring::Entry {
    keyring::Entry::new_with_credential(Box::new(Credential {
        name: super::ROOT.into(),
        remote: host::remote(),
    }))
}

impl CredentialApi for Credential {
    fn set_secret(&self, secret: &[u8]) -> keyring::Result<()> {
        if let Some(remote) = &self.remote {
            return remote.request(1, secret).map(|_| ());
        }
        secrets()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(self.name.clone(), Zeroizing::new(secret.to_vec()));
        Ok(())
    }

    fn get_secret(&self) -> keyring::Result<Vec<u8>> {
        if let Some(remote) = &self.remote {
            return remote.request(2, &[]);
        }
        let secrets = secrets().lock().unwrap_or_else(|error| error.into_inner());
        let secret = secrets.get(&self.name).ok_or(Error::NoEntry)?;
        Ok(secret.to_vec())
    }

    fn delete_credential(&self) -> keyring::Result<()> {
        if let Some(remote) = &self.remote {
            return remote.request(3, &[]).map(|_| ());
        }
        secrets()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&self.name)
            .map(|_| ())
            .ok_or(Error::NoEntry)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
