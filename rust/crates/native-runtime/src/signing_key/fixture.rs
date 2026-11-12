//! Fixture builds select an injected process-local or parent-hosted memory
//! store. Neither route constructs a native OS credential handle.
use super::{MemorySigningStore, SigningStore};
use keyring::{Error, credential::CredentialApi};
use std::{io, sync::OnceLock};
mod host;
pub use host::FixtureCustody;

pub fn store() -> io::Result<&'static MemorySigningStore> {
    static STORE: OnceLock<io::Result<MemorySigningStore>> = OnceLock::new();
    STORE
        .get_or_init(MemorySigningStore::new)
        .as_ref()
        .map_err(|error| io::Error::new(error.kind(), error.to_string()))
}

pub(super) fn entry(name: &str) -> io::Result<keyring::Entry> {
    match host::remote() {
        Some(remote) => Ok(keyring::Entry::new_with_credential(Box::new(Credential {
            name: name.into(),
            remote,
        }))),
        None => store()?.entry(name),
    }
}

#[derive(Debug)]
struct Credential {
    name: String,
    remote: host::Remote,
}

impl CredentialApi for Credential {
    fn set_secret(&self, secret: &[u8]) -> keyring::Result<()> {
        self.remote.request(1, &self.name, secret).map(|_| ())
    }
    fn get_secret(&self) -> keyring::Result<Vec<u8>> {
        self.remote.request(2, &self.name, &[])
    }
    fn delete_credential(&self) -> keyring::Result<()> {
        self.remote.request(3, &self.name, &[]).map(|_| ())
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
