//! Isolated, zeroizing in-memory credential custody injected by the host.
use super::SigningStore;
use keyring::{Error, credential::CredentialApi};
use std::{
    collections::BTreeMap,
    io,
    sync::{Arc, Mutex},
};
use zeroize::Zeroizing;

type Secrets = BTreeMap<String, Zeroizing<Vec<u8>>>;

/// An isolated credential store whose secret bytes are zeroized on removal.
pub struct MemorySigningStore {
    identity: [u8; 32],
    secrets: Arc<Mutex<Secrets>>,
}

impl MemorySigningStore {
    /// Creates an empty store with a fresh, independent custody identity.
    pub fn new() -> io::Result<Self> {
        Ok(Self {
            identity: crate::owner_record::new_key()?,
            secrets: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Returns the number of credentials currently held by this store.
    pub fn len(&self) -> usize {
        self.secrets.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
    /// Reports whether this store holds no credentials.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl SigningStore for MemorySigningStore {
    fn identity(&self) -> [u8; 32] {
        self.identity
    }
    fn entry(&self, name: &str) -> io::Result<keyring::Entry> {
        Ok(keyring::Entry::new_with_credential(Box::new(Credential {
            name: name.into(),
            secrets: Arc::clone(&self.secrets),
        })))
    }
}

struct Credential {
    name: String,
    secrets: Arc<Mutex<Secrets>>,
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str("MemorySigningCredential")
    }
}

impl CredentialApi for Credential {
    fn set_secret(&self, secret: &[u8]) -> keyring::Result<()> {
        self.secrets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(self.name.clone(), Zeroizing::new(secret.to_vec()));
        Ok(())
    }
    fn get_secret(&self) -> keyring::Result<Vec<u8>> {
        self.secrets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&self.name)
            .map(|v| v.to_vec())
            .ok_or(Error::NoEntry)
    }
    fn delete_credential(&self) -> keyring::Result<()> {
        self.secrets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.name)
            .map(|_| ())
            .ok_or(Error::NoEntry)
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
