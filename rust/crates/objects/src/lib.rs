//! Immutable Objects provider contract and in-memory implementation.

use acyclic_contracts::{Error, Result};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::RwLock;

/// Content-addressed immutable object reference.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRef {
    /// SHA-256 digest in lowercase hexadecimal.
    pub version: String,
}

/// Provider interface for immutable objects.
#[async_trait]
pub trait ObjectsProvider: Send + Sync {
    /// Stores bytes and returns their immutable reference.
    async fn put(&self, value: Vec<u8>) -> Result<ObjectRef>;
    /// Retrieves bytes by immutable reference.
    async fn get(&self, reference: &ObjectRef) -> Result<Vec<u8>>;
}

/// Process-local immutable object provider.
#[derive(Clone, Default)]
pub struct MemoryObjects {
    values: Arc<RwLock<BTreeMap<String, Vec<u8>>>>,
}

#[async_trait]
impl ObjectsProvider for MemoryObjects {
    async fn put(&self, value: Vec<u8>) -> Result<ObjectRef> {
        let version: String = Sha256::digest(&value)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        self.values.write().await.insert(version.clone(), value);
        Ok(ObjectRef { version })
    }

    async fn get(&self, reference: &ObjectRef) -> Result<Vec<u8>> {
        self.values
            .read()
            .await
            .get(&reference.version)
            .cloned()
            .ok_or_else(|| Error::NotFound(reference.version.clone()))
    }
}
