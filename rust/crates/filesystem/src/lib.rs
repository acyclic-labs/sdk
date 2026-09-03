//! Filesystem provider contract and deterministic in-memory implementation.

use acyclic_contracts::{Error, Result};
use async_trait::async_trait;
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::RwLock;
use uuid::Uuid;

/// An immutable filesystem generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Generation {
    /// Stable generation identity.
    pub id: String,
    /// Immutable path contents.
    pub files: BTreeMap<String, Vec<u8>>,
}

/// Provider interface implemented by memory, customer, and Acyclic backends.
#[async_trait]
pub trait FilesystemProvider: Send + Sync {
    /// Creates an empty workspace and returns its initial generation.
    async fn create(&self) -> Result<Generation>;
    /// Reads a known immutable generation.
    async fn get(&self, id: &str) -> Result<Generation>;
    /// Creates a derived immutable generation containing one write.
    async fn write(&self, base: &str, path: String, value: Vec<u8>) -> Result<Generation>;
    /// Joins non-conflicting changes relative to a common base.
    async fn join(&self, base: &str, children: &[String]) -> Result<Generation>;
}

/// Process-local reference provider.
#[derive(Clone, Default)]
pub struct MemoryFilesystem {
    generations: Arc<RwLock<BTreeMap<String, Generation>>>,
}

impl MemoryFilesystem {
    fn next(files: BTreeMap<String, Vec<u8>>) -> Generation {
        Generation {
            id: Uuid::new_v4().to_string(),
            files,
        }
    }
}

#[async_trait]
impl FilesystemProvider for MemoryFilesystem {
    async fn create(&self) -> Result<Generation> {
        let generation = Self::next(BTreeMap::new());
        self.generations
            .write()
            .await
            .insert(generation.id.clone(), generation.clone());
        Ok(generation)
    }

    async fn get(&self, id: &str) -> Result<Generation> {
        self.generations
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| Error::NotFound(id.into()))
    }

    async fn write(&self, base: &str, path: String, value: Vec<u8>) -> Result<Generation> {
        let mut files = self.get(base).await?.files;
        files.insert(path, value);
        let generation = Self::next(files);
        self.generations
            .write()
            .await
            .insert(generation.id.clone(), generation.clone());
        Ok(generation)
    }

    async fn join(&self, base: &str, children: &[String]) -> Result<Generation> {
        let base_files = self.get(base).await?.files;
        let mut joined = base_files.clone();
        let mut changes: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for child_id in children {
            for (path, value) in self.get(child_id).await?.files {
                if base_files.get(&path) == Some(&value) {
                    continue;
                }
                if let Some(previous) = changes.insert(path.clone(), value.clone())
                    && previous != value
                {
                    return Err(Error::Conflict(path));
                }
                joined.insert(path, value);
            }
        }
        let generation = Self::next(joined);
        self.generations
            .write()
            .await
            .insert(generation.id.clone(), generation.clone());
        Ok(generation)
    }
}
