//! Disposable generation/path query acceleration.
//!
//! Generation identity and filesystem semantics never depend on this cache.
//! Missing, corrupt, or unwritable entries fall back to the authenticated
//! namespace traversal; complete results can then be reused by every context
//! sharing the same filesystem deployment.

use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
use crate::model::VolumeLimits;
use crate::{FileId, GenerationId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Mutex;

const PATH_QUERY_VERSION: u32 = 1;
const MAXIMUM_PATH_INDEX_ENTRIES: usize = 256;
const MAXIMUM_PATH_INDEX_RETAINED_BYTES: usize = 16 * 1024 * 1024;
const MAXIMUM_PATH_INDEX_VALUE_BYTES: usize = 1024 * 1024;

/// Exact cached answer for one sorted set of file identities in a generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct GenerationPathQuery {
    version: u32,
    generation: GenerationId,
    query: [u8; 32],
    entries: Vec<GenerationPathQueryEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct GenerationPathQueryEntry {
    file_id: FileId,
    paths: Vec<CachedPath>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct CachedPath(Vec<CachedName>);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct CachedName {
    encoding: u8,
    bytes: Vec<u8>,
}

impl CachedPath {
    pub(crate) fn from_namespace(path: &NamespacePath) -> Self {
        Self(
            path.components()
                .iter()
                .map(|name| CachedName {
                    encoding: match name.encoding() {
                        NameEncoding::Utf8 => 1,
                        NameEncoding::PosixBytes => 2,
                        NameEncoding::WindowsUtf16Le => 3,
                    },
                    bytes: name.as_bytes().to_vec(),
                })
                .collect(),
        )
    }

    pub(crate) fn into_namespace(self, limits: VolumeLimits) -> Option<NamespacePath> {
        let components = self
            .0
            .into_iter()
            .map(|name| {
                let encoding = match name.encoding {
                    1 => NameEncoding::Utf8,
                    2 => NameEncoding::PosixBytes,
                    3 => NameEncoding::WindowsUtf16Le,
                    _ => return None,
                };
                LogicalName::new(encoding, name.bytes, limits.maximum_component_bytes).ok()
            })
            .collect::<Option<Vec<_>>>()?;
        NamespacePath::new(components, limits).ok()
    }
}

impl GenerationPathQuery {
    pub(crate) fn new(
        generation: GenerationId,
        file_ids: impl IntoIterator<Item = FileId>,
        paths: &BTreeMap<FileId, Vec<CachedPath>>,
    ) -> Self {
        let file_ids = file_ids.into_iter().collect::<Vec<_>>();
        Self {
            version: PATH_QUERY_VERSION,
            generation,
            query: query_id(file_ids.iter().copied()),
            entries: file_ids
                .into_iter()
                .map(|file_id| GenerationPathQueryEntry {
                    file_id,
                    paths: paths.get(&file_id).cloned().unwrap_or_default(),
                })
                .collect(),
        }
    }

    pub(crate) fn decode(
        bytes: &[u8],
        generation: GenerationId,
        file_ids: impl IntoIterator<Item = FileId>,
    ) -> Option<BTreeMap<FileId, Vec<CachedPath>>> {
        let file_ids = file_ids.into_iter().collect::<Vec<_>>();
        let value: Self = serde_json::from_slice(bytes).ok()?;
        if value.version != PATH_QUERY_VERSION
            || value.generation != generation
            || value.query != query_id(file_ids.iter().copied())
            || value.entries.len() != file_ids.len()
            || value
                .entries
                .iter()
                .zip(file_ids)
                .any(|(entry, expected)| entry.file_id != expected)
        {
            return None;
        }
        Some(
            value
                .entries
                .into_iter()
                .map(|entry| (entry.file_id, entry.paths))
                .collect(),
        )
    }

    pub(crate) fn encode(&self) -> Option<Vec<u8>> {
        serde_json::to_vec(self).ok()
    }
}

/// Derives the stable cache key for one exact generation/query pair.
pub(crate) fn cache_key(
    generation: GenerationId,
    file_ids: impl IntoIterator<Item = FileId>,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-generation-path-query-v1\0");
    hasher.update(generation.digest().as_bytes());
    hasher.update(&query_id(file_ids));
    *hasher.finalize().as_bytes()
}

fn query_id(file_ids: impl IntoIterator<Item = FileId>) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-generation-path-file-ids-v1\0");
    for file_id in file_ids {
        hasher.update(&file_id.into_bytes());
    }
    *hasher.finalize().as_bytes()
}

/// Best-effort derived cache. Errors are deliberately misses: this capability
/// may improve work but can never affect exact filesystem results.
pub(crate) trait GenerationPathIndex: Send + Sync {
    fn load(&self, key: [u8; 32]) -> Option<Vec<u8>>;
    fn store(&self, key: [u8; 32], value: &[u8]);
}

pub(crate) struct MemoryGenerationPathIndex {
    state: Mutex<MemoryPathIndexState>,
}

#[derive(Default)]
struct MemoryPathIndexState {
    entries: BTreeMap<[u8; 32], Vec<u8>>,
    retained_bytes: usize,
}

impl Default for MemoryGenerationPathIndex {
    fn default() -> Self {
        Self {
            state: Mutex::new(MemoryPathIndexState::default()),
        }
    }
}

impl GenerationPathIndex for MemoryGenerationPathIndex {
    fn load(&self, key: [u8; 32]) -> Option<Vec<u8>> {
        self.state.lock().ok()?.entries.get(&key).cloned()
    }

    fn store(&self, key: [u8; 32], value: &[u8]) {
        if value.len() > MAXIMUM_PATH_INDEX_VALUE_BYTES {
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            if let Some(previous) = state.entries.remove(&key) {
                state.retained_bytes = state.retained_bytes.saturating_sub(previous.len());
            }
            while state.entries.len() >= MAXIMUM_PATH_INDEX_ENTRIES
                || state.retained_bytes.saturating_add(value.len())
                    > MAXIMUM_PATH_INDEX_RETAINED_BYTES
            {
                let Some(oldest) = state.entries.first_key_value().map(|(&key, _)| key) else {
                    break;
                };
                if let Some(evicted) = state.entries.remove(&oldest) {
                    state.retained_bytes = state.retained_bytes.saturating_sub(evicted.len());
                }
            }
            state.retained_bytes = state.retained_bytes.saturating_add(value.len());
            state.entries.insert(key, value.to_vec());
        }
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(crate) struct NativeGenerationPathIndex {
    root: std::path::PathBuf,
    state: Mutex<NativePathIndexState>,
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[derive(Default)]
struct NativePathIndexState {
    entries: BTreeMap<[u8; 32], usize>,
    retained_bytes: usize,
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl NativeGenerationPathIndex {
    pub(crate) fn new(root: std::path::PathBuf) -> Self {
        let mut state = NativePathIndexState::default();
        if let Ok(entries) = std::fs::read_dir(&root) {
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(key) = path
                    .file_stem()
                    .and_then(std::ffi::OsStr::to_str)
                    .and_then(|stem| hex::decode(stem).ok())
                    .and_then(|bytes| bytes.try_into().ok())
                else {
                    continue;
                };
                let Ok(metadata) = entry.metadata() else {
                    continue;
                };
                let Ok(bytes) = usize::try_from(metadata.len()) else {
                    let _ = std::fs::remove_file(path);
                    continue;
                };
                if bytes > MAXIMUM_PATH_INDEX_VALUE_BYTES {
                    let _ = std::fs::remove_file(path);
                    continue;
                }
                state.retained_bytes = state.retained_bytes.saturating_add(bytes);
                state.entries.insert(key, bytes);
            }
        }
        let index = Self {
            root,
            state: Mutex::new(state),
        };
        index.enforce_bounds();
        index
    }

    fn path(&self, key: [u8; 32]) -> std::path::PathBuf {
        self.root.join(format!("{}.json", hex::encode(key)))
    }

    fn enforce_bounds(&self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        while state.entries.len() > MAXIMUM_PATH_INDEX_ENTRIES
            || state.retained_bytes > MAXIMUM_PATH_INDEX_RETAINED_BYTES
        {
            let Some(oldest) = state.entries.first_key_value().map(|(&key, _)| key) else {
                break;
            };
            if let Some(bytes) = state.entries.remove(&oldest) {
                state.retained_bytes = state.retained_bytes.saturating_sub(bytes);
                let _ = std::fs::remove_file(self.path(oldest));
            }
        }
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl GenerationPathIndex for NativeGenerationPathIndex {
    fn load(&self, key: [u8; 32]) -> Option<Vec<u8>> {
        let bytes = std::fs::read(self.path(key)).ok()?;
        (bytes.len() <= MAXIMUM_PATH_INDEX_VALUE_BYTES).then_some(bytes)
    }

    fn store(&self, key: [u8; 32], value: &[u8]) {
        if value.len() > MAXIMUM_PATH_INDEX_VALUE_BYTES {
            return;
        }
        let path = self.path(key);
        let Some(parent) = path.parent() else { return };
        if std::fs::create_dir_all(parent).is_err() {
            return;
        }
        let temporary = path.with_extension(format!("{}.next", std::process::id()));
        let mut stored = false;
        if std::fs::write(&temporary, value).is_ok() {
            let _ = std::fs::remove_file(&path);
            stored = std::fs::rename(&temporary, &path).is_ok();
        }
        let _ = std::fs::remove_file(temporary);
        if stored {
            if let Ok(mut state) = self.state.lock() {
                if let Some(previous) = state.entries.insert(key, value.len()) {
                    state.retained_bytes = state.retained_bytes.saturating_sub(previous);
                }
                state.retained_bytes = state.retained_bytes.saturating_add(value.len());
            }
            self.enforce_bounds();
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn memory_index_has_exact_entry_and_byte_bounds() {
        let index = MemoryGenerationPathIndex::default();
        for ordinal in 0..=MAXIMUM_PATH_INDEX_ENTRIES {
            let mut key = [0_u8; 32];
            key[..8].copy_from_slice(&u64::try_from(ordinal).expect("ordinal").to_be_bytes());
            index.store(key, b"value");
        }
        let state = index.state.lock().expect("path index");
        assert_eq!(state.entries.len(), MAXIMUM_PATH_INDEX_ENTRIES);
        assert_eq!(state.retained_bytes, MAXIMUM_PATH_INDEX_ENTRIES * 5);
        assert!(!state.entries.contains_key(&[0; 32]));
        drop(state);

        index.store([0xff; 32], &vec![0; MAXIMUM_PATH_INDEX_VALUE_BYTES + 1]);
        assert!(index.load([0xff; 32]).is_none());
    }
}
