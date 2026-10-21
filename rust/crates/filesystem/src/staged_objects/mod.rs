//! Bounded private immutable staging before authority publication.

use crate::async_storage::{
    AsyncObjectStore, DecodedCacheAdmission, DecodedCacheKey, DecodedCacheValue, PublicationScope,
};
#[cfg(test)]
use crate::storage::ObjectFailure;
use crate::storage::{
    HashedObject, ObjectId, ObjectRead, ObjectReadRequest, ObjectReadRetention, ObjectStoreError,
    ObjectWrite,
};
use crate::{
    AuthorityWork, CancellationToken, NativeAllocation, NativeMaterializationWork,
    NativeTreeMaterializationError, WorkBudget, WorkCounters,
};
use acyclic_native_runtime::{NativeFile, OwnedRead, OwnedWrite};
use acyclic_stream::OwnedBytes;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, PoisonError};
use tokio::sync::RwLock;

const MAXIMUM_DRAIN_BYTES: u64 = 4 * 1_024 * 1_024;
const MAXIMUM_RESIDENT_BYTES: u64 = 4 * 1_024 * 1_024;
const MAXIMUM_SPILL_BYTES: u64 = 1_024 * 1_024 * 1_024;
const MAXIMUM_DRAINED_IDENTITIES: usize = 4_096;

mod index;
use index::{Index, Lookup, Staged};
mod io;
use io::{staged_native_read, staged_owned_bytes, staged_vector};
use read::resident_read;
mod admission;
mod publication;
mod read;
mod spill;

/// One private local admission buffer backed by a durable provider.
pub struct StagedObjects<S> {
    inner: S,
    spill: NativeFile,
    index: std::sync::RwLock<Index>,
    spill_io: RwLock<()>,
    collection: Arc<crate::Collection>,
}

impl<S> StagedObjects<S> {
    /// Opens an unlinked spill file for bounded private staging.
    pub async fn open(inner: S, spill_directory: PathBuf) -> std::io::Result<Self> {
        let spill =
            acyclic_native_runtime::run_blocking_io(move || create_spill(&spill_directory), None)
                .expect("unmetered task construction cannot fail")
                .await??;
        Ok(Self {
            inner,
            spill: NativeFile::from_file(spill, None).map_err(|(error, _)| error)?,
            index: std::sync::RwLock::new(Index::default()),
            spill_io: RwLock::new(()),
            collection: Arc::default(),
        })
    }
    pub(crate) fn inner(&self) -> &S {
        &self.inner
    }
    fn index(&self) -> std::sync::RwLockReadGuard<'_, Index> {
        self.index.read().unwrap_or_else(PoisonError::into_inner)
    }
    fn index_mut(&self) -> std::sync::RwLockWriteGuard<'_, Index> {
        self.index.write().unwrap_or_else(PoisonError::into_inner)
    }
    fn lookup(&self, object_id: ObjectId) -> Lookup {
        match self.index().objects.get(&object_id) {
            None => Lookup::Absent,
            Some(Staged::Resident {
                bytes, referenced, ..
            }) => {
                referenced.store(true, Ordering::Relaxed);
                Lookup::Resident(bytes.clone())
            }
            Some(&Staged::Spilled { offset, length }) => Lookup::Spilled { offset, length },
        }
    }
}

fn byte_length(bytes: &OwnedBytes) -> u64 {
    bytes.len() as u64
}

fn frame_allocation<F>(
    native: Option<&NativeMaterializationWork>,
    future: &F,
) -> Result<Option<NativeAllocation>, ObjectStoreError> {
    let bytes = std::mem::size_of_val(future) as u64;
    native
        .map(|native| native.claim(bytes, u64::from(bytes != 0), WorkCounters::UNCHARGED))
        .transpose()
        .map_err(Into::into)
}

#[cfg(test)]
mod tests;

#[cfg(unix)]
fn create_spill(directory: &Path) -> std::io::Result<std::fs::File> {
    tempfile::tempfile_in(directory)
}

/// Creates a spill file no other handle names, which the host removes once
/// its last handle closes, including after a crash. The native runtime
/// reopens the handle for overlapped I/O, so the file admits shared access.
#[cfg(windows)]
fn create_spill(directory: &Path) -> std::io::Result<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt as _;
    use windows::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_TEMPORARY, FILE_FLAG_DELETE_ON_CLOSE, FILE_SHARE_DELETE, FILE_SHARE_READ,
        FILE_SHARE_WRITE,
    };
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
        .attributes(FILE_ATTRIBUTE_TEMPORARY.0)
        .custom_flags(FILE_FLAG_DELETE_ON_CLOSE.0)
        .open(directory.join(format!(
            ".acyclic-staging-{}",
            crate::identity::fresh_uuid().simple()
        )))
}
