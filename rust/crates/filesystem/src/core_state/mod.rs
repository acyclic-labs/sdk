//! Journaled local persistence for filesystem control-plane state.

use crate::record_store::Revisioned;
use crate::workspace_context::{
    WorkspaceContextChildren, discard_context_subtree, plan_context_subtree_discard,
    update_context_children,
};
use crate::{
    GitCompatState, GitCompatStore, LazyOverlay, LazyOverlayId, LazyWorkspaceState,
    LazyWorkspaceStore, MaterializationJournal, MaterializationJournalStore, MultiRootPublication,
    MultiRootPublicationStore, OperationId, WorkspaceContext, WorkspaceContextDiscardOutcome,
    WorkspaceContextId, WorkspaceContextStore, WorkspaceId, WorkspaceLineageRecord,
    WorkspaceLineageStore,
};
use acyclic_native_runtime::{FileLock, FileLockMode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockWriteGuard};
use std::time::{Duration, Instant};
use thiserror::Error;
use uuid::Uuid;

const RECORD_LOCK_DEADLINE: Duration = Duration::from_secs(5);
const RECORD_LOCK_RETRY: Duration = Duration::from_millis(5);
const GIT_COMPAT_NAMESPACE: &str = "git-compat-v9";
const MULTI_ROOT_PARENT_CLAIM_FAMILY: &str = "multi-root-parent-claims";
const MULTI_ROOT_PUBLICATION_FAMILY: &str = "multi-root-publications";
const LAZY_WORKSPACE_FAMILY: &str = "lazy-workspaces";
const MATERIALIZATION_FAMILY: &str = "materialization";
const OWNER_LOCK: &str = "owner.lock";

/// One journaled private namespace shared by core control-plane state.
#[derive(Clone)]
pub struct LocalCoreStateStore {
    root: Arc<PathBuf>,
    ownership: Ownership,
    /// Open while this handle's commits defer durability.
    deferral: Option<Arc<AtomicBool>>,
    journal_work: Option<crate::materializer::NativeMaterializationWork>,
    // Last: retained storage owners must outlive the namespace's open WAL and locks.
    ownership_anchor: Option<acyclic_native_runtime::OwnershipAnchor>,
}

impl std::fmt::Debug for LocalCoreStateStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalCoreStateStore")
            .field("root", &self.root)
            .field("ownership", &self.ownership)
            .field("deferral", &self.deferral)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
enum Ownership {
    /// Each transaction is admitted by a shared `owner.lock`.
    Shared,
    /// Sole owner; clones share its lock and record cache.
    Owned(Arc<Owner>),
}

/// Lifetime-long exclusive ownership of one namespace.
#[derive(Debug)]
struct Owner {
    /// Held without a descriptor; transactions of other handles refuse while
    /// it names a live owner.
    _claim: acyclic_native_runtime::StoreClaim,
    /// Cached logged records, `None` when absent. Joinable owners bound this cache.
    residents: Arc<RwLock<Residents>>,
    /// Core-state log, opened by the first logged transaction. Its mutex
    log: Mutex<Option<CoreLog>>,
    cache_limit: Option<usize>,
}

/// One held lock file, released when dropped.
#[derive(Debug)]
struct OwnerLock(FileLock, OwnerLockRelease);

#[derive(Debug)]
enum OwnerLockRelease {
    Ordinary,
    Native(crate::materializer::NativeCleanupWrite),
    Released,
}

impl OwnerLock {
    fn new(file: FileLock, cleanup: Option<crate::materializer::NativeCleanupWrite>) -> Self {
        Self(
            file,
            match cleanup {
                Some(cleanup) => OwnerLockRelease::Native(cleanup),
                None => OwnerLockRelease::Ordinary,
            },
        )
    }

    fn unlock(&mut self) -> Result<(), LocalCoreStateStoreError> {
        // One release attempt per acquisition, even when recording its native
        // work fails.
        match std::mem::replace(&mut self.1, OwnerLockRelease::Released) {
            OwnerLockRelease::Ordinary => self.0.unlock()?,
            OwnerLockRelease::Native(cleanup) => {
                let (unlocked, recorded) = cleanup.attempt(|| self.0.unlock());
                recorded?;
                unlocked?;
            }
            OwnerLockRelease::Released => {}
        }
        Ok(())
    }

    /// Admits one transaction of a handle that does not own the namespace.
    fn shared(root: &Path) -> Result<Self, LocalCoreStateStoreError> {
        std::fs::create_dir_all(root)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join(OWNER_LOCK))?;
        let lock = match FileLock::try_acquire(lock, FileLockMode::Shared) {
            Ok(lock) => Self::new(lock, None),
            Err(error) if acyclic_native_runtime::is_exclusive_lock_contention(&error) => {
                return Err(LocalCoreStateStoreError::OwnershipConflict(
                    root.to_path_buf(),
                ));
            }
            Err(error) => return Err(error.into()),
        };
        if acyclic_native_runtime::StoreClaim::is_claimed(&lock.0)? {
            return Err(LocalCoreStateStoreError::OwnershipConflict(
                root.to_path_buf(),
            ));
        }
        Ok(lock)
    }
}

impl Drop for OwnerLock {
    fn drop(&mut self) {
        let _ = self.unlock();
    }
}

/// One admitted transaction's view of a namespace.
#[derive(Debug)]
struct Namespace {
    root: PathBuf,
    admission: Admission,
    durability: Durability,
    _allocation: Option<crate::materializer::NativeAllocation>,
}

/// When a transaction's log commits become durable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Durability {
    /// Before the commit returns.
    Flushed,
    /// With the next flush of the log.
    Deferred,
}

#[derive(Debug)]
enum Admission {
    /// Excludes any owner until the transaction ends.
    Shared {
        _lock: OwnerLock,
    },
    Owned(Arc<Owner>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MultiRootParentClaim {
    operation_id: OperationId,
}

impl LocalCoreStateStore {
    /// Opens a private companion namespace below `root` for transactional use.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Arc::new(root.into()),
            ownership: Ownership::Shared,
            deferral: None,
            journal_work: None,
            ownership_anchor: None,
        }
    }

    /// Owns the namespace exclusively until this store and all its clones drop.
    pub fn open_owned(root: impl Into<PathBuf>) -> Result<Self, LocalCoreStateStoreError> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        let claim = acyclic_native_runtime::StoreClaim::acquire(&root.join(OWNER_LOCK)).map_err(
            |error| {
                if acyclic_native_runtime::is_exclusive_lock_contention(&error) {
                    LocalCoreStateStoreError::OwnershipConflict(root.clone())
                } else {
                    error.into()
                }
            },
        )?;
        Ok(Self {
            root: Arc::new(root),
            ownership: Ownership::Owned(Arc::new(Owner {
                _claim: claim,
                residents: Arc::default(),
                log: Mutex::default(),
                cache_limit: None,
            })),
            deferral: None,
            journal_work: None,
            ownership_anchor: None,
        })
    }

    /// Defers logged commits until the returned guard commits them.
    pub fn defer_durability(&self) -> (Self, DeferredDurability) {
        let deferral = Arc::new(AtomicBool::new(true));
        let store = Self {
            deferral: Some(deferral),
            ..self.clone()
        };
        (store.clone(), DeferredDurability { store })
    }

    /// Retains an external storage owner through clones and in-flight transactions.
    #[doc(hidden)]
    #[must_use]
    pub fn with_ownership_anchor(
        mut self,
        owner: Option<acyclic_native_runtime::OwnershipAnchor>,
    ) -> Self {
        if let Some(owner) = owner {
            self.ownership_anchor = Some(match self.ownership_anchor.take() {
                Some(previous) => acyclic_native_runtime::OwnershipAnchor::new((previous, owner)),
                None => owner,
            });
        }
        self
    }

    /// Returns the namespace root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn namespace(&self) -> Result<Namespace, LocalCoreStateStoreError> {
        let work = self.journal_work.as_ref();
        let (root, allocation) = match work {
            Some(work) => {
                let (root, allocation) = crate::native_host::clone_path(&self.root, &mut &*work)?;
                (root, Some(allocation))
            }
            None => (self.root.as_ref().clone(), None),
        };
        let admission = match (&self.ownership, work) {
            (Ownership::Shared, Some(work)) => {
                crate::native_host::create_directories(&root, &mut &*work)?;
                let mut paths_allocation = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
                let path = measured_record_path(&root, OWNER_LOCK, work, &mut paths_allocation)?;
                let lock = materialization_open_options(
                    &path,
                    &crate::native_host::NativeOpenOptions::ReadWriteCreate,
                    work,
                )?;
                let cleanup = work.reserve_cleanup_write()?;
                let reads = |backend_read_operations| crate::WorkCounters {
                    backend_read_operations,
                    ..crate::WorkCounters::UNCHARGED
                };
                // The lock, then the claim check: its record and the probes
                // of the owner it names.
                work.admit(reads(
                    1 + acyclic_native_runtime::StoreClaim::CHECK_READ_OPERATIONS,
                ))?;
                // Every buffer the check may allocate, held across it.
                let check_allocation = work.claim(
                    acyclic_native_runtime::StoreClaim::CHECK_ALLOCATION_BYTES,
                    acyclic_native_runtime::StoreClaim::CHECK_ALLOCATIONS,
                    crate::WorkCounters::UNCHARGED,
                )?;
                let mut spent = 1;
                let acquired = FileLock::try_acquire(lock, FileLockMode::Shared).and_then(|lock| {
                    // A claim names its owner inside the lock file instead of
                    // holding the lock, so a free lock alone does not admit.
                    let checked = acyclic_native_runtime::StoreClaim::check(&lock);
                    // A failed check made at most every read it may make.
                    spent += checked.as_ref().map_or(
                        acyclic_native_runtime::StoreClaim::CHECK_READ_OPERATIONS,
                        |check| check.read_operations,
                    );
                    // Refused, the lock unlocks as it drops.
                    match checked {
                        Ok(check) if !check.claimed => Ok(OwnerLock::new(lock, Some(cleanup))),
                        Ok(_) => Err(std::io::Error::from(std::io::ErrorKind::WouldBlock)),
                        Err(error) => Err(error),
                    }
                });
                drop(check_allocation);
                work.spent(reads(spent))?;
                match acquired {
                    Ok(owner_lock) => Admission::Shared { _lock: owner_lock },
                    Err(error) if acyclic_native_runtime::is_exclusive_lock_contention(&error) => {
                        return Err(LocalCoreStateStoreError::LazyWork(
                            crate::LazyWorkspaceError::NativeRecordOwnershipConflict(
                                root, allocation,
                            ),
                        ));
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            (Ownership::Shared, None) => Admission::Shared {
                _lock: OwnerLock::shared(&root)?,
            },
            (Ownership::Owned(owner), _) => Admission::Owned(Arc::clone(owner)),
        };
        let deferred = matches!(admission, Admission::Owned(_))
            && self
                .deferral
                .as_ref()
                .is_some_and(|deferral| deferral.load(Ordering::Acquire));
        Ok(Namespace {
            root,
            admission,
            durability: if deferred {
                Durability::Deferred
            } else {
                Durability::Flushed
            },
            _allocation: allocation,
        })
    }

    /// Runs one admitted transaction without blocking an async executor.
    async fn transaction<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Namespace) -> Result<T, LocalCoreStateStoreError> + Send + 'static,
    ) -> Result<T, LocalCoreStateStoreError> {
        if let Some(work) = &self.journal_work {
            work.admit(crate::WorkCounters::UNCHARGED)?;
        }
        let meter = self
            .journal_work
            .as_ref()
            .map(|work| (work.runtime_meter(), false));
        let native = meter.is_some();
        let runtime_error = |error| {
            if native {
                LocalCoreStateStoreError::from(
                    crate::NativeTreeMaterializationError::from_runtime_error(error),
                )
            } else {
                LocalCoreStateStoreError::Io(error)
            }
        };
        let store = self.clone();
        let task =
            acyclic_native_runtime::run_blocking_io(move || operation(store.namespace()?), meter)
                .map_err(&runtime_error)?;
        task.await.map_err(runtime_error)?
    }

    /// Loads one journaled record without blocking an async executor.
    async fn load_record<T: DeserializeOwned + Send + 'static>(
        &self,
        family: &'static str,
        key: [u8; 16],
    ) -> Result<Option<T>, LocalCoreStateStoreError> {
        self.transaction(move |namespace| read_locked(&mut namespace.record(family, &key)?))
            .await
    }

    /// Replaces one journaled record only at `expected_revision` (`0` means absent).
    async fn compare_and_swap_record<T>(
        &self,
        family: &'static str,
        key: [u8; 16],
        expected_revision: u64,
        replacement: T,
    ) -> Result<bool, LocalCoreStateStoreError>
    where
        T: DeserializeOwned + Serialize + Revisioned + Send + 'static,
    {
        self.transaction(move |namespace| {
            compare_and_swap(
                &mut namespace.record(family, &key)?,
                expected_revision,
                &replacement,
            )
        })
        .await
    }

    /// Removes one journaled record only at the expected revision.
    async fn compare_and_delete_record<T: DeserializeOwned + Revisioned>(
        &self,
        family: &'static str,
        key: [u8; 16],
        expected_revision: u64,
    ) -> Result<bool, LocalCoreStateStoreError> {
        self.transaction(move |namespace| {
            let mut paths = namespace.record(family, &key)?;
            with_lock(&mut paths, |paths| {
                let current = read_recoverable::<T>(paths)?;
                if current.as_ref().map_or(0, |(value, _)| value.revision()) != expected_revision {
                    return Ok(false);
                }
                remove_record(paths)?;
                Ok(true)
            })
        })
        .await
    }

    /// Lists materialization operation ids with durable recovery state.
    pub async fn materialization_operations_async(
        &self,
    ) -> Result<Vec<OperationId>, LocalCoreStateStoreError> {
        self.transaction(|namespace| namespace.operation_records(MATERIALIZATION_FAMILY))
            .await
    }

    /// Removes a terminal materialization journal and its recovery copies
    pub async fn remove_materialization_async(
        &self,
        operation_id: OperationId,
    ) -> Result<(), LocalCoreStateStoreError> {
        self.transaction(move |namespace| {
            let mut paths = namespace.record(MATERIALIZATION_FAMILY, &operation_id.into_bytes())?;
            with_lock(&mut paths, |paths| {
                let journal = read_recoverable::<MaterializationJournal>(paths)?;
                if journal.as_ref().is_some_and(|(journal, _)| {
                    !matches!(
                        journal.phase,
                        crate::MaterializationPhase::Applied
                            | crate::MaterializationPhase::RolledBack
                    )
                }) {
                    return Err(LocalCoreStateStoreError::MaterializationInProgress);
                }
                remove_record(paths)
            })
        })
        .await
    }
}

impl Namespace {
    fn family(&self, family: &str) -> PathBuf {
        self.root.join(family)
    }

    fn record(
        &self,
        family: &str,
        key: &[u8],
    ) -> Result<RecordPaths<'_>, LocalCoreStateStoreError> {
        let work = self
            ._allocation
            .as_ref()
            .map(|allocation| allocation.work());
        let key_bytes = key
            .len()
            .checked_mul(2)
            .ok_or(crate::WorkError::Overflow)
            .map_err(crate::NativeTreeMaterializationError::from)?;
        let mut name = [0_u8; 96];
        let encoded = name.get_mut(..key_bytes).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "record identity exceeds the bounded key width",
            )
        })?;
        let encoding = crate::WorkCounters {
            bytes_encoded: key_bytes as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        if let Some(work) = work {
            work.admit(encoding)?;
        }
        hex::encode_to_slice(key, encoded).map_err(std::io::Error::other)?;
        if let Some(work) = work {
            work.spent(encoding)?;
        }
        let mut allocation = work
            .map(|work| work.claim(0, 0, crate::WorkCounters::UNCHARGED))
            .transpose()?;
        let mut path = |parent: &Path, leaf: &str| match work {
            Some(work) => measured_record_path(
                parent,
                leaf,
                work,
                allocation
                    .as_mut()
                    .ok_or(crate::NativeTreeMaterializationError::MissingCustody)?,
            ),
            None => Ok(parent.join(leaf)),
        };
        let directory = path(&self.root, family)?;
        let mut entry = |suffix: &str| -> Result<PathBuf, LocalCoreStateStoreError> {
            let end = key_bytes
                .checked_add(suffix.len())
                .ok_or(crate::WorkError::Overflow)
                .map_err(crate::NativeTreeMaterializationError::from)?;
            name.get_mut(key_bytes..end)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "record identity and suffix exceed the bounded key width",
                    )
                })?
                .copy_from_slice(suffix.as_bytes());
            path(
                &directory,
                std::str::from_utf8(&name[..end]).map_err(std::io::Error::other)?,
            )
        };
        let current = entry(".json")?;
        let previous = entry(".previous.json")?;
        let temporary = entry(".next.json")?;
        let lock = entry(".lock")?;
        let directory_bytes = directory.capacity() as u64;
        drop(directory);
        if let Some(allocation) = &mut allocation {
            allocation.release(directory_bytes)?;
        }
        Ok(RecordPaths {
            current,
            previous,
            temporary,
            lock,
            namespace: PhantomData,
            allocation,
        })
    }

    fn operation_records(
        &self,
        family: &str,
    ) -> Result<Vec<OperationId>, LocalCoreStateStoreError> {
        let operations = collection::family_stems(self, family, &[".previous.json", ".json"])?
            .into_iter()
            .filter_map(|stem| <[u8; 16]>::try_from(stem).ok())
            .map(OperationId::from_bytes);
        Ok(operations.collect())
    }
}

/// Guard for a [`LocalCoreStateStore::defer_durability`] scope.
#[must_use = "deferred commits are durable only once committed"]
#[derive(Debug)]
pub struct DeferredDurability {
    store: LocalCoreStateStore,
}

impl DeferredDurability {
    /// Ends the scope and makes every commit made through the log so far durable.
    pub async fn commit(self) -> Result<(), LocalCoreStateStoreError> {
        self.store
            .deferral
            .as_ref()
            .expect("deferred store")
            .store(false, Ordering::Release);
        self.store
            .transaction(|root| root.with_log(|log| log.flush(&root)))
            .await
    }
}

impl Drop for DeferredDurability {
    fn drop(&mut self) {
        // Abandoned commits become durable with the log's next flush.
        self.store
            .deferral
            .as_ref()
            .expect("deferred store")
            .store(false, Ordering::Release);
    }
}

/// Journaled companion-state failure.
#[derive(Debug, Error)]
pub enum LocalCoreStateStoreError {
    /// Native filesystem operation failed.
    #[error("core state I/O failed: {0}")]
    Io(#[source] std::io::Error),
    /// Another owner held a record lock beyond the bounded transaction deadline.
    #[error("core state record lock timed out: {}", .0.display())]
    LockTimeout(PathBuf),
    /// Another store owns the namespace or, when opening an owner, is using it.
    #[error("core state namespace is in use by another store: {}", .0.display())]
    OwnershipConflict(PathBuf),
    /// A persisted record was malformed or incompatible with its schema.
    #[error("core state serialization failed: {0}")]
    Json(#[source] serde_json::Error),
    /// A nonterminal materialization journal cannot be discarded.
    #[error("materialization is still in progress")]
    MaterializationInProgress,
    /// A measured lazy-node clone or digest boundary failed.
    #[error(transparent)]
    LazyWork(#[from] crate::LazyWorkspaceError),
    /// A record identity, payload, or declared bound is inconsistent.
    #[error("core state record integrity failed")]
    Integrity,
    /// The core-state log holds an intact frame that does not decode, so it may commit changes this store cannot apply.
    #[error("core state log holds an undecodable intact frame: {}", .0.display())]
    Corrupt(PathBuf),
}

impl From<std::io::Error> for LocalCoreStateStoreError {
    fn from(error: std::io::Error) -> Self {
        if !error
            .get_ref()
            .is_some_and(|cause| cause.is::<crate::NativeTreeMaterializationError>())
        {
            return Self::Io(error);
        }
        let kind = error.kind();
        match error.into_inner() {
            Some(inner) => match inner.downcast::<crate::NativeTreeMaterializationError>() {
                Ok(error) => Self::from(*error),
                Err(error) => Self::Io(std::io::Error::new(kind, error)),
            },
            None => Self::Io(kind.into()),
        }
    }
}

impl From<serde_json::Error> for LocalCoreStateStoreError {
    fn from(error: serde_json::Error) -> Self {
        if error.is_io() {
            Self::from(std::io::Error::from(error))
        } else {
            Self::Json(error)
        }
    }
}

impl From<crate::NativeTreeMaterializationError> for LocalCoreStateStoreError {
    fn from(error: crate::NativeTreeMaterializationError) -> Self {
        Self::LazyWork(match error {
            crate::NativeTreeMaterializationError::Cancelled => {
                crate::LazyWorkspaceError::Cancelled
            }
            crate::NativeTreeMaterializationError::Work(error) => {
                crate::LazyWorkspaceError::Demand(crate::DemandError::Work(error))
            }
            error => crate::LazyWorkspaceError::NativeWork(error),
        })
    }
}

mod contexts;
mod owners;
use contexts::*;
mod values;
use values::*;
mod log;
use log::*;
mod native_io;
pub(crate) use native_io::reserve;
mod native_log;
use native_io::*;
mod journal;
pub use journal::LocalMaterializationActionCompletion;
mod decode;
use decode::*;
mod nodes;
mod publications;
use nodes::*;
mod collection;
mod lazy_log;
mod records;
use records::*;
mod native_records;
#[cfg(test)]
mod tests;
