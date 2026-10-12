//! Journaled local persistence for filesystem control-plane state.

use crate::record_store::Revisioned;
use crate::lazy_workspace::{MemoryLazyNode, NativeOwnedCoreState};
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
use fs2::FileExt;
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
const LAZY_WORKSPACE_FAMILY: &str = "lazy-workspaces";
const MATERIALIZATION_FAMILY: &str = "materialization";
const MULTI_ROOT_PARENT_CLAIM_FAMILY: &str = "multi-root-parent-claims";
const MULTI_ROOT_PUBLICATION_FAMILY: &str = "multi-root-publications";
const OWNER_LOCK: &str = "owner.lock";

/// One journaled private namespace shared by core control-plane state.
#[derive(Clone)]
pub struct LocalCoreStateStore {
    root: Arc<PathBuf>,
    ownership: Ownership,
    /// Open while this handle's commits defer durability.
    deferral: Option<Arc<AtomicBool>>,
    nodes: Arc<NodeMemo>,
    journal_work: Option<crate::materializer::NativeMaterializationWork>,
}

impl std::fmt::Debug for LocalCoreStateStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("LocalCoreStateStore")
            .field("root", &self.root).field("ownership", &self.ownership)
            .field("deferral", &self.deferral).field("nodes", &self.nodes)
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
    _lock: OwnerLock,
    /// Every logged record read or committed so far, `None` when absent.
    residents: Arc<RwLock<Residents>>,
    /// Core-state log, opened by the first logged transaction. Its mutex
    log: Mutex<Option<CoreLog>>,
}

/// One held `owner.lock`, released when dropped.
#[derive(Debug)]
struct OwnerLock(File, Option<crate::materializer::NativeCleanupWrite>);

impl OwnerLock {
    fn acquire(root: &Path, mode: LockMode) -> Result<Self, LocalCoreStateStoreError> {
        std::fs::create_dir_all(root)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join(OWNER_LOCK))?;
        let locked = match mode {
            LockMode::Shared => FileExt::try_lock_shared(&lock),
            LockMode::Exclusive => lock.try_lock_exclusive(),
        };
        match locked {
            Ok(()) => Ok(Self(lock, None)),
            Err(error) if acyclic_native_runtime::is_exclusive_lock_contention(&error) => Err(
                LocalCoreStateStoreError::OwnershipConflict(root.to_path_buf()),
            ),
            Err(error) => Err(error.into()),
        }
    }
}

impl Drop for OwnerLock {
    fn drop(&mut self) {
        // Closing the handle releases the lock too; unlocking first makes the
        // release immediate on Windows.
        if let Some(cleanup) = self.1.take() {
            let _ = cleanup.attempt(|| FileExt::unlock(&self.0));
        } else {
            let _ = FileExt::unlock(&self.0);
        }
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
    revision: u64,
    operation_id: OperationId,
}

impl Revisioned for MultiRootParentClaim {
    fn revision(&self) -> u64 {
        self.revision
    }
}

impl LocalCoreStateStore {
    /// Opens a private companion namespace below `root` for transactional use.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Arc::new(root.into()),
            ownership: Ownership::Shared,
            deferral: None,
            nodes: Arc::default(),
            journal_work: None,
        }
    }

    /// Owns the namespace exclusively until this store and all its clones drop.
    pub fn open_owned(root: impl Into<PathBuf>) -> Result<Self, LocalCoreStateStoreError> {
        let root = root.into();
        let lock = OwnerLock::acquire(&root, LockMode::Exclusive)?;
        Ok(Self {
            root: Arc::new(root),
            ownership: Ownership::Owned(Arc::new(Owner {
                _lock: lock,
                residents: Arc::default(),
                log: Mutex::default(),
            })),
            deferral: None,
            nodes: Arc::default(),
            journal_work: None,
        })
    }

    /// Defers logged commits until the returned guard commits them.
    pub fn defer_durability(&self) -> (Self, DeferredDurability) {
        let deferral = Arc::new(AtomicBool::new(true));
        let store = Self {
            deferral: Some(Arc::clone(&deferral)),
            ..self.clone()
        };
        (store.clone(), DeferredDurability { store, deferral })
    }

    /// Returns the namespace root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn namespace(&self) -> Result<Namespace, LocalCoreStateStoreError> {
        let work = self.journal_work.as_ref();
        let (root, allocation) = match work {
            Some(work) => { let (root, allocation) = crate::native_host::clone_path_with_work(&self.root, &mut &*work)?; (root, Some(allocation)) }
            None => (self.root.as_ref().clone(), None),
        };
        let admission = match (&self.ownership, work) {
            (Ownership::Shared, Some(work)) => {
                crate::native_host::create_directories(&root, &mut &*work)?;
                let mut paths_allocation = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
                let path = measured_record_path(&root, OWNER_LOCK, work, &mut paths_allocation)?;
                let lock = materialization_open_options(&path, &crate::native_host::NativeOpenOptions::ReadWriteCreate, work)?;
                let cleanup = work.reserve_cleanup_write()?;
                let read = crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::UNCHARGED };
                work.admit(read)?;
                let acquired = FileExt::try_lock_shared(&lock);
                let owner_lock = acquired.as_ref().is_ok().then(|| OwnerLock(lock, Some(cleanup)));
                work.spent(read)?;
                match acquired {
                    Ok(()) => Admission::Shared { _lock: owner_lock.expect("acquired lock") },
                    Err(error) if acyclic_native_runtime::is_exclusive_lock_contention(&error) =>
                        return Err(LocalCoreStateStoreError::LazyWork(crate::LazyWorkspaceError::NativeRecordOwnershipConflict(root, allocation))),
                    Err(error) => return Err(error.into()),
                }
            }
            (Ownership::Shared, None) => Admission::Shared { _lock: OwnerLock::acquire(&root, LockMode::Shared)? },
            (Ownership::Owned(owner), _) => Admission::Owned(Arc::clone(owner)),
        };
        let deferred = matches!(admission, Admission::Owned(_))
            && self.deferral.as_ref().is_some_and(|deferral| deferral.load(Ordering::Acquire));
        Ok(Namespace { root, admission, durability: if deferred { Durability::Deferred } else { Durability::Flushed }, _allocation: allocation })
    }

    /// Runs one admitted transaction without blocking an async executor.
    async fn transaction<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Namespace) -> Result<T, LocalCoreStateStoreError> + Send + 'static,
    ) -> Result<T, LocalCoreStateStoreError> {
        if let Some(work) = &self.journal_work { work.admit(crate::WorkCounters::UNCHARGED)?; }
        let meter = self.journal_work.as_ref().map(|work| (work.runtime_meter(), false));
        let native = meter.is_some();
        let runtime_error = |error| {
            if native { LocalCoreStateStoreError::from(crate::NativeTreeMaterializationError::from_runtime_error(error)) }
            else { LocalCoreStateStoreError::Io(error) }
        };
        let store = self.clone();
        let task = acyclic_native_runtime::run_blocking_io(move || operation(store.namespace()?), meter).map_err(&runtime_error)?;
        task.await.map_err(runtime_error)?
    }

    /// Loads one journaled record without blocking an async executor.
    async fn load_record<T: DeserializeOwned + Send + 'static>(
        &self,
        family: &'static str,
        key: [u8; 16],
    ) -> Result<Option<T>, LocalCoreStateStoreError> {
        self.transaction(move |namespace| read_locked(&namespace.record(family, &key)))
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
                &namespace.record(family, &key),
                expected_revision,
                &replacement,
            )
        })
        .await
    }

    /// Removes one journaled record only at `expected_revision`.
    async fn compare_and_delete_record<T: DeserializeOwned + Revisioned>(
        &self,
        family: &'static str,
        key: [u8; 16],
        expected_revision: u64,
    ) -> Result<bool, LocalCoreStateStoreError> {
        self.transaction(move |namespace| {
            compare_and_delete::<T>(&namespace.record(family, &key), expected_revision)
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
        if let Some(work) = &self.journal_work {
            return self.remove_journal(operation_id, work).await;
        }
        self.transaction(move |namespace| {
            let paths = namespace.record(MATERIALIZATION_FAMILY, &operation_id.into_bytes());
            with_lock(&paths, || {
                let journal: Option<MaterializationJournal> = read_recoverable(&paths)?;
                if journal.as_ref().is_some_and(|journal| {
                    !matches!(
                        journal.phase,
                        crate::MaterializationPhase::Applied
                            | crate::MaterializationPhase::RolledBack
                    )
                }) {
                    return Err(LocalCoreStateStoreError::MaterializationInProgress);
                }
                remove_record(&paths)
            })
        })
        .await
    }
}

impl Namespace {
    fn family(&self, family: &str) -> PathBuf {
        self.root.join(family)
    }

    fn record(&self, family: &str, key: &[u8]) -> RecordPaths<'_> {
        let directory = self.family(family);
        let stem = hex::encode(key);
        RecordPaths {
            current: directory.join(format!("{stem}.json")),
            previous: directory.join(format!("{stem}.previous.json")),
            temporary: directory.join(format!("{stem}.next.json")),
            lock: directory.join(format!("{stem}.lock")),
            directory,
            namespace: PhantomData,
        }
    }

    fn operation_records(
        &self,
        family: &str,
    ) -> Result<Vec<OperationId>, LocalCoreStateStoreError> {
        let entries = match std::fs::read_dir(self.family(family)) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut operations = std::collections::BTreeSet::new();
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(stem) = name.strip_suffix(".json") else {
                continue;
            };
            let stem = stem.strip_suffix(".previous").unwrap_or(stem);
            if stem.ends_with(".next") {
                continue;
            }
            let Ok(bytes) = hex::decode(stem) else {
                continue;
            };
            let Ok(bytes) = <[u8; 16]>::try_from(bytes) else {
                continue;
            };
            operations.insert(OperationId::from_bytes(bytes));
        }
        Ok(operations.into_iter().collect())
    }
}

/// Guard for a [`LocalCoreStateStore::defer_durability`] scope.
#[must_use = "deferred commits are durable only once committed"]
#[derive(Debug)]
pub struct DeferredDurability {
    store: LocalCoreStateStore,
    deferral: Arc<AtomicBool>,
}

impl DeferredDurability {
    /// Ends the scope and makes every commit made through the log so far durable.
    pub async fn commit(self) -> Result<(), LocalCoreStateStoreError> {
        self.deferral.store(false, Ordering::Release);
        self.store
            .transaction(|root| root.with_log(|log| log.flush(&root)))
            .await
    }
}

impl Drop for DeferredDurability {
    fn drop(&mut self) {
        // Abandoned commits become durable with the log's next flush.
        self.deferral.store(false, Ordering::Release);
    }
}

/// Journaled companion-state failure.
#[derive(Debug, Error)]
pub enum LocalCoreStateStoreError {
    /// Native filesystem operation failed.
    #[error("core state I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Another owner held a record lock beyond the bounded transaction deadline.
    #[error("core state record lock timed out: {}", .0.display())]
    LockTimeout(PathBuf),
    /// Another store owns the namespace or, when opening an owner, is using it.
    #[error("core state namespace is in use by another store: {}", .0.display())]
    OwnershipConflict(PathBuf),
    /// A persisted record was malformed or incompatible with its schema.
    #[error("core state serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    /// A nonterminal materialization journal cannot be discarded.
    #[error("materialization is still in progress")]
    MaterializationInProgress,
    /// A native materialization work allowance or cancellation boundary failed.
    #[error(transparent)]
    MaterializationWork(#[from] crate::materializer::NativeTreeMaterializationError),
    /// A measured lazy-node clone or digest boundary failed.
    #[error(transparent)]
    LazyWork(#[from] crate::LazyWorkspaceError),
    /// Content-addressed state did not match its claimed digest.
    #[error("core state content address does not match its payload")]
    Integrity,
    /// A transferred native payload belonged to a different original ledger.
    #[error("native core-state payload belongs to another operation ledger")]
    NativeLedgerMismatch,
    /// A durable workspace-context transaction was malformed or incompatible.
    #[error("workspace-context transaction is incompatible: {0}")]
    ContextTransaction(String),
    /// The core-state log holds an intact frame that does not decode, so it may commit changes this store cannot apply.
    #[error("core state log holds an undecodable intact frame: {}", .0.display())]
    Corrupt(PathBuf),
}


mod contexts;
use contexts::*;
mod values;
use values::*;
mod log;
use log::*;
mod native_log;
mod native_io;
use native_io::*;
mod journal;
pub use journal::LocalMaterializationActionCompletion;
mod decode;
use decode::*;
mod publications;
mod nodes;
use nodes::*;
mod collection;
mod lazy_log;
use lazy_log::*;
mod records;
use records::*;
mod native_records;
#[cfg(test)]
mod tests;
