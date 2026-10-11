//! Journaled local persistence for filesystem control-plane state.
//!
//! Distributed deployments can implement the small CAS traits over their
//! authority stream. Embedded native deployments use this companion namespace:
//! one recoverable JSON record per workspace and state family. Contexts,
//! lineage, and lazy-workspace bindings change through one write-ahead log
//! (`CoreLog`); every other family locks and journals each record.
//! Keeping the implementation here prevents adapters from inventing separate
//! lineage, lease, and Git-compatibility databases.

use crate::record_store::Revisioned;
use crate::lazy_workspace::NativeOwnedCoreState;
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

mod native_records;
mod native_transactions;

const RECORD_LOCK_DEADLINE: Duration = Duration::from_secs(5);
const RECORD_LOCK_RETRY: Duration = Duration::from_millis(5);
const GIT_COMPAT_NAMESPACE: &str = "git-compat-v9";
const LAZY_WORKSPACE_FAMILY: &str = "lazy-workspaces";
const MATERIALIZATION_FAMILY: &str = "materialization";
const MULTI_ROOT_PARENT_CLAIM_FAMILY: &str = "multi-root-parent-claims";
const MULTI_ROOT_PUBLICATION_FAMILY: &str = "multi-root-publications";
const OWNER_LOCK: &str = "owner.lock";

// Journal locks, directory enumeration, and namespace mutations have no
// portable native completion path. Bound entire transactions, not individual
// syscalls, so cancellation cannot split a lock/CAS/durability sequence.
async fn run_local_transaction<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, LocalCoreStateStoreError> + Send + 'static,
) -> Result<T, LocalCoreStateStoreError> {
    acyclic_native_runtime::run_blocking_io(operation).await?
}

/// One journaled private namespace shared by core control-plane state.
///
/// A namespace has either one owner or any number of transactional users,
/// never both. [`Self::open_owned`] holds `owner.lock` exclusively for the
/// owner's lifetime, and every transaction of a [`Self::new`] store holds it
/// shared. File locks conflict between separate handles within one process as
/// well as across processes, so nothing but an owner and its clones can observe
/// or change an owned namespace, and an owner may serve records from memory.
#[derive(Clone, Debug)]
pub struct LocalCoreStateStore {
    root: PathBuf,
    ownership: Ownership,
    /// Open while this handle's commits defer durability.
    deferral: Option<Arc<AtomicBool>>,
    nodes: Arc<NodeMemo>,
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
    /// Only the log changes it, under the log's mutex.
    residents: Arc<RwLock<Residents>>,
    /// Core-state log, opened by the first logged transaction. Its mutex
    /// replaces the log's file lock for every owned logged access.
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
///
/// Record paths exist only as borrows of a namespace, so no record can be
/// touched unless its store owns the namespace or holds a shared admission
/// that excludes any owner for the whole transaction.
#[derive(Debug)]
struct Namespace {
    root: PathBuf,
    admission: Admission,
    durability: Durability,
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
    ///
    /// Directories are created lazily. Every transaction fails with
    /// [`LocalCoreStateStoreError::OwnershipConflict`] while an owner holds the
    /// namespace.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            ownership: Ownership::Shared,
            deferral: None,
            nodes: Arc::default(),
        }
    }

    /// Opens the namespace below `root` as its sole owner for as long as this
    /// store or any clone of it is alive.
    ///
    /// Fails with [`LocalCoreStateStoreError::OwnershipConflict`] while another
    /// store owns the namespace or has a transaction in flight. An owner serves
    /// logged records from memory after their first load.
    pub fn open_owned(root: impl Into<PathBuf>) -> Result<Self, LocalCoreStateStoreError> {
        let root = root.into();
        let lock = OwnerLock::acquire(&root, LockMode::Exclusive)?;
        Ok(Self {
            root,
            ownership: Ownership::Owned(Arc::new(Owner {
                _lock: lock,
                residents: Arc::default(),
                log: Mutex::default(),
            })),
            deferral: None,
            nodes: Arc::default(),
        })
    }

    /// Returns a handle whose context, lineage, and lazy-workspace commits
    /// defer durability until [`DeferredDurability::commit`], so a sequence of
    /// commits costs one flush.
    ///
    /// Deferred commits are visible at once and stay atomic and ordered: a
    /// crash before the flush loses a suffix of them. Once the returned guard
    /// commits or drops, the handle and its clones flush every commit again.
    /// Only an owner defers; a shared handle's commits stay flushed.
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
        let admission = match &self.ownership {
            Ownership::Shared => Admission::Shared {
                _lock: OwnerLock::acquire(&self.root, LockMode::Shared)?,
            },
            Ownership::Owned(owner) => Admission::Owned(Arc::clone(owner)),
        };
        let deferred = matches!(admission, Admission::Owned(_))
            && self
                .deferral
                .as_ref()
                .is_some_and(|deferral| deferral.load(Ordering::Acquire));
        Ok(Namespace {
            root: self.root.clone(),
            admission,
            durability: if deferred {
                Durability::Deferred
            } else {
                Durability::Flushed
            },
        })
    }

    /// Runs one admitted transaction without blocking an async executor.
    async fn transaction<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Namespace) -> Result<T, LocalCoreStateStoreError> + Send + 'static,
    ) -> Result<T, LocalCoreStateStoreError> {
        let store = self.clone();
        run_local_transaction(move || operation(&store.namespace()?)).await
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
    ///
    /// Only canonical journal names are returned. Temporary, previous, and
    /// lock files remain internal recovery details.
    pub async fn materialization_operations_async(
        &self,
    ) -> Result<Vec<OperationId>, LocalCoreStateStoreError> {
        self.transaction(|namespace| namespace.operation_records(MATERIALIZATION_FAMILY))
            .await
    }

    /// Removes a terminal materialization journal and its recovery copies
    /// without blocking an async executor.
    pub async fn remove_materialization_async(
        &self,
        operation_id: OperationId,
    ) -> Result<(), LocalCoreStateStoreError> {
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

/// Files of one record.
///
/// Borrowing the namespace that derived them confines every record access to
/// an admitted transaction.
#[derive(Debug)]
struct RecordPaths<'a> {
    directory: PathBuf,
    current: PathBuf,
    previous: PathBuf,
    temporary: PathBuf,
    lock: PathBuf,
    namespace: PhantomData<&'a Namespace>,
}

fn compare_and_swap<T: DeserializeOwned + Serialize + Revisioned>(
    paths: &RecordPaths,
    expected_revision: u64,
    replacement: &T,
) -> Result<bool, LocalCoreStateStoreError> {
    with_lock(paths, || {
        let current: Option<T> = read_recoverable(paths)?;
        if current.as_ref().map_or(0, T::revision) != expected_revision {
            return Ok(false);
        }
        write_journaled(paths, replacement)?;
        Ok(true)
    })
}

fn compare_and_delete<T: DeserializeOwned + Revisioned>(
    paths: &RecordPaths,
    expected_revision: u64,
) -> Result<bool, LocalCoreStateStoreError> {
    with_lock(paths, || {
        let current: Option<T> = read_recoverable(paths)?;
        if current.as_ref().map_or(0, T::revision) != expected_revision {
            return Ok(false);
        }
        remove_record(paths)?;
        Ok(true)
    })
}

/// Removes a record with its recovery copies.
fn remove_record(paths: &RecordPaths) -> Result<(), LocalCoreStateStoreError> {
    remove_if_present(&paths.temporary)?;
    remove_if_present(&paths.previous)?;
    remove_if_present(&paths.current)?;
    sync_directory(&paths.directory)?;
    Ok(())
}

#[derive(Clone, Copy)]
enum LockMode {
    Shared,
    Exclusive,
}

fn with_lock<T>(
    paths: &RecordPaths,
    action: impl FnOnce() -> Result<T, LocalCoreStateStoreError>,
) -> Result<T, LocalCoreStateStoreError> {
    with_lock_mode(paths, LockMode::Exclusive, action)
}

/// Reads one record. A readable current record needs no recovery, so readers
/// share the lock; only a missing or torn current record takes the exclusive
/// recovery path, which may promote the previous record.
fn read_locked<T: DeserializeOwned>(
    paths: &RecordPaths,
) -> Result<Option<T>, LocalCoreStateStoreError> {
    let current = with_lock_mode(paths, LockMode::Shared, || {
        Ok(read_json(&paths.current).ok().flatten())
    })?;
    match current {
        Some(value) => Ok(Some(value)),
        None => with_lock(paths, || read_recoverable(paths)),
    }
}

fn with_lock_mode<T>(
    paths: &RecordPaths,
    mode: LockMode,
    action: impl FnOnce() -> Result<T, LocalCoreStateStoreError>,
) -> Result<T, LocalCoreStateStoreError> {
    std::fs::create_dir_all(&paths.directory)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&paths.lock)?;
    let deadline = Instant::now() + RECORD_LOCK_DEADLINE;
    loop {
        let locked = match mode {
            LockMode::Shared => FileExt::try_lock_shared(&lock),
            LockMode::Exclusive => lock.try_lock_exclusive(),
        };
        match locked {
            Ok(()) => break,
            Err(error) if acyclic_native_runtime::is_exclusive_lock_contention(&error) => {
                if Instant::now() >= deadline {
                    return Err(LocalCoreStateStoreError::LockTimeout(paths.lock.clone()));
                }
                std::thread::sleep(RECORD_LOCK_RETRY);
            }
            Err(error) => return Err(error.into()),
        }
    }
    let result = action();
    FileExt::unlock(&lock)?;
    result
}

fn read_recoverable<T: DeserializeOwned>(
    paths: &RecordPaths,
) -> Result<Option<T>, LocalCoreStateStoreError> {
    match read_json(&paths.current) {
        Ok(Some(value)) => Ok(Some(value)),
        Ok(None) => match read_json(&paths.previous) {
            Ok(Some(value)) => {
                promote_previous(paths)?;
                Ok(Some(value))
            }
            other => other,
        },
        Err(current_error) => match read_json(&paths.previous) {
            Ok(Some(value)) => {
                promote_previous(paths)?;
                Ok(Some(value))
            }
            _ => Err(current_error),
        },
    }
}

fn promote_previous(paths: &RecordPaths) -> Result<(), LocalCoreStateStoreError> {
    std::fs::copy(&paths.previous, &paths.current)?;
    OpenOptions::new()
        .write(true)
        .open(&paths.current)?
        .sync_all()?;
    sync_directory(&paths.directory)?;
    Ok(())
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>, LocalCoreStateStoreError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(LocalCoreStateStoreError::Json)
}

fn read_json_bounded<T: DeserializeOwned>(
    path: &Path,
    maximum_bytes: u64,
) -> Result<Option<T>, LocalCoreStateStoreError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if file.metadata()?.len() > maximum_bytes {
        return Err(LocalCoreStateStoreError::ContextTransaction(
            "workspace-context child bucket exceeds its declared bound".to_owned(),
        ));
    }
    let mut bytes = Vec::new();
    file.take(maximum_bytes.saturating_add(1))
        .read_to_end(&mut bytes)?;
    let length = u64::try_from(bytes.len()).map_err(|_| {
        LocalCoreStateStoreError::ContextTransaction(
            "workspace-context child bucket length is not representable".to_owned(),
        )
    })?;
    if length > maximum_bytes {
        return Err(LocalCoreStateStoreError::ContextTransaction(
            "workspace-context child bucket grew beyond its declared bound".to_owned(),
        ));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(LocalCoreStateStoreError::Json)
}

fn write_journaled<T: Serialize>(
    paths: &RecordPaths,
    value: &T,
) -> Result<(), LocalCoreStateStoreError> {
    let bytes = serde_json::to_vec(value)?;
    let mut temporary = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&paths.temporary)?;
    temporary.write_all(&bytes)?;
    temporary.sync_all()?;
    drop(temporary);

    #[cfg(windows)]
    {
        // Windows has no safe, portable directory-fsync API. Preserve a
        // fully flushed recovery copy before modifying the current file, then
        // flush the current file itself. A crash can therefore expose either
        // the prior record or the complete replacement, never only a torn
        // current record.
        remove_if_present(&paths.previous)?;
        if paths.current.exists() {
            std::fs::copy(&paths.current, &paths.previous)?;
        } else {
            std::fs::copy(&paths.temporary, &paths.previous)?;
        }
        OpenOptions::new()
            .write(true)
            .open(&paths.previous)?
            .sync_all()?;
        std::fs::copy(&paths.temporary, &paths.current)?;
        OpenOptions::new()
            .write(true)
            .open(&paths.current)?
            .sync_all()?;
        remove_if_present(&paths.temporary)?;
        remove_if_present(&paths.previous)?;
        Ok(())
    }

    #[cfg(not(windows))]
    {
        remove_if_present(&paths.previous)?;
        if paths.current.exists() {
            std::fs::rename(&paths.current, &paths.previous)?;
        }
        if let Err(error) = std::fs::rename(&paths.temporary, &paths.current) {
            if paths.previous.exists() && !paths.current.exists() {
                let _ = std::fs::rename(&paths.previous, &paths.current);
            }
            return Err(error.into());
        }
        sync_directory(&paths.directory)?;
        remove_if_present(&paths.previous)?;
        Ok(())
    }
}

fn remove_if_present(path: &Path) -> Result<(), std::io::Error> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Makes the entries of directory `path` durable, with the one directory
/// synchronization every store shares; Windows flushes a directory handle.
fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    acyclic_native_runtime::sync_parent(path, acyclic_native_runtime::Durability::Full)
}

/// Guard for a [`LocalCoreStateStore::defer_durability`] scope.
#[must_use = "deferred commits are durable only once committed"]
#[derive(Debug)]
pub struct DeferredDurability {
    store: LocalCoreStateStore,
    deferral: Arc<AtomicBool>,
}

impl DeferredDurability {
    /// Ends the scope and makes every commit made through the log so far
    /// durable.
    pub async fn commit(self) -> Result<(), LocalCoreStateStoreError> {
        self.deferral.store(false, Ordering::Release);
        self.store
            .transaction(|root| root.with_log(CoreLog::flush))
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
    /// The core-state log holds an intact frame that does not decode, so it
    /// may commit changes this store cannot apply.
    #[error("core state log holds an undecodable intact frame: {}", .0.display())]
    Corrupt(PathBuf),
}



impl WorkspaceLineageStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<WorkspaceLineageRecord>, Self::Error> {
        self.load_logged(workspace_id).await
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_logged(workspace_id, expected_revision, replacement)
            .await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn load_with_native_work(
        &self,
        workspace_id: WorkspaceId,
        original: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Option<NativeOwnedCoreState<WorkspaceLineageRecord>>,
        (crate::lineage::WorkspaceLineageError<Self::Error>, Option<crate::materializer::NativeAllocation>)> {
        self.native_logged_transaction(original, cancellation, move |root, log, work, cancellation| {
            native_records::load_native_lineage(root, log, workspace_id, work, cancellation)
        }).await.map_err(|error| (crate::lineage::WorkspaceLineageError::Store(error), None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_swap_with_native_work(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: NativeOwnedCoreState<WorkspaceLineageRecord>,
        original: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<bool,
        (crate::lineage::WorkspaceLineageError<Self::Error>, Option<crate::materializer::NativeAllocation>)> {
        let replacement = native_transactions::checked_native_packet(
            replacement, original, cancellation, native_records::native_lineage_owned_bytes,
        ).map_err(|error| (crate::lineage::WorkspaceLineageError::Store(error), None))?;
        self.native_logged_transaction(original, cancellation, move |root, log, work, cancellation| {
            native_records::compare_native_lineage(root, log, workspace_id, expected_revision, replacement, work, cancellation)
        }).await.map_err(|error| (crate::lineage::WorkspaceLineageError::Store(error), None))
    }
}

impl WorkspaceContextStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(
        &self,
        context_id: WorkspaceContextId,
    ) -> Result<Option<WorkspaceContext>, Self::Error> {
        self.load_logged(context_id).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn load_with_native_work(
        &self,
        context_id: WorkspaceContextId,
        original: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Option<NativeOwnedCoreState<WorkspaceContext>>,
        (crate::workspace_context::WorkspaceContextError<Self::Error>, Option<crate::materializer::NativeAllocation>)> {
        self.native_logged_transaction(original, cancellation, move |root, log, work, cancellation| {
            native_records::load_native_context(root, log, context_id, work, cancellation)
        }).await.map_err(|error| (crate::workspace_context::WorkspaceContextError::Store(error), None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn compare_and_swap_with_native_work(
        &self,
        context_id: WorkspaceContextId,
        expected_revision: u64,
        replacement: NativeOwnedCoreState<WorkspaceContext>,
        original: &crate::materializer::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<bool,
        (crate::workspace_context::WorkspaceContextError<Self::Error>, Option<crate::materializer::NativeAllocation>)> {
        let replacement = native_transactions::checked_native_packet(
            replacement, original, cancellation, native_records::native_context_owned_bytes,
        ).map_err(|error| (crate::workspace_context::WorkspaceContextError::Store(error), None))?;
        self.native_logged_transaction(original, cancellation, move |root, log, work, cancellation| {
            native_records::compare_native_context(root, log, context_id, expected_revision, replacement, work, cancellation)
        }).await.map_err(|error| (crate::workspace_context::WorkspaceContextError::Store(error), None))
    }

    async fn list(&self) -> Result<Vec<WorkspaceContext>, Self::Error> {
        self.transaction(move |root| {
            root.with_log(|log| {
                let contexts = load_all_contexts(root, log)?;
                if !context_children_index_ready(root, log)? {
                    log.commit(root, index_change(contexts.iter().cloned())?)?;
                }
                Ok(contexts)
            })
        })
        .await
    }

    async fn compare_and_swap(
        &self,
        context_id: WorkspaceContextId,
        expected_revision: u64,
        replacement: WorkspaceContext,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_many(
            BTreeMap::from([(context_id, expected_revision)]),
            vec![replacement],
            false,
        )
        .await
    }

    async fn compare_and_swap_many(
        &self,
        expected_revisions: BTreeMap<WorkspaceContextId, u64>,
        replacements: Vec<WorkspaceContext>,
        require_exact_set: bool,
    ) -> Result<bool, Self::Error> {
        self.transaction(move |root| {
            root.with_log(|log| {
                if require_exact_set
                    && live_context_ids(root, log)?.len() != expected_revisions.len()
                {
                    return Ok(false);
                }
                let mut before = BTreeMap::new();
                for (context_id, expected) in &expected_revisions {
                    let current = log.get::<WorkspaceContext>(root, *context_id)?;
                    if current.as_ref().map_or(0, Revisioned::revision) != *expected {
                        return Ok(false);
                    }
                    before.insert(*context_id, current);
                }
                let after = replacements
                    .into_iter()
                    .map(|replacement| (replacement.context_id, replacement))
                    .collect::<BTreeMap<_, _>>();
                if after
                    .keys()
                    .any(|context_id| !expected_revisions.contains_key(context_id))
                {
                    return Ok(false);
                }
                if !context_children_index_ready(root, log)? {
                    let contexts = load_all_contexts(root, log)?;
                    log.commit(root, index_change(contexts)?)?;
                }
                let affected_parents = before
                    .values()
                    .filter_map(|record| record.as_ref()?.parent_context_id)
                    .chain(after.values().filter_map(|record| record.parent_context_id))
                    .collect::<BTreeSet<_>>();
                let mut children = WorkspaceContextChildren::new();
                for parent in &affected_parents {
                    let current = log.get::<ContextChildren>(root, *parent)?;
                    children.insert(*parent, current.unwrap_or_default().0);
                }
                for (context_id, replacement) in &after {
                    update_context_children(
                        &mut children,
                        before.get(context_id).and_then(Option::as_ref),
                        Some(replacement),
                    );
                }
                let mut change = Change::default();
                for (context_id, record) in after {
                    change.put(context_id, Some(record));
                }
                for parent in affected_parents {
                    change.put_children(parent, children.remove(&parent))?;
                }
                log.commit(root, change)?;
                Ok(true)
            })
        })
        .await
    }

    async fn discard_subtree(
        &self,
        parent: WorkspaceContextId,
        child: WorkspaceContextId,
        maximum: u32,
    ) -> Result<WorkspaceContextDiscardOutcome, Self::Error> {
        self.transaction(move |root_path| {
            root_path.with_log(|log| {
                if !context_children_index_ready(root_path, log)? {
                    return Ok(WorkspaceContextDiscardOutcome::IncompatibleState);
                }
                let root = log.get::<WorkspaceContext>(root_path, child)?;
                let discarded = match plan_local_context_subtree_discard(
                    root_path,
                    log,
                    root.as_ref(),
                    parent,
                    child,
                    maximum,
                )? {
                    Ok(discarded) => discarded,
                    Err(outcome) => return Ok(outcome),
                };
                let mut records = BTreeMap::from_iter(root.map(|record| (child, record)));
                for context_id in discarded.iter().copied().filter(|id| *id != child) {
                    let Some(record) = log.get::<WorkspaceContext>(root_path, context_id)? else {
                        return Ok(WorkspaceContextDiscardOutcome::IncompatibleState);
                    };
                    records.insert(context_id, record);
                }
                let selected_children = context_children(records.values().cloned());
                let outcome = discard_context_subtree(
                    &mut records,
                    &selected_children,
                    parent,
                    child,
                    maximum,
                );
                if !matches!(
                    &outcome,
                    WorkspaceContextDiscardOutcome::Discarded(actual) if actual == &discarded
                ) {
                    return Ok(match outcome {
                        WorkspaceContextDiscardOutcome::Discarded(_) => {
                            WorkspaceContextDiscardOutcome::IncompatibleState
                        }
                        outcome => outcome,
                    });
                }
                let WorkspaceContextDiscardOutcome::Discarded(discarded) = &outcome else {
                    return Ok(outcome);
                };
                let mut change = Change::default();
                for context_id in discarded {
                    let replacement = records.get(context_id).cloned().ok_or_else(|| {
                        LocalCoreStateStoreError::ContextTransaction(
                            "discarded context replacement is absent".to_owned(),
                        )
                    })?;
                    change.put(*context_id, Some(replacement));
                }
                log.commit(root_path, change)?;
                Ok(outcome)
            })
        })
        .await
    }
}

const WORKSPACE_CONTEXT_FAMILY: &str = "workspace-contexts-v2";
const CONTEXT_CHILDREN_FAMILY: &str = "workspace-context-children-v1";
const CONTEXT_CHILDREN_INDEX_FAMILY: &str = "workspace-context-children-index-v1";
const CONTEXT_CHILD_COUNT_FAMILY: &str = "workspace-context-child-counts-v1";
const LINEAGE_FAMILY: &str = "lineage";
const CORE_LOG_FAMILY: &str = "core-state-log-v1";
/// Commits an owner keeps in its log before writing their records back. Tests
/// checkpoint every few commits so crash tests cover owned checkpoints.
const OWNED_CHECKPOINT: u64 = if cfg!(test) { 3 } else { 256 };

impl LocalCoreStateStore {
    /// Loads one logged record, from an owner's memory when it is resident.
    async fn load_logged<T: Logged>(
        &self,
        id: T::Id,
    ) -> Result<Option<T>, LocalCoreStateStoreError> {
        if let Ownership::Owned(owner) = &self.ownership
            && let Some(value) = owner.resident::<T>(id)
        {
            return Ok(value);
        }
        self.transaction(move |root| root.with_log(|log| log.get::<T>(root, id)))
            .await
    }

    /// Commits `replacement` only while the record is at `expected_revision`
    /// (`0` means absent).
    async fn compare_and_swap_logged<T: Logged + Revisioned>(
        &self,
        id: T::Id,
        expected_revision: u64,
        replacement: T,
    ) -> Result<bool, LocalCoreStateStoreError> {
        self.transaction(move |root| {
            root.with_log(|log| {
                if log.get::<T>(root, id)?.as_ref().map_or(0, T::revision) != expected_revision {
                    return Ok(false);
                }
                let mut change = Change::default();
                change.put(id, Some(replacement));
                log.commit(root, change)?;
                Ok(true)
            })
        })
        .await
    }
}

impl Owner {
    /// Returns a resident record (`Some(None)` for a known absence) without
    /// touching the log. Every commit and every replay updates residents
    /// before its transaction ends, so a resident record is never stale.
    fn resident<T: Logged>(&self, id: T::Id) -> Option<Option<T>> {
        let residents = self.residents.read().ok()?;
        let value = residents.get(&T::key(id))?;
        if matches!(value, Some(RecordValue::FlatContext(_)) | Some(RecordValue::FlatChildren(_))) {
            return None;
        }
        Some(value.as_ref().and_then(T::from_value).cloned())
    }

    fn log(&self) -> MutexGuard<'_, Option<CoreLog>> {
        // A transaction that panics has taken the log, so a poisoned slot is
        // empty and the next transaction reopens and replays.
        self.log.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Namespace {
    /// Runs one transaction over the replayed core-state log.
    ///
    /// An owner serializes its transactions on its own mutex, keeps its log
    /// open between them, and replays a predecessor's log only when it opens
    /// it; a due checkpoint runs before the next transaction rather than inside
    /// the commit that made it due, so a committed change is never reported as
    /// failed. A shared store takes the log's file lock, replays and
    /// checkpoints whatever an earlier transaction or owner left, and
    /// checkpoints its own commits before releasing the lock. Its frames are
    /// already flushed then, so the next opener finds an empty log instead of
    /// re-flushing and replaying it; a failed checkpoint leaves the committed
    /// frames for that opener, exactly as before.
    fn with_log<T>(
        &self,
        operation: impl FnOnce(&mut CoreLog) -> Result<T, LocalCoreStateStoreError>,
    ) -> Result<T, LocalCoreStateStoreError> {
        match &self.admission {
            Admission::Shared { .. } => with_lock(&core_log_paths(self), || {
                let mut log = CoreLog::open(self, Arc::default())?;
                let result = operation(&mut log);
                if !log.dirty.is_empty() && !log.broken {
                    drop(log.checkpoint(self));
                }
                result
            }),
            Admission::Owned(owner) => {
                let mut slot = owner.log();
                let mut log = match slot.take().filter(|log| !log.broken) {
                    Some(log) => log,
                    None => CoreLog::open(self, Arc::clone(&owner.residents))?,
                };
                let result = if log.sequence >= OWNED_CHECKPOINT {
                    log.checkpoint(self)
                } else {
                    Ok(())
                }
                .and_then(|()| operation(&mut log));
                *slot = Some(log);
                result
            }
        }
    }
}

/// Identity of one record served through the core-state log.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
enum RecordKey {
    Context(WorkspaceContextId),
    Children(WorkspaceContextId),
    ChildCount(WorkspaceContextId),
    ChildIndex,
    Lineage(WorkspaceId),
    LazyWorkspace(WorkspaceId),
}

impl RecordKey {
    fn paths(self, root: &Namespace) -> RecordPaths<'_> {
        match self {
            Self::Context(id) => root.record(WORKSPACE_CONTEXT_FAMILY, &id.into_bytes()),
            Self::Children(id) => root.record(CONTEXT_CHILDREN_FAMILY, &id.into_bytes()),
            Self::ChildCount(id) => root.record(CONTEXT_CHILD_COUNT_FAMILY, &id.into_bytes()),
            Self::ChildIndex => root.record(CONTEXT_CHILDREN_INDEX_FAMILY, &[0; 16]),
            Self::Lineage(id) => root.record(LINEAGE_FAMILY, &id.into_bytes()),
            Self::LazyWorkspace(id) => root.record(LAZY_WORKSPACE_FAMILY, &id.into_bytes()),
        }
    }
}

/// One logged record's value; its variant always matches its key's.
#[derive(Clone, Debug, PartialEq, Deserialize)]
enum RecordValue {
    Context(WorkspaceContext),
    Children(ContextChildren),
    ChildCount(ContextChildCount),
    ChildIndex(ContextChildIndex),
    Lineage(WorkspaceLineageRecord),
    LazyWorkspace(LazyWorkspaceState),
    /// Flat replay DTOs stay flat until an ordinary context consumer requests them.
    FlatContext(FlatWorkspaceContext),
    FlatChildren(Vec<WorkspaceContextId>),
}

impl Serialize for RecordValue {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Context(value) => serializer.serialize_newtype_variant("RecordValue", 0, "Context", value),
            Self::Children(value) => serializer.serialize_newtype_variant("RecordValue", 1, "Children", value),
            Self::ChildCount(value) => serializer.serialize_newtype_variant("RecordValue", 2, "ChildCount", value),
            Self::ChildIndex(value) => serializer.serialize_newtype_variant("RecordValue", 3, "ChildIndex", value),
            Self::Lineage(value) => serializer.serialize_newtype_variant("RecordValue", 4, "Lineage", value),
            Self::LazyWorkspace(value) => serializer.serialize_newtype_variant("RecordValue", 5, "LazyWorkspace", value),
            Self::FlatContext(value) => serializer.serialize_newtype_variant("RecordValue", 0, "Context", value),
            Self::FlatChildren(value) => serializer.serialize_newtype_variant("RecordValue", 1, "Children", value),
        }
    }
}

impl RecordValue {
    /// Encodes a record file: the bare value, or `null` for an absence.
    fn encode(value: Option<&Self>) -> Result<Vec<u8>, serde_json::Error> {
        match value {
            None => serde_json::to_vec(&()),
            Some(Self::Context(value)) => serde_json::to_vec(value),
            Some(Self::Children(value)) => serde_json::to_vec(value),
            Some(Self::ChildCount(value)) => serde_json::to_vec(value),
            Some(Self::ChildIndex(value)) => serde_json::to_vec(value),
            Some(Self::Lineage(value)) => serde_json::to_vec(value),
            Some(Self::LazyWorkspace(value)) => serde_json::to_vec(value),
            Some(Self::FlatContext(value)) => serde_json::to_vec(value),
            Some(Self::FlatChildren(value)) => serde_json::to_vec(value),
        }
    }
}

/// A record type served through the core-state log, bound to its key.
trait Logged: Clone + Serialize + DeserializeOwned + Send + 'static {
    type Id: Copy + Send + 'static;

    fn key(id: Self::Id) -> RecordKey;
    fn into_value(self) -> RecordValue;
    fn from_value(value: &RecordValue) -> Option<&Self>;
}

macro_rules! logged {
    ($type:ty, $id:ty, $variant:ident, |$key:ident| $record:expr) => {
        impl Logged for $type {
            type Id = $id;

            fn key($key: $id) -> RecordKey {
                $record
            }

            fn into_value(self) -> RecordValue {
                RecordValue::$variant(self)
            }

            fn from_value(value: &RecordValue) -> Option<&Self> {
                match value {
                    RecordValue::$variant(value) => Some(value),
                    _ => None,
                }
            }
        }
    };
}

/// Direct children of one context.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
struct ContextChildren(BTreeSet<WorkspaceContextId>);

/// Number of direct children of one context, read before its bucket.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
struct ContextChildCount(u64);

/// Marks the child index as complete for every context.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
struct ContextChildIndex(u16);

logged!(WorkspaceContext, WorkspaceContextId, Context, |id| {
    RecordKey::Context(id)
});
logged!(ContextChildren, WorkspaceContextId, Children, |id| {
    RecordKey::Children(id)
});
logged!(ContextChildCount, WorkspaceContextId, ChildCount, |id| {
    RecordKey::ChildCount(id)
});
logged!(ContextChildIndex, (), ChildIndex, |_id| {
    RecordKey::ChildIndex
});
logged!(WorkspaceLineageRecord, WorkspaceId, Lineage, |id| {
    RecordKey::Lineage(id)
});
logged!(LazyWorkspaceState, WorkspaceId, LazyWorkspace, |id| {
    RecordKey::LazyWorkspace(id)
});

/// Complete after-images of the records one commit changes; `None` records
/// an absence. Deserialization checks key/value types before replay.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "Vec<(RecordKey, Option<RecordValue>)>")]
struct Change(Vec<(RecordKey, Option<RecordValue>)>);

impl TryFrom<Vec<(RecordKey, Option<RecordValue>)>> for Change {
    type Error = &'static str;

    fn try_from(records: Vec<(RecordKey, Option<RecordValue>)>) -> Result<Self, Self::Error> {
        for (key, value) in &records {
            let Some(value) = value else { continue };
            let compatible = match key {
                RecordKey::Context(_) => WorkspaceContext::from_value(value).is_some(),
                RecordKey::Children(_) => ContextChildren::from_value(value).is_some(),
                RecordKey::ChildCount(_) => ContextChildCount::from_value(value).is_some(),
                RecordKey::ChildIndex => ContextChildIndex::from_value(value).is_some(),
                RecordKey::Lineage(_) => WorkspaceLineageRecord::from_value(value).is_some(),
                RecordKey::LazyWorkspace(_) => LazyWorkspaceState::from_value(value).is_some(),
            };
            if !compatible {
                return Err("core-state record value does not match its key");
            }
        }
        Ok(Self(records))
    }
}

impl Change {
    fn put<T: Logged>(&mut self, id: T::Id, value: Option<T>) {
        self.0.push((T::key(id), value.map(T::into_value)));
    }

    /// Replaces a child bucket together with the count read before it.
    fn put_children(
        &mut self,
        parent: WorkspaceContextId,
        descendants: Option<BTreeSet<WorkspaceContextId>>,
    ) -> Result<(), LocalCoreStateStoreError> {
        let count = child_bucket_count(descendants.as_ref().map(BTreeSet::len))?;
        self.put(parent, descendants.map(ContextChildren));
        self.put(parent, count);
        Ok(())
    }
}

/// Both ordinary and original-funded afterimages use the same absence/count policy.
fn child_bucket_count(descendants: Option<usize>) -> Result<Option<ContextChildCount>, LocalCoreStateStoreError> {
    descendants.map(|count| {
        u64::try_from(count).map(ContextChildCount).map_err(|_| {
            LocalCoreStateStoreError::MaterializationWork(
                crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow),
            )
        })
    }).transpose()
}
/// Original after-images and their original-ledger payload/capacity custody.
#[derive(Debug)]
pub(super) struct NativeChange {
    change: Change,
    payloads: Vec<crate::materializer::NativeAllocation>,
    slots: NativeSlotCustody,
}

impl NativeChange {
    pub(super) fn new(work: &crate::materializer::NativeMaterializationWork) -> Result<Self, LocalCoreStateStoreError> {
        work.charge(crate::WorkCounters::UNCHARGED)?;
        Ok(Self { change: Change::default(), payloads: Vec::new(), slots: NativeSlotCustody::default() })
    }

    pub(super) fn put_owned(
        &mut self, key: RecordKey, value: NativeOwnedCoreState<RecordValue>,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        self.push(key, Some(value), work)
    }

    pub(super) fn put_scalar(
        &mut self, key: RecordKey, value: Option<RecordValue>,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        if let Some(value) = &value {
            if native_records::native_record_owned_bytes(value, work)? != 0 {
                return Err(LocalCoreStateStoreError::Integrity);
            }
        }
        let value = match value {
            Some(value) => Some(NativeOwnedCoreState::from_parts(value, work.claim_allocation(0, 0)?)),
            None => None,
        };
        self.push(key, value, work)
    }

    fn push(
        &mut self, key: RecordKey, value: Option<NativeOwnedCoreState<RecordValue>>,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        let mut empty = work.claim_allocation(0, 0)?;
        let needed = self.change.0.len() + 1;
        self.slots.reserve(&mut self.change.0, needed, work)?;
        self.slots.reserve(&mut self.payloads, needed, work)?;
        let value = if let Some(value) = value {
            let (value, mut allocation) = value.into_parts();
            let value = value;
            let compatible = matches!((key, &value),
                (RecordKey::Context(_), RecordValue::Context(_) | RecordValue::FlatContext(_)) |
                (RecordKey::Children(_), RecordValue::Children(_) | RecordValue::FlatChildren(_)) |
                (RecordKey::ChildCount(_), RecordValue::ChildCount(_)) |
                (RecordKey::ChildIndex, RecordValue::ChildIndex(_)) |
                (RecordKey::Lineage(_), RecordValue::Lineage(_)) |
                (RecordKey::LazyWorkspace(_), RecordValue::LazyWorkspace(_)));
            if !compatible { return Err(LocalCoreStateStoreError::Integrity); }
            if !allocation.matches_work(work) {
                drop(value);
                return Err(LocalCoreStateStoreError::NativeLedgerMismatch);
            }
            empty.absorb(&mut allocation)?;
            Some(value)
        } else { None };
        self.change.0.push((key, value));
        self.payloads.push(empty);
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct NativeInstall {
    key: RecordKey,
    resident: usize,
    payload: usize,
    dirty: bool,
}

/// Direct children of every context in `records`.
fn context_children(
    records: impl IntoIterator<Item = WorkspaceContext>,
) -> WorkspaceContextChildren {
    let mut children = WorkspaceContextChildren::new();
    for record in records {
        update_context_children(&mut children, None, Some(&record));
    }
    children
}

/// Installs the child index derived from every context.
fn index_change(
    records: impl IntoIterator<Item = WorkspaceContext>,
) -> Result<Change, LocalCoreStateStoreError> {
    let mut change = Change::default();
    for (parent, descendants) in context_children(records) {
        change.put_children(parent, Some(descendants))?;
    }
    change.put((), Some(ContextChildIndex(1)));
    Ok(change)
}

/// One log line: `<blake3 hex> <frame json>`.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogFrame<C> {
    epoch: Uuid,
    sequence: u64,
    change: C,
}

/// Explicit slots make resident admission measurable without allocator bucket guesses.
#[derive(Debug, Default)]
struct Residents(
    Vec<(RecordKey, Option<RecordValue>)>,
    Vec<(RecordKey, crate::materializer::NativeAllocation)>,
    NativeSlotCustody,
);

impl Residents {
    fn get(&self, key: &RecordKey) -> Option<&Option<RecordValue>> {
        self.0.iter().find(|(candidate, _)| candidate == key).map(|(_, value)| value)
    }

    fn insert(&mut self, key: RecordKey, value: Option<RecordValue>) {
        if let Some((_, resident)) = self.0.iter_mut().find(|(candidate, _)| *candidate == key) {
            *resident = value;
        } else {
            self.0.push((key, value));
        }
        self.1.retain(|(candidate, _)| *candidate != key);
    }

    fn clear(&mut self) {
        self.0.clear();
        self.1.clear();
    }
    fn keys(&self) -> impl Iterator<Item = &RecordKey> { self.0.iter().map(|(key, _)| key) }

    fn payload_slot(
        &mut self,
        key: RecordKey,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<usize, LocalCoreStateStoreError> {
        for (index, (candidate, _)) in self.1.iter().enumerate() {
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
            if *candidate == key { return Ok(index); }
        }
        let needed = self.1.len() + 1;
        self.2.reserve(&mut self.1, needed, work)?;
        self.1.push((key, work.claim(0)?));
        Ok(self.1.len() - 1)
    }

    fn adopt_native(&mut self, work: &crate::materializer::NativeMaterializationWork) -> Result<(), LocalCoreStateStoreError> {
        let needed = self.0.len();
        self.2.reserve(&mut self.0, needed, work)?;
        let needed = self.1.len();
        self.2.reserve(&mut self.1, needed, work)?;
        for index in 0..self.0.len() {
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
            let key = self.0[index].0;
            let bytes = self.0[index].1.as_ref()
                .map(|value| native_records::native_record_owned_bytes(value, work)).transpose()?.unwrap_or(0);
            let slot = self.payload_slot(key, work)?;
            if self.1[slot].1.bytes() < bytes {
                let mut prefix = work.claim_allocation(bytes - self.1[slot].1.bytes(), 0)?;
                self.1[slot].1.absorb(&mut prefix)?;
            }
        }
        Ok(())
    }
}

/// Public context records use trees; log replay retains their exact flat JSON shape.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FlatWorkspaceContext {
    version: u32,
    revision: u64,
    context_id: WorkspaceContextId,
    parent_context_id: Option<WorkspaceContextId>,
    roots: FlatMap<crate::WorkspaceRootId, FlatWorkspaceContextRoot>,
    state: crate::workspace_context::WorkspaceContextState,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FlatWorkspaceContextRoot {
    root_id: crate::WorkspaceRootId,
    source_path: String,
    workspace_id: WorkspaceId,
    workspace_name: String,
    parent_workspace_id: Option<WorkspaceId>,
    mount_path: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
struct FlatMap<K, V>(Vec<(K, V)>);

impl<K: Serialize, V: Serialize> Serialize for FlatMap<K, V> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in &self.0 { map.serialize_entry(key, value)?; }
        map.end()
    }
}

impl<'de, K: Deserialize<'de>, V: Deserialize<'de>> Deserialize<'de> for FlatMap<K, V> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FlatMapVisitor<K, V>(PhantomData<(K, V)>);
        impl<'de, K: Deserialize<'de>, V: Deserialize<'de>> serde::de::Visitor<'de> for FlatMapVisitor<K, V> {
            type Value = FlatMap<K, V>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a core-state flat map")
            }
            fn visit_newtype_struct<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
                deserializer.deserialize_map(self)
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut entries = Vec::new();
                while let Some(entry) = map.next_entry()? { entries.push(entry); }
                Ok(FlatMap(entries))
            }
        }
        deserializer.deserialize_newtype_struct("CoreStateFlatMap", FlatMapVisitor(PhantomData))
    }
}

#[derive(Deserialize)]
enum FlatRecordValue {
    Context(FlatWorkspaceContext),
    Children(Vec<WorkspaceContextId>),
    ChildCount(ContextChildCount),
    ChildIndex(ContextChildIndex),
    Lineage(WorkspaceLineageRecord),
    LazyWorkspace(LazyWorkspaceState),
}

impl FlatRecordValue {
    fn owned_bytes(
        &self,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<u64, LocalCoreStateStoreError> {
        Ok(match self {
            Self::Context(context) => {
                work.charge(crate::WorkCounters { items_examined: context.roots.0.len() as u64, ..Default::default() })?;
                context.owned_bytes()
            }
            Self::Children(children) => (children.capacity() * std::mem::size_of::<WorkspaceContextId>()) as u64,
            Self::Lineage(lineage) => lineage.workspace_name.capacity() as u64
                + lineage.parent_workspace_name.as_ref().map_or(0, |name| name.capacity() as u64),
            _ => 0,
        })
    }

    fn matches(&self, key: RecordKey) -> bool {
        matches!((key, self),
            (RecordKey::Context(_), Self::Context(_)) |
            (RecordKey::Children(_), Self::Children(_)) |
            (RecordKey::ChildCount(_), Self::ChildCount(_)) |
            (RecordKey::ChildIndex, Self::ChildIndex(_)) |
            (RecordKey::Lineage(_), Self::Lineage(_)) |
            (RecordKey::LazyWorkspace(_), Self::LazyWorkspace(_)))
    }

    fn into_resident(self) -> RecordValue {
        match self {
            Self::Context(value) => RecordValue::FlatContext(value),
            Self::Children(value) => RecordValue::FlatChildren(value),
            Self::ChildCount(value) => RecordValue::ChildCount(value),
            Self::ChildIndex(value) => RecordValue::ChildIndex(value),
            Self::Lineage(value) => RecordValue::Lineage(value),
            Self::LazyWorkspace(value) => RecordValue::LazyWorkspace(value),
        }
    }
}

impl FlatWorkspaceContext {
    fn owned_bytes(&self) -> u64 {
        let slots = (self.roots.0.capacity()
            * std::mem::size_of::<(crate::WorkspaceRootId, FlatWorkspaceContextRoot)>()) as u64;
        self.roots.0.iter().fold(slots, |bytes, (_, root)| {
            bytes + root.source_path.capacity() as u64
                + root.workspace_name.capacity() as u64
                + root.mount_path.as_ref().map_or(0, |path| path.capacity() as u64)
        })
    }
}

/// Write-ahead log through which workspace contexts, their child index,
/// lineage, and lazy-workspace bindings change.
///
/// A change is committed once its checksummed frame is flushed. The change is
/// then resident in memory, and its record files are written back only by a
/// checkpoint, which flushes the log, writes and flushes every changed record,
/// and only then truncates the log into a new epoch. Every opener replays the
/// log's valid prefix into memory and checkpoints it before any record is
/// read, so a crash of the process or the machine exposes every record of a
/// change or none of them.
///
/// Record files are only ever rewritten in place, and an absent record is
/// written as `null` rather than deleted, so recovery never depends on a
/// directory update becoming durable: on Windows, which has no directory
/// flush, a checkpoint needs only each record file flushed through its own
/// handle, exactly as the journaled single-record path relies on.
///
/// Frames carry the log's epoch and a consecutive sequence. Replay stops at
/// the first torn or out-of-sequence frame, which can only follow the last
/// flush, so its changes were never written back. A checkpoint starts a new
/// epoch, so frames a not-yet-durable truncation leaves behind are never
/// replayed after a newer frame.
///
/// A deferred commit is appended without a flush; it becomes durable together
/// with the next flush of the log, and a crash before then loses it together
/// with every later frame.
#[derive(Debug)]
struct CoreLog {
    file: File,
    /// Bytes appended to the log.
    length: u64,
    /// Bytes of the log known to be durable.
    flushed: u64,
    epoch: Uuid,
    sequence: u64,
    residents: Arc<RwLock<Residents>>,
    dirty: Vec<RecordKey>,
    /// The log may hold a frame whose outcome is unknown, so it must be
    /// reopened and replayed before it is used again.
    broken: bool,
    slots: NativeSlotCustody,
    residents_control: Option<crate::materializer::NativeAllocation>,
}

impl CoreLog {
    fn open(
        root: &Namespace,
        residents: Arc<RwLock<Residents>>,
    ) -> Result<Self, LocalCoreStateStoreError> {
        let paths = core_log_paths(root);
        std::fs::create_dir_all(&paths.directory)?;
        let mut file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&paths.current)
        {
            Ok(file) => {
                sync_directory(&paths.directory)?;
                file
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => OpenOptions::new()
                .read(true)
                .write(true)
                .open(&paths.current)?,
            Err(error) => return Err(error.into()),
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        // A frame found here may have been appended by a process that never
        // flushed it, so the checkpoint below flushes the log first.
        let mut log = Self {
            file,
            length: bytes.len() as u64,
            flushed: 0,
            epoch: Uuid::now_v7(),
            sequence: 0,
            residents,
            dirty: Vec::new(),
            broken: false,
            slots: NativeSlotCustody::default(),
            residents_control: None,
        };
        log.residents().clear();
        if bytes.is_empty() {
            return Ok(log);
        }
        let changes = replayable_changes(&bytes)
            .map_err(|_| LocalCoreStateStoreError::Corrupt(paths.current.clone()))?;
        for change in changes {
            log.apply(change);
        }
        log.checkpoint(root)?;
        Ok(log)
    }

    fn residents(&self) -> RwLockWriteGuard<'_, Residents> {
        self.residents
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Reads one record from memory, or from its file on first use.
    fn get<T: Logged>(
        &mut self,
        root: &Namespace,
        id: T::Id,
    ) -> Result<Option<T>, LocalCoreStateStoreError> {
        self.get_bounded(root, id, None)
    }

    /// Reads one record, refusing a file larger than `maximum_bytes`.
    fn get_bounded<T: Logged>(
        &mut self,
        root: &Namespace,
        id: T::Id,
        maximum_bytes: Option<u64>,
    ) -> Result<Option<T>, LocalCoreStateStoreError> {
        let key = T::key(id);
        if let Some(value) = self.residents().get(&key) {
            return match value {
                Some(RecordValue::FlatContext(_)) | Some(RecordValue::FlatChildren(_)) =>
                    Ok(serde_json::from_slice(&RecordValue::encode(value.as_ref())?)?),
                _ => Ok(value.as_ref().and_then(T::from_value).cloned()),
            };
        }
        let paths = key.paths(root);
        let value = match maximum_bytes {
            Some(maximum_bytes) => read_json_bounded::<Option<T>>(&paths.current, maximum_bytes)?,
            None => read_json::<Option<T>>(&paths.current)?,
        }
        .flatten();
        self.residents()
            .insert(key, value.clone().map(T::into_value));
        Ok(value)
    }

    /// Keys of resident records that satisfy `select`.
    fn resident_keys<K>(&self, select: impl Fn(RecordKey) -> Option<K>) -> Vec<K> {
        self.residents
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .filter_map(|key| select(*key))
            .collect()
    }

    /// Commits `change`, durably unless `root` defers durability.
    ///
    /// A failed append is cut back off the log and flushed away, so the change
    /// is reported failed only when it can never be replayed; if that repair
    /// fails too, the log is marked broken and the outcome is unknown.
    fn commit(&mut self, root: &Namespace, change: Change) -> Result<(), LocalCoreStateStoreError> {
        let frame = serde_json::to_vec(&LogFrame {
            epoch: self.epoch,
            sequence: self.sequence,
            change: &change,
        })?;
        let mut line = blake3::hash(&frame).to_hex().as_bytes().to_vec();
        line.push(b' ');
        line.extend_from_slice(&frame);
        line.push(b'\n');
        let deferred = root.durability == Durability::Deferred;
        if let Err(error) = self.append(&line, deferred) {
            let length = self.length;
            self.broken = self
                .file
                .set_len(length)
                .and_then(|()| self.file.seek(SeekFrom::Start(length)).map(drop))
                .and_then(|()| self.file.sync_data())
                .is_err();
            return Err(error.into());
        }
        self.sequence += 1;
        self.apply(change);
        Ok(())
    }

    fn append(&mut self, line: &[u8], deferred: bool) -> std::io::Result<()> {
        crash_point("before frame", self.flushed);
        self.file.write_all(line)?;
        let length = self.length + line.len() as u64;
        crash_point("frame written", self.flushed);
        if !deferred {
            self.file.sync_data()?;
            self.flushed = length;
            crash_point("frame flushed", self.flushed);
        }
        self.length = length;
        Ok(())
    }

    /// Makes every appended frame durable.
    fn flush(&mut self) -> Result<(), LocalCoreStateStoreError> {
        if self.flushed < self.length {
            self.file.sync_data()?;
            self.flushed = self.length;
            crash_point("frames flushed", self.flushed);
        }
        Ok(())
    }

    /// Makes a committed change resident; its files are written back later.
    fn apply(&mut self, change: Change) {
        let mut residents = self
            .residents
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        for (key, value) in change.0 {
            residents.insert(key, value);
            if !self.dirty.contains(&key) {
                self.dirty.push(key);
            }
        }
    }

    /// Flushes the log, writes back and flushes every changed record, then
    /// truncates the log into a new epoch.
    fn checkpoint(&mut self, root: &Namespace) -> Result<(), LocalCoreStateStoreError> {
        self.flush()?;
        let mut directories = BTreeSet::new();
        for key in &self.dirty {
            let bytes = RecordValue::encode(
                self.residents
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(key)
                    .and_then(Option::as_ref),
            )?;
            let paths = key.paths(root);
            std::fs::create_dir_all(&paths.directory)?;
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&paths.current)?;
            file.write_all(&bytes)?;
            crash_point("record written", self.flushed);
            file.sync_all()?;
            crash_point("record flushed", self.flushed);
            directories.insert(paths.directory);
        }
        for directory in directories {
            sync_directory(&directory)?;
        }
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.length = 0;
        self.flushed = 0;
        crash_point("log truncated", self.flushed);
        self.epoch = Uuid::now_v7();
        self.sequence = 0;
        self.dirty.clear();
        Ok(())
    }
}

fn measured_slots<T>(
    values: &mut Vec<T>,
    needed: usize,
    work: &crate::materializer::NativeMaterializationWork,
    allocation: &mut crate::materializer::NativeAllocation,
) -> Result<(), LocalCoreStateStoreError> {
    let element = std::mem::size_of::<T>();
    let prior = values.capacity().checked_mul(element)
        .ok_or(crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow))? as u64;
    if allocation.bytes() < prior {
        let mut prefix = work.claim_allocation(prior - allocation.bytes(), 0)?;
        allocation.absorb(&mut prefix)?;
    }
    if needed <= values.capacity() { return Ok(()); }
    let next = needed.max(values.capacity().saturating_mul(2)).max(1);
    let bytes = next.checked_mul(element)
        .ok_or(crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow))? as u64;
    let copied = crate::WorkCounters {
        bytes_copied: (values.len() * element) as u64, ..Default::default()
    };
    work.admit(copied)?;
    // The old physical buffer and the entire new allocation overlap in realloc.
    let mut replacement = work.claim_allocation(bytes, 1)?;
    values.try_reserve_exact(next - values.len())
        .map_err(crate::materializer::NativeTreeMaterializationError::AllocationFailed)?;
    let actual = (values.capacity() * element) as u64;
    let late = if actual > bytes {
        let (mut excess, result) = work.record_completed_allocation(actual - bytes);
        replacement.absorb(&mut excess)?;
        result
    } else {
        replacement.release(bytes - actual)
    };
    // Realloc has dropped the old buffer; attach all new capacity before refusal.
    let retired = std::mem::replace(allocation, replacement);
    drop(retired);
    late?;
    work.spent(copied)?;
    Ok(())
}

/// Retains real backing-vector capacity until its physical owner is dropped.
/// These are guards in the original ledger, not another allocation ledger.
#[derive(Debug, Default)]
struct NativeSlotCustody {
    guards: Vec<(usize, crate::materializer::NativeAllocation)>,
    // Never put this guard inside the allocation it protects.
    backing: Option<crate::materializer::NativeAllocation>,
}

impl NativeSlotCustody {
    fn reserve<T>(
        &mut self,
        values: &mut Vec<T>,
        needed: usize,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        let pointer = values.as_ptr() as usize;
        let existing = (values.capacity() != 0).then(|| {
            self.guards.iter().position(|(candidate, _)| *candidate == pointer)
        }).flatten();
        let index = match existing {
            Some(index) => index,
            None => {
                let backing = self.backing.get_or_insert(work.claim_allocation(0, 0)?);
                let needed = self.guards.len() + 1;
                measured_slots(&mut self.guards, needed, work, backing)?;
                self.guards.push((pointer, work.claim_allocation(0, 0)?));
                self.guards.len() - 1
            }
        };
        let result = measured_slots(values, needed, work, &mut self.guards[index].1);
        // Even a late refusal may follow successful physical reallocation.
        self.guards[index].0 = values.as_ptr() as usize;
        result
    }
}

fn measured_order<K: Ord, V>(
    values: &mut Vec<(K, V)>,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<(), LocalCoreStateStoreError> {
    // Preserve the public BTreeMap's last-write-wins and sorted encoding.
    for index in 1..values.len() {
        let mut position = index;
        while position != 0 {
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
            if values[position - 1].0 <= values[position].0 { break; }
            work.charge(crate::WorkCounters {
                bytes_copied: (3 * std::mem::size_of::<(K, V)>()) as u64,
                ..Default::default()
            })?;
            values.swap(position - 1, position);
            position -= 1;
        }
    }
    let mut index = 1;
    while index < values.len() {
        work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
        if values[index - 1].0 == values[index].0 {
            work.charge(crate::WorkCounters {
                bytes_copied: ((values.len() - index) * std::mem::size_of::<(K, V)>()) as u64,
                ..Default::default()
            })?;
            values.remove(index - 1);
        } else {
            index += 1;
        }
    }
    Ok(())
}

impl RecordValue {
    fn write_bare<W: Write>(value: Option<&Self>, writer: W) -> Result<(), serde_json::Error> {
        match value {
            None => serde_json::to_writer(writer, &()),
            Some(Self::Context(value)) => serde_json::to_writer(writer, value),
            Some(Self::Children(value)) => serde_json::to_writer(writer, value),
            Some(Self::ChildCount(value)) => serde_json::to_writer(writer, value),
            Some(Self::ChildIndex(value)) => serde_json::to_writer(writer, value),
            Some(Self::Lineage(value)) => serde_json::to_writer(writer, value),
            Some(Self::LazyWorkspace(value)) => serde_json::to_writer(writer, value),
            Some(Self::FlatContext(value)) => serde_json::to_writer(writer, value),
            Some(Self::FlatChildren(value)) => serde_json::to_writer(writer, value),
        }
    }
}

impl RecordKey {
    fn measured_paths<'a>(
        self,
        root: &'a Namespace,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(RecordPaths<'a>, crate::materializer::NativeAllocation), LocalCoreStateStoreError> {
        let (family, id) = match self {
            Self::Context(id) => (WORKSPACE_CONTEXT_FAMILY, id.into_bytes()),
            Self::Children(id) => (CONTEXT_CHILDREN_FAMILY, id.into_bytes()),
            Self::ChildCount(id) => (CONTEXT_CHILD_COUNT_FAMILY, id.into_bytes()),
            Self::ChildIndex => (CONTEXT_CHILDREN_INDEX_FAMILY, [0; 16]),
            Self::Lineage(id) => (LINEAGE_FAMILY, id.into_bytes()),
            Self::LazyWorkspace(id) => (LAZY_WORKSPACE_FAMILY, id.into_bytes()),
        };
        root.measured_record(family, &id, work)
    }
}

/// In-memory encoding is admitted at every actual Vec growth, before serde writes.
struct MeasuredLogBuffer<'a> {
    bytes: Vec<u8>,
    allocation: crate::materializer::NativeAllocation,
    work: &'a crate::materializer::NativeMaterializationWork,
    failure: Option<LocalCoreStateStoreError>,
}

impl Write for MeasuredLogBuffer<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let result = (|| {
            let needed = self.bytes.len().checked_add(bytes.len())
                .ok_or(crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow))?;
            measured_slots(&mut self.bytes, needed, self.work, &mut self.allocation)?;
            self.work.charge(crate::WorkCounters {
                bytes_encoded: bytes.len() as u64,
                bytes_copied: bytes.len() as u64,
                ..Default::default()
            })?;
            self.bytes.extend_from_slice(bytes);
            Ok::<_, LocalCoreStateStoreError>(bytes.len())
        })();
        match result {
            Ok(count) => Ok(count),
            Err(error) => {
                self.failure = Some(error);
                Err(std::io::ErrorKind::Other.into())
            }
        }
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

impl Namespace {
    fn with_measured_log<T>(
        &self,
        work: &crate::materializer::NativeMaterializationWork,
        operation: impl FnOnce(&mut CoreLog)
            -> Result<T, LocalCoreStateStoreError>,
    ) -> Result<T, LocalCoreStateStoreError> {
        match &self.admission {
            Admission::Shared { .. } => {
                let (paths, _paths_allocation) = self.measured_record(CORE_LOG_FAMILY, &[0; 16], work)?;
                materialization_lock(paths, _paths_allocation, work, |_| {
                    let residents_allocation = work.claim_allocation(std::mem::size_of::<RwLock<Residents>>() as u64, 1)?;
                    let residents = Arc::default();
                    let mut log = CoreLog::open_measured(self, residents, work)?;
                    log.residents_control = Some(residents_allocation);
                    let result = operation(&mut log);
                    if !log.dirty.is_empty() && !log.broken {
                        // Acceptance belongs to the already flushed WAL, never to
                        // an optional postcommit checkpoint (same as with_log).
                        drop(log.checkpoint_measured(self, work));
                    }
                    result
                })
            }
            Admission::Owned(owner) => {
                let mut slot = owner.log();
                let mut log = match slot.take().filter(|log| !log.broken) {
                    Some(log) => log,
                    None => CoreLog::open_measured(self, Arc::clone(&owner.residents), work)?,
                };
                let result = if log.sequence >= OWNED_CHECKPOINT {
                    log.checkpoint_measured(self, work)
                } else {
                    Ok(())
                }.and_then(|()| operation(&mut log));
                *slot = Some(log);
                result
            }
        }
    }
}

fn clone_native_lazy_state(
    state: &LazyWorkspaceState,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<LazyWorkspaceState, LocalCoreStateStoreError> {
    work.charge(crate::WorkCounters::UNCHARGED)?;
    // This schema contains only inline identifiers, scalars and fixed metadata.
    Ok(state.clone())
}

impl LocalCoreStateStore {
    async fn load_lazy_logged(
        &self,
        id: WorkspaceId,
        work: crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<LazyWorkspaceState>, LocalCoreStateStoreError> {
        work.charge(crate::WorkCounters::default())?;
        if let Ownership::Owned(owner) = &self.ownership {
            let residents = owner.residents.read().unwrap_or_else(PoisonError::into_inner);
            for (key, value) in &residents.0 {
                work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
                if *key == RecordKey::LazyWorkspace(id) {
                    return value.as_ref().and_then(LazyWorkspaceState::from_value)
                        .map(|state| clone_native_lazy_state(state, &work)).transpose();
                }
            }
        }
        self.measured_transaction(work, move |root, work| {
            root.with_measured_log(work, |log| log.get_lazy_measured(root, id, work))
        }).await
    }

    async fn swap_lazy_logged(
        &self,
        id: WorkspaceId,
        expected: u64,
        replacement: LazyWorkspaceState,
        work: crate::materializer::NativeMaterializationWork,
    ) -> Result<bool, LocalCoreStateStoreError> {
        self.measured_transaction(work, move |root, work| {
            root.with_measured_log(work, |log| {
                let current = log.get_lazy_measured(root, id, work)?;
                if current.as_ref().map_or(0, |state| state.revision) != expected {
                    return Ok(false);
                }
                let prior_origin = current.as_ref().and_then(|state| state.fork_origin);
                if prior_origin.is_some() || replacement.fork_origin.is_some() {
                    work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
                    let Some(origin) = replacement.fork_origin else { return Ok(false); };
                    if replacement.schema_version != crate::lazy_workspace::LAZY_STATE_SCHEMA
                        || replacement.workspace_id != id
                        || replacement.parent_workspace_id != Some(origin.snapshot.workspace_id)
                        || origin.snapshot.workspace_id == id
                    {
                        return Ok(false);
                    }
                    if let Some(prior) = prior_origin {
                        let current = current.as_ref().expect("retained origin has a binding");
                        if current.schema_version != replacement.schema_version
                            || current.workspace_id != id
                            || current.parent_workspace_id != replacement.parent_workspace_id
                            || prior.operation_id != origin.operation_id
                            || prior.config != origin.config
                            || prior.snapshot != origin.snapshot
                            || (prior.phase == crate::LazyForkPhase::Committed
                                && origin.phase != crate::LazyForkPhase::Committed)
                        {
                            return Ok(false);
                        }
                    } else if current.is_some() || expected != 0 || replacement.revision != 1
                        || origin.phase != crate::LazyForkPhase::Prepared
                    {
                        return Ok(false);
                    }
                    if prior_origin.is_none_or(|prior| prior.phase == crate::LazyForkPhase::Prepared)
                        && (replacement.source != origin.snapshot.source
                            || replacement.overlay != origin.snapshot.overlay
                            || replacement.shadows != origin.snapshot.shadows
                            || replacement.pending_remove.is_some())
                    {
                        return Ok(false);
                    }
                    if prior_origin.is_none() {
                        // Authenticate only the first freeze while companion collection
                        // and source binding updates share this same log transaction.
                        let Some(parent) = log.get_lazy_measured(
                            root, origin.snapshot.workspace_id, work,
                        )? else { return Ok(false); };
                        if parent.schema_version != replacement.schema_version
                            || parent.workspace_id != origin.snapshot.workspace_id
                            || parent.source != origin.snapshot.source
                            || parent.overlay != origin.snapshot.overlay
                            || parent.shadows != origin.snapshot.shadows
                            || parent.fork_origin.is_some_and(|origin|
                                origin.phase == crate::LazyForkPhase::Prepared)
                        {
                            return Ok(false);
                        }
                    }
                }
                log.commit_lazy_measured(root, id, replacement, work)?;
                Ok(true)
            })
        }).await
    }
}

impl CoreLog {
    fn adopt_native(&mut self, work: &crate::materializer::NativeMaterializationWork) -> Result<(), LocalCoreStateStoreError> {
        self.residents().adopt_native(work)?;
        let needed = self.dirty.len();
        self.slots.reserve(&mut self.dirty, needed, work)
    }

    pub(super) fn get_native_value(
        &mut self,
        root: &Namespace,
        key: RecordKey,
        work: &crate::materializer::NativeMaterializationWork,
        cxl: &crate::CancellationToken,
    ) -> Result<Option<NativeOwnedCoreState<RecordValue>>, LocalCoreStateStoreError> {
        if cxl.is_cancelled() {
            return Err(crate::materializer::NativeTreeMaterializationError::Cancelled.into());
        }
        self.adopt_native(work)?;
        {
            let residents = self.residents();
            for (candidate, value) in &residents.0 {
                work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
                if *candidate == key {
                    return value.as_ref().map(|value| native_records::clone_native_record_value(value, work, cxl)).transpose();
                }
            }
        }
        let (paths, _paths_allocation) = key.measured_paths(root, work)?;
        let value = native_records::read_native_record(key, &paths.current, work, cxl)?;
        let output = value.as_ref().map(|value| native_records::clone_native_record_value(value.as_state(), work, cxl)).transpose()?;
        let mut residents = self.residents();
        let needed = residents.0.len() + 1;
        let Residents(values, _, slots) = &mut *residents;
        slots.reserve(values, needed, work)?;
        let payload = residents.payload_slot(key, work)?;
        let (value, allocation) = match value {
            Some(value) => { let (value, allocation) = value.into_parts(); (Some(value), allocation) },
            None => (None, work.claim_allocation(0, 0)?),
        };
        residents.0.push((key, value));
        residents.1[payload].1 = allocation;
        Ok(output)
    }
    fn open_measured(
        root: &Namespace,
        residents: Arc<RwLock<Residents>>,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Self, LocalCoreStateStoreError> {
        residents.write().unwrap_or_else(PoisonError::into_inner).adopt_native(work)?;
        let (paths, mut paths_allocation) = root.measured_record(CORE_LOG_FAMILY, &[0; 16], work)?;
        crate::native_host::create_directories_materialization_measured(&paths.directory, work)?;
        let mut file = match materialization_open_options(
            &paths.current, &crate::native_host::NativeOpenOptions::ReadWriteCreateNew, work,
        ) {
            Ok(file) => { materialization_sync(&paths.directory, work)?; file }
            Err(crate::materializer::NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    materialization_open_options(
                        &paths.current, &crate::native_host::NativeOpenOptions::ReadWrite, work,
                    )?
                }
            Err(error) => return Err(error.into()),
        };
        let length = crate::native_host::stat_file_with_work(&file, &mut &*work)?.len();
        let size = usize::try_from(length)
            .map_err(|_| crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow))?;
        let mut bytes_allocation = work.claim_allocation(0, 0)?;
        let mut bytes = Vec::new();
        measured_slots(&mut bytes, size, work, &mut bytes_allocation)?;
        work.charge(crate::WorkCounters { bytes_copied: length, ..Default::default() })?;
        bytes.resize(size, 0);
        let mut position = 0;
        while position < bytes.len() {
            let end = bytes.len().min(position.saturating_add(64 * 1024));
            let count = crate::native_host::read_builtin_file(&file, &mut bytes[position..end], work)?;
            if count == 0 { return Err(std::io::ErrorKind::UnexpectedEof.into()); }
            position += count;
        }
        {
            let mut previous = residents.write().unwrap_or_else(PoisonError::into_inner);
            for (_, value) in &previous.0 {
                let children = match value {
                    Some(RecordValue::Context(context)) => context.roots.len(),
                    Some(RecordValue::Children(children)) => children.0.len(),
                    Some(RecordValue::FlatContext(context)) => context.roots.0.len(),
                    Some(RecordValue::FlatChildren(children)) => children.len(),
                    _ => 0,
                };
                work.charge(crate::WorkCounters {
                    items_examined: (children as u64).saturating_add(1),
                    ..Default::default()
                })?;
            }
            previous.clear();
        }
        // A failed finite replay must never publish a partially applied prefix
        // into the owner's fast-path cache.
        let published = matches!(&root.admission, Admission::Owned(_)).then(|| Arc::clone(&residents));
        let _provisional_allocation = if published.is_some() {
            Some(work.claim(std::mem::size_of::<RwLock<Residents>>() as u64)?)
        } else { None };
        let replay_residents = if published.is_some() { Arc::default() } else { residents };
        let mut log = Self {
            file, length, flushed: 0, epoch: Uuid::now_v7(), sequence: 0,
            residents: replay_residents, dirty: Vec::new(), broken: false,
            slots: NativeSlotCustody::default(),
            residents_control: None,
        };
        let mut epoch = None;
        let mut sequence = 0;
        let mut line_start = 0;
        while line_start < bytes.len() {
            let mut line_end = line_start;
            while line_end < bytes.len() {
                work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
                let byte = bytes[line_end];
                line_end += 1;
                if byte == b'\n' { break; }
            }
            let line = &bytes[line_start..line_end];
            line_start = line_end;
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
            let Some(line) = line.strip_suffix(b"\n") else { break; };
            let Some((checksum, frame)) = line.split_first_chunk::<64>() else { break; };
            let Some(frame) = frame.strip_prefix(b" ") else { break; };
            work.charge(crate::WorkCounters { bytes_hashed: frame.len() as u64, ..Default::default() })?;
            if blake3::hash(frame).to_hex().as_bytes() != checksum { break; }
            let (frame, mut frame_allocation) = match
                measured_decode_slice::<LogFrame<Vec<(RecordKey, Option<FlatRecordValue>)>>>(frame, work)
            {
                Ok(frame) => frame,
                Err(error @ LocalCoreStateStoreError::MaterializationWork(_)) => return Err(error),
                Err(_) => return Err(native_corrupt_log(paths, paths_allocation)?),
            };
            // The decoded frame's physical allocations precede their guard's drop.
            let frame = frame;
            for (key, value) in &frame.change {
                work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
                if value.as_ref().is_some_and(|value| !value.matches(*key)) {
                    return Err(native_corrupt_log(paths, paths_allocation)?);
                }
            }
            if *epoch.get_or_insert(frame.epoch) != frame.epoch || frame.sequence != sequence { break; }
            for (key, mut value) in frame.change {
                let prior_owned = value.as_ref().map(|value| value.owned_bytes(work)).transpose()?.unwrap_or(0);
                if let Some(value) = &mut value {
                    match value {
                        FlatRecordValue::Context(context) => measured_order(&mut context.roots.0, work)?,
                        FlatRecordValue::Children(children) => {
                            // The original set normalizes duplicate and out-of-order children.
                            for index in 1..children.len() {
                                let mut position = index;
                                while position != 0 {
                                    work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
                                    if children[position - 1] <= children[position] { break; }
                                    work.charge(crate::WorkCounters {
                                        bytes_copied: (3 * std::mem::size_of::<WorkspaceContextId>()) as u64,
                                        ..Default::default()
                                    })?;
                                    children.swap(position - 1, position);
                                    position -= 1;
                                }
                            }
                            let mut index = 1;
                            while index < children.len() {
                                work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
                                if children[index - 1] == children[index] {
                                    work.charge(crate::WorkCounters {
                                        bytes_copied: ((children.len() - index - 1) * std::mem::size_of::<WorkspaceContextId>()) as u64,
                                        ..Default::default()
                                    })?;
                                    children.remove(index);
                                } else {
                                    index += 1;
                                }
                            }
                        }
                        _ => {}
                    }
                }
                let normalized_owned = value.as_ref().map(|value| value.owned_bytes(work)).transpose()?.unwrap_or(0);
                frame_allocation.release(prior_owned - normalized_owned)?;
                let value = value.map(FlatRecordValue::into_resident);
                let (index, dirty) = log.admit_lazy_slots(key, work)?;
                let mut residents = log.residents();
                let payload_slot = residents.payload_slot(key, work)?;
                let payload = frame_allocation.split_off(normalized_owned)?;
                let previous_payload = std::mem::replace(&mut residents.1[payload_slot].1, payload);
                if let Some(index) = index {
                    residents.0[index].1 = value;
                } else {
                    residents.0.push((key, value));
                }
                // The replaced payload has now physically dropped.
                drop(previous_payload);
                drop(residents);
                if dirty { log.dirty.push(key); }
            }
            drop(frame_allocation);
            sequence += 1;
        }
        if length != 0 { log.checkpoint_measured(root, work)?; }
        if let Some(published) = published {
            {
                let mut destination = published.write().unwrap_or_else(PoisonError::into_inner);
                let mut replayed = log.residents();
                *destination = std::mem::take(&mut *replayed);
            }
            log.residents = published;
        }
        Ok(log)
    }

    fn admit_lazy_slots(
        &mut self,
        key: RecordKey,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(Option<usize>, bool), LocalCoreStateStoreError> {
        let mut residents = self.residents();
        let mut found = None;
        for (index, (candidate, _)) in residents.0.iter().enumerate() {
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
            if *candidate == key { found = Some(index); break; }
        }
        if found.is_none() {
            let needed = residents.0.len() + 1;
            let Residents(values, _, slots) = &mut *residents;
            slots.reserve(values, needed, work)?;
        }
        drop(residents);
        let mut dirty = false;
        for candidate in &self.dirty {
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
            if *candidate == key { dirty = true; break; }
        }
        if !dirty {
            let needed = self.dirty.len() + 1;
            self.slots.reserve(&mut self.dirty, needed, work)?;
        }
        Ok((found, !dirty))
    }

    fn get_lazy_measured(
        &mut self,
        root: &Namespace,
        id: WorkspaceId,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<LazyWorkspaceState>, LocalCoreStateStoreError> {
        self.get_native_value(root, RecordKey::LazyWorkspace(id), work, work.cancellation())?
            .map(|owned| {
                let (value, allocation) = owned.into_parts();
                let _allocation = allocation;
                match value {
                    RecordValue::LazyWorkspace(value) => Ok(value),
                    _ => Err(LocalCoreStateStoreError::Integrity),
                }
            }).transpose()
    }

    fn checkpoint_measured(
        &mut self,
        root: &Namespace,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        self.adopt_native(work)?;
        if self.flushed < self.length {
            work.charge(crate::WorkCounters { durability_operations: 1, ..Default::default() })?;
            self.file.sync_data()?;
            self.flushed = self.length;
            crash_point("frames flushed", self.flushed);
        }
        for key in &self.dirty {
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
            let (paths, _paths_allocation) = key.measured_paths(root, work)?;
            crate::native_host::create_directories_materialization_measured(&paths.directory, work)?;
            let file = materialization_open_options(
                &paths.current, &crate::native_host::NativeOpenOptions::ReadWriteCreate, work,
            )?;
            work.charge(crate::WorkCounters { backend_write_operations: 1, ..Default::default() })?;
            file.set_len(0)?;
            let mut writer = MaterializationWriter { file, work, encoded: true };
            let residents = self.residents();
            let mut value = None;
            for (candidate, resident) in &residents.0 {
                work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
                if candidate == key {
                    value = resident.as_ref();
                    break;
                }
            }
            let result = RecordValue::write_bare(value, &mut writer);
            materialization_writer_result(result, &mut writer)?;
            crash_point("record written", self.flushed);
            work.charge(crate::WorkCounters { durability_operations: 1, ..Default::default() })?;
            writer.file.sync_all()?;
            crash_point("record flushed", self.flushed);
            materialization_sync(&paths.directory, work)?;
        }
        let mut reset = work.reserve_work(crate::WorkCounters {
            backend_write_operations: 1, backend_read_operations: 1, ..Default::default()
        })?;
        let truncated = self.file.set_len(0);
        let truncate_recorded = reset.record_completed(crate::WorkCounters {
            backend_write_operations: 1, ..Default::default()
        });
        truncated?;
        let sought = self.file.seek(SeekFrom::Start(0));
        let seek_recorded = reset.record_completed(crate::WorkCounters {
            backend_read_operations: 1, ..Default::default()
        });
        if let Err(error) = sought {
            self.broken = true;
            return Err(error.into());
        }
        self.length = 0;
        self.flushed = 0;
        crash_point("log truncated", self.flushed);
        self.epoch = Uuid::now_v7();
        self.sequence = 0;
        self.dirty.clear();
        truncate_recorded?;
        seek_recorded?;
        Ok(())
    }

    fn commit_lazy_measured(
        &mut self,
        root: &Namespace,
        id: WorkspaceId,
        replacement: LazyWorkspaceState,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        let mut change = NativeChange::new(work)?;
        change.put_scalar(RecordKey::LazyWorkspace(id), Some(RecordValue::LazyWorkspace(replacement)), work)?;
        self.commit_native_change(root, change, work, work.cancellation())
    }

    pub(super) fn commit_native_change(
        &mut self,
        root: &Namespace,
        mut change: NativeChange,
        work: &crate::materializer::NativeMaterializationWork,
        cxl: &crate::CancellationToken,
    ) -> Result<(), LocalCoreStateStoreError> {
        if cxl.is_cancelled() {
            return Err(crate::materializer::NativeTreeMaterializationError::Cancelled.into());
        }
        self.adopt_native(work)?;
        let next_sequence = self.sequence.checked_add(1)
            .ok_or(crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow))?;
        let mut install_allocation = work.claim_allocation(0, 0)?;
        let mut installs: Vec<NativeInstall> = Vec::new();
        measured_slots(&mut installs, change.change.0.len(), work, &mut install_allocation)?;
        {
            let mut residents = self.residents();
            let needed = residents.0.len().checked_add(change.change.0.len())
                .ok_or(crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow))?;
            let Residents(values, _, slots) = &mut *residents;
            slots.reserve(values, needed, work)?;
            let mut next = residents.0.len();
            for (key, _) in &change.change.0 {
                let mut resident = None;
                for (index, (candidate, _)) in residents.0.iter().enumerate() {
                    work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
                    if candidate == key { resident = Some(index); break; }
                }
                let mut prior = None;
                for install in &installs {
                    work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
                    if install.key == *key { prior = Some(install.resident); break; }
                }
                let resident = resident.or(prior).unwrap_or_else(|| { let index = next; next += 1; index });
                let mut dirty = prior.is_none();
                for candidate in &self.dirty {
                    work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
                    if candidate == key { dirty = false; break; }
                }
                let payload = residents.payload_slot(*key, work)?;
                installs.push(NativeInstall { key: *key, resident, payload, dirty });
            }
        }
        let needed = self.dirty.len().checked_add(installs.iter().filter(|install| install.dirty).count())
            .ok_or(crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow))?;
        self.slots.reserve(&mut self.dirty, needed, work)?;
        let mut buffer = MeasuredLogBuffer {
            bytes: Vec::new(), allocation: work.claim(0)?, work, failure: None,
        };
        measured_slots(&mut buffer.bytes, 65, work, &mut buffer.allocation)?;
        work.charge(crate::WorkCounters { bytes_copied: 65, ..Default::default() })?;
        buffer.bytes.extend_from_slice(&[b' '; 65]);
        let encoded = serde_json::to_writer(&mut buffer, &LogFrame {
            epoch: self.epoch, sequence: self.sequence, change: &change.change,
        });
        if let Some(error) = buffer.failure.take() { return Err(error); }
        encoded?;
        work.charge(crate::WorkCounters { bytes_hashed: (buffer.bytes.len() - 65) as u64, ..Default::default() })?;
        let checksum = blake3::hash(&buffer.bytes[65..]).to_hex();
        let needed = buffer.bytes.len() + 1;
        measured_slots(&mut buffer.bytes, needed, work, &mut buffer.allocation)?;
        work.charge(crate::WorkCounters { bytes_copied: 65, ..Default::default() })?;
        buffer.bytes[..64].copy_from_slice(checksum.as_bytes());
        buffer.bytes.push(b'\n');
        let line = &buffer.bytes;
        let deferred = root.durability == Durability::Deferred;
        let new_length = self.length.checked_add(line.len() as u64)
            .ok_or(crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow))?;
        // Repair credit cannot be consumed by append, quota, or cancellation.
        let mut repair = work.reserve_work(crate::WorkCounters {
            backend_write_operations: 1, backend_read_operations: 1,
            durability_operations: 1, ..Default::default()
        })?;
        let mut flush = work.reserve_work(crate::WorkCounters {
            durability_operations: u64::from(!deferred), ..Default::default()
        })?;
        crash_point("before frame", self.flushed);
        let append = (|| {
            let mut position = 0;
            while position < line.len() {
                let mut write = work.reserve_work(crate::WorkCounters {
                    backend_write_operations: 1,
                    object_bytes_written: (line.len() - position) as u64,
                    ..Default::default()
                })?;
                let result = self.file.write(&line[position..]);
                let count = result.as_ref().copied().unwrap_or(0);
                let recorded = write.record_completed(crate::WorkCounters {
                    backend_write_operations: 1, object_bytes_written: count as u64, ..Default::default()
                });
                let count = result?;
                recorded?;
                if count == 0 { return Err(std::io::ErrorKind::WriteZero.into()); }
                position += count;
            }
            crash_point("frame written", self.flushed);
            if !deferred {
                let synced = self.file.sync_data();
                let recorded = flush.record_completed(crate::WorkCounters {
                    durability_operations: 1, ..Default::default()
                });
                synced?;
                // Physical durability wins over late accounting refusal.
                drop(recorded);
            }
            Ok::<_, LocalCoreStateStoreError>(())
        })();
        if let Err(error) = append {
            let truncated = self.file.set_len(self.length);
            drop(repair.record_completed(crate::WorkCounters { backend_write_operations: 1, ..Default::default() }));
            let repaired = truncated.and_then(|()| {
                let sought = self.file.seek(SeekFrom::Start(self.length));
                drop(repair.record_completed(crate::WorkCounters { backend_read_operations: 1, ..Default::default() }));
                sought
            }).and_then(|_| {
                let synced = self.file.sync_data();
                drop(repair.record_completed(crate::WorkCounters { durability_operations: 1, ..Default::default() }));
                synced
            });
            self.broken = repaired.is_err();
            return Err(error);
        }
        self.length = new_length;
        if !deferred {
            self.flushed = self.length;
            crash_point("frame flushed", self.flushed);
        }
        self.sequence = next_sequence;
        // All typed slots and payload guards exist before WAL acceptance.
        let mut payloads = change.payloads.drain(..);
        for ((key, value), install) in change.change.0.drain(..).zip(installs) {
            let payload = payloads.next().expect("one admitted payload per after-image");
            let mut residents = self.residents();
            let previous = std::mem::replace(&mut residents.1[install.payload].1, payload);
            if install.resident < residents.0.len() {
                residents.0[install.resident].1 = value;
            } else {
                residents.0.push((key, value));
            }
            drop(previous);
            drop(residents);
            if install.dirty { self.dirty.push(key); }
        }
        Ok(())
    }
}

fn native_corrupt_log(
    paths: RecordPaths<'_>,
    mut allocation: crate::materializer::NativeAllocation,
) -> Result<LocalCoreStateStoreError, LocalCoreStateStoreError> {
    let current = paths.current;
    drop((paths.directory, paths.previous, paths.temporary, paths.lock));
    allocation.substitute(current.capacity() as u64)?;
    Ok(LocalCoreStateStoreError::LazyWork(crate::LazyWorkspaceError::NativeRecordCorrupt(
        current, Some(allocation),
    )))
}

/// Decodes the log's valid prefix: frames of the first frame's epoch in
/// consecutive sequence, each intact.
///
/// A torn frame ends the prefix, but an intact frame that does not decode
/// fails: its checksum proves it was written whole, so it may be a committed
/// change, and treating it as torn would let the next checkpoint discard it.
fn replayable_changes(bytes: &[u8]) -> Result<Vec<Change>, serde_json::Error> {
    let mut changes = Vec::new();
    let mut epoch = None;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        let Some(line) = line.strip_suffix(b"\n") else {
            break;
        };
        let Some((checksum, frame)) = line.split_first_chunk::<64>() else {
            break;
        };
        let Some(frame) = frame.strip_prefix(b" ") else {
            break;
        };
        if blake3::hash(frame).to_hex().as_bytes() != checksum {
            break;
        }
        let frame = serde_json::from_slice::<LogFrame<Change>>(frame)?;
        if *epoch.get_or_insert(frame.epoch) != frame.epoch
            || frame.sequence != changes.len() as u64
        {
            break;
        }
        changes.push(frame.change);
    }
    Ok(changes)
}

/// Aborts the process at the configured crash point of a crash-atomicity test,
/// reporting the point and how many log bytes were durable.
#[cfg(test)]
fn crash_point(label: &str, flushed: u64) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static REACHED: AtomicU64 = AtomicU64::new(0);
    let Some(target) = std::env::var_os("ACYCLIC_CORE_STATE_CRASH_POINT") else {
        return;
    };
    let reached = REACHED.fetch_add(1, Ordering::SeqCst) + 1;
    if target.to_str().and_then(|target| target.parse().ok()) == Some(reached) {
        if let Some(report) = std::env::var_os("ACYCLIC_CORE_STATE_CRASH_REPORT") {
            let _ = std::fs::write(report, format!("{label}\n{flushed}"));
        }
        std::process::abort();
    }
}

#[cfg(not(test))]
fn crash_point(_label: &str, _flushed: u64) {}

fn core_log_paths(root: &Namespace) -> RecordPaths<'_> {
    root.record(CORE_LOG_FAMILY, &[0; 16])
}

fn context_children_index_ready(
    root: &Namespace,
    log: &mut CoreLog,
) -> Result<bool, LocalCoreStateStoreError> {
    Ok(log.get::<ContextChildIndex>(root, ())?.is_some())
}

fn plan_local_context_subtree_discard(
    root_path: &Namespace,
    log: &mut CoreLog,
    root: Option<&WorkspaceContext>,
    parent: WorkspaceContextId,
    child: WorkspaceContextId,
    maximum: u32,
) -> Result<Result<Vec<WorkspaceContextId>, WorkspaceContextDiscardOutcome>, LocalCoreStateStoreError>
{
    let mut selected = match plan_context_subtree_discard(
        root,
        &WorkspaceContextChildren::new(),
        parent,
        child,
        maximum,
    ) {
        Ok(selected) => selected,
        Err(outcome) => return Ok(Err(outcome)),
    };
    let maximum = maximum as usize;
    let mut pending = vec![child];
    let mut seen = BTreeSet::new();
    while let Some(context_id) = pending.pop() {
        if !seen.insert(context_id) || seen.len() > maximum {
            return Ok(Err(WorkspaceContextDiscardOutcome::TraversalLimit));
        }
        let count = log
            .get::<ContextChildCount>(root_path, context_id)?
            .map_or(0, |count| count.0);
        let remaining = maximum.saturating_sub(seen.len() + pending.len());
        if count > remaining as u64 {
            return Ok(Err(WorkspaceContextDiscardOutcome::TraversalLimit));
        }
        // UUIDs serialize as quoted 36-byte strings. This deliberately leaves
        // headroom for JSON delimiters while keeping allocation proportional
        // to the already-checked child count.
        let maximum_bucket_bytes = count
            .checked_mul(64)
            .and_then(|bytes| bytes.checked_add(2))
            .ok_or_else(|| {
                LocalCoreStateStoreError::ContextTransaction(
                    "workspace-context child bucket bound overflowed".to_owned(),
                )
            })?;
        let descendants = log
            .get_bounded::<ContextChildren>(root_path, context_id, Some(maximum_bucket_bytes))?
            .unwrap_or_default()
            .0;
        if usize::try_from(count) != Ok(descendants.len()) {
            return Ok(Err(WorkspaceContextDiscardOutcome::IncompatibleState));
        }
        pending.extend(descendants);
    }
    selected.clear();
    selected.extend(seen);
    Ok(Ok(selected))
}

/// Every context present in memory or on disk with a matching stored identity.
fn live_context_ids(
    root: &Namespace,
    log: &mut CoreLog,
) -> Result<Vec<WorkspaceContextId>, LocalCoreStateStoreError> {
    let entries = match std::fs::read_dir(root.family(WORKSPACE_CONTEXT_FAMILY)) {
        Ok(entries) => Some(entries),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let mut ids = BTreeSet::new();
    for entry in entries.into_iter().flatten() {
        let name = entry?.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(".json") else {
            continue;
        };
        let Ok(bytes) = hex::decode(stem) else {
            continue;
        };
        let Ok(bytes) = <[u8; 16]>::try_from(bytes) else {
            continue;
        };
        ids.insert(WorkspaceContextId::from_bytes(bytes));
    }
    ids.extend(log.resident_keys(|key| match key {
        RecordKey::Context(context_id) => Some(context_id),
        _ => None,
    }));
    let mut live = Vec::with_capacity(ids.len());
    for context_id in ids {
        if let Some(context) = log.get::<WorkspaceContext>(root, context_id)? {
            if context.context_id != context_id {
                return Err(LocalCoreStateStoreError::ContextTransaction(
                    "workspace context identity does not match its stored key".to_owned(),
                ));
            }
            live.push(context_id);
        }
    }
    live.sort_unstable_by_key(|id| id.into_bytes());
    Ok(live)
}

fn load_all_contexts(
    root: &Namespace,
    log: &mut CoreLog,
) -> Result<Vec<WorkspaceContext>, LocalCoreStateStoreError> {
    live_context_ids(root, log)?
        .into_iter()
        .map(|id| {
            log.get::<WorkspaceContext>(root, id)?.ok_or_else(|| {
                LocalCoreStateStoreError::ContextTransaction(
                    "workspace context disappeared during locked discovery".to_owned(),
                )
            })
        })
        .collect()
}

impl GitCompatStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(&self, workspace_id: WorkspaceId) -> Result<Option<GitCompatState>, Self::Error> {
        self.load_record(GIT_COMPAT_NAMESPACE, workspace_id.into_bytes())
            .await
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: GitCompatState,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_record(
            GIT_COMPAT_NAMESPACE,
            workspace_id.into_bytes(),
            expected_revision,
            replacement,
        )
        .await
    }

    async fn compare_and_delete(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
    ) -> Result<bool, Self::Error> {
        self.compare_and_delete_record::<GitCompatState>(
            GIT_COMPAT_NAMESPACE,
            workspace_id.into_bytes(),
            expected_revision,
        )
        .await
    }
}

impl MaterializationJournalStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    #[cfg(not(target_arch = "wasm32"))]
    type ActionCompletion<'a> = LocalMaterializationActionCompletion where Self: 'a;

    #[cfg(not(target_arch = "wasm32"))]
    async fn prepare_action<'a>(&'a self, mut journal: crate::materializer::OwnedMaterializationJournal) -> Result<Self::ActionCompletion<'a>, Self::Error> {
        let work = match &journal.custody {
            Some(allocation) => allocation.work().clone(),
            None => crate::materializer::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
                .map_err(|(cause, _work)| LocalCoreStateStoreError::MaterializationWork(cause))?,
        };
        if journal.custody.is_none() {
            journal.custody = Some(work.retain_owned(crate::materializer::NativeMaterializationWork::journal_control_bytes(&journal.journal)?)?);
        }
        self.materialization_with_work(work).prepare_action(journal).await
    }

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<crate::materializer::OwnedMaterializationJournal>, Self::Error> {
        self.load_record(MATERIALIZATION_FAMILY, operation_id.into_bytes())
            .await
            .map(|journal| journal.map(|journal| crate::materializer::OwnedMaterializationJournal {
                journal,
                #[cfg(not(target_arch = "wasm32"))]
                custody: None,
            }))
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: crate::materializer::OwnedMaterializationJournal,
    ) -> Result<(bool, crate::materializer::OwnedMaterializationJournal), Self::Error> {
        self.transaction(move |namespace| {
            let stored = compare_and_swap(
                &namespace.record(MATERIALIZATION_FAMILY, &operation_id.into_bytes()),
                expected_revision,
                &replacement.journal,
            )?;
            Ok((stored, replacement))
        }).await
    }
}

/// The existing materialization journal family with one shared execution meter.
pub struct MeasuredMaterializationStore<'a> {
    store: &'a LocalCoreStateStore,
    work: crate::materializer::NativeMaterializationWork,
}

/// The original materialization record's accepted, lock-held completion.
/// Fields are private: work custody alone cannot create this provider proof.
pub struct LocalMaterializationActionCompletion {
    prepared: Option<LocalPreparedMaterializationAction>,
    _body_custody: crate::materializer::NativeAllocation,
}

struct LocalPreparedMaterializationAction {
    journal: crate::materializer::OwnedMaterializationJournal,
    encoding: PreparedMaterializationEncoding,
    host: crate::native_host::PreparedMaterializationRecord,
    record_lock: MaterializationRecordLock,
    paths: RecordPaths<'static>,
    namespace: Namespace,
    work: crate::materializer::NativeMaterializationWork,
    paths_custody: crate::materializer::NativeAllocation,
    namespace_custody: crate::materializer::NativeAllocation,
}

impl crate::materializer::MaterializationActionCompletion for LocalMaterializationActionCompletion {
    type Error = LocalCoreStateStoreError;

    fn complete(self) -> Self { self }
}

impl Future for LocalMaterializationActionCompletion {
    type Output = Result<crate::materializer::OwnedMaterializationJournal, LocalCoreStateStoreError>;

    fn poll(self: std::pin::Pin<&mut Self>, _context: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        let Some(prepared) = this.prepared.take() else {
            return std::task::Poll::Ready(Err(crate::materializer::NativeTreeMaterializationError::InvalidLayout.into()));
        };
        std::task::Poll::Ready(prepared.complete())
    }
}

impl LocalPreparedMaterializationAction {
    fn complete(mut self) -> Result<crate::materializer::OwnedMaterializationJournal, LocalCoreStateStoreError> {
        self.journal.journal.work = self.work.journal_work()?;
        let length = self.encoding.encode(&self.journal.journal)?;
        self.host.complete(&self.encoding.bytes[..length])?;
        // A committed current image remains authoritative if unlock reports a
        // lost acknowledgement. Keep its same native work and record identity.
        let cleanup = self.record_lock.1.take()
            .ok_or(crate::materializer::NativeTreeMaterializationError::MissingCustody)?;
        let (unlocked, recorded) = cleanup.attempt_recorded(|| FileExt::unlock(&self.record_lock.0));
        recorded?;
        unlocked?;
        Ok(self.journal)
    }
}

struct PreparedMaterializationEncoding {
    bytes: Vec<u8>,
    credit: crate::materializer::NativeWorkReservation,
    _allocation: crate::materializer::NativeAllocation,
}

struct MaterializationEncodedSize<'a> {
    work: &'a crate::materializer::NativeMaterializationWork,
    length: usize,
    failure: Option<crate::materializer::NativeTreeMaterializationError>,
}

impl Write for MaterializationEncodedSize<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let result = (|| {
            self.work.charge(crate::WorkCounters { bytes_encoded: bytes.len() as u64, ..crate::WorkCounters::UNCHARGED })?;
            self.length = self.length.checked_add(bytes.len()).ok_or(crate::WorkError::Overflow)?;
            Ok::<_, crate::materializer::NativeTreeMaterializationError>(bytes.len())
        })();
        match result {
            Ok(length) => Ok(length),
            Err(error) => { self.failure = Some(error); Err(std::io::ErrorKind::Other.into()) }
        }
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

struct MaterializationFixedEncoding<'a> {
    bytes: &'a mut [u8],
    position: usize,
    credit: &'a mut crate::materializer::NativeWorkReservation,
    failure: Option<crate::materializer::NativeTreeMaterializationError>,
}

impl Write for MaterializationFixedEncoding<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let Some(end) = self.position.checked_add(bytes.len()).filter(|end| *end <= self.bytes.len()) else {
            self.failure = Some(crate::materializer::NativeTreeMaterializationError::InvalidLayout);
            return Err(std::io::ErrorKind::Other.into());
        };
        // The same buffer and accepted encoding quota exist before the action.
        self.bytes[self.position..end].copy_from_slice(bytes);
        self.position = end;
        if let Err(error) = self.credit.record_completed(crate::WorkCounters {
            bytes_encoded: bytes.len() as u64, bytes_copied: bytes.len() as u64,
            ..crate::WorkCounters::UNCHARGED
        }) {
            self.failure = Some(error);
            return Err(std::io::ErrorKind::Other.into());
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

impl PreparedMaterializationEncoding {
    fn prepare(
        journal: &mut MaterializationJournal,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Self, LocalCoreStateStoreError> {
        // Only the inline work scalars can change in this accepted window.
        // Serialize the actual immutable journal with maximal decimal widths
        // using its existing serializer, rather than a guessed schema estimate.
        let original_work = journal.work;
        journal.work = crate::WorkBudget::UNBOUNDED;
        let mut count = MaterializationEncodedSize { work, length: 0, failure: None };
        let encoded = serde_json::to_writer(&mut count, journal);
        journal.work = original_work;
        if let Some(error) = count.failure { return Err(error.into()); }
        encoded?;
        let mut allocation = work.claim(count.length as u64)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(count.length)
            .map_err(crate::materializer::NativeTreeMaterializationError::AllocationFailed)?;
        bytes.resize(count.length, 0);
        allocation.reconcile_capacity(bytes.capacity() as u64)?;
        let credit = work.reserve_work(crate::WorkCounters {
            bytes_encoded: count.length as u64, bytes_copied: count.length as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        Ok(Self { bytes, credit, _allocation: allocation })
    }

    fn encode(&mut self, journal: &MaterializationJournal) -> Result<usize, LocalCoreStateStoreError> {
        let mut writer = MaterializationFixedEncoding {
            bytes: &mut self.bytes, position: 0, credit: &mut self.credit, failure: None,
        };
        let encoded = serde_json::to_writer(&mut writer, journal);
        if let Some(error) = writer.failure { return Err(error.into()); }
        encoded?;
        Ok(writer.position)
    }
}

impl LocalCoreStateStore {
    /// Binds the existing journal family to the native execution accounting boundary.
    pub fn materialization_with_work(
        &self,
        work: crate::materializer::NativeMaterializationWork,
    ) -> MeasuredMaterializationStore<'_> {
        MeasuredMaterializationStore { store: self, work }
    }

    async fn measured_transaction<T: Send + 'static>(
        &self,
        work: crate::materializer::NativeMaterializationWork,
        operation: impl FnOnce(&Namespace, &crate::materializer::NativeMaterializationWork) -> Result<T, LocalCoreStateStoreError> + Send + 'static,
    ) -> Result<T, LocalCoreStateStoreError> {
        self.measured_transaction_owned(work, move |namespace, custody, work| {
            let _custody = custody;
            let namespace = namespace;
            operation(&namespace, work)
        }).await
    }

    async fn measured_transaction_owned<T: Send + 'static>(
        &self,
        work: crate::materializer::NativeMaterializationWork,
        operation: impl FnOnce(Namespace, crate::materializer::NativeAllocation, &crate::materializer::NativeMaterializationWork) -> Result<T, LocalCoreStateStoreError> + Send + 'static,
    ) -> Result<T, LocalCoreStateStoreError> {
        let bytes = self.root.as_os_str().len() as u64;
        let copy = crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::default() };
        work.admit(copy)?;
        let store_allocation = work.claim(bytes)?;
        let store = self.clone();
        work.spent(copy)?;
        run_local_transaction(move || {
            let _store_allocation = store_allocation;
            let admission = match &store.ownership {
                Ownership::Shared => {
                    crate::native_host::create_directories_materialization_measured(&store.root, &work)?;
                    let mut path_allocation = work.claim(0)?;
                    let path = measured_record_path(&store.root, OWNER_LOCK, &work, &mut path_allocation)?;
                    let lock = materialization_open_options(&path, &crate::native_host::NativeOpenOptions::ReadWriteCreate, &work)?;
                    let cleanup = work.reserve_cleanup_write()?;
                    work.charge(crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::default() })?;
                    match FileExt::try_lock_shared(&lock) {
                        Ok(()) => Admission::Shared { _lock: OwnerLock(lock, Some(cleanup)) },
                        Err(error) if acyclic_native_runtime::is_exclusive_lock_contention(&error) => return Err(LocalCoreStateStoreError::LazyWork(crate::LazyWorkspaceError::NativeRecordOwnershipConflict(store.root, Some(_store_allocation)))),
                        Err(error) => return Err(error.into()),
                    }
                }
                Ownership::Owned(owner) => Admission::Owned(Arc::clone(owner)),
            };
            let deferred = matches!(admission, Admission::Owned(_))
                && store.deferral.as_ref().is_some_and(|deferral| deferral.load(Ordering::Acquire));
            work.admit(copy)?;
            let namespace_allocation = work.claim(bytes)?;
            let root = store.root.clone();
            work.spent(copy)?;
            let namespace = Namespace { root, admission, durability: if deferred { Durability::Deferred } else { Durability::Flushed } };
            operation(namespace, namespace_allocation, &work)
        }).await
    }

    /// Removes a terminal journal using the same finite native/store boundary.
    pub async fn remove_materialization_bounded(&self, operation_id: OperationId, budget: crate::WorkBudget, cancellation: &crate::CancellationToken) -> Result<crate::OperationReceipt<()>, (LocalCoreStateStoreError, crate::WorkCounters)> {
        let work = crate::materializer::NativeMaterializationWork::new(budget, cancellation)
            .map_err(|(cause, work)| (LocalCoreStateStoreError::MaterializationWork(cause), work))?;
        match self.materialization_with_work(work.clone()).remove(operation_id).await {
            Ok(value) => Ok(crate::OperationReceipt { value, work: work.receipt() }),
            Err(error) => Err((error, work.receipt())),
        }
    }
}

impl MaterializationJournalStore for MeasuredMaterializationStore<'_> {
    type Error = LocalCoreStateStoreError;

    type ActionCompletion<'a> = LocalMaterializationActionCompletion where Self: 'a;

    async fn prepare_action<'a>(&'a self, mut journal: crate::materializer::OwnedMaterializationJournal) -> Result<Self::ActionCompletion<'a>, Self::Error> {
        let custody = journal.custody.as_ref().ok_or(crate::materializer::NativeTreeMaterializationError::MissingCustody)?;
        if !custody.work().shares_allocation_context(&self.work) {
            return Err(LocalCoreStateStoreError::NativeLedgerMismatch);
        }
        if journal.journal.phase != crate::MaterializationPhase::Preparing
            || !journal.journal.is_valid_for(journal.journal.plan.operation_id) {
            return Err(crate::materializer::NativeTreeMaterializationError::InvalidLayout.into());
        }
        self.store.measured_transaction_owned(self.work.clone(), move |namespace, namespace_custody, work| {
            let (paths, paths_custody) = namespace.measured_record(MATERIALIZATION_FAMILY, &journal.journal.plan.operation_id.into_bytes(), work)?;
            let (paths, paths_custody, record_lock) = materialization_acquire_lock(paths, paths_custody, work)?;
            let current = measured_read_record::<MaterializationJournal>(&paths, work)?
                .ok_or(crate::materializer::NativeTreeMaterializationError::InvalidLayout)?;
            if current.0 != journal.journal {
                return Err(crate::materializer::NativeTreeMaterializationError::InvalidLayout.into());
            }
            drop(current);
            let body_custody = work.retain_owned(std::mem::size_of::<LocalMaterializationActionCompletion>() as u64)?;
            journal.journal.revision = journal.journal.revision.checked_add(1).ok_or(crate::WorkError::Overflow)
                .map_err(crate::materializer::NativeTreeMaterializationError::from)?;
            let encoding = PreparedMaterializationEncoding::prepare(&mut journal.journal, work)?;
            // Preserve the actual old image durably before the action. This is
            // the same previous/current family, not a second journal.
            materialization_copy(&paths.current, &paths.previous, work)?;
            materialization_sync(&paths.directory, work)?;
            let host = crate::native_host::PreparedMaterializationRecord::prepare(
                &paths.directory,
                Path::new(paths.current.file_name().ok_or(crate::materializer::NativeTreeMaterializationError::InvalidLayout)?),
                Path::new(paths.previous.file_name().ok_or(crate::materializer::NativeTreeMaterializationError::InvalidLayout)?),
                Path::new(paths.temporary.file_name().ok_or(crate::materializer::NativeTreeMaterializationError::InvalidLayout)?),
                true, work, encoding.bytes.len(),
            )?;
            let RecordPaths { directory, current, previous, temporary, lock, .. } = paths;
            let paths = RecordPaths { directory, current, previous, temporary, lock, namespace: PhantomData };
            Ok(LocalMaterializationActionCompletion {
                prepared: Some(LocalPreparedMaterializationAction {
                    journal, encoding, host, record_lock, paths, namespace,
                    work: work.clone(), paths_custody, namespace_custody,
                }),
                _body_custody: body_custody,
            })
        }).await
    }

    async fn load(&self, operation_id: OperationId) -> Result<Option<crate::materializer::OwnedMaterializationJournal>, Self::Error> {
        self.store.measured_transaction(self.work.clone(), move |namespace, work| {
            let (paths, _paths_allocation) = namespace.measured_record(MATERIALIZATION_FAMILY, &operation_id.into_bytes(), work)?;
            materialization_lock(paths, _paths_allocation, work, |paths| {
                let journal = measured_read_record::<MaterializationJournal>(&paths, work)?;
                let journal = journal.map(|(journal, allocation)| crate::materializer::OwnedMaterializationJournal {
                    journal, custody: Some(allocation),
                });
                work.set_original_work(journal.as_ref().map_or_else(crate::WorkCounters::default, |record| record.journal.work));
                Ok(journal)
            })
        }).await
    }

    async fn compare_and_swap(&self, operation_id: OperationId, expected_revision: u64, mut replacement: crate::materializer::OwnedMaterializationJournal) -> Result<(bool, crate::materializer::OwnedMaterializationJournal), Self::Error> {
        let allocation = replacement.custody.as_ref()
            .ok_or(crate::materializer::NativeTreeMaterializationError::MissingCustody)?;
        if !allocation.work().shares_ledger(&self.work) {
            return Err(LocalCoreStateStoreError::NativeLedgerMismatch);
        }
        self.store.measured_transaction(self.work.clone(), move |namespace, work| {
            let (paths, _paths_allocation) = namespace.measured_record(MATERIALIZATION_FAMILY, &operation_id.into_bytes(), work)?;
            let stored = materialization_lock(paths, _paths_allocation, work, |paths| {
                let current = measured_read_record::<MaterializationJournal>(&paths, work)?;
                if current.as_ref().map_or(0, |(journal, _allocation)| journal.revision) != expected_revision { return Ok(false); }
                work.set_original_work(current.as_ref().map_or_else(crate::WorkCounters::default, |(journal, _allocation)| journal.work));
                replacement.journal.work = work.journal_work()?;
                measured_write_record(&paths, &replacement.journal, work)?;
                Ok(true)
            })?;
            Ok((stored, replacement))
        }).await
    }
}

impl MeasuredMaterializationStore<'_> {
    /// Retains terminal intent until the final record removal; partial cleanup
    /// remains observable and retryable through this same journal family.
    pub async fn remove(&self, operation_id: OperationId) -> Result<(), LocalCoreStateStoreError> {
        self.store.measured_transaction(self.work.clone(), move |namespace, work| {
            let (paths, _paths_allocation) = namespace.measured_record(MATERIALIZATION_FAMILY, &operation_id.into_bytes(), work)?;
            materialization_lock(paths, _paths_allocation, work, |paths| {
                let current = measured_read_record::<MaterializationJournal>(&paths, work)?;
                if current.as_ref().is_some_and(|(journal, _allocation)| !matches!(journal.phase, crate::MaterializationPhase::Applied | crate::MaterializationPhase::RolledBack)) {
                    return Err(LocalCoreStateStoreError::MaterializationInProgress);
                }
                materialization_remove(&paths.temporary, work)?;
                materialization_remove(&paths.previous, work)?;
                materialization_remove(&paths.current, work)?;
                materialization_sync(&paths.directory, work)
            })
        }).await
    }
}

fn measured_record_path(root: &Path, leaf: &str, work: &crate::materializer::NativeMaterializationWork, allocation: &mut crate::materializer::NativeAllocation) -> Result<PathBuf, LocalCoreStateStoreError> {
    let capacity = root.as_os_str().len().checked_add(1).and_then(|bytes| bytes.checked_add(leaf.len()))
        .ok_or(crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow))?;
    let copy = crate::WorkCounters { bytes_copied: root.as_os_str().len() as u64, ..crate::WorkCounters::default() };
    work.admit(copy)?;
    allocation.grow(capacity as u64)?;
    let mut path = PathBuf::with_capacity(capacity);
    path.push(root);
    path.push(leaf);
    work.spent(copy)?;
    Ok(path)
}

impl Namespace {
    fn measured_record(&self, family: &str, key: &[u8], work: &crate::materializer::NativeMaterializationWork) -> Result<(RecordPaths<'_>, crate::materializer::NativeAllocation), LocalCoreStateStoreError> {
        let key_bytes = key.len().checked_mul(2).ok_or(crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow))?;
        let mut name = [0_u8; 96];
        let encoded = name.get_mut(..key_bytes).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "record identity exceeds the bounded key width"))?;
        let encoding = crate::WorkCounters { bytes_encoded: key_bytes as u64, ..crate::WorkCounters::default() };
        work.admit(encoding)?;
        hex::encode_to_slice(key, encoded).map_err(std::io::Error::other)?;
        work.spent(encoding)?;
        let mut allocation = work.claim(0)?;
        let directory = measured_record_path(&self.root, family, work, &mut allocation)?;
        let mut entry = |suffix: &str| -> Result<PathBuf, LocalCoreStateStoreError> {
            let end = key_bytes.checked_add(suffix.len()).ok_or(crate::materializer::NativeTreeMaterializationError::Work(crate::WorkError::Overflow))?;
            name.get_mut(key_bytes..end).ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "record identity and suffix exceed the bounded key width"))?.copy_from_slice(suffix.as_bytes());
            let leaf = std::str::from_utf8(&name[..end]).map_err(std::io::Error::other)?;
            measured_record_path(&directory, leaf, work, &mut allocation)
        };
        let current = entry(".json")?;
        let previous = entry(".previous.json")?;
        let temporary = entry(".next.json")?;
        let lock = entry(".lock")?;
        Ok((RecordPaths { directory, current, previous, temporary, lock, namespace: PhantomData }, allocation))
    }
}

fn materialization_lock<T>(
    paths: RecordPaths<'_>,
    paths_allocation: crate::materializer::NativeAllocation,
    work: &crate::materializer::NativeMaterializationWork,
    action: impl FnOnce(&RecordPaths<'_>) -> Result<T, LocalCoreStateStoreError>,
) -> Result<T, LocalCoreStateStoreError> {
    let (paths, _paths_allocation, mut lock) = materialization_acquire_lock(paths, paths_allocation, work)?;
    let result = action(&paths);
    let cleanup = lock.1.take().ok_or(crate::materializer::NativeTreeMaterializationError::MissingCustody)?;
    let (unlocked, recorded) = cleanup.attempt_recorded(|| FileExt::unlock(&lock.0));
    match result {
        Err(error) => Err(error),
        Ok(value) => { recorded?; unlocked?; Ok(value) }
    }
}

struct MaterializationRecordLock(File, Option<crate::materializer::NativeCleanupWrite>);

impl Drop for MaterializationRecordLock {
    fn drop(&mut self) {
        if let Some(cleanup) = self.1.take() {
            let _ = cleanup.attempt_recorded(|| FileExt::unlock(&self.0));
        }
    }
}

fn materialization_acquire_lock<'a>(
    paths: RecordPaths<'a>,
    mut paths_allocation: crate::materializer::NativeAllocation,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<(RecordPaths<'a>, crate::materializer::NativeAllocation, MaterializationRecordLock), LocalCoreStateStoreError> {
    crate::native_host::create_directories_materialization_measured(&paths.directory, work)?;
    let lock = materialization_open_options(&paths.lock, &crate::native_host::NativeOpenOptions::ReadWriteCreate, work)?;
    let cleanup = work.reserve_cleanup_write()?;
    let deadline = Instant::now() + RECORD_LOCK_DEADLINE;
    loop {
        work.charge(crate::WorkCounters { backend_read_operations: 1, ..crate::WorkCounters::default() })?;
        match lock.try_lock_exclusive() {
            Ok(()) => break,
            Err(error) if acyclic_native_runtime::is_exclusive_lock_contention(&error) => {
                if Instant::now() >= deadline {
                    paths_allocation.substitute(paths.lock.as_os_str().len() as u64)?;
                    return Err(LocalCoreStateStoreError::LazyWork(crate::LazyWorkspaceError::NativeRecordLockTimeout(
                        paths.lock, Some(paths_allocation),
                    )));
                }
                std::thread::sleep(RECORD_LOCK_RETRY);
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok((paths, paths_allocation, MaterializationRecordLock(lock, Some(cleanup))))
}

struct MaterializationReader<'a> {
    file: File,
    work: &'a crate::materializer::NativeMaterializationWork,
    remaining_file_bytes: u64,
    original_length: u64,
}

impl Read for MaterializationReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() { return Ok(0); }
        if self.remaining_file_bytes == 0 {
            if crate::native_host::stat_file_with_work(&self.file, &mut &*self.work).map_err(std::io::Error::other)?.len() != self.original_length {
                return Err(std::io::Error::other("materialization journal changed while reading"));
            }
            return Ok(0);
        }
        let remaining = self.work.remaining().map_err(std::io::Error::other)?;
        let length = buffer.len()
            .min(usize::try_from(remaining.source_bytes_read.min(remaining.bytes_copied)).unwrap_or(usize::MAX))
            .min(usize::try_from(self.remaining_file_bytes).unwrap_or(usize::MAX));
        if length == 0 && !buffer.is_empty() {
            self.work.charge(crate::WorkCounters {
                source_bytes_read: 1, bytes_copied: 1, ..Default::default()
            }).map_err(std::io::Error::other)?;
        }
        let count = crate::native_host::read_builtin_file(
            &self.file,
            buffer.get_mut(..length).ok_or_else(|| std::io::Error::other("invalid journal read bound"))?,
            self.work,
        ).map_err(std::io::Error::other)?;
        self.remaining_file_bytes -= count as u64;
        Ok(count)
    }
}

/// Tracks serde's owned string copies and sequence capacities before its
/// visitors allocate them. Fixed arrays, identifiers and borrowed strings do
/// not masquerade as owned allocations.
struct MaterializationDecode<'a> {
    work: &'a crate::materializer::NativeMaterializationWork,
    allocation: std::cell::RefCell<crate::materializer::NativeAllocation>,
    failure: std::cell::RefCell<Option<crate::materializer::NativeTreeMaterializationError>>,
    ignored: std::cell::Cell<bool>,
}

impl MaterializationDecode<'_> {
    fn charge(&self, delta: crate::WorkCounters) -> Result<(), &'static str> {
        self.work.charge(delta).map_err(|error| {
            *self.failure.borrow_mut() = Some(error);
            "materialization decode work boundary"
        })
    }

    fn admit(&self, delta: crate::WorkCounters) -> Result<(), &'static str> {
        self.work.admit(delta).map_err(|error| {
            *self.failure.borrow_mut() = Some(error);
            "decode work admission failed"
        })
    }

    fn spent(&self, delta: crate::WorkCounters) -> Result<(), &'static str> {
        self.work.spent(delta).map_err(|error| {
            *self.failure.borrow_mut() = Some(error);
            "decode work accounting failed"
        })
    }

    fn grow(&self, bytes: u64) -> Result<(), &'static str> {
        self.allocation.borrow_mut().grow(bytes).map_err(|error| {
            *self.failure.borrow_mut() = Some(error);
            "materialization decode allocation boundary"
        })
    }
}

struct MaterializationDeserializer<'a, 'b, D> {
    inner: D,
    decode: &'a MaterializationDecode<'b>,
    flat_map: bool,
}

struct MaterializationVisitor<'a, 'b, V> {
    inner: V,
    decode: &'a MaterializationDecode<'b>,
    owned_string: bool,
    owned_sequence: bool,
    owned_map: bool,
}

macro_rules! materialization_deserialize {
    ($method:ident, $string:expr, $sequence:expr $(, $argument:ident : $type:ty)*) => {
        fn $method<V: serde::de::Visitor<'de>>(self, $($argument: $type,)* visitor: V) -> Result<V::Value, Self::Error> {
            self.inner.$method($($argument,)* MaterializationVisitor {
                inner: visitor, decode: self.decode,
                owned_string: $string, owned_sequence: $sequence, owned_map: false,
            })
        }
    };
}

impl<'de, D: serde::Deserializer<'de>> serde::Deserializer<'de> for MaterializationDeserializer<'_, '_, D> {
    type Error = D::Error;
    materialization_deserialize!(deserialize_any, false, false);
    materialization_deserialize!(deserialize_bool, false, false);
    materialization_deserialize!(deserialize_i8, false, false);
    materialization_deserialize!(deserialize_i16, false, false);
    materialization_deserialize!(deserialize_i32, false, false);
    materialization_deserialize!(deserialize_i64, false, false);
    materialization_deserialize!(deserialize_i128, false, false);
    materialization_deserialize!(deserialize_u8, false, false);
    materialization_deserialize!(deserialize_u16, false, false);
    materialization_deserialize!(deserialize_u32, false, false);
    materialization_deserialize!(deserialize_u64, false, false);
    materialization_deserialize!(deserialize_u128, false, false);
    materialization_deserialize!(deserialize_f32, false, false);
    materialization_deserialize!(deserialize_f64, false, false);
    materialization_deserialize!(deserialize_char, false, false);
    materialization_deserialize!(deserialize_str, false, false);
    materialization_deserialize!(deserialize_string, true, false);
    materialization_deserialize!(deserialize_bytes, false, false);
    materialization_deserialize!(deserialize_byte_buf, true, false);
    materialization_deserialize!(deserialize_option, false, false);
    materialization_deserialize!(deserialize_unit, false, false);
    materialization_deserialize!(deserialize_unit_struct, false, false, name: &'static str);
    fn deserialize_newtype_struct<V: serde::de::Visitor<'de>>(self, name: &'static str, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_newtype_struct(name, MaterializationVisitor {
            inner: visitor, decode: self.decode, owned_string: false, owned_sequence: false,
            owned_map: name == "CoreStateFlatMap",
        })
    }
    materialization_deserialize!(deserialize_seq, false, true);
    materialization_deserialize!(deserialize_tuple, false, false, length: usize);
    materialization_deserialize!(deserialize_tuple_struct, false, false, name: &'static str, length: usize);
    fn deserialize_map<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_map(MaterializationVisitor {
            inner: visitor, decode: self.decode, owned_string: false, owned_sequence: false, owned_map: self.flat_map,
        })
    }
    materialization_deserialize!(deserialize_struct, false, false, name: &'static str, fields: &'static [&'static str]);
    materialization_deserialize!(deserialize_enum, false, false, name: &'static str, variants: &'static [&'static str]);
    materialization_deserialize!(deserialize_identifier, false, false);
    fn deserialize_ignored_any<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let prior = self.decode.ignored.replace(true);
        let result = self.inner.deserialize_ignored_any(MaterializationVisitor {
            inner: visitor, decode: self.decode, owned_string: false, owned_sequence: false, owned_map: false,
        });
        self.decode.ignored.set(prior);
        result
    }
    fn is_human_readable(&self) -> bool { self.inner.is_human_readable() }
}

macro_rules! materialization_visit {
    ($method:ident, $type:ty) => {
        fn $method<E: serde::de::Error>(self, value: $type) -> Result<Self::Value, E> {
            self.inner.$method(value)
        }
    };
}

impl<'de, V: serde::de::Visitor<'de>> serde::de::Visitor<'de> for MaterializationVisitor<'_, '_, V> {
    type Value = V::Value;
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.inner.expecting(formatter) }
    materialization_visit!(visit_bool, bool);
    materialization_visit!(visit_i8, i8);
    materialization_visit!(visit_i16, i16);
    materialization_visit!(visit_i32, i32);
    materialization_visit!(visit_i64, i64);
    materialization_visit!(visit_i128, i128);
    materialization_visit!(visit_u8, u8);
    materialization_visit!(visit_u16, u16);
    materialization_visit!(visit_u32, u32);
    materialization_visit!(visit_u64, u64);
    materialization_visit!(visit_u128, u128);
    materialization_visit!(visit_f32, f32);
    materialization_visit!(visit_f64, f64);
    materialization_visit!(visit_char, char);

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        let copy = crate::WorkCounters { bytes_copied: value.len() as u64, ..crate::WorkCounters::default() };
        if self.owned_string {
            self.decode.admit(copy).map_err(E::custom)?;
            self.decode.grow(value.len() as u64).map_err(E::custom)?;
        }
        let output = self.inner.visit_str(value)?;
        if self.owned_string { self.decode.spent(copy).map_err(E::custom)?; }
        Ok(output)
    }

    fn visit_borrowed_str<E: serde::de::Error>(self, value: &'de str) -> Result<Self::Value, E> {
        let copy = crate::WorkCounters { bytes_copied: value.len() as u64, ..crate::WorkCounters::default() };
        if self.owned_string {
            self.decode.admit(copy).map_err(E::custom)?;
            self.decode.grow(value.len() as u64).map_err(E::custom)?;
        }
        let output = self.inner.visit_borrowed_str(value)?;
        if self.owned_string { self.decode.spent(copy).map_err(E::custom)?; }
        Ok(output)
    }

    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
        self.decode.grow(value.capacity() as u64).map_err(E::custom)?;
        self.inner.visit_string(value)
    }

    fn visit_bytes<E: serde::de::Error>(self, value: &[u8]) -> Result<Self::Value, E> { self.inner.visit_bytes(value) }
    fn visit_borrowed_bytes<E: serde::de::Error>(self, value: &'de [u8]) -> Result<Self::Value, E> { self.inner.visit_borrowed_bytes(value) }
    fn visit_byte_buf<E: serde::de::Error>(self, value: Vec<u8>) -> Result<Self::Value, E> { self.inner.visit_byte_buf(value) }
    fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> { self.inner.visit_none() }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> { self.inner.visit_unit() }
    fn visit_some<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        self.inner.visit_some(MaterializationDeserializer { inner: deserializer, decode: self.decode, flat_map: false })
    }
    fn visit_newtype_struct<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        self.inner.visit_newtype_struct(MaterializationDeserializer { inner: deserializer, decode: self.decode, flat_map: self.owned_map })
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_seq(MaterializationSequence { inner: access, decode: self.decode, owned: self.owned_sequence, length: 0, capacity: 0 })
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_map(MaterializationMap { inner: access, decode: self.decode, owned: self.owned_map, key_layout: std::alloc::Layout::new::<()>(), length: 0, capacity: 0 })
    }
    fn visit_enum<A: serde::de::EnumAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_enum(MaterializationEnum { inner: access, decode: self.decode })
    }
}

struct MaterializationSeed<'a, 'b, S> { inner: S, decode: &'a MaterializationDecode<'b>, item: bool }
impl<'de, S: serde::de::DeserializeSeed<'de>> serde::de::DeserializeSeed<'de> for MaterializationSeed<'_, '_, S> {
    type Value = S::Value;
    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        use serde::de::Error as _;
        if self.item {
            self.decode.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::default() }).map_err(D::Error::custom)?;
        }
        self.inner.deserialize(MaterializationDeserializer { inner: deserializer, decode: self.decode, flat_map: false })
    }
}

struct MaterializationSequence<'a, 'b, A> {
    inner: A,
    decode: &'a MaterializationDecode<'b>,
    owned: bool,
    length: usize,
    capacity: usize,
}

impl<'de, A: serde::de::SeqAccess<'de>> serde::de::SeqAccess<'de> for MaterializationSequence<'_, '_, A> {
    type Error = A::Error;
    fn next_element_seed<S: serde::de::DeserializeSeed<'de>>(&mut self, seed: S) -> Result<Option<S::Value>, Self::Error> {
        use serde::de::Error as _;
        let value = self.inner.next_element_seed(MaterializationSeed { inner: seed, decode: self.decode, item: true })?;
        if value.is_some() {
            let width = std::mem::size_of::<S::Value>();
            if self.owned && width != 0 && self.length == self.capacity {
                // With no size hint serde's Vec visitor starts at zero; these
                // are Vec's actual minimum and amortized growth capacities.
                let minimum = if width == 1 { 8 } else if width <= 1024 { 4 } else { 1 };
                let next = if self.capacity == 0 { minimum } else { self.capacity.checked_mul(2).ok_or_else(|| Self::Error::custom("journal sequence capacity overflow"))? };
                let bytes = next.checked_sub(self.capacity).and_then(|count| count.checked_mul(width)).ok_or_else(|| Self::Error::custom("journal sequence allocation overflow"))?;
                self.decode.grow(bytes as u64).map_err(Self::Error::custom)?;
                self.capacity = next;
            }
            self.length += 1;
        }
        Ok(value)
    }
    fn size_hint(&self) -> Option<usize> { None }
}

struct MaterializationMap<'a, 'b, A> {
    inner: A,
    decode: &'a MaterializationDecode<'b>,
    owned: bool,
    key_layout: std::alloc::Layout,
    length: usize,
    capacity: usize,
}
impl<'de, A: serde::de::MapAccess<'de>> serde::de::MapAccess<'de> for MaterializationMap<'_, '_, A> {
    type Error = A::Error;
    fn next_key_seed<S: serde::de::DeserializeSeed<'de>>(&mut self, seed: S) -> Result<Option<S::Value>, Self::Error> {
        self.key_layout = std::alloc::Layout::new::<S::Value>();
        self.inner.next_key_seed(MaterializationSeed { inner: seed, decode: self.decode, item: self.owned })
    }
    fn next_value_seed<S: serde::de::DeserializeSeed<'de>>(&mut self, seed: S) -> Result<S::Value, Self::Error> {
        use serde::de::Error as _;
        let value = self.inner.next_value_seed(MaterializationSeed { inner: seed, decode: self.decode, item: false })?;
        if self.owned {
            let value_layout = std::alloc::Layout::new::<S::Value>();
            let alignment = self.key_layout.align().max(value_layout.align());
            let size = self.key_layout.size().checked_add(value_layout.size())
                .and_then(|bytes| bytes.checked_add(alignment - 1))
                .map(|bytes| bytes & !(alignment - 1))
                .ok_or_else(|| Self::Error::custom("flat map entry layout overflow"))?;
            if size != 0 && self.length == self.capacity {
                let minimum = if size == 1 { 8 } else if size <= 1024 { 4 } else { 1 };
                let next = if self.capacity == 0 { minimum } else { self.capacity.checked_mul(2).ok_or_else(|| Self::Error::custom("flat map capacity overflow"))? };
                let bytes = (next - self.capacity).checked_mul(size).ok_or_else(|| Self::Error::custom("flat map allocation overflow"))?;
                self.decode.grow(bytes as u64).map_err(Self::Error::custom)?;
                self.capacity = next;
            }
            self.length += 1;
        }
        Ok(value)
    }
    fn size_hint(&self) -> Option<usize> { None }
}

struct MaterializationEnum<'a, 'b, A> { inner: A, decode: &'a MaterializationDecode<'b> }
impl<'de, 'a, 'b, A: serde::de::EnumAccess<'de>> serde::de::EnumAccess<'de> for MaterializationEnum<'a, 'b, A> {
    type Error = A::Error;
    type Variant = MaterializationVariant<'a, 'b, A::Variant>;
    fn variant_seed<S: serde::de::DeserializeSeed<'de>>(self, seed: S) -> Result<(S::Value, Self::Variant), Self::Error> {
        let (value, variant) = self.inner.variant_seed(MaterializationSeed { inner: seed, decode: self.decode, item: false })?;
        Ok((value, MaterializationVariant { inner: variant, decode: self.decode }))
    }
}

struct MaterializationVariant<'a, 'b, A> { inner: A, decode: &'a MaterializationDecode<'b> }

impl<'de, A: serde::de::VariantAccess<'de>> serde::de::VariantAccess<'de> for MaterializationVariant<'_, '_, A> {
    type Error = A::Error;
    fn unit_variant(self) -> Result<(), Self::Error> { self.inner.unit_variant() }
    fn newtype_variant_seed<S: serde::de::DeserializeSeed<'de>>(self, seed: S) -> Result<S::Value, Self::Error> {
        self.inner.newtype_variant_seed(MaterializationSeed { inner: seed, decode: self.decode, item: false })
    }
    fn tuple_variant<V: serde::de::Visitor<'de>>(self, length: usize, visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.tuple_variant(length, MaterializationVisitor { inner: visitor, decode: self.decode, owned_string: false, owned_sequence: false, owned_map: false })
    }
    fn struct_variant<V: serde::de::Visitor<'de>>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value, Self::Error> {
        self.inner.struct_variant(fields, MaterializationVisitor { inner: visitor, decode: self.decode, owned_string: false, owned_sequence: false, owned_map: false })
    }
}

fn measured_read_file<T: DeserializeOwned>(path: &Path, work: &crate::materializer::NativeMaterializationWork) -> Result<Option<(T, crate::materializer::NativeAllocation)>, LocalCoreStateStoreError> {
    let file = match materialization_open(path, false, work) {
        Ok(file) => file,
        Err(crate::materializer::NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let length = crate::native_host::stat_file_with_work(&file, &mut &*work)?.len();
    let _buffer = work.claim(64 * 1024)?;
    let reader = std::io::BufReader::with_capacity(64 * 1024, MaterializationReader { file, work, remaining_file_bytes: length, original_length: length });
    measured_decode_reader(reader, work, std::marker::PhantomData::<T>).map(Some)
}

// The JSON reader uses a reusable Vec<u8> for strings, including identifiers.
// Admit exactly its push/reserve growth before returning the triggering byte.
// Ignored values use serde_json's allocation-free ignore_str instead.
#[derive(Clone, Copy)]
enum JsonStringState {
    Outside,
    Text,
    Escape,
    Unicode { value: u16, digits: u8, high: Option<u16> },
    SurrogateSlash(u16),
    SurrogateU(u16),
    Invalid,
}

struct MeasuredJsonReader<'a, 'b, R> {
    inner: R,
    decode: &'a MaterializationDecode<'b>,
    allocation: crate::materializer::NativeAllocation,
    state: JsonStringState,
    length: usize,
    capacity: usize,
    pending_copy: u64,
}

impl<R> MeasuredJsonReader<'_, '_, R> {
    fn emit(&mut self, length: usize, reserve: usize, copied: u64) -> std::io::Result<()> {
        if self.decode.ignored.get() { return Ok(()); }
        let delta = crate::WorkCounters { bytes_copied: copied, ..crate::WorkCounters::default() };
        self.decode.admit(delta).map_err(std::io::Error::other)?;
        let required = self.length.checked_add(reserve).ok_or_else(|| std::io::Error::other("JSON scratch capacity overflow"))?;
        if required > self.capacity {
            let next = self.capacity.checked_mul(2).ok_or_else(|| std::io::Error::other("JSON scratch growth overflow"))?.max(8).max(required);
            self.allocation.grow((next - self.capacity) as u64).map_err(std::io::Error::other)?;
            self.capacity = next;
        }
        self.length = self.length.checked_add(length).ok_or_else(|| std::io::Error::other("JSON string length overflow"))?;
        // The parser performs the corresponding push after this read returns.
        // Record it on the next read, before admitting any further work.
        self.pending_copy = copied;
        Ok(())
    }

    fn byte(&mut self, byte: u8) -> std::io::Result<()> {
        use JsonStringState as S;
        match self.state {
            S::Outside if byte == b'"' => { self.length = 0; self.state = S::Text; }
            S::Text => match byte {
                b'"' => self.state = S::Outside,
                b'\\' => self.state = S::Escape,
                0..=31 => self.state = S::Invalid,
                _ => self.emit(1, 1, 1)?,
            },
            S::Escape => match byte {
                b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {
                    self.emit(1, 1, 0)?;
                    self.state = S::Text;
                }
                b'u' => self.state = S::Unicode { value: 0, digits: 0, high: None },
                _ => self.state = S::Invalid,
            },
            S::Unicode { value, digits, high } => {
                let digit = match byte {
                    b'0'..=b'9' => Some(byte - b'0'),
                    b'a'..=b'f' => Some(byte - b'a' + 10),
                    b'A'..=b'F' => Some(byte - b'A' + 10),
                    _ => None,
                };
                let Some(digit) = digit else { self.state = S::Invalid; return Ok(()); };
                let value = (value << 4) | u16::from(digit);
                if digits != 3 {
                    self.state = S::Unicode { value, digits: digits + 1, high };
                } else if let Some(high) = high {
                    if !(0xdc00..=0xdfff).contains(&value) { self.state = S::Invalid; return Ok(()); }
                    let scalar = 0x10000 + ((u32::from(high) - 0xd800) << 10) + u32::from(value) - 0xdc00;
                    let Some(character) = char::from_u32(scalar) else { self.state = S::Invalid; return Ok(()); };
                    self.emit(character.len_utf8(), 4, 0)?;
                    self.state = S::Text;
                } else if (0xd800..=0xdbff).contains(&value) {
                    self.state = S::SurrogateSlash(value);
                } else if (0xdc00..=0xdfff).contains(&value) {
                    self.state = S::Invalid;
                } else {
                    let Some(character) = char::from_u32(u32::from(value)) else { self.state = S::Invalid; return Ok(()); };
                    self.emit(character.len_utf8(), if value < 0x80 { 1 } else { 4 }, 0)?;
                    self.state = S::Text;
                }
            }
            S::SurrogateSlash(high) => self.state = if byte == b'\\' { S::SurrogateU(high) } else { S::Invalid },
            S::SurrogateU(high) => self.state = if byte == b'u' { S::Unicode { value: 0, digits: 0, high: Some(high) } } else { S::Invalid },
            _ => {}
        }
        Ok(())
    }
}

impl<R: Read> Read for MeasuredJsonReader<'_, '_, R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.pending_copy != 0 {
            self.decode.spent(crate::WorkCounters { bytes_copied: self.pending_copy, ..crate::WorkCounters::default() }).map_err(std::io::Error::other)?;
            self.pending_copy = 0;
        }
        if buffer.is_empty() { return Ok(0); }
        self.decode.work.remaining().map_err(std::io::Error::other)?;
        let count = self.inner.read(&mut buffer[..1])?;
        if count != 0 { self.byte(buffer[0])?; }
        Ok(count)
    }
}

fn measured_json_error(error: serde_json::Error) -> LocalCoreStateStoreError {
    if !error.is_io() { return LocalCoreStateStoreError::Json(error); }
    let error = std::io::Error::from(error);
    if !error.get_ref().is_some_and(|cause| cause.is::<crate::materializer::NativeTreeMaterializationError>()) {
        return LocalCoreStateStoreError::Io(error);
    }
    let kind = error.kind();
    match error.into_inner() {
        Some(inner) => match inner.downcast::<crate::materializer::NativeTreeMaterializationError>() {
            Ok(error) => LocalCoreStateStoreError::MaterializationWork(*error),
            Err(error) => LocalCoreStateStoreError::Io(std::io::Error::new(kind, error)),
        },
        None => LocalCoreStateStoreError::Io(kind.into()),
    }
}

// Inputs are flat DTOs: scalars/arrays, owned Vec/String, or explicitly marked
// CoreStateFlatMap Vec pairs. Tree/Box/Arc reconstruction is separately metered.
fn measured_decode_reader<'de, R: Read, S: serde::de::DeserializeSeed<'de>>(reader: R, work: &crate::materializer::NativeMaterializationWork, seed: S) -> Result<(S::Value, crate::materializer::NativeAllocation), LocalCoreStateStoreError> {
    let decode = MaterializationDecode {
        work, allocation: std::cell::RefCell::new(work.claim(0)?),
        failure: std::cell::RefCell::new(None), ignored: std::cell::Cell::new(false),
    };
    let reader = MeasuredJsonReader { inner: reader, decode: &decode, allocation: work.claim(0)?, state: JsonStringState::Outside, length: 0, capacity: 0, pending_copy: 0 };
    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    let result = seed.deserialize(MaterializationDeserializer { inner: &mut deserializer, decode: &decode, flat_map: false })
        .and_then(|value| deserializer.end().map(|()| value));
    if let Some(error) = decode.failure.borrow_mut().take() {
        return Err(LocalCoreStateStoreError::MaterializationWork(error));
    }
    let value = result.map_err(measured_json_error)?;
    drop(deserializer);
    Ok((value, decode.allocation.into_inner()))
}

fn measured_decode_slice<T: DeserializeOwned>(bytes: &[u8], work: &crate::materializer::NativeMaterializationWork) -> Result<(T, crate::materializer::NativeAllocation), LocalCoreStateStoreError> {
    measured_decode_reader(std::io::Cursor::new(bytes), work, std::marker::PhantomData::<T>)
}

fn measured_decode_slice_seed<'de, S: serde::de::DeserializeSeed<'de>>(bytes: &[u8], work: &crate::materializer::NativeMaterializationWork, seed: S) -> Result<(S::Value, crate::materializer::NativeAllocation), LocalCoreStateStoreError> {
    measured_decode_reader(std::io::Cursor::new(bytes), work, seed)
}

fn materialization_writer_result(result: Result<(), serde_json::Error>, _writer: &mut MaterializationWriter<'_>) -> Result<(), LocalCoreStateStoreError> {
    result.map_err(measured_json_error)
}

fn measured_read_record<T: DeserializeOwned>(paths: &RecordPaths, work: &crate::materializer::NativeMaterializationWork) -> Result<Option<(T, crate::materializer::NativeAllocation)>, LocalCoreStateStoreError> {
    match measured_read_file(&paths.current, work) {
        Ok(Some(value)) => Ok(Some(value)),
        Err(LocalCoreStateStoreError::Io(error)) => Err(LocalCoreStateStoreError::Io(error)),
        Err(LocalCoreStateStoreError::MaterializationWork(error)) => Err(LocalCoreStateStoreError::MaterializationWork(error)),
        Err(LocalCoreStateStoreError::Json(error)) if error.is_io() => Err(LocalCoreStateStoreError::Json(error)),
        current => match measured_read_file(&paths.previous, work)? {
            Some(value) => {
                materialization_copy(&paths.previous, &paths.current, work)?;
                materialization_sync(&paths.directory, work)?;
                Ok(Some(value))
            }
            None => current,
        },
    }
}

struct MaterializationWriter<'a> {
    file: File,
    work: &'a crate::materializer::NativeMaterializationWork,
    encoded: bool,
}

impl Write for MaterializationWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.is_empty() { return Ok(0); }
        let remaining = self.work.remaining().map_err(std::io::Error::other)?;
        let length = bytes.len().min(remaining.object_bytes_written as usize).min(if self.encoded { remaining.bytes_encoded as usize } else { usize::MAX });
        if length == 0 && !bytes.is_empty() {
            self.work.charge(crate::WorkCounters { object_bytes_written: 1, bytes_encoded: u64::from(self.encoded), ..crate::WorkCounters::default() }).map_err(std::io::Error::other)?;
        }
        self.work.charge(crate::WorkCounters { backend_write_operations: 1, ..crate::WorkCounters::default() }).map_err(std::io::Error::other)?;
        let count = self.file.write(bytes.get(..length).ok_or_else(|| std::io::Error::other("invalid journal write bound"))?)?;
        self.work.spent(crate::WorkCounters { object_bytes_written: count as u64, bytes_encoded: if self.encoded { count as u64 } else { 0 }, ..crate::WorkCounters::default() }).map_err(std::io::Error::other)?;
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

fn materialization_sync(path: &Path, work: &crate::materializer::NativeMaterializationWork) -> Result<(), LocalCoreStateStoreError> {
    let root = crate::native_host::HostRoot::open_materialization_measured(path, work)?;
    let directory = root.open_dir_materialization_measured(Path::new(""), work)?;
    crate::native_host::sync_directory_held_measured(&directory, work)?;
    Ok(())
}

fn materialization_parent(path: &Path, work: &crate::materializer::NativeMaterializationWork) -> Result<cap_std::fs::Dir, crate::materializer::NativeTreeMaterializationError> {
    let parent = path.parent().filter(|path| !path.as_os_str().is_empty()).unwrap_or_else(|| Path::new("."));
    let held = crate::native_host::HostRoot::open_materialization_measured(parent, work)?;
    held.open_dir_materialization_measured(Path::new(""), work)
}

fn materialization_open(path: &Path, truncate: bool, work: &crate::materializer::NativeMaterializationWork) -> Result<File, crate::materializer::NativeTreeMaterializationError> {
    let options = if truncate { crate::native_host::NativeOpenOptions::ReadWriteCreate } else { crate::native_host::NativeOpenOptions::Read };
    let file = materialization_open_options(path, &options, work)?;
    if truncate { crate::native_host::truncate_file_with_work(&file, 0, &mut &*work)?; }
    Ok(file)
}

fn materialization_open_options(path: &Path, options: &crate::native_host::NativeOpenOptions, work: &crate::materializer::NativeMaterializationWork) -> Result<File, crate::materializer::NativeTreeMaterializationError> {
    let parent = materialization_parent(path, work)?;
    let leaf = path.file_name().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "journal file has no leaf"))?;
    crate::native_host::open_leaf_with_work(&parent, Path::new(leaf), options, &mut &*work)
}

fn materialization_exists(path: &Path, work: &crate::materializer::NativeMaterializationWork) -> Result<bool, LocalCoreStateStoreError> {
    let parent = materialization_parent(path, work)?;
    let leaf = path.file_name().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "journal file has no leaf"))?;
    match crate::materializer::native_stat_held(&parent, Path::new(leaf), work) {
        Ok(_) => Ok(true),
        Err(crate::materializer::NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

#[cfg(not(windows))]
fn materialization_rename(from: &Path, to: &Path, work: &crate::materializer::NativeMaterializationWork) -> Result<(), LocalCoreStateStoreError> {
    let parent = materialization_parent(from, work)?;
    let source = from.file_name().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "journal source has no leaf"))?;
    let target = to.file_name().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "journal destination has no leaf"))?;
    crate::native_host::rename_no_replace_held_measured(&parent, Path::new(source), &parent, Path::new(target), work)?;
    Ok(())
}

fn materialization_copy(from: &Path, to: &Path, work: &crate::materializer::NativeMaterializationWork) -> Result<(), LocalCoreStateStoreError> {
    let file = materialization_open(from, false, work)?;
    let length = crate::native_host::stat_file_with_work(&file, &mut &*work)?.len();
    let mut source = MaterializationReader { file, work, remaining_file_bytes: length, original_length: length };
    let mut destination = MaterializationWriter { file: materialization_open(to, true, work)?, work, encoded: false };
    let mut bytes = [0; 64 * 1024];
    loop {
        let count = source.read(&mut bytes)?;
        if count == 0 { break; }
        destination.write_all(bytes.get(..count).ok_or_else(|| std::io::Error::other("invalid journal copy length"))?)?;
    }
    crate::native_host::sync_file_held_with_work(&destination.file, &mut &*work)?;
    Ok(())
}

fn materialization_remove(path: &Path, work: &crate::materializer::NativeMaterializationWork) -> Result<(), LocalCoreStateStoreError> {
    let parent = materialization_parent(path, work)?;
    let leaf = path.file_name().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "journal file has no leaf"))?;
    match crate::native_host::remove_leaf_with_work(&parent, Path::new(leaf), &mut &*work) {
        Ok(()) => {}
        Err(crate::materializer::NativeTreeMaterializationError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn measured_write_record<T: Serialize>(paths: &RecordPaths, journal: &T, work: &crate::materializer::NativeMaterializationWork) -> Result<(), LocalCoreStateStoreError> {
    let mut writer = MaterializationWriter { file: materialization_open(&paths.temporary, true, work)?, work, encoded: true };
    let encoded = serde_json::to_writer(&mut writer, journal);
    materialization_writer_result(encoded, &mut writer)?;
    crate::native_host::sync_file_held_with_work(&writer.file, &mut &*work)?;
    drop(writer);
    materialization_remove(&paths.previous, work)?;
    #[cfg(windows)]
    {
        let previous = if materialization_exists(&paths.current, work)? { &paths.current } else { &paths.temporary };
        materialization_copy(previous, &paths.previous, work)?;
        materialization_copy(&paths.temporary, &paths.current, work)?;
        materialization_remove(&paths.temporary, work)?;
    }
    #[cfg(not(windows))]
    {
        if materialization_exists(&paths.current, work)? {
            materialization_rename(&paths.current, &paths.previous, work)?;
        }
        materialization_rename(&paths.temporary, &paths.current, work)?;
        materialization_sync(&paths.directory, work)?;
    }
    materialization_remove(&paths.previous, work)?;
    Ok(())
}

impl MultiRootPublicationStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MultiRootPublication>, Self::Error> {
        self.load_record(MULTI_ROOT_PUBLICATION_FAMILY, operation_id.into_bytes())
            .await
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MultiRootPublication,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_record(
            MULTI_ROOT_PUBLICATION_FAMILY,
            operation_id.into_bytes(),
            expected_revision,
            replacement,
        )
        .await
    }

    async fn list_operations(&self) -> Result<Vec<OperationId>, Self::Error> {
        self.transaction(|namespace| namespace.operation_records(MULTI_ROOT_PUBLICATION_FAMILY))
            .await
    }

    async fn compare_and_delete(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
    ) -> Result<bool, Self::Error> {
        self.compare_and_delete_record::<MultiRootPublication>(
            MULTI_ROOT_PUBLICATION_FAMILY,
            operation_id.into_bytes(),
            expected_revision,
        )
        .await
    }

    async fn claim_parent(
        &self,
        parent_context_id: WorkspaceContextId,
        operation_id: OperationId,
    ) -> Result<bool, Self::Error> {
        self.transaction(move |namespace| {
            let paths = namespace.record(
                MULTI_ROOT_PARENT_CLAIM_FAMILY,
                &parent_context_id.into_bytes(),
            );
            for _ in 0..2 {
                if let Some(claim) = read_locked::<MultiRootParentClaim>(&paths)? {
                    return Ok(claim.operation_id == operation_id);
                }
                if compare_and_swap(
                    &paths,
                    0,
                    &MultiRootParentClaim {
                        revision: 1,
                        operation_id,
                    },
                )? {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
    }

    async fn release_parent(
        &self,
        parent_context_id: WorkspaceContextId,
        operation_id: OperationId,
    ) -> Result<bool, Self::Error> {
        self.transaction(move |namespace| {
            let paths = namespace.record(
                MULTI_ROOT_PARENT_CLAIM_FAMILY,
                &parent_context_id.into_bytes(),
            );
            let Some(claim) = read_locked::<MultiRootParentClaim>(&paths)? else {
                return Ok(true);
            };
            if claim.operation_id != operation_id {
                return Ok(true);
            }
            compare_and_delete::<MultiRootParentClaim>(&paths, claim.revision)
        })
        .await
    }
}

/// Entries retained per cache generation before the older generation is dropped.
const NODE_CACHE_GENERATION: usize = 16_384;

/// Two-generation bounded memo: an entry read since the last rotation
/// survives the next one, so hot treap spines stay resident.
#[derive(Debug)]
struct Generations<K, V> {
    current: Vec<(K, NativeOwnedCoreState<V>)>,
    previous: Vec<(K, NativeOwnedCoreState<V>)>,
    /// When each node was last put; see [`LAZY_NODE_GRACE`].
    put: Vec<(K, Instant)>,
    /// The size at which expired put times are next pruned: twice what the
    /// last pruning kept, so pruning stays amortized constant per put.
    prune_at: usize,
    /// Counts nodes collected; a load memoizes what it read only if none was
    /// collected since it began, so the memo never outlives a node's file.
    collected: u64,
    slots: NativeSlotCustody,
}

/// How long a node that was put stays safe from collection: long enough for
/// the mutation that put it to swap it into its workspace's state.
const LAZY_NODE_GRACE: Duration = if cfg!(test) {
    Duration::ZERO
} else {
    Duration::from_secs(10 * 60)
};

impl<K: Copy + Ord, V> Generations<K, V> {
    fn new() -> Self {
        Self {
            current: Vec::new(),
            previous: Vec::new(),
            put: Vec::new(),
            prune_at: NODE_CACHE_GENERATION,
            collected: 0,
            slots: NativeSlotCustody::default(),
        }
    }

    fn position<T>(
        entries: &[(K, T)],
        key: &K,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Result<usize, usize>, LocalCoreStateStoreError> {
        let mut start = 0;
        let mut end = entries.len();
        while start < end {
            let index = start + (end - start) / 2;
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })?;
            match entries[index].0.cmp(key) {
                std::cmp::Ordering::Less => start = index + 1,
                std::cmp::Ordering::Greater => end = index,
                std::cmp::Ordering::Equal => return Ok(Ok(index)),
            }
        }
        Ok(Err(start))
    }

    /// Grow only explicit typed slots; opaque map buckets never enter the ledger.
    fn reserve<T>(
        entries: &mut Vec<T>,
        work: &crate::materializer::NativeMaterializationWork,
        slots: &mut NativeSlotCustody,
    ) -> Result<(), LocalCoreStateStoreError> {
        let needed = entries.len() + 1;
        slots.reserve(entries, needed, work)
    }

    /// Records puts before the durable boundary under collection's own lock.
    fn touch(
        &mut self,
        key: K,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        if self.put.len() >= self.prune_at {
            let width = std::mem::size_of::<(K, Instant)>() as u64;
            work.admit(crate::WorkCounters {
                items_examined: self.put.len() as u64,
                bytes_copied: self.put.len() as u64 * width,
                ..Default::default()
            })?;
            let mut hole = false;
            let mut moved = 0_u64;
            let visited = self.put.len();
            self.put.retain(|(_, instant)| {
                if instant.elapsed() >= LAZY_NODE_GRACE {
                    hole = true;
                    false
                } else {
                    moved += u64::from(hole);
                    true
                }
            });
            work.spent(crate::WorkCounters {
                items_examined: visited as u64,
                bytes_copied: moved * width,
                ..Default::default()
            })?;
            self.prune_at = NODE_CACHE_GENERATION.max(self.put.len().saturating_mul(2));
        }
        match Self::position(&self.put, &key, work)? {
            Ok(index) => self.put[index].1 = Instant::now(),
            Err(index) => {
                Self::reserve(&mut self.put, work, &mut self.slots)?;
                work.charge(crate::WorkCounters {
                    bytes_copied: ((self.put.len() - index) * std::mem::size_of::<(K, Instant)>()) as u64,
                    ..Default::default()
                })?;
                self.put.insert(index, (key, Instant::now()));
            }
        }
        Ok(())
    }

    /// Forgets a collected node, retaining the original put grace custody.
    fn release(&mut self, key: &K) -> bool {
        if self.put.iter().any(|(candidate, put)| candidate == key && put.elapsed() < LAZY_NODE_GRACE) {
            return false;
        }
        self.put.retain(|(candidate, _)| candidate != key);
        self.current.retain(|(candidate, _)| candidate != key);
        self.previous.retain(|(candidate, _)| candidate != key);
        self.collected += 1;
        true
    }

    fn get(
        &mut self,
        key: &K,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<&V>, LocalCoreStateStoreError> {
        if let Ok(index) = Self::position(&self.current, key, work)? {
            return Ok(Some(self.current[index].1.as_state()));
        }
        let Ok(index) = Self::position(&self.previous, key, work)? else {
            return Ok(None);
        };
        // Promote the owned node without cloning it twice.
        work.charge(crate::WorkCounters {
            bytes_copied: ((self.previous.len() - index - 1) * std::mem::size_of::<(K, NativeOwnedCoreState<V>)>()) as u64,
            ..Default::default()
        })?;
        let (key, value) = self.previous.remove(index);
        let index = self.insert(key, value, work)?;
        Ok(Some(self.current[index].1.as_state()))
    }

    fn insert(
        &mut self,
        key: K,
        value: NativeOwnedCoreState<V>,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<usize, LocalCoreStateStoreError> {
        let mut index = match Self::position(&self.current, &key, work)? {
            Ok(index) => {
                self.current[index].1 = value;
                return Ok(index);
            }
            Err(index) => index,
        };
        if self.current.len() >= NODE_CACHE_GENERATION {
            work.charge(crate::WorkCounters {
                items_examined: self.previous.len() as u64,
                ..Default::default()
            })?;
            self.previous.clear();
            std::mem::swap(&mut self.previous, &mut self.current);
            index = 0;
        }
        Self::reserve(&mut self.current, work, &mut self.slots)?;
        work.charge(crate::WorkCounters {
            bytes_copied: ((self.current.len() - index) * std::mem::size_of::<(K, NativeOwnedCoreState<V>)>()) as u64,
            ..Default::default()
        })?;
        self.current.insert(index, (key, value));
        Ok(index)
    }
}

/// Immutable overlay and shadow nodes this store and its clones have read or
/// written.
///
/// A node's identifier is the digest of its verified encoding, so a memoized
/// node can never be stale: eviction only costs a reload.
#[derive(Debug)]
struct NodeMemo {
    overlays: Mutex<Generations<LazyOverlayId, LazyOverlay>>,
    shadows: Mutex<Generations<crate::LazyShadowId, crate::LazyShadow>>,
}

impl Default for NodeMemo {
    fn default() -> Self {
        Self {
            overlays: Mutex::new(Generations::new()),
            shadows: Mutex::new(Generations::new()),
        }
    }
}

/// An immutable content-addressed lazy node.
///
/// Every store implies the canonical empty node (`Default`) without storing
/// it, so attaching a root writes no node at all.
trait LazyNode: Clone + Default + PartialEq + Serialize + DeserializeOwned + Send + 'static {
    type Id: Copy + Ord + Send + 'static;
    type Wire: DeserializeOwned;
    const FAMILY: &'static str;

    fn address(id: Self::Id) -> [u8; 32];
    fn memo(nodes: &NodeMemo) -> MutexGuard<'_, Generations<Self::Id, Self>>;
    fn from_wire(
        wire: Self::Wire,
        work: &crate::materializer::NativeMaterializationWork,
        allocation: &mut crate::materializer::NativeAllocation,
    ) -> Result<Self, LocalCoreStateStoreError>;
    fn clone_owned(
        &self,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(Self, crate::materializer::NativeAllocation), LocalCoreStateStoreError>;
    fn owned_bytes(&self) -> u64;
}

impl LazyNode for LazyOverlay {
    type Id = LazyOverlayId;
    type Wire = Self;
    const FAMILY: &'static str = "lazy-overlays";

    fn address(id: Self::Id) -> [u8; 32] {
        id.into_bytes()
    }

    fn memo(nodes: &NodeMemo) -> MutexGuard<'_, Generations<Self::Id, Self>> {
        nodes
            .overlays
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn from_wire(
        wire: Self::Wire,
        _work: &crate::materializer::NativeMaterializationWork,
        _allocation: &mut crate::materializer::NativeAllocation,
    ) -> Result<Self, LocalCoreStateStoreError> {
        Ok(wire)
    }

    fn clone_owned(
        &self,
        meter: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(Self, crate::materializer::NativeAllocation), LocalCoreStateStoreError> {
        meter.with_owned_allocations(|allocations, work, budget, cancellation| {
            Ok(self.clone_measured(allocations, work, budget, cancellation)?)
        })
    }

    fn owned_bytes(&self) -> u64 { self.logical_owned_bytes() }
}

impl LazyNode for crate::LazyShadow {
    type Id = crate::LazyShadowId;
    type Wire = LazyShadowWire;
    const FAMILY: &'static str = "lazy-shadows-v1";

    fn address(id: Self::Id) -> [u8; 32] {
        id.into_bytes()
    }

    fn memo(nodes: &NodeMemo) -> MutexGuard<'_, Generations<Self::Id, Self>> {
        nodes.shadows.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn from_wire(
        wire: Self::Wire,
        work: &crate::materializer::NativeMaterializationWork,
        allocation: &mut crate::materializer::NativeAllocation,
    ) -> Result<Self, LocalCoreStateStoreError> {
        Ok(match wire {
            LazyShadowWire::Empty => Self::Empty,
            LazyShadowWire::Node { file_id, priority, record, metadata, left, right } => {
                let bytes = std::mem::size_of::<crate::WorkspaceMetadata>() as u64;
                work.admit(crate::WorkCounters { bytes_copied: bytes, ..Default::default() })?;
                allocation.grow(bytes)?;
                let metadata = Box::new(metadata);
                work.spent(crate::WorkCounters { bytes_copied: bytes, ..Default::default() })?;
                Self::Node { file_id, priority, record, metadata, left, right }
            }
        })
    }

    fn clone_owned(
        &self,
        meter: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(Self, crate::materializer::NativeAllocation), LocalCoreStateStoreError> {
        meter.with_owned_allocations(|allocations, work, budget, cancellation| {
            Ok(self.clone_measured(allocations, work, budget, cancellation)?)
        })
    }

    fn owned_bytes(&self) -> u64 { self.logical_owned_bytes() }
}

/// The same durable shadow JSON, with its metadata inline during bounded decode.
#[derive(Deserialize)]
enum LazyShadowWire {
    Empty,
    Node {
        file_id: crate::FileId,
        priority: u64,
        record: Vec<u8>,
        metadata: crate::WorkspaceMetadata,
        left: crate::LazyShadowId,
        right: crate::LazyShadowId,
    },
}

fn lazy_digest<T: Serialize>(
    value: &T,
    meter: &crate::materializer::NativeMaterializationWork,
) -> Result<[u8; 32], LocalCoreStateStoreError> {
    meter.with_allocations(|_, work, budget, cancellation| {
        crate::lazy_workspace::hash_lazy_value_with_counters(value, work, budget, cancellation)
            .map_err(LocalCoreStateStoreError::LazyWork)
    })
}


fn native_core_error(error: LocalCoreStateStoreError) -> crate::LazyWorkspaceError {
    use crate::LazyWorkspaceError as Lazy;
    match error {
        LocalCoreStateStoreError::Io(error) => Lazy::NativeRecordIo(error, None),
        LocalCoreStateStoreError::LockTimeout(path) => Lazy::NativeRecordLockTimeout(path, None),
        LocalCoreStateStoreError::OwnershipConflict(path) => Lazy::NativeRecordOwnershipConflict(path, None),
        // serde_json's private error message/formatter storage is opaque codec
        // storage; decoded SDK payloads retain their separate original guards.
        LocalCoreStateStoreError::Json(error) => Lazy::NativeRecordJson(error, None),
        LocalCoreStateStoreError::MaterializationInProgress => Lazy::NativeRecordMaterializationInProgress,
        LocalCoreStateStoreError::MaterializationWork(error) => match error {
            crate::materializer::NativeTreeMaterializationError::Cancelled => Lazy::Cancelled,
            crate::materializer::NativeTreeMaterializationError::Work(error) => Lazy::DemandWork(error),
            error => Lazy::NativeWork(error),
        },
        LocalCoreStateStoreError::LazyWork(error) => error,
        LocalCoreStateStoreError::Integrity => Lazy::NativeRecordIntegrity,
        LocalCoreStateStoreError::NativeLedgerMismatch => Lazy::NativeLedgerMismatch,
        LocalCoreStateStoreError::ContextTransaction(message) => Lazy::NativeRecordContextTransaction(message, None),
        LocalCoreStateStoreError::Corrupt(path) => Lazy::NativeRecordCorrupt(path, None),
    }
}

fn lazy_native_failure(
    (cause, work): (crate::materializer::NativeTreeMaterializationError, crate::WorkCounters),
) -> crate::LazyWorkspaceError {
    crate::LazyWorkspaceError::NativeConstruction { cause, work }
}

fn finish_lazy_receipt<T>(
    result: Result<T, LocalCoreStateStoreError>,
    meter: &crate::materializer::NativeMaterializationWork,
) -> Result<crate::OperationReceipt<T>, crate::LazyWorkspaceError> {
    match result {
        Ok(value) => Ok(crate::OperationReceipt { value, work: meter.receipt() }),
        Err(error) => {
            let error = native_core_error(error);
            Err(crate::LazyWorkspaceError::SpentWork(Box::new(
                crate::OperationFailure::new(error, meter.receipt()),
            )))
        }
    }
}

fn unbounded_lazy_work() -> Result<crate::materializer::NativeMaterializationWork, LocalCoreStateStoreError> {
    crate::materializer::NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ).map_err(|failure| LocalCoreStateStoreError::LazyWork(lazy_native_failure(failure)))
}

impl LocalCoreStateStore {
    async fn load_node<T: LazyNode>(
        &self,
        id: T::Id,
        meter: crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<T>, LocalCoreStateStoreError> {
        Ok(self.load_node_owned::<T>(id, meter).await?
            .map(|owned| owned.into_parts().0))
    }

    async fn load_node_owned<T: LazyNode>(
        &self,
        id: T::Id,
        meter: crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<NativeOwnedCoreState<T>>, LocalCoreStateStoreError> {
        if T::address(id) == lazy_digest(&T::default(), &meter)? {
            return Ok(Some(NativeOwnedCoreState::from_parts(T::default(), meter.claim(0)?)));
        }
        let collected = {
            let mut memo = T::memo(&self.nodes);
            if let Some(value) = memo.get(&id, &meter)? {
                let (value, allocation) = value.clone_owned(&meter)?;
                return Ok(Some(NativeOwnedCoreState::from_parts(value, allocation)));
            }
            memo.collected
        };
        let value = self.measured_transaction(meter.clone(), move |namespace, work| {
            let (paths, _paths_allocation) =
                namespace.measured_record(T::FAMILY, &T::address(id), work)?;
            materialization_lock(paths, _paths_allocation, work, |paths| {
                let Some((wire, mut allocation)) =
                    measured_read_record::<T::Wire>(&paths, work)? else {
                    return Ok(None);
                };
                let value = T::from_wire(wire, work, &mut allocation)?;
                allocation.substitute(value.owned_bytes())?;
                let owned = NativeOwnedCoreState::from_parts(value, allocation);
                if lazy_digest(owned.as_state(), work)? != T::address(id) {
                    return Err(LocalCoreStateStoreError::Integrity);
                }
                Ok(Some(owned))
            })
        }).await?;
        if let Some(owned) = value {
            let mut memo = T::memo(&self.nodes);
            // Collection's generation guards against a late read resurrecting a memo.
            if memo.collected == collected {
                let (cached, allocation) = owned.as_state().clone_owned(&meter)?;
                memo.insert(id, NativeOwnedCoreState::from_parts(cached, allocation), &meter)?;
            }
            Ok(Some(owned))
        } else {
            Ok(None)
        }
    }

    async fn put_node<T: LazyNode>(
        &self,
        id: T::Id,
        value: T,
        meter: crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        let allocation = meter.retain_owned(value.owned_bytes())?;
        self.put_node_owned(id, NativeOwnedCoreState::from_parts(value, allocation), meter).await
    }

    async fn put_node_owned<T: LazyNode>(
        &self,
        id: T::Id,
        owned: NativeOwnedCoreState<T>,
        meter: crate::materializer::NativeMaterializationWork,
    ) -> Result<(), LocalCoreStateStoreError> {
        let (value, allocation) = owned.into_parts();
        let same_ledger = meter.shares_ledger(allocation.work());
        let owned = NativeOwnedCoreState::from_parts(value, allocation);
        if !same_ledger {
            return Err(LocalCoreStateStoreError::NativeLedgerMismatch);
        }
        let empty = lazy_digest(&T::default(), &meter)?;
        if lazy_digest(owned.as_state(), &meter)? != T::address(id) {
            return Err(LocalCoreStateStoreError::Integrity);
        }
        if T::address(id) == empty {
            return if owned.as_state() == &T::default() { Ok(()) }
                else { Err(LocalCoreStateStoreError::Integrity) };
        }
        {
            let mut memo = T::memo(&self.nodes);
            memo.touch(id, &meter)?;
            if let Some(existing) = memo.get(&id, &meter)? {
                meter.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
                return if existing == owned.as_state() { Ok(()) }
                    else { Err(LocalCoreStateStoreError::Integrity) };
            }
        }
        let stored = self.measured_transaction(meter.clone(), move |namespace, work| {
            let (paths, _paths_allocation) =
                namespace.measured_record(T::FAMILY, &T::address(id), work)?;
            materialization_lock(paths, _paths_allocation, work, |paths| {
                if let Some((wire, mut allocation)) = measured_read_record::<T::Wire>(&paths, work)? {
                    let existing = T::from_wire(wire, work, &mut allocation)?;
                    work.charge(crate::WorkCounters {
                        items_examined: 1,
                        ..Default::default()
                    })?;
                    if &existing != owned.as_state() {
                        return Err(LocalCoreStateStoreError::Integrity);
                    }
                } else {
                    measured_write_record(&paths, owned.as_state(), work)?;
                }
                Ok(owned)
            })
        }).await?;
        T::memo(&self.nodes).insert(id, stored, &meter)?;
        Ok(())
    }
}

impl LazyWorkspaceStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<LazyWorkspaceState>, Self::Error> {
        self.load_lazy_logged(workspace_id, unbounded_lazy_work()?).await
    }

    async fn compare_and_swap_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
    ) -> Result<bool, Self::Error> {
        self.swap_lazy_logged(workspace_id, expected_revision, replacement, unbounded_lazy_work()?).await
    }

    async fn load_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
    ) -> Result<Option<LazyOverlay>, Self::Error> {
        self.load_node(overlay, unbounded_lazy_work()?).await
    }

    async fn put_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
        value: LazyOverlay,
    ) -> Result<(), Self::Error> {
        self.put_node(overlay, value, unbounded_lazy_work()?).await
    }

    async fn load_lazy_shadow(
        &self,
        shadow: crate::LazyShadowId,
    ) -> Result<Option<crate::LazyShadow>, Self::Error> {
        self.load_node(shadow, unbounded_lazy_work()?).await
    }

    async fn put_lazy_shadow(
        &self,
        shadow: crate::LazyShadowId,
        value: crate::LazyShadow,
    ) -> Result<(), Self::Error> {
        self.put_node(shadow, value, unbounded_lazy_work()?).await
    }

    async fn load_lazy_workspace_measured(
        &self,
        workspace_id: WorkspaceId,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Option<LazyWorkspaceState>>, crate::LazyWorkspaceError> {
        let work = crate::materializer::NativeMaterializationWork::new(budget, cancellation)
            .map_err(lazy_native_failure)?;
        finish_lazy_receipt(self.load_lazy_logged(workspace_id, work.clone()).await, &work)
    }

    async fn compare_and_swap_lazy_workspace_measured(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<bool>, crate::LazyWorkspaceError> {
        let work = crate::materializer::NativeMaterializationWork::new(budget, cancellation)
            .map_err(lazy_native_failure)?;
        finish_lazy_receipt(
            self.swap_lazy_logged(workspace_id, expected_revision, replacement, work.clone()).await,
            &work,
        )
    }

    async fn load_lazy_overlay_measured(
        &self,
        overlay: LazyOverlayId,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Option<LazyOverlay>>, crate::LazyWorkspaceError> {
        let work = crate::materializer::NativeMaterializationWork::new(budget, cancellation)
            .map_err(lazy_native_failure)?;
        finish_lazy_receipt(self.load_node(overlay, work.clone()).await, &work)
    }

    async fn put_lazy_overlay_measured(
        &self,
        overlay: LazyOverlayId,
        value: LazyOverlay,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<()>, crate::LazyWorkspaceError> {
        let work = crate::materializer::NativeMaterializationWork::new(budget, cancellation)
            .map_err(lazy_native_failure)?;
        finish_lazy_receipt(self.put_node(overlay, value, work.clone()).await, &work)
    }

    async fn load_lazy_shadow_measured(
        &self,
        shadow: crate::LazyShadowId,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Option<crate::LazyShadow>>, crate::LazyWorkspaceError> {
        let work = crate::materializer::NativeMaterializationWork::new(budget, cancellation)
            .map_err(lazy_native_failure)?;
        finish_lazy_receipt(self.load_node(shadow, work.clone()).await, &work)
    }

    async fn put_lazy_shadow_measured(
        &self,
        shadow: crate::LazyShadowId,
        value: crate::LazyShadow,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<()>, crate::LazyWorkspaceError> {
        let work = crate::materializer::NativeMaterializationWork::new(budget, cancellation)
            .map_err(lazy_native_failure)?;
        finish_lazy_receipt(self.put_node(shadow, value, work.clone()).await, &work)
    }

    async fn load_lazy_workspace_with_native_work(
        &self,
        id: WorkspaceId,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<NativeOwnedCoreState<LazyWorkspaceState>>, crate::LazyWorkspaceError> {
        let state = self.load_lazy_logged(id, work.clone()).await.map_err(native_core_error)?;
        state.map(|state| {
            work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })
                .map_err(|error| native_core_error(error.into()))?;
            if state.schema_version != crate::lazy_workspace::LAZY_STATE_SCHEMA || state.workspace_id != id {
                return Err(crate::LazyWorkspaceError::NativeRecordIntegrity);
            }
            let allocation = work.claim(0).map_err(|error| native_core_error(error.into()))?;
            Ok(NativeOwnedCoreState::from_parts(state, allocation))
        }).transpose()
    }

    async fn compare_and_swap_lazy_workspace_with_native_work(
        &self,
        id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<bool, crate::LazyWorkspaceError> {
        work.charge(crate::WorkCounters { items_examined: 1, ..Default::default() })
            .map_err(|error| native_core_error(error.into()))?;
        if replacement.schema_version != crate::lazy_workspace::LAZY_STATE_SCHEMA || replacement.workspace_id != id {
            return Err(crate::LazyWorkspaceError::NativeRecordIntegrity);
        }
        self.swap_lazy_logged(id, expected_revision, replacement, work.clone()).await.map_err(native_core_error)
    }

    async fn load_lazy_overlay_with_native_work(
        &self,
        id: LazyOverlayId,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<NativeOwnedCoreState<LazyOverlay>>, crate::LazyWorkspaceError> {
        self.load_node_owned::<LazyOverlay>(id, work.clone()).await.map_err(native_core_error)
    }

    async fn put_lazy_overlay_with_native_work(
        &self,
        id: LazyOverlayId,
        value: NativeOwnedCoreState<LazyOverlay>,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), crate::LazyWorkspaceError> {
        self.put_node_owned(id, value, work.clone()).await.map_err(native_core_error)
    }

    async fn load_lazy_shadow_with_native_work(
        &self,
        id: crate::LazyShadowId,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<Option<NativeOwnedCoreState<crate::LazyShadow>>, crate::LazyWorkspaceError> {
        self.load_node_owned::<crate::LazyShadow>(id, work.clone()).await.map_err(native_core_error)
    }

    async fn put_lazy_shadow_with_native_work(
        &self,
        id: crate::LazyShadowId,
        value: NativeOwnedCoreState<crate::LazyShadow>,
        work: &crate::materializer::NativeMaterializationWork,
    ) -> Result<(), crate::LazyWorkspaceError> {
        self.put_node_owned(id, value, work.clone()).await.map_err(native_core_error)
    }
}

impl LocalCoreStateStore {
    /// Every file record a stored lazy shadow holds, which an object
    /// collection keeps: a lazy workspace may still resolve any of them.
    pub(crate) async fn lazy_shadow_records(
        &self,
    ) -> Result<Vec<crate::kernel::FileRecord>, LocalCoreStateStoreError> {
        self.transaction(|namespace| {
            let family = <crate::LazyShadow as LazyNode>::FAMILY;
            let entries = match std::fs::read_dir(namespace.family(family)) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(Vec::new());
                }
                Err(error) => return Err(error.into()),
            };
            let mut addresses = std::collections::BTreeSet::new();
            for entry in entries {
                let name = entry?.file_name();
                let Some(stem) = name.to_str().and_then(|name| name.strip_suffix(".json")) else {
                    continue;
                };
                let stem = stem.strip_suffix(".previous").unwrap_or(stem);
                if let Some(address) = hex::decode(stem)
                    .ok()
                    .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
                {
                    addresses.insert(address);
                }
            }
            let mut records = Vec::new();
            for address in addresses {
                if let Some(crate::LazyShadow::Node { record, .. }) =
                    load_content_addressed::<crate::LazyShadow>(namespace, family, address)?
                {
                    records.push(
                        crate::kernel::decode_file_record(&record)
                            .map_err(|_| LocalCoreStateStoreError::Integrity)?,
                    );
                }
            }
            Ok(records)
        })
        .await
    }

    /// Exact generations retained by original forks and completed selected imports.
    pub(crate) async fn lazy_retained_generations(
        &self,
    ) -> Result<Vec<(crate::GenerationId, crate::model::VolumeConfig)>, LocalCoreStateStoreError> {
        self.transaction(|root| {
            root.with_log(|log| {
                let mut workspaces = log.resident_keys(|key| match key {
                    RecordKey::LazyWorkspace(id) => Some(id),
                    _ => None,
                }).into_iter().collect::<BTreeSet<_>>();
                for stem in family_stems(root, LAZY_WORKSPACE_FAMILY)? {
                    if let Ok(bytes) = <[u8; 16]>::try_from(stem.as_slice()) {
                        workspaces.insert(WorkspaceId::from_bytes(bytes));
                    }
                }
                let mut generations = Vec::new();
                for workspace in workspaces {
                    let Some(state) = log.get::<LazyWorkspaceState>(root, workspace)? else {
                        continue;
                    };
                    if let Some(origin) = state.fork_origin {
                        generations.push((origin.snapshot.authored_generation, origin.config));
                    }
                    if let Some(completed) = state.completed_selected_import {
                        generations.extend([
                            (completed.snapshot.authored_generation, completed.config),
                            (completed.destination.authored_generation, completed.config),
                            (completed.source.authored_generation, completed.source_config),
                        ]);
                    }
                }
                Ok(generations)
            })
        }).await
    }
}

impl LocalCoreStateStore {
    /// Every workspace this store keeps a lineage, lazy binding, or
    /// Git-compatibility record for, with the parents those records name.
    pub(crate) async fn workspace_records(
        &self,
    ) -> Result<BTreeMap<WorkspaceId, Vec<WorkspaceId>>, LocalCoreStateStoreError> {
        self.transaction(|root| {
            root.with_log(|log| {
                let mut workspaces = log
                    .resident_keys(|key| match key {
                        RecordKey::Lineage(id) | RecordKey::LazyWorkspace(id) => Some(id),
                        _ => None,
                    })
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                for family in [LINEAGE_FAMILY, LAZY_WORKSPACE_FAMILY, GIT_COMPAT_NAMESPACE] {
                    for stem in family_stems(root, family)? {
                        if let Ok(bytes) = <[u8; 16]>::try_from(stem.as_slice()) {
                            workspaces.insert(WorkspaceId::from_bytes(bytes));
                        }
                    }
                }
                // A logged absence stays on disk as `null`.
                let mut recorded = BTreeMap::new();
                for workspace in workspaces {
                    let lineage = log.get::<WorkspaceLineageRecord>(root, workspace)?;
                    let lazy = log.get::<LazyWorkspaceState>(root, workspace)?;
                    let git = read_locked::<GitCompatState>(
                        &root.record(GIT_COMPAT_NAMESPACE, &workspace.into_bytes()),
                    )?;
                    if lineage.is_none() && lazy.is_none() && git.is_none() {
                        continue;
                    }
                    // A durable freeze is retention metadata, not evidence that
                    // a destination filesystem workspace was ever created.
                    if lineage.is_none() && git.is_none()
                        && lazy.as_ref().and_then(|state| state.fork_origin)
                            .is_some_and(|origin| origin.phase == crate::LazyForkPhase::Prepared)
                    {
                        continue;
                    }
                    let mut parents: Vec<_> = lineage
                        .and_then(|record| record.parent_workspace_id)
                        .into_iter()
                        .chain(lazy.as_ref().and_then(|state| state.fork_origin
                            .map(|origin| origin.snapshot.workspace_id)
                            .or(state.parent_workspace_id)))
                        .collect();
                    if let Some(completed) = lazy.and_then(|state| state.completed_selected_import) {
                        for parent in [
                            completed.snapshot.workspace_id,
                            completed.destination.workspace_id,
                            completed.source.workspace_id,
                        ] {
                            if parent != workspace && !parents.contains(&parent) {
                                parents.push(parent);
                            }
                        }
                    }
                    recorded.insert(workspace, parents);
                }
                Ok(recorded)
            })
        })
        .await
    }

    /// Forgets every record of a deleted workspace: its lineage, its lazy
    /// binding, and its Git-compatibility state.
    pub(crate) async fn forget_workspace(
        &self,
        workspace: WorkspaceId,
    ) -> Result<(), LocalCoreStateStoreError> {
        self.transaction(move |root| {
            root.with_log(|log| {
                let mut change = Change::default();
                change.put::<WorkspaceLineageRecord>(workspace, None);
                change.put::<LazyWorkspaceState>(workspace, None);
                log.commit(root, change)
            })?;
            // Unsynchronized: a removal a crash undoes is found again by the
            // next collection.
            let paths = root.record(GIT_COMPAT_NAMESPACE, &workspace.into_bytes());
            for path in [
                &paths.temporary,
                &paths.previous,
                &paths.current,
                &paths.lock,
            ] {
                remove_if_present(path)?;
            }
            Ok(())
        })
        .await
    }

    /// Removes every lazy overlay and shadow node no lazy workspace reaches,
    /// and returns how many it removed.
    ///
    /// It runs as one log transaction, so no workspace state changes while
    /// it marks and sweeps; a node put within the grace period is kept for
    /// the mutation still to swap it in.
    ///
    /// # Errors
    ///
    /// Fails, removing nothing, when a reachable node is missing or damaged.
    pub async fn collect_lazy_nodes(&self) -> Result<u64, LocalCoreStateStoreError> {
        let nodes = Arc::clone(&self.nodes);
        self.transaction(move |root| {
            root.with_log(|log| {
                let mut workspaces = log
                    .resident_keys(|key| match key {
                        RecordKey::LazyWorkspace(id) => Some(id),
                        _ => None,
                    })
                    .into_iter()
                    .collect::<BTreeSet<_>>();
                for stem in family_stems(root, LAZY_WORKSPACE_FAMILY)? {
                    if let Ok(bytes) = <[u8; 16]>::try_from(stem.as_slice()) {
                        workspaces.insert(WorkspaceId::from_bytes(bytes));
                    }
                }
                let mut overlays = Vec::new();
                let mut shadows = Vec::new();
                for workspace in workspaces {
                    let Some(state) = log.get::<LazyWorkspaceState>(root, workspace)? else {
                        continue;
                    };
                    overlays.push(state.overlay);
                    shadows.push(state.shadows);
                    if let Some(origin) = state.fork_origin {
                        overlays.push(origin.snapshot.overlay);
                        shadows.push(origin.snapshot.shadows);
                    }
                    if let Some(completed) = state.completed_selected_import {
                        overlays.extend([
                            completed.snapshot.overlay,
                            completed.destination.overlay,
                            completed.source.overlay,
                        ]);
                        shadows.extend([
                            completed.snapshot.shadows,
                            completed.destination.shadows,
                            completed.source.shadows,
                        ]);
                    }
                    if let Some(pending) = state.pending_remove {
                        overlays.extend([pending.prior_overlay, pending.prepared_overlay]);
                        shadows.push(pending.prior_shadows);
                    }
                }
                let overlays = reachable_nodes::<LazyOverlay>(root, overlays, |node| match node {
                    LazyOverlay::Node { left, right, .. } => {
                        vec![left.into_bytes(), right.into_bytes()]
                    }
                    LazyOverlay::Empty => Vec::new(),
                })?;
                let shadows =
                    reachable_nodes::<crate::LazyShadow>(root, shadows, |node| match node {
                        crate::LazyShadow::Node { left, right, .. } => {
                            vec![left.into_bytes(), right.into_bytes()]
                        }
                        crate::LazyShadow::Empty => Vec::new(),
                    })?;
                Ok(
                    sweep_nodes::<LazyOverlay>(root, &nodes, &overlays, LazyOverlayId::from_bytes)?
                        + sweep_nodes::<crate::LazyShadow>(
                            root,
                            &nodes,
                            &shadows,
                            crate::LazyShadowId::from_bytes,
                        )?,
                )
            })
        })
        .await
    }
}

/// The distinct record keys one family holds, whatever recovery copies
/// accompany them.
fn family_stems(
    root: &Namespace,
    family: &str,
) -> Result<BTreeSet<Vec<u8>>, LocalCoreStateStoreError> {
    let entries = match std::fs::read_dir(root.family(family)) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeSet::new()),
        Err(error) => return Err(error.into()),
    };
    let mut stems = BTreeSet::new();
    for entry in entries {
        let name = entry?.file_name();
        let Some(name) = name.to_str() else { continue };
        let stem = name
            .strip_suffix(".previous.json")
            .or_else(|| name.strip_suffix(".next.json"))
            .or_else(|| name.strip_suffix(".json"))
            .or_else(|| name.strip_suffix(".lock"));
        if let Some(bytes) = stem.and_then(|stem| hex::decode(stem).ok()) {
            stems.insert(bytes);
        }
    }
    Ok(stems)
}

/// Every stored node of one family the `roots` reach.
fn reachable_nodes<T: LazyNode>(
    root: &Namespace,
    roots: Vec<T::Id>,
    children: impl Fn(T) -> Vec<[u8; 32]>,
) -> Result<std::collections::HashSet<[u8; 32]>, LocalCoreStateStoreError> {
    let empty = content_address(&T::default())?;
    let mut reachable = std::collections::HashSet::new();
    let mut pending = roots.into_iter().map(T::address).collect::<Vec<_>>();
    while let Some(address) = pending.pop() {
        if address == empty || !reachable.insert(address) {
            continue;
        }
        let node = load_content_addressed::<T>(root, T::FAMILY, address)?
            .ok_or(LocalCoreStateStoreError::Integrity)?;
        pending.extend(children(node));
    }
    Ok(reachable)
}

/// Removes every stored node of one family outside `reachable` that was not
/// put within the grace period.
fn sweep_nodes<T: LazyNode>(
    root: &Namespace,
    nodes: &NodeMemo,
    reachable: &std::collections::HashSet<[u8; 32]>,
    id: impl Fn([u8; 32]) -> T::Id,
) -> Result<u64, LocalCoreStateStoreError> {
    let mut removed = 0;
    for stem in family_stems(root, T::FAMILY)? {
        let Ok(address) = <[u8; 32]>::try_from(stem.as_slice()) else {
            continue;
        };
        if reachable.contains(&address) {
            continue;
        }
        // Decided and removed under the memo lock a put records under.
        let mut memo = T::memo(nodes);
        if !memo.release(&id(address)) {
            continue;
        }
        // Unsynchronized: a removal a crash undoes leaves only garbage.
        let paths = root.record(T::FAMILY, &address);
        for path in [
            &paths.temporary,
            &paths.previous,
            &paths.current,
            &paths.lock,
        ] {
            remove_if_present(path)?;
        }
        removed += 1;
    }
    Ok(removed)
}

/// Loads an immutable record whose key is the BLAKE3 digest of its encoding.
fn load_content_addressed<T: DeserializeOwned + Serialize>(
    namespace: &Namespace,
    family: &str,
    address: [u8; 32],
) -> Result<Option<T>, LocalCoreStateStoreError> {
    let Some(value) = read_locked(&namespace.record(family, &address))? else {
        return Ok(None);
    };
    verify_content_address(address, &value)?;
    Ok(Some(value))
}

/// Stores an immutable content-addressed record, idempotently rejecting
/// digest mismatches.
fn put_content_addressed<T: DeserializeOwned + Serialize + PartialEq>(
    namespace: &Namespace,
    family: &str,
    address: [u8; 32],
    value: &T,
) -> Result<(), LocalCoreStateStoreError> {
    verify_content_address(address, value)?;
    let paths = namespace.record(family, &address);
    with_lock(&paths, || {
        if let Some(existing) = read_recoverable::<T>(&paths)? {
            return if &existing == value {
                Ok(())
            } else {
                Err(LocalCoreStateStoreError::Integrity)
            };
        }
        write_journaled(&paths, value)
    })
}

fn verify_content_address<T: Serialize>(
    address: [u8; 32],
    value: &T,
) -> Result<(), LocalCoreStateStoreError> {
    if content_address(value)? != address {
        return Err(LocalCoreStateStoreError::Integrity);
    }
    Ok(())
}

fn content_address<T: Serialize>(value: &T) -> Result<[u8; 32], LocalCoreStateStoreError> {
    Ok(*blake3::hash(&serde_json::to_vec(value)?).as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Digest, WorkspaceContextRoot, WorkspaceContextState, WorkspaceName, WorkspaceRootId,
    };

    fn finite_materialization_budget() -> crate::WorkBudget {
        let mut encoded = serde_json::to_value(crate::WorkBudget::UNBOUNDED).expect("budget fields");
        for value in encoded.as_object_mut().expect("budget object").values_mut() {
            *value = serde_json::Value::from(100_000_000_u64);
        }
        serde_json::from_value(encoded).expect("finite native budget")
    }

    #[test]
    fn materialization_writer_rejects_bytes_before_any_native_write_attempt() {
        let temporary = tempfile::tempdir().expect("temporary state");
        let path = temporary.path().join("record");
        let file = File::create(&path).expect("record fixture");
        let mut budget = finite_materialization_budget();
        budget.object_bytes_written = 0;
        budget.bytes_encoded = 0;
        let work = crate::materializer::NativeMaterializationWork::new(budget, &crate::CancellationToken::new()).expect("native meter");
        let mut writer = MaterializationWriter { file, work: &work, encoded: true };
        writer.write(b"not admitted").expect_err("byte admission");
        assert_eq!(work.receipt().backend_write_operations, 0);
        assert_eq!(work.receipt().object_bytes_written, 0);
        assert_eq!(std::fs::metadata(path).expect("untouched record").len(), 0);
        work.receipt().verify(budget).expect("failed writer prefix");
    }

    #[test]
    fn materialization_json_releases_parser_scratch_but_retains_actual_dto_buffers() {
        #[derive(Deserialize)]
        struct Image { value: String, values: Vec<u64> }
        let budget = finite_materialization_budget();
        let work = crate::materializer::NativeMaterializationWork::new(budget, &crate::CancellationToken::new()).expect("native meter");
        let original_live = work.live_allocation_bytes();
        let (image, custody) = measured_decode_slice::<Image>(br#"{"value":"x\uD83D\uDE00z","values":[1,2,3],"ignored":"\uD83D\uDE00"}"#, &work).expect("real unicode parser");
        assert_eq!(image.value, "x\u{1f600}z");
        assert_eq!(image.values, [1, 2, 3]);
        assert_eq!(custody.bytes(), image.value.capacity() as u64 + (image.values.capacity() * std::mem::size_of::<u64>()) as u64);
        assert_eq!(work.live_allocation_bytes(), original_live + custody.bytes());
        assert!(work.receipt().bytes_decoded > 0);
        assert!(work.receipt().allocation_operations > 1);
        drop(image);
        drop(custody);
        assert_eq!(work.live_allocation_bytes(), original_live);
        work.receipt().verify(budget).expect("unicode parser receipt");
    }

    #[test]
    fn record_families_keep_their_durable_names() {
        assert_eq!(
            [
                GIT_COMPAT_NAMESPACE,
                LAZY_WORKSPACE_FAMILY,
                MATERIALIZATION_FAMILY,
                MULTI_ROOT_PARENT_CLAIM_FAMILY,
                MULTI_ROOT_PUBLICATION_FAMILY,
                WORKSPACE_CONTEXT_FAMILY,
                CONTEXT_CHILDREN_FAMILY,
                CONTEXT_CHILDREN_INDEX_FAMILY,
                CONTEXT_CHILD_COUNT_FAMILY,
                LINEAGE_FAMILY,
                CORE_LOG_FAMILY,
            ],
            [
                "git-compat-v9",
                "lazy-workspaces",
                "materialization",
                "multi-root-parent-claims",
                "multi-root-publications",
                "workspace-contexts-v2",
                "workspace-context-children-v1",
                "workspace-context-children-index-v1",
                "workspace-context-child-counts-v1",
                "lineage",
                "core-state-log-v1",
            ]
        );
    }

    #[test]
    fn local_transactions_work_without_a_tokio_runtime() {
        let directory = tempfile::tempdir().expect("local state directory");
        let store = LocalCoreStateStore::new(directory.path());
        let result = futures::executor::block_on(WorkspaceLineageStore::load(&store, workspace()));
        assert!(result.expect("runtime-independent transaction").is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn held_record_lock_is_bounded_without_stalling_unrelated_records() {
        let directory = tempfile::tempdir().expect("local state directory");
        let store = LocalCoreStateStore::new(directory.path());
        let held_id = workspace();
        let other_id = WorkspaceId::from_bytes([0x55; 16]);
        let namespace = store.namespace().expect("admitted namespace");
        let paths = namespace.record(GIT_COMPAT_NAMESPACE, &held_id.into_bytes());
        std::fs::create_dir_all(&paths.directory).expect("lock directory");
        let held = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&paths.lock)
            .expect("held record lock");
        held.lock_exclusive().expect("hold record lock");

        let contender = tokio::spawn(async move { GitCompatStore::load(&store, held_id).await });
        let unrelated = LocalCoreStateStore::new(directory.path());
        tokio::time::timeout(
            Duration::from_secs(2),
            GitCompatStore::load(&unrelated, other_id),
        )
        .await
        .expect("unrelated record must not stall")
        .expect("unrelated record load");
        assert!(matches!(
            contender.await.expect("contender task"),
            Err(LocalCoreStateStoreError::LockTimeout(path)) if path == paths.lock
        ));
        FileExt::unlock(&held).expect("release held lock");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn admitted_local_transaction_does_not_block_executor_or_stop_on_observer_drop() {
        use std::sync::mpsc;
        use std::time::{Duration, Instant};

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = mpsc::channel();
        let observer = tokio::spawn(async move {
            run_local_transaction(move || {
                entered_tx.send(()).expect("test observer is alive");
                release_rx.recv().expect("test permits completion");
                finished_tx
                    .send(())
                    .expect("test completion observer is alive");
                Ok(())
            })
            .await
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while entered_rx.try_recv().is_err() {
            assert!(Instant::now() < deadline, "transaction did not enter");
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        observer.abort();
        release_tx.send(()).expect("transaction remains admitted");
        while finished_rx.try_recv().is_err() {
            assert!(Instant::now() < deadline, "admitted transaction was lost");
            tokio::task::yield_now().await;
        }
    }

    fn context_paths(namespace: &Namespace, id: WorkspaceContextId) -> RecordPaths<'_> {
        RecordKey::Context(id).paths(namespace)
    }

    fn children_paths(namespace: &Namespace, id: WorkspaceContextId) -> RecordPaths<'_> {
        RecordKey::Children(id).paths(namespace)
    }

    fn count_paths(namespace: &Namespace, id: WorkspaceContextId) -> RecordPaths<'_> {
        RecordKey::ChildCount(id).paths(namespace)
    }

    fn workspace() -> WorkspaceId {
        WorkspaceId::derive(
            [4; 16],
            &WorkspaceName::new("durable").expect("valid workspace"),
        )
    }

    fn workspace_context(
        directory: &Path,
        context_id: WorkspaceContextId,
        revision: u64,
        state: WorkspaceContextState,
    ) -> WorkspaceContext {
        let root_id = WorkspaceRootId::from_bytes([9; 16]);
        WorkspaceContext {
            version: 1,
            revision,
            context_id,
            parent_context_id: None,
            roots: [(
                root_id,
                WorkspaceContextRoot {
                    root_id,
                    source_path: directory.to_path_buf(),
                    workspace_id: workspace(),
                    workspace_name: "durable".to_owned(),
                    parent_workspace_id: None,
                    mount_path: None,
                },
            )]
            .into_iter()
            .collect(),
            state,
        }
    }

    #[tokio::test]
    async fn corrupt_context_identity_cannot_enter_listing_cas_or_root_removal() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let namespace = store.namespace().expect("admitted namespace");
        let target_id = WorkspaceContextId::from_bytes([41; 16]);
        let stored_id = WorkspaceContextId::from_bytes([42; 16]);
        let removed_root = WorkspaceRootId::from_bytes([10; 16]);
        let mut target = workspace_context(
            directory.path(),
            target_id,
            1,
            WorkspaceContextState::Active,
        );
        let mut second = target.roots.values().next().expect("first root").clone();
        second.root_id = removed_root;
        second.source_path = directory.path().join("second");
        second.workspace_id = WorkspaceId::from_bytes([10; 16]);
        second.workspace_name = "second".to_owned();
        target.roots.insert(removed_root, second);
        let corrupt = workspace_context(
            directory.path(),
            WorkspaceContextId::from_bytes([99; 16]),
            1,
            WorkspaceContextState::Active,
        );
        let mut files = Vec::new();
        for (id, record) in [(target_id, target), (stored_id, corrupt)] {
            let paths = context_paths(&namespace, id);
            std::fs::create_dir_all(&paths.directory).expect("record directory");
            write_journaled(&paths, &record).expect("persist fixture");
            files.push((
                paths.current.clone(),
                std::fs::read(&paths.current).expect("original bytes"),
            ));
        }
        let listed = WorkspaceContextStore::list(&store).await;
        let swapped = WorkspaceContextStore::compare_and_swap_many(
            &store,
            BTreeMap::from([(target_id, 1), (stored_id, 1)]),
            Vec::new(),
            true,
        )
        .await;
        let registry = crate::WorkspaceContextRegistry::new(store);
        let removed = tokio::time::timeout(
            Duration::from_secs(2),
            registry.remove_root(target_id, removed_root),
        )
        .await;
        for (path, before) in files {
            assert_eq!(std::fs::read(path).expect("unchanged bytes"), before);
        }
        assert!(
            matches!(
                listed.as_ref(),
                Err(LocalCoreStateStoreError::ContextTransaction(_))
            ) && matches!(
                swapped.as_ref(),
                Err(LocalCoreStateStoreError::ContextTransaction(_))
            ) && matches!(
                removed.as_ref(),
                Ok(Err(crate::WorkspaceContextError::Store(
                    LocalCoreStateStoreError::ContextTransaction(_)
                )))
            ),
            "corrupt discovery must fail closed, not retry: list={listed:?}, cas={swapped:?}, removal={removed:?}"
        );
    }

    #[tokio::test]
    async fn state_survives_reopen() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let namespace = store.namespace().expect("admitted namespace");
        let state = WorkspaceLineageRecord {
            version: 1,
            revision: 1,
            ready: true,
            workspace_id: workspace(),
            workspace_name: "durable".to_owned(),
            parent_workspace_id: None,
            parent_workspace_name: None,
            fork_generation: crate::GenerationId::new(Digest::from_bytes([3; 32])),
            initial_generation: crate::GenerationId::new(Digest::from_bytes([3; 32])),
        };
        assert!(
            WorkspaceLineageStore::compare_and_swap(&store, workspace(), 0, state.clone())
                .await
                .expect("initial write")
        );
        // A shared commit is written back before its lock is released.
        assert!(
            std::fs::read(core_log_paths(&namespace).current)
                .expect("core log")
                .is_empty()
        );
        WorkspaceLineageStore::load(&store, workspace())
            .await
            .expect("write the record back");
        let current = namespace
            .record("lineage", &workspace().into_bytes())
            .current;
        let reopened = LocalCoreStateStore::new(directory.path());
        assert_eq!(
            WorkspaceLineageStore::load(&reopened, workspace())
                .await
                .expect("reopen"),
            Some(state)
        );
        assert!(
            read_json::<WorkspaceLineageRecord>(&current)
                .expect("written-back record")
                .is_some()
        );
    }

    /// One non-empty overlay node and its address.
    fn overlay_node(path: &str) -> (LazyOverlayId, LazyOverlay) {
        let node: LazyOverlay = serde_json::from_value(serde_json::json!({
            "Node": {
                "path": path,
                "priority": 0,
                "change": {"Tombstone": {"removed": null}},
                "left": vec![0_u8; 32],
                "right": vec![0_u8; 32]
            }
        }))
        .expect("overlay node");
        let id = LazyOverlayId::from_bytes(content_address(&node).expect("overlay address"));
        (id, node)
    }

    #[tokio::test]
    async fn lazy_overlay_load_rejects_content_address_mismatch() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let (overlay, node) = overlay_node("/original");
        store
            .put_lazy_overlay(overlay, node)
            .await
            .expect("persist overlay");
        let (_, tampered) = overlay_node("/tampered");
        let paths = store
            .namespace()
            .expect("admitted namespace")
            .record("lazy-overlays", &overlay.into_bytes())
            .current;
        std::fs::write(
            &paths,
            serde_json::to_vec(&tampered).expect("serialize tampered overlay"),
        )
        .expect("tamper overlay");
        assert!(matches!(
            LocalCoreStateStore::new(directory.path())
                .load_lazy_overlay(overlay)
                .await,
            Err(LocalCoreStateStoreError::Integrity)
        ));
    }

    #[tokio::test]
    async fn immutable_nodes_are_served_from_memory_after_first_use() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::open_owned(directory.path()).expect("owner");
        let (overlay, node) = overlay_node("/memoized");
        store
            .put_lazy_overlay(overlay, node.clone())
            .await
            .expect("put overlay");
        std::fs::remove_dir_all(directory.path().join("lazy-overlays")).expect("drop overlays");
        let clone = store.clone();
        assert_eq!(
            clone
                .load_lazy_overlay(overlay)
                .await
                .expect("memoized overlay"),
            Some(node)
        );
        let (_, different) = overlay_node("/different");
        assert!(
            matches!(
                clone.put_lazy_overlay(overlay, different).await,
                Err(LocalCoreStateStoreError::Integrity)
            ),
            "a memoized address still rejects a different value"
        );
    }

    #[tokio::test]
    async fn empty_nodes_are_implied_without_being_stored() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let overlay = LazyOverlayId::from_bytes(
            content_address(&LazyOverlay::default()).expect("empty overlay address"),
        );
        let shadow: crate::LazyShadowId = serde_json::from_value(serde_json::json!(
            content_address(&crate::LazyShadow::default()).expect("empty shadow address")
        ))
        .expect("empty shadow id");
        store
            .put_lazy_overlay(overlay, LazyOverlay::default())
            .await
            .expect("put empty overlay");
        store
            .put_lazy_shadow(shadow, crate::LazyShadow::default())
            .await
            .expect("put empty shadow");
        assert_eq!(
            std::fs::read_dir(directory.path())
                .expect("store root")
                .count(),
            0,
            "empty nodes are never written"
        );
        assert_eq!(
            store
                .load_lazy_overlay(overlay)
                .await
                .expect("empty overlay"),
            Some(LazyOverlay::default())
        );
        assert_eq!(
            store.load_lazy_shadow(shadow).await.expect("empty shadow"),
            Some(crate::LazyShadow::default())
        );
    }

    #[test]
    fn node_memo_rotation_keeps_recently_used_entries() {
        let mut generations = Generations::new();
        let work = unbounded_lazy_work().expect("unbounded memo meter");
        for key in 0..NODE_CACHE_GENERATION {
            let owned = NativeOwnedCoreState::from_parts(key, work.claim(0).expect("scalar memo custody"));
            generations.insert(key, owned, &work).expect("memo insert");
        }
        let owned = NativeOwnedCoreState::from_parts(NODE_CACHE_GENERATION, work.claim(0).expect("scalar memo custody"));
        generations.insert(NODE_CACHE_GENERATION, owned, &work)
            .expect("memo rotation");
        assert_eq!(generations.get(&0, &work).expect("memo lookup"), Some(&0));
        for key in NODE_CACHE_GENERATION + 1..2 * NODE_CACHE_GENERATION {
            let owned = NativeOwnedCoreState::from_parts(key, work.claim(0).expect("scalar memo custody"));
            generations.insert(key, owned, &work).expect("memo insert");
        }
        assert_eq!(
            generations.get(&0, &work).expect("memo lookup"),
            Some(&0),
            "a promoted entry survives rotation"
        );
        assert_eq!(generations.get(&1, &work).expect("memo lookup"),
            None, "an unused entry is evicted");
    }

    #[tokio::test]
    async fn workspace_contexts_are_durable_and_discoverable() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let context_id = WorkspaceContextId::from_bytes([8; 16]);
        let root_id = WorkspaceRootId::from_bytes([9; 16]);
        let context = WorkspaceContext {
            version: 1,
            revision: 1,
            context_id,
            parent_context_id: None,
            roots: [(
                root_id,
                WorkspaceContextRoot {
                    root_id,
                    source_path: directory.path().to_path_buf(),
                    workspace_id: workspace(),
                    workspace_name: "durable".to_owned(),
                    parent_workspace_id: None,
                    mount_path: None,
                },
            )]
            .into_iter()
            .collect(),
            state: WorkspaceContextState::Active,
        };
        assert!(
            WorkspaceContextStore::compare_and_swap(&store, context_id, 0, context.clone())
                .await
                .expect("write context")
        );
        let reopened = LocalCoreStateStore::new(directory.path());
        assert_eq!(
            WorkspaceContextStore::list(&reopened)
                .await
                .expect("list contexts"),
            [context]
        );
    }

    #[tokio::test]
    async fn context_discard_reads_only_the_bounded_subtree() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let namespace = store.namespace().expect("admitted namespace");
        let parent_id = WorkspaceContextId::from_bytes([20; 16]);
        let child_id = WorkspaceContextId::from_bytes([21; 16]);
        let unrelated_id = WorkspaceContextId::from_bytes([22; 16]);
        let parent = workspace_context(
            directory.path(),
            parent_id,
            1,
            WorkspaceContextState::Active,
        );
        let mut child =
            workspace_context(directory.path(), child_id, 1, WorkspaceContextState::Active);
        child.parent_context_id = Some(parent_id);
        let unrelated = workspace_context(
            directory.path(),
            unrelated_id,
            1,
            WorkspaceContextState::Active,
        );
        for context in [parent, child, unrelated] {
            assert!(
                WorkspaceContextStore::compare_and_swap(&store, context.context_id, 0, context,)
                    .await
                    .expect("register context")
            );
        }

        std::fs::write(context_paths(&namespace, unrelated_id).current, b"not-json")
            .expect("corrupt unrelated record");

        assert_eq!(
            WorkspaceContextStore::discard_subtree(&store, parent_id, child_id, 1)
                .await
                .expect("discard bounded subtree"),
            WorkspaceContextDiscardOutcome::Discarded(vec![child_id])
        );
        assert_eq!(
            WorkspaceContextStore::load(&store, child_id)
                .await
                .expect("load discarded child")
                .expect("child remains durable")
                .state,
            WorkspaceContextState::Discarded
        );
    }

    #[tokio::test]
    async fn context_discard_checks_child_count_before_reading_bucket() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let namespace = store.namespace().expect("admitted namespace");
        let parent_id = WorkspaceContextId::from_bytes([25; 16]);
        let child_id = WorkspaceContextId::from_bytes([26; 16]);
        let parent = workspace_context(
            directory.path(),
            parent_id,
            1,
            WorkspaceContextState::Active,
        );
        let mut child =
            workspace_context(directory.path(), child_id, 1, WorkspaceContextState::Active);
        child.parent_context_id = Some(parent_id);
        for context in [parent, child] {
            assert!(
                WorkspaceContextStore::compare_and_swap(&store, context.context_id, 0, context,)
                    .await
                    .expect("register context")
            );
        }
        let count = count_paths(&namespace, child_id);
        std::fs::create_dir_all(&count.directory).expect("child count directory");
        write_journaled(&count, &10_000_u64).expect("write oversized child count");
        let bucket = children_paths(&namespace, child_id);
        std::fs::create_dir_all(&bucket.directory).expect("child bucket directory");
        std::fs::write(&bucket.current, b"not-json").expect("corrupt oversized child bucket");

        assert_eq!(
            WorkspaceContextStore::discard_subtree(&store, parent_id, child_id, 1)
                .await
                .expect("reject before child bucket read"),
            WorkspaceContextDiscardOutcome::TraversalLimit
        );
    }

    #[tokio::test]
    async fn context_discard_bounds_bucket_bytes_before_deserialization() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let namespace = store.namespace().expect("admitted namespace");
        let parent_id = WorkspaceContextId::from_bytes([30; 16]);
        let child_id = WorkspaceContextId::from_bytes([31; 16]);
        let parent = workspace_context(
            directory.path(),
            parent_id,
            1,
            WorkspaceContextState::Active,
        );
        let mut child =
            workspace_context(directory.path(), child_id, 1, WorkspaceContextState::Active);
        child.parent_context_id = Some(parent_id);
        for context in [parent, child] {
            assert!(
                WorkspaceContextStore::compare_and_swap(&store, context.context_id, 0, context,)
                    .await
                    .expect("register context")
            );
        }
        let count = count_paths(&namespace, child_id);
        std::fs::create_dir_all(&count.directory).expect("child count directory");
        write_journaled(&count, &1_u64).expect("write understated child count");
        let bucket = children_paths(&namespace, child_id);
        std::fs::create_dir_all(&bucket.directory).expect("child bucket directory");
        std::fs::write(&bucket.current, vec![b' '; 1_000_000])
            .expect("write oversized child bucket");

        assert!(matches!(
            WorkspaceContextStore::discard_subtree(&store, parent_id, child_id, 2).await,
            Err(LocalCoreStateStoreError::ContextTransaction(message))
                if message.contains("exceeds its declared bound")
        ));
    }

    #[tokio::test]
    async fn context_discard_never_rebuilds_a_missing_index() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let namespace = store.namespace().expect("admitted namespace");
        let parent_id = WorkspaceContextId::from_bytes([27; 16]);
        let child_id = WorkspaceContextId::from_bytes([28; 16]);
        let unrelated_id = WorkspaceContextId::from_bytes([29; 16]);
        let mut child =
            workspace_context(directory.path(), child_id, 1, WorkspaceContextState::Active);
        child.parent_context_id = Some(parent_id);
        for context in [
            workspace_context(
                directory.path(),
                parent_id,
                1,
                WorkspaceContextState::Active,
            ),
            child,
            workspace_context(
                directory.path(),
                unrelated_id,
                1,
                WorkspaceContextState::Active,
            ),
        ] {
            let paths = context_paths(&namespace, context.context_id);
            std::fs::create_dir_all(&paths.directory).expect("context directory");
            write_journaled(&paths, &context).expect("write context");
        }
        std::fs::write(context_paths(&namespace, unrelated_id).current, b"not-json")
            .expect("corrupt unrelated record");

        assert_eq!(
            WorkspaceContextStore::discard_subtree(&store, parent_id, child_id, 1)
                .await
                .expect("fail closed without rebuilding index"),
            WorkspaceContextDiscardOutcome::IncompatibleState
        );
    }

    #[tokio::test]
    async fn git_compatibility_delete_is_revision_fenced_and_durable() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        assert!(
            GitCompatStore::load(&store, workspace())
                .await
                .expect("absent Git state")
                .is_none()
        );
        let mut state = GitCompatState::new("main", workspace());
        state.revision = 1;
        assert!(
            GitCompatStore::compare_and_swap(&store, workspace(), 0, state)
                .await
                .expect("write Git state")
        );
        assert!(
            !GitCompatStore::compare_and_delete(&store, workspace(), 0)
                .await
                .expect("reject stale delete")
        );
        assert!(
            GitCompatStore::compare_and_delete(&store, workspace(), 1)
                .await
                .expect("delete Git state")
        );
        let reopened = LocalCoreStateStore::new(directory.path());
        assert!(
            GitCompatStore::load(&reopened, workspace())
                .await
                .expect("load deleted Git state")
                .is_none()
        );
    }

    #[tokio::test]
    async fn materialization_operations_are_discoverable_and_terminal_cleanup_is_safe() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let operation_id = OperationId::new();
        let journal = MaterializationJournal {
            version: 2,
            revision: 1,
            root_identity: None,
            preparation_identity: None,
            preparation_progress: None,
            work: crate::WorkCounters::default(),
            plan: crate::MaterializationPlan {
                operation_id,
                intent: crate::MaterializationIntent::GenerationTransition {
                    from: crate::GenerationId::new(Digest::from_bytes([1; 32])),
                    to: crate::GenerationId::new(Digest::from_bytes([2; 32])),
                },
                edits: Vec::new(),
            },
            preimages: Vec::new(),
            phase: crate::MaterializationPhase::Prepared,
            applied: 0,
            restored: 0,
        };
        assert!(
            MaterializationJournalStore::compare_and_swap(&store, operation_id, 0, crate::materializer::OwnedMaterializationJournal {
                journal: journal.clone(),
                #[cfg(not(target_arch = "wasm32"))]
                custody: None,
            })
                .await
                .expect("write journal").0
        );
        assert_eq!(
            store
                .materialization_operations_async()
                .await
                .expect("list materializations"),
            [operation_id]
        );
        assert!(matches!(
            store.remove_materialization_async(operation_id).await,
            Err(LocalCoreStateStoreError::MaterializationInProgress)
        ));
        let mut terminal = journal;
        terminal.revision = 2;
        terminal.phase = crate::MaterializationPhase::Applied;
        assert!(
            MaterializationJournalStore::compare_and_swap(&store, operation_id, 1, crate::materializer::OwnedMaterializationJournal {
                journal: terminal,
                #[cfg(not(target_arch = "wasm32"))]
                custody: None,
            })
                .await
                .expect("finish journal").0
        );
        store
            .remove_materialization_async(operation_id)
            .await
            .expect("remove terminal journal");
        assert!(
            store
                .materialization_operations_async()
                .await
                .expect("list cleaned materializations")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn previous_only_materialization_journal_is_discoverable_and_recovered() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let namespace = store.namespace().expect("admitted namespace");
        let operation_id = OperationId::from_bytes([0x73; 16]);
        let journal = MaterializationJournal {
            version: 2,
            revision: 1,
            root_identity: None,
            preparation_identity: None,
            preparation_progress: None,
            work: crate::WorkCounters::default(),
            plan: crate::MaterializationPlan {
                operation_id,
                intent: crate::MaterializationIntent::GenerationTransition {
                    from: crate::GenerationId::new(Digest::from_bytes([1; 32])),
                    to: crate::GenerationId::new(Digest::from_bytes([2; 32])),
                },
                edits: vec![crate::MaterializationEdit::Install {
                    path: "file.txt".to_owned(), image: vec![1, 2, 3],
                }],
            },
            preimages: vec![crate::MaterializationPreimage { image: vec![4, 5, 6] }],
            phase: crate::MaterializationPhase::Prepared,
            applied: 0,
            restored: 0,
        };
        let paths = namespace.record("materialization", &operation_id.into_bytes());
        std::fs::create_dir_all(&paths.directory).expect("journal directory");
        std::fs::write(
            &paths.previous,
            serde_json::to_vec(&journal).expect("serialize journal"),
        )
        .expect("write previous journal");

        assert_eq!(
            store
                .materialization_operations_async()
                .await
                .expect("discover journal"),
            [operation_id]
        );
        assert_eq!(
            MaterializationJournalStore::load(&store, operation_id)
                .await
                .expect("recover journal")
                .map(|record| record.journal),
            Some(journal)
        );
        assert!(paths.current.exists());
        #[cfg(not(target_arch = "wasm32"))]
        {
            let mut value = serde_json::to_value(crate::WorkBudget::UNBOUNDED).expect("canonical budget fixture");
            for field in value.as_object_mut().expect("budget fields").values_mut() {
                *field = serde_json::Value::from(100_000_000_u64);
            }
            let mut budget: crate::WorkBudget = serde_json::from_value(value).expect("finite budget");
            budget.source_bytes_read = std::fs::metadata(&paths.current).expect("actual durable record").len();
            let cancellation = crate::CancellationToken::new();
            let work = crate::materializer::NativeMaterializationWork::new(budget, &cancellation).expect("original finite meter");
            let baseline = work.live_allocation_bytes();
            let record = MaterializationJournalStore::load(&store.materialization_with_work(work.clone()), operation_id)
                .await.expect("bounded original durable decoder").expect("original journal");
            let allocation = record.custody.as_ref().expect("actual decoded payload custody");
            assert!(allocation.bytes() > 0, "actual path and image buffers retain decoder custody");
            assert!(allocation.work().shares_ledger(&work));
            assert_eq!(work.live_allocation_bytes(), baseline + allocation.bytes());
            assert_eq!(work.receipt().source_bytes_read, budget.source_bytes_read);
            work.receipt().verify(budget).expect("exact record read and all finite work");
            cancellation.cancel();
            let prefix = work.receipt();
            drop(record);
            assert_eq!(work.live_allocation_bytes(), baseline);
            assert_eq!(work.receipt(), prefix, "original decoded guard retires without a second admission or cancellation check");
        }
    }

    #[tokio::test]
    async fn accepted_materialization_completion_survives_cancellation_without_new_allocation() {
        use crate::materializer::MaterializationActionCompletion as _;
        let directory = tempfile::tempdir().expect("real journal namespace");
        let root = directory.path().join("checkout");
        std::fs::create_dir(&root).expect("original checkout allocation");
        let effect = root.join("file.txt");
        std::fs::write(&effect, b"before").expect("actual staged action fixture");
        let state_root = directory.path().join("state");
        let store = LocalCoreStateStore::new(&state_root);
        let cancellation = crate::CancellationToken::new();
        let mut value = serde_json::to_value(crate::WorkBudget::UNBOUNDED).expect("canonical budget");
        for field in value.as_object_mut().expect("budget fields").values_mut() {
            *field = serde_json::Value::from(100_000_000_u64);
        }
        let budget: crate::WorkBudget = serde_json::from_value(value).expect("all finite");
        let work = crate::materializer::NativeMaterializationWork::new(budget, &cancellation).expect("original finite work");
        let held = crate::native_host::HostRoot::open_materialization_measured(&root, &work).expect("actual root identity");
        let operation = OperationId::new();
        let journal = MaterializationJournal {
            version: 2, revision: 1, root_identity: Some(held.identity().to_bytes()),
            preparation_identity: None, preparation_progress: None, work: crate::WorkCounters::UNCHARGED,
            plan: crate::MaterializationPlan {
                operation_id: operation,
                intent: crate::MaterializationIntent::GenerationTransition {
                    from: crate::GenerationId::new(Digest::from_bytes([1; 32])),
                    to: crate::GenerationId::new(Digest::from_bytes([2; 32])),
                },
                edits: vec![crate::MaterializationEdit::Install { path: "file.txt".to_owned(), image: vec![1] }],
            },
            preimages: Vec::new(), phase: crate::MaterializationPhase::Preparing, applied: 0, restored: 0,
        };
        assert!(store.compare_and_swap(operation, 0, crate::materializer::OwnedMaterializationJournal {
            journal, custody: None,
        }).await.expect("original durable preparing intent").0);
        let measured = store.materialization_with_work(work.clone());
        let original = measured.load(operation).await.expect("original decoder").expect("same operation");
        let root_identity = original.journal.root_identity;
        let mut file = work.open_state_file_with_native_work(&effect, crate::materializer::NativeStateFileMode::ReadWriteCreate)
            .expect("held authorized fixture file");
        let completion = measured.prepare_action(original).await.expect("accepted exact original completion");
        file.write_bytes(b"after").expect("actual bounded SDK action");
        cancellation.cancel();
        let known_prefix = work.journal_work().expect("actual post-action prefix");
        let allocations = work.receipt().allocation_operations;
        let completed = completion.complete().await.expect("only the previously accepted metadata completion");
        assert_eq!(work.receipt().allocation_operations, allocations, "completion allocates no buffer, filename, request or boxed future");
        assert_eq!(completed.journal.revision, 2);
        assert_eq!(completed.journal.root_identity, root_identity);
        assert_eq!(completed.journal.work, known_prefix);
        assert_eq!(completed.journal.phase, crate::MaterializationPhase::Preparing);
        assert!(completed.custody.as_ref().expect("original packet").work().shares_ledger(&work));
        work.receipt().verify(budget).expect("all completion work remains within original finite budget");
        drop(completed);
        drop(measured);
        drop(store);
        let restarted = LocalCoreStateStore::new(&state_root);
        let recovered = restarted.load(operation).await.expect("real process-style restart").expect("same durable operation");
        assert_eq!(recovered.journal.revision, 2);
        assert_eq!(recovered.journal.root_identity, root_identity);
        assert_eq!(recovered.journal.work, known_prefix, "durable prefix is a lower bound, not the later commit work");
        assert_eq!(std::fs::read(effect).expect("real completed source effect"), b"aftere");
    }

    /// Canonical root binding without original fork evidence.
    fn lazy_state(workspace_id: WorkspaceId, revision: u64) -> LazyWorkspaceState {
        let shadows = [3_u8; 32];
        serde_json::from_value(serde_json::json!({
            "schema_version": 10,
            "revision": revision,
            "workspace_id": workspace_id,
            "parent_workspace_id": null,
            "fork_origin": null,
            "completed_selected_import": null,
            "pending_write": null,
            "completed_write": null,
            "source": crate::demand::SourceReference { identity: [1; 16], epoch: 0 },
            "overlay": LazyOverlayId::from_bytes([2; 32]),
            "shadows": shadows,
            "pending_remove": null,
        }))
        .expect("lazy binding fixture")
    }

    #[test]
    fn canonical_lazy_companion_layout_requires_schema_ten() {
        let workspace = WorkspaceId::from_bytes([0x39; 16]);
        let mut state = lazy_state(workspace, 1);
        assert_eq!(state.schema_version, 10);
        state.validate_root_binding(workspace, state.source).expect("current root layout");
        state.schema_version = 9;
        assert!(matches!(
            state.validate_root_binding(workspace, state.source),
            Err(crate::LazyWorkspaceError::StaleSource),
        ));
    }

    #[tokio::test]
    async fn native_lazy_state_uses_original_meter_and_preserves_completed_snapshot_on_restart() {
        let directory = tempfile::tempdir().expect("native core namespace");
        let id = WorkspaceId::from_bytes([0xb1; 16]);
        let mut state = lazy_state(id, 1);
        let snapshot = crate::LazySnapshotRef::new(
            id, crate::GenerationId::new(Digest::from_bytes([0xb2; 32])),
            state.source, state.overlay, state.shadows,
        );
        state.completed_selected_import = Some(crate::CompletedSelectedImport {
            operation: OperationId::from_bytes([0xb3; 16]),
            snapshot, destination: snapshot, source: snapshot,
            input: Digest::from_bytes([0xb4; 32]),
            config: crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral),
            source_config: crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral),
        });
        let cancellation = crate::CancellationToken::new();
        let work = crate::materializer::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).expect("original native meter");
        work.charge(crate::WorkCounters { bytes_hashed: 5, ..Default::default() }).expect("original prior producer");
        let store = LocalCoreStateStore::new(directory.path());
        assert!(store.compare_and_swap_lazy_workspace_with_native_work(
            id, 0, state.clone(), &work,
        ).await.expect("native original CAS"));
        let first = store.load_lazy_workspace_with_native_work(id, &work).await
            .expect("native original load").expect("actual state");
        assert_eq!(first.as_state(), &state);
        let (_, allocation) = first.into_parts();
        assert!(work.shares_ledger(allocation.work()));
        drop(allocation);
        assert_eq!(work.receipt().bytes_hashed, 5);
        let before = work.receipt().object_bytes_written;
        let mut stale = state.clone();
        stale.revision += 1;
        assert!(!store.compare_and_swap_lazy_workspace_with_native_work(
            id, 0, stale, &work,
        ).await.expect("original CAS miss"));
        assert_eq!(work.receipt().object_bytes_written, before);
        drop(store);
        let restarted = LocalCoreStateStore::new(directory.path());
        let original = restarted.load_lazy_workspace_with_native_work(id, &work).await
            .expect("real backend restart").expect("original retained state");
        assert_eq!(original.as_state().completed_selected_import, state.completed_selected_import);
        assert_eq!(original.as_state().source, state.source);
        assert_eq!(original.as_state().workspace_id, id);
    }

    #[tokio::test]
    async fn native_lazy_state_exact_read_budget_retains_prior_work_and_typed_cancellation() {
        let directory = tempfile::tempdir().expect("native core namespace");
        let id = WorkspaceId::from_bytes([0xb5; 16]);
        let store = LocalCoreStateStore::new(directory.path());
        assert!(store.compare_and_swap_lazy_workspace(id, 0, lazy_state(id, 1))
            .await.expect("durable fixture"));
        drop(store);
        let cancellation = crate::CancellationToken::new();
        let budget = crate::WorkBudget { source_bytes_read: 7, ..crate::WorkBudget::UNBOUNDED };
        let work = crate::materializer::NativeMaterializationWork::new(budget, &cancellation)
            .expect("original finite meter");
        work.charge(crate::WorkCounters { bytes_hashed: 5, ..Default::default() }).expect("prior actual work");
        let restarted = LocalCoreStateStore::new(directory.path());
        let error = restarted.load_lazy_workspace_with_native_work(id, &work).await
            .expect_err("actual decoder stops at original exact source allowance");
        assert!(matches!(error, crate::LazyWorkspaceError::DemandWork(_)));
        assert_eq!(work.receipt().source_bytes_read, 7);
        assert_eq!(work.receipt().bytes_hashed, 5);
        work.receipt().verify(budget).expect("all actual spent prefix");
        let before = work.receipt();
        cancellation.cancel();
        assert!(matches!(
            restarted.load_lazy_workspace_with_native_work(id, &work).await,
            Err(crate::LazyWorkspaceError::Cancelled),
        ));
        assert_eq!(work.receipt(), before);
    }

    #[tokio::test]
    async fn native_lazy_overlay_retains_output_and_cache_custody_and_refuses_another_ledger() {
        let directory = tempfile::tempdir().expect("native immutable node namespace");
        let store = LocalCoreStateStore::new(directory.path());
        let value = LazyOverlay::Node {
            path: format!("/{}", "owned".repeat(100)),
            priority: 3,
            change: crate::LazyOverlayChange::Tombstone { removed: None },
            left: LazyOverlayId::from_bytes(content_address(&LazyOverlay::Empty).expect("empty ID")),
            right: LazyOverlayId::from_bytes(content_address(&LazyOverlay::Empty).expect("empty ID")),
        };
        let id = LazyOverlayId::from_bytes(content_address(&value).expect("actual node ID"));
        let cancellation = crate::CancellationToken::new();
        let work = crate::materializer::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).expect("original meter");
        let (owned, allocation) = value.clone_owned(&work).expect("actual admitted clone");
        let payload_bytes = allocation.bytes();
        store.put_lazy_overlay_with_native_work(
            id, NativeOwnedCoreState::from_parts(owned, allocation), &work,
        ).await.expect("original native put");
        let cache_live = work.live_allocation_bytes();
        assert!(cache_live >= crate::materializer::NativeMaterializationWork::accounting_state_bytes() + payload_bytes);
        let output = store.load_lazy_overlay_with_native_work(id, &work).await
            .expect("native cached clone").expect("actual output");
        assert_eq!(output.as_state(), &value);
        assert!(work.live_allocation_bytes() >= cache_live + payload_bytes);
        drop(output);
        assert_eq!(work.live_allocation_bytes(), cache_live);
        let other = crate::materializer::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).expect("other original meter");
        let (foreign, allocation) = value.clone_owned(&other).expect("foreign payload");
        let before = work.receipt();
        assert!(matches!(store.put_lazy_overlay_with_native_work(
            id, NativeOwnedCoreState::from_parts(foreign, allocation), &work,
        ).await, Err(crate::LazyWorkspaceError::NativeLedgerMismatch)));
        assert_eq!(work.receipt(), before);
        drop(store);
        assert_eq!(work.live_allocation_bytes(), crate::materializer::NativeMaterializationWork::accounting_state_bytes());
    }

    #[tokio::test]
    async fn native_owned_lazy_cas_keeps_real_resident_backing_until_owner_drop() {
        let directory = tempfile::tempdir().expect("owned native namespace");
        let id = WorkspaceId::from_bytes([0xb6; 16]);
        let owner = LocalCoreStateStore::open_owned(directory.path()).expect("original owner");
        let cancellation = crate::CancellationToken::new();
        let work = crate::materializer::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).expect("original native meter");
        let baseline = work.live_allocation_bytes();
        assert!(owner.compare_and_swap_lazy_workspace_with_native_work(
            id, 0, lazy_state(id, 1), &work,
        ).await.expect("native owned CAS"));
        let retained = work.live_allocation_bytes();
        assert!(retained > baseline, "actual resident and dirty-vector backing is still live");
        let state = owner.load_lazy_workspace_with_native_work(id, &work).await
            .expect("actual owned-cache load").expect("original state");
        assert_eq!(state.as_state().workspace_id, id);
        drop(state);
        assert_eq!(work.live_allocation_bytes(), retained);
        drop(owner);
        assert_eq!(work.live_allocation_bytes(), baseline);
        let restarted = LocalCoreStateStore::new(directory.path());
        let retained = restarted.load_lazy_workspace_with_native_work(id, &work).await
            .expect("real namespace restart").expect("acknowledged original decision");
        assert_eq!(retained.as_state().revision, 1);
    }

    #[tokio::test]
    async fn native_intact_corrupt_frame_moves_original_path_custody_without_error_allocation() {
        let directory = tempfile::tempdir().expect("native corrupt-frame namespace");
        let store = LocalCoreStateStore::new(directory.path());
        let original_path = store.transaction(|namespace| {
            let paths = core_log_paths(namespace);
            std::fs::create_dir_all(&paths.directory)?;
            let frame = b"not-json";
            let mut bytes = blake3::hash(frame).to_hex().as_bytes().to_vec();
            bytes.push(b' ');
            bytes.extend_from_slice(frame);
            bytes.push(b'\n');
            std::fs::write(&paths.current, bytes)?;
            Ok(paths.current)
        }).await.expect("real intact malformed WAL frame");
        let cancellation = crate::CancellationToken::new();
        let work = crate::materializer::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED, &cancellation,
        ).expect("original native meter");
        let baseline = work.live_allocation_bytes();
        let error = store.load_lazy_workspace_with_native_work(
            WorkspaceId::from_bytes([0xb7; 16]), &work,
        ).await.expect_err("intact malformed frame refuses native replay");
        let prior = work.receipt();
        cancellation.cancel();
        let (error, allocation) = error.into_native_failure();
        let allocation = allocation.expect("original path backing is retained with its cause");
        assert!(allocation.work().shares_ledger(&work));
        assert!(matches!(&error, crate::LazyWorkspaceError::NativeRecordCorrupt(path, None)
            if path == &original_path));
        assert_eq!(work.receipt(), prior, "pure extraction does not allocate, check or recharge");
        assert_eq!(work.live_allocation_bytes(), baseline + original_path.as_os_str().len() as u64);
        drop(error);
        drop(allocation);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }


    #[tokio::test]
    async fn measured_lazy_family_preserves_pre_cancelled_failures_without_io() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let workspace = WorkspaceId::from_bytes([41; 16]);
        let overlay = LazyOverlayId::from_bytes([42; 32]);
        let shadow = crate::LazyShadowId::from_bytes([43; 32]);
        let cancellation = crate::CancellationToken::new();
        cancellation.cancel();
        let budget = crate::WorkBudget::UNBOUNDED;
        let errors = [
            store.load_lazy_workspace_measured(workspace, budget, &cancellation).await.unwrap_err(),
            store.compare_and_swap_lazy_workspace_measured(
                workspace, 0, lazy_state(workspace, 1), budget, &cancellation,
            ).await.unwrap_err(),
            store.load_lazy_overlay_measured(overlay, budget, &cancellation).await.unwrap_err(),
            store.put_lazy_overlay_measured(overlay, LazyOverlay::Empty, budget, &cancellation).await.unwrap_err(),
            store.load_lazy_shadow_measured(shadow, budget, &cancellation).await.unwrap_err(),
            store.put_lazy_shadow_measured(shadow, crate::LazyShadow::Empty, budget, &cancellation).await.unwrap_err(),
        ];
        for error in errors {
            assert!(matches!(error.semantic_error(), crate::LazyWorkspaceError::Cancelled));
            assert_eq!(error.spent_work(), crate::WorkCounters::UNCHARGED);
        }
        assert!(std::fs::read_dir(directory.path()).expect("namespace entries").next().is_none());
    }

    fn is_ownership_conflict<T>(result: &Result<T, LocalCoreStateStoreError>) -> bool {
        matches!(result, Err(LocalCoreStateStoreError::OwnershipConflict(_)))
    }

    #[tokio::test]
    async fn owner_excludes_every_other_store_in_its_process() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let owner = LocalCoreStateStore::open_owned(directory.path()).expect("owner");
        let clone = owner.clone();
        let shared = LocalCoreStateStore::new(directory.path());
        assert!(is_ownership_conflict(&LocalCoreStateStore::open_owned(
            directory.path()
        )));
        assert!(is_ownership_conflict(
            &WorkspaceLineageStore::load(&shared, workspace()).await
        ));
        assert!(is_ownership_conflict(
            &shared.load_lazy_workspace(workspace()).await
        ));

        drop(owner);
        assert!(
            is_ownership_conflict(&WorkspaceLineageStore::load(&shared, workspace()).await),
            "a clone keeps the namespace owned"
        );
        drop(clone);
        assert!(
            WorkspaceLineageStore::load(&shared, workspace())
                .await
                .expect("released namespace")
                .is_none()
        );

        let admitted = shared.namespace().expect("shared admission");
        let concurrent = LocalCoreStateStore::new(directory.path())
            .namespace()
            .expect("shared admissions coexist");
        assert!(is_ownership_conflict(&LocalCoreStateStore::open_owned(
            directory.path()
        )));
        drop(admitted);
        drop(concurrent);
        LocalCoreStateStore::open_owned(directory.path()).expect("owner after admissions end");
    }

    #[test]
    fn ownership_child_acts_on_its_namespace() -> Result<(), Box<dyn std::error::Error>> {
        let Some(root) = std::env::var_os("ACYCLIC_CORE_STATE_CHILD_ROOT") else {
            return Ok(());
        };
        if let Some(ready) = std::env::var_os("ACYCLIC_CORE_STATE_CHILD_READY") {
            let _owner = LocalCoreStateStore::open_owned(&root)?;
            std::fs::write(ready, b"ready")?;
            loop {
                std::thread::park();
            }
        }
        if !is_ownership_conflict(&LocalCoreStateStore::new(&root).namespace()) {
            return Err("child admitted a shared store to an owned namespace".into());
        }
        if !is_ownership_conflict(&LocalCoreStateStore::open_owned(&root)) {
            return Err("child opened a second owner".into());
        }
        Ok(())
    }

    fn ownership_child(root: &Path) -> Result<std::process::Command, std::io::Error> {
        let mut child = std::process::Command::new(std::env::current_exe()?);
        child
            .args([
                "--exact",
                "core_state::tests::ownership_child_acts_on_its_namespace",
                "--nocapture",
            ])
            .env("ACYCLIC_CORE_STATE_CHILD_ROOT", root);
        Ok(child)
    }

    #[test]
    fn owner_excludes_other_processes_until_it_dies() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let root = directory.path().join("core-state");
        let owner = LocalCoreStateStore::open_owned(&root)?;
        assert!(
            acyclic_native_runtime::process_status(
                &mut ownership_child(&root)?,
                std::time::Duration::from_secs(120)
            )?
            .success(),
            "another process entered an owned namespace"
        );
        drop(owner);

        let ready = directory.path().join("ready");
        let mut child = acyclic_native_runtime::spawn_process_tree(
            ownership_child(&root)?.env("ACYCLIC_CORE_STATE_CHILD_READY", &ready),
        )?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.is_file() {
            if let Some(status) = child.try_wait()? {
                return Err(format!("owner child exited before readiness: {status}").into());
            }
            if Instant::now() >= deadline {
                return Err("owner child readiness timed out".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(is_ownership_conflict(
            &LocalCoreStateStore::new(&root).namespace()
        ));
        assert!(is_ownership_conflict(&LocalCoreStateStore::open_owned(
            &root
        )));
        child.terminate_descendants()?;
        child.wait(Duration::from_secs(5))?;
        LocalCoreStateStore::open_owned(&root)?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn owned_lazy_workspace_cache_is_never_stale_under_concurrency() {
        use std::sync::atomic::{AtomicU64, Ordering};

        let directory = tempfile::tempdir().expect("temporary directory");
        let workspaces = [
            WorkspaceId::from_bytes([0x61; 16]),
            WorkspaceId::from_bytes([0x62; 16]),
        ];
        let seed = LocalCoreStateStore::new(directory.path());
        for workspace_id in workspaces {
            assert!(
                seed.compare_and_swap_lazy_workspace(workspace_id, 0, lazy_state(workspace_id, 1))
                    .await
                    .expect("seed binding")
            );
        }
        drop(seed);

        // Every binding is durable but uncached, so first loads race their
        // fills against concurrent writes.
        let owner = LocalCoreStateStore::open_owned(directory.path()).expect("owner");
        let committed = Arc::new(workspaces.map(|workspace_id| (workspace_id, AtomicU64::new(1))));
        let tasks = (0..8_usize)
            .map(|task| {
                let owner = owner.clone();
                let committed = Arc::clone(&committed);
                tokio::spawn(async move {
                    let mut wins = 0_u64;
                    for round in 0..64_usize {
                        let (workspace_id, floor) = committed
                            .get((task + round) % committed.len())
                            .expect("binding slot");
                        let floor_before_load = floor.load(Ordering::SeqCst);
                        let current = owner
                            .load_lazy_workspace(*workspace_id)
                            .await
                            .expect("load binding")
                            .expect("binding is present");
                        assert!(
                            current.revision >= floor_before_load,
                            "load returned revision {} after revision {floor_before_load} committed",
                            current.revision
                        );
                        let expected_revision = current.revision;
                        let replacement = LazyWorkspaceState {
                            revision: expected_revision + 1,
                            ..current
                        };
                        let revision = replacement.revision;
                        if owner
                            .compare_and_swap_lazy_workspace(
                                *workspace_id,
                                expected_revision,
                                replacement,
                            )
                            .await
                            .expect("swap binding")
                        {
                            floor.fetch_max(revision, Ordering::SeqCst);
                            wins += 1;
                        }
                    }
                    wins
                })
            })
            .collect::<Vec<_>>();
        let mut wins = 0;
        for task in tasks {
            wins += task.await.expect("contender task");
        }

        let mut advanced = 0;
        let mut cached = Vec::new();
        for (workspace_id, floor) in committed.iter() {
            let state = owner
                .load_lazy_workspace(*workspace_id)
                .await
                .expect("load final binding")
                .expect("final binding is present");
            assert_eq!(state.revision, floor.load(Ordering::SeqCst));
            advanced += state.revision - 1;
            cached.push(state);
        }
        assert_eq!(
            advanced, wins,
            "every successful swap advanced exactly once"
        );

        drop(owner);
        let durable = LocalCoreStateStore::new(directory.path());
        for state in cached {
            assert_eq!(
                durable
                    .load_lazy_workspace(state.workspace_id)
                    .await
                    .expect("load durable binding"),
                Some(state)
            );
        }
    }

    #[tokio::test]
    async fn committed_change_survives_a_failing_checkpoint() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let owner = LocalCoreStateStore::open_owned(directory.path()).expect("owner");
        for revision in 1..OWNED_CHECKPOINT {
            assert!(
                owner
                    .compare_and_swap_lazy_workspace(
                        workspace(),
                        revision - 1,
                        lazy_state(workspace(), revision),
                    )
                    .await
                    .expect("commit below the checkpoint threshold")
            );
        }
        assert!(
            WorkspaceLineageStore::load(&owner, CRASH_WORKSPACE)
                .await
                .expect("absent lineage becomes resident")
                .is_none()
        );
        // The lineage family's directory cannot be created, so the checkpoint
        // that the last commit makes due fails.
        std::fs::write(directory.path().join(LINEAGE_FAMILY), b"obstacle").expect("obstacle");
        let record = crash_lineage();
        assert!(
            WorkspaceLineageStore::compare_and_swap(&owner, CRASH_WORKSPACE, 0, record.clone())
                .await
                .expect("the commit that makes a checkpoint due succeeds")
        );
        assert!(
            WorkspaceLineageStore::load(&owner, CRASH_WORKSPACE)
                .await
                .is_ok(),
            "resident reads need no checkpoint"
        );
        assert!(
            owner
                .compare_and_swap_lazy_workspace(
                    workspace(),
                    OWNED_CHECKPOINT - 1,
                    lazy_state(workspace(), OWNED_CHECKPOINT),
                )
                .await
                .is_err(),
            "the next transaction reports the failed checkpoint"
        );
        std::fs::remove_file(directory.path().join(LINEAGE_FAMILY)).expect("remove obstacle");
        assert!(
            owner
                .compare_and_swap_lazy_workspace(
                    workspace(),
                    OWNED_CHECKPOINT - 1,
                    lazy_state(workspace(), OWNED_CHECKPOINT),
                )
                .await
                .expect("the retried checkpoint succeeds")
        );
        drop(owner);
        assert_eq!(
            WorkspaceLineageStore::load(
                &LocalCoreStateStore::new(directory.path()),
                CRASH_WORKSPACE
            )
            .await
            .expect("durable lineage"),
            Some(record)
        );
    }

    /// Rewrites the log's only frame with `edit` and a matching checksum.
    fn rewrite_intact_frame(root: &Path, edit: impl FnOnce(&mut serde_json::Value)) -> Vec<u8> {
        let log = {
            let store = LocalCoreStateStore::new(root);
            let namespace = store.namespace().expect("admitted namespace");
            core_log_paths(&namespace).current
        };
        let bytes = std::fs::read(&log).expect("read core log");
        let line = bytes.strip_suffix(b"\n").expect("one complete frame");
        let mut frame: serde_json::Value = serde_json::from_slice(&line[65..]).expect("frame json");
        edit(&mut frame);
        let frame = serde_json::to_vec(&frame).expect("encode frame");
        let mut rewritten = blake3::hash(&frame).to_hex().as_bytes().to_vec();
        rewritten.push(b' ');
        rewritten.extend_from_slice(&frame);
        rewritten.push(b'\n');
        std::fs::write(&log, &rewritten).expect("rewrite core log");
        rewritten
    }

    #[tokio::test]
    async fn intact_mismatched_frame_cannot_checkpoint_or_hide_context() {
        for owned in [false, true] {
            let directory = tempfile::tempdir().expect("temporary directory");
            let context_id = WorkspaceContextId::from_bytes([41; 16]);
            let store = LocalCoreStateStore::new(directory.path());
            let context = workspace_context(
                directory.path(),
                context_id,
                1,
                WorkspaceContextState::Active,
            );
            assert!(
                WorkspaceContextStore::compare_and_swap(&store, context_id, 0, context)
                    .await
                    .expect("persist context")
            );
            let namespace = store.namespace().expect("admitted namespace");
            let context_path = context_paths(&namespace, context_id).current;
            let log_path = core_log_paths(&namespace).current;
            let before = std::fs::read(&context_path).expect("context bytes");
            drop(namespace);
            drop(store);
            let writer = LocalCoreStateStore::open_owned(directory.path()).expect("writer");
            assert!(
                writer
                    .compare_and_swap_lazy_workspace(workspace(), 0, lazy_state(workspace(), 1))
                    .await
                    .expect("commit lazy workspace frame")
            );
            drop(writer);
            let frame = rewrite_intact_frame(directory.path(), |frame| {
                frame["change"][0][0] = serde_json::to_value(RecordKey::Context(context_id))
                    .expect("context record key");
            });
            let reader = if owned {
                LocalCoreStateStore::open_owned(directory.path()).expect("reader")
            } else {
                LocalCoreStateStore::new(directory.path())
            };
            let listed = WorkspaceContextStore::list(&reader).await;
            let after = std::fs::read(&context_path).expect("context remains readable");
            let after_frame = std::fs::read(log_path).expect("log bytes");
            assert!(
                matches!(listed.as_ref(), Err(LocalCoreStateStoreError::Corrupt(_)))
                    && before == after
                    && frame == after_frame,
                "owned={owned}: reject before residency/checkpoint: list={listed:?}, context preserved={}, frame preserved={}",
                before == after,
                frame == after_frame
            );
        }
    }

    #[tokio::test]
    async fn intact_undecodable_frame_fails_closed_and_is_kept() {
        type Edit = fn(&mut serde_json::Value);
        let edits: [(&str, Edit); 3] = [
            ("unknown record key", |frame| {
                frame["change"][0][0] = serde_json::json!({ "Future": [0] });
            }),
            ("unknown frame field", |frame| {
                frame["future"] = serde_json::json!(1);
            }),
            ("unknown payload field", |frame| {
                frame["change"][0][1]["LazyWorkspace"]["future"] = serde_json::json!(1);
            }),
        ];
        for (case, edit) in edits {
            let directory = tempfile::tempdir().expect("temporary directory");
            let owner = LocalCoreStateStore::open_owned(directory.path()).expect("owner");
            assert!(
                owner
                    .compare_and_swap_lazy_workspace(workspace(), 0, lazy_state(workspace(), 1))
                    .await
                    .expect("commit")
            );
            // The owner leaves its committed frame in the log for replay.
            drop(owner);
            let log = rewrite_intact_frame(directory.path(), edit);
            let store = LocalCoreStateStore::new(directory.path());
            assert!(
                matches!(
                    store.load_lazy_workspace(workspace()).await,
                    Err(LocalCoreStateStoreError::Corrupt(_))
                ),
                "{case}: an intact frame that does not decode is corruption"
            );
            let namespace = store.namespace().expect("admitted namespace");
            assert_eq!(
                std::fs::read(core_log_paths(&namespace).current).expect("read core log"),
                log,
                "{case}: a failed replay must not checkpoint the log away"
            );
        }
    }

    #[tokio::test]
    async fn deferred_commits_are_visible_at_once_and_durable_on_commit() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let owner = LocalCoreStateStore::open_owned(directory.path()).expect("owner");
        let (deferred, durability) = owner.defer_durability();
        assert!(
            deferred
                .compare_and_swap_lazy_workspace(workspace(), 0, lazy_state(workspace(), 1))
                .await
                .expect("deferred commit")
        );
        assert_eq!(
            owner
                .load_lazy_workspace(workspace())
                .await
                .expect("visible deferred commit"),
            Some(lazy_state(workspace(), 1))
        );
        durability.commit().await.expect("flush deferred commits");
        assert!(
            deferred
                .compare_and_swap_lazy_workspace(workspace(), 1, lazy_state(workspace(), 2))
                .await
                .expect("commit after the scope ended")
        );
        let log = {
            let namespace = owner.namespace().expect("admitted namespace");
            core_log_paths(&namespace).current
        };
        let bytes = std::fs::read(&log).expect("read core log");
        assert_eq!(
            replayable_changes(&bytes).expect("decodable log").len(),
            2,
            "both frames are in the log"
        );
        drop((owner, deferred));
        assert_eq!(
            LocalCoreStateStore::new(directory.path())
                .load_lazy_workspace(workspace())
                .await
                .expect("durable binding"),
            Some(lazy_state(workspace(), 2))
        );
    }

    const CRASH_PARENT: WorkspaceContextId = WorkspaceContextId::from_bytes([0x41; 16]);
    const CRASH_CHILD: WorkspaceContextId = WorkspaceContextId::from_bytes([0x42; 16]);
    const CRASH_WORKSPACE: WorkspaceId = WorkspaceId::from_bytes([0x43; 16]);
    /// Commits of the crash script, grouped by what one caller acknowledges;
    /// the multi-commit group runs through one deferred-durability scope.
    const CRASH_GROUPS: [std::ops::Range<usize>; 6] = [0..1, 1..2, 2..5, 5..6, 6..7, 7..8];

    fn crash_lineage() -> WorkspaceLineageRecord {
        WorkspaceLineageRecord {
            version: 1,
            revision: 1,
            ready: true,
            workspace_id: CRASH_WORKSPACE,
            workspace_name: "crash-child".to_owned(),
            parent_workspace_id: Some(workspace()),
            parent_workspace_name: Some("durable".to_owned()),
            fork_generation: crate::GenerationId::new(Digest::from_bytes([5; 32])),
            initial_generation: crate::GenerationId::new(Digest::from_bytes([6; 32])),
        }
    }

    /// Every logged record the crash script changes.
    #[derive(Debug, PartialEq)]
    struct CoreSnapshot {
        contexts: Vec<WorkspaceContext>,
        children: Option<ContextChildren>,
        child_count: Option<ContextChildCount>,
        index_ready: bool,
        lineage: Option<WorkspaceLineageRecord>,
        lazy: Option<LazyWorkspaceState>,
    }

    /// Recovers the namespace through `store` and reads every scripted record.
    fn core_snapshot(store: &LocalCoreStateStore) -> CoreSnapshot {
        let namespace = store.namespace().expect("admitted namespace");
        namespace
            .with_log(|log| {
                Ok(CoreSnapshot {
                    contexts: load_all_contexts(&namespace, log)?,
                    children: log.get::<ContextChildren>(&namespace, CRASH_PARENT)?,
                    child_count: log.get::<ContextChildCount>(&namespace, CRASH_PARENT)?,
                    index_ready: context_children_index_ready(&namespace, log)?,
                    lineage: log.get::<WorkspaceLineageRecord>(&namespace, CRASH_WORKSPACE)?,
                    lazy: log.get::<LazyWorkspaceState>(&namespace, CRASH_WORKSPACE)?,
                })
            })
            .expect("recovered snapshot")
    }

    /// Runs one commit of the crash script: index, register a parent, register
    /// its child with the child's lineage and binding, freeze the child,
    /// advance the binding, discard the child.
    fn crash_step(store: &LocalCoreStateStore, step: usize) {
        let source = std::env::temp_dir();
        let parent = workspace_context(&source, CRASH_PARENT, 1, WorkspaceContextState::Active);
        let mut child = workspace_context(&source, CRASH_CHILD, 1, WorkspaceContextState::Active);
        child.parent_context_id = Some(CRASH_PARENT);
        let committed = futures::executor::block_on(async {
            match step {
                0 => WorkspaceContextStore::list(store).await.map(|_| true),
                1 => {
                    store
                        .compare_and_swap_many(
                            BTreeMap::from([(CRASH_PARENT, 0)]),
                            vec![parent],
                            false,
                        )
                        .await
                }
                2 => {
                    store
                        .compare_and_swap_many(
                            BTreeMap::from([(CRASH_CHILD, 0), (CRASH_PARENT, 1)]),
                            vec![child],
                            false,
                        )
                        .await
                }
                3 => {
                    WorkspaceLineageStore::compare_and_swap(
                        store,
                        CRASH_WORKSPACE,
                        0,
                        crash_lineage(),
                    )
                    .await
                }
                4 => {
                    store
                        .compare_and_swap_lazy_workspace(
                            CRASH_WORKSPACE,
                            0,
                            lazy_state(CRASH_WORKSPACE, 1),
                        )
                        .await
                }
                5 => {
                    child.revision = 2;
                    child.state = WorkspaceContextState::Frozen;
                    store
                        .compare_and_swap_many(
                            BTreeMap::from([(CRASH_CHILD, 1)]),
                            vec![child],
                            false,
                        )
                        .await
                }
                6 => {
                    store
                        .compare_and_swap_lazy_workspace(
                            CRASH_WORKSPACE,
                            1,
                            lazy_state(CRASH_WORKSPACE, 2),
                        )
                        .await
                }
                _ => WorkspaceContextStore::discard_subtree(store, CRASH_PARENT, CRASH_CHILD, 8)
                    .await
                    .map(|outcome| matches!(outcome, WorkspaceContextDiscardOutcome::Discarded(_))),
            }
        })
        .expect("crash script step");
        assert!(committed, "crash script step {step} was rejected");
    }

    fn crash_store(root: &Path, owned: bool) -> LocalCoreStateStore {
        if owned {
            LocalCoreStateStore::open_owned(root).expect("owner")
        } else {
            LocalCoreStateStore::new(root)
        }
    }

    #[test]
    fn core_crash_child_runs_until_its_crash_point() -> Result<(), Box<dyn std::error::Error>> {
        let Some(root) = std::env::var_os("ACYCLIC_CORE_STATE_CRASH_ROOT") else {
            return Ok(());
        };
        let acknowledged = std::env::var_os("ACYCLIC_CORE_STATE_CRASH_ACKNOWLEDGED")
            .ok_or("crash child acknowledgement path is absent")?;
        let store = crash_store(
            Path::new(&root),
            std::env::var_os("ACYCLIC_CORE_STATE_CRASH_OWNED").is_some(),
        );
        for group in CRASH_GROUPS {
            if group.len() == 1 {
                crash_step(&store, group.start);
            } else {
                let (deferred, durability) = store.defer_durability();
                for step in group.clone() {
                    crash_step(&deferred, step);
                }
                futures::executor::block_on(durability.commit())?;
            }
            std::fs::write(&acknowledged, group.end.to_string())?;
        }
        Ok(())
    }

    /// Simulates losing every write not yet flushed: the log keeps its durable
    /// prefix and half of the next frame, and every record a durable frame
    /// names reads back torn.
    fn lose_unflushed_writes(root: &Path, flushed: usize) {
        let store = LocalCoreStateStore::new(root);
        let namespace = store.namespace().expect("admitted namespace");
        let log = core_log_paths(&namespace).current;
        let mut bytes = std::fs::read(&log).expect("read core log");
        let torn = flushed + (bytes.len().saturating_sub(flushed)) / 2;
        bytes.truncate(torn);
        std::fs::write(&log, &bytes).expect("tear unflushed frames");
        for change in
            replayable_changes(bytes.get(..flushed).unwrap_or(&bytes)).expect("decodable log")
        {
            for (key, _) in change.0 {
                let paths = key.paths(&namespace);
                std::fs::create_dir_all(&paths.directory).expect("record directory");
                std::fs::write(paths.current, b"{\"torn").expect("tear unflushed record");
            }
        }
    }

    fn copy_tree(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).expect("copy directory");
        for entry in std::fs::read_dir(from).expect("read copied directory") {
            let entry = entry.expect("copied entry");
            let target = to.join(entry.file_name());
            if entry.file_type().expect("copied entry type").is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).expect("copy file");
            }
        }
    }

    /// Kills a writer at every crash point of every commit, then recovers both
    /// what the killed process left and what a power loss would leave. Each
    /// recovery must expose exactly the state after some prefix of the
    /// script's commits: at least every acknowledged commit, and at most the
    /// ones in flight.
    fn assert_core_commits_are_crash_atomic(owned: bool) {
        let directory = tempfile::tempdir().expect("temporary directory");
        let reference_store = crash_store(&directory.path().join("reference"), owned);
        let mut snapshots = vec![core_snapshot(&reference_store)];
        for step in 0..CRASH_GROUPS[CRASH_GROUPS.len() - 1].end {
            crash_step(&reference_store, step);
            snapshots.push(core_snapshot(&reference_store));
        }
        drop(reference_store);

        let executable = std::env::current_exe().expect("test executable");
        let mut crashes = 0;
        for point in 1.. {
            assert!(point < 1_000, "crash script never completed");
            let run = directory.path().join(format!("run-{point}"));
            let root = run.join("state");
            let acknowledged = run.join("acknowledged");
            let report = run.join("crash-point");
            std::fs::create_dir_all(&run).expect("run directory");
            let mut child = std::process::Command::new(&executable);
            child
                .args([
                    "--exact",
                    "core_state::tests::core_crash_child_runs_until_its_crash_point",
                    "--nocapture",
                ])
                .env("ACYCLIC_CORE_STATE_CRASH_ROOT", &root)
                .env("ACYCLIC_CORE_STATE_CRASH_ACKNOWLEDGED", &acknowledged)
                .env("ACYCLIC_CORE_STATE_CRASH_POINT", point.to_string())
                .env("ACYCLIC_CORE_STATE_CRASH_REPORT", &report)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            if owned {
                child.env("ACYCLIC_CORE_STATE_CRASH_OWNED", "1");
            }
            let status = acyclic_native_runtime::process_status(
                &mut child,
                std::time::Duration::from_secs(120),
            )
            .expect("crash child");
            if status.success() {
                assert!(
                    !report.exists(),
                    "a crash child reported success after aborting"
                );
                assert_eq!(
                    core_snapshot(&crash_store(&root, owned)),
                    snapshots[snapshots.len() - 1]
                );
                break;
            }
            crashes += 1;
            let report = std::fs::read_to_string(&report).expect("crash point report");
            let (label, flushed) = report.split_once('\n').expect("crash point fields");
            let flushed = flushed.parse().expect("durable log length");
            let done: usize = std::fs::read_to_string(&acknowledged)
                .map_or(0, |count| count.parse().expect("acknowledged count"));
            let in_flight = CRASH_GROUPS
                .iter()
                .find(|group| group.start == done)
                .expect("in-flight group");
            let lost = run.join("power-loss");
            copy_tree(&root, &lost);
            lose_unflushed_writes(&lost, flushed);
            for (recovered, outcome) in [(&root, "kill"), (&lost, "power loss")] {
                let snapshot = core_snapshot(&crash_store(recovered, !owned));
                assert!(
                    snapshots[done..=in_flight.end].contains(&snapshot),
                    "{outcome} at point {point} ({label}) after {done} commits exposed {snapshot:?}"
                );
            }
        }
        assert!(crashes > 16, "crash points were not exercised");
    }

    #[test]
    fn owned_core_commits_are_crash_atomic() {
        assert_core_commits_are_crash_atomic(true);
    }

    #[test]
    fn shared_core_commits_are_crash_atomic() {
        assert_core_commits_are_crash_atomic(false);
    }

    /// A treap node over `left` and `right`, as mutations build them.
    fn linked_overlay(
        path: &str,
        left: LazyOverlayId,
        right: LazyOverlayId,
    ) -> (LazyOverlayId, LazyOverlay) {
        let node: LazyOverlay = serde_json::from_value(serde_json::json!({
            "Node": {
                "path": path,
                "priority": 0,
                "change": {"Tombstone": {"removed": null}},
                "left": left,
                "right": right
            }
        }))
        .expect("overlay node");
        let id = LazyOverlayId::from_bytes(content_address(&node).expect("overlay address"));
        (id, node)
    }

    /// Collection removes every node no lazy workspace reaches, keeps every
    /// node one does, and a node put again after removal is stored again.
    #[tokio::test(flavor = "multi_thread")]
    async fn lazy_node_collection_keeps_exactly_the_reachable_nodes() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::open_owned(directory.path()).expect("owned store");
        let empty = LazyOverlayId::from_bytes(content_address(&LazyOverlay::Empty).expect("empty"));
        let (leaf, leaf_node) = linked_overlay("/a", empty, empty);
        let (root, root_node) = linked_overlay("/b", leaf, empty);
        let (orphan, orphan_node) = linked_overlay("/old", empty, empty);
        for (id, node) in [
            (leaf, leaf_node),
            (root, root_node),
            (orphan, orphan_node.clone()),
        ] {
            store.put_lazy_overlay(id, node).await.expect("put overlay");
        }
        let empty_shadows = crate::LazyShadowId::from_bytes(
            content_address(&crate::LazyShadow::Empty).expect("empty shadow"),
        );
        // Collection walks the index, not the records it holds.
        let record = vec![1, 2, 3];
        let metadata =
            crate::WorkspaceMetadata::from_engine(crate::kernel::FileMetadata::default());
        let shadow = crate::LazyShadow::Node {
            file_id: crate::foundation::FileId::from_bytes([7; 16]),
            priority: 0,
            record: record.clone(),
            metadata: Box::new(metadata),
            left: empty_shadows,
            right: empty_shadows,
        };
        let shadow_id =
            crate::LazyShadowId::from_bytes(content_address(&shadow).expect("shadow address"));
        store
            .put_lazy_shadow(shadow_id, shadow)
            .await
            .expect("put shadow");
        let orphan_shadow = crate::LazyShadow::Node {
            file_id: crate::foundation::FileId::from_bytes([8; 16]),
            priority: 0,
            record,
            metadata: Box::new(metadata),
            left: empty_shadows,
            right: empty_shadows,
        };
        let orphan_shadow_id = crate::LazyShadowId::from_bytes(
            content_address(&orphan_shadow).expect("orphan shadow address"),
        );
        store
            .put_lazy_shadow(orphan_shadow_id, orphan_shadow)
            .await
            .expect("put orphan shadow");
        let mut state = lazy_state(workspace(), 1);
        state.overlay = root;
        state.shadows = shadow_id;
        assert!(
            store
                .compare_and_swap_lazy_workspace(workspace(), 0, state)
                .await
                .expect("bind lazy workspace")
        );

        assert_eq!(store.collect_lazy_nodes().await.expect("collect"), 2);
        let reopened = LocalCoreStateStore::new(directory.path());
        drop(store);
        for id in [root, leaf] {
            assert!(
                reopened
                    .load_lazy_overlay(id)
                    .await
                    .expect("load")
                    .is_some()
            );
        }
        assert!(
            reopened
                .load_lazy_shadow(shadow_id)
                .await
                .expect("load")
                .is_some()
        );
        assert!(
            reopened
                .load_lazy_overlay(orphan)
                .await
                .expect("load")
                .is_none()
        );
        assert!(
            reopened
                .load_lazy_shadow(orphan_shadow_id)
                .await
                .expect("load")
                .is_none()
        );
        reopened
            .put_lazy_overlay(orphan, orphan_node)
            .await
            .expect("put again");
        assert!(
            reopened
                .load_lazy_overlay(orphan)
                .await
                .expect("load")
                .is_some()
        );
    }

    async fn origin_parent_fixture(store: &LocalCoreStateStore) -> LazyWorkspaceState {
        let empty = LazyOverlayId::from_bytes(content_address(&LazyOverlay::Empty).expect("empty"));
        let (overlay, node) = linked_overlay("/original", empty, empty);
        store.put_lazy_overlay(overlay, node).await.expect("original overlay");
        let empty_shadows = crate::LazyShadowId::from_bytes(
            content_address(&crate::LazyShadow::Empty).expect("empty shadows"),
        );
        let shadow = crate::LazyShadow::Node {
            file_id: crate::FileId::from_bytes([0xa1; 16]),
            priority: 0,
            record: vec![1, 2, 3],
            metadata: Box::new(crate::WorkspaceMetadata::from_engine(
                crate::kernel::FileMetadata::default(),
            )),
            left: empty_shadows,
            right: empty_shadows,
        };
        let shadows = crate::LazyShadowId::from_bytes(content_address(&shadow).expect("shadow"));
        store.put_lazy_shadow(shadows, shadow).await.expect("original shadows");
        let parent = LazyWorkspaceState {
            overlay,
            shadows,
            ..lazy_state(WorkspaceId::from_bytes([0xa0; 16]), 1)
        };
        assert!(store.compare_and_swap_lazy_workspace(
            parent.workspace_id, 0, parent.clone(),
        ).await.expect("parent binding"));
        parent
    }

    fn prepared_origin_fixture(parent: &LazyWorkspaceState) -> LazyWorkspaceState {
        let snapshot = crate::LazySnapshotRef::new(
            parent.workspace_id,
            crate::GenerationId::new(Digest::from_bytes([0xa2; 32])),
            parent.source,
            parent.overlay,
            parent.shadows,
        );
        LazyWorkspaceState {
            parent_workspace_id: Some(parent.workspace_id),
            fork_origin: Some(crate::LazyForkOrigin {
                operation_id: OperationId::from_bytes([0xa3; 16]),
                snapshot,
                config: crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral),
                phase: crate::LazyForkPhase::Prepared,
            }),
            source: parent.source,
            overlay: parent.overlay,
            shadows: parent.shadows,
            ..lazy_state(WorkspaceId::from_bytes([0xa4; 16]), 1)
        }
    }

    fn advanced_origin_parent(parent: &LazyWorkspaceState) -> LazyWorkspaceState {
        LazyWorkspaceState {
            revision: parent.revision + 1,
            source: crate::demand::SourceReference { epoch: parent.source.epoch + 1, ..parent.source },
            overlay: LazyOverlayId::from_bytes(content_address(&LazyOverlay::Empty).expect("empty")),
            shadows: crate::LazyShadowId::from_bytes(
                content_address(&crate::LazyShadow::Empty).expect("empty shadows"),
            ),
            ..parent.clone()
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn first_prepared_freeze_rejects_concurrent_source_update_and_collected_roots() {
        for component in 0..3 {
            let directory = tempfile::tempdir().expect("temporary directory");
            let store = LocalCoreStateStore::open_owned(directory.path()).expect("owned store");
            let parent = origin_parent_fixture(&store).await;
            let prepared = prepared_origin_fixture(&parent);
            let advanced = advanced_origin_parent(&parent);
            let mut updated = LazyWorkspaceState { revision: parent.revision + 1, ..parent.clone() };
            match component {
                0 => updated.source = advanced.source,
                1 => updated.overlay = advanced.overlay,
                _ => updated.shadows = advanced.shadows,
            }
            let updater = store.clone();
            let update = updated.clone();
            let expected = parent.revision;
            assert!(tokio::spawn(async move {
                updater.compare_and_swap_lazy_workspace(update.workspace_id, expected, update)
                    .await.expect("concurrent source binding update")
            }).await.expect("update task"));
            assert_eq!(store.collect_lazy_nodes().await.expect("collect old source roots"), u64::from(component != 0));
            if component == 1 {
                assert!(store.load_lazy_overlay(parent.overlay).await.expect("old overlay").is_none());
            } else if component == 2 {
                assert!(store.load_lazy_shadow(parent.shadows).await.expect("old shadow").is_none());
            }

            let cancellation = crate::CancellationToken::new();
            let rejected = store.compare_and_swap_lazy_workspace_measured(
                prepared.workspace_id, 0, prepared.clone(), crate::WorkBudget::UNBOUNDED, &cancellation,
            ).await.expect("stale freeze is a CAS miss");
            assert!(!rejected.value);
            assert!(rejected.work.items_examined > 0);
            assert_eq!(rejected.work.object_bytes_written, 0);
            assert!(store.load_lazy_workspace(prepared.workspace_id).await.expect("child").is_none());

            let refreshed = prepared_origin_fixture(&updated);
            assert!(store.compare_and_swap_lazy_workspace(
                refreshed.workspace_id, 0, refreshed,
            ).await.expect("current source freeze"));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn original_freeze_recovers_without_ack_and_retains_roots_after_parent_changes() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::open_owned(directory.path()).expect("owned store");
        let parent = origin_parent_fixture(&store).await;
        let prepared = prepared_origin_fixture(&parent);
        // The caller loses the first write's acknowledgement, then recovers
        // only the authentic persisted original record after reopening.
        let _ = store.compare_and_swap_lazy_workspace(
            prepared.workspace_id, 0, prepared.clone(),
        ).await.expect("durable original freeze");
        drop(store);
        let store = LocalCoreStateStore::open_owned(directory.path()).expect("reopened store");
        assert_eq!(store.load_lazy_workspace(prepared.workspace_id).await.expect("recover"), Some(prepared.clone()));
        let updated = advanced_origin_parent(&parent);
        assert!(store.compare_and_swap_lazy_workspace(
            parent.workspace_id, parent.revision, updated.clone(),
        ).await.expect("advance mutable parent"));
        assert_eq!(store.collect_lazy_nodes().await.expect("prepared origin roots"), 0);
        assert!(!store.compare_and_swap_lazy_workspace(
            prepared.workspace_id, 0, prepared.clone(),
        ).await.expect("lost acknowledgement retry is revision fenced"));

        let mut committed = prepared.clone();
        committed.revision += 1;
        committed.fork_origin.as_mut().expect("origin").phase = crate::LazyForkPhase::Committed;
        assert!(store.compare_and_swap_lazy_workspace(
            committed.workspace_id, prepared.revision, committed.clone(),
        ).await.expect("commit original without rereading mutable parent"));
        let child = LazyWorkspaceState {
            revision: committed.revision + 1,
            source: updated.source,
            overlay: updated.overlay,
            shadows: updated.shadows,
            ..committed.clone()
        };
        assert!(store.compare_and_swap_lazy_workspace(
            child.workspace_id, committed.revision, child.clone(),
        ).await.expect("later child observation epoch"));
        assert_eq!(store.collect_lazy_nodes().await.expect("committed origin roots"), 0);
        assert!(store.load_lazy_overlay(parent.overlay).await.expect("original overlay retained").is_some());
        assert!(store.load_lazy_shadow(parent.shadows).await.expect("original shadows retained").is_some());
        let origin = prepared.fork_origin.expect("original");
        assert_eq!(store.lazy_retained_generations().await.expect("original generation roots"),
            [(origin.snapshot.authored_generation, origin.config)]);

        let next = LazyWorkspaceState { revision: child.revision + 1, ..child.clone() };
        let mut removed = next.clone();
        removed.fork_origin = None;
        let mut new_operation = next.clone();
        new_operation.fork_origin.as_mut().expect("origin").operation_id = OperationId::from_bytes([0xb0; 16]);
        let mut new_config = next.clone();
        new_config.fork_origin.as_mut().expect("origin").config.limits.maximum_read_bytes += 1;
        let mut new_snapshot = next.clone();
        new_snapshot.fork_origin.as_mut().expect("origin").snapshot.authored_generation =
            crate::GenerationId::new(Digest::from_bytes([0xb1; 32]));
        let mut new_parent = next.clone();
        new_parent.parent_workspace_id = Some(WorkspaceId::from_bytes([0xb2; 16]));
        let mut regressed = next;
        regressed.fork_origin.as_mut().expect("origin").phase = crate::LazyForkPhase::Prepared;
        for replacement in [removed, new_operation, new_config, new_snapshot, new_parent, regressed] {
            assert!(!store.compare_and_swap_lazy_workspace(
                child.workspace_id, child.revision, replacement,
            ).await.expect("immutable original rejects replacement"));
        }
        assert_eq!(store.load_lazy_workspace(child.workspace_id).await.expect("unchanged child"), Some(child));
    }
}
