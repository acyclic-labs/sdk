//! Journaled local persistence for filesystem control-plane state.
//!
//! Distributed deployments can implement the small CAS traits over their
//! authority stream. Embedded native deployments use this companion namespace:
//! one lock and one recoverable JSON record per workspace and state family.
//! Keeping the implementation here prevents adapters from inventing separate
//! lineage, lease, and Git-compatibility databases.

use crate::workspace_context::{
    WorkspaceContextChildren, context_children, discard_context_subtree,
    plan_context_subtree_discard, update_context_children,
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
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};
use thiserror::Error;

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
    /// Durable `lazy-workspaces` records, `None` when absent.
    ///
    /// The mutex serializes every owned access to that family's files, so
    /// owned access needs no per-record file lock. An entry exists only while
    /// it equals the durable record: a miss fills under the mutex, and a
    /// compare-and-swap removes its entry before writing and inserts the
    /// replacement only once it is durable. A failed or panicking write leaves
    /// the entry absent, so even a poisoned map holds only exact entries.
    lazy_workspaces: Mutex<HashMap<WorkspaceId, Option<LazyWorkspaceState>>>,
}

impl Owner {
    fn lazy_workspaces(&self) -> MutexGuard<'_, HashMap<WorkspaceId, Option<LazyWorkspaceState>>> {
        self.lazy_workspaces
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// One held `owner.lock`, released when dropped.
#[derive(Debug)]
struct OwnerLock(File);

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
            Ok(()) => Ok(Self(lock)),
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
        let _ = FileExt::unlock(&self.0);
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
        }
    }

    /// Opens the namespace below `root` as its sole owner for as long as this
    /// store or any clone of it is alive.
    ///
    /// Fails with [`LocalCoreStateStoreError::OwnershipConflict`] while another
    /// store owns the namespace or has a transaction in flight. An owner serves
    /// lazy-workspace bindings from memory after their first load.
    pub fn open_owned(root: impl Into<PathBuf>) -> Result<Self, LocalCoreStateStoreError> {
        let root = root.into();
        let lock = OwnerLock::acquire(&root, LockMode::Exclusive)?;
        Ok(Self {
            root,
            ownership: Ownership::Owned(Arc::new(Owner {
                _lock: lock,
                lazy_workspaces: Mutex::default(),
            })),
        })
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
        Ok(Namespace {
            root: self.root.clone(),
            admission,
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

    /// Returns an owner's cached binding (`Some(None)` caches absence).
    ///
    /// A compare-and-swap holds the cache across durable I/O, so contention
    /// falls back to a transaction that waits off the executor.
    fn cached_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Option<Option<LazyWorkspaceState>> {
        let Ownership::Owned(owner) = &self.ownership else {
            return None;
        };
        owner
            .lazy_workspaces
            .try_lock()
            .ok()?
            .get(&workspace_id)
            .cloned()
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

    fn load_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<LazyWorkspaceState>, LocalCoreStateStoreError> {
        let paths = self.record(LAZY_WORKSPACE_FAMILY, &workspace_id.into_bytes());
        let Admission::Owned(owner) = &self.admission else {
            return read_locked(&paths);
        };
        let mut records = owner.lazy_workspaces();
        if let Some(state) = records.get(&workspace_id) {
            return Ok(state.clone());
        }
        let state = read_recoverable(&paths)?;
        records.insert(workspace_id, state.clone());
        Ok(state)
    }

    fn compare_and_swap_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
    ) -> Result<bool, LocalCoreStateStoreError> {
        let paths = self.record(LAZY_WORKSPACE_FAMILY, &workspace_id.into_bytes());
        let Admission::Owned(owner) = &self.admission else {
            return compare_and_swap(&paths, expected_revision, &replacement, |state| {
                state.revision
            });
        };
        let mut records = owner.lazy_workspaces();
        let current = match records.remove(&workspace_id) {
            Some(current) => current,
            None => read_recoverable(&paths)?,
        };
        if current.as_ref().map_or(0, |state| state.revision) != expected_revision {
            records.insert(workspace_id, current);
            return Ok(false);
        }
        std::fs::create_dir_all(&paths.directory)?;
        write_journaled(&paths, &replacement)?;
        records.insert(workspace_id, Some(replacement));
        Ok(true)
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

fn compare_and_swap<T: DeserializeOwned + Serialize>(
    paths: &RecordPaths,
    expected_revision: u64,
    replacement: &T,
    revision: impl Fn(&T) -> u64,
) -> Result<bool, LocalCoreStateStoreError> {
    with_lock(paths, || {
        let current: Option<T> = read_recoverable(paths)?;
        if current.as_ref().map_or(0, &revision) != expected_revision {
            return Ok(false);
        }
        write_journaled(paths, replacement)?;
        Ok(true)
    })
}

fn compare_and_delete<T: DeserializeOwned>(
    paths: &RecordPaths,
    expected_revision: u64,
    revision: impl Fn(&T) -> u64,
) -> Result<bool, LocalCoreStateStoreError> {
    with_lock(paths, || {
        let current: Option<T> = read_recoverable(paths)?;
        if current.as_ref().map_or(0, &revision) != expected_revision {
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

fn read_recoverable_bounded<T: DeserializeOwned>(
    paths: &RecordPaths,
    maximum_bytes: u64,
) -> Result<Option<T>, LocalCoreStateStoreError> {
    match read_json_bounded(&paths.current, maximum_bytes) {
        Ok(Some(value)) => Ok(Some(value)),
        Ok(None) => match read_json_bounded(&paths.previous, maximum_bytes) {
            Ok(Some(value)) => {
                promote_previous(paths)?;
                Ok(Some(value))
            }
            other => other,
        },
        Err(current_error) => match read_json_bounded(&paths.previous, maximum_bytes) {
            Ok(Some(value)) => {
                promote_previous(paths)?;
                Ok(Some(value))
            }
            _ => Err(current_error),
        },
    }
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

fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
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
    /// Content-addressed state did not match its claimed digest.
    #[error("core state content address does not match its payload")]
    Integrity,
    /// A durable workspace-context transaction was malformed or incompatible.
    #[error("workspace-context transaction is incompatible: {0}")]
    ContextTransaction(String),
}

impl WorkspaceLineageStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<WorkspaceLineageRecord>, Self::Error> {
        self.transaction(move |namespace| {
            read_locked(&namespace.record("lineage", &workspace_id.into_bytes()))
        })
        .await
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        self.transaction(move |namespace| {
            compare_and_swap(
                &namespace.record("lineage", &workspace_id.into_bytes()),
                expected_revision,
                &replacement,
                |record| record.revision,
            )
        })
        .await
    }
}

impl WorkspaceContextStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(
        &self,
        context_id: WorkspaceContextId,
    ) -> Result<Option<WorkspaceContext>, Self::Error> {
        self.transaction(move |root| {
            let transaction = context_transaction_paths(root);
            with_lock(&transaction, || {
                recover_context_transaction(root, &transaction)?;
                read_recoverable(&context_record_paths(root, context_id))
            })
        })
        .await
    }

    async fn list(&self) -> Result<Vec<WorkspaceContext>, Self::Error> {
        self.transaction(move |root| {
            let transaction = context_transaction_paths(root);
            with_lock(&transaction, || {
                recover_context_transaction(root, &transaction)?;
                let contexts = load_all_contexts(root)?;
                if !context_children_index_ready(root)? {
                    install_context_children_index(root, contexts.iter().cloned())?;
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
            let transaction = context_transaction_paths(root);
            with_lock(&transaction, || {
                recover_context_transaction(root, &transaction)?;
                if require_exact_set && context_record_ids(root)?.len() != expected_revisions.len()
                {
                    return Ok(false);
                }
                let mut before = BTreeMap::new();
                for (context_id, expected) in &expected_revisions {
                    let current: Option<WorkspaceContext> =
                        read_recoverable(&context_record_paths(root, *context_id))?;
                    if current.as_ref().map_or(0, |record| record.revision) != *expected {
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
                ensure_context_children_index(root)?;
                let affected_parents = before
                    .values()
                    .filter_map(|record| record.as_ref()?.parent_context_id)
                    .chain(after.values().filter_map(|record| record.parent_context_id))
                    .collect::<BTreeSet<_>>();
                let mut before_children = BTreeMap::new();
                let mut after_children = WorkspaceContextChildren::new();
                for parent in affected_parents {
                    let current = read_recoverable(&context_children_paths(root, parent))?;
                    after_children.insert(parent, current.clone().unwrap_or_default());
                    before_children.insert(parent, current);
                }
                for (context_id, replacement) in &after {
                    update_context_children(
                        &mut after_children,
                        before.get(context_id).and_then(Option::as_ref),
                        Some(replacement),
                    );
                }
                let journal = ContextTransaction {
                    version: 2,
                    phase: ContextTransactionPhase::Prepared,
                    before: before
                        .into_iter()
                        .filter(|(context_id, _)| after.contains_key(context_id))
                        .collect(),
                    after,
                    before_children,
                    after_children,
                };
                write_journaled(&transaction, &journal)?;
                apply_context_transaction(root, &journal)?;
                write_journaled(
                    &transaction,
                    &ContextTransaction {
                        phase: ContextTransactionPhase::Committed,
                        ..journal
                    },
                )?;
                recover_context_transaction(root, &transaction)?;
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
            let transaction = context_transaction_paths(root_path);
            with_lock(&transaction, || {
                recover_context_transaction(root_path, &transaction)?;
                if !context_children_index_ready(root_path)? {
                    return Ok(WorkspaceContextDiscardOutcome::IncompatibleState);
                }
                let root: Option<WorkspaceContext> =
                    read_recoverable(&context_record_paths(root_path, child))?;
                let discarded = match plan_local_context_subtree_discard(
                    root_path,
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
                    let Some(record) =
                        read_recoverable(&context_record_paths(root_path, context_id))?
                    else {
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
                let mut before = BTreeMap::new();
                let mut after = BTreeMap::new();
                for context_id in discarded {
                    let current: Option<WorkspaceContext> =
                        read_recoverable(&context_record_paths(root_path, *context_id))?;
                    before.insert(*context_id, current);
                    let replacement = records.get(context_id).cloned().ok_or_else(|| {
                        LocalCoreStateStoreError::ContextTransaction(
                            "discarded context replacement is absent".to_owned(),
                        )
                    })?;
                    after.insert(*context_id, replacement);
                }
                let journal = ContextTransaction {
                    version: 1,
                    phase: ContextTransactionPhase::Prepared,
                    before,
                    after,
                    before_children: BTreeMap::new(),
                    after_children: WorkspaceContextChildren::new(),
                };
                write_journaled(&transaction, &journal)?;
                apply_context_records(root_path, &journal.after)?;
                write_journaled(
                    &transaction,
                    &ContextTransaction {
                        phase: ContextTransactionPhase::Committed,
                        ..journal
                    },
                )?;
                recover_context_transaction(root_path, &transaction)?;
                Ok(outcome)
            })
        })
        .await
    }
}

const WORKSPACE_CONTEXT_FAMILY: &str = "workspace-contexts-v2";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum ContextTransactionPhase {
    Prepared,
    Committed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ContextTransaction {
    version: u16,
    phase: ContextTransactionPhase,
    before: BTreeMap<WorkspaceContextId, Option<WorkspaceContext>>,
    after: BTreeMap<WorkspaceContextId, WorkspaceContext>,
    #[serde(default)]
    before_children: BTreeMap<WorkspaceContextId, Option<BTreeSet<WorkspaceContextId>>>,
    #[serde(default)]
    after_children: WorkspaceContextChildren,
}

fn context_transaction_paths(root: &Namespace) -> RecordPaths<'_> {
    root.record("workspace-context-transactions-v2", &[0; 16])
}

fn context_record_paths(root: &Namespace, context_id: WorkspaceContextId) -> RecordPaths<'_> {
    root.record(WORKSPACE_CONTEXT_FAMILY, &context_id.into_bytes())
}

fn context_children_paths(root: &Namespace, parent: WorkspaceContextId) -> RecordPaths<'_> {
    root.record("workspace-context-children-v1", &parent.into_bytes())
}

fn context_children_marker_paths(root: &Namespace) -> RecordPaths<'_> {
    root.record("workspace-context-children-index-v1", &[0; 16])
}

fn context_children_count_paths(root: &Namespace, parent: WorkspaceContextId) -> RecordPaths<'_> {
    root.record("workspace-context-child-counts-v1", &parent.into_bytes())
}

fn context_children_index_ready(root: &Namespace) -> Result<bool, LocalCoreStateStoreError> {
    Ok(read_recoverable::<u16>(&context_children_marker_paths(root))?.is_some())
}

fn write_context_children(
    root: &Namespace,
    parent: WorkspaceContextId,
    descendants: &BTreeSet<WorkspaceContextId>,
) -> Result<(), LocalCoreStateStoreError> {
    let paths = context_children_paths(root, parent);
    std::fs::create_dir_all(&paths.directory)?;
    write_journaled(&paths, descendants)?;
    let count = u64::try_from(descendants.len()).map_err(|_| {
        LocalCoreStateStoreError::ContextTransaction(
            "workspace-context child count exceeds durable representation".to_owned(),
        )
    })?;
    let count_paths = context_children_count_paths(root, parent);
    std::fs::create_dir_all(&count_paths.directory)?;
    write_journaled(&count_paths, &count)
}

fn ensure_context_children_index(root: &Namespace) -> Result<(), LocalCoreStateStoreError> {
    if context_children_index_ready(root)? {
        return Ok(());
    }
    install_context_children_index(root, load_all_contexts(root)?)
}

fn install_context_children_index(
    root: &Namespace,
    records: impl IntoIterator<Item = WorkspaceContext>,
) -> Result<(), LocalCoreStateStoreError> {
    let marker = context_children_marker_paths(root);
    let children = context_children(records);
    for (parent, descendants) in children {
        write_context_children(root, parent, &descendants)?;
    }
    std::fs::create_dir_all(&marker.directory)?;
    write_journaled(&marker, &1_u16)
}

fn plan_local_context_subtree_discard(
    root_path: &Namespace,
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
        let count = read_recoverable::<u64>(&context_children_count_paths(root_path, context_id))?
            .unwrap_or(0);
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
        let descendants: BTreeSet<WorkspaceContextId> = read_recoverable_bounded(
            &context_children_paths(root_path, context_id),
            maximum_bucket_bytes,
        )?
        .unwrap_or_default();
        if usize::try_from(count) != Ok(descendants.len()) {
            return Ok(Err(WorkspaceContextDiscardOutcome::IncompatibleState));
        }
        pending.extend(descendants);
    }
    selected.clear();
    selected.extend(seen);
    Ok(Ok(selected))
}

fn context_record_ids(
    root: &Namespace,
) -> Result<Vec<WorkspaceContextId>, LocalCoreStateStoreError> {
    let entries = match std::fs::read_dir(root.family(WORKSPACE_CONTEXT_FAMILY)) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut ids = Vec::new();
    for entry in entries {
        let name = entry?.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(".json") else {
            continue;
        };
        if stem.ends_with(".previous") || stem.ends_with(".next") {
            continue;
        }
        let Ok(bytes) = hex::decode(stem) else {
            continue;
        };
        let Ok(bytes) = <[u8; 16]>::try_from(bytes) else {
            continue;
        };
        ids.push(WorkspaceContextId::from_bytes(bytes));
    }
    ids.sort_unstable_by_key(|id| id.into_bytes());
    Ok(ids)
}

fn load_all_contexts(root: &Namespace) -> Result<Vec<WorkspaceContext>, LocalCoreStateStoreError> {
    context_record_ids(root)?
        .into_iter()
        .map(|id| {
            read_recoverable(&context_record_paths(root, id))?.ok_or_else(|| {
                LocalCoreStateStoreError::ContextTransaction(
                    "workspace context disappeared during locked discovery".to_owned(),
                )
            })
        })
        .collect()
}

fn apply_context_records(
    root: &Namespace,
    records: &BTreeMap<WorkspaceContextId, WorkspaceContext>,
) -> Result<(), LocalCoreStateStoreError> {
    for (context_id, record) in records {
        let paths = context_record_paths(root, *context_id);
        std::fs::create_dir_all(&paths.directory)?;
        write_journaled(&paths, record)?;
    }
    Ok(())
}

fn apply_context_transaction(
    root: &Namespace,
    journal: &ContextTransaction,
) -> Result<(), LocalCoreStateStoreError> {
    apply_context_records(root, &journal.after)?;
    if journal.version >= 2 {
        for (parent, descendants) in &journal.after_children {
            write_context_children(root, *parent, descendants)?;
        }
    }
    Ok(())
}

fn restore_context_transaction(
    root: &Namespace,
    journal: &ContextTransaction,
) -> Result<(), LocalCoreStateStoreError> {
    restore_context_records(root, &journal.before)?;
    if journal.version >= 2 {
        for (parent, descendants) in &journal.before_children {
            let paths = context_children_paths(root, *parent);
            if let Some(descendants) = descendants {
                write_context_children(root, *parent, descendants)?;
            } else {
                std::fs::create_dir_all(&paths.directory)?;
                remove_record(&paths)?;
                let count_paths = context_children_count_paths(root, *parent);
                std::fs::create_dir_all(&count_paths.directory)?;
                remove_record(&count_paths)?;
            }
        }
    }
    Ok(())
}

fn restore_context_records(
    root: &Namespace,
    records: &BTreeMap<WorkspaceContextId, Option<WorkspaceContext>>,
) -> Result<(), LocalCoreStateStoreError> {
    for (context_id, record) in records {
        let paths = context_record_paths(root, *context_id);
        std::fs::create_dir_all(&paths.directory)?;
        if let Some(record) = record {
            write_journaled(&paths, record)?;
        } else {
            remove_record(&paths)?;
        }
    }
    Ok(())
}

fn recover_context_transaction(
    root: &Namespace,
    paths: &RecordPaths,
) -> Result<(), LocalCoreStateStoreError> {
    let Some(journal) = read_recoverable::<ContextTransaction>(paths)? else {
        return Ok(());
    };
    if !matches!(journal.version, 1 | 2) {
        return Err(LocalCoreStateStoreError::ContextTransaction(
            "unsupported workspace-context transaction version".to_owned(),
        ));
    }
    match journal.phase {
        ContextTransactionPhase::Prepared => restore_context_transaction(root, &journal)?,
        ContextTransactionPhase::Committed => apply_context_transaction(root, &journal)?,
    }
    remove_record(paths)
}

impl GitCompatStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(&self, workspace_id: WorkspaceId) -> Result<Option<GitCompatState>, Self::Error> {
        self.transaction(move |namespace| {
            read_locked(&namespace.record(GIT_COMPAT_NAMESPACE, &workspace_id.into_bytes()))
        })
        .await
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: GitCompatState,
    ) -> Result<bool, Self::Error> {
        self.transaction(move |namespace| {
            compare_and_swap(
                &namespace.record(GIT_COMPAT_NAMESPACE, &workspace_id.into_bytes()),
                expected_revision,
                &replacement,
                |record| record.revision,
            )
        })
        .await
    }

    async fn compare_and_delete(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
    ) -> Result<bool, Self::Error> {
        self.transaction(move |namespace| {
            compare_and_delete(
                &namespace.record(GIT_COMPAT_NAMESPACE, &workspace_id.into_bytes()),
                expected_revision,
                |record: &GitCompatState| record.revision,
            )
        })
        .await
    }
}

impl MaterializationJournalStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationJournal>, Self::Error> {
        self.transaction(move |namespace| {
            read_locked(&namespace.record(MATERIALIZATION_FAMILY, &operation_id.into_bytes()))
        })
        .await
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MaterializationJournal,
    ) -> Result<bool, Self::Error> {
        self.transaction(move |namespace| {
            compare_and_swap(
                &namespace.record(MATERIALIZATION_FAMILY, &operation_id.into_bytes()),
                expected_revision,
                &replacement,
                |journal| journal.revision,
            )
        })
        .await
    }
}

impl MultiRootPublicationStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MultiRootPublication>, Self::Error> {
        self.transaction(move |namespace| {
            read_locked(
                &namespace.record(MULTI_ROOT_PUBLICATION_FAMILY, &operation_id.into_bytes()),
            )
        })
        .await
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MultiRootPublication,
    ) -> Result<bool, Self::Error> {
        self.transaction(move |namespace| {
            compare_and_swap(
                &namespace.record(MULTI_ROOT_PUBLICATION_FAMILY, &operation_id.into_bytes()),
                expected_revision,
                &replacement,
                |publication| publication.revision,
            )
        })
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
        self.transaction(move |namespace| {
            compare_and_delete(
                &namespace.record(MULTI_ROOT_PUBLICATION_FAMILY, &operation_id.into_bytes()),
                expected_revision,
                |publication: &MultiRootPublication| publication.revision,
            )
        })
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
                    |claim| claim.revision,
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
            compare_and_delete(&paths, claim.revision, |claim: &MultiRootParentClaim| {
                claim.revision
            })
        })
        .await
    }
}

#[async_trait::async_trait]
impl LazyWorkspaceStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<LazyWorkspaceState>, Self::Error> {
        if let Some(state) = self.cached_lazy_workspace(workspace_id) {
            return Ok(state);
        }
        self.transaction(move |namespace| namespace.load_lazy_workspace(workspace_id))
            .await
    }

    async fn compare_and_swap_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
    ) -> Result<bool, Self::Error> {
        self.transaction(move |namespace| {
            namespace.compare_and_swap_lazy_workspace(workspace_id, expected_revision, replacement)
        })
        .await
    }

    async fn load_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
    ) -> Result<Option<LazyOverlay>, Self::Error> {
        self.transaction(move |namespace| {
            load_content_addressed(namespace, "lazy-overlays", overlay.into_bytes())
        })
        .await
    }

    async fn put_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
        value: LazyOverlay,
    ) -> Result<(), Self::Error> {
        self.transaction(move |namespace| {
            put_content_addressed(namespace, "lazy-overlays", overlay.into_bytes(), &value)
        })
        .await
    }

    async fn load_lazy_shadow(
        &self,
        shadow: crate::LazyShadowId,
    ) -> Result<Option<crate::LazyShadow>, Self::Error> {
        self.transaction(move |namespace| {
            load_content_addressed(namespace, "lazy-shadows-v1", shadow.into_bytes())
        })
        .await
    }

    async fn put_lazy_shadow(
        &self,
        shadow: crate::LazyShadowId,
        value: crate::LazyShadow,
    ) -> Result<(), Self::Error> {
        self.transaction(move |namespace| {
            put_content_addressed(namespace, "lazy-shadows-v1", shadow.into_bytes(), &value)
        })
        .await
    }
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
    let encoded = serde_json::to_vec(value)?;
    if blake3::hash(&encoded).as_bytes() != &address {
        return Err(LocalCoreStateStoreError::Integrity);
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{
        Digest, WorkspaceContextRoot, WorkspaceContextState, WorkspaceName, WorkspaceRootId,
    };

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
        let paths = namespace.record("lineage", &held_id.into_bytes());
        std::fs::create_dir_all(&paths.directory).expect("lock directory");
        let held = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&paths.lock)
            .expect("held record lock");
        held.lock_exclusive().expect("hold record lock");

        let contender =
            tokio::spawn(async move { WorkspaceLineageStore::load(&store, held_id).await });
        let unrelated = LocalCoreStateStore::new(directory.path());
        tokio::time::timeout(
            Duration::from_secs(2),
            WorkspaceLineageStore::load(&unrelated, other_id),
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
    async fn state_survives_reopen_and_recovers_previous_record() {
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
        let paths = namespace.record("lineage", &workspace().into_bytes());
        std::fs::copy(&paths.current, &paths.previous).expect("preserve previous record");
        std::fs::write(&paths.current, b"torn").expect("simulate torn current record");
        std::fs::write(&paths.temporary, b"uncommitted").expect("simulate temporary record");
        let reopened = LocalCoreStateStore::new(directory.path());
        assert_eq!(
            WorkspaceLineageStore::load(&reopened, workspace())
                .await
                .expect("reopen"),
            Some(state)
        );
        assert!(
            read_json::<WorkspaceLineageRecord>(&paths.current)
                .expect("repaired current")
                .is_some()
        );
    }

    #[tokio::test]
    async fn lazy_overlay_load_rejects_content_address_mismatch() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let namespace = store.namespace().expect("admitted namespace");
        let empty = LazyOverlay::default();
        let encoded = serde_json::to_vec(&empty).expect("serialize overlay");
        let overlay = LazyOverlayId::from_bytes(*blake3::hash(&encoded).as_bytes());
        LazyWorkspaceStore::put_lazy_overlay(&store, overlay, empty)
            .await
            .expect("persist overlay");

        let paths = namespace.record("lazy-overlays", &overlay.into_bytes());
        let different = serde_json::json!({
            "Node": {
                "path": "/tampered",
                "priority": 0,
                "change": "Tombstone",
                "left": vec![0_u8; 32],
                "right": vec![0_u8; 32]
            }
        });
        std::fs::write(
            &paths.current,
            serde_json::to_vec(&different).expect("serialize tampered overlay"),
        )
        .expect("tamper overlay");
        assert!(matches!(
            LazyWorkspaceStore::load_lazy_overlay(&store, overlay).await,
            Err(LocalCoreStateStoreError::Integrity)
        ));
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

        std::fs::write(
            context_record_paths(&namespace, unrelated_id).current,
            b"not-json",
        )
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
        write_journaled(
            &context_children_count_paths(&namespace, child_id),
            &10_000_u64,
        )
        .expect("write oversized child count");
        let bucket = context_children_paths(&namespace, child_id);
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
        write_journaled(&context_children_count_paths(&namespace, child_id), &1_u64)
            .expect("write understated child count");
        let bucket = context_children_paths(&namespace, child_id);
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
            let paths = context_record_paths(&namespace, context.context_id);
            std::fs::create_dir_all(&paths.directory).expect("context directory");
            write_journaled(&paths, &context).expect("write legacy context");
        }
        std::fs::write(
            context_record_paths(&namespace, unrelated_id).current,
            b"not-json",
        )
        .expect("corrupt unrelated legacy record");

        assert_eq!(
            WorkspaceContextStore::discard_subtree(&store, parent_id, child_id, 1)
                .await
                .expect("fail closed without rebuilding index"),
            WorkspaceContextDiscardOutcome::IncompatibleState
        );
    }

    #[tokio::test]
    async fn prepared_context_transaction_rolls_back_partial_record_application() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let namespace = store.namespace().expect("admitted namespace");
        let context_id = WorkspaceContextId::from_bytes([10; 16]);
        let before = workspace_context(
            directory.path(),
            context_id,
            1,
            WorkspaceContextState::Active,
        );
        let after = workspace_context(
            directory.path(),
            context_id,
            2,
            WorkspaceContextState::Frozen,
        );
        std::fs::create_dir_all(directory.path().join(WORKSPACE_CONTEXT_FAMILY))
            .expect("context directory");
        std::fs::create_dir_all(directory.path().join("workspace-context-transactions-v2"))
            .expect("transaction directory");
        write_journaled(&context_record_paths(&namespace, context_id), &before)
            .expect("initial context");
        write_journaled(
            &context_transaction_paths(&namespace),
            &ContextTransaction {
                version: 1,
                phase: ContextTransactionPhase::Prepared,
                before: [(context_id, Some(before.clone()))].into_iter().collect(),
                after: [(context_id, after.clone())].into_iter().collect(),
                before_children: BTreeMap::new(),
                after_children: WorkspaceContextChildren::new(),
            },
        )
        .expect("prepared transaction");
        write_journaled(&context_record_paths(&namespace, context_id), &after)
            .expect("partial application");

        assert_eq!(
            WorkspaceContextStore::load(&store, context_id)
                .await
                .expect("recover prepared transaction"),
            Some(before)
        );
    }

    #[tokio::test]
    async fn prepared_context_transaction_rolls_back_child_index_with_record() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let namespace = store.namespace().expect("admitted namespace");
        let parent_id = WorkspaceContextId::from_bytes([23; 16]);
        let child_id = WorkspaceContextId::from_bytes([24; 16]);
        let mut child =
            workspace_context(directory.path(), child_id, 1, WorkspaceContextState::Active);
        child.parent_context_id = Some(parent_id);
        ensure_context_children_index(&namespace).expect("initialize child index");
        let journal = ContextTransaction {
            version: 2,
            phase: ContextTransactionPhase::Prepared,
            before: [(child_id, None)].into_iter().collect(),
            after: [(child_id, child.clone())].into_iter().collect(),
            before_children: [(parent_id, None)].into_iter().collect(),
            after_children: [(parent_id, BTreeSet::from([child_id]))]
                .into_iter()
                .collect(),
        };
        std::fs::create_dir_all(directory.path().join("workspace-context-transactions-v2"))
            .expect("transaction directory");
        write_journaled(&context_transaction_paths(&namespace), &journal)
            .expect("prepared transaction");
        apply_context_transaction(&namespace, &journal).expect("partial application");

        assert_eq!(
            WorkspaceContextStore::load(&store, child_id)
                .await
                .expect("recover prepared transaction"),
            None
        );
        assert_eq!(
            read_recoverable::<BTreeSet<WorkspaceContextId>>(&context_children_paths(
                &namespace, parent_id,
            ))
            .expect("read recovered child bucket"),
            None
        );
    }

    #[tokio::test]
    async fn committed_context_transaction_rolls_forward_missing_record_application() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let namespace = store.namespace().expect("admitted namespace");
        let context_id = WorkspaceContextId::from_bytes([11; 16]);
        let before = workspace_context(
            directory.path(),
            context_id,
            1,
            WorkspaceContextState::Active,
        );
        let after = workspace_context(
            directory.path(),
            context_id,
            2,
            WorkspaceContextState::Frozen,
        );
        std::fs::create_dir_all(directory.path().join(WORKSPACE_CONTEXT_FAMILY))
            .expect("context directory");
        std::fs::create_dir_all(directory.path().join("workspace-context-transactions-v2"))
            .expect("transaction directory");
        write_journaled(&context_record_paths(&namespace, context_id), &before)
            .expect("initial context");
        write_journaled(
            &context_transaction_paths(&namespace),
            &ContextTransaction {
                version: 1,
                phase: ContextTransactionPhase::Committed,
                before: [(context_id, Some(before))].into_iter().collect(),
                after: [(context_id, after.clone())].into_iter().collect(),
                before_children: BTreeMap::new(),
                after_children: WorkspaceContextChildren::new(),
            },
        )
        .expect("committed transaction");

        assert_eq!(
            WorkspaceContextStore::load(&store, context_id)
                .await
                .expect("recover committed transaction"),
            Some(after)
        );
    }

    #[tokio::test]
    async fn git_compatibility_delete_is_revision_fenced_and_durable() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let namespace = store.namespace().expect("admitted namespace");
        let old_paths = namespace.record("git-compat", &workspace().into_bytes());
        std::fs::create_dir_all(&old_paths.directory).expect("legacy state directory");
        write_journaled(&old_paths, &GitCompatState::new("main", workspace()))
            .expect("legacy state remains isolated");
        assert!(
            GitCompatStore::load(&store, workspace())
                .await
                .expect("new namespace load")
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
        assert!(old_paths.current.exists(), "legacy state is left untouched");
    }

    #[tokio::test]
    async fn materialization_operations_are_discoverable_and_terminal_cleanup_is_safe() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let operation_id = OperationId::new();
        let journal = MaterializationJournal {
            version: 1,
            revision: 1,
            plan: crate::MaterializationPlan {
                operation_id,
                from: crate::GenerationId::new(Digest::from_bytes([1; 32])),
                to: crate::GenerationId::new(Digest::from_bytes([2; 32])),
                edits: Vec::new(),
            },
            preimages: Vec::new(),
            phase: crate::MaterializationPhase::Prepared,
            applied: 0,
            restored: 0,
        };
        assert!(
            MaterializationJournalStore::compare_and_swap(&store, operation_id, 0, journal.clone())
                .await
                .expect("write journal")
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
            MaterializationJournalStore::compare_and_swap(&store, operation_id, 1, terminal)
                .await
                .expect("finish journal")
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
            version: 1,
            revision: 1,
            plan: crate::MaterializationPlan {
                operation_id,
                from: crate::GenerationId::new(Digest::from_bytes([1; 32])),
                to: crate::GenerationId::new(Digest::from_bytes([2; 32])),
                edits: Vec::new(),
            },
            preimages: Vec::new(),
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
                .expect("recover journal"),
            Some(journal)
        );
        assert!(paths.current.exists());
    }

    /// The store treats bindings as opaque records.
    fn lazy_state(workspace_id: WorkspaceId, revision: u64) -> LazyWorkspaceState {
        let shadows = [3_u8; 32];
        serde_json::from_value(serde_json::json!({
            "schema_version": 7,
            "revision": revision,
            "workspace_id": workspace_id,
            "parent_workspace_id": null,
            "source": crate::demand::SourceReference { identity: [1; 16], epoch: 0 },
            "overlay": LazyOverlayId::from_bytes([2; 32]),
            "shadows": shadows,
            "pending_remove": null,
        }))
        .expect("lazy binding fixture")
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
        struct KillOnDrop(std::process::Child);

        impl Drop for KillOnDrop {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        let directory = tempfile::tempdir()?;
        let root = directory.path().join("core-state");
        let owner = LocalCoreStateStore::open_owned(&root)?;
        assert!(
            ownership_child(&root)?.status()?.success(),
            "another process entered an owned namespace"
        );
        drop(owner);

        let ready = directory.path().join("ready");
        let mut child = KillOnDrop(
            ownership_child(&root)?
                .env("ACYCLIC_CORE_STATE_CHILD_READY", &ready)
                .spawn()?,
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready.is_file() {
            if let Some(status) = child.0.try_wait()? {
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
        child.0.kill()?;
        child.0.wait()?;
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
    async fn owner_recovers_a_torn_lazy_workspace_record() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let owner = LocalCoreStateStore::open_owned(directory.path()).expect("owner");
        let state = lazy_state(workspace(), 1);
        assert!(
            owner
                .compare_and_swap_lazy_workspace(workspace(), 0, state.clone())
                .await
                .expect("initial binding")
        );
        drop(owner);

        let current = {
            let namespace = LocalCoreStateStore::new(directory.path())
                .namespace()
                .expect("admitted namespace");
            let paths = namespace.record(LAZY_WORKSPACE_FAMILY, &workspace().into_bytes());
            std::fs::copy(&paths.current, &paths.previous).expect("preserve previous record");
            std::fs::write(&paths.current, b"torn").expect("simulate torn current record");
            paths.current.clone()
        };

        let owner = LocalCoreStateStore::open_owned(directory.path()).expect("reopened owner");
        assert_eq!(
            owner
                .load_lazy_workspace(workspace())
                .await
                .expect("recover binding"),
            Some(state.clone())
        );
        assert_eq!(
            read_json::<LazyWorkspaceState>(&current).expect("repaired current"),
            Some(state.clone())
        );
        assert!(
            owner
                .compare_and_swap_lazy_workspace(
                    workspace(),
                    1,
                    LazyWorkspaceState {
                        revision: 2,
                        ..state
                    },
                )
                .await
                .expect("swap recovered binding")
        );
    }
}
