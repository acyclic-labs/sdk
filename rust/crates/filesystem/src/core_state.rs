//! Journaled local persistence for filesystem control-plane state.
//!
//! Distributed deployments can implement the small CAS traits over their
//! authority stream. Embedded native deployments use this companion namespace:
//! one recoverable JSON record per workspace and state family. Contexts,
//! lineage, and lazy-workspace bindings change through one write-ahead log
//! (`CoreLog`); every other family locks and journals each record.
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
use std::hash::Hash;
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
        self.load_logged(workspace_id).await
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_logged(workspace_id, expected_revision, replacement, |record| {
            record.revision
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
        self.load_logged(context_id).await
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
                for parent in affected_parents {
                    let current = log.get::<ContextChildren>(root, parent)?;
                    children.insert(parent, current.unwrap_or_default().0);
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
                for (parent, descendants) in children {
                    change.put_children(parent, Some(descendants))?;
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
const LEGACY_CONTEXT_TRANSACTION_FAMILY: &str = "workspace-context-transactions-v2";
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
    async fn compare_and_swap_logged<T: Logged>(
        &self,
        id: T::Id,
        expected_revision: u64,
        replacement: T,
        revision: impl Fn(&T) -> u64 + Send + 'static,
    ) -> Result<bool, LocalCoreStateStoreError> {
        self.transaction(move |root| {
            root.with_log(|log| {
                if log.get::<T>(root, id)?.as_ref().map_or(0, &revision) != expected_revision {
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
    /// failed. A shared store takes the log's file lock and replays and
    /// checkpoints whatever an earlier transaction or owner left.
    fn with_log<T>(
        &self,
        operation: impl FnOnce(&mut CoreLog) -> Result<T, LocalCoreStateStoreError>,
    ) -> Result<T, LocalCoreStateStoreError> {
        match &self.admission {
            Admission::Shared { .. } => with_lock(&core_log_paths(self), || {
                operation(&mut CoreLog::open(self, Arc::default())?)
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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum RecordValue {
    Context(WorkspaceContext),
    Children(ContextChildren),
    ChildCount(ContextChildCount),
    ChildIndex(ContextChildIndex),
    Lineage(WorkspaceLineageRecord),
    LazyWorkspace(LazyWorkspaceState),
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
/// an absence. Replaying a change is idempotent.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct Change(Vec<(RecordKey, Option<RecordValue>)>);

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
        let count = descendants
            .as_ref()
            .map(|descendants| {
                u64::try_from(descendants.len())
                    .map(ContextChildCount)
                    .map_err(|_| {
                        LocalCoreStateStoreError::ContextTransaction(
                            "workspace-context child count exceeds durable representation"
                                .to_owned(),
                        )
                    })
            })
            .transpose()?;
        self.put(parent, descendants.map(ContextChildren));
        self.put(parent, count);
        Ok(())
    }
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
struct LogFrame<C> {
    epoch: Uuid,
    sequence: u64,
    change: C,
}

type Residents = HashMap<RecordKey, Option<RecordValue>>;

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
    dirty: BTreeSet<RecordKey>,
    /// The log may hold a frame whose outcome is unknown, so it must be
    /// reopened and replayed before it is used again.
    broken: bool,
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
            dirty: BTreeSet::new(),
            broken: false,
        };
        log.residents().clear();
        let legacy = legacy_context_change(root)?;
        if bytes.is_empty() && legacy.is_none() {
            return Ok(log);
        }
        let settles_legacy = legacy.is_some();
        for change in legacy.into_iter().chain(replayable_changes(&bytes)) {
            log.apply(change);
        }
        log.checkpoint(root)?;
        if settles_legacy {
            remove_record(&root.record(LEGACY_CONTEXT_TRANSACTION_FAMILY, &[0; 16]))?;
        }
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
            return Ok(value.as_ref().and_then(T::from_value).cloned());
        }
        let paths = key.paths(root);
        let value = match maximum_bytes {
            Some(maximum_bytes) => read_recoverable_bounded::<Option<T>>(&paths, maximum_bytes)?,
            None => read_recoverable::<Option<T>>(&paths)?,
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
            self.dirty.insert(key);
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

/// Decodes the log's valid prefix: frames of the first frame's epoch in
/// consecutive sequence, each intact.
fn replayable_changes(bytes: &[u8]) -> Vec<Change> {
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
        let Ok(frame) = serde_json::from_slice::<LogFrame<Change>>(frame) else {
            break;
        };
        if *epoch.get_or_insert(frame.epoch) != frame.epoch
            || frame.sequence != changes.len() as u64
        {
            break;
        }
        changes.push(frame.change);
    }
    changes
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum LegacyContextTransactionPhase {
    Prepared,
    Committed,
}

/// Two-phase journal written before the core-state log existed.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct LegacyContextTransaction {
    version: u16,
    phase: LegacyContextTransactionPhase,
    before: BTreeMap<WorkspaceContextId, Option<WorkspaceContext>>,
    after: BTreeMap<WorkspaceContextId, WorkspaceContext>,
    #[serde(default)]
    before_children: BTreeMap<WorkspaceContextId, Option<BTreeSet<WorkspaceContextId>>>,
    #[serde(default)]
    after_children: WorkspaceContextChildren,
}

/// Converts an interrupted legacy transaction into the change that settles
/// it: a prepared one rolls back, a committed one rolls forward.
fn legacy_context_change(root: &Namespace) -> Result<Option<Change>, LocalCoreStateStoreError> {
    let Some(journal) = read_recoverable::<LegacyContextTransaction>(
        &root.record(LEGACY_CONTEXT_TRANSACTION_FAMILY, &[0; 16]),
    )?
    else {
        return Ok(None);
    };
    if !matches!(journal.version, 1 | 2) {
        return Err(LocalCoreStateStoreError::ContextTransaction(
            "unsupported workspace-context transaction version".to_owned(),
        ));
    }
    let mut change = Change::default();
    match journal.phase {
        LegacyContextTransactionPhase::Prepared => {
            for (context_id, record) in journal.before {
                change.put(context_id, record);
            }
            for (parent, descendants) in journal.before_children {
                change.put_children(parent, descendants)?;
            }
        }
        LegacyContextTransactionPhase::Committed => {
            for (context_id, record) in journal.after {
                change.put(context_id, Some(record));
            }
            for (parent, descendants) in journal.after_children {
                change.put_children(parent, Some(descendants))?;
            }
        }
    }
    Ok(Some(change))
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

/// Every context present in memory or on disk, absent ones excluded.
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
        if stem.ends_with(".previous") || stem.ends_with(".next") {
            continue;
        }
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
        if log.get::<WorkspaceContext>(root, context_id)?.is_some() {
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

/// Entries retained per cache generation before the older generation is dropped.
const NODE_CACHE_GENERATION: usize = 16_384;

/// Two-generation bounded memo: an entry read since the last rotation
/// survives the next one, so hot treap spines stay resident.
#[derive(Debug)]
struct Generations<K, V> {
    current: HashMap<K, V>,
    previous: HashMap<K, V>,
}

impl<K: Copy + Eq + Hash, V: Clone> Generations<K, V> {
    fn new() -> Self {
        Self {
            current: HashMap::new(),
            previous: HashMap::new(),
        }
    }

    fn get(&mut self, key: &K) -> Option<V> {
        if let Some(value) = self.current.get(key) {
            return Some(value.clone());
        }
        let value = self.previous.remove(key)?;
        self.insert(*key, value.clone());
        Some(value)
    }

    fn insert(&mut self, key: K, value: V) {
        if self.current.len() >= NODE_CACHE_GENERATION {
            self.previous = std::mem::take(&mut self.current);
        }
        self.current.insert(key, value);
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
trait LazyNode: Clone + PartialEq + Serialize + DeserializeOwned + Send + 'static {
    type Id: Copy + Eq + Hash + Send + 'static;
    const FAMILY: &'static str;

    fn address(id: Self::Id) -> [u8; 32];
    fn memo(nodes: &NodeMemo) -> MutexGuard<'_, Generations<Self::Id, Self>>;
}

impl LazyNode for LazyOverlay {
    type Id = LazyOverlayId;
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
}

impl LazyNode for crate::LazyShadow {
    type Id = crate::LazyShadowId;
    const FAMILY: &'static str = "lazy-shadows-v1";

    fn address(id: Self::Id) -> [u8; 32] {
        id.into_bytes()
    }

    fn memo(nodes: &NodeMemo) -> MutexGuard<'_, Generations<Self::Id, Self>> {
        nodes.shadows.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl LocalCoreStateStore {
    async fn load_node<T: LazyNode>(
        &self,
        id: T::Id,
    ) -> Result<Option<T>, LocalCoreStateStoreError> {
        if let Some(value) = T::memo(&self.nodes).get(&id) {
            return Ok(Some(value));
        }
        let value = self
            .transaction(move |namespace| {
                load_content_addressed::<T>(namespace, T::FAMILY, T::address(id))
            })
            .await?;
        if let Some(value) = &value {
            T::memo(&self.nodes).insert(id, value.clone());
        }
        Ok(value)
    }

    async fn put_node<T: LazyNode>(
        &self,
        id: T::Id,
        value: T,
    ) -> Result<(), LocalCoreStateStoreError> {
        if let Some(existing) = T::memo(&self.nodes).get(&id) {
            return if existing == value {
                Ok(())
            } else {
                Err(LocalCoreStateStoreError::Integrity)
            };
        }
        let stored = value.clone();
        self.transaction(move |namespace| {
            put_content_addressed(namespace, T::FAMILY, T::address(id), &stored)
        })
        .await?;
        T::memo(&self.nodes).insert(id, value);
        Ok(())
    }
}

#[async_trait::async_trait]
impl LazyWorkspaceStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<LazyWorkspaceState>, Self::Error> {
        self.load_logged(workspace_id).await
    }

    async fn compare_and_swap_lazy_workspace(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: LazyWorkspaceState,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_logged(workspace_id, expected_revision, replacement, |state| {
            state.revision
        })
        .await
    }

    async fn load_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
    ) -> Result<Option<LazyOverlay>, Self::Error> {
        self.load_node(overlay).await
    }

    async fn put_lazy_overlay(
        &self,
        overlay: LazyOverlayId,
        value: LazyOverlay,
    ) -> Result<(), Self::Error> {
        self.put_node(overlay, value).await
    }

    async fn load_lazy_shadow(
        &self,
        shadow: crate::LazyShadowId,
    ) -> Result<Option<crate::LazyShadow>, Self::Error> {
        self.load_node(shadow).await
    }

    async fn put_lazy_shadow(
        &self,
        shadow: crate::LazyShadowId,
        value: crate::LazyShadow,
    ) -> Result<(), Self::Error> {
        self.put_node(shadow, value).await
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
        WorkspaceLineageStore::load(&store, workspace())
            .await
            .expect("write the record back");
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
            LazyWorkspaceStore::load_lazy_overlay(
                &LocalCoreStateStore::new(directory.path()),
                overlay
            )
            .await,
            Err(LocalCoreStateStoreError::Integrity)
        ));
    }

    #[tokio::test]
    async fn immutable_nodes_are_served_from_memory_after_first_use() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::open_owned(directory.path()).expect("owner");
        let overlay = LazyOverlay::default();
        let overlay_id = LazyOverlayId::from_bytes(
            *blake3::hash(&serde_json::to_vec(&overlay).expect("encode overlay")).as_bytes(),
        );
        let shadow = crate::LazyShadow::default();
        let shadow_id: crate::LazyShadowId = serde_json::from_value(serde_json::json!(
            blake3::hash(&serde_json::to_vec(&shadow).expect("encode shadow")).as_bytes()
        ))
        .expect("shadow id");
        store
            .put_lazy_overlay(overlay_id, overlay.clone())
            .await
            .expect("put overlay");
        store
            .put_lazy_shadow(shadow_id, shadow.clone())
            .await
            .expect("put shadow");
        std::fs::remove_dir_all(directory.path().join("lazy-overlays")).expect("drop overlays");
        std::fs::remove_dir_all(directory.path().join("lazy-shadows-v1")).expect("drop shadows");
        let clone = store.clone();
        assert_eq!(
            clone
                .load_lazy_overlay(overlay_id)
                .await
                .expect("memoized overlay"),
            Some(overlay)
        );
        assert_eq!(
            clone
                .load_lazy_shadow(shadow_id)
                .await
                .expect("memoized shadow"),
            Some(shadow)
        );
        assert!(
            matches!(
                clone
                    .put_lazy_overlay(
                        overlay_id,
                        serde_json::from_value(serde_json::json!({
                            "Node": {
                                "path": "/different",
                                "priority": 0,
                                "change": "Tombstone",
                                "left": vec![0_u8; 32],
                                "right": vec![0_u8; 32]
                            }
                        }))
                        .expect("different overlay"),
                    )
                    .await,
                Err(LocalCoreStateStoreError::Integrity)
            ),
            "a memoized address still rejects a different value"
        );
    }

    #[test]
    fn node_memo_rotation_keeps_recently_used_entries() {
        let mut generations = Generations::new();
        for key in 0..NODE_CACHE_GENERATION {
            generations.insert(key, key);
        }
        generations.insert(NODE_CACHE_GENERATION, NODE_CACHE_GENERATION);
        assert_eq!(generations.get(&0), Some(0));
        for key in NODE_CACHE_GENERATION + 1..2 * NODE_CACHE_GENERATION {
            generations.insert(key, key);
        }
        assert_eq!(
            generations.get(&0),
            Some(0),
            "a promoted entry survives rotation"
        );
        assert_eq!(generations.get(&1), None, "an unused entry is evicted");
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
            write_journaled(&paths, &context).expect("write legacy context");
        }
        std::fs::write(context_paths(&namespace, unrelated_id).current, b"not-json")
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
        write_journaled(&context_paths(&namespace, context_id), &before).expect("initial context");
        write_journaled(
            &namespace.record(LEGACY_CONTEXT_TRANSACTION_FAMILY, &[0; 16]),
            &LegacyContextTransaction {
                version: 1,
                phase: LegacyContextTransactionPhase::Prepared,
                before: [(context_id, Some(before.clone()))].into_iter().collect(),
                after: [(context_id, after.clone())].into_iter().collect(),
                before_children: BTreeMap::new(),
                after_children: WorkspaceContextChildren::new(),
            },
        )
        .expect("prepared transaction");
        write_journaled(&context_paths(&namespace, context_id), &after)
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
        let marker = RecordKey::ChildIndex.paths(&namespace);
        std::fs::create_dir_all(&marker.directory).expect("index directory");
        write_journaled(&marker, &1_u16).expect("initialize child index");
        let journal = LegacyContextTransaction {
            version: 2,
            phase: LegacyContextTransactionPhase::Prepared,
            before: [(child_id, None)].into_iter().collect(),
            after: [(child_id, child.clone())].into_iter().collect(),
            before_children: [(parent_id, None)].into_iter().collect(),
            after_children: [(parent_id, BTreeSet::from([child_id]))]
                .into_iter()
                .collect(),
        };
        std::fs::create_dir_all(directory.path().join("workspace-context-transactions-v2"))
            .expect("transaction directory");
        write_journaled(
            &namespace.record(LEGACY_CONTEXT_TRANSACTION_FAMILY, &[0; 16]),
            &journal,
        )
        .expect("prepared transaction");
        let record = context_paths(&namespace, child_id);
        let bucket = children_paths(&namespace, parent_id);
        for directory in [&record.directory, &bucket.directory] {
            std::fs::create_dir_all(directory).expect("record directory");
        }
        write_journaled(&record, &child).expect("partial record application");
        write_journaled(&bucket, &BTreeSet::from([child_id])).expect("partial bucket application");

        assert_eq!(
            WorkspaceContextStore::load(&store, child_id)
                .await
                .expect("recover prepared transaction"),
            None
        );
        assert_eq!(
            read_recoverable::<Option<ContextChildren>>(&children_paths(&namespace, parent_id))
                .expect("read recovered child bucket")
                .flatten(),
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
        write_journaled(&context_paths(&namespace, context_id), &before).expect("initial context");
        write_journaled(
            &namespace.record(LEGACY_CONTEXT_TRANSACTION_FAMILY, &[0; 16]),
            &LegacyContextTransaction {
                version: 1,
                phase: LegacyContextTransactionPhase::Committed,
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
        assert!(
            !namespace
                .record(LEGACY_CONTEXT_TRANSACTION_FAMILY, &[0; 16])
                .current
                .exists(),
            "settled legacy journal is removed"
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
        LocalCoreStateStore::new(directory.path())
            .load_lazy_workspace(workspace())
            .await
            .expect("write the binding back");

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
            replayable_changes(&bytes).len(),
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

    #[tokio::test]
    async fn legacy_rollback_records_absence_without_deleting_files() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let namespace = store.namespace().expect("admitted namespace");
        let context = workspace_context(
            directory.path(),
            CRASH_CHILD,
            1,
            WorkspaceContextState::Active,
        );
        let record = RecordKey::Context(CRASH_CHILD).paths(&namespace);
        std::fs::create_dir_all(&record.directory).expect("context directory");
        write_journaled(&record, &context).expect("partial legacy application");
        let journal = namespace.record(LEGACY_CONTEXT_TRANSACTION_FAMILY, &[0; 16]);
        std::fs::create_dir_all(&journal.directory).expect("journal directory");
        write_journaled(
            &journal,
            &LegacyContextTransaction {
                version: 1,
                phase: LegacyContextTransactionPhase::Prepared,
                before: [(CRASH_CHILD, None)].into_iter().collect(),
                after: [(CRASH_CHILD, context)].into_iter().collect(),
                before_children: BTreeMap::new(),
                after_children: WorkspaceContextChildren::new(),
            },
        )
        .expect("prepared legacy journal");
        let current = record.current.clone();
        drop(namespace);

        assert!(
            WorkspaceContextStore::list(&store)
                .await
                .expect("settle legacy journal")
                .is_empty()
        );
        assert_eq!(
            std::fs::read(&current).expect("rolled-back record stays in place"),
            b"null"
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
        for change in replayable_changes(bytes.get(..flushed).unwrap_or(&bytes)) {
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
            let status = child.status().expect("crash child");
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
}
