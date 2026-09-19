//! Journaled local persistence for filesystem control-plane state.
//!
//! Distributed deployments can implement the small CAS traits over their
//! authority stream. Embedded native deployments use this companion namespace:
//! one lock and one recoverable JSON record per workspace and state family.
//! Keeping the implementation here prevents adapters from inventing separate
//! lineage, lease, and Git-compatibility databases.

use crate::{
    GitCompatState, GitCompatStore, MaterializationJournal, MaterializationJournalStore,
    OperationId, OperationWindowSnapshot, OperationWindowStore, WorkspaceId,
    WorkspaceLineageRecord, WorkspaceLineageStore,
};
use fs2::FileExt;
use serde::{Serialize, de::DeserializeOwned};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// One journaled private namespace shared by core control-plane state.
#[derive(Clone, Debug)]
pub struct LocalCoreStateStore {
    root: PathBuf,
}

impl LocalCoreStateStore {
    /// Opens a private companion namespace below `root`.
    ///
    /// Directories are created lazily on first mutation.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Returns the namespace root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Lists materialization operation ids with durable recovery state.
    ///
    /// Only canonical journal names are returned. Temporary, previous, and
    /// lock files remain internal recovery details.
    pub fn materialization_operations(&self) -> Result<Vec<OperationId>, LocalCoreStateStoreError> {
        let directory = self.root.join("materialization");
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut operations = Vec::new();
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
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
            operations.push(OperationId::from_bytes(bytes));
        }
        operations.sort_unstable_by_key(|operation| operation.into_bytes());
        Ok(operations)
    }

    /// Removes a terminal materialization journal and its recovery copies.
    pub fn remove_materialization(
        &self,
        operation_id: OperationId,
    ) -> Result<(), LocalCoreStateStoreError> {
        let paths = RecordPaths::new_key(&self.root, "materialization", &operation_id.into_bytes());
        with_lock(&paths, || {
            let journal: Option<MaterializationJournal> = read_recoverable(&paths)?;
            if journal.as_ref().is_some_and(|journal| {
                !matches!(
                    journal.phase,
                    crate::MaterializationPhase::Applied | crate::MaterializationPhase::RolledBack
                )
            }) {
                return Err(LocalCoreStateStoreError::MaterializationInProgress);
            }
            remove_if_present(&paths.current)?;
            remove_if_present(&paths.previous)?;
            remove_if_present(&paths.temporary)?;
            sync_directory(&paths.directory)?;
            Ok(())
        })
    }

    fn load_record<T: DeserializeOwned>(
        &self,
        family: &str,
        workspace_id: WorkspaceId,
    ) -> Result<Option<T>, LocalCoreStateStoreError> {
        let paths = RecordPaths::new(&self.root, family, workspace_id);
        with_lock(&paths, || read_recoverable(&paths))
    }

    fn compare_and_swap_record<T: DeserializeOwned + Serialize>(
        &self,
        family: &str,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: &T,
        revision: impl Fn(&T) -> u64,
    ) -> Result<bool, LocalCoreStateStoreError> {
        let paths = RecordPaths::new(&self.root, family, workspace_id);
        with_lock(&paths, || {
            let current: Option<T> = read_recoverable(&paths)?;
            if current.as_ref().map_or(0, &revision) != expected_revision {
                return Ok(false);
            }
            write_journaled(&paths, replacement)?;
            Ok(true)
        })
    }

    fn compare_and_delete_record<T: DeserializeOwned>(
        &self,
        family: &str,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        revision: impl Fn(&T) -> u64,
    ) -> Result<bool, LocalCoreStateStoreError> {
        let paths = RecordPaths::new(&self.root, family, workspace_id);
        with_lock(&paths, || {
            let current: Option<T> = read_recoverable(&paths)?;
            if current.as_ref().map_or(0, &revision) != expected_revision {
                return Ok(false);
            }
            remove_if_present(&paths.temporary)?;
            remove_if_present(&paths.previous)?;
            remove_if_present(&paths.current)?;
            sync_directory(&paths.directory)?;
            Ok(true)
        })
    }

    fn load_operation_record<T: DeserializeOwned>(
        &self,
        family: &str,
        operation_id: OperationId,
    ) -> Result<Option<T>, LocalCoreStateStoreError> {
        let paths = RecordPaths::new_key(&self.root, family, &operation_id.into_bytes());
        with_lock(&paths, || read_recoverable(&paths))
    }

    fn compare_and_swap_operation_record<T: DeserializeOwned + Serialize>(
        &self,
        family: &str,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: &T,
        revision: impl Fn(&T) -> u64,
    ) -> Result<bool, LocalCoreStateStoreError> {
        let paths = RecordPaths::new_key(&self.root, family, &operation_id.into_bytes());
        with_lock(&paths, || {
            let current: Option<T> = read_recoverable(&paths)?;
            if current.as_ref().map_or(0, &revision) != expected_revision {
                return Ok(false);
            }
            write_journaled(&paths, replacement)?;
            Ok(true)
        })
    }
}

#[derive(Debug)]
struct RecordPaths {
    directory: PathBuf,
    current: PathBuf,
    previous: PathBuf,
    temporary: PathBuf,
    lock: PathBuf,
}

impl RecordPaths {
    fn new(root: &Path, family: &str, workspace_id: WorkspaceId) -> Self {
        Self::new_key(root, family, &workspace_id.into_bytes())
    }

    fn new_key(root: &Path, family: &str, key: &[u8]) -> Self {
        let directory = root.join(family);
        let stem = hex::encode(key);
        Self {
            current: directory.join(format!("{stem}.json")),
            previous: directory.join(format!("{stem}.previous.json")),
            temporary: directory.join(format!("{stem}.next.json")),
            lock: directory.join(format!("{stem}.lock")),
            directory,
        }
    }
}

fn with_lock<T>(
    paths: &RecordPaths,
    action: impl FnOnce() -> Result<T, LocalCoreStateStoreError>,
) -> Result<T, LocalCoreStateStoreError> {
    std::fs::create_dir_all(&paths.directory)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&paths.lock)?;
    lock.lock_exclusive()?;
    let result = action();
    FileExt::unlock(&lock)?;
    result
}

fn read_recoverable<T: DeserializeOwned>(
    paths: &RecordPaths,
) -> Result<Option<T>, LocalCoreStateStoreError> {
    match read_json(&paths.current) {
        Ok(Some(value)) => Ok(Some(value)),
        Ok(None) => read_json(&paths.previous),
        Err(current_error) => match read_json(&paths.previous) {
            Ok(Some(value)) => {
                std::fs::copy(&paths.previous, &paths.current)?;
                Ok(Some(value))
            }
            _ => Err(current_error),
        },
    }
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
    /// A persisted record was malformed or incompatible with its schema.
    #[error("core state serialization failed: {0}")]
    Json(#[from] serde_json::Error),
    /// A nonterminal materialization journal cannot be discarded.
    #[error("materialization is still in progress")]
    MaterializationInProgress,
}

impl WorkspaceLineageStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<WorkspaceLineageRecord>, Self::Error> {
        self.load_record("lineage", workspace_id)
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: WorkspaceLineageRecord,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_record(
            "lineage",
            workspace_id,
            expected_revision,
            &replacement,
            |record| record.revision,
        )
    }
}

impl OperationWindowStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<Option<OperationWindowSnapshot>, Self::Error> {
        self.load_record("operation-windows", workspace_id)
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: OperationWindowSnapshot,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_record(
            "operation-windows",
            workspace_id,
            expected_revision,
            &replacement,
            |record| record.revision,
        )
    }
}

impl GitCompatStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(&self, workspace_id: WorkspaceId) -> Result<Option<GitCompatState>, Self::Error> {
        self.load_record("git-compat", workspace_id)
    }

    async fn compare_and_swap(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
        replacement: GitCompatState,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_record(
            "git-compat",
            workspace_id,
            expected_revision,
            &replacement,
            |record| record.revision,
        )
    }

    async fn compare_and_delete(
        &self,
        workspace_id: WorkspaceId,
        expected_revision: u64,
    ) -> Result<bool, Self::Error> {
        self.compare_and_delete_record::<GitCompatState>(
            "git-compat",
            workspace_id,
            expected_revision,
            |record| record.revision,
        )
    }
}

impl MaterializationJournalStore for LocalCoreStateStore {
    type Error = LocalCoreStateStoreError;

    async fn load(
        &self,
        operation_id: OperationId,
    ) -> Result<Option<MaterializationJournal>, Self::Error> {
        self.load_operation_record("materialization", operation_id)
    }

    async fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MaterializationJournal,
    ) -> Result<bool, Self::Error> {
        self.compare_and_swap_operation_record(
            "materialization",
            operation_id,
            expected_revision,
            &replacement,
            |journal| journal.revision,
        )
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::{Digest, WorkspaceName};

    fn workspace() -> WorkspaceId {
        WorkspaceId::derive(
            [4; 16],
            &WorkspaceName::new("durable").expect("valid workspace"),
        )
    }

    #[tokio::test]
    async fn state_survives_reopen_and_recovers_previous_record() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
        let state = WorkspaceLineageRecord {
            version: 1,
            revision: 1,
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
        let reopened = LocalCoreStateStore::new(directory.path());
        assert_eq!(
            WorkspaceLineageStore::load(&reopened, workspace())
                .await
                .expect("reopen"),
            Some(state)
        );
    }

    #[tokio::test]
    async fn git_compatibility_delete_is_revision_fenced_and_durable() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = LocalCoreStateStore::new(directory.path());
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
                .materialization_operations()
                .expect("list materializations"),
            [operation_id]
        );
        assert!(matches!(
            store.remove_materialization(operation_id),
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
            .remove_materialization(operation_id)
            .expect("remove terminal journal");
        assert!(
            store
                .materialization_operations()
                .expect("list cleaned materializations")
                .is_empty()
        );
    }
}
