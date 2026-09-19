//! Customer workspace identities and immutable generation handles.
//!
//! A workspace is the sole customer-visible unit of mutation, publication,
//! retention, and convergence. The engine's volume and authority identities
//! remain implementation details.

use crate::foundation::{FileId, GenerationId, OperationId, VolumeId};
use crate::kernel::{
    ExtentKind, FileKind, FileMetadata, FilePayload, LogicalName, MetadataField, NamespacePath,
};
use crate::model::{CheckoutMode, GenerationSelector};
use crate::path::PortablePath;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, Checkout, CheckoutCommitOutcome,
    ConflictKey, ConflictKind, ConflictSide, ConflictValue, FsError, GenerationDiff, MergeConflict,
    MergeDriverRegistry, MergePlan, MergePlanResolutionError, MergeResolution,
    MergeResolutionCache, UnpublishedMergeCandidate, Volume, resolve_merge_plan,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use thiserror::Error;
use unicode_normalization::UnicodeNormalization;

const WORKSPACE_ID_DOMAIN: &[u8] = b"acyclic-fs-workspace-id-v1\0";
const MAXIMUM_WORKSPACE_NAME_BYTES: usize = 255;

/// Canonical immutable customer workspace name.
///
/// Names are NFC-normalized, case-sensitive UTF-8 and cannot be reused for a
/// different workspace in one filesystem namespace.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkspaceName(Arc<str>);

impl WorkspaceName {
    /// Canonicalizes and validates one friendly workspace name.
    ///
    /// # Errors
    ///
    /// Rejects empty, reserved, path-like, control-containing, or oversized
    /// names before deriving any durable identity.
    pub fn new(name: impl AsRef<str>) -> Result<Self, WorkspaceNameError> {
        let canonical: String = name.as_ref().nfc().collect();
        if canonical.is_empty() {
            return Err(WorkspaceNameError::Empty);
        }
        if canonical == "." || canonical == ".." {
            return Err(WorkspaceNameError::Reserved);
        }
        if canonical.len() > MAXIMUM_WORKSPACE_NAME_BYTES {
            return Err(WorkspaceNameError::TooLong);
        }
        if canonical
            .chars()
            .any(|character| character.is_control() || matches!(character, '/' | '\\'))
        {
            return Err(WorkspaceNameError::InvalidCharacter);
        }
        Ok(Self(Arc::from(canonical)))
    }

    /// Returns the canonical customer spelling.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for WorkspaceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("WorkspaceName")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for WorkspaceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Stable opaque identity of one named workspace.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkspaceId([u8; 16]);

impl WorkspaceId {
    pub(crate) const fn from_volume_id(volume_id: VolumeId) -> Self {
        Self(volume_id.into_bytes())
    }

    /// Restores a stable workspace identity from canonical bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Derives the stable identity for a canonical name in one deployment
    /// namespace. The namespace is deliberately internal to the selected
    /// `Fs` deployment.
    #[must_use]
    pub(crate) fn derive(namespace: [u8; 16], name: &WorkspaceName) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(WORKSPACE_ID_DOMAIN);
        hasher.update(&namespace);
        hasher.update(
            &u64::try_from(name.as_str().len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        hasher.update(name.as_str().as_bytes());
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
        Self(bytes)
    }

    /// Returns the stable opaque bytes for persistence or transport.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    pub(crate) const fn volume_id(self) -> VolumeId {
        VolumeId::from_bytes(self.0)
    }
}

/// Stable retry identity for one customer-visible mutation, fork, or join.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdempotencyKey([u8; 16]);

impl IdempotencyKey {
    /// Creates a fresh time-ordered retry identity.
    #[must_use]
    pub fn new() -> Self {
        Self(OperationId::new().into_bytes())
    }

    /// Restores an exact key after an ambiguous outcome.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the stable wire bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    pub(crate) const fn operation_id(self) -> OperationId {
        OperationId::from_bytes(self.0)
    }
}

impl Default for IdempotencyKey {
    fn default() -> Self {
        Self::new()
    }
}

/// One named mutable filesystem head.
pub struct Workspace<A, O> {
    pub(crate) name: WorkspaceName,
    pub(crate) id: WorkspaceId,
    pub(crate) volume: Volume<A, O>,
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    pub(crate) source: Option<crate::Source<A, O>>,
}

impl<A, O> Clone for Workspace<A, O> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            id: self.id,
            volume: self.volume.clone(),
            #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
            source: self.source.clone(),
        }
    }
}

impl<A, O> Workspace<A, O> {
    /// Stable opaque workspace identity.
    #[must_use]
    pub const fn id(&self) -> WorkspaceId {
        self.id
    }

    /// Immutable canonical workspace name.
    #[must_use]
    pub fn name(&self) -> &WorkspaceName {
        &self.name
    }

    /// Exact immutable filesystem profile selected when this workspace was created.
    #[must_use]
    pub const fn profile(&self) -> crate::model::FilesystemProfile {
        self.volume.config.profile
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Resolves the exact generation published by one prior workspace operation.
    ///
    /// This is the recovery boundary for adapters that persisted an
    /// idempotency key before publication but lost the successful result.
    pub async fn operation_generation(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<Generation<A, O>>, WorkspaceError> {
        self.volume
            .fs
            .workspace_operation_generation(&self.volume, idempotency_key.operation_id())
            .await
            .map(|generation| {
                generation.map(|id| Generation {
                    workspace: self.clone(),
                    id,
                })
            })
    }

    /// Opens an authenticated checkout using the requested generation and mode.
    ///
    /// # Errors
    ///
    /// Rejects unsupported mode combinations, unavailable generations, and
    /// authority or immutable-storage failures.
    pub async fn checkout(
        &self,
        selector: GenerationSelector,
        mode: CheckoutMode,
    ) -> Result<Checkout<A, O>, WorkspaceError> {
        self.engine_checkout(selector, mode).await
    }

    /// Returns the attached native source, when this handle was created by
    /// [`crate::Fs::attach_directory`].
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[must_use]
    pub fn source(&self) -> Option<&crate::Source<A, O>> {
        self.source.as_ref()
    }

    /// Captures all remaining source state and returns an independent exact
    /// generation. Workspaces without a source simply return their head.
    ///
    /// # Errors
    ///
    /// Returns source reconciliation or generation authentication failures.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    pub async fn seal(&self) -> Result<Generation<A, O>, crate::SourceError> {
        match &self.source {
            Some(source) => source.seal().await,
            None => self
                .head()
                .await
                .map_err(|error| crate::SourceError::Engine(error.to_string())),
        }
    }
    /// Resolves and pins the current immutable generation for this operation.
    ///
    /// # Errors
    ///
    /// Returns a typed workspace failure when the head cannot be authenticated.
    pub async fn head(&self) -> Result<Generation<A, O>, WorkspaceError> {
        let checkout = self
            .volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?
            .value;
        Ok(Generation {
            workspace: self.clone(),
            id: checkout.generation_id(),
        })
    }

    /// Reopens and authenticates one exact immutable generation belonging to
    /// this workspace. This is the stateless transport/restart counterpart to
    /// retaining a live [`Generation`] handle.
    ///
    /// # Errors
    ///
    /// Rejects absent, foreign, corrupt, or unauthenticated generation state.
    pub async fn generation(
        &self,
        generation_id: GenerationId,
    ) -> Result<Generation<A, O>, WorkspaceError> {
        let checkout = self
            .engine_checkout(
                GenerationSelector::Exact(generation_id),
                CheckoutMode::read_only_pinned(),
            )
            .await?;
        Ok(Generation {
            workspace: self.clone(),
            id: checkout.generation_id(),
        })
    }

    /// Atomically moves this workspace head to one of its own exact immutable
    /// generations. The caller supplies the observed head, making restoration
    /// a composable compare-and-swap rather than an unconditional reset.
    ///
    /// This primitive is also the recovery boundary for compatibility layers:
    /// retrying the same key and inputs reports the already-restored generation,
    /// while reusing a key for another generation fails closed.
    pub async fn restore_generation(
        &self,
        generation: &Generation<A, O>,
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
    ) -> Result<WorkspaceRestore<A, O>, WorkspaceError> {
        if generation.workspace.id != self.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let outcome = self
            .volume
            .fs
            .restore_workspace_generation(
                &self.volume,
                generation.id,
                if_current,
                idempotency_key.operation_id(),
            )
            .await?;
        let restored = |id| Generation {
            workspace: self.clone(),
            id,
        };
        Ok(match outcome {
            WorkspaceRestoreOutcome::Restored(id) => WorkspaceRestore::Restored(restored(id)),
            WorkspaceRestoreOutcome::AlreadyRestored(id) => {
                WorkspaceRestore::AlreadyRestored(restored(id))
            }
            WorkspaceRestoreOutcome::Current(id) => WorkspaceRestore::Current(restored(id)),
            WorkspaceRestoreOutcome::Stale(id) => WorkspaceRestore::Stale(restored(id)),
            WorkspaceRestoreOutcome::Fenced => WorkspaceRestore::Fenced,
            WorkspaceRestoreOutcome::IdempotencyConflict => WorkspaceRestore::IdempotencyConflict,
        })
    }

    /// Atomically restores selected paths from an immutable compatible
    /// generation while leaving every other live path untouched.
    ///
    /// This is the core primitive used by compatibility layers for checkout,
    /// restore, and hard reset. Exact records are reused by reference, richer
    /// metadata is retained, and live hard-link topology remains authoritative
    /// for bindings outside `paths`.
    pub async fn restore_paths_from(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
    ) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        if !self.volume.fs.same_deployment(&source.workspace.volume.fs) {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let current = self.head().await?;
        if current.id != if_current {
            return Ok(TransactionCommit::Conflict { actual: current });
        }
        let mut source_checkout = source
            .workspace
            .engine_checkout(
                GenerationSelector::Exact(source.id),
                CheckoutMode::read_only_pinned(),
            )
            .await?;
        let mut transaction = self.begin_transaction(idempotency_key).await?;
        let limits = self.volume.config.limits;
        let cancellation = crate::CancellationToken::new();
        let parsed = paths
            .iter()
            .map(|path| {
                let relative = path.trim_start_matches('/');
                let absolute = format!("/{relative}");
                customer_path(&absolute, limits).map(|path| (relative, path))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let lookup_paths = parsed
            .iter()
            .map(|(_, path)| path.clone())
            .collect::<Vec<_>>();
        let source_records = source_checkout
            .lookup_batch_no_follow(&lookup_paths, crate::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(WorkspaceError::engine)?
            .value
            .entries
            .into_iter()
            .map(|entry| entry.record)
            .collect::<Vec<_>>();
        let missing_paths = parsed
            .iter()
            .zip(&source_records)
            .filter(|(_, record)| record.is_none())
            .map(|((_, path), _)| path.clone())
            .collect::<Vec<_>>();
        let current_records = if missing_paths.is_empty() {
            Vec::new()
        } else {
            transaction
                .checkout
                .lookup_batch_no_follow(&missing_paths, crate::WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(WorkspaceError::engine)?
                .value
                .entries
                .into_iter()
                .map(|entry| entry.record)
                .collect()
        };
        let mut current_records = current_records.into_iter();
        let mut operations = Vec::with_capacity(paths.len());
        for ((relative, path), source_record) in parsed.into_iter().zip(source_records) {
            if let Some(record) = source_record {
                if record.kind != FileKind::Directory
                    && let Some((parent, _)) = relative.rsplit_once('/')
                {
                    transaction.create_dir_all(&format!("/{parent}")).await?;
                }
                operations.push(crate::kernel::Mutation::Restore { path, record });
            } else {
                let current_record = current_records
                    .next()
                    .ok_or_else(|| WorkspaceError::engine("missing batch lookup result"))?;
                if let Some(record) = current_record {
                    operations.push(crate::kernel::Mutation::Remove {
                        path,
                        expected_file_id: MetadataField::Value(record.file_id),
                    });
                }
            }
        }
        if !operations.is_empty() {
            transaction
                .checkout
                .mutate(operations, crate::WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(WorkspaceError::engine)?;
        }
        transaction.commit().await
    }

    /// Applies only the paths changed between `base` and `source`, rejecting
    /// paths independently changed in the live workspace. This is a bounded,
    /// exact three-way patch primitive for cherry-pick, revert, and similar
    /// compatibility operations.
    #[allow(
        clippy::too_many_lines,
        reason = "one atomic path application keeps conflict classification and publication together"
    )]
    pub async fn apply_paths_from(
        &self,
        base: Option<&Generation<A, O>>,
        source: Option<&Generation<A, O>>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
    ) -> Result<WorkspacePathApply<A, O>, WorkspaceError> {
        if base.is_some_and(|base| !self.volume.fs.same_deployment(&base.workspace.volume.fs))
            || source
                .is_some_and(|source| !self.volume.fs.same_deployment(&source.workspace.volume.fs))
        {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let current = self.head().await?;
        if current.id != if_current {
            return Ok(WorkspacePathApply::Stale(current));
        }
        let mut base_checkout = match base {
            Some(base) => Some(
                base.workspace
                    .engine_checkout(
                        GenerationSelector::Exact(base.id),
                        CheckoutMode::read_only_pinned(),
                    )
                    .await?,
            ),
            None => None,
        };
        let mut source_checkout = match source {
            Some(source) => Some(
                source
                    .workspace
                    .engine_checkout(
                        GenerationSelector::Exact(source.id),
                        CheckoutMode::read_only_pinned(),
                    )
                    .await?,
            ),
            None => None,
        };
        let mut transaction = self.begin_transaction(idempotency_key).await?;
        let limits = self.volume.config.limits;
        let cancellation = crate::CancellationToken::new();
        let parsed = paths
            .iter()
            .map(|path| {
                let relative = path.trim_start_matches('/');
                let absolute = format!("/{relative}");
                customer_path(&absolute, limits).map(|path| (relative, path))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let lookup_paths = parsed
            .iter()
            .map(|(_, path)| path.clone())
            .collect::<Vec<_>>();
        let base_records = match &mut base_checkout {
            Some(checkout) => checkout
                .lookup_batch_no_follow(&lookup_paths, crate::WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(WorkspaceError::engine)?
                .value
                .entries
                .into_iter()
                .map(|entry| entry.record)
                .collect(),
            None => vec![None; parsed.len()],
        };
        let source_records = match &mut source_checkout {
            Some(checkout) => checkout
                .lookup_batch_no_follow(&lookup_paths, crate::WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(WorkspaceError::engine)?
                .value
                .entries
                .into_iter()
                .map(|entry| entry.record)
                .collect(),
            None => vec![None; parsed.len()],
        };
        let changed_paths = parsed
            .iter()
            .zip(base_records.iter().zip(&source_records))
            .filter(|(_, (base, source))| base != source)
            .map(|((_, path), _)| path.clone())
            .collect::<Vec<_>>();
        let current_records = if changed_paths.is_empty() {
            Vec::new()
        } else {
            transaction
                .checkout
                .lookup_batch_no_follow(&changed_paths, crate::WorkBudget::UNBOUNDED, &cancellation)
                .await
                .map_err(WorkspaceError::engine)?
                .value
                .entries
                .into_iter()
                .map(|entry| entry.record)
                .collect()
        };
        let mut current_records = current_records.into_iter();
        let mut operations = Vec::with_capacity(paths.len());
        let mut conflicts = Vec::new();
        for (((relative, path), base_record), source_record) in
            parsed.into_iter().zip(base_records).zip(source_records)
        {
            if base_record == source_record {
                continue;
            }
            let current_record = current_records
                .next()
                .ok_or_else(|| WorkspaceError::engine("missing batch lookup result"))?;
            if current_record == source_record {
                continue;
            }
            if current_record != base_record {
                conflicts.push(WorkspacePathConflict {
                    path: relative.to_owned(),
                    kind: path_conflict_kind(base_record, current_record, source_record),
                });
                continue;
            }
            match source_record {
                Some(record) => {
                    if record.kind != FileKind::Directory
                        && let Some((parent, _)) = relative.rsplit_once('/')
                    {
                        transaction.create_dir_all(&format!("/{parent}")).await?;
                    }
                    operations.push(crate::kernel::Mutation::Restore { path, record });
                }
                None => {
                    if let Some(record) = current_record {
                        operations.push(crate::kernel::Mutation::Remove {
                            path,
                            expected_file_id: MetadataField::Value(record.file_id),
                        });
                    }
                }
            }
        }
        if !conflicts.is_empty() {
            return Ok(WorkspacePathApply::Conflicted(conflicts));
        }
        if operations.is_empty() {
            return Ok(WorkspacePathApply::NoChanges(current));
        }
        transaction
            .checkout
            .mutate(operations, crate::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(WorkspaceError::engine)?;
        Ok(match transaction.commit().await? {
            TransactionCommit::Committed(generation) => WorkspacePathApply::Applied(generation),
            TransactionCommit::AlreadyCommitted(generation) => {
                WorkspacePathApply::AlreadyApplied(generation)
            }
            TransactionCommit::Conflict { actual } => WorkspacePathApply::Stale(actual),
            TransactionCommit::Fenced => WorkspacePathApply::Fenced,
            TransactionCommit::IdempotencyConflict => WorkspacePathApply::IdempotencyConflict,
        })
    }

    /// Returns the current complete immutable workspace state after all prior
    /// SDK operations on this handle have reached their publication boundary.
    ///
    /// # Errors
    ///
    /// Returns an authority or authentication failure when the current head
    /// cannot be resolved exactly.
    pub async fn sync(&self) -> Result<WorkspaceSync<A, O>, WorkspaceError> {
        Ok(WorkspaceSync {
            generation: self.head().await?,
        })
    }

    /// Creates an independent named workspace at one exact generation without
    /// copying unchanged namespace pages or file bodies.
    ///
    /// # Errors
    ///
    /// Rejects invalid destination names, foreign generations, incompatible
    /// existing destinations, or storage/publication failures.
    pub async fn fork(
        &self,
        destination: impl AsRef<str>,
        options: ForkOptions<A, O>,
    ) -> Result<Self, WorkspaceError> {
        if options.generation.workspace.id != self.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let destination = WorkspaceName::new(destination)?;
        Box::pin(self.volume.fs.fork_workspace(
            destination,
            &options.generation,
            options.idempotency_key,
        ))
        .await
    }

    /// Opens one sparse atomic transaction against the current generation.
    /// No mutation is visible outside this transaction before [`Transaction::commit`].
    ///
    /// # Errors
    ///
    /// Returns an authentication, authority, storage, or workspace-state failure.
    pub async fn begin_transaction(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Transaction<A, O>, WorkspaceError> {
        let generation = self.head().await?;
        self.open_transaction_at(&generation, idempotency_key, false)
            .await
    }

    /// Opens one sparse atomic transaction against an exact immutable generation.
    ///
    /// This is the stateless transport boundary: a remote caller can retain its
    /// base generation, submit sparse mutations later, and receive the same
    /// observation-safe commit or rebase result as an embedded caller. The
    /// generation must belong to this workspace.
    ///
    /// # Errors
    ///
    /// Returns [`WorkspaceError::ForeignGeneration`] for another workspace, or
    /// an authentication, authority, storage, or workspace-state failure.
    pub async fn begin_transaction_at(
        &self,
        generation: &Generation<A, O>,
        idempotency_key: IdempotencyKey,
    ) -> Result<Transaction<A, O>, WorkspaceError> {
        if generation.workspace.id != self.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let requires_rebase = self.head().await?.id != generation.id;
        self.open_transaction_at(generation, idempotency_key, requires_rebase)
            .await
    }

    async fn open_transaction_at(
        &self,
        generation: &Generation<A, O>,
        idempotency_key: IdempotencyKey,
        requires_rebase: bool,
    ) -> Result<Transaction<A, O>, WorkspaceError> {
        let mut checkout = self
            .engine_checkout(
                GenerationSelector::Exact(generation.id),
                CheckoutMode::tracking_transaction(),
            )
            .await?;
        checkout.bind_authored_operation(idempotency_key.operation_id());
        Ok(Transaction {
            checkout,
            workspace: self.clone(),
            idempotency_key,
            requires_rebase,
        })
    }

    /// Retains the current generation under one permanent human-readable label.
    /// Repeating the same label and generation is idempotent; binding the label
    /// to another generation is rejected.
    ///
    /// # Errors
    ///
    /// Rejects invalid labels, generation authentication failures, or an
    /// existing label bound to different state.
    pub async fn checkpoint(
        &self,
        label: impl AsRef<str>,
    ) -> Result<Checkpoint<A, O>, WorkspaceError> {
        let label = WorkspaceName::new(label)?;
        let generation = self.head().await?;
        self.volume
            .fs
            .retain_workspace_generation(
                &self.volume,
                generation.id,
                crate::kernel::RetentionKind::Checkpoint,
                label.as_str().to_owned(),
            )
            .await?;
        Ok(Checkpoint { label, generation })
    }

    /// Durably pins the current immutable generation under an opaque customer
    /// identity. Repeating the same identity at the same generation is exact
    /// and binding it to different state fails closed.
    ///
    /// # Errors
    ///
    /// Rejects invalid identities, generation authentication failures, or an
    /// existing identity bound to different state.
    pub async fn pin(
        &self,
        identity: impl AsRef<str>,
    ) -> Result<GenerationPin<A, O>, WorkspaceError> {
        self.head().await?.pin(identity).await
    }

    /// Terminally deletes this workspace head while leaving independently
    /// retained checkpoints, pins, and forks valid.
    ///
    /// # Errors
    ///
    /// Returns a storage or authority failure when deletion cannot be durably
    /// resolved. Semantic races are returned as typed outcomes.
    pub async fn delete(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<WorkspaceDelete, WorkspaceError> {
        self.volume
            .fs
            .delete_workspace_volume(&self.volume, idempotency_key.operation_id())
            .await
    }

    /// Computes one immutable bounded semantic change set between two exact
    /// generations of this workspace.
    ///
    /// # Errors
    ///
    /// Rejects foreign endpoints, zero bounds, malformed state, or a truncated
    /// authenticated diff frontier.
    pub async fn diff(
        &self,
        from: &Generation<A, O>,
        to: &Generation<A, O>,
        maximum_changes: u32,
    ) -> Result<ChangeSet<A, O>, WorkspaceError> {
        if from.workspace.id != self.id || to.workspace.id != self.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let receipt = self
            .volume
            .diff_generations(
                from.id,
                to.id,
                maximum_changes,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?;
        if receipt.value.truncated {
            return Err(WorkspaceError::JoinLimit);
        }
        Ok(ChangeSet {
            from: from.clone(),
            to: to.clone(),
            changes: receipt.value,
            work: receipt.work,
        })
    }

    /// Begins a side-effect-free join description from this workspace into a
    /// target workspace in the same filesystem deployment.
    #[must_use]
    pub fn join_into(&self, target: &Workspace<A, O>) -> JoinBuilder<A, O> {
        JoinBuilder {
            source: self.clone(),
            target: target.clone(),
            history: JoinHistory::Merge,
            maximum_generations: 4_096,
            maximum_changes: self.volume.config().limits.maximum_paths_per_batch,
            maximum_conflicts: 1_024,
        }
    }

    /// Advances this fork to its source workspace's current generation while
    /// preserving independently committed local changes.
    ///
    /// The source is recovered from authenticated immutable ancestry; no
    /// mutable fork catalog is required. Planning and merge are side-effect
    /// free, conflicts leave this workspace unchanged, and success publishes
    /// through one idempotent head CAS.
    ///
    /// # Errors
    ///
    /// Rejects a workspace that is not a fork, incompatible source semantics,
    /// exhausted lineage/conflict bounds, malformed state, or backend failure.
    pub async fn live_rebase(
        &self,
        idempotency_key: IdempotencyKey,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
    ) -> Result<WorkspaceRebase<A, O>, WorkspaceError> {
        let outcome = self
            .volume
            .fs
            .live_rebase_workspace(crate::facade::WorkspaceRebaseRequest {
                target: &self.volume,
                operation_id: idempotency_key.operation_id(),
                maximum_generations,
                maximum_changes,
                maximum_conflicts,
            })
            .await?;
        let generation = |id| Generation {
            workspace: self.clone(),
            id,
        };
        Ok(match outcome {
            crate::facade::WorkspaceJoinOutcome::Applied(id) => {
                WorkspaceRebase::Rebased(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::AlreadyApplied(id) => {
                WorkspaceRebase::AlreadyRebased(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::NoChanges(id) => {
                WorkspaceRebase::Current(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::Stale(id) => {
                WorkspaceRebase::Stale(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::Conflicted(conflicts, truncated) => {
                WorkspaceRebase::Conflicted {
                    conflicts,
                    truncated,
                }
            }
            crate::facade::WorkspaceJoinOutcome::Fenced => WorkspaceRebase::Fenced,
            crate::facade::WorkspaceJoinOutcome::IdempotencyConflict => {
                WorkspaceRebase::IdempotencyConflict
            }
        })
    }

    /// Reads at most `maximum_bytes` from one complete file at the current
    /// immutable head.
    ///
    /// # Errors
    ///
    /// Rejects invalid/absent/non-file paths, files above the supplied bound,
    /// and authenticated backend failures.
    pub async fn read(&self, path: &str, maximum_bytes: u64) -> Result<Bytes, WorkspaceError> {
        read_generation(self, GenerationSelector::Head, path, maximum_bytes).await
    }

    /// Reads one exact regular-file range without materializing unrelated bytes.
    ///
    /// # Errors
    ///
    /// Rejects invalid/absent/non-file paths, invalid ranges, and authenticated
    /// backend failures.
    pub async fn read_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, WorkspaceError> {
        read_generation_range(self, GenerationSelector::Head, path, offset, length).await
    }

    /// Returns complete path identity, kind, size, link count, and metadata.
    ///
    /// # Errors
    ///
    /// Rejects invalid or absent paths and authenticated backend failures.
    pub async fn stat(&self, path: &str) -> Result<WorkspaceStat, WorkspaceError> {
        stat_generation(self, GenerationSelector::Head, path).await
    }

    /// Returns one authenticated bounded directory page.
    ///
    /// # Errors
    ///
    /// Rejects invalid/absent/non-directory paths, invalid cursors or bounds,
    /// and authenticated backend failures.
    pub async fn list_directory(
        &self,
        path: &str,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<WorkspaceDirectoryPage, WorkspaceError> {
        list_generation_directory(self, GenerationSelector::Head, path, after, maximum_entries)
            .await
    }

    /// Reads exact opaque symbolic-link target bytes without following it.
    ///
    /// # Errors
    ///
    /// Rejects invalid/absent/non-symbolic-link paths and authenticated backend
    /// failures.
    pub async fn read_symbolic_link(&self, path: &str) -> Result<Bytes, WorkspaceError> {
        read_generation_symbolic_link(self, GenerationSelector::Head, path).await
    }

    /// Plans sparse logical spans without reading content bodies.
    ///
    /// # Errors
    ///
    /// Rejects invalid/absent/non-file paths, invalid ranges or bounds, and
    /// authenticated backend failures.
    pub async fn plan_extents(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        maximum_spans: u32,
    ) -> Result<WorkspaceExtentPlan, WorkspaceError> {
        plan_generation_extents(
            self,
            GenerationSelector::Head,
            path,
            offset,
            length,
            maximum_spans,
        )
        .await
    }

    /// Creates or replaces one complete UTF-8 file in a single atomic
    /// generation publication.
    ///
    /// # Errors
    ///
    /// Rejects invalid paths, unsupported destinations, stale publication, or
    /// authenticated storage failures.
    pub async fn write_text(
        &self,
        path: &str,
        text: &str,
    ) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        self.write(path, Bytes::copy_from_slice(text.as_bytes()))
            .await
    }

    /// Creates or replaces one complete file in a single atomic generation.
    ///
    /// # Errors
    ///
    /// Rejects invalid paths, unsupported destinations, stale publication,
    /// size limits, or authenticated storage failures.
    pub async fn write(
        &self,
        path: &str,
        bytes: Bytes,
    ) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        let mut transaction = self.begin_transaction(IdempotencyKey::new()).await?;
        transaction.write(path, bytes).await?;
        transaction.commit().await
    }

    /// Removes one existing path in a single atomic generation publication.
    ///
    /// # Errors
    ///
    /// Rejects invalid or absent paths and bounded engine failures.
    pub async fn remove(&self, path: &str) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        let mut transaction = self.begin_transaction(IdempotencyKey::new()).await?;
        transaction.remove(path).await?;
        transaction.commit().await
    }

    /// Copies one complete regular file within this workspace using immutable
    /// extent references instead of reading or copying file bodies.
    ///
    /// # Errors
    ///
    /// Rejects invalid paths, non-regular sources or destinations, and bounded
    /// publication failures.
    pub async fn copy(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        let mut transaction = self.begin_transaction(IdempotencyKey::new()).await?;
        transaction.copy(source, destination).await?;
        transaction.commit().await
    }

    async fn read_selected(
        &self,
        selector: GenerationSelector,
        path: &str,
        maximum_bytes: u64,
    ) -> Result<Bytes, WorkspaceError> {
        let mut checkout = self
            .engine_checkout(selector, CheckoutMode::read_only_pinned())
            .await?;
        let path = customer_path(path, checkout.volume_config().limits)?;
        let lookup = checkout
            .lookup_no_follow(
                &path,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?
            .value
            .record
            .ok_or(WorkspaceError::NotFound)?;
        if lookup.kind != FileKind::Regular {
            return Err(WorkspaceError::NotRegularFile);
        }
        let logical_bytes = regular_file_bytes(lookup)?;
        if logical_bytes > maximum_bytes {
            return Err(WorkspaceError::ReadLimitExceeded);
        }
        checkout
            .read_file_range(
                &path,
                crate::ByteRange {
                    offset: 0,
                    length: logical_bytes,
                },
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value.bytes)
            .map_err(WorkspaceError::engine)
    }

    pub(crate) async fn engine_checkout(
        &self,
        selector: GenerationSelector,
        mode: CheckoutMode,
    ) -> Result<Checkout<A, O>, WorkspaceError> {
        self.volume
            .checkout(
                selector,
                mode,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(WorkspaceError::engine)
    }
}

async fn read_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    maximum_bytes: u64,
) -> Result<Bytes, WorkspaceError> {
    workspace.read_selected(selector, path, maximum_bytes).await
}

async fn read_generation_range<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    offset: u64,
    length: u64,
) -> Result<Bytes, WorkspaceError> {
    let mut checkout = workspace
        .engine_checkout(selector, CheckoutMode::read_only_pinned())
        .await?;
    let path = customer_path(path, checkout.volume_config().limits)?;
    checkout
        .read_file_range(
            &path,
            crate::ByteRange { offset, length },
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value.bytes)
        .map_err(WorkspaceError::engine)
}

async fn stat_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
) -> Result<WorkspaceStat, WorkspaceError> {
    let mut checkout = workspace
        .engine_checkout(selector, CheckoutMode::read_only_pinned())
        .await?;
    let path = customer_path(path, checkout.volume_config().limits)?;
    let lookup = checkout
        .lookup_no_follow_with_metadata(
            &path,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map_err(WorkspaceError::engine)?
        .value
        .ok_or(WorkspaceError::NotFound)?;
    let logical_bytes = match &lookup.record.payload {
        FilePayload::InlineRegular(bytes) => {
            Some(u64::try_from(bytes.as_bytes().len()).map_err(WorkspaceError::engine)?)
        }
        FilePayload::Regular { logical_bytes, .. }
        | FilePayload::SymbolicLink {
            target_bytes: logical_bytes,
            ..
        }
        | FilePayload::ReparsePoint {
            payload_bytes: logical_bytes,
            ..
        } => Some(*logical_bytes),
        FilePayload::Directory { .. } | FilePayload::Device { .. } | FilePayload::Empty => None,
    };
    Ok(WorkspaceStat {
        file_id: lookup.record.file_id,
        kind: lookup.record.kind,
        link_count: lookup.record.link_count,
        logical_bytes,
        metadata: WorkspaceMetadata::from_engine(lookup.metadata),
    })
}

async fn list_generation_directory<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    after: Option<&LogicalName>,
    maximum_entries: u32,
) -> Result<WorkspaceDirectoryPage, WorkspaceError> {
    let mut checkout = workspace
        .engine_checkout(selector, CheckoutMode::read_only_pinned())
        .await?;
    let path = customer_path(path, checkout.volume_config().limits)?;
    checkout
        .list_directory(
            &path,
            after,
            maximum_entries,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| WorkspaceDirectoryPage {
            entries: receipt
                .value
                .entries
                .into_iter()
                .map(|entry| WorkspaceDirectoryEntry {
                    name: entry.name,
                    file_id: entry.file_id,
                    kind: entry.kind,
                })
                .collect(),
            has_more: receipt.value.has_more,
        })
        .map_err(WorkspaceError::engine)
}

async fn read_generation_symbolic_link<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
) -> Result<Bytes, WorkspaceError> {
    let mut checkout = workspace
        .engine_checkout(selector, CheckoutMode::read_only_pinned())
        .await?;
    let path = customer_path(path, checkout.volume_config().limits)?;
    checkout
        .read_symbolic_link(
            &path,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(WorkspaceError::engine)
}

async fn plan_generation_extents<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    offset: u64,
    length: u64,
    maximum_spans: u32,
) -> Result<WorkspaceExtentPlan, WorkspaceError> {
    let mut checkout = workspace
        .engine_checkout(selector, CheckoutMode::read_only_pinned())
        .await?;
    let path = customer_path(path, checkout.volume_config().limits)?;
    let plan = checkout
        .plan_file_extents(
            &path,
            crate::ByteRange { offset, length },
            maximum_spans,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map_err(WorkspaceError::engine)?
        .value;
    let spans = match plan {
        Some(plan) => plan
            .spans
            .into_iter()
            .map(|span| WorkspaceExtentSpan {
                offset: span.offset,
                length: span.length,
                source_end: span.source_end,
                kind: match span.kind {
                    ExtentKind::Hole => WorkspaceExtentKind::Hole,
                    ExtentKind::AllocatedZero => WorkspaceExtentKind::AllocatedZero,
                    ExtentKind::Content { .. } => WorkspaceExtentKind::Content,
                },
            })
            .collect(),
        None if length == 0 => Vec::new(),
        None => vec![WorkspaceExtentSpan {
            offset,
            length,
            source_end: offset
                .checked_add(length)
                .ok_or_else(|| WorkspaceError::engine("file range overflows"))?,
            kind: WorkspaceExtentKind::Content,
        }],
    };
    Ok(WorkspaceExtentPlan { spans })
}

/// One unpublished sparse atomic workspace transaction.
pub struct Transaction<A, O> {
    workspace: Workspace<A, O>,
    checkout: Checkout<A, O>,
    idempotency_key: IdempotencyKey,
    requires_rebase: bool,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    pub(crate) fn workspace_id(&self) -> WorkspaceId {
        self.workspace.id()
    }

    /// Creates one new regular file and rejects an existing destination.
    ///
    /// # Errors
    ///
    /// Rejects invalid or existing paths, unsupported metadata, size limits,
    /// and authenticated engine failures.
    pub async fn create_file(
        &mut self,
        path: &str,
        bytes: Bytes,
        metadata: FileMetadata,
    ) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config().limits)?;
        self.apply(vec![AuthoredMutation::CreateFile {
            path,
            bytes,
            metadata,
        }])
        .await
    }

    /// Streams bytes into immutable content without changing the transaction's
    /// candidate namespace. Unused content remains a safe collectible orphan.
    ///
    /// # Errors
    ///
    /// Returns source, size, cancellation, allocation, or storage failures.
    pub async fn stage_content<R: crate::kernel::AsyncBlobSource>(
        &self,
        source: &mut R,
        maximum_source_bytes: u64,
    ) -> Result<crate::StagedContent, WorkspaceError> {
        self.checkout
            .stage_content(
                source,
                maximum_source_bytes,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(WorkspaceError::engine)
    }

    /// Creates every missing directory on one canonical absolute path.
    ///
    /// # Errors
    ///
    /// Rejects invalid paths, a non-directory prefix, or a bounded engine failure.
    pub async fn create_dir_all(&mut self, path: &str) -> Result<(), WorkspaceError> {
        let portable = PortablePath::parse(path, self.checkout.volume_config().limits)
            .map_err(WorkspaceError::path)?;
        let mut current = String::new();
        for component in portable.components() {
            current.push('/');
            current.push_str(component);
            let path = customer_path(&current, self.checkout.volume_config().limits)?;
            let existing = self
                .checkout
                .lookup_no_follow(
                    &path,
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .await
                .map_err(WorkspaceError::engine)?
                .value
                .record;
            match existing {
                Some(value) if value.kind == FileKind::Directory => {}
                Some(_) => return Err(WorkspaceError::NotDirectory),
                None => {
                    self.apply(vec![AuthoredMutation::CreateDirectory {
                        path,
                        metadata: FileMetadata::default(),
                    }])
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// Creates exactly one empty directory with canonical metadata.
    ///
    /// # Errors
    ///
    /// Rejects invalid/existing paths or authenticated engine failures.
    pub async fn create_directory(&mut self, path: &str) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config().limits)?;
        self.apply(vec![AuthoredMutation::CreateDirectory {
            path,
            metadata: FileMetadata::default(),
        }])
        .await
    }

    /// Creates one symbolic link with opaque target bytes.
    ///
    /// # Errors
    ///
    /// Rejects invalid/existing paths, unsupported profiles, or engine failures.
    pub async fn create_symbolic_link(
        &mut self,
        path: &str,
        target: Bytes,
    ) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config().limits)?;
        self.apply(vec![AuthoredMutation::CreateSymbolicLink {
            path,
            target,
            metadata: FileMetadata::default(),
        }])
        .await
    }

    /// Creates or atomically replaces one complete UTF-8 file.
    ///
    /// # Errors
    ///
    /// Rejects invalid paths, non-regular destinations, size limits, and
    /// authenticated engine failures.
    pub async fn write_text(&mut self, path: &str, text: &str) -> Result<(), WorkspaceError> {
        self.write(path, Bytes::copy_from_slice(text.as_bytes()))
            .await
    }

    /// Creates or atomically replaces one complete file.
    ///
    /// # Errors
    ///
    /// Rejects invalid paths, non-regular destinations, size limits, and
    /// authenticated engine failures.
    pub async fn write(&mut self, path: &str, bytes: Bytes) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config().limits)?;
        let existing = self
            .checkout
            .lookup_no_follow(
                &path,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?
            .value
            .record;
        let operations = match existing {
            None => vec![AuthoredMutation::CreateFile {
                path,
                bytes,
                metadata: FileMetadata::default(),
            }],
            Some(value) if value.kind == FileKind::Regular => vec![
                AuthoredMutation::Resize {
                    path: path.clone(),
                    logical_bytes: 0,
                },
                AuthoredMutation::Write {
                    path,
                    offset: 0,
                    bytes,
                },
            ],
            Some(_) => return Err(WorkspaceError::NotRegularFile),
        };
        self.apply(operations).await
    }

    /// Creates or replaces one complete file by concatenating already staged
    /// authenticated content without copying part bodies.
    ///
    /// # Errors
    ///
    /// Rejects empty input, overflow, invalid destinations, and engine limits.
    pub async fn write_staged(
        &mut self,
        path: &str,
        parts: &[crate::StagedContent],
    ) -> Result<(), WorkspaceError> {
        if parts.is_empty() {
            return Err(WorkspaceError::EmptyContentSet);
        }
        let path = customer_path(path, self.checkout.volume_config().limits)?;
        let existing = self
            .checkout
            .lookup_no_follow(
                &path,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?
            .value
            .record;
        let mut operations = match existing {
            None => vec![AuthoredMutation::CreateFile {
                path: path.clone(),
                bytes: Bytes::new(),
                metadata: FileMetadata::default(),
            }],
            Some(record) if record.kind == FileKind::Regular => vec![AuthoredMutation::Resize {
                path: path.clone(),
                logical_bytes: 0,
            }],
            Some(_) => return Err(WorkspaceError::NotRegularFile),
        };
        let mut offset = 0_u64;
        for part in parts {
            if part.logical_bytes() != 0 {
                operations.push(AuthoredMutation::WriteFromContent {
                    path: path.clone(),
                    offset,
                    content: *part,
                });
            }
            offset = offset
                .checked_add(part.logical_bytes())
                .ok_or(WorkspaceError::ContentLengthOverflow)?;
        }
        self.apply(operations).await
    }

    /// Removes one existing namespace binding from this transaction.
    ///
    /// # Errors
    ///
    /// Rejects invalid or absent paths and authenticated engine failures.
    pub async fn remove(&mut self, path: &str) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config().limits)?;
        let existing = self
            .checkout
            .lookup_no_follow(
                &path,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?
            .value
            .record
            .ok_or(WorkspaceError::NotFound)?;
        self.apply(vec![AuthoredMutation::Remove {
            path,
            expected_file_id: Some(existing.file_id),
        }])
        .await
    }

    /// Replaces one destination with a complete copy-on-write clone.
    ///
    /// # Errors
    ///
    /// Rejects invalid paths, non-regular endpoints, and bounded engine errors.
    pub async fn copy(&mut self, source: &str, destination: &str) -> Result<(), WorkspaceError> {
        let limits = self.checkout.volume_config().limits;
        let source = customer_path(source, limits)?;
        let destination = customer_path(destination, limits)?;
        let source_record = self
            .checkout
            .lookup_no_follow(
                &source,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?
            .value
            .record
            .ok_or(WorkspaceError::NotFound)?;
        let logical_bytes = regular_file_bytes(source_record)?;
        let destination_record = self
            .checkout
            .lookup_no_follow(
                &destination,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?
            .value
            .record;
        let mut operations = match destination_record {
            None => vec![AuthoredMutation::CreateFile {
                path: destination.clone(),
                bytes: Bytes::new(),
                metadata: FileMetadata::default(),
            }],
            Some(record) if record.kind == FileKind::Regular => vec![AuthoredMutation::Resize {
                path: destination.clone(),
                logical_bytes: 0,
            }],
            Some(_) => return Err(WorkspaceError::NotRegularFile),
        };
        if logical_bytes != 0 {
            operations.push(AuthoredMutation::CloneRange(crate::FileCloneRequest {
                source,
                source_offset: 0,
                destination,
                destination_offset: 0,
                length: logical_bytes,
            }));
        }
        self.apply(operations).await
    }

    /// Atomically renames one namespace binding within the workspace.
    ///
    /// # Errors
    ///
    /// Rejects invalid or missing paths and bounded engine failures.
    pub async fn rename(&mut self, source: &str, destination: &str) -> Result<(), WorkspaceError> {
        self.rename_with_replace(source, destination, true).await
    }

    /// Atomically renames one namespace binding with an exact replacement policy.
    ///
    /// # Errors
    ///
    /// Rejects invalid or missing paths, a disallowed existing destination,
    /// and bounded engine failures.
    pub async fn rename_with_replace(
        &mut self,
        source: &str,
        destination: &str,
        replace: bool,
    ) -> Result<(), WorkspaceError> {
        let limits = self.checkout.volume_config().limits;
        self.apply(vec![AuthoredMutation::Rename {
            source: customer_path(source, limits)?,
            destination: customer_path(destination, limits)?,
            replace,
        }])
        .await
    }

    /// Creates one same-workspace hard link without copying file content.
    ///
    /// # Errors
    ///
    /// Rejects invalid/foreign paths, incompatible kinds, or engine failures.
    pub async fn hard_link(
        &mut self,
        source: &str,
        destination: &str,
    ) -> Result<(), WorkspaceError> {
        let limits = self.checkout.volume_config().limits;
        self.apply(vec![AuthoredMutation::HardLink {
            source: customer_path(source, limits)?,
            destination: customer_path(destination, limits)?,
        }])
        .await
    }

    /// Replaces one regular-file range without rewriting untouched extents.
    ///
    /// # Errors
    ///
    /// Rejects invalid paths, overflow, incompatible kinds, or engine failures.
    pub async fn write_range(
        &mut self,
        path: &str,
        offset: u64,
        bytes: Bytes,
    ) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config().limits)?;
        self.apply(vec![AuthoredMutation::Write {
            path,
            offset,
            bytes,
        }])
        .await
    }

    /// Changes one regular file's logical length while preserving sparse layout.
    ///
    /// # Errors
    ///
    /// Rejects invalid paths, incompatible kinds, limits, or engine failures.
    pub async fn resize(&mut self, path: &str, logical_bytes: u64) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config().limits)?;
        self.apply(vec![AuthoredMutation::Resize {
            path,
            logical_bytes,
        }])
        .await
    }

    /// Punches a hole or installs allocated zeros over one exact range.
    ///
    /// # Errors
    ///
    /// Rejects invalid ranges/paths, incompatible kinds, or engine failures.
    pub async fn zero_range(
        &mut self,
        path: &str,
        range: crate::ByteRange,
        allocated: bool,
        extend: bool,
    ) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config().limits)?;
        self.apply(vec![AuthoredMutation::ZeroRange {
            path,
            range,
            allocated,
            extend,
        }])
        .await
    }

    /// Preallocates one sparse range without replacing existing content.
    ///
    /// # Errors
    ///
    /// Rejects invalid ranges/paths, incompatible kinds, or engine failures.
    pub async fn preallocate(
        &mut self,
        path: &str,
        range: crate::ByteRange,
        keep_size: bool,
    ) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config().limits)?;
        self.apply(vec![AuthoredMutation::Preallocate {
            path,
            range,
            keep_size,
        }])
        .await
    }

    /// Clones one immutable range without reading or copying content bytes.
    ///
    /// # Errors
    ///
    /// Rejects invalid ranges/paths, incompatible files, or engine failures.
    pub async fn clone_range(
        &mut self,
        source: &str,
        source_offset: u64,
        destination: &str,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), WorkspaceError> {
        let limits = self.checkout.volume_config().limits;
        self.apply(vec![AuthoredMutation::CloneRange(
            crate::FileCloneRequest {
                source: customer_path(source, limits)?,
                source_offset,
                destination: customer_path(destination, limits)?,
                destination_offset,
                length,
            },
        )])
        .await
    }

    /// Replaces complete canonical metadata for one existing path.
    ///
    /// # Errors
    ///
    /// Rejects invalid/absent paths, unsupported metadata, or engine failures.
    pub async fn set_metadata(
        &mut self,
        path: &str,
        metadata: FileMetadata,
    ) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config().limits)?;
        self.apply(vec![AuthoredMutation::SetMetadata { path, metadata }])
            .await
    }

    /// Publishes the complete candidate with one stable idempotency key.
    /// Conflicts retain the candidate in `self` for explicit rebase or retry.
    ///
    /// # Errors
    ///
    /// Returns authentication, closure, storage, cancellation, or indeterminate
    /// authority failures. Semantic publication rejections are typed outcomes.
    pub async fn commit(&mut self) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        if self.requires_rebase {
            let retried = Box::pin(self.checkout.retry_stale_commit(
                self.idempotency_key.operation_id(),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            ))
            .await
            .map_err(WorkspaceError::engine)?
            .value;
            let Some(outcome) = retried else {
                return Ok(TransactionCommit::Conflict {
                    actual: Box::pin(self.workspace.head()).await?,
                });
            };
            return Box::pin(self.commit_outcome(outcome)).await;
        }
        let outcome = Box::pin(self.checkout.commit(
            self.idempotency_key.operation_id(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        ))
        .await
        .map_err(WorkspaceError::engine)?
        .value;
        Box::pin(self.commit_outcome(outcome)).await
    }

    async fn commit_outcome(
        &self,
        outcome: CheckoutCommitOutcome,
    ) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        match outcome {
            CheckoutCommitOutcome::Committed { generation_id, .. } => {
                Ok(TransactionCommit::Committed(Generation {
                    workspace: self.workspace.clone(),
                    id: generation_id,
                }))
            }
            CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => {
                Ok(TransactionCommit::AlreadyCommitted(Generation {
                    workspace: self.workspace.clone(),
                    id: generation_id,
                }))
            }
            CheckoutCommitOutcome::Conflict { .. } => Ok(TransactionCommit::Conflict {
                actual: Box::pin(self.workspace.head()).await?,
            }),
            CheckoutCommitOutcome::Fenced { .. } => Ok(TransactionCommit::Fenced),
            CheckoutCommitOutcome::IdempotencyConflict { .. } => {
                Ok(TransactionCommit::IdempotencyConflict)
            }
        }
    }

    /// Safely advances this retained sparse candidate to the current workspace
    /// head and replays only its local mutations.
    ///
    /// # Errors
    ///
    /// Returns bounded dependency-probe, authentication, storage, or replay
    /// failures. Semantic overlap is returned as a typed conflict and leaves
    /// the transaction unchanged.
    pub async fn rebase(
        &mut self,
        maximum_conflicts: u32,
    ) -> Result<TransactionRebase<A, O>, WorkspaceError> {
        let decision = self
            .checkout
            .rebase_head(
                maximum_conflicts,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?
            .value;
        Ok(match decision {
            crate::kernel::RebaseDecision::Safe { generation } => {
                self.requires_rebase = false;
                TransactionRebase::Rebased(Generation {
                    workspace: self.workspace.clone(),
                    id: generation,
                })
            }
            crate::kernel::RebaseDecision::Conflicted {
                conflicts,
                truncated,
            } => TransactionRebase::Conflicted {
                conflicts: conflicts
                    .into_iter()
                    .map(TransactionConflict::from_engine)
                    .collect(),
                truncated,
            },
        })
    }

    async fn apply(&mut self, operations: Vec<AuthoredMutation>) -> Result<(), WorkspaceError> {
        self.checkout
            .apply_authored_transaction(
                operations,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|_| ())
            .map_err(WorkspaceError::engine)
    }
}

/// Terminal result of one atomic workspace transaction publication.
pub enum TransactionCommit<A, O> {
    /// This call published the generation.
    Committed(Generation<A, O>),
    /// The same key and transaction were already durable.
    AlreadyCommitted(Generation<A, O>),
    /// Another writer changed an observed dependency first.
    Conflict {
        /// Actual immutable workspace head.
        actual: Generation<A, O>,
    },
    /// This writer was superseded by a newer authority epoch.
    Fenced,
    /// The key was previously bound to different transaction input.
    IdempotencyConflict,
}

/// Observation-safe advancement of one retained transaction candidate.
pub enum TransactionRebase<A, O> {
    /// The current head is now the candidate's immutable base and its sparse
    /// local mutations were replayed without crossing a dependency.
    Rebased(Generation<A, O>),
    /// One or more exact observed or mutated regions changed upstream.
    Conflicted {
        /// Stable region-specific conflicts bounded by the caller.
        conflicts: Vec<TransactionConflict>,
        /// Additional conflicts exceeded the retained result bound.
        truncated: bool,
    },
}

/// Terminal outcome of advancing one fork onto its source workspace.
pub enum WorkspaceRebase<A, O> {
    /// A new rebased generation became durable.
    Rebased(Generation<A, O>),
    /// The identical retry was already durable.
    AlreadyRebased(Generation<A, O>),
    /// The fork already includes the source's current generation.
    Current(Generation<A, O>),
    /// This workspace changed after rebase planning; retry from the new head.
    Stale(Generation<A, O>),
    /// Local and upstream semantic changes overlap.
    Conflicted {
        /// Stable path-independent conflict regions.
        conflicts: Vec<MergeConflict>,
        /// Additional conflicts exceeded the retained bound.
        truncated: bool,
    },
    /// Writer ownership changed before publication.
    Fenced,
    /// The retry identity was previously bound to another input.
    IdempotencyConflict,
}

/// One exact customer-visible dependency conflict.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionConflict {
    /// Exact semantic region that changed.
    pub region: TransactionConflictRegion,
    /// Whether local work observed, mutated, or both observed and mutated it.
    pub usage: TransactionDependencyUse,
    /// Digest of the old bounded semantic state; `None` means absent.
    pub expected: Option<crate::Digest>,
    /// Digest of the current bounded semantic state; `None` means absent.
    pub actual: Option<crate::Digest>,
}

/// Exact region retained by an observation-safe transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionConflictRegion {
    /// Complete path-independent file record.
    FileRecord(crate::FileId),
    /// Complete file metadata.
    Metadata(crate::FileId),
    /// Exact logical file length.
    FileLength(crate::FileId),
    /// Exact non-empty byte range.
    ContentRange {
        /// Stable file identity.
        file_id: crate::FileId,
        /// Inclusive logical offset.
        offset: u64,
        /// Positive byte length.
        length: u64,
    },
    /// One sparse seek observation.
    SparseSeek {
        /// Stable file identity.
        file_id: crate::FileId,
        /// Inclusive logical starting offset.
        offset: u64,
        /// Sparse class that was observed.
        target: TransactionSparseSeek,
    },
    /// One exact directory binding, including absence.
    DirectoryName {
        /// Stable parent directory identity.
        directory_id: crate::FileId,
        /// Exact canonical child name.
        name: LogicalName,
    },
    /// One exact bounded directory cursor interval.
    DirectoryRange {
        /// Stable parent directory identity.
        directory_id: crate::FileId,
        /// Exclusive lower cursor.
        after: Option<LogicalName>,
        /// Exact page bound that was observed.
        maximum_entries: u32,
    },
}

/// How one local transaction depended on a conflicting region.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionDependencyUse {
    /// Read-only observation.
    Observation,
    /// Mutation precondition.
    Mutation,
    /// Both observation and mutation precondition.
    ObservationAndMutation,
}

/// Sparse query class retained by a transaction dependency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionSparseSeek {
    /// Seek to represented data.
    Data,
    /// Seek to an unallocated hole.
    Hole,
}

impl TransactionConflict {
    fn from_engine(value: crate::kernel::RebaseConflict) -> Self {
        use crate::kernel::{DependencyRegion, DependencyState, DependencyUse, ExtentSeekTarget};
        let state = |value| match value {
            DependencyState::Absent => None,
            DependencyState::Present(digest) => Some(digest),
        };
        Self {
            region: match value.region {
                DependencyRegion::FileRecord(id) => TransactionConflictRegion::FileRecord(id),
                DependencyRegion::Metadata(id) => TransactionConflictRegion::Metadata(id),
                DependencyRegion::FileLength(id) => TransactionConflictRegion::FileLength(id),
                DependencyRegion::ContentRange {
                    file_id,
                    offset,
                    length,
                } => TransactionConflictRegion::ContentRange {
                    file_id,
                    offset,
                    length,
                },
                DependencyRegion::SparseSeek {
                    file_id,
                    offset,
                    target,
                } => TransactionConflictRegion::SparseSeek {
                    file_id,
                    offset,
                    target: match target {
                        ExtentSeekTarget::Data => TransactionSparseSeek::Data,
                        ExtentSeekTarget::Hole => TransactionSparseSeek::Hole,
                    },
                },
                DependencyRegion::DirectoryName { directory_id, name } => {
                    TransactionConflictRegion::DirectoryName { directory_id, name }
                }
                DependencyRegion::DirectoryRange {
                    directory_id,
                    after,
                    maximum_entries,
                } => TransactionConflictRegion::DirectoryRange {
                    directory_id,
                    after,
                    maximum_entries,
                },
            },
            usage: match value.usage {
                DependencyUse::Observation => TransactionDependencyUse::Observation,
                DependencyUse::Mutation => TransactionDependencyUse::Mutation,
                DependencyUse::ObservationAndMutation => {
                    TransactionDependencyUse::ObservationAndMutation
                }
            },
            expected: state(value.expected),
            actual: state(value.actual),
        }
    }
}

/// Immutable complete filesystem state belonging to one workspace.
pub struct Generation<A, O> {
    pub(crate) workspace: Workspace<A, O>,
    pub(crate) id: GenerationId,
}

/// Exact synchronization result for one workspace head observation.
pub struct WorkspaceSync<A, O> {
    generation: Generation<A, O>,
}

impl<A, O> WorkspaceSync<A, O> {
    /// Immutable generation selected at the synchronization boundary.
    #[must_use]
    pub const fn generation(&self) -> &Generation<A, O> {
        &self.generation
    }

    /// Consumes the receipt and returns its immutable generation.
    #[must_use]
    pub fn into_generation(self) -> Generation<A, O> {
        self.generation
    }
}

/// Durable human-readable retention of one exact immutable generation.
pub struct Checkpoint<A, O> {
    label: WorkspaceName,
    generation: Generation<A, O>,
}

/// Durable opaque retention of one exact immutable generation.
pub struct GenerationPin<A, O> {
    identity: WorkspaceName,
    generation: Generation<A, O>,
}

/// Terminal result of deleting one named workspace head.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceDelete {
    /// The terminal deletion fact became durable.
    Deleted,
    /// The workspace was already terminally deleted.
    AlreadyDeleted,
    /// Another authority operation won; reopen before retrying.
    Conflict,
    /// The retry identity was previously bound to different input.
    IdempotencyConflict,
}

/// Complete customer-visible facts for one path without following links.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceStat {
    /// Stable path-independent identity.
    pub file_id: crate::FileId,
    /// Exact filesystem object kind.
    pub kind: FileKind,
    /// Number of namespace bindings to this identity.
    pub link_count: u64,
    /// Logical bytes for regular files and symbolic links.
    pub logical_bytes: Option<u64>,
    /// Complete scalar cross-profile metadata and explicit opaque-payload presence.
    pub metadata: WorkspaceMetadata,
}

/// Customer-visible metadata without storage object identities.
///
/// `None` means the source profile did not represent or observe the fact; zero
/// remains an exact represented value. Opaque ACL, security-descriptor, and
/// named-attribute contents are accessed through their dedicated bounded APIs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkspaceMetadata {
    /// POSIX permission/type bits.
    pub posix_mode: Option<u32>,
    /// POSIX numeric owner identity.
    pub posix_uid: Option<u32>,
    /// POSIX numeric group identity.
    pub posix_gid: Option<u32>,
    /// POSIX inode flags.
    pub posix_flags: Option<u64>,
    /// Windows file-attribute bitset.
    pub windows_attributes: Option<u32>,
    /// Creation/birth time in signed Unix-epoch nanoseconds.
    pub created_ns: Option<i64>,
    /// Last content-modification time in signed Unix-epoch nanoseconds.
    pub modified_ns: Option<i64>,
    /// Last access time in signed Unix-epoch nanoseconds.
    pub accessed_ns: Option<i64>,
    /// Last metadata-change time in signed Unix-epoch nanoseconds.
    pub changed_ns: Option<i64>,
    /// Whether authenticated named attributes exist.
    pub has_named_attributes: bool,
    /// Whether an authenticated ACL exists.
    pub has_acl: bool,
    /// Whether an authenticated Windows security descriptor exists.
    pub has_security_descriptor: bool,
}

impl WorkspaceMetadata {
    fn from_engine(metadata: FileMetadata) -> Self {
        Self {
            posix_mode: metadata_value(metadata.posix_mode),
            posix_uid: metadata_value(metadata.posix_uid),
            posix_gid: metadata_value(metadata.posix_gid),
            posix_flags: metadata_value(metadata.posix_flags),
            windows_attributes: metadata_value(metadata.windows_attributes),
            created_ns: metadata_value(metadata.created_ns),
            modified_ns: metadata_value(metadata.modified_ns),
            accessed_ns: metadata_value(metadata.accessed_ns),
            changed_ns: metadata_value(metadata.changed_ns),
            has_named_attributes: matches!(metadata.named_attributes, MetadataField::Value(_)),
            has_acl: matches!(metadata.acl, MetadataField::Value(_)),
            has_security_descriptor: matches!(
                metadata.security_descriptor,
                MetadataField::Value(_)
            ),
        }
    }
}

const fn metadata_value<T: Copy>(field: MetadataField<T>) -> Option<T> {
    match field {
        MetadataField::Unavailable => None,
        MetadataField::Value(value) => Some(value),
    }
}

/// One exact child binding in an authenticated directory page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceDirectoryEntry {
    /// Exact profile-aware name.
    pub name: LogicalName,
    /// Stable path-independent target identity.
    pub file_id: crate::FileId,
    /// Exact target kind.
    pub kind: FileKind,
}

/// One bounded authenticated directory page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceDirectoryPage {
    /// Entries strictly after the supplied cursor.
    pub entries: Vec<WorkspaceDirectoryEntry>,
    /// Whether another page exists.
    pub has_more: bool,
}

/// Customer-visible sparse representation class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkspaceExtentKind {
    /// Unallocated logical bytes.
    Hole,
    /// Physically represented zeros.
    AllocatedZero,
    /// Immutable content bytes.
    Content,
}

/// One clipped sparse logical span.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkspaceExtentSpan {
    /// Inclusive logical offset.
    pub offset: u64,
    /// Positive span length.
    pub length: u64,
    /// Exclusive end of the complete source extent.
    pub source_end: u64,
    /// Physical representation class without backend identity.
    pub kind: WorkspaceExtentKind,
}

/// Bounded sparse plan without storage topology or object identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceExtentPlan {
    /// Ordered contiguous spans covering the request.
    pub spans: Vec<WorkspaceExtentSpan>,
}

/// Immutable semantic delta between two exact generations.
pub struct ChangeSet<A, O> {
    from: Generation<A, O>,
    to: Generation<A, O>,
    changes: GenerationDiff,
    work: crate::WorkCounters,
}

/// One changed exact path between two immutable generations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangedPath {
    /// Canonical exact path in either endpoint.
    pub path: NamespacePath,
    /// Complete path-independent record in the earlier generation, when present.
    pub before: Option<crate::kernel::FileRecord>,
    /// Complete path-independent record in the later generation, when present.
    pub after: Option<crate::kernel::FileRecord>,
}

impl<A, O> ChangeSet<A, O> {
    /// Exact immutable base endpoint.
    #[must_use]
    pub const fn from(&self) -> &Generation<A, O> {
        &self.from
    }

    /// Exact immutable resulting endpoint.
    #[must_use]
    pub const fn to(&self) -> &Generation<A, O> {
        &self.to
    }

    /// Stable path-independent records and namespace binding changes.
    #[must_use]
    pub const fn changes(&self) -> &GenerationDiff {
        &self.changes
    }

    /// Exact bounded work used to authenticate and compute this delta.
    #[must_use]
    pub const fn work(&self) -> crate::WorkCounters {
        self.work
    }
}

fn merge_binding_endpoint(
    paths: &mut BTreeMap<
        NamespacePath,
        (
            Option<crate::kernel::FileRecord>,
            Option<crate::kernel::FileRecord>,
        ),
    >,
    directories: &BTreeMap<FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>>,
    records: &BTreeMap<FileId, crate::kernel::FileRecord>,
    change: &crate::DirectoryBindingChange,
    limits: crate::model::VolumeLimits,
    before: bool,
) -> Result<(), WorkspaceError> {
    let Some(directories) = directories.get(&change.directory_id) else {
        return Ok(());
    };
    let binding = if before {
        change.before.as_ref()
    } else {
        change.after.as_ref()
    };
    let record = binding.and_then(|entry| records.get(&entry.file_id).copied());
    for (directory, _) in directories {
        let path = child_namespace_path(directory, change.name.clone(), limits)?;
        let endpoints = paths.entry(path).or_default();
        if before {
            endpoints.0 = record;
        } else {
            endpoints.1 = record;
        }
    }
    Ok(())
}

struct NamespaceRecordSearch {
    records: BTreeMap<FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>>,
    complete: bool,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ChangeSet<A, O> {
    /// Resolves every changed record or binding to exact paths with one bounded traversal of each
    /// endpoint. Equal Merkle subtrees remain skipped by the semantic diff that created this set.
    ///
    /// # Errors
    ///
    /// Returns [`WorkspaceError::ChangedPathLimit`] instead of a partial result when either
    /// endpoint exceeds `maximum_entries`.
    pub async fn changed_paths(
        &self,
        maximum_entries: u32,
    ) -> Result<Vec<ChangedPath>, WorkspaceError> {
        if self.changes.truncated {
            return Err(WorkspaceError::ChangedPathLimit);
        }
        let file_ids: BTreeSet<_> = self
            .changes
            .files
            .iter()
            .map(|change| change.file_id)
            .chain(
                self.changes
                    .bindings
                    .iter()
                    .map(|change| change.directory_id),
            )
            .chain(self.changes.bindings.iter().flat_map(|change| {
                change
                    .before
                    .iter()
                    .chain(change.after.iter())
                    .map(|entry| entry.file_id)
            }))
            .collect();
        let before = self
            .from
            .namespace_records_for_file_ids(file_ids.iter().copied(), maximum_entries);
        let after = self
            .to
            .namespace_records_for_file_ids(file_ids.iter().copied(), maximum_entries);
        let (before, after) = futures::try_join!(before, after)?;
        if !before.complete || !after.complete {
            return Err(WorkspaceError::ChangedPathLimit);
        }
        let before = before.records;
        let after = after.records;
        let mut paths = BTreeMap::<NamespacePath, (Option<_>, Option<_>)>::new();
        let before_records: BTreeMap<_, _> = before
            .iter()
            .filter_map(|(&file_id, records)| records.first().map(|(_, record)| (file_id, *record)))
            .collect();
        let after_records: BTreeMap<_, _> = after
            .iter()
            .filter_map(|(&file_id, records)| records.first().map(|(_, record)| (file_id, *record)))
            .collect();
        for records in before.values() {
            for (path, record) in records {
                paths.entry(path.clone()).or_default().0 = Some(*record);
            }
        }
        for records in after.values() {
            for (path, record) in records {
                paths.entry(path.clone()).or_default().1 = Some(*record);
            }
        }
        for change in &self.changes.bindings {
            merge_binding_endpoint(
                &mut paths,
                &before,
                &before_records,
                change,
                self.from.workspace.volume.config.limits,
                true,
            )?;
            merge_binding_endpoint(
                &mut paths,
                &after,
                &after_records,
                change,
                self.to.workspace.volume.config.limits,
                false,
            )?;
        }
        Ok(paths
            .into_iter()
            .filter(|(_, (before, after))| match (before, after) {
                (Some(before), Some(after)) => {
                    before.kind != after.kind
                        || before.metadata != after.metadata
                        || (before.kind != FileKind::Directory && before.payload != after.payload)
                }
                (None, None) => false,
                _ => true,
            })
            .map(|(path, (before, after))| ChangedPath {
                path,
                before,
                after,
            })
            .collect())
    }

    /// Composes contiguous immutable deltas into their exact net semantic
    /// change. Intermediate changes that cancel are absent from the result.
    ///
    /// The implementation diffs the outer Merkle endpoints instead of
    /// concatenating records, preserving canonical rename, hard-link, revert,
    /// and equal-subtree behavior.
    ///
    /// # Errors
    ///
    /// Rejects non-contiguous or foreign deltas and bounded diff exhaustion.
    pub async fn compose(
        &self,
        next: &ChangeSet<A, O>,
        maximum_changes: u32,
    ) -> Result<ChangeSet<A, O>, WorkspaceError> {
        if self.to.id != next.from.id
            || self.to.workspace.id != next.from.workspace.id
            || !self
                .to
                .workspace
                .volume
                .fs
                .same_deployment(&next.from.workspace.volume.fs)
        {
            return Err(WorkspaceError::ChangeSetContinuity);
        }
        self.from
            .workspace
            .diff(&self.from, &next.to, maximum_changes)
            .await
    }
}

/// How a successful join records immutable ancestry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinHistory {
    /// Preserve source and target as two parents.
    Merge,
    /// Replay source state onto the target as one target-parent generation.
    Rebase,
    /// Collapse the complete source delta into one target-parent generation.
    Squash,
    /// Apply the selected source delta as one target-parent generation.
    CherryPick,
}

/// Side-effect-free join builder.
pub struct JoinBuilder<A, O> {
    source: Workspace<A, O>,
    target: Workspace<A, O>,
    history: JoinHistory,
    maximum_generations: u32,
    maximum_changes: u32,
    maximum_conflicts: u32,
}

impl<A, O> JoinBuilder<A, O> {
    fn validate_bounds(&self) -> Result<(), WorkspaceError> {
        if self.maximum_generations == 0 || self.maximum_changes == 0 || self.maximum_conflicts == 0
        {
            Err(WorkspaceError::JoinLimit)
        } else {
            Ok(())
        }
    }

    /// Selects the immutable ancestry shape of a successful join.
    #[must_use]
    pub const fn history(mut self, history: JoinHistory) -> Self {
        self.history = history;
        self
    }

    /// Sets exact bounded lineage, diff, and conflict frontiers.
    #[must_use]
    pub const fn bounds(
        mut self,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
    ) -> Self {
        self.maximum_generations = maximum_generations;
        self.maximum_changes = maximum_changes;
        self.maximum_conflicts = maximum_conflicts;
        self
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> JoinBuilder<A, O> {
    /// Authenticates endpoints and discovers one bounded common ancestor
    /// without writing immutable objects or changing either authority.
    ///
    /// # Errors
    ///
    /// Rejects unrelated deployments, incompatible semantics, missing or
    /// over-bound lineage, and malformed authenticated state.
    pub async fn plan(self) -> Result<JoinPlan<A, O>, WorkspaceError> {
        self.validate_bounds()?;
        let source_head = self.source.head().await?;
        let (target_head_id, target_authority_head) = self
            .target
            .volume
            .fs
            .workspace_head_state(&self.target.volume)
            .await?;
        let target_head = Generation {
            workspace: self.target.clone(),
            id: target_head_id,
        };
        self.plan_generations(source_head, target_head, target_authority_head)
            .await
    }

    pub(crate) async fn plan_generations(
        self,
        source_head: Generation<A, O>,
        target_head: Generation<A, O>,
        target_authority_head: crate::Head,
    ) -> Result<JoinPlan<A, O>, WorkspaceError> {
        self.validate_bounds()?;
        if source_head.workspace.id != self.source.id || target_head.workspace.id != self.target.id
        {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let base = self
            .source
            .volume
            .fs
            .workspace_common_ancestor(&source_head, &target_head, self.maximum_generations)
            .await?;
        Ok(JoinPlan {
            source: self.source,
            target: self.target,
            source_head,
            target_head,
            target_authority_head,
            base,
            history: self.history,
            maximum_generations: self.maximum_generations,
            maximum_changes: self.maximum_changes,
            maximum_conflicts: self.maximum_conflicts,
        })
    }
}

/// Immutable side-effect-free join plan.
pub struct JoinPlan<A, O> {
    source: Workspace<A, O>,
    target: Workspace<A, O>,
    source_head: Generation<A, O>,
    target_head: Generation<A, O>,
    target_authority_head: crate::Head,
    base: crate::facade::WorkspaceCommonAncestor,
    history: JoinHistory,
    maximum_generations: u32,
    maximum_changes: u32,
    maximum_conflicts: u32,
}

impl<A, O> JoinPlan<A, O> {
    /// Source generation captured while planning.
    #[must_use]
    pub const fn source_head(&self) -> GenerationId {
        self.source_head.id
    }

    /// Target generation observed while planning.
    #[must_use]
    pub const fn target_head(&self) -> GenerationId {
        self.target_head.id
    }

    /// Exact discovered common ancestor.
    #[must_use]
    pub const fn common_ancestor(&self) -> GenerationId {
        self.base.id
    }

    /// Workspace volume that authenticated the exact common ancestor.
    #[must_use]
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) const fn common_ancestor_volume(&self) -> VolumeId {
        self.base.volume_id
    }

    /// Source workspace retained by the plan.
    #[must_use]
    pub const fn source(&self) -> &Workspace<A, O> {
        &self.source
    }
}

/// Atomic join application preconditions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplyOptions {
    /// Exact target generation that must still be current.
    pub if_target: GenerationId,
    /// Stable identity reused only for an exact retry.
    pub idempotency_key: IdempotencyKey,
}

/// Terminal semantic join outcome.
pub enum JoinOutcome<A, O> {
    /// A new target generation became durable.
    Applied(Generation<A, O>),
    /// The same exact application was already durable.
    AlreadyApplied(Generation<A, O>),
    /// Source changes were already represented by the target.
    NoChanges(Generation<A, O>),
    /// Target changed after planning; no join was published.
    StaleTarget(Generation<A, O>),
    /// Exact conflicts prevented candidate publication.
    Conflicted {
        /// Stable path-independent conflict regions.
        conflicts: Vec<MergeConflict>,
        /// Additional conflicts exceeded the retained result bound.
        truncated: bool,
    },
    /// Target writer ownership changed before publication.
    Fenced,
    /// Retry identity was previously bound to another join input.
    IdempotencyConflict,
}

pub(crate) enum WorkspaceRestoreOutcome {
    Restored(GenerationId),
    AlreadyRestored(GenerationId),
    Current(GenerationId),
    Stale(GenerationId),
    Fenced,
    IdempotencyConflict,
}

/// Terminal semantic outcome of an exact workspace-head restoration.
pub enum WorkspaceRestore<A, O> {
    /// The requested historical generation became current.
    Restored(Generation<A, O>),
    /// The same restoration was already durable under this retry identity.
    AlreadyRestored(Generation<A, O>),
    /// The requested generation was already current; no publication occurred.
    Current(Generation<A, O>),
    /// The workspace advanced after the caller observed it.
    Stale(Generation<A, O>),
    /// Workspace writer ownership changed before publication.
    Fenced,
    /// The retry identity was previously bound to another restoration.
    IdempotencyConflict,
}

/// One exact path rejected by a three-way path application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspacePathConflict {
    /// Portable path relative to the workspace root.
    pub path: String,
    /// Semantic category suitable for driver or UI selection.
    pub kind: ConflictKind,
}

/// Terminal outcome of applying a generation delta to selected paths.
pub enum WorkspacePathApply<A, O> {
    /// A new generation became durable.
    Applied(Generation<A, O>),
    /// The exact idempotent application was already durable.
    AlreadyApplied(Generation<A, O>),
    /// The requested delta was already represented by the live workspace.
    NoChanges(Generation<A, O>),
    /// Independent live changes overlap the requested generation delta.
    Conflicted(Vec<WorkspacePathConflict>),
    /// The live workspace advanced after the caller's observation.
    Stale(Generation<A, O>),
    /// Workspace writer ownership changed before publication.
    Fenced,
    /// The retry identity was previously used for different inputs.
    IdempotencyConflict,
}

/// Failure while resolving or validating a declarative real-workspace join.
#[derive(Debug, Error)]
pub enum DrivenJoinError {
    /// Workspace planning, storage, or publication failed.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// Driver selection, execution, or cache reuse failed.
    #[error(transparent)]
    Resolution(#[from] MergePlanResolutionError),
    /// Candidate generations or conflict identities do not match this join plan.
    #[error("merge candidate does not match the immutable workspace join plan")]
    StaleCandidate,
    /// The current kernel cannot safely materialize this synthesized resolution.
    #[error("merge resolution for {0:?} is not representable by this join")]
    UnsupportedResolution(ConflictKey),
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> JoinPlan<A, O> {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn source_changes(
        &self,
    ) -> Result<crate::FsReceipt<GenerationDiff>, WorkspaceError> {
        self.source
            .volume
            .fs
            .workspace_join_changes(self.base.id, &self.source_head, self.maximum_changes)
            .await
    }

    /// Applies this immutable plan through one target-head CAS.
    ///
    /// # Errors
    ///
    /// Returns authenticated storage, compatibility, bound, or publication
    /// failures. Semantic conflicts and races are typed outcomes.
    pub async fn apply(&self, options: ApplyOptions) -> Result<JoinOutcome<A, O>, WorkspaceError> {
        self.apply_resolutions(options, BTreeMap::new()).await
    }

    /// Expands exact kernel conflicts into immutable, driver-ready values.
    ///
    /// This is side-effect free and lets adapters present the same typed paths
    /// and alternatives used by [`Self::apply_with_drivers`].
    pub async fn describe_conflicts(
        &self,
        conflicts: &[MergeConflict],
        truncated: bool,
    ) -> Result<MergePlan, WorkspaceError> {
        self.typed_merge_plan(conflicts.to_vec(), truncated).await
    }

    /// Runs registered immutable-input drivers for real join conflicts and
    /// publishes the validated candidate through the ordinary target CAS.
    pub async fn apply_with_drivers<C: MergeResolutionCache>(
        &self,
        options: ApplyOptions,
        registry: &MergeDriverRegistry,
        cache: &mut C,
        replanning: bool,
    ) -> Result<JoinOutcome<A, O>, DrivenJoinError> {
        let initial = self.apply(options).await?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = initial
        else {
            return Ok(initial);
        };
        let plan = self.typed_merge_plan(conflicts, truncated).await?;
        let candidate = resolve_merge_plan(plan, registry, cache, replanning)?;
        self.apply_candidate(options, &candidate).await
    }

    /// Applies caller-selected immutable sides after validating that they
    /// exactly cover the conflicts produced by this join plan.
    pub async fn apply_sides(
        &self,
        options: ApplyOptions,
        selections: BTreeMap<MergeConflict, ConflictSide>,
    ) -> Result<JoinOutcome<A, O>, DrivenJoinError> {
        let initial = self.apply(options).await?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = initial
        else {
            return Ok(initial);
        };
        if truncated
            || conflicts.len() != selections.len()
            || conflicts
                .iter()
                .any(|conflict| !selections.contains_key(conflict))
        {
            return Err(DrivenJoinError::StaleCandidate);
        }
        let resolutions = selections
            .into_iter()
            .map(|(conflict, side)| {
                let side = match side {
                    ConflictSide::Base => crate::kernel::MergeConflictSide::Base,
                    ConflictSide::Ours => crate::kernel::MergeConflictSide::Ours,
                    ConflictSide::Theirs => crate::kernel::MergeConflictSide::Theirs,
                };
                (
                    conflict,
                    crate::kernel::MergeConflictResolution::Select(side),
                )
            })
            .collect();
        self.apply_resolutions(options, resolutions)
            .await
            .map_err(Into::into)
    }

    /// Validates caller-selected declarations against this exact immutable
    /// join and publishes them through the same fenced compare-and-swap path.
    pub async fn apply_candidate(
        &self,
        options: ApplyOptions,
        candidate: &UnpublishedMergeCandidate,
    ) -> Result<JoinOutcome<A, O>, DrivenJoinError> {
        if candidate.plan.base != self.base.id
            || candidate.plan.ours != self.target_head.id
            || candidate.plan.theirs != self.source_head.id
            || candidate.plan.truncated
            || candidate.plan.conflicts.len() != candidate.resolutions.len()
        {
            return Err(DrivenJoinError::StaleCandidate);
        }
        let mut resolutions = BTreeMap::new();
        for conflict in &candidate.plan.conflicts {
            let resolution = candidate
                .resolutions
                .get(&conflict.key)
                .ok_or(DrivenJoinError::StaleCandidate)?;
            if !resolution_matches_conflict(&conflict.key, resolution) {
                return Err(DrivenJoinError::UnsupportedResolution(conflict.key.clone()));
            }
            let resolution = match resolution {
                MergeResolution::Select(ConflictSide::Base) => {
                    crate::kernel::MergeConflictResolution::Select(
                        crate::kernel::MergeConflictSide::Base,
                    )
                }
                MergeResolution::Select(ConflictSide::Ours) => {
                    crate::kernel::MergeConflictResolution::Select(
                        crate::kernel::MergeConflictSide::Ours,
                    )
                }
                MergeResolution::Select(ConflictSide::Theirs) => {
                    crate::kernel::MergeConflictResolution::Select(
                        crate::kernel::MergeConflictSide::Theirs,
                    )
                }
                MergeResolution::Text(text) => crate::kernel::MergeConflictResolution::File(Some(
                    self.stage_regular_resolution(
                        &conflict.key,
                        Bytes::copy_from_slice(text.as_bytes()),
                    )
                    .await?,
                )),
                MergeResolution::Binary(bytes) => {
                    crate::kernel::MergeConflictResolution::File(Some(
                        self.stage_regular_resolution(&conflict.key, Bytes::copy_from_slice(bytes))
                            .await?,
                    ))
                }
                MergeResolution::Metadata(bytes) => {
                    let file_id = conflict_file_id(&conflict.key).ok_or_else(|| {
                        DrivenJoinError::UnsupportedResolution(conflict.key.clone())
                    })?;
                    let mut record = self
                        .target
                        .volume
                        .fs
                        .workspace_conflict_record(
                            &self.target.volume,
                            self.target_head.id,
                            file_id,
                        )
                        .await?
                        .ok_or(WorkspaceError::InvalidMergeResolution)?;
                    record.metadata = self
                        .target
                        .volume
                        .fs
                        .stage_merge_metadata(&self.target.volume, bytes)
                        .await?;
                    crate::kernel::MergeConflictResolution::File(Some(record))
                }
                MergeResolution::Binding(file_id) => {
                    crate::kernel::MergeConflictResolution::Binding(*file_id)
                }
                MergeResolution::Unresolved => {
                    return Err(DrivenJoinError::UnsupportedResolution(conflict.key.clone()));
                }
            };
            resolutions.insert(
                kernel_conflict(
                    &conflict.key,
                    self.target.volume.config.limits.maximum_component_bytes,
                )?,
                resolution,
            );
        }
        self.apply_resolutions(options, resolutions)
            .await
            .map_err(Into::into)
    }

    async fn stage_regular_resolution(
        &self,
        key: &ConflictKey,
        bytes: Bytes,
    ) -> Result<crate::kernel::FileRecord, DrivenJoinError> {
        let file_id = conflict_file_id(key)
            .ok_or_else(|| DrivenJoinError::UnsupportedResolution(key.clone()))?;
        let record = self
            .target
            .volume
            .fs
            .workspace_conflict_record(&self.target.volume, self.target_head.id, file_id)
            .await?
            .ok_or(WorkspaceError::InvalidMergeResolution)?;
        self.target
            .volume
            .fs
            .stage_merge_regular_record(&self.target.volume, record, bytes)
            .await
            .map_err(Into::into)
    }

    async fn typed_merge_plan(
        &self,
        conflicts: Vec<MergeConflict>,
        truncated: bool,
    ) -> Result<MergePlan, WorkspaceError> {
        let mut typed = Vec::with_capacity(conflicts.len());
        for conflict in conflicts {
            typed.push(self.conflict_view(conflict).await?);
        }
        Ok(MergePlan {
            base: self.base.id,
            ours: self.target_head.id,
            theirs: self.source_head.id,
            conflicts: typed,
            truncated,
        })
    }

    async fn conflict_view(
        &self,
        conflict: MergeConflict,
    ) -> Result<crate::ConflictView, WorkspaceError> {
        let MergeConflict::File(file_id) = conflict else {
            let mut view = binding_conflict_view(self, conflict.clone());
            if let MergeConflict::Binding { directory_id, name } = conflict
                && let Some(directory) = self.conflict_path(directory_id).await?
                && let Ok(name) = logical_name_text(&name)
            {
                view.path = Some(if directory == "/" {
                    format!("/{name}")
                } else {
                    format!("{directory}/{name}")
                });
            }
            return Ok(view);
        };
        let path = self.conflict_path(file_id).await?;
        let fs = &self.target.volume.fs;
        let base = fs
            .workspace_conflict_record(&self.target.volume, self.base.id, file_id)
            .await?;
        let ours = fs
            .workspace_conflict_record(&self.target.volume, self.target_head.id, file_id)
            .await?;
        let theirs = fs
            .workspace_conflict_record(&self.target.volume, self.source_head.id, file_id)
            .await?;
        let fallback = |generation, record: Option<crate::kernel::FileRecord>| match record {
            Some(_) => ConflictValue::Record {
                generation,
                file_id,
            },
            None => ConflictValue::Absent,
        };
        let Some((base_record, ours_record, theirs_record)) = base
            .zip(ours)
            .zip(theirs)
            .map(|((base, ours), theirs)| (base, ours, theirs))
        else {
            return Ok(crate::ConflictView {
                key: ConflictKey::File(file_id),
                path,
                kind: ConflictKind::Record,
                base: fallback(self.base.id, base),
                ours: fallback(self.target_head.id, ours),
                theirs: fallback(self.source_head.id, theirs),
            });
        };
        if base_record.link_count != ours_record.link_count
            || base_record.link_count != theirs_record.link_count
        {
            return Ok(record_conflict_view(
                self,
                file_id,
                path,
                ConflictKind::HardLink,
            ));
        }
        if base_record.metadata != ours_record.metadata
            || base_record.metadata != theirs_record.metadata
        {
            let values = [base_record, ours_record, theirs_record];
            let mut metadata = Vec::with_capacity(3);
            for record in values {
                metadata.push(ConflictValue::Metadata(
                    fs.workspace_conflict_metadata(&self.target.volume, record)
                        .await?,
                ));
            }
            return Ok(crate::ConflictView {
                key: ConflictKey::Metadata(file_id),
                path,
                kind: ConflictKind::Metadata,
                base: metadata.remove(0),
                ours: metadata.remove(0),
                theirs: metadata.remove(0),
            });
        }
        if let Some(view) = self
            .regular_conflict_view(
                file_id,
                path.clone(),
                [base_record, ours_record, theirs_record],
            )
            .await?
        {
            return Ok(view);
        }
        let kind = match base_record.kind {
            FileKind::Directory => ConflictKind::Directory,
            FileKind::SymbolicLink => ConflictKind::SymbolicLink,
            _ => ConflictKind::Special,
        };
        Ok(record_conflict_view(self, file_id, path, kind))
    }

    async fn conflict_path(&self, file_id: FileId) -> Result<Option<String>, WorkspaceError> {
        let maximum = self.maximum_changes.max(1);
        let mut paths = self.target_head.paths_for_file_id(file_id, maximum).await?;
        if paths.is_empty() {
            paths = self.source_head.paths_for_file_id(file_id, maximum).await?;
        }
        Ok(paths.into_iter().next())
    }

    async fn regular_conflict_view(
        &self,
        file_id: FileId,
        path: Option<String>,
        records: [crate::kernel::FileRecord; 3],
    ) -> Result<Option<crate::ConflictView>, WorkspaceError> {
        if records
            .iter()
            .any(|record| record.kind != FileKind::Regular)
        {
            return Ok(None);
        }
        let mut bodies = Vec::with_capacity(3);
        for record in records {
            let Some(bytes) = self
                .target
                .volume
                .fs
                .workspace_conflict_bytes(&self.target.volume, record)
                .await?
            else {
                return Ok(Some(record_conflict_view(
                    self,
                    file_id,
                    path,
                    ConflictKind::Binary,
                )));
            };
            bodies.push(bytes);
        }
        let text = bodies
            .iter()
            .all(|bytes| std::str::from_utf8(bytes).is_ok());
        let mut values = bodies.into_iter().map(|bytes| {
            if text {
                ConflictValue::Text(String::from_utf8(bytes.to_vec()).unwrap_or_default())
            } else {
                ConflictValue::Binary(bytes.to_vec())
            }
        });
        Ok(Some(crate::ConflictView {
            key: ConflictKey::File(file_id),
            path,
            kind: if text {
                ConflictKind::Text
            } else {
                ConflictKind::Binary
            },
            base: values
                .next()
                .ok_or(WorkspaceError::InvalidMergeResolution)?,
            ours: values
                .next()
                .ok_or(WorkspaceError::InvalidMergeResolution)?,
            theirs: values
                .next()
                .ok_or(WorkspaceError::InvalidMergeResolution)?,
        }))
    }

    async fn apply_resolutions(
        &self,
        options: ApplyOptions,
        resolutions: BTreeMap<MergeConflict, crate::kernel::MergeConflictResolution>,
    ) -> Result<JoinOutcome<A, O>, WorkspaceError> {
        if options.if_target != self.target_head.id {
            return Ok(JoinOutcome::StaleTarget(self.target.head().await?));
        }
        let outcome = self
            .target
            .volume
            .fs
            .apply_workspace_join(crate::facade::WorkspaceJoinRequest {
                target: &self.target.volume,
                base: self.base,
                source: &self.source_head,
                expected_target: options.if_target,
                expected_head: self.target_authority_head,
                history: self.history,
                operation_id: options.idempotency_key.operation_id(),
                maximum_generations: self.maximum_generations,
                maximum_changes: self.maximum_changes,
                maximum_conflicts: self.maximum_conflicts,
                resolutions,
            })
            .await?;
        let generation = |id| Generation {
            workspace: self.target.clone(),
            id,
        };
        Ok(match outcome {
            crate::facade::WorkspaceJoinOutcome::Applied(id) => {
                JoinOutcome::Applied(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::AlreadyApplied(id) => {
                JoinOutcome::AlreadyApplied(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::NoChanges(id) => {
                JoinOutcome::NoChanges(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::Stale(id) => {
                JoinOutcome::StaleTarget(generation(id))
            }
            crate::facade::WorkspaceJoinOutcome::Conflicted(conflicts, truncated) => {
                JoinOutcome::Conflicted {
                    conflicts,
                    truncated,
                }
            }
            crate::facade::WorkspaceJoinOutcome::Fenced => JoinOutcome::Fenced,
            crate::facade::WorkspaceJoinOutcome::IdempotencyConflict => {
                JoinOutcome::IdempotencyConflict
            }
        })
    }
}

fn binding_conflict_view<A, O>(
    plan: &JoinPlan<A, O>,
    conflict: MergeConflict,
) -> crate::ConflictView {
    match conflict {
        MergeConflict::File(file_id) => crate::ConflictView {
            key: ConflictKey::File(file_id),
            path: None,
            kind: ConflictKind::Record,
            base: ConflictValue::RecordReference {
                generation: plan.base.id,
                file_id,
            },
            ours: ConflictValue::RecordReference {
                generation: plan.target_head.id,
                file_id,
            },
            theirs: ConflictValue::RecordReference {
                generation: plan.source_head.id,
                file_id,
            },
        },
        MergeConflict::Binding { directory_id, name } => {
            let name = canonical_name_key(&name);
            let value = |generation| ConflictValue::BindingReference {
                generation,
                directory_id,
                name: name.clone(),
            };
            crate::ConflictView {
                key: ConflictKey::Binding {
                    directory_id,
                    name: name.clone(),
                },
                path: None,
                kind: ConflictKind::Binding,
                base: value(plan.base.id),
                ours: value(plan.target_head.id),
                theirs: value(plan.source_head.id),
            }
        }
    }
}

fn record_conflict_view<A, O>(
    plan: &JoinPlan<A, O>,
    file_id: FileId,
    path: Option<String>,
    kind: ConflictKind,
) -> crate::ConflictView {
    let value = |generation| ConflictValue::Record {
        generation,
        file_id,
    };
    crate::ConflictView {
        key: ConflictKey::File(file_id),
        path,
        kind,
        base: value(plan.base.id),
        ours: value(plan.target_head.id),
        theirs: value(plan.source_head.id),
    }
}

fn canonical_name_key(name: &LogicalName) -> Vec<u8> {
    let mut key = Vec::with_capacity(name.as_bytes().len().saturating_add(1));
    key.push(match name.encoding() {
        crate::kernel::NameEncoding::Utf8 => 1,
        crate::kernel::NameEncoding::PosixBytes => 2,
        crate::kernel::NameEncoding::WindowsUtf16Le => 3,
    });
    key.extend_from_slice(name.as_bytes());
    key
}

fn logical_name_text(name: &LogicalName) -> Result<&str, WorkspaceError> {
    match name.encoding() {
        crate::kernel::NameEncoding::Utf8 => {
            std::str::from_utf8(name.as_bytes()).map_err(WorkspaceError::path)
        }
        crate::kernel::NameEncoding::PosixBytes | crate::kernel::NameEncoding::WindowsUtf16Le => {
            Err(WorkspaceError::path(
                "non-UTF-8 path cannot be projected as a portable string",
            ))
        }
    }
}

fn namespace_path_text(path: &NamespacePath) -> Result<String, WorkspaceError> {
    if path.is_root() {
        return Ok("/".to_owned());
    }
    let components = path
        .components()
        .iter()
        .map(logical_name_text)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("/{}", components.join("/")))
}

fn child_namespace_path(
    directory: &NamespacePath,
    name: LogicalName,
    limits: crate::model::VolumeLimits,
) -> Result<NamespacePath, WorkspaceError> {
    let mut components = directory.components().to_vec();
    components.push(name);
    NamespacePath::new(components, limits).map_err(WorkspaceError::path)
}

fn kernel_conflict(
    key: &ConflictKey,
    maximum_component_bytes: u32,
) -> Result<MergeConflict, DrivenJoinError> {
    match key {
        ConflictKey::File(file_id)
        | ConflictKey::Metadata(file_id)
        | ConflictKey::HardLinks(file_id)
        | ConflictKey::Directory(file_id)
        | ConflictKey::SpecialPayload(file_id)
        | ConflictKey::ContentRange { file_id, .. }
        | ConflictKey::Rename { file_id, .. } => Ok(MergeConflict::File(*file_id)),
        ConflictKey::Binding { directory_id, name } => {
            let (&tag, bytes) = name.split_first().ok_or(DrivenJoinError::StaleCandidate)?;
            let encoding = match tag {
                1 => crate::kernel::NameEncoding::Utf8,
                2 => crate::kernel::NameEncoding::PosixBytes,
                3 => crate::kernel::NameEncoding::WindowsUtf16Le,
                _ => return Err(DrivenJoinError::StaleCandidate),
            };
            let name = LogicalName::new(encoding, bytes.to_vec(), maximum_component_bytes)
                .map_err(|_| DrivenJoinError::StaleCandidate)?;
            Ok(MergeConflict::Binding {
                directory_id: *directory_id,
                name,
            })
        }
    }
}

fn conflict_file_id(key: &ConflictKey) -> Option<FileId> {
    match key {
        ConflictKey::File(file_id)
        | ConflictKey::Metadata(file_id)
        | ConflictKey::HardLinks(file_id)
        | ConflictKey::Directory(file_id)
        | ConflictKey::SpecialPayload(file_id)
        | ConflictKey::ContentRange { file_id, .. }
        | ConflictKey::Rename { file_id, .. } => Some(*file_id),
        ConflictKey::Binding { .. } => None,
    }
}

fn resolution_matches_conflict(key: &ConflictKey, resolution: &MergeResolution) -> bool {
    match resolution {
        MergeResolution::Select(_) | MergeResolution::Unresolved => true,
        MergeResolution::Text(_) | MergeResolution::Binary(_) => {
            matches!(key, ConflictKey::File(_) | ConflictKey::ContentRange { .. })
        }
        MergeResolution::Metadata(_) => matches!(key, ConflictKey::Metadata(_)),
        MergeResolution::Binding(_) => matches!(key, ConflictKey::Binding { .. }),
    }
}

#[cfg(test)]
mod merge_resolution_tests {
    use super::*;

    #[test]
    fn synthesized_resolutions_match_only_their_exact_conflict_shape() {
        let file_id = FileId::new();
        let file = ConflictKey::File(file_id);
        let metadata = ConflictKey::Metadata(file_id);
        let binding = ConflictKey::Binding {
            directory_id: file_id,
            name: vec![1, b'x'],
        };

        for resolution in [
            MergeResolution::Text("merged".into()),
            MergeResolution::Binary(vec![1]),
        ] {
            assert!(resolution_matches_conflict(&file, &resolution));
            assert!(!resolution_matches_conflict(&metadata, &resolution));
            assert!(!resolution_matches_conflict(&binding, &resolution));
        }
        let metadata_resolution = MergeResolution::Metadata(vec![1]);
        assert!(resolution_matches_conflict(&metadata, &metadata_resolution));
        assert!(!resolution_matches_conflict(&file, &metadata_resolution));
        let binding_resolution = MergeResolution::Binding(Some(file_id));
        assert!(resolution_matches_conflict(&binding, &binding_resolution));
        assert!(!resolution_matches_conflict(&file, &binding_resolution));
        assert!(resolution_matches_conflict(
            &file,
            &MergeResolution::Select(ConflictSide::Ours)
        ));
    }
}

impl<A, O> GenerationPin<A, O> {
    /// Canonical opaque pin identity.
    #[must_use]
    pub fn identity(&self) -> &WorkspaceName {
        &self.identity
    }

    /// Exact retained immutable generation.
    #[must_use]
    pub const fn generation(&self) -> &Generation<A, O> {
        &self.generation
    }
}

impl<A, O> Checkpoint<A, O> {
    /// Canonical retained label.
    #[must_use]
    pub fn label(&self) -> &WorkspaceName {
        &self.label
    }

    /// Exact retained immutable generation.
    #[must_use]
    pub const fn generation(&self) -> &Generation<A, O> {
        &self.generation
    }
}

impl<A, O> Clone for Generation<A, O> {
    fn clone(&self) -> Self {
        Self {
            workspace: self.workspace.clone(),
            id: self.id,
        }
    }
}

impl<A, O> Generation<A, O> {
    /// Content-addressed generation identity.
    #[must_use]
    pub const fn id(&self) -> GenerationId {
        self.id
    }

    /// Owning workspace identity.
    #[must_use]
    pub const fn workspace_id(&self) -> WorkspaceId {
        self.workspace.id
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Computes one semantic delta to a compatible generation in any workspace
    /// from the same filesystem deployment.
    pub async fn diff_to(
        &self,
        to: &Generation<A, O>,
        maximum_changes: u32,
    ) -> Result<ChangeSet<A, O>, WorkspaceError> {
        let receipt = self
            .workspace
            .volume
            .fs
            .workspace_join_changes(self.id, to, maximum_changes)
            .await?;
        Ok(ChangeSet {
            from: self.clone(),
            to: to.clone(),
            changes: receipt.value,
            work: receipt.work,
        })
    }

    /// Materializes this immutable generation into an existing empty host
    /// directory using the SDK's native capability-rooted adapter.
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub async fn materialize(
        &self,
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError> {
        let mut checkout = self
            .workspace
            .engine_checkout(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
            )
            .await?;
        crate::materialize_checkout(&mut checkout, options, budget, cancellation)
            .await
            .map_err(|failure| WorkspaceError::engine(failure.error))
    }

    /// Materializes one path from this generation below an existing empty
    /// host directory. The same relative path is reproduced below the
    /// destination, allowing callers to stage and atomically exchange it.
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub async fn materialize_path(
        &self,
        path: &str,
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError> {
        let mut checkout = self
            .workspace
            .engine_checkout(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
            )
            .await?;
        let path = customer_path(path, checkout.volume_config().limits)?;
        crate::materialize_checkout_path(&mut checkout, &path, options, budget, cancellation)
            .await
            .map_err(|failure| WorkspaceError::engine(failure.error))
    }

    /// Returns the exact immutable parent generation identities.
    ///
    /// # Errors
    ///
    /// Returns an authenticated storage or identity failure for malformed state.
    pub async fn parents(&self) -> Result<Vec<GenerationId>, WorkspaceError> {
        self.workspace
            .volume
            .fs
            .generation_parents(&self.workspace.volume, self.id)
            .await
    }

    /// Reads at most `maximum_bytes` from one complete file in this exact
    /// immutable generation.
    ///
    /// # Errors
    ///
    /// Rejects invalid, absent, non-file, oversized, or unauthenticated state.
    pub async fn read(&self, path: &str, maximum_bytes: u64) -> Result<Bytes, WorkspaceError> {
        read_generation(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            maximum_bytes,
        )
        .await
    }

    /// Reads one exact range from this immutable generation.
    ///
    /// # Errors
    ///
    /// Rejects invalid/absent/non-file paths, invalid ranges, and authenticated
    /// backend failures.
    pub async fn read_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, WorkspaceError> {
        read_generation_range(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            offset,
            length,
        )
        .await
    }

    /// Returns complete metadata for one path in this immutable generation.
    ///
    /// # Errors
    ///
    /// Rejects invalid or absent paths and authenticated backend failures.
    pub async fn stat(&self, path: &str) -> Result<WorkspaceStat, WorkspaceError> {
        stat_generation(&self.workspace, GenerationSelector::Exact(self.id), path).await
    }

    /// Resolves an ordered exact path batch while sharing authenticated directory and file-table
    /// frontiers. Absence is returned as `None`; links are not followed and file bodies are not
    /// read.
    ///
    /// # Errors
    ///
    /// Rejects an empty or excessive batch, foreign path encodings, cancellation, and
    /// authenticated backend failures.
    pub async fn lookup_paths(
        &self,
        paths: &[NamespacePath],
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::FsResult<Vec<Option<crate::kernel::FileRecord>>> {
        let checkout = self
            .workspace
            .volume
            .checkout(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
                budget,
                cancellation,
            )
            .await?;
        let checkout_work = checkout.work;
        let remaining = checkout_work
            .remaining(budget)
            .map_err(|error| crate::OperationFailure::new(error.into(), checkout_work))?;
        let mut checkout = checkout.value;
        let lookup = checkout
            .lookup_batch_no_follow(paths, remaining, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(checkout_work, |error| error))?;
        let work = checkout_work
            .checked_add(lookup.work)
            .map_err(|error| crate::OperationFailure::new(error.into(), checkout_work))?;
        Ok(crate::FsReceipt {
            value: lookup
                .value
                .entries
                .into_iter()
                .map(|entry| entry.record)
                .collect(),
            work,
        })
    }

    /// Returns one authenticated bounded directory page from this generation.
    ///
    /// # Errors
    ///
    /// Rejects invalid/absent/non-directory paths, invalid cursors or bounds,
    /// and authenticated backend failures.
    pub async fn list_directory(
        &self,
        path: &str,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<WorkspaceDirectoryPage, WorkspaceError> {
        list_generation_directory(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            after,
            maximum_entries,
        )
        .await
    }

    /// Finds portable paths bound to one stable file identity in this generation.
    ///
    /// The traversal is bounded by `maximum_entries`; results are sorted and
    /// include every matching hard link encountered before that bound.
    pub async fn paths_for_file_id(
        &self,
        file_id: FileId,
        maximum_entries: u32,
    ) -> Result<Vec<String>, WorkspaceError> {
        Ok(self
            .paths_for_file_ids([file_id], maximum_entries)
            .await?
            .remove(&file_id)
            .unwrap_or_default())
    }

    /// Finds portable paths for multiple stable file identities in one namespace traversal.
    ///
    /// The traversal is bounded by `maximum_entries`; each result is sorted and includes every
    /// matching hard link encountered before that bound. Identities without a matching binding
    /// are omitted.
    pub async fn paths_for_file_ids(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
    ) -> Result<BTreeMap<FileId, Vec<String>>, WorkspaceError> {
        let paths = self
            .namespace_paths_for_file_ids(file_ids, maximum_entries)
            .await?;
        paths
            .into_iter()
            .map(|(file_id, paths)| {
                paths
                    .into_iter()
                    .map(|path| namespace_path_text(&path))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|paths| (file_id, paths))
            })
            .collect()
    }

    async fn namespace_paths_for_file_ids(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
    ) -> Result<BTreeMap<FileId, Vec<NamespacePath>>, WorkspaceError> {
        Ok(self
            .namespace_records_for_file_ids(file_ids, maximum_entries)
            .await?
            .records
            .into_iter()
            .map(|(file_id, records)| {
                (file_id, records.into_iter().map(|(path, _)| path).collect())
            })
            .collect())
    }

    async fn namespace_records_for_file_ids(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
    ) -> Result<NamespaceRecordSearch, WorkspaceError> {
        let file_ids: BTreeSet<_> = file_ids.into_iter().collect();
        if file_ids.is_empty() {
            return Ok(NamespaceRecordSearch {
                records: BTreeMap::new(),
                complete: true,
            });
        }
        if maximum_entries == 0 {
            return Ok(NamespaceRecordSearch {
                records: BTreeMap::new(),
                complete: false,
            });
        }
        let maximum = usize::try_from(maximum_entries).unwrap_or(usize::MAX);
        let limits = self.workspace.volume.config.limits;
        let mut matches: BTreeMap<_, Vec<(NamespacePath, crate::kernel::FileRecord)>> =
            BTreeMap::new();
        let mut checkout = self
            .workspace
            .engine_checkout(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
            )
            .await?;
        let cancellation = crate::CancellationToken::new();
        let root = NamespacePath::new(Vec::new(), limits).map_err(WorkspaceError::path)?;
        if let Some(record) = checkout
            .lookup_no_follow(&root, crate::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(WorkspaceError::engine)?
            .value
            .record
            && file_ids.contains(&record.file_id)
        {
            matches
                .entry(record.file_id)
                .or_default()
                .push((root.clone(), record));
        }
        let mut pending = vec![root];
        let mut examined = 0_usize;
        while let Some(directory) = pending.pop() {
            let mut after = None;
            loop {
                let page = checkout
                    .list_directory_records(
                        &directory,
                        after.as_ref(),
                        1_024,
                        crate::WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await
                    .map_err(WorkspaceError::engine)?
                    .value;
                for entry in &page.entries {
                    examined = examined.saturating_add(1);
                    if examined > maximum {
                        for records in matches.values_mut() {
                            records.sort_by(|left, right| left.0.cmp(&right.0));
                        }
                        return Ok(NamespaceRecordSearch {
                            records: matches,
                            complete: false,
                        });
                    }
                    let mut components = directory.components().to_vec();
                    components.push(entry.name.clone());
                    let path =
                        NamespacePath::new(components, limits).map_err(WorkspaceError::path)?;
                    if file_ids.contains(&entry.record.file_id) {
                        matches
                            .entry(entry.record.file_id)
                            .or_default()
                            .push((path.clone(), entry.record));
                    }
                    if entry.record.kind == FileKind::Directory {
                        pending.push(path);
                    }
                }
                if !page.has_more {
                    break;
                }
                after = page.entries.last().map(|entry| entry.name.clone());
            }
        }
        for records in matches.values_mut() {
            records.sort_by(|left, right| left.0.cmp(&right.0));
        }
        Ok(NamespaceRecordSearch {
            records: matches,
            complete: true,
        })
    }

    /// Reads one opaque symbolic-link target from this generation.
    ///
    /// # Errors
    ///
    /// Rejects invalid/absent/non-symbolic-link paths and authenticated backend
    /// failures.
    pub async fn read_symbolic_link(&self, path: &str) -> Result<Bytes, WorkspaceError> {
        read_generation_symbolic_link(&self.workspace, GenerationSelector::Exact(self.id), path)
            .await
    }

    /// Plans sparse logical spans without reading content bodies.
    ///
    /// # Errors
    ///
    /// Rejects invalid/absent/non-file paths, invalid ranges or bounds, and
    /// authenticated backend failures.
    pub async fn plan_extents(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        maximum_spans: u32,
    ) -> Result<WorkspaceExtentPlan, WorkspaceError> {
        plan_generation_extents(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            offset,
            length,
            maximum_spans,
        )
        .await
    }

    /// Retains this exact immutable generation under one opaque stable identity.
    ///
    /// # Errors
    ///
    /// Rejects invalid identities, foreign or corrupt generation state, and
    /// conflicting reuse of an existing pin identity.
    pub async fn pin(
        &self,
        identity: impl AsRef<str>,
    ) -> Result<GenerationPin<A, O>, WorkspaceError> {
        let identity = WorkspaceName::new(identity)?;
        self.workspace
            .volume
            .fs
            .retain_workspace_generation(
                &self.workspace.volume,
                self.id,
                crate::kernel::RetentionKind::Pin,
                identity.as_str().to_owned(),
            )
            .await?;
        Ok(GenerationPin {
            identity,
            generation: self.clone(),
        })
    }
}

/// Exact source selection for a cheap workspace fork.
#[derive(Clone)]
pub struct ForkOptions<A, O> {
    pub(crate) generation: Generation<A, O>,
    pub(crate) idempotency_key: IdempotencyKey,
}

impl<A, O> ForkOptions<A, O> {
    /// Forks from one exact immutable generation.
    #[must_use]
    pub fn from_generation(generation: Generation<A, O>, idempotency_key: IdempotencyKey) -> Self {
        Self {
            generation,
            idempotency_key,
        }
    }
}

/// Canonical workspace-name validation failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum WorkspaceNameError {
    /// Names must contain at least one scalar.
    #[error("workspace name is empty")]
    Empty,
    /// Dot names are reserved for namespace traversal prevention.
    #[error("workspace name is reserved")]
    Reserved,
    /// Canonical UTF-8 exceeds the stable bound.
    #[error("workspace name exceeds 255 UTF-8 bytes")]
    TooLong,
    /// Names cannot contain separators or control characters.
    #[error("workspace name contains a path separator or control character")]
    InvalidCharacter,
}

/// Customer workspace operation failure.
#[derive(Debug, Error)]
pub enum WorkspaceError {
    /// Friendly name was invalid.
    #[error(transparent)]
    Name(#[from] WorkspaceNameError),
    /// The canonical engine rejected or could not complete the operation.
    #[error("workspace engine failure: {0}")]
    Engine(String),
    /// A fork generation belongs to another filesystem deployment.
    #[error("fork generation belongs to another filesystem deployment")]
    ForeignGeneration,
    /// Canonical customer path was invalid.
    #[error("invalid workspace path: {0}")]
    Path(String),
    /// Requested path does not exist.
    #[error("workspace path does not exist")]
    NotFound,
    /// An operation required a regular file.
    #[error("workspace path is not a regular file")]
    NotRegularFile,
    /// An operation required a directory.
    #[error("workspace path is not a directory")]
    NotDirectory,
    /// A bounded whole-file helper encountered a larger file.
    #[error("workspace file exceeds the caller's read bound")]
    ReadLimitExceeded,
    /// A retention label already identifies different immutable state.
    #[error("retention label is already bound to another generation")]
    RetentionConflict,
    /// Workspace semantics or lineage are incompatible for the requested join.
    #[error("workspaces are incompatible")]
    IncompatibleWorkspace,
    /// No common immutable ancestor exists within the admitted lineage.
    #[error("workspaces have no common ancestor")]
    NoCommonAncestor,
    /// Bounded lineage discovery exhausted its exact generation frontier.
    #[error("workspace lineage exceeds the configured bound")]
    LineageLimit,
    /// Join input, diff, or conflict bounds are zero or exhausted.
    #[error("workspace join exceeds its configured bound")]
    JoinLimit,
    /// A declarative merge resolution does not match its typed conflict.
    #[error("merge resolution is incompatible with its conflict")]
    InvalidMergeResolution,
    /// Change sets do not share one exact contiguous endpoint and deployment.
    #[error("change sets are not contiguous")]
    ChangeSetContinuity,
    /// Resolving changed paths exhausted the caller's namespace traversal bound.
    #[error("changed paths exceed the caller's traversal bound")]
    ChangedPathLimit,
    /// A staged concatenation omitted every part.
    #[error("staged content set is empty")]
    EmptyContentSet,
    /// The workspace has no authenticated foreign source in its ancestry.
    #[error("workspace is not a fork")]
    NotFork,
    /// Staged logical lengths overflowed the filesystem range.
    #[error("staged content length overflow")]
    ContentLengthOverflow,
}

impl WorkspaceError {
    pub(crate) fn engine(error: impl fmt::Display) -> Self {
        Self::Engine(error.to_string())
    }

    pub(crate) fn path(error: impl fmt::Display) -> Self {
        Self::Path(error.to_string())
    }
}

impl From<FsError> for WorkspaceError {
    fn from(error: FsError) -> Self {
        Self::Engine(error.to_string())
    }
}

pub(crate) fn customer_path(
    path: &str,
    limits: crate::model::VolumeLimits,
) -> Result<NamespacePath, WorkspaceError> {
    let portable = PortablePath::parse(path, limits).map_err(WorkspaceError::path)?;
    NamespacePath::from_portable(&portable, limits).map_err(WorkspaceError::path)
}

fn path_conflict_kind(
    base: Option<crate::kernel::FileRecord>,
    current: Option<crate::kernel::FileRecord>,
    source: Option<crate::kernel::FileRecord>,
) -> ConflictKind {
    let (Some(base), Some(current), Some(source)) = (base, current, source) else {
        return ConflictKind::Binding;
    };
    if base.kind != current.kind
        || base.kind != source.kind
        || base.file_id != current.file_id
        || base.file_id != source.file_id
    {
        return ConflictKind::Binding;
    }
    if base.link_count != current.link_count || base.link_count != source.link_count {
        return ConflictKind::HardLink;
    }
    if base.metadata != current.metadata || base.metadata != source.metadata {
        return ConflictKind::Metadata;
    }
    match base.kind {
        FileKind::Regular => ConflictKind::Binary,
        FileKind::Directory => ConflictKind::Directory,
        FileKind::SymbolicLink => ConflictKind::SymbolicLink,
        FileKind::Fifo
        | FileKind::Socket
        | FileKind::CharacterDevice
        | FileKind::BlockDevice
        | FileKind::ReparsePoint
        | FileKind::MountBoundary => ConflictKind::Special,
    }
}

fn regular_file_bytes(record: crate::kernel::FileRecord) -> Result<u64, WorkspaceError> {
    match record.payload {
        FilePayload::InlineRegular(bytes) => {
            Ok(u64::try_from(bytes.as_bytes().len()).unwrap_or(u64::MAX))
        }
        FilePayload::Regular { logical_bytes, .. } => Ok(logical_bytes),
        _ => Err(WorkspaceError::NotRegularFile),
    }
}

#[cfg(test)]
#[path = "tests/workspace.rs"]
mod tests;
