//! Customer workspace identities and immutable generation handles.
//!
//! A workspace is the sole customer-visible unit of mutation, publication,
//! retention, and convergence. The engine's volume and authority identities
//! remain implementation details.

use crate::foundation::{FileId, GenerationId, OperationId, VolumeId};
use crate::heap_future::in_heap;
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

    /// Returns the volume identity represented by this workspace identity.
    pub const fn volume_id(self) -> VolumeId {
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

    pub(crate) fn authority(&self) -> &A {
        self.volume.fs.authority()
    }

    pub(crate) const fn authority_id(&self) -> crate::AuthorityId {
        crate::kernel::volume_authority_id(self.id.volume_id())
    }

    /// Immutable canonical workspace name.
    #[must_use]
    pub fn name(&self) -> &WorkspaceName {
        &self.name
    }

    /// Derives the stable identity of a sibling workspace before creating it.
    ///
    /// Durable orchestration stores use this to record a recoverable fork
    /// intent before the child authority is created.
    pub fn fork_workspace_id(
        &self,
        destination: impl AsRef<str>,
    ) -> Result<WorkspaceId, WorkspaceNameError> {
        self.volume.fs.workspace_id(destination)
    }

    /// Exact immutable filesystem profile selected when this workspace was created.
    #[must_use]
    pub const fn profile(&self) -> crate::model::FilesystemProfile {
        self.volume.config.profile
    }

    /// Exact immutable bounds selected for this workspace volume.
    #[must_use]
    pub const fn limits(&self) -> crate::model::VolumeLimits {
        self.volume.config.limits
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Exclusively fences this workspace's mutable head while a cross-provider
    /// publication decides whether to expose a pinned generation. Exact
    /// retries with the same operation return the same durable reservation.
    pub async fn reserve_publication(
        &self,
        operation_id: OperationId,
    ) -> Result<crate::PublicationReservation, WorkspaceError> {
        let cancellation = crate::CancellationToken::new();
        let head = self
            .authority()
            .head(
                self.authority_id(),
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| WorkspaceError::engine(failure.error))?
            .value;
        match self
            .authority()
            .reserve_publication(
                self.authority_id(),
                head,
                operation_id,
                crate::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| WorkspaceError::engine(failure.error))?
            .value
        {
            crate::ReservationOutcome::Reserved(reservation)
            | crate::ReservationOutcome::AlreadyReserved(reservation) => Ok(reservation),
            crate::ReservationOutcome::Conflict { .. } => Err(WorkspaceError::StaleGeneration),
        }
    }

    /// Releases only this workspace's exact durable publication reservation.
    /// An ambiguous release response can be retried with the same proof.
    pub async fn release_publication(
        &self,
        reservation: crate::PublicationReservation,
    ) -> Result<(), WorkspaceError> {
        if reservation.authority_id != self.authority_id() {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        self.authority()
            .release_publication(
                reservation,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(|failure| WorkspaceError::engine(failure.error))?;
        Ok(())
    }

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

    /// Proves an immutable result was committed by this workspace's exact
    /// join operation, not by an unrelated mutation with a plausible parent.
    pub async fn verify_join_commit(
        &self,
        witness: &crate::JoinCommitWitness,
    ) -> Result<bool, WorkspaceError> {
        self.volume
            .fs
            .verify_workspace_join_commit(&self.volume, witness)
            .await
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
        self.head_measured(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
    }

    pub(crate) async fn head_measured(
        &self,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Generation<A, O>>, WorkspaceError> {
        in_heap(move || async move {
            let receipt = self
                .volume
                .checkout(
                    GenerationSelector::Head,
                    CheckoutMode::read_only_pinned(),
                    budget,
                    cancellation,
                )
                .await
                .map_err(|failure| WorkspaceError::from(failure.error))?;
            Ok(crate::OperationReceipt {
                value: Generation {
                    workspace: self.clone(),
                    id: receipt.value.generation_id(),
                },
                work: receipt.work,
            })
        })
        .await
    }

    /// Selects the head generation and reports whether its file table holds
    /// a record for `file_id`: one keyed read, where finding the names bound
    /// to that identity is a namespace traversal. No name binds an identity
    /// without a record.
    pub(crate) async fn head_with_file_record_measured(
        &self,
        file_id: FileId,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<(Generation<A, O>, bool)>, WorkspaceError> {
        let reader = self.head_reader_measured(budget, cancellation).await?;
        let records = reader
            .value
            .file_records_by_id(
                &[file_id],
                reader
                    .work
                    .remaining(budget)
                    .map_err(WorkspaceError::engine)?,
                cancellation,
            )
            .await
            .map_err(|failure| WorkspaceError::from(failure.error))?;
        let work = reader
            .work
            .checked_add(records.work)
            .map_err(WorkspaceError::engine)?;
        Ok(crate::OperationReceipt {
            value: (
                Generation {
                    workspace: self.clone(),
                    id: reader.value.generation_id(),
                },
                records.value.first().is_some_and(Option::is_some),
            ),
            work,
        })
    }

    /// Opens one immutable reader pinned to the head generation, which can
    /// answer many identity questions, such as every hard-link alias check
    /// of a directory page, against one head.
    pub(crate) async fn head_reader_measured(
        &self,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<crate::PinnedReader<A, O>>, WorkspaceError> {
        let checkout = self
            .engine_checkout_measured(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                budget,
                cancellation,
            )
            .await?;
        Ok(crate::OperationReceipt {
            value: checkout
                .value
                .pinned_reader()
                .map_err(WorkspaceError::engine)?,
            work: checkout.work,
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
        self.generation_measured(
            generation_id,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
    }

    pub(crate) async fn generation_measured(
        &self,
        generation_id: GenerationId,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Generation<A, O>>, WorkspaceError> {
        let checkout = self
            .engine_checkout_measured(
                GenerationSelector::Exact(generation_id),
                CheckoutMode::read_only_pinned(),
                budget,
                cancellation,
            )
            .await?;
        Ok(crate::OperationReceipt {
            value: Generation {
                workspace: self.clone(),
                id: checkout.value.generation_id(),
            },
            work: checkout.work,
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
        self.restore_generation_with_permit(
            generation,
            if_current,
            idempotency_key,
            crate::PublicationPermit::Unrestricted,
        )
        .await
    }

    /// Restores one exact generation only while the supplied writer permit remains valid.
    pub async fn restore_generation_with_permit(
        &self,
        generation: &Generation<A, O>,
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
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
                permit,
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
        self.restore_paths_from_with_permit(
            source,
            paths,
            if_current,
            idempotency_key,
            crate::PublicationPermit::Unrestricted,
        )
        .await
    }

    /// Restores selected paths only while the supplied writer permit remains valid.
    pub async fn restore_paths_from_with_permit(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
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
        let config = self.volume.config;
        let cancellation = crate::CancellationToken::new();
        let parsed = paths
            .iter()
            .map(|path| {
                let relative = path.trim_start_matches('/');
                let absolute = format!("/{relative}");
                customer_path(&absolute, config).map(|path| (relative, path))
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
        transaction.commit_with_permit(permit).await
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
        self.apply_paths_from_with_permit(
            base,
            source,
            paths,
            if_current,
            idempotency_key,
            crate::PublicationPermit::Unrestricted,
        )
        .await
    }

    /// Applies an exact three-way path delta under one writer permit.
    #[allow(
        clippy::too_many_lines,
        reason = "one transaction keeps path planning, typed conflicts, and fenced publication atomic"
    )]
    pub async fn apply_paths_from_with_permit(
        &self,
        base: Option<&Generation<A, O>>,
        source: Option<&Generation<A, O>>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
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
        let config = self.volume.config;
        let cancellation = crate::CancellationToken::new();
        let parsed = paths
            .iter()
            .map(|path| {
                let relative = path.trim_start_matches('/');
                let absolute = format!("/{relative}");
                customer_path(&absolute, config).map(|path| (relative, path))
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
        Ok(match transaction.commit_with_permit(permit).await? {
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
        self.fork_measured(
            destination,
            options,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
    }

    pub(crate) async fn fork_measured(
        &self,
        destination: impl AsRef<str>,
        options: ForkOptions<A, O>,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Self>, WorkspaceError> {
        if options.generation.workspace.id != self.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let destination = WorkspaceName::new(destination)?;
        in_heap(|| {
            self.volume.fs.fork_workspace_measured(
                destination,
                &options.generation,
                options.idempotency_key,
                budget,
                cancellation,
            )
        })
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
        self.begin_transaction_measured(
            idempotency_key,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
    }

    pub(crate) async fn begin_transaction_measured(
        &self,
        idempotency_key: IdempotencyKey,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Transaction<A, O>>, WorkspaceError> {
        let head = self.head_measured(budget, cancellation).await?;
        let transaction = self
            .open_transaction_at_measured(
                &head.value,
                idempotency_key,
                false,
                head.work
                    .remaining(budget)
                    .map_err(WorkspaceError::engine)?,
                cancellation,
            )
            .await?;
        Ok(crate::OperationReceipt {
            value: transaction.value,
            work: head
                .work
                .checked_add(transaction.work)
                .map_err(WorkspaceError::engine)?,
        })
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

    pub(crate) async fn begin_pinned_transaction_measured(
        &self,
        generation: &Generation<A, O>,
        idempotency_key: IdempotencyKey,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Transaction<A, O>>, WorkspaceError> {
        if generation.workspace.id != self.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        self.open_transaction_at_measured(generation, idempotency_key, false, budget, cancellation)
            .await
    }

    async fn open_transaction_at(
        &self,
        generation: &Generation<A, O>,
        idempotency_key: IdempotencyKey,
        requires_rebase: bool,
    ) -> Result<Transaction<A, O>, WorkspaceError> {
        self.open_transaction_at_measured(
            generation,
            idempotency_key,
            requires_rebase,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
    }

    async fn open_transaction_at_measured(
        &self,
        generation: &Generation<A, O>,
        idempotency_key: IdempotencyKey,
        requires_rebase: bool,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Transaction<A, O>>, WorkspaceError> {
        let receipt = self
            .engine_checkout_measured(
                GenerationSelector::Exact(generation.id),
                CheckoutMode::tracking_transaction(),
                budget,
                cancellation,
            )
            .await?;
        let mut checkout = receipt.value;
        checkout.bind_authored_operation(idempotency_key.operation_id());
        Ok(crate::OperationReceipt {
            value: Transaction {
                checkout,
                workspace: self.clone(),
                idempotency_key,
                requires_rebase,
            },
            work: receipt.work,
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
            crate::facade::WorkspaceJoinOutcome::Joined(_, _)
            | crate::facade::WorkspaceJoinOutcome::AlreadyJoined(_, _) => {
                return Err(WorkspaceError::IncompatibleWorkspace);
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

    /// The parent head that [`Self::live_rebase`] would rebase this fork onto,
    /// or `None` when the fork is already based on it.
    pub(crate) async fn parent_advance(
        &self,
        maximum_generations: u32,
    ) -> Result<Option<GenerationId>, WorkspaceError> {
        self.volume
            .fs
            .parent_advance(&self.volume, maximum_generations)
            .await
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

    pub(crate) async fn stat_optional_measured(
        &self,
        path: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Option<WorkspaceStat>>, WorkspaceError> {
        stat_generation_optional_measured(
            self,
            GenerationSelector::Head,
            path,
            budget,
            cancellation,
        )
        .await
    }

    pub(crate) async fn record_by_id(
        &self,
        file_id: FileId,
    ) -> Result<crate::kernel::FileRecord, WorkspaceError> {
        let mut checkout = self
            .engine_checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned())
            .await?;
        checkout
            .read_file_record_by_id(
                file_id,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(WorkspaceError::engine)
    }

    /// Makes every object `records` reach durable before another durable
    /// store records them.
    pub(crate) async fn make_records_durable(
        &self,
        records: &[crate::kernel::FileRecord],
    ) -> Result<(), WorkspaceError> {
        self.volume
            .fs
            .make_records_durable(
                self.volume.config,
                records,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)
    }

    pub(crate) fn detached_record(
        &self,
        record: crate::kernel::FileRecord,
    ) -> crate::DetachedFile<A, O> {
        crate::DetachedFile::from_record(self.volume.clone(), record)
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

    // Lazy workspace composition still needs a bounded authenticated page
    // internally, even though the public eager-directory API is removed.
    pub(crate) async fn list_directory_measured(
        &self,
        path: &str,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<WorkspaceDirectoryPage>, WorkspaceError> {
        in_heap(move || async move {
            list_generation_directory_measured(
                self,
                GenerationSelector::Head,
                path,
                after,
                maximum_entries,
                budget,
                cancellation,
            )
            .await
        })
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
        let path = customer_path(path, checkout.volume_config())?;
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
        self.engine_checkout_measured(
            selector,
            mode,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
    }

    pub(crate) async fn engine_checkout_measured(
        &self,
        selector: GenerationSelector,
        mode: CheckoutMode,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Checkout<A, O>>, WorkspaceError> {
        self.volume
            .checkout(selector, mode, budget, cancellation)
            .await
            .map_err(|failure| WorkspaceError::from(failure.error))
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
    let path = customer_path(path, checkout.volume_config())?;
    checkout
        .read_file_range(
            &path,
            crate::ByteRange { offset, length },
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value.bytes)
        .map_err(|failure| WorkspaceError::from(failure.error))
}

async fn stat_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
) -> Result<WorkspaceStat, WorkspaceError> {
    stat_generation_measured(
        workspace,
        selector,
        path,
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
}

async fn stat_generation_measured<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<crate::OperationReceipt<WorkspaceStat>, WorkspaceError> {
    let receipt =
        stat_generation_optional_measured(workspace, selector, path, budget, cancellation).await?;
    Ok(crate::OperationReceipt {
        value: receipt.value.ok_or(WorkspaceError::NotFound)?,
        work: receipt.work,
    })
}

async fn stat_generation_optional_measured<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<crate::OperationReceipt<Option<WorkspaceStat>>, WorkspaceError> {
    let checkout = workspace
        .engine_checkout_measured(
            selector,
            CheckoutMode::read_only_pinned(),
            budget,
            cancellation,
        )
        .await?;
    let mut work = checkout.work;
    let mut checkout = checkout.value;
    let path = customer_path(path, checkout.volume_config())?;
    let lookup = checkout
        .lookup_no_follow_with_metadata(
            &path,
            work.remaining(budget).map_err(WorkspaceError::from)?,
            cancellation,
        )
        .await
        .map_err(|failure| WorkspaceError::from(failure.error))?;
    work = work
        .checked_add(lookup.work)
        .map_err(WorkspaceError::from)?;
    let Some(lookup) = lookup.value else {
        return Ok(crate::OperationReceipt { value: None, work });
    };
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
    Ok(crate::OperationReceipt {
        value: Some(WorkspaceStat {
            file_id: lookup.record.file_id,
            kind: lookup.record.kind,
            link_count: lookup.record.link_count,
            logical_bytes,
            metadata: WorkspaceMetadata::from_engine(lookup.metadata),
        }),
        work,
    })
}

async fn list_generation_directory<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    after: Option<&LogicalName>,
    maximum_entries: u32,
) -> Result<WorkspaceDirectoryPage, WorkspaceError> {
    list_generation_directory_measured(
        workspace,
        selector,
        path,
        after,
        maximum_entries,
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .await
    .map(|receipt| receipt.value)
}

#[allow(clippy::too_many_arguments)]
async fn list_generation_directory_measured<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    after: Option<&LogicalName>,
    maximum_entries: u32,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<crate::OperationReceipt<WorkspaceDirectoryPage>, WorkspaceError> {
    let checkout = workspace
        .engine_checkout_measured(
            selector,
            CheckoutMode::read_only_pinned(),
            budget,
            cancellation,
        )
        .await?;
    let prior = checkout.work;
    let mut checkout = checkout.value;
    let path = customer_path(path, checkout.volume_config())?;
    let page = checkout
        .list_directory(
            &path,
            after,
            maximum_entries,
            prior.remaining(budget).map_err(WorkspaceError::from)?,
            cancellation,
        )
        .await
        .map_err(|failure| WorkspaceError::from(failure.error))?;
    let work = prior.checked_add(page.work).map_err(WorkspaceError::from)?;
    Ok(crate::OperationReceipt {
        value: WorkspaceDirectoryPage {
            entries: page
                .value
                .entries
                .into_iter()
                .map(|entry| WorkspaceDirectoryEntry {
                    name: entry.name,
                    file_id: entry.file_id,
                    kind: entry.kind,
                })
                .collect(),
            has_more: page.value.has_more,
        },
        work,
    })
}

async fn read_generation_symbolic_link<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
) -> Result<Bytes, WorkspaceError> {
    let mut checkout = workspace
        .engine_checkout(selector, CheckoutMode::read_only_pinned())
        .await?;
    let path = customer_path(path, checkout.volume_config())?;
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
    let path = customer_path(path, checkout.volume_config())?;
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
    /// Reuses the workspace transaction compiler against a private checkout
    /// candidate without publishing a second workspace head.
    #[cfg(feature = "native-mount")]
    pub(crate) fn for_checkout_candidate(
        workspace: &Workspace<A, O>,
        checkout: Checkout<A, O>,
    ) -> Self {
        Self {
            workspace: workspace.clone(),
            checkout,
            idempotency_key: IdempotencyKey::new(),
            requires_rebase: false,
        }
    }

    #[cfg(feature = "native-mount")]
    pub(crate) fn into_checkout_candidate(self) -> Checkout<A, O> {
        self.checkout
    }

    pub(crate) fn workspace_id(&self) -> WorkspaceId {
        self.workspace.id()
    }

    pub(crate) async fn restore_record_measured(
        &mut self,
        path: &str,
        record: crate::kernel::FileRecord,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::WorkCounters, WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        self.apply_engine_measured(
            vec![crate::kernel::Mutation::Restore { path, record }],
            budget,
            cancellation,
        )
        .await
    }

    pub(crate) async fn create_dir_all_measured(
        &mut self,
        path: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::WorkCounters, WorkspaceError> {
        let portable = PortablePath::parse(path, self.checkout.volume_config().limits)
            .map_err(WorkspaceError::path)?;
        let mut current = String::new();
        let mut work = crate::WorkCounters::default();
        for component in portable.components() {
            cancellation.check().map_err(WorkspaceError::from)?;
            current.push('/');
            current.push_str(component);
            let path = customer_path(&current, self.checkout.volume_config())?;
            let lookup = self
                .checkout
                .lookup_no_follow(
                    &path,
                    work.remaining(budget).map_err(WorkspaceError::from)?,
                    cancellation,
                )
                .await
                .map_err(|failure| WorkspaceError::from(failure.error))?;
            work = work
                .checked_add(lookup.work)
                .map_err(WorkspaceError::from)?;
            match lookup.value.record {
                Some(value) if value.kind == FileKind::Directory => {}
                Some(_) => return Err(WorkspaceError::NotDirectory),
                None => {
                    let applied = self
                        .apply_measured(
                            vec![AuthoredMutation::CreateDirectory {
                                path,
                                metadata: FileMetadata::default(),
                            }],
                            work.remaining(budget).map_err(WorkspaceError::from)?,
                            cancellation,
                        )
                        .await?;
                    work = work.checked_add(applied).map_err(WorkspaceError::from)?;
                }
            }
        }
        Ok(work)
    }

    pub(crate) async fn apply_authored_measured(
        &mut self,
        operation: AuthoredMutation,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::WorkCounters, WorkspaceError> {
        self.apply_measured(vec![operation], budget, cancellation)
            .await
    }

    /// Admits a bounded ordered capture as one unpublished candidate update.
    pub(crate) async fn apply_authored_bulk_measured(
        &mut self,
        operations: Vec<AuthoredMutation>,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::WorkCounters, WorkspaceError> {
        self.checkout
            .apply_authored_bulk_transaction(operations, budget, cancellation)
            .await
            .map(|receipt| receipt.work)
            .map_err(|failure| WorkspaceError::from(failure.error))
    }

    pub(crate) async fn create_directory_measured(
        &mut self,
        path: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::WorkCounters, WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        self.apply_authored_measured(
            AuthoredMutation::CreateDirectory {
                path,
                metadata: FileMetadata::default(),
            },
            budget,
            cancellation,
        )
        .await
    }

    pub(crate) async fn create_file_measured(
        &mut self,
        path: &str,
        bytes: Bytes,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::WorkCounters, WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        self.apply_authored_measured(
            AuthoredMutation::CreateFile {
                path,
                bytes,
                metadata: FileMetadata::default(),
            },
            budget,
            cancellation,
        )
        .await
    }

    pub(crate) async fn write_range_measured(
        &mut self,
        path: &str,
        offset: u64,
        bytes: Bytes,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::WorkCounters, WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        self.apply_authored_measured(
            AuthoredMutation::Write {
                path,
                offset,
                bytes,
            },
            budget,
            cancellation,
        )
        .await
    }

    pub(crate) async fn create_symbolic_link_measured(
        &mut self,
        path: &str,
        target: Bytes,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::WorkCounters, WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        self.apply_authored_measured(
            AuthoredMutation::CreateSymbolicLink {
                path,
                target,
                metadata: FileMetadata::default(),
            },
            budget,
            cancellation,
        )
        .await
    }

    pub(crate) async fn create_special_measured(
        &mut self,
        path: &str,
        kind: FileKind,
        device: Option<(u32, u32)>,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::WorkCounters, WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        let operation = match (kind, device) {
            (FileKind::Fifo | FileKind::Socket, None) => AuthoredMutation::CreateEmptySpecial {
                path,
                kind,
                metadata: FileMetadata::default(),
            },
            (FileKind::CharacterDevice | FileKind::BlockDevice, Some((major, minor))) => {
                AuthoredMutation::CreateDevice {
                    path,
                    kind,
                    major,
                    minor,
                    metadata: FileMetadata::default(),
                }
            }
            _ => return Err(WorkspaceError::IncompatibleWorkspace),
        };
        self.apply_authored_measured(operation, budget, cancellation)
            .await
    }

    pub(crate) async fn set_metadata_measured(
        &mut self,
        path: &str,
        metadata: FileMetadata,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::WorkCounters, WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        self.apply_authored_measured(
            AuthoredMutation::SetMetadata { path, metadata },
            budget,
            cancellation,
        )
        .await
    }

    pub(crate) async fn preserve_file_identity_measured(
        &mut self,
        path: &str,
        file_id: FileId,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::WorkCounters, WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        self.apply_authored_measured(
            AuthoredMutation::Reidentify { path, file_id },
            budget,
            cancellation,
        )
        .await
    }

    pub(crate) async fn hard_link_measured(
        &mut self,
        source: &str,
        destination: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::WorkCounters, WorkspaceError> {
        let config = self.checkout.volume_config();
        self.apply_authored_measured(
            AuthoredMutation::HardLink {
                source: customer_path(source, config)?,
                destination: customer_path(destination, config)?,
            },
            budget,
            cancellation,
        )
        .await
    }

    async fn apply_engine_measured(
        &mut self,
        operations: Vec<crate::kernel::Mutation>,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::WorkCounters, WorkspaceError> {
        self.checkout
            .mutate(operations, budget, cancellation)
            .await
            .map(|receipt| receipt.work)
            .map_err(|failure| WorkspaceError::from(failure.error))
    }

    /// Preserves a stable source identity for an existing non-directory object.
    ///
    /// Reusing an identity creates a hard-link alias only when kind, metadata,
    /// and payload match exactly; collisions fail closed.
    pub async fn preserve_file_identity(
        &mut self,
        path: &str,
        file_id: FileId,
    ) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        self.apply(vec![AuthoredMutation::Reidentify { path, file_id }])
            .await
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
        let path = customer_path(path, self.checkout.volume_config())?;
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
        self.stage_content_measured(
            source,
            maximum_source_bytes,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
    }

    pub(crate) async fn stage_content_measured<R: crate::kernel::AsyncBlobSource>(
        &self,
        source: &mut R,
        maximum_source_bytes: u64,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<crate::StagedContent>, WorkspaceError> {
        self.checkout
            .stage_content(source, maximum_source_bytes, budget, cancellation)
            .await
            .map(|receipt| crate::OperationReceipt {
                value: receipt.value,
                work: receipt.work,
            })
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
            let path = customer_path(&current, self.checkout.volume_config())?;
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
        let path = customer_path(path, self.checkout.volume_config())?;
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
        let path = customer_path(path, self.checkout.volume_config())?;
        self.apply(vec![AuthoredMutation::CreateSymbolicLink {
            path,
            target,
            metadata: FileMetadata::default(),
        }])
        .await
    }

    /// Creates one payload-free POSIX special node or exact device node.
    pub async fn create_special(
        &mut self,
        path: &str,
        kind: FileKind,
        device: Option<(u32, u32)>,
    ) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        let mutation = match (kind, device) {
            (FileKind::Fifo | FileKind::Socket, None) => AuthoredMutation::CreateEmptySpecial {
                path,
                kind,
                metadata: FileMetadata::default(),
            },
            (FileKind::CharacterDevice | FileKind::BlockDevice, Some((major, minor))) => {
                AuthoredMutation::CreateDevice {
                    path,
                    kind,
                    major,
                    minor,
                    metadata: FileMetadata::default(),
                }
            }
            _ => return Err(WorkspaceError::IncompatibleWorkspace),
        };
        self.apply(vec![mutation]).await
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
        let path = customer_path(path, self.checkout.volume_config())?;
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
        let path = customer_path(path, self.checkout.volume_config())?;
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
        let path = customer_path(path, self.checkout.volume_config())?;
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

    /// Removes one binding only when its authenticated identity still matches.
    ///
    /// The precondition is compiled into the same atomic checkout mutation as
    /// the removal; callers never perform a check-then-delete sequence.
    pub async fn remove_if(
        &mut self,
        path: &str,
        expected_file_id: crate::FileId,
    ) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        self.apply(vec![AuthoredMutation::Remove {
            path,
            expected_file_id: Some(expected_file_id),
        }])
        .await
    }

    /// Replaces one destination with a complete copy-on-write clone.
    ///
    /// # Errors
    ///
    /// Rejects invalid paths, non-regular endpoints, and bounded engine errors.
    pub async fn copy(&mut self, source: &str, destination: &str) -> Result<(), WorkspaceError> {
        let config = self.checkout.volume_config();
        let source = customer_path(source, config)?;
        let destination = customer_path(destination, config)?;
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
        let config = self.checkout.volume_config();
        self.apply(vec![AuthoredMutation::Rename {
            source: customer_path(source, config)?,
            destination: customer_path(destination, config)?,
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
        let config = self.checkout.volume_config();
        self.apply(vec![AuthoredMutation::HardLink {
            source: customer_path(source, config)?,
            destination: customer_path(destination, config)?,
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
        let path = customer_path(path, self.checkout.volume_config())?;
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
        let path = customer_path(path, self.checkout.volume_config())?;
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
        let path = customer_path(path, self.checkout.volume_config())?;
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
        let path = customer_path(path, self.checkout.volume_config())?;
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
        let config = self.checkout.volume_config();
        self.apply(vec![AuthoredMutation::CloneRange(
            crate::FileCloneRequest {
                source: customer_path(source, config)?,
                source_offset,
                destination: customer_path(destination, config)?,
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
        let path = customer_path(path, self.checkout.volume_config())?;
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
        self.commit_with_permit(crate::PublicationPermit::Unrestricted)
            .await
    }

    /// Publishes this transaction only while an SDK operation-window permit
    /// is still active at the authority linearization point.
    pub async fn commit_with_permit(
        &mut self,
        permit: crate::PublicationPermit,
    ) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        in_heap(move || async move {
            self.commit_with_permit_measured(
                permit,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
        })
        .await
    }

    /// Publishes this transaction under an exact work budget and cancellation token.
    pub(crate) async fn commit_with_permit_measured(
        &mut self,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError> {
        cancellation.check().map_err(WorkspaceError::from)?;
        if self.requires_rebase {
            let receipt = self
                .checkout
                .retry_stale_commit(
                    self.idempotency_key.operation_id(),
                    permit,
                    budget,
                    cancellation,
                )
                .await
                .map_err(|failure| WorkspaceError::from(failure.error))?;
            let work = receipt.work;
            let Some(outcome) = receipt.value else {
                let actual = self
                    .workspace
                    .head_measured(
                        work.remaining(budget).map_err(WorkspaceError::from)?,
                        cancellation,
                    )
                    .await?;
                return Ok(crate::OperationReceipt {
                    value: TransactionCommit::Conflict {
                        actual: actual.value,
                    },
                    work: work
                        .checked_add(actual.work)
                        .map_err(WorkspaceError::from)?,
                });
            };
            return self
                .commit_outcome_measured(outcome, work, budget, cancellation)
                .await;
        }
        let receipt = self
            .checkout
            .commit_with_permit(
                self.idempotency_key.operation_id(),
                permit,
                budget,
                cancellation,
            )
            .await
            .map_err(|failure| WorkspaceError::from(failure.error))?;
        self.commit_outcome_measured(receipt.value, receipt.work, budget, cancellation)
            .await
    }

    async fn commit_outcome_measured(
        &self,
        outcome: CheckoutCommitOutcome,
        work: crate::WorkCounters,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError> {
        in_heap(move || async move {
            let value = match outcome {
                CheckoutCommitOutcome::Committed { generation_id, .. } => {
                    TransactionCommit::Committed(Generation {
                        workspace: self.workspace.clone(),
                        id: generation_id,
                    })
                }
                CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => {
                    TransactionCommit::AlreadyCommitted(Generation {
                        workspace: self.workspace.clone(),
                        id: generation_id,
                    })
                }
                CheckoutCommitOutcome::Conflict { .. } => {
                    let actual = self
                        .workspace
                        .head_measured(
                            work.remaining(budget).map_err(WorkspaceError::from)?,
                            cancellation,
                        )
                        .await?;
                    return Ok(crate::OperationReceipt {
                        value: TransactionCommit::Conflict {
                            actual: actual.value,
                        },
                        work: work
                            .checked_add(actual.work)
                            .map_err(WorkspaceError::from)?,
                    });
                }
                CheckoutCommitOutcome::Fenced { .. } => TransactionCommit::Fenced,
                CheckoutCommitOutcome::IdempotencyConflict { .. } => {
                    TransactionCommit::IdempotencyConflict
                }
            };
            Ok(crate::OperationReceipt { value, work })
        })
        .await
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
        self.apply_measured(
            operations,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|_| ())
    }

    async fn apply_measured(
        &mut self,
        operations: Vec<AuthoredMutation>,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::WorkCounters, WorkspaceError> {
        self.checkout
            .apply_authored_transaction(operations, budget, cancellation)
            .await
            .map(|receipt| receipt.work)
            .map_err(|failure| WorkspaceError::from(failure.error))
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
    pub(crate) fn from_engine(metadata: FileMetadata) -> Self {
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
        self.changed_paths_bounded(
            maximum_entries,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
    }

    /// Resolves changed paths with one cumulative work budget and explicit
    /// cancellation token. Partial traversal is never reported as exact.
    #[allow(clippy::too_many_lines)]
    pub async fn changed_paths_bounded(
        &self,
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Vec<ChangedPath>>, WorkspaceError> {
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
        self.work.verify(budget).map_err(WorkspaceError::engine)?;
        let before_receipt = self
            .from
            .namespace_records_for_file_ids_bounded(
                file_ids.iter().copied(),
                maximum_entries,
                self.work
                    .remaining(budget)
                    .map_err(WorkspaceError::engine)?,
                cancellation,
            )
            .await?;
        let mut work = self
            .work
            .checked_add(before_receipt.work)
            .map_err(WorkspaceError::engine)?;
        let after_receipt = self
            .to
            .namespace_records_for_file_ids_bounded(
                file_ids.iter().copied(),
                maximum_entries,
                work.remaining(budget).map_err(WorkspaceError::engine)?,
                cancellation,
            )
            .await?;
        work = work
            .checked_add(after_receipt.work)
            .map_err(WorkspaceError::engine)?;
        let before = before_receipt.value;
        let after = after_receipt.value;
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
        let paths = paths
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
            .collect();
        Ok(crate::OperationReceipt { value: paths, work })
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
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

    /// Plans against caller-pinned immutable source and target generations.
    ///
    /// The target must still be current when planning begins. The source may
    /// have advanced: publication deliberately retains the generation pinned
    /// when the parent began inspecting the child.
    pub async fn plan_pinned(
        self,
        source_head: Generation<A, O>,
        target_head: Generation<A, O>,
    ) -> Result<JoinPlan<A, O>, WorkspaceError> {
        self.validate_bounds()?;
        if source_head.workspace.id != self.source.id || target_head.workspace.id != self.target.id
        {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let (current_target, target_authority_head) = self
            .target
            .volume
            .fs
            .workspace_head_state(&self.target.volume)
            .await?;
        if current_target != target_head.id {
            return Err(WorkspaceError::StaleGeneration);
        }
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

    /// Captures exact provider-owned inputs for later verification of this
    /// durable application, even after the mutable target head advances.
    pub fn commit_witness(
        &self,
        application: &JoinApplication<A, O>,
        idempotency_key: IdempotencyKey,
    ) -> Result<crate::JoinCommitWitness, WorkspaceError> {
        if application.generation.workspace.id() != self.target.id()
            || application.generation.id() == self.target_head.id()
        {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        Ok(crate::JoinCommitWitness {
            source_workspace: self.source.id().volume_id(),
            source_generation: self.source_head.id,
            target_workspace: self.target.id().volume_id(),
            expected_target: self.target_head.id,
            base_workspace: self.base.volume_id,
            base_generation: self.base.id,
            history: self.history,
            maximum_generations: self.maximum_generations,
            maximum_changes: self.maximum_changes,
            maximum_conflicts: self.maximum_conflicts,
            resolutions_digest: application.resolutions_digest,
            expected_head: self.target_authority_head,
            operation_id: idempotency_key.operation_id(),
            result_generation: application.generation.id,
        })
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
    Applied(JoinApplication<A, O>),
    /// The same exact application was already durable.
    AlreadyApplied(JoinApplication<A, O>),
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

/// One durable join result together with its exact conflict-resolution input.
/// The digest is generated inside Filesystem's publication path, so receipt
/// producers cannot accidentally attest a different set of resolutions.
pub struct JoinApplication<A, O> {
    generation: Generation<A, O>,
    resolutions_digest: crate::Digest,
}

impl<A, O> JoinApplication<A, O> {
    /// Immutable generation made durable by this join.
    #[must_use]
    pub const fn generation(&self) -> &Generation<A, O> {
        &self.generation
    }

    /// Consumes the proof wrapper when only the resulting generation is needed.
    #[must_use]
    pub fn into_generation(self) -> Generation<A, O> {
        self.generation
    }

    /// Domain-separated digest of the exact applied conflict resolutions.
    #[must_use]
    pub const fn resolutions_digest(&self) -> crate::Digest {
        self.resolutions_digest
    }
}

impl<A, O> std::ops::Deref for JoinApplication<A, O> {
    type Target = Generation<A, O>;

    fn deref(&self) -> &Self::Target {
        &self.generation
    }
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
        self.apply_with_permit(options, crate::PublicationPermit::Unrestricted)
            .await
    }

    /// Applies this immutable plan only while the supplied writer permit remains valid.
    pub async fn apply_with_permit(
        &self,
        options: ApplyOptions,
        permit: crate::PublicationPermit,
    ) -> Result<JoinOutcome<A, O>, WorkspaceError> {
        self.apply_resolutions(options, BTreeMap::new(), permit)
            .await
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
        self.apply_with_drivers_and_permit(
            options,
            registry,
            cache,
            replanning,
            crate::PublicationPermit::Unrestricted,
        )
        .await
    }

    /// Runs merge drivers and publishes only while the supplied writer permit remains valid.
    pub async fn apply_with_drivers_and_permit<C: MergeResolutionCache>(
        &self,
        options: ApplyOptions,
        registry: &MergeDriverRegistry,
        cache: &mut C,
        replanning: bool,
        permit: crate::PublicationPermit,
    ) -> Result<JoinOutcome<A, O>, DrivenJoinError> {
        let initial = self.apply_with_permit(options, permit).await?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = initial
        else {
            return Ok(initial);
        };
        let plan = self.typed_merge_plan(conflicts, truncated).await?;
        let candidate = resolve_merge_plan(plan, registry, cache, replanning)?;
        self.apply_candidate_with_permit(options, &candidate, permit)
            .await
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
        self.apply_resolutions(options, resolutions, crate::PublicationPermit::Unrestricted)
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
        self.apply_candidate_with_permit(options, candidate, crate::PublicationPermit::Unrestricted)
            .await
    }

    /// Validates and publishes a declarative candidate under one writer permit.
    pub async fn apply_candidate_with_permit(
        &self,
        options: ApplyOptions,
        candidate: &UnpublishedMergeCandidate,
        permit: crate::PublicationPermit,
    ) -> Result<JoinOutcome<A, O>, DrivenJoinError> {
        let conflict_keys = candidate
            .plan
            .conflicts
            .iter()
            .map(|conflict| &conflict.key)
            .collect::<BTreeSet<_>>();
        if candidate.plan.base != self.base.id
            || candidate.plan.ours != self.target_head.id
            || candidate.plan.theirs != self.source_head.id
            || candidate.plan.truncated
            || candidate.plan.conflicts.len() != candidate.resolutions.len()
            || conflict_keys.len() != candidate.plan.conflicts.len()
            || !candidate.resolutions.keys().eq(conflict_keys.into_iter())
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
        self.apply_resolutions(options, resolutions, permit)
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
        permit: crate::PublicationPermit,
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
                permit,
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
            crate::facade::WorkspaceJoinOutcome::Joined(id, resolutions_digest) => {
                JoinOutcome::Applied(JoinApplication {
                    generation: generation(id),
                    resolutions_digest,
                })
            }
            crate::facade::WorkspaceJoinOutcome::AlreadyJoined(id, resolutions_digest) => {
                JoinOutcome::AlreadyApplied(JoinApplication {
                    generation: generation(id),
                    resolutions_digest,
                })
            }
            crate::facade::WorkspaceJoinOutcome::Applied(_)
            | crate::facade::WorkspaceJoinOutcome::AlreadyApplied(_) => {
                return Err(WorkspaceError::IncompatibleWorkspace);
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

fn logical_name_text(name: &LogicalName) -> Result<std::borrow::Cow<'_, str>, WorkspaceError> {
    name.unicode_text().ok_or_else(|| {
        WorkspaceError::path("non-Unicode path cannot be projected as a portable string")
    })
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

    /// Returns the workspace capability that owns this immutable generation.
    ///
    /// The returned handle is cheap and retains the same authenticated storage
    /// deployment. It is useful for composing generation-oriented algorithms
    /// without an adapter-owned workspace registry.
    #[must_use]
    pub fn workspace(&self) -> Workspace<A, O> {
        self.workspace.clone()
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Computes the exact normalized second-parent identity that a merge of
    /// this source generation into `target` must retain. This is read-only.
    pub async fn normalized_join_parent_for(
        &self,
        target: &Generation<A, O>,
    ) -> Result<GenerationId, WorkspaceError> {
        self.workspace
            .volume
            .fs
            .workspace_normalized_join_parent(self, target)
            .await
    }

    /// Opens one cheap immutable reader pinned to this exact generation.
    ///
    /// The generation root is authenticated once while the reader is opened;
    /// descendant objects remain lazy and are fetched only by the operation
    /// that needs them. Clone the returned reader to overlap independent
    /// requests without reopening the generation.
    ///
    /// # Errors
    ///
    /// Rejects unavailable, foreign, or corrupt generation state and bounded
    /// backend failures.
    pub async fn reader(&self) -> Result<crate::PinnedReader<A, O>, WorkspaceError> {
        self.workspace
            .engine_checkout(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
            )
            .await?
            .pinned_reader()
            .map_err(WorkspaceError::engine)
    }

    /// Computes one semantic delta to a compatible generation in any workspace
    /// from the same filesystem deployment.
    pub async fn diff_to(
        &self,
        to: &Generation<A, O>,
        maximum_changes: u32,
    ) -> Result<ChangeSet<A, O>, WorkspaceError> {
        self.diff_to_bounded(
            to,
            maximum_changes,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
    }

    /// Computes one semantic delta with explicit cumulative work and
    /// cancellation bounds.
    pub async fn diff_to_bounded(
        &self,
        to: &Generation<A, O>,
        maximum_changes: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<ChangeSet<A, O>, WorkspaceError> {
        let receipt = self
            .workspace
            .volume
            .fs
            .workspace_join_changes_bounded(self.id, to, maximum_changes, budget, cancellation)
            .await?;
        Ok(ChangeSet {
            from: self.clone(),
            to: to.clone(),
            changes: receipt.value,
            work: receipt.work,
        })
    }

    /// Materializes this immutable generation into an existing empty host
    /// directory using the SDK's native capability-rooted adapter. Unix ctime
    /// and Linux birth time are host-generated view-local facts; their exact
    /// canonical values remain in this generation, not in the host inode.
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub async fn materialize(
        &self,
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError> {
        self.materialize_with_mode(
            options,
            budget,
            cancellation,
            crate::native_mount::MaterializeMode::DurableOutput,
        )
        .await
    }

    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub(crate) async fn materialize_with_mode(
        &self,
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        mode: crate::native_mount::MaterializeMode,
    ) -> Result<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError> {
        let checkout = self
            .workspace
            .engine_checkout_measured(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
                budget,
                cancellation,
            )
            .await?;
        let mut value = checkout.value;
        let receipt = crate::native_mount::materialize_checkout_with_mode(
            &mut value,
            options,
            checkout
                .work
                .remaining(budget)
                .map_err(WorkspaceError::from)?,
            cancellation,
            mode,
        )
        .await
        .map_err(|failure| WorkspaceError::engine(failure.error))?;
        merge_workspace_work(checkout.work, receipt, budget)
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
        self.materialize_path_with_mode(
            path,
            options,
            budget,
            cancellation,
            crate::native_mount::MaterializeMode::DurableOutput,
        )
        .await
    }

    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub(crate) async fn materialize_path_with_mode(
        &self,
        path: &str,
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        mode: crate::native_mount::MaterializeMode,
    ) -> Result<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError> {
        let checkout = self
            .workspace
            .engine_checkout_measured(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
                budget,
                cancellation,
            )
            .await?;
        let mut value = checkout.value;
        let path = customer_path(path, value.volume_config())?;
        let receipt = crate::native_mount::materialize_checkout_paths_with_mode(
            &mut value,
            &[path],
            options,
            checkout
                .work
                .remaining(budget)
                .map_err(WorkspaceError::from)?,
            cancellation,
            mode,
        )
        .await
        .map_err(|failure| WorkspaceError::engine(failure.error))?;
        merge_workspace_work(checkout.work, receipt, budget)
    }

    /// Materializes multiple paths with one pinned checkout and shared
    /// file-identity table, preserving hard links across selected paths.
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub async fn materialize_paths(
        &self,
        paths: &[String],
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError> {
        let checkout = self
            .workspace
            .engine_checkout_measured(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
                budget,
                cancellation,
            )
            .await?;
        let mut value = checkout.value;
        let paths = paths
            .iter()
            .map(|path| customer_path(path, value.volume_config()))
            .collect::<Result<Vec<_>, _>>()?;
        let receipt = crate::materialize_checkout_paths(
            &mut value,
            &paths,
            options,
            checkout
                .work
                .remaining(budget)
                .map_err(WorkspaceError::from)?,
            cancellation,
        )
        .await
        .map_err(|failure| WorkspaceError::engine(failure.error))?;
        merge_workspace_work(checkout.work, receipt, budget)
    }

    /// Restores one path from this immutable generation into a host tree.
    ///
    /// Only the selected path is read and touched. Publication is same-volume
    /// and atomic for an ordinary host tree, or mount-visible for a live mount.
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub async fn restore_host_path(
        &self,
        relative: &std::path::Path,
        replacement: crate::HostPathReplacement,
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<crate::HostPathRestore>, WorkspaceError> {
        let checkout = self
            .workspace
            .engine_checkout_measured(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
                budget,
                cancellation,
            )
            .await?;
        let mut value = checkout.value;
        let receipt = crate::restore_checkout_host_path(
            &mut value,
            relative,
            replacement,
            options,
            checkout
                .work
                .remaining(budget)
                .map_err(WorkspaceError::from)?,
            cancellation,
        )
        .await
        .map_err(|failure| WorkspaceError::engine(failure.error))?;
        merge_workspace_work(checkout.work, receipt, budget)
    }

    /// Restores a bounded sequence of paths through one pinned checkout.
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub async fn restore_host_paths(
        &self,
        paths: &[std::path::PathBuf],
        replacement: crate::HostPathReplacement,
        options: &crate::MaterializeOptions,
        cancellation: &crate::CancellationToken,
    ) -> Result<Vec<crate::HostPathRestore>, WorkspaceError> {
        let mut checkout = self
            .workspace
            .engine_checkout_measured(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
                crate::WorkBudget::UNBOUNDED,
                cancellation,
            )
            .await?
            .value;
        let mut restored = Vec::new();
        restored
            .try_reserve_exact(paths.len())
            .map_err(|_| WorkspaceError::engine("restore path result allocation failed"))?;
        for path in paths {
            restored.push(
                crate::restore_checkout_host_path(
                    &mut checkout,
                    path,
                    replacement,
                    options,
                    crate::WorkBudget::UNBOUNDED,
                    cancellation,
                )
                .await
                .map_err(|failure| WorkspaceError::engine(failure.error))?
                .value,
            );
        }
        Ok(restored)
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
        in_heap(move || async move {
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
        })
        .await
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

    pub(crate) async fn paths_for_file_id_measured(
        &self,
        file_id: FileId,
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<Vec<String>>, WorkspaceError> {
        let receipt = self
            .namespace_records_for_file_ids_bounded(
                [file_id],
                maximum_entries,
                budget,
                cancellation,
            )
            .await?;
        let paths = receipt
            .value
            .records
            .get(&file_id)
            .into_iter()
            .flatten()
            .map(|(path, _)| namespace_path_text(path))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(crate::OperationReceipt {
            value: paths,
            work: receipt.work,
        })
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
        self.namespace_records_for_file_ids_bounded(
            file_ids,
            maximum_entries,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
    }

    #[allow(clippy::too_many_lines)]
    async fn namespace_records_for_file_ids_bounded(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::OperationReceipt<NamespaceRecordSearch>, WorkspaceError> {
        let file_ids: BTreeSet<_> = file_ids.into_iter().collect();
        if file_ids.is_empty() {
            return Ok(crate::OperationReceipt {
                value: NamespaceRecordSearch {
                    records: BTreeMap::new(),
                    complete: true,
                },
                work: crate::WorkCounters::default(),
            });
        }
        if maximum_entries == 0 {
            return Ok(crate::OperationReceipt {
                value: NamespaceRecordSearch {
                    records: BTreeMap::new(),
                    complete: false,
                },
                work: crate::WorkCounters::default(),
            });
        }
        let maximum = usize::try_from(maximum_entries).unwrap_or(usize::MAX);
        let limits = self.workspace.volume.config.limits;
        let cache_key = crate::path_index::cache_key(self.id, file_ids.iter().copied());
        if let Some(bytes) = self.workspace.volume.fs.path_index().load(cache_key)
            && let Some(cached) = crate::path_index::GenerationPathQuery::decode(
                &bytes,
                self.id,
                file_ids.iter().copied(),
            )
        {
            let cached_paths = cached
                .into_iter()
                .flat_map(|(file_id, paths)| paths.into_iter().map(move |path| (file_id, path)))
                .map(|(file_id, path)| path.into_namespace(limits).map(|path| (file_id, path)))
                .collect::<Option<Vec<_>>>();
            if let Some(cached_paths) = cached_paths {
                if cached_paths.is_empty() {
                    return Ok(crate::OperationReceipt {
                        value: NamespaceRecordSearch {
                            records: BTreeMap::new(),
                            complete: true,
                        },
                        work: crate::WorkCounters::default(),
                    });
                }
                // A complete cached answer larger than the caller's bound
                // cannot be truncated without changing which namespace paths
                // a cold bounded traversal would have encountered. Fall back
                // to that traversal instead of returning a false empty result.
                if cached_paths.len() <= maximum {
                    let paths = cached_paths
                        .iter()
                        .map(|(_, path)| path.clone())
                        .collect::<Vec<_>>();
                    let lookup = self
                        .lookup_paths(&paths, budget, cancellation)
                        .await
                        .map_err(WorkspaceError::engine)?;
                    let mut records = BTreeMap::<_, Vec<_>>::new();
                    let mut valid = true;
                    for ((expected, path), record) in cached_paths.into_iter().zip(lookup.value) {
                        let Some(record) = record else {
                            valid = false;
                            break;
                        };
                        if record.file_id != expected {
                            valid = false;
                            break;
                        }
                        records.entry(expected).or_default().push((path, record));
                    }
                    if valid {
                        return Ok(crate::OperationReceipt {
                            value: NamespaceRecordSearch {
                                records,
                                complete: true,
                            },
                            work: lookup.work,
                        });
                    }
                }
            }
        }
        let mut matches: BTreeMap<_, Vec<(NamespacePath, crate::kernel::FileRecord)>> =
            BTreeMap::new();
        let checkout = self
            .workspace
            .engine_checkout_measured(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
                budget,
                cancellation,
            )
            .await?;
        let mut work = checkout.work;
        let mut checkout = checkout.value;
        let root = NamespacePath::new(Vec::new(), limits).map_err(WorkspaceError::path)?;
        let root_lookup = checkout
            .lookup_no_follow(
                &root,
                work.remaining(budget).map_err(WorkspaceError::engine)?,
                cancellation,
            )
            .await
            .map_err(WorkspaceError::engine)?;
        work = work
            .checked_add(root_lookup.work)
            .map_err(WorkspaceError::engine)?;
        if let Some(record) = root_lookup.value.record
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
                let page_receipt = checkout
                    .list_directory_records(
                        &directory,
                        after.as_ref(),
                        1_024,
                        work.remaining(budget).map_err(WorkspaceError::engine)?,
                        cancellation,
                    )
                    .await
                    .map_err(WorkspaceError::engine)?;
                work = work
                    .checked_add(page_receipt.work)
                    .map_err(WorkspaceError::engine)?;
                let page = page_receipt.value;
                for entry in &page.entries {
                    examined = examined.saturating_add(1);
                    if examined > maximum {
                        for records in matches.values_mut() {
                            records.sort_by(|left, right| left.0.cmp(&right.0));
                        }
                        return Ok(crate::OperationReceipt {
                            value: NamespaceRecordSearch {
                                records: matches,
                                complete: false,
                            },
                            work,
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
        let cached_paths = matches
            .iter()
            .map(|(&file_id, records)| {
                (
                    file_id,
                    records
                        .iter()
                        .map(|(path, _)| crate::path_index::CachedPath::from_namespace(path))
                        .collect(),
                )
            })
            .collect();
        let cached = crate::path_index::GenerationPathQuery::new(
            self.id,
            file_ids.iter().copied(),
            &cached_paths,
        );
        if let Some(bytes) = cached.encode() {
            self.workspace
                .volume
                .fs
                .path_index()
                .store(cache_key, &bytes);
        }
        Ok(crate::OperationReceipt {
            value: NamespaceRecordSearch {
                records: matches,
                complete: true,
            },
            work,
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
    /// Caller cancellation stopped a measured workspace operation.
    #[error(transparent)]
    Cancelled(#[from] crate::CancellationError),
    /// A measured workspace operation exhausted its admitted work budget.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// A fork generation belongs to another filesystem deployment.
    #[error("fork generation belongs to another filesystem deployment")]
    ForeignGeneration,
    /// A caller-pinned target generation is no longer current.
    #[error("workspace generation is stale")]
    StaleGeneration,
    /// A conditional mutation targeted a different filesystem identity.
    #[error("workspace file identity is stale")]
    StaleIdentity,
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
        match error {
            FsError::Cancelled(error) => Self::Cancelled(error),
            FsError::Work(error) => Self::Work(error),
            FsError::NotFound => Self::NotFound,
            FsError::NotDirectory => Self::NotDirectory,
            FsError::FileRead(crate::kernel::FileRangeReadError::NotRegular) => {
                Self::NotRegularFile
            }
            FsError::Mutation(crate::kernel::GenerationMutationError::FileIdentityConflict) => {
                Self::StaleIdentity
            }
            error => Self::Engine(error.to_string()),
        }
    }
}

pub(crate) fn customer_path(
    path: &str,
    config: crate::model::VolumeConfig,
) -> Result<NamespacePath, WorkspaceError> {
    let portable = PortablePath::parse(path, config.limits).map_err(WorkspaceError::path)?;
    NamespacePath::from_portable_in_profile(&portable, config.profile, config.limits)
        .map_err(WorkspaceError::path)
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
fn merge_workspace_work<T>(
    prior: crate::WorkCounters,
    receipt: crate::OperationReceipt<T>,
    budget: crate::WorkBudget,
) -> Result<crate::OperationReceipt<T>, WorkspaceError> {
    let work = prior
        .checked_add(receipt.work)
        .map_err(WorkspaceError::from)?;
    work.verify(budget).map_err(WorkspaceError::from)?;
    Ok(crate::OperationReceipt {
        value: receipt.value,
        work,
    })
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
