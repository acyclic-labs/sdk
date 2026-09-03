//! Customer workspace identities and immutable generation handles.
//!
//! A workspace is the sole customer-visible unit of mutation, publication,
//! retention, and convergence. The engine's volume and authority identities
//! remain implementation details.

use crate::foundation::{GenerationId, OperationId, VolumeId};
use crate::kernel::{
    ExtentKind, FileKind, FileMetadata, FilePayload, LogicalName, MetadataField, NamespacePath,
};
use crate::model::{CheckoutMode, GenerationSelector};
use crate::path::PortablePath;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, Checkout, CheckoutCommitOutcome,
    FsError, GenerationDiff, MergeConflict, Volume,
};
use bytes::Bytes;
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
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkspaceId([u8; 16]);

impl WorkspaceId {
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
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
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
        self.volume
            .fs
            .fork_workspace(destination, &options.generation, options.idempotency_key)
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
        Ok(Transaction {
            checkout: self
                .engine_checkout(
                    GenerationSelector::Head,
                    CheckoutMode::tracking_transaction(),
                )
                .await?,
            workspace: self.clone(),
            idempotency_key,
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
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
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
        let outcome = self
            .checkout
            .commit(
                self.idempotency_key.operation_id(),
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map_err(WorkspaceError::engine)?
            .value;
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
                actual: self.workspace.head().await?,
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

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ChangeSet<A, O> {
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
        if self.maximum_generations == 0 || self.maximum_changes == 0 || self.maximum_conflicts == 0
        {
            return Err(WorkspaceError::JoinLimit);
        }
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
    base: GenerationId,
    history: JoinHistory,
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
        self.base
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

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> JoinPlan<A, O> {
    /// Applies this immutable plan through one target-head CAS.
    ///
    /// # Errors
    ///
    /// Returns authenticated storage, compatibility, bound, or publication
    /// failures. Semantic conflicts and races are typed outcomes.
    pub async fn apply(&self, options: ApplyOptions) -> Result<JoinOutcome<A, O>, WorkspaceError> {
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
                maximum_changes: self.maximum_changes,
                maximum_conflicts: self.maximum_conflicts,
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
    /// Change sets do not share one exact contiguous endpoint and deployment.
    #[error("change sets are not contiguous")]
    ChangeSetContinuity,
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
