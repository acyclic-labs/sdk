//! Customer workspace identities and immutable generation handles.
//!
//! A workspace is the sole customer-visible unit of mutation, publication,
//! retention, and convergence. The engine's volume and authority identities
//! remain implementation details.

use crate::CancellationToken;
use crate::foundation::{FileId, GenerationId, OperationId, VolumeId};
use crate::heap_future::in_heap;
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::kernel::binary_search_with_accounting;
use crate::kernel::{
    ExtentKind, FileKind, FileMetadata, FilePayload, LogicalName, MetadataField, NamespacePath,
};
use crate::model::{CheckoutMode, GenerationSelector};
use crate::path::PortablePath;
use crate::performance::WorkBudget;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, AuthoredMutation, Checkout, ConflictKey, ConflictKind,
    ConflictSide, ConflictValue, FsError, GenerationDiff, LivePublicationObservation,
    MergeConflict, MergeDriverRegistry, MergePlan, MergePlanResolutionError, MergeResolution,
    MergeResolutionCache, UnpublishedMergeCandidate, Volume, resolve_merge_plan,
};
use acyclic_stream::ProviderTask;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use thiserror::Error;
use unicode_normalization::UnicodeNormalization;

const WORKSPACE_ID_DOMAIN: &[u8] = b"acyclic-fs-workspace-id-v1\0";
const MAXIMUM_WORKSPACE_NAME_BYTES: usize = 255;

/// Canonical immutable customer workspace name.
#[derive(Clone)]
pub struct WorkspaceName(Arc<(String, KernelOwnedAllocation)>);

impl WorkspaceName {
    /// Canonicalizes a name under the supplied work limits.
    pub fn new<W: crate::facade::FsWork>(
        name: impl AsRef<str>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Self, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = Self::with_accounting(name.as_ref(), &mut accounting);
        work.finish(result, &accounting)
    }

    pub(crate) fn with_accounting(
        name: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, WorkspaceError> {
        accounting.check()?;
        let (mut pending, mut maximum_pending, mut maximum_chunk) = (0_u64, 0_u64, 0_u64);
        for character in name.chars() {
            accounting.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            let mut chunk = 0_u64;
            let mut failure = None;
            unicode_normalization::char::decompose_canonical(character, |scalar| {
                if failure.is_some() {
                    return;
                }
                failure = (|| -> Result<(), WorkspaceError> {
                    accounting.charge(crate::WorkCounters {
                        items_examined: 1,
                        ..crate::WorkCounters::UNCHARGED
                    })?;
                    chunk = chunk.checked_add(1).ok_or(crate::WorkError::Overflow)?;
                    if unicode_normalization::char::canonical_combining_class(scalar) == 0 {
                        pending = 0;
                    } else {
                        pending = pending.checked_add(1).ok_or(crate::WorkError::Overflow)?;
                        maximum_pending = maximum_pending.max(pending);
                    }
                    Ok(())
                })()
                .err();
            });
            if let Some(error) = failure {
                return Err(error);
            }
            maximum_chunk = maximum_chunk.max(chunk);
        }
        // NFC's two TinyVec buffers and combining-mark sort scratch are admitted before iteration.
        let buffer = |count: u64, width: usize| -> Result<u64, WorkspaceError> {
            if count <= 4 {
                return Ok(0);
            }
            count
                .checked_next_power_of_two()
                .and_then(|n| n.checked_mul(2))
                .and_then(|n| n.checked_mul(width as u64))
                .ok_or(crate::WorkError::Overflow.into())
        };
        let scratch = buffer(
            maximum_pending
                .checked_add(maximum_chunk)
                .ok_or(crate::WorkError::Overflow)?,
            std::mem::size_of::<(u8, char)>(),
        )?
        .checked_add(buffer(maximum_pending, std::mem::size_of::<char>())?)
        .and_then(|n| {
            maximum_pending
                .checked_mul(std::mem::size_of::<(u8, char)>() as u64)
                .and_then(|sort| n.checked_add(sort))
        })
        .ok_or(crate::WorkError::Overflow)?;
        let live = match accounting {
            KernelAccounting::Standalone { allocations, .. } => allocations.live_bytes(),
            KernelAccounting::BorrowedStandalone { allocations, .. } => allocations.live_bytes(),
            #[cfg(not(target_arch = "wasm32"))]
            KernelAccounting::NativeOriginal { work, .. } => work.live_allocation_bytes(),
        };
        accounting.admit(crate::WorkCounters {
            peak_allocation_bytes: live
                .checked_add(scratch)
                .ok_or(crate::WorkError::Overflow)?,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let mut output = [0_u8; MAXIMUM_WORKSPACE_NAME_BYTES];
        let mut length = 0;
        let mut normalized = name.nfc();
        loop {
            accounting.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            let Some(character) = normalized.next() else {
                break;
            };
            let end = length + character.len_utf8();
            if end > output.len() {
                return Err(WorkspaceNameError::TooLong.into());
            }
            let copied = crate::WorkCounters {
                bytes_copied: character.len_utf8() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            character.encode_utf8(&mut output[length..end]);
            length = end;
            accounting.record_completed(copied)?;
        }
        drop(normalized);
        accounting.charge(crate::WorkCounters {
            items_examined: length as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let canonical = std::str::from_utf8(&output[..length]).map_err(WorkspaceError::engine)?;
        if canonical.is_empty() {
            return Err(WorkspaceNameError::Empty.into());
        }
        if canonical == "." || canonical == ".." {
            return Err(WorkspaceNameError::Reserved.into());
        }
        for character in canonical.chars() {
            accounting.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            if character.is_control() || matches!(character, '/' | '\\') {
                return Err(WorkspaceNameError::InvalidCharacter.into());
            }
        }
        let copied = crate::WorkCounters {
            bytes_copied: length as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        let mut owned = crate::kernel::Owned::<Vec<u8>>::allocate(length, accounting)?;
        owned.value.extend_from_slice(canonical.as_bytes());
        accounting.record_completed(copied)?;
        let name = String::from_utf8(owned.value).map_err(WorkspaceError::engine)?;
        let control = crate::kernel::allocation::bytes_owner_control_bytes::<(
            String,
            KernelOwnedAllocation,
        )>()?;
        owned
            .allocation
            .absorb(&mut accounting.claim_owned(control, 1)?)?;
        Ok(Self(Arc::new((name, owned.allocation))))
    }

    /// Returns the canonical customer spelling.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0.0
    }

    pub(crate) fn retained_bytes(&self) -> u64 {
        self.as_str().len() as u64
    }
}

impl PartialEq for WorkspaceName {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl Eq for WorkspaceName {}
impl PartialOrd for WorkspaceName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for WorkspaceName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}
impl std::hash::Hash for WorkspaceName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.as_str(), state);
    }
}
impl fmt::Debug for WorkspaceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("WorkspaceName")
            .field(&self.as_str())
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

    /// Derives the stable identity for a canonical name in one deployment namespace. The namespace is deliberately internal to the selected `Fs` deployment.
    #[must_use]
    pub(crate) fn derive<W: crate::facade::FsWork>(
        namespace: [u8; 16],
        name: &WorkspaceName,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Self, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| -> Result<Self, WorkspaceError> {
            accounting.check()?;
            let performed = crate::WorkCounters {
                bytes_hashed: (WORKSPACE_ID_DOMAIN.len() + 16 + 8 + name.as_str().len()) as u64,
                bytes_copied: 16,
                ..crate::WorkCounters::UNCHARGED
            };
            accounting.admit(performed)?;
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
            accounting.record_completed(performed)?;
            Ok(Self(bytes))
        })();
        work.finish(result, &accounting)
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
    /// Authenticated immutable volume configuration.
    #[must_use]
    pub const fn volume_config(&self) -> crate::model::VolumeConfig {
        self.volume.config
    }

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
    /// Fences this workspace's mutable head for an exact publication operation.
    pub async fn reserve_publication<W: crate::facade::FsWork>(
        &self,
        operation_id: OperationId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::PublicationReservation, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            accounting.check()?;
            let authority = self.volume.fs.authority();
            let authority_id = self.authority_id();
            let mut authority_work =
                crate::AuthorityWork::new(accounting.remaining()?, cancellation);
            #[cfg(not(target_arch = "wasm32"))]
            {
                authority_work.original = accounting.native_work();
            }
            let reserved = async {
                let head = authority.head(authority_id, &mut authority_work).await?;
                authority_work.remaining()?;
                authority
                    .reserve_publication(authority_id, head, operation_id, &mut authority_work)
                    .await
            }
            .await
            .map_err(|error| WorkspaceError::from(FsError::Authority(error)));
            let reserved = match accounting.standalone_parts() {
                Some((_, spent, _, _)) => {
                    merge_workspace_backend(authority_work.receipt(reserved), spent, 0)?.value
                }
                None => reserved?,
            };
            let reservation = match reserved {
                crate::ReservationOutcome::Reserved(proof)
                | crate::ReservationOutcome::AlreadyReserved(proof) => proof,
                crate::ReservationOutcome::Conflict { .. } => {
                    #[cfg(not(target_arch = "wasm32"))]
                    if accounting.native_work().is_some() {
                        return Err(FsError::Authority(
                            crate::AuthorityStoreError::RejectedStatic(
                                "publication reservation head or gate changed",
                            ),
                        )
                        .into());
                    }
                    return Err(WorkspaceError::StaleGeneration);
                }
            };
            if reservation.authority_id != authority_id || reservation.operation_id != operation_id
            {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            Ok(reservation)
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Releases only this workspace's authenticated reservation.
    pub async fn release_publication<W: crate::facade::FsWork>(
        &self,
        reservation: crate::PublicationReservation,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if reservation.authority_id != self.authority_id() {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            accounting.check()?;
            let mut authority_work =
                crate::AuthorityWork::new(accounting.remaining()?, cancellation);
            #[cfg(not(target_arch = "wasm32"))]
            {
                authority_work.original = accounting.native_work();
            }
            let released = self
                .volume
                .fs
                .authority()
                .release_publication(reservation, &mut authority_work)
                .await
                .map_err(|error| WorkspaceError::from(FsError::Authority(error)));
            match accounting.standalone_parts() {
                Some((_, spent, _, _)) => {
                    merge_workspace_backend(authority_work.receipt(released), spent, 0)
                        .map(|receipt| receipt.value)
                }
                None => released,
            }
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Queries the durable reservation for this exact operation.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn publication_reservation(
        &self,
        operation_id: OperationId,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<Option<crate::PublicationReservation>, WorkspaceError> {
        original
            .admit(crate::WorkCounters::UNCHARGED)
            .map_err(WorkspaceError::from)?;
        cancellation.check()?;
        let mut authority_work = crate::AuthorityWork::native(original, cancellation);
        let reservation = self
            .volume
            .fs
            .authority()
            .publication_reservation(self.authority_id(), operation_id, &mut authority_work)
            .await
            .map_err(|error| WorkspaceError::from(FsError::Authority(error)))?;
        if let Some(proof) = reservation
            && (proof.authority_id != self.authority_id() || proof.operation_id != operation_id)
        {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        Ok(reservation)
    }

    /// Resolves an immutable operation without adopting the mutable head.
    pub async fn operation_generation<W: crate::facade::FsWork>(
        &self,
        key: IdempotencyKey,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<Generation<A, O>>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .volume
            .fs
            .workspace_operation_generation(
                &self.volume,
                key.operation_id(),
                &mut accounting,
                cancellation,
            )
            .await
            .map(|mut observed| {
                let id = observed.0.take().map(|(id, record)| {
                    drop(record);
                    id
                });
                drop(observed);
                id.map(|id| Generation {
                    workspace: self.clone(),
                    id,
                    publication_closure: None,
                })
            });
        work.finish(result, &accounting)
    }

    pub(crate) async fn operation_generation_record<W: crate::facade::FsWork>(
        &self,
        key: IdempotencyKey,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<
        (
            Option<(Generation<A, O>, crate::DurableCommit)>,
            KernelOwnedAllocation,
        ),
        WorkspaceError,
    > {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .volume
            .fs
            .workspace_operation_generation(
                &self.volume,
                key.operation_id(),
                &mut accounting,
                cancellation,
            )
            .await
            .map(|(value, allocation)| {
                (
                    value.map(|(id, record)| {
                        (
                            Generation {
                                workspace: self.clone(),
                                id,
                                publication_closure: None,
                            },
                            record,
                        )
                    }),
                    allocation,
                )
            });
        work.finish(result, &accounting)
    }

    /// Verifies the exact immutable result of this workspace's join operation.
    pub async fn verify_join_commit<W: crate::facade::FsWork>(
        &self,
        witness: &crate::JoinCommitWitness,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<bool, WorkspaceError> {
        self.volume
            .fs
            .verify_workspace_join_commit(&self.volume, witness, work, cancellation)
            .await
    }

    /// Opens an authored private or immutable checkout.
    pub async fn checkout<W: crate::facade::FsWork>(
        &self,
        selector: GenerationSelector,
        mode: CheckoutMode,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Checkout<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .checkout_accounted(selector, mode, &mut accounting, cancellation)
            .await;
        work.finish(result, &accounting)
    }

    pub(crate) async fn checkout_accounted(
        &self,
        selector: GenerationSelector,
        mode: CheckoutMode,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Checkout<A, O>, WorkspaceError> {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            return self
                .volume
                .checkout(selector, mode, original, cancellation)
                .await
                .map_err(WorkspaceError::from);
        }
        let opened = self
            .volume
            .checkout(selector, mode, accounting.remaining()?, cancellation)
            .await;
        let (allocations, work, budget, _) = accounting
            .standalone_parts()
            .ok_or(crate::WorkError::Overflow)?;
        let checkout = merge_workspace_backend(opened, work, allocations.live_bytes())?.value;
        work.verify(budget)?;
        Ok(checkout)
    }

    pub(crate) async fn open_checkout(
        &self,
        selector: GenerationSelector,
        mode: CheckoutMode,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<Checkout<A, O>>, WorkspaceError> {
        let checkout = self
            .checkout_accounted(selector, mode, accounting, cancellation)
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        if accounting.native_work().is_some() {
            return Ok(crate::kernel::Owned::new(
                checkout,
                accounting.claim_owned(0, 0)?,
                0,
            ));
        }
        let bytes = checkout.logical_owned_bytes();
        Ok(crate::kernel::Owned::new(
            checkout,
            accounting.claim_owned(bytes, 0)?,
            bytes,
        ))
    }

    pub(crate) async fn open_read_path(
        &self,
        selector: GenerationSelector,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<
        (
            crate::kernel::Owned<Checkout<A, O>>,
            crate::kernel::Owned<NamespacePath>,
        ),
        WorkspaceError,
    > {
        let checkout = self
            .open_checkout(
                selector,
                CheckoutMode::read_only_pinned(),
                accounting,
                cancellation,
            )
            .await?;
        let (path, allocation) = customer_path_with_accounting(
            path,
            checkout.value.volume_config(),
            accounting,
            cancellation,
        )?;
        let bytes = path.retained_bytes();
        Ok((checkout, crate::kernel::Owned::new(path, allocation, bytes)))
    }

    /// Pins the authenticated head under supplied work limits.
    pub async fn head<W: crate::facade::FsWork>(
        &self,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Generation<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .checkout_accounted(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                &mut accounting,
                cancellation,
            )
            .await
            .map(|checkout| Generation {
                workspace: self.clone(),
                id: checkout.generation_id(),
                publication_closure: None,
            });
        work.finish(result, &accounting)
    }

    /// Authenticates an exact immutable generation.
    pub async fn generation<W: crate::facade::FsWork>(
        &self,
        id: GenerationId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Generation<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .checkout_accounted(
                GenerationSelector::Exact(id),
                CheckoutMode::read_only_pinned(),
                &mut accounting,
                cancellation,
            )
            .await
            .map(|checkout| Generation {
                workspace: self.clone(),
                id: checkout.generation_id(),
                publication_closure: None,
            });
        work.finish(result, &accounting)
    }

    /// Opens a private transaction against the current head.
    pub async fn begin_transaction<W: crate::facade::FsWork>(
        &self,
        key: IdempotencyKey,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Transaction<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .checkout_accounted(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                &mut accounting,
                cancellation,
            )
            .await
            .map(|checkout| Transaction::from_checkout(self, checkout, key, false));
        work.finish(result, &accounting)
    }

    /// Opens an exact transaction and observes whether its head has advanced.
    pub async fn begin_transaction_at<W: crate::facade::FsWork>(
        &self,
        generation: &Generation<A, O>,
        key: IdempotencyKey,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Transaction<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            accounting.check()?;
            if generation.workspace.id != self.id {
                return Err(WorkspaceError::ForeignGeneration);
            }
            let head = self
                .checkout_accounted(
                    GenerationSelector::Head,
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    cancellation,
                )
                .await?
                .generation_id();
            let checkout = self
                .checkout_accounted(
                    GenerationSelector::Exact(generation.id),
                    CheckoutMode::tracking_transaction(),
                    &mut accounting,
                    cancellation,
                )
                .await?;
            Ok(Transaction::from_checkout(
                self,
                checkout,
                key,
                head != generation.id,
            ))
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Opens an exact transaction without consulting the mutable head.
    pub async fn begin_pinned_transaction<W: crate::facade::FsWork>(
        &self,
        generation: &Generation<A, O>,
        key: IdempotencyKey,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Transaction<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            accounting.check()?;
            if generation.workspace.id != self.id {
                return Err(WorkspaceError::ForeignGeneration);
            }
            let checkout = self
                .checkout_accounted(
                    GenerationSelector::Exact(generation.id),
                    CheckoutMode::tracking_transaction(),
                    &mut accounting,
                    cancellation,
                )
                .await?;
            Ok(Transaction::from_checkout(self, checkout, key, false))
        }
        .await;
        work.finish(result, &accounting)
    }

    pub(crate) async fn head_with_file_record(
        &self,
        file_id: FileId,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<(Generation<A, O>, bool)>, WorkspaceError>
    {
        let checkout = self
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                budget,
                cancellation,
            )
            .await?;
        let reader = checkout.value.pinned_reader().map_err(|error| {
            crate::OperationFailure::new(WorkspaceError::engine(error), checkout.work)
        })?;
        let prior = checkout.work;
        drop(checkout.value);
        let remaining = prior
            .remaining(budget)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
        let records = reader
            .file_records_by_id(&[file_id], remaining, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(prior, WorkspaceError::from))?;
        let work = prior
            .checked_add(records.work)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
        Ok(crate::OperationReceipt {
            value: (
                Generation {
                    workspace: self.clone(),
                    id: reader.generation_id(),
                    publication_closure: None,
                },
                records.value.first().is_some_and(Option::is_some),
            ),
            work,
        })
    }

    /// Retains the current generation under a permanent label.
    pub async fn checkpoint(
        &self,
        label: impl AsRef<str>,
    ) -> Result<(WorkspaceName, Generation<A, O>), WorkspaceError> {
        let cancellation = CancellationToken::new();
        let label = WorkspaceName::new(label, WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|failure| failure.error)?
            .value;
        let generation = self
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value;
        let text = label.as_str().to_owned();
        let bytes = text.capacity() as u64;
        self.volume
            .fs
            .retain_workspace_generation(
                &self.volume,
                generation.id,
                crate::kernel::RetentionKind::Checkpoint,
                crate::kernel::Owned::new(text, KernelOwnedAllocation::Standalone, bytes),
                None,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| failure.error)?;
        Ok((label, generation))
    }

    /// Durably pins the current immutable generation.
    /// Performs the operation under the supplied limits.
    pub async fn pin(
        &self,
        identity: impl AsRef<str>,
    ) -> Result<(WorkspaceName, Generation<A, O>), WorkspaceError> {
        let cancellation = CancellationToken::new();
        self.head(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)?
            .value
            .pin(identity, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(|failure| failure.error)
            .map(|receipt| receipt.value)
    }

    /// Permanently deletes this named head using the exact retry identity.
    pub async fn delete(&self, key: IdempotencyKey) -> Result<WorkspaceDelete, WorkspaceError> {
        self.volume
            .fs
            .delete_workspace_volume(&self.volume, key.operation_id())
            .await
    }

    /// Returns this workspace's foreign source when supported.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[must_use]
    pub fn source(&self) -> Option<&crate::Source<A, O>> {
        self.source.as_ref()
    }

    /// Captures all remaining foreign source state.
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    pub async fn seal(&self) -> Result<Generation<A, O>, crate::SourceError> {
        match &self.source {
            Some(source) => source.seal().await,
            None => self
                .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| crate::SourceError::Engine(failure.error.to_string())),
        }
    }

    /// Plans a join into the target workspace.
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

    /// Rebases this fork onto its current source.
    pub async fn live_rebase(
        &self,
        idempotency_key: IdempotencyKey,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
    ) -> Result<WorkspaceRebase<A, O>, WorkspaceError> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.workspace.live_rebase",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            workspace_id = crate::obs::hex(&self.id.0),
        );
        crate::obs::outcome_on(
            &span,
            crate::obs::in_span(&span, async move {
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
                    publication_closure: None,
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
            })
            .await,
        )
    }

    async fn same_path_state(
        &self,
        left: Option<crate::kernel::FileRecord>,
        right: Option<crate::kernel::FileRecord>,
    ) -> Result<bool, WorkspaceError> {
        match (left, right) {
            (Some(left), Some(right)) if left != right => {
                self.volume
                    .fs
                    .same_file_state(&self.volume, left, right)
                    .await
            }
            (left, right) => Ok(left == right),
        }
    }

    /// Applies an exact three-way path delta under one writer permit.
    pub async fn apply_paths_from(
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
        let current = self
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?
            .value;
        if current.id != if_current {
            return Ok(WorkspacePathApply::Stale(current));
        }
        let mut base_checkout = match base {
            Some(base) => Some(
                base.workspace
                    .checkout(
                        GenerationSelector::Exact(base.id),
                        CheckoutMode::read_only_pinned(),
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| failure.error)?
                    .value,
            ),
            None => None,
        };
        let mut source_checkout = match source {
            Some(source) => Some(
                source
                    .workspace
                    .checkout(
                        GenerationSelector::Exact(source.id),
                        CheckoutMode::read_only_pinned(),
                        WorkBudget::UNBOUNDED,
                        &CancellationToken::new(),
                    )
                    .await
                    .map_err(|failure| failure.error)?
                    .value,
            ),
            None => None,
        };
        let mut transaction = self
            .begin_transaction(
                idempotency_key,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
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
                .ok_or_else(|| WorkspaceError::Engine("missing batch lookup result".to_owned()))?;
            if self.same_path_state(current_record, source_record).await? {
                continue;
            }
            if !self.same_path_state(current_record, base_record).await? {
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
                        transaction
                            .create_dir_all(
                                &format!("/{parent}"),
                                WorkBudget::UNBOUNDED,
                                &cancellation,
                            )
                            .await
                            .map_err(|failure| failure.error)?;
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
        // Parents are restored before their children, and children removed
        // before their parents.
        operations.sort_by_key(|operation| match operation {
            crate::kernel::Mutation::Remove { path, .. } => {
                (1, std::cmp::Reverse(path.components().len()))
            }
            _ => (0, std::cmp::Reverse(0)),
        });
        transaction
            .checkout
            .mutate(operations, crate::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(WorkspaceError::engine)?;
        Ok(
            match transaction
                .commit(
                    None,
                    permit,
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                    |_| async {
                        Ok::<_, crate::OperationFailure<WorkspaceError>>(crate::OperationReceipt {
                            value: (),
                            work: crate::WorkCounters::UNCHARGED,
                        })
                    },
                )
                .await
                .map_err(|failure| failure.error)?
                .value
            {
                TransactionCommit::Committed(generation) => WorkspacePathApply::Applied(generation),
                TransactionCommit::AlreadyCommitted(generation) => {
                    WorkspacePathApply::AlreadyApplied(generation)
                }
                TransactionCommit::Conflict { actual } => WorkspacePathApply::Stale(actual),
                TransactionCommit::Fenced => WorkspacePathApply::Fenced,
                TransactionCommit::IdempotencyConflict => WorkspacePathApply::IdempotencyConflict,
            },
        )
    }

    /// Restores an exact generation under the supplied publication permit.
    pub async fn restore_generation(
        &self,
        generation: &Generation<A, O>,
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
    ) -> Result<WorkspaceRestore<Generation<A, O>>, WorkspaceError> {
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
            publication_closure: None,
        };
        Ok(match outcome {
            WorkspaceRestore::Restored(id) => WorkspaceRestore::Restored(restored(id)),
            WorkspaceRestore::AlreadyRestored(id) => {
                WorkspaceRestore::AlreadyRestored(restored(id))
            }
            WorkspaceRestore::Current(id) => WorkspaceRestore::Current(restored(id)),
            WorkspaceRestore::Stale(id) => WorkspaceRestore::Stale(restored(id)),
            WorkspaceRestore::Fenced => WorkspaceRestore::Fenced,
            WorkspaceRestore::IdempotencyConflict => WorkspaceRestore::IdempotencyConflict,
        })
    }

    /// Restores the caller-expanded frozen selection at the original native seal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn restore_paths_from_with_native_work<'original, E, F, Fut>(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
        before_publication: &mut F,
    ) -> Result<TransactionCommit<A, O>, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        if !self.volume.fs.same_deployment(&source.workspace.volume.fs) {
            return Err(E::from(WorkspaceError::ForeignGeneration));
        }
        let current = self.head(original, cancellation).await.map_err(E::from)?;
        if current.id != if_current {
            return Ok(TransactionCommit::Conflict { actual: current });
        }
        let mut transaction = self
            .begin_pinned_transaction(&current, idempotency_key, original, cancellation)
            .await
            .map_err(E::from)?;
        let mut accounting = KernelAccounting::native(original, cancellation);
        transaction
            .checkout
            .validate_native_work(original)
            .map_err(FsError::from)?;
        let source_root = source
            .workspace
            .open_checkout(
                GenerationSelector::Exact(source.id),
                CheckoutMode::read_only_pinned(),
                &mut accounting,
                cancellation,
            )
            .await
            .map_err(E::from)?;
        transaction
            .restore_paths_from_root_with_accounting(
                paths,
                source_root,
                &mut accounting,
                cancellation,
            )
            .await
            .map_err(E::from)?;
        transaction
            .commit_with_native_work(None, permit, original, cancellation, before_publication)
            .await
    }

    /// Restores exact records and prepares selected directories before one publication seal.
    pub async fn restore_paths_from<F, Fut>(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        mut before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError>
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        crate::heap_future::in_heap_measured(
            move |budget| async move {
                let mut accounting = KernelAccounting::standalone(budget, cancellation);
                let result = async {
                    if !self.volume.fs.same_deployment(&source.workspace.volume.fs) {
                        return Err(WorkspaceError::ForeignGeneration);
                    }
                    let id = self
                        .checkout_accounted(
                            GenerationSelector::Head,
                            CheckoutMode::read_only_pinned(),
                            &mut accounting,
                            cancellation,
                        )
                        .await?
                        .generation_id();
                    if id != if_current {
                        return Ok(TransactionCommit::Conflict {
                            actual: Generation {
                                workspace: self.clone(),
                                id,
                                publication_closure: None,
                            },
                        });
                    }
                    let source_root = source
                        .workspace
                        .open_checkout(
                            GenerationSelector::Exact(source.id),
                            CheckoutMode::read_only_pinned(),
                            &mut accounting,
                            cancellation,
                        )
                        .await?;
                    let opened = self
                        .open_checkout(
                            GenerationSelector::Exact(id),
                            CheckoutMode::tracking_transaction(),
                            &mut accounting,
                            cancellation,
                        )
                        .await?;
                    let mut transaction =
                        Transaction::from_checkout(self, opened.value, idempotency_key, false);
                    transaction
                        .restore_paths_from_root_with_accounting(
                            paths,
                            source_root,
                            &mut accounting,
                            cancellation,
                        )
                        .await?;
                    let committed = transaction
                        .commit(
                            None,
                            permit,
                            accounting.remaining()?,
                            cancellation,
                            |budget| {
                                let future = before_publication(budget);
                                async move {
                                    future
                                        .await
                                        .map_err(|failure| failure.map_error(WorkspaceError::from))
                                }
                            },
                        )
                        .await;
                    let (allocations, spent, _, _) = accounting
                        .standalone_parts()
                        .ok_or(crate::WorkError::Overflow)?;
                    let live = allocations.live_bytes();
                    merge_workspace_backend(committed, spent, live).map(|receipt| receipt.value)
                }
                .await;
                accounting.check().map_err(|error| {
                    crate::OperationFailure::new(WorkspaceError::from(error), accounting.spent())
                })?;
                crate::facade::measured(result, &accounting)
            },
            budget,
            cancellation,
            WorkspaceError::Cancelled(crate::CancellationError),
            None,
        )
        .await
    }

    /// Forks an exact generation under supplied limits and a publication seal.
    pub async fn fork<F, Fut>(
        &self,
        destination: impl AsRef<str>,
        options: ForkOptions<A, O>,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        mut before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Self>, WorkspaceError>
    where
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        crate::heap_future::in_heap_measured(
            |budget| async move {
                let mut accounting = KernelAccounting::standalone(budget, cancellation);
                let result = async {
                    let bytes = options
                        .paths
                        .as_ref()
                        .map_or(Ok(0), |paths| workspace_owned_strings_bytes(paths))?;
                    let (mut allocation, admitted) = accounting.record_actual_owned(bytes)?;
                    if options.generation.workspace.id != self.id {
                        return Err(WorkspaceError::ForeignGeneration);
                    }
                    accounting.check()?;
                    admitted?;
                    let name =
                        WorkspaceName::with_accounting(destination.as_ref(), &mut accounting)?;
                    accounting.release_owned(&mut allocation, bytes)?;
                    // The backend takes over the name buffer; its Arc control stays live here.
                    accounting.release_owned(
                        &mut KernelOwnedAllocation::Standalone,
                        name.retained_bytes(),
                    )?;
                    let mut guard = &mut before_publication;
                    let forked = self
                        .volume
                        .fs
                        .fork_workspace::<_, _, WorkspaceError>(
                            name,
                            &options.generation,
                            options.idempotency_key,
                            options.paths,
                            permit,
                            accounting.remaining()?,
                            cancellation,
                            Some(&mut guard),
                        )
                        .await;
                    let (allocations, spent, budget, _) = accounting
                        .standalone_parts()
                        .ok_or(crate::WorkError::Overflow)?;
                    let value =
                        merge_workspace_backend(forked, spent, allocations.live_bytes())?.value;
                    spent.verify(budget)?;
                    Ok(value)
                }
                .await;
                crate::facade::measured(result, &accounting)
            },
            budget,
            cancellation,
            WorkspaceError::Cancelled(crate::CancellationError),
            None,
        )
        .await
    }

    /// Forks on the original native invocation with its publication seal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn fork_with_native_work<'original, E, F, Fut>(
        &self,
        destination: &str,
        options: ForkOptions<A, O>,
        permit: crate::PublicationPermit,
        original: &'original crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> Result<Self, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let mut accounting = KernelAccounting::native(original, cancellation);
        let bytes = options
            .paths
            .as_ref()
            .map_or(Ok(0), |paths| workspace_owned_strings_bytes(paths))
            .map_err(E::from)?;
        let (allocation, admitted) = accounting.record_actual_owned(bytes)?;
        let mut options = crate::kernel::Owned::new(options, allocation, bytes);
        admitted?;
        if options.value.generation.workspace.id != self.id {
            return Err(WorkspaceError::ForeignGeneration.into());
        }
        accounting.check()?;
        let name = WorkspaceName::new(destination, original, cancellation).map_err(E::from)?;
        accounting.release_owned(&mut options.allocation, bytes)?;
        let ForkOptions {
            generation,
            idempotency_key,
            paths,
        } = options.value;
        let mut guard = crate::NativeAuthorityGuard(before_publication.clone());
        self.volume
            .fs
            .fork_workspace(
                name,
                &generation,
                idempotency_key,
                paths,
                permit,
                original,
                cancellation,
                Some(&mut guard),
            )
            .await
    }

    /// Computes a bounded semantic delta.

    /// Performs the operation under the supplied limits.
    pub async fn diff(
        &self,
        from: &Generation<A, O>,
        to: &Generation<A, O>,
        maximum_changes: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<ChangeSet<A, O>>, WorkspaceError> {
        if from.workspace.id != self.id
            || to.workspace.id != self.id
            || !self.volume.fs.same_deployment(&from.workspace.volume.fs)
            || !self.volume.fs.same_deployment(&to.workspace.volume.fs)
        {
            return Err(crate::OperationFailure::before_work(
                WorkspaceError::ForeignGeneration,
            ));
        }
        let receipt = self
            .volume
            .diff_generations(from.id, to.id, maximum_changes, budget, cancellation)
            .await
            .map_err(|failure| failure.map_error(WorkspaceError::engine))?;
        if receipt.value.truncated {
            return Err(crate::OperationFailure::new(
                WorkspaceError::JoinLimit,
                receipt.work,
            ));
        }
        Ok(crate::OperationReceipt {
            value: ChangeSet {
                from: from.clone(),
                to: to.clone(),
                changes: receipt.value,
                work: receipt.work,
            },
            work: receipt.work,
        })
    }
}

async fn read_selected<A: AsyncAuthorityStore, O: AsyncObjectStore, W: crate::facade::FsWork>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    maximum_bytes: u64,
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<acyclic_stream::OwnedBytes, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let (mut checkout, path) = workspace
            .open_read_path(selector, path, &mut accounting, cancellation)
            .await?;
        let (lookup, allocation) = checkout
            .value
            .lookup_no_follow_with_accounting(&path.value, &mut accounting, cancellation, true)
            .await?;
        drop(allocation);
        let record = lookup.record.ok_or(WorkspaceError::NotFound)?;
        if record.kind != FileKind::Regular {
            return Err(WorkspaceError::NotRegularFile);
        }
        let length = regular_file_bytes(record)?;
        if length > maximum_bytes {
            return Err(WorkspaceError::ReadLimitExceeded);
        }
        checkout
            .value
            .read_file_range_with_accounting(
                &path.value,
                crate::ByteRange { offset: 0, length },
                &mut accounting,
                cancellation,
            )
            .await
            .map(|read| read.bytes)
            .map_err(WorkspaceError::from)
    }
    .await;
    work.finish(result, &accounting)
}

async fn read_range<A: AsyncAuthorityStore, O: AsyncObjectStore, W: crate::facade::FsWork>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    offset: u64,
    length: u64,
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let (mut checkout, path) = workspace
            .open_read_path(selector, path, &mut accounting, cancellation)
            .await?;
        let read = checkout
            .value
            .read_file_range_with_accounting(
                &path.value,
                crate::ByteRange { offset, length },
                &mut accounting,
                cancellation,
            )
            .await?;
        let bytes = read.logical_owned_bytes();
        Ok::<_, WorkspaceError>(crate::kernel::OwnedSourceChunk::claimed(read.bytes, bytes))
    }
    .await;
    work.finish(result, &accounting)
}

async fn read_link<A: AsyncAuthorityStore, O: AsyncObjectStore, W: crate::facade::FsWork>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let (mut checkout, path) = workspace
            .open_read_path(selector, path, &mut accounting, cancellation)
            .await?;
        checkout
            .value
            .read_symbolic_link_with_accounting(&path.value, &mut accounting, cancellation)
            .await
            .map_err(WorkspaceError::from)
    }
    .await;
    work.finish(result, &accounting)
}

async fn lookup_stat<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<Option<WorkspaceStat>, WorkspaceError> {
    let (mut checkout, path) = workspace
        .open_read_path(selector, path, accounting, cancellation)
        .await?;
    checkout
        .value
        .lookup_no_follow_with_metadata_with_accounting(&path.value, accounting, cancellation)
        .await?
        .map(|lookup| {
            let logical_bytes = match lookup.record.payload {
                FilePayload::InlineRegular(bytes) => Some(
                    u64::try_from(bytes.as_bytes().len())
                        .map_err(|_| crate::WorkError::Overflow)?,
                ),
                FilePayload::Regular { logical_bytes, .. } => Some(logical_bytes),
                FilePayload::SymbolicLink { target_bytes, .. } => Some(target_bytes),
                FilePayload::ReparsePoint { payload_bytes, .. } => Some(payload_bytes),
                FilePayload::Directory { .. } | FilePayload::Device { .. } | FilePayload::Empty => {
                    None
                }
            };
            Ok(WorkspaceStat {
                file_id: lookup.record.file_id,
                kind: lookup.record.kind,
                link_count: lookup.record.link_count,
                logical_bytes,
                metadata: WorkspaceMetadata::from_engine(lookup.metadata),
            })
        })
        .transpose()
}

async fn stat<A: AsyncAuthorityStore, O: AsyncObjectStore, W: crate::facade::FsWork>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<WorkspaceStat, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = lookup_stat(workspace, selector, path, &mut accounting, cancellation)
        .await
        .and_then(|value| value.ok_or(WorkspaceError::NotFound));
    work.finish(result, &accounting)
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Reads one complete regular file within the caller's bound.
    pub async fn read<W: crate::facade::FsWork>(
        &self,
        path: &str,
        maximum_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<acyclic_stream::OwnedBytes, WorkspaceError> {
        read_selected(
            self,
            GenerationSelector::Head,
            path,
            maximum_bytes,
            work,
            cancellation,
        )
        .await
    }

    /// Reads one exact file range.
    pub async fn read_range<W: crate::facade::FsWork>(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_range(
            self,
            GenerationSelector::Head,
            path,
            offset,
            length,
            work,
            cancellation,
        )
        .await
    }

    /// Reads complete no-follow metadata for one path.
    pub async fn stat<W: crate::facade::FsWork>(
        &self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<WorkspaceStat, WorkspaceError> {
        stat(self, GenerationSelector::Head, path, work, cancellation).await
    }

    pub(crate) async fn stat_optional(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Option<WorkspaceStat>>, WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = lookup_stat(
            self,
            GenerationSelector::Head,
            path,
            &mut accounting,
            cancellation,
        )
        .await;
        crate::facade::measured(result, &accounting)
    }

    pub(crate) async fn record_by_id(
        &self,
        file_id: FileId,
    ) -> Result<crate::kernel::FileRecord, WorkspaceError> {
        let mut checkout = self
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        checkout
            .read_file_record_by_id(file_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(WorkspaceError::engine)
    }

    /// Reads one opaque symbolic-link target.
    pub async fn read_symbolic_link<W: crate::facade::FsWork>(
        &self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_link(self, GenerationSelector::Head, path, work, cancellation).await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Reads one complete regular file from this exact generation.
    pub async fn read<W: crate::facade::FsWork>(
        &self,
        path: &str,
        maximum_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<acyclic_stream::OwnedBytes, WorkspaceError> {
        read_selected(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            maximum_bytes,
            work,
            cancellation,
        )
        .await
    }

    /// Reads one exact range from this immutable generation.
    pub async fn read_range<W: crate::facade::FsWork>(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_range(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            offset,
            length,
            work,
            cancellation,
        )
        .await
    }

    /// Reads complete no-follow metadata from this immutable generation.
    pub async fn stat<W: crate::facade::FsWork>(
        &self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<WorkspaceStat, WorkspaceError> {
        stat(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            work,
            cancellation,
        )
        .await
    }

    /// Reads one opaque symbolic-link target from this exact generation.
    pub async fn read_symbolic_link<W: crate::facade::FsWork>(
        &self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_link(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            work,
            cancellation,
        )
        .await
    }

    /// Resolves an ordered no-follow path batch, preserving absence and duplicates.
    pub async fn lookup_paths(
        &self,
        paths: &[NamespacePath],
        budget: WorkBudget,
        cancellation: &CancellationToken,
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
            let mut accounting = KernelAccounting::Standalone {
                allocations: crate::kernel::allocation::AllocationLedger::default(),
                work: checkout.work,
                budget,
                cancellation,
            };
            let mut checkout = checkout.value;
            let result = async {
                accounting.claim_owned(checkout.logical_owned_bytes(), 0)?;
                let (lookup, allocation) = checkout
                    .lookup_batch_no_follow_with_accounting(
                        paths,
                        false,
                        &mut accounting,
                        cancellation,
                    )
                    .await?;
                let bytes = lookup.retained_allocation_bytes;
                let lookup = crate::kernel::Owned::new(lookup, allocation, bytes);
                let mut records =
                    crate::kernel::Owned::allocate(lookup.value.entries.len(), &mut accounting)
                        .map_err(|error| match error {
                            FsError::Decode(
                                crate::kernel::CanonicalDecodeError::AllocationFailed,
                            ) => FsError::HandleAllocationFailed,
                            error => error,
                        })?;
                for entry in lookup.value.entries {
                    accounting.charge(crate::WorkCounters {
                        items_examined: 1,
                        ..crate::WorkCounters::UNCHARGED
                    })?;
                    records.value.push(entry.record);
                }
                Ok(records.value)
            }
            .await;
            crate::facade::measured(result, &accounting)
        })
        .await
    }
}

/// One unpublished sparse atomic workspace transaction.
pub struct Transaction<A, O> {
    workspace: Workspace<A, O>,
    pub(crate) checkout: Checkout<A, O>,
    idempotency_key: IdempotencyKey,
    requires_rebase: bool,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    pub(crate) fn from_checkout(
        workspace: &Workspace<A, O>,
        mut checkout: Checkout<A, O>,
        idempotency_key: IdempotencyKey,
        requires_rebase: bool,
    ) -> Self {
        checkout.bind_authored_operation(idempotency_key.operation_id());
        Self {
            workspace: workspace.clone(),
            checkout,
            idempotency_key,
            requires_rebase,
        }
    }

    pub(crate) fn for_checkout_candidate<W: crate::facade::FsWork>(
        workspace: &Workspace<A, O>,
        checkout: Checkout<A, O>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Self, WorkspaceError> {
        let accounting = work.accounting(cancellation);
        let result = (|| -> Result<Self, WorkspaceError> {
            accounting.check()?;
            let idempotency_key = match checkout.bound_authored_operation_id() {
                #[cfg(not(target_arch = "wasm32"))]
                Some(id) if accounting.native_work().is_some() => {
                    IdempotencyKey::from_bytes(id.into_bytes())
                }
                #[cfg(not(target_arch = "wasm32"))]
                None if accounting.native_work().is_some() => {
                    return Err(FsError::MutationNotAllowed.into());
                }
                _ => IdempotencyKey::new(),
            };
            Ok(Self {
                workspace: workspace.clone(),
                checkout,
                idempotency_key,
                requires_rebase: false,
            })
        })();
        work.finish(result, &accounting)
    }

    pub(crate) fn mutation_path(
        &self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::kernel::Owned<NamespacePath>, WorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.checkout.validate_native_work(original)?;
        }
        let (path, allocation) = customer_path_with_accounting(
            path,
            self.checkout.volume_config(),
            accounting,
            cancellation,
        )?;
        let bytes = path.retained_bytes();
        Ok(crate::kernel::Owned::new(path, allocation, bytes))
    }

    pub(crate) fn push_mutation<T>(
        operations: &mut crate::kernel::Owned<Vec<T>>,
        mut operation: crate::kernel::Owned<T>,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), WorkspaceError> {
        let copied = crate::WorkCounters {
            bytes_copied: std::mem::size_of::<T>() as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        operations.allocation.absorb(&mut operation.allocation)?;
        operations.bytes = operations
            .bytes
            .checked_add(operation.bytes)
            .ok_or(crate::WorkError::Overflow)?;
        operations.value.push(operation.value);
        accounting.record_completed(copied)?;
        Ok(())
    }

    pub(crate) async fn apply_authored(
        &mut self,
        operations: crate::kernel::Owned<Vec<AuthoredMutation>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        self.checkout
            .apply_authored_transaction_with_accounting(operations, accounting, cancellation)
            .await?
            .retire(accounting)
            .map_err(WorkspaceError::from)
    }

    pub(crate) async fn mutate<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        build: impl FnOnce(NamespacePath) -> Result<AuthoredMutation, WorkspaceError>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let path = self.mutation_path(path, &mut accounting, cancellation)?;
            let operation = match build(path.value) {
                Ok(value) => crate::kernel::Owned::new(value, path.allocation, path.bytes),
                Err(error) => {
                    let mut allocation = path.allocation;
                    accounting.release_owned(&mut allocation, path.bytes)?;
                    return Err(error);
                }
            };
            let mut operations = crate::kernel::Owned::allocate(1, &mut accounting)?;
            Self::push_mutation(&mut operations, operation, &mut accounting)?;
            self.apply_authored(operations, &mut accounting, cancellation)
                .await
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Creates one directory with canonical metadata.
    pub async fn create_directory<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::CreateDirectory {
                    path,
                    metadata: FileMetadata::default(),
                })
            },
            work,
            cancellation,
        )
        .await
    }

    /// Creates one new file with canonical metadata.
    pub async fn create_file<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        bytes: impl Into<acyclic_stream::OwnedBytes>,
        metadata: FileMetadata,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::CreateFile {
                    path,
                    bytes: bytes.into(),
                    metadata,
                })
            },
            work,
            cancellation,
        )
        .await
    }

    /// Replaces one exact file range.
    pub async fn write_range<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        offset: u64,
        bytes: impl Into<acyclic_stream::OwnedBytes>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::Write {
                    path,
                    offset,
                    bytes: bytes.into(),
                })
            },
            work,
            cancellation,
        )
        .await
    }

    /// Creates a symbolic link with an opaque target.
    pub async fn create_symbolic_link<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        target: impl Into<acyclic_stream::OwnedBytes>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::CreateSymbolicLink {
                    path,
                    target: target.into(),
                    metadata: FileMetadata::default(),
                })
            },
            work,
            cancellation,
        )
        .await
    }

    /// Creates one FIFO, socket, or device node.
    pub async fn create_special<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        kind: FileKind,
        device: Option<(u32, u32)>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| match (kind, device) {
                (FileKind::Fifo | FileKind::Socket, None) => {
                    Ok(AuthoredMutation::CreateEmptySpecial {
                        path,
                        kind,
                        metadata: FileMetadata::default(),
                    })
                }
                (FileKind::CharacterDevice | FileKind::BlockDevice, Some((major, minor))) => {
                    Ok(AuthoredMutation::CreateDevice {
                        path,
                        kind,
                        major,
                        minor,
                        metadata: FileMetadata::default(),
                    })
                }
                _ => Err(WorkspaceError::IncompatibleWorkspace),
            },
            work,
            cancellation,
        )
        .await
    }

    /// Replaces canonical metadata.
    pub async fn set_metadata<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        metadata: FileMetadata,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| Ok(AuthoredMutation::SetMetadata { path, metadata }),
            work,
            cancellation,
        )
        .await
    }

    /// Retains the inspected identity of a staged file.
    pub async fn preserve_file_identity<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        file_id: FileId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| Ok(AuthoredMutation::Reidentify { path, file_id }),
            work,
            cancellation,
        )
        .await
    }

    pub(crate) async fn restore_record<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        record: crate::kernel::FileRecord,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let path = self.mutation_path(path, &mut accounting, cancellation)?;
            let mut operations = crate::kernel::Owned::allocate(1, &mut accounting)?;
            Self::push_mutation(
                &mut operations,
                crate::kernel::Owned::new(
                    crate::kernel::Mutation::Restore {
                        path: path.value,
                        record,
                    },
                    path.allocation,
                    path.bytes,
                ),
                &mut accounting,
            )?;
            self.checkout
                .mutate_owned_with_accounting(operations, &mut accounting, cancellation)
                .await
                .map_err(WorkspaceError::from)
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Streams immutable content under supplied work limits.
    pub async fn stage_content<R: crate::kernel::AsyncBlobSource, W: crate::facade::FsWork>(
        &self,
        source: &mut R,
        maximum_source_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::StagedContent, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .checkout
            .stage_content_with_accounting(
                source,
                maximum_source_bytes,
                &mut accounting,
                cancellation,
            )
            .await
            .map_err(WorkspaceError::from);
        work.finish(result, &accounting)
    }

    /// Creates missing directory prefixes without replacing existing objects.
    pub async fn create_dir_all<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.create_dirs(path, &mut accounting, cancellation).await;
        work.finish(result, &accounting)
    }

    pub(crate) async fn create_dirs(
        &mut self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.checkout.validate_native_work(original)?;
        }
        if PortablePath::validate(path, self.checkout.volume_config().limits)
            .map_err(|error| WorkspaceError::from(crate::kernel::NamespacePathError::Path(error)))?
            == 0
        {
            return Ok(());
        }
        for end in path
            .match_indices('/')
            .skip(1)
            .map(|(index, _)| index)
            .chain(std::iter::once(path.len()))
        {
            let path = self.mutation_path(&path[..end], accounting, cancellation)?;
            match self
                .checkout
                .lookup_no_follow_with_accounting(&path.value, accounting, cancellation, true)
                .await?
                .0
                .record
            {
                Some(record) if record.kind == FileKind::Directory => path.retire(accounting)?,
                Some(_) => {
                    path.retire(accounting)?;
                    return Err(WorkspaceError::NotDirectory);
                }
                None => {
                    let mut operations = crate::kernel::Owned::allocate(1, accounting)?;
                    Self::push_mutation(
                        &mut operations,
                        crate::kernel::Owned::new(
                            AuthoredMutation::CreateDirectory {
                                path: path.value,
                                metadata: FileMetadata::default(),
                            },
                            path.allocation,
                            path.bytes,
                        ),
                        accounting,
                    )?;
                    self.apply_authored(operations, accounting, cancellation)
                        .await?;
                }
            }
        }
        Ok(())
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    /// Creates or atomically replaces one complete UTF-8 file.
    pub async fn write_text(&mut self, path: &str, text: &str) -> Result<(), WorkspaceError> {
        self.write(
            path,
            Bytes::copy_from_slice(text.as_bytes()),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
    }

    /// Creates or replaces one complete file under supplied work limits.
    pub async fn write<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        bytes: impl Into<acyclic_stream::OwnedBytes>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let bytes = bytes.into();
        let result = async {
            let mut path = self.mutation_path(path, &mut accounting, cancellation)?;
            let existing = self
                .checkout
                .lookup_no_follow_with_accounting(&path.value, &mut accounting, cancellation, true)
                .await?
                .0
                .record;
            if existing.is_some_and(|record| record.kind != FileKind::Regular) {
                return Err(WorkspaceError::NotRegularFile);
            }
            let count = 1 + usize::from(existing.is_some());
            let copied = crate::WorkCounters {
                bytes_copied: (count * std::mem::size_of::<AuthoredMutation>()) as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            let mut operations = crate::kernel::Owned::allocate(count, &mut accounting)?;
            operations.allocation.absorb(&mut path.allocation)?;
            operations.bytes = operations
                .bytes
                .checked_add(path.bytes)
                .ok_or(crate::WorkError::Overflow)?;
            if existing.is_some() {
                let (copy, mut allocation) = path
                    .value
                    .clone_with_accounting(&mut accounting, cancellation)?;
                operations.bytes = operations
                    .bytes
                    .checked_add(copy.retained_bytes())
                    .ok_or(crate::WorkError::Overflow)?;
                operations.allocation.absorb(&mut allocation)?;
                operations.value.push(AuthoredMutation::Resize {
                    path: copy,
                    logical_bytes: 0,
                });
                operations.value.push(AuthoredMutation::Write {
                    path: path.value,
                    offset: 0,
                    bytes,
                });
            } else {
                operations.value.push(AuthoredMutation::CreateFile {
                    path: path.value,
                    bytes,
                    metadata: FileMetadata::default(),
                });
            }
            accounting.record_completed(copied)?;
            self.apply_authored(operations, &mut accounting, cancellation)
                .await
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Removes one existing binding under supplied work limits.
    pub async fn remove<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .remove_record(path, &mut accounting, cancellation)
            .await;
        work.finish(result, &accounting)
    }

    pub(crate) async fn remove_record(
        &mut self,
        path: &str,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let path = self.mutation_path(path, accounting, cancellation)?;
        let record = self
            .checkout
            .lookup_no_follow_with_accounting(&path.value, accounting, cancellation, true)
            .await?
            .0
            .record
            .ok_or(WorkspaceError::NotFound)?;
        let mut operations = crate::kernel::Owned::allocate(1, accounting)?;
        Self::push_mutation(
            &mut operations,
            crate::kernel::Owned::new(
                AuthoredMutation::Remove {
                    path: path.value,
                    expected_file_id: Some(record.file_id),
                },
                path.allocation,
                path.bytes,
            ),
            accounting,
        )?;
        self.apply_authored(operations, accounting, cancellation)
            .await
    }

    async fn mutate_pair<W: crate::facade::FsWork>(
        &mut self,
        source: &str,
        destination: &str,
        build: impl FnOnce(NamespacePath, NamespacePath) -> AuthoredMutation,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let mut source = self.mutation_path(source, &mut accounting, cancellation)?;
            let mut destination = self.mutation_path(destination, &mut accounting, cancellation)?;
            source.allocation.absorb(&mut destination.allocation)?;
            let bytes = source
                .bytes
                .checked_add(destination.bytes)
                .ok_or(crate::WorkError::Overflow)?;
            let operation = crate::kernel::Owned::new(
                build(source.value, destination.value),
                source.allocation,
                bytes,
            );
            let mut operations = crate::kernel::Owned::allocate(1, &mut accounting)?;
            Self::push_mutation(&mut operations, operation, &mut accounting)?;
            self.apply_authored(operations, &mut accounting, cancellation)
                .await
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Creates one same-workspace hard link.
    pub async fn hard_link<W: crate::facade::FsWork>(
        &mut self,
        source: &str,
        destination: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate_pair(
            source,
            destination,
            |source, destination| AuthoredMutation::HardLink {
                source,
                destination,
            },
            work,
            cancellation,
        )
        .await
    }

    /// Renames one binding with an exact replacement policy.
    pub async fn rename<W: crate::facade::FsWork>(
        &mut self,
        source: &str,
        destination: &str,
        replace: bool,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate_pair(
            source,
            destination,
            |source, destination| AuthoredMutation::Rename {
                source,
                destination,
                replace,
            },
            work,
            cancellation,
        )
        .await
    }
    /// Creates or replaces one complete file from already staged content.
    pub async fn write_staged<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        parts: &[crate::StagedContent],
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if parts.is_empty() {
                return Err(WorkspaceError::EmptyContentSet);
            }
            let path = self.mutation_path(path, &mut accounting, cancellation)?;
            let record = self
                .checkout
                .lookup_no_follow_with_accounting(&path.value, &mut accounting, cancellation, true)
                .await?
                .0
                .record;
            if record.is_some_and(|record| record.kind != FileKind::Regular) {
                return Err(WorkspaceError::NotRegularFile);
            }
            let mut operations = crate::kernel::Owned::allocate(1, &mut accounting)?;
            let (first_path, allocation) = path
                .value
                .clone_with_accounting(&mut accounting, cancellation)?;
            let bytes = first_path.retained_bytes();
            let first = match record {
                None => AuthoredMutation::CreateFile {
                    path: first_path,
                    bytes: acyclic_stream::OwnedBytes::new(),
                    metadata: FileMetadata::default(),
                },
                Some(_) => AuthoredMutation::Resize {
                    path: first_path,
                    logical_bytes: 0,
                },
            };
            Self::push_mutation(
                &mut operations,
                crate::kernel::Owned::new(first, allocation, bytes),
                &mut accounting,
            )?;
            let maximum = parts
                .len()
                .checked_add(1)
                .ok_or(crate::WorkError::Overflow)?;
            let mut offset = 0_u64;
            for part in parts {
                accounting.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                if part.logical_bytes() != 0 {
                    operations.ensure_for_push(maximum, &mut accounting)?;
                    let (path, allocation) = path
                        .value
                        .clone_with_accounting(&mut accounting, cancellation)?;
                    let bytes = path.retained_bytes();
                    let mutation = AuthoredMutation::WriteFromContent {
                        path,
                        offset,
                        content: *part,
                    };
                    Self::push_mutation(
                        &mut operations,
                        crate::kernel::Owned::new(mutation, allocation, bytes),
                        &mut accounting,
                    )?;
                }
                offset = offset
                    .checked_add(part.logical_bytes())
                    .ok_or(WorkspaceError::ContentLengthOverflow)?;
            }
            path.retire(&mut accounting)?;
            self.apply_authored(operations, &mut accounting, cancellation)
                .await
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Removes one binding only when its authenticated identity still matches.
    pub async fn remove_if<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        expected_file_id: crate::FileId,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::Remove {
                    path,
                    expected_file_id: Some(expected_file_id),
                })
            },
            work,
            cancellation,
        )
        .await
    }

    /// Replaces one destination with a complete copy-on-write clone.
    pub async fn copy<W: crate::facade::FsWork>(
        &mut self,
        source: &str,
        destination: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let mut source = self.mutation_path(source, &mut accounting, cancellation)?;
            let destination = self.mutation_path(destination, &mut accounting, cancellation)?;
            let record = self
                .checkout
                .lookup_no_follow_with_accounting(
                    &source.value,
                    &mut accounting,
                    cancellation,
                    true,
                )
                .await?
                .0
                .record
                .ok_or(WorkspaceError::NotFound)?;
            let logical_bytes = regular_file_bytes(record)?;
            let record = self
                .checkout
                .lookup_no_follow_with_accounting(
                    &destination.value,
                    &mut accounting,
                    cancellation,
                    true,
                )
                .await?
                .0
                .record;
            if record.is_some_and(|record| record.kind != FileKind::Regular) {
                return Err(WorkspaceError::NotRegularFile);
            }
            let mut operations = crate::kernel::Owned::allocate(
                1 + usize::from(logical_bytes != 0),
                &mut accounting,
            )?;
            let cloned = if logical_bytes != 0 {
                Some(
                    destination
                        .value
                        .clone_with_accounting(&mut accounting, cancellation)?,
                )
            } else {
                None
            };
            let first = match record {
                None => AuthoredMutation::CreateFile {
                    path: destination.value,
                    bytes: acyclic_stream::OwnedBytes::new(),
                    metadata: FileMetadata::default(),
                },
                Some(_) => AuthoredMutation::Resize {
                    path: destination.value,
                    logical_bytes: 0,
                },
            };
            Self::push_mutation(
                &mut operations,
                crate::kernel::Owned::new(first, destination.allocation, destination.bytes),
                &mut accounting,
            )?;
            if let Some((destination, mut allocation)) = cloned {
                source.allocation.absorb(&mut allocation)?;
                let bytes = source
                    .bytes
                    .checked_add(destination.retained_bytes())
                    .ok_or(crate::WorkError::Overflow)?;
                let clone = AuthoredMutation::CloneRange(crate::FileCloneRequest {
                    source: source.value,
                    source_offset: 0,
                    destination,
                    destination_offset: 0,
                    length: logical_bytes,
                });
                Self::push_mutation(
                    &mut operations,
                    crate::kernel::Owned::new(clone, source.allocation, bytes),
                    &mut accounting,
                )?;
            } else {
                source.retire(&mut accounting)?;
            }
            self.apply_authored(operations, &mut accounting, cancellation)
                .await
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Changes one regular file's logical length while preserving sparse layout.
    pub async fn resize<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        logical_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::Resize {
                    path,
                    logical_bytes,
                })
            },
            work,
            cancellation,
        )
        .await
    }

    /// Punches a hole or installs allocated zeros over one exact range.
    pub async fn zero_range<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        range: crate::ByteRange,
        allocated: bool,
        extend: bool,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::ZeroRange {
                    path,
                    range,
                    allocated,
                    extend,
                })
            },
            work,
            cancellation,
        )
        .await
    }

    /// Preallocates one sparse range without replacing existing content.
    pub async fn preallocate<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        range: crate::ByteRange,
        keep_size: bool,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate(
            path,
            |path| {
                Ok(AuthoredMutation::Preallocate {
                    path,
                    range,
                    keep_size,
                })
            },
            work,
            cancellation,
        )
        .await
    }

    /// Clones one immutable range without reading or copying content bytes.
    pub async fn clone_range<W: crate::facade::FsWork>(
        &mut self,
        source: &str,
        source_offset: u64,
        destination: &str,
        destination_offset: u64,
        length: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.mutate_pair(
            source,
            destination,
            |source, destination| {
                AuthoredMutation::CloneRange(crate::FileCloneRequest {
                    source,
                    source_offset,
                    destination,
                    destination_offset,
                    length,
                })
            },
            work,
            cancellation,
        )
        .await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Creates or atomically replaces one complete UTF-8 file.
    pub async fn write_text(
        &self,
        path: &str,
        text: &str,
    ) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        self.write(path, Bytes::copy_from_slice(text.as_bytes()))
            .await
    }

    /// Creates or replaces one complete file in a single atomic generation.
    pub async fn write(
        &self,
        path: &str,
        bytes: Bytes,
    ) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        let mut transaction = self
            .begin_transaction(
                IdempotencyKey::new(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        transaction
            .write(
                path,
                bytes,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?;
        transaction
            .commit(
                None,
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                |_| async {
                    Ok::<_, crate::OperationFailure<WorkspaceError>>(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    })
                },
            )
            .await
            .map_err(|failure| failure.error)
            .map(|receipt| receipt.value)
    }

    /// Removes one existing path in a single atomic generation publication.
    pub async fn remove(&self, path: &str) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        let mut transaction = self
            .begin_transaction(
                IdempotencyKey::new(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        transaction
            .remove(path, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await
            .map_err(|failure| failure.error)?;
        transaction
            .commit(
                None,
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                |_| async {
                    Ok::<_, crate::OperationFailure<WorkspaceError>>(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    })
                },
            )
            .await
            .map_err(|failure| failure.error)
            .map(|receipt| receipt.value)
    }

    /// Copies one complete regular file within this workspace using immutable
    pub async fn copy(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        let mut transaction = self
            .begin_transaction(
                IdempotencyKey::new(),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        transaction
            .copy(
                source,
                destination,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        transaction
            .commit(
                None,
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                |_| async {
                    Ok::<_, crate::OperationFailure<WorkspaceError>>(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    })
                },
            )
            .await
            .map_err(|failure| failure.error)
            .map(|receipt| receipt.value)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    /// Publishes this private candidate under the supplied limits and seal.
    pub async fn commit<E, F, Fut>(
        &mut self,
        context: Option<(
            crate::Digest,
            acyclic_stream::OwnedBytes,
            crate::PublicationPermit,
        )>,
        permit: crate::PublicationPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
        mut before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, E>
    where
        E: From<WorkspaceError> + From<FsError> + Send,
        F: FnMut(WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::MeasuredResult<crate::OperationReceipt<()>, E>> + ProviderTask,
    {
        let (context, evidence, fingerprint_permit) = match context {
            Some((context, evidence, permit)) => (Some(context), Some(evidence), Some(permit)),
            None => (None, None, None),
        };
        crate::heap_future::in_heap_measured_with_error(
            move |budget| async move {
                let (outcome, mut work) = if self.requires_rebase {
                    let receipt = self
                        .checkout
                        .retry_stale_commit(
                            self.idempotency_key.operation_id(),
                            permit,
                            budget,
                            cancellation,
                            &mut before_publication,
                            None,
                            context,
                            evidence,
                            fingerprint_permit,
                        )
                        .await?;
                    (receipt.value, receipt.work)
                } else {
                    let receipt = self
                        .checkout
                        .commit(
                            self.idempotency_key.operation_id(),
                            context,
                            evidence,
                            fingerprint_permit,
                            permit,
                            budget,
                            cancellation,
                            &mut before_publication,
                        )
                        .await?;
                    (Some(receipt.value), receipt.work)
                };
                let value = match outcome.and_then(|outcome| self.commit_value(outcome)) {
                    Some(value) => value,
                    None => {
                        let remaining = work.remaining(budget).map_err(|error| {
                            crate::OperationFailure::new(E::from(WorkspaceError::from(error)), work)
                        })?;
                        let actual = self.workspace.head(remaining, cancellation).await.map_err(
                            |failure| {
                                failure
                                    .map_with_prior_work(work, std::convert::identity)
                                    .map_error(E::from)
                            },
                        )?;
                        work = work.checked_add(actual.work).map_err(|error| {
                            crate::OperationFailure::new(E::from(WorkspaceError::from(error)), work)
                        })?;
                        TransactionCommit::Conflict {
                            actual: actual.value,
                        }
                    }
                };
                Ok(crate::OperationReceipt { value, work })
            },
            budget,
            cancellation,
            E::from(WorkspaceError::Cancelled(crate::CancellationError)),
            None,
            |error| E::from(WorkspaceError::from(error)),
        )
        .await
    }

    fn commit_value(&self, outcome: LivePublicationObservation) -> Option<TransactionCommit<A, O>> {
        let generation = |id| Generation {
            workspace: self.workspace.clone(),
            id,
            publication_closure: self.checkout.publication_closure(id),
        };
        Some(match outcome {
            LivePublicationObservation::Committed { generation_id, .. } => {
                TransactionCommit::Committed(generation(generation_id))
            }
            LivePublicationObservation::AlreadyCommitted { generation_id, .. } => {
                TransactionCommit::AlreadyCommitted(generation(generation_id))
            }
            LivePublicationObservation::Conflict { .. } => return None,
            LivePublicationObservation::Fenced { .. } => TransactionCommit::Fenced,
            LivePublicationObservation::IdempotencyConflict { .. } => {
                TransactionCommit::IdempotencyConflict
            }
        })
    }

    /// Publishes on the original native scope with the supplied seal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn commit_with_native_work<'a, E, F, Fut>(
        &mut self,
        context: Option<(
            crate::Digest,
            acyclic_stream::OwnedBytes,
            crate::PublicationPermit,
        )>,
        permit: crate::PublicationPermit,
        original: &'a crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> Result<TransactionCommit<A, O>, E>
    where
        E: From<FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let (context, evidence, fingerprint_permit) = match context {
            Some((context, evidence, permit)) => (Some(context), Some(evidence), Some(permit)),
            None => (None, None, None),
        };
        let outcome = if self.requires_rebase {
            self.checkout
                .retry_stale_commit_with_native_work(
                    self.idempotency_key.operation_id(),
                    permit,
                    original,
                    cancellation,
                    before_publication,
                    None,
                    context,
                    evidence,
                    fingerprint_permit,
                )
                .await?
        } else {
            let mut before_publication = (*before_publication).clone();
            let mode = self.checkout.mode();
            if mode.access != crate::model::AccessMode::ReadWrite
                || mode.mutations != crate::model::MutationMode::PrivateOverlay
            {
                return Err(FsError::MutationNotAllowed.into());
            }
            Some(
                self.checkout
                    .publish_pending_with_native_work(
                        self.idempotency_key.operation_id(),
                        context,
                        evidence,
                        fingerprint_permit,
                        permit,
                        original,
                        cancellation,
                        &mut before_publication,
                        None,
                    )
                    .await?,
            )
        };
        if let Some(value) = outcome.and_then(|outcome| self.commit_value(outcome)) {
            return Ok(value);
        }
        let actual = self
            .workspace
            .volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                original,
                cancellation,
            )
            .await?;
        Ok(TransactionCommit::Conflict {
            actual: Generation {
                workspace: self.workspace.clone(),
                id: actual.generation_id(),
                publication_closure: None,
            },
        })
    }

    /// Advances a private candidate only when its observed dependencies remain valid.
    pub async fn rebase(
        &mut self,
        maximum_conflicts: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionRebase<A, O>>, WorkspaceError>
    {
        let receipt = self
            .checkout
            .rebase_head(maximum_conflicts, budget, cancellation)
            .await
            .map_err(|failure| failure.map_error(WorkspaceError::engine))?;
        let value = match receipt.value {
            crate::kernel::RebaseDecision::Safe { generation } => {
                self.requires_rebase = false;
                TransactionRebase::Rebased(Generation {
                    workspace: self.workspace.clone(),
                    id: generation,
                    publication_closure: None,
                })
            }
            crate::kernel::RebaseDecision::Conflicted {
                conflicts,
                truncated,
            } => TransactionRebase::Conflicted {
                conflicts,
                truncated,
            },
        };
        Ok(crate::OperationReceipt {
            value,
            work: receipt.work,
        })
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
    /// The current head is now the candidate's immutable base and its sparse local mutations were replayed without crossing a dependency.
    Rebased(Generation<A, O>),
    /// One or more exact observed or mutated regions changed upstream.
    Conflicted {
        /// Stable region-specific conflicts bounded by the caller.
        conflicts: Vec<crate::kernel::RebaseConflict>,
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

/// Immutable complete filesystem state belonging to one workspace.
pub struct Generation<A, O> {
    pub(crate) workspace: Workspace<A, O>,
    pub(crate) id: GenerationId,
    pub(crate) publication_closure: Option<crate::facade::PublicationClosure<A, O>>,
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

    /// Retained diff buffer bytes.
    pub fn retained_bytes(&self) -> u64 {
        workspace_vec_bytes(&self.changes.files)
            + workspace_vec_bytes(&self.changes.bindings)
            + self
                .changes
                .bindings
                .iter()
                .map(|binding| {
                    crate::foundation::usize_to_u64(binding.name.retained_bytes())
                        + binding.before.as_ref().map_or(0, |entry| {
                            crate::foundation::usize_to_u64(entry.name.retained_bytes())
                        })
                        + binding.after.as_ref().map_or(0, |entry| {
                            crate::foundation::usize_to_u64(entry.name.retained_bytes())
                        })
                })
                .sum::<u64>()
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

    fn validate_workspaces(&self) -> Result<(), WorkspaceError> {
        self.validate_bounds()?;
        if !self
            .source
            .volume
            .fs
            .same_deployment(&self.target.volume.fs)
        {
            return Err(WorkspaceError::ForeignGeneration);
        }
        if self.source.volume.config != self.target.volume.config {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        self.source
            .volume
            .config
            .validate()
            .map_err(FsError::from)?;
        Ok(())
    }

    fn validate_endpoints(
        &self,
        source: &Generation<A, O>,
        target: &Generation<A, O>,
    ) -> Result<(), WorkspaceError> {
        self.validate_workspaces()?;
        if source.workspace.id != self.source.id
            || target.workspace.id != self.target.id
            || !self
                .source
                .volume
                .fs
                .same_deployment(&source.workspace.volume.fs)
            || !self
                .target
                .volume
                .fs
                .same_deployment(&target.workspace.volume.fs)
        {
            return Err(WorkspaceError::ForeignGeneration);
        }
        if source.workspace.volume.config != self.source.volume.config
            || target.workspace.volume.config != self.target.volume.config
        {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        Ok(())
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
    /// Authenticates current endpoints and discovers a bounded common ancestor.
    pub async fn plan<W: crate::facade::FsWork>(
        self,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<JoinPlan<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            self.validate_workspaces()?;
            let source_id = self
                .source
                .checkout_accounted(
                    GenerationSelector::Head,
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    cancellation,
                )
                .await?
                .generation_id();
            let (target_id, authority_head) = self
                .target
                .volume
                .fs
                .workspace_head_state(&self.target.volume, &mut accounting, cancellation)
                .await?;
            self.target
                .checkout_accounted(
                    GenerationSelector::Exact(target_id),
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let source = Generation {
                workspace: self.source.clone(),
                id: source_id,
                publication_closure: None,
            };
            let target = Generation {
                workspace: self.target.clone(),
                id: target_id,
                publication_closure: None,
            };
            self.plan_generations(
                source,
                target,
                authority_head,
                &mut accounting,
                cancellation,
            )
            .await
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Plans exact immutable endpoints under supplied work limits.
    pub async fn plan_at<W: crate::facade::FsWork>(
        self,
        source: &Generation<A, O>,
        target: &Generation<A, O>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<JoinPlan<A, O>, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            self.validate_endpoints(source, target)?;
            let (target_head_id, target_authority_head) = self
                .target
                .volume
                .fs
                .workspace_head_state(&self.target.volume, &mut accounting, cancellation)
                .await?;
            if target.id != target_head_id {
                return Err(WorkspaceError::StaleTarget);
            }
            let copied = crate::WorkCounters {
                bytes_copied: (2 * std::mem::size_of::<Generation<A, O>>()) as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            let source = source.clone();
            let target = target.clone();
            accounting.record_completed(copied)?;
            let plan = self
                .plan_generations(
                    source,
                    target,
                    target_authority_head,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            Ok::<_, WorkspaceError>(plan)
        }
        .await;
        work.finish(result, &accounting)
    }

    pub(crate) async fn plan_generations(
        self,
        mut source_head: Generation<A, O>,
        mut target_head: Generation<A, O>,
        target_authority_head: crate::Head,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<JoinPlan<A, O>, WorkspaceError> {
        self.validate_endpoints(&source_head, &target_head)?;
        let base = self
            .source
            .volume
            .fs
            .workspace_common_ancestor_with_accounting(
                &source_head,
                &target_head,
                self.maximum_generations,
                accounting,
                cancellation,
            )
            .await?;
        let copied = crate::WorkCounters {
            bytes_copied: std::mem::size_of::<JoinPlan<A, O>>() as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        source_head.workspace = self.source;
        target_head.workspace = self.target;
        let plan = JoinPlan {
            source_head,
            target_head,
            target_authority_head,
            base,
            history: self.history,
            maximum_generations: self.maximum_generations,
            maximum_changes: self.maximum_changes,
            maximum_conflicts: self.maximum_conflicts,
        };
        accounting.record_completed(copied)?;
        Ok(plan)
    }
}

/// Immutable side-effect-free join plan.
pub struct JoinPlan<A, O> {
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
        &self.source_head.workspace
    }

    /// Captures exact join inputs under supplied work limits.
    pub fn commit_witness<W: crate::facade::FsWork>(
        &self,
        application: &JoinApplication<A, O>,
        idempotency_key: IdempotencyKey,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::JoinCommitWitness, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = (|| -> Result<_, WorkspaceError> {
            let examined = crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            };
            accounting.admit(examined)?;
            let incompatible = application.generation.workspace.id()
                != self.target_head.workspace.id()
                || application.generation.id() == self.target_head.id();
            accounting.record_completed(examined)?;
            if incompatible {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            let copied = crate::WorkCounters {
                bytes_copied: std::mem::size_of::<crate::JoinCommitWitness>() as u64,
                ..crate::WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            let witness = crate::JoinCommitWitness {
                source_workspace: self.source_head.workspace.id().volume_id(),
                source_generation: self.source_head.id,
                target_workspace: self.target_head.workspace.id().volume_id(),
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
            };
            accounting.record_completed(copied)?;
            Ok(witness)
        })();
        work.finish(result, &accounting)
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

/// One durable join result together with its exact conflict-resolution input. The digest is generated inside Filesystem's publication path, so receipt producers cannot accidentally attest a different set of resolutions.
pub struct JoinApplication<A, O> {
    /// Immutable generation made durable by this join.
    pub generation: Generation<A, O>,
    /// Digest of the applied conflict resolutions.
    pub resolutions_digest: crate::Digest,
}

/// Terminal semantic outcome of an exact workspace-head restoration.
pub enum WorkspaceRestore<G> {
    /// The requested historical generation became current.
    Restored(G),
    /// The same restoration was already durable under this retry identity.
    AlreadyRestored(G),
    /// The requested generation was already current; no publication occurred.
    Current(G),
    /// The workspace advanced after the caller observed it.
    Stale(G),
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
#[derive(Debug, Error, strum::IntoStaticStr)]
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
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<GenerationDiff>, WorkspaceError> {
        self.source_head
            .workspace
            .volume
            .fs
            .workspace_join_changes(
                self.base.id,
                &self.source_head,
                self.maximum_changes,
                budget,
                cancellation,
            )
            .await
            .map(|receipt| crate::OperationReceipt {
                value: receipt.value.0,
                work: receipt.work,
            })
    }

    /// Applies this immutable plan only while the supplied writer permit remains valid.
    pub async fn apply(
        &self,
        options: ApplyOptions,
        permit: crate::PublicationPermit,
    ) -> Result<JoinOutcome<A, O>, WorkspaceError> {
        self.apply_resolutions(options, BTreeMap::new(), permit)
            .await
    }

    /// Seals publication through the canonical native publisher before its CAS.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn apply_with_native_work<'original, F, Fut, E>(
        &self,
        options: ApplyOptions,
        permit: crate::PublicationPermit,
        original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
        before_publication: &mut F,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut
            + Clone
            + Send
            + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
    {
        self.apply_resolutions_guarded_with_native_work(
            options,
            &[],
            permit,
            original,
            cancellation,
            before_publication,
        )
        .await
    }

    /// Expands exact kernel conflicts into immutable, driver-ready values.
    pub async fn describe_conflicts(
        &self,
        conflicts: &[MergeConflict],
        truncated: bool,
    ) -> Result<MergePlan, WorkspaceError> {
        self.typed_merge_plan(conflicts.to_vec(), truncated).await
    }

    /// Resolves real join conflicts with registered immutable-input drivers.
    pub async fn apply_with_drivers<C: MergeResolutionCache>(
        &self,
        options: ApplyOptions,
        registry: &MergeDriverRegistry,
        cache: &mut C,
        replanning: bool,
    ) -> Result<JoinOutcome<A, O>, DrivenJoinError> {
        let initial = self
            .apply(options, crate::PublicationPermit::Unrestricted)
            .await?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = initial
        else {
            return Ok(initial);
        };
        let plan = self.typed_merge_plan(conflicts, truncated).await?;
        let candidate = resolve_merge_plan(plan, registry, cache, replanning)?;
        self.apply_candidate(options, &candidate, crate::PublicationPermit::Unrestricted)
            .await
    }

    /// Applies validated caller-selected immutable conflict sides.
    pub async fn apply_sides(
        &self,
        options: ApplyOptions,
        selections: BTreeMap<MergeConflict, ConflictSide>,
    ) -> Result<JoinOutcome<A, O>, DrivenJoinError> {
        let initial = self
            .apply(options, crate::PublicationPermit::Unrestricted)
            .await?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = initial
        else {
            return Ok(initial);
        };
        let cancellation = crate::CancellationToken::new();
        validate_join_side_selections(
            &conflicts,
            truncated,
            &selections,
            &mut crate::kernel::allocation::KernelAccounting::standalone(
                WorkBudget::UNBOUNDED,
                &cancellation,
            ),
        )?;
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

    /// Validates the ordinary conflict set, then seals the same second application.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn apply_sides_with_permit_guarded_with_native_work<'original, F, Fut, E>(
        &self,
        options: ApplyOptions,
        selections: &BTreeMap<MergeConflict, ConflictSide>,
        permit: crate::PublicationPermit,
        original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
        before_publication: &mut F,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut
            + Clone
            + Send
            + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + From<DrivenJoinError> + Send + 'static,
    {
        let (initial, allocation) = self
            .apply_with_native_work(options, permit, original, cancellation, before_publication)
            .await?;
        let JoinOutcome::Conflicted {
            conflicts,
            truncated,
        } = initial
        else {
            return Ok((initial, allocation));
        };
        let bytes = allocation.bytes();
        let conflicts = crate::kernel::Owned::new(
            conflicts,
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation),
            bytes,
        );
        let mut accounting =
            crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        let validation =
            validate_join_side_selections(&conflicts.value, truncated, selections, &mut accounting);
        conflicts.retire(&mut accounting).map_err(E::from)?;
        validation.map_err(E::from)?;
        let resolutions = native_join_side_resolutions(selections, &mut accounting, cancellation)
            .map_err(E::from)?;
        let applied = self
            .apply_resolutions_guarded_with_native_work(
                options,
                &resolutions.value,
                permit,
                original,
                cancellation,
                before_publication,
            )
            .await;
        resolutions.retire(&mut accounting).map_err(E::from)?;
        applied
    }

    /// Validates and publishes a declarative candidate under one writer permit.
    pub async fn apply_candidate(
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
            || !candidate.resolutions.keys().eq(conflict_keys)
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
                        .target_head
                        .workspace
                        .volume
                        .fs
                        .workspace_conflict_record(
                            &self.target_head.workspace.volume,
                            self.target_head.id,
                            file_id,
                        )
                        .await?
                        .ok_or(WorkspaceError::InvalidMergeResolution)?;
                    record.metadata = self
                        .target_head
                        .workspace
                        .volume
                        .fs
                        .stage_merge_metadata(&self.target_head.workspace.volume, bytes)
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
                    self.target_head
                        .workspace
                        .volume
                        .config
                        .limits
                        .maximum_component_bytes,
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
            .target_head
            .workspace
            .volume
            .fs
            .workspace_conflict_record(
                &self.target_head.workspace.volume,
                self.target_head.id,
                file_id,
            )
            .await?
            .ok_or(WorkspaceError::InvalidMergeResolution)?;
        self.target_head
            .workspace
            .volume
            .fs
            .stage_merge_regular_record(&self.target_head.workspace.volume, record, bytes)
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
        let fs = &self.target_head.workspace.volume.fs;
        let base = fs
            .workspace_conflict_record(&self.target_head.workspace.volume, self.base.id, file_id)
            .await?;
        let ours = fs
            .workspace_conflict_record(
                &self.target_head.workspace.volume,
                self.target_head.id,
                file_id,
            )
            .await?;
        let theirs = fs
            .workspace_conflict_record(
                &self.target_head.workspace.volume,
                self.source_head.id,
                file_id,
            )
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
                    fs.workspace_conflict_metadata(&self.target_head.workspace.volume, record)
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
        let mut paths = self
            .target_head
            .paths_for_file_id(
                file_id,
                maximum,
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)?;
        if paths.is_empty() {
            paths = self
                .source_head
                .paths_for_file_id(
                    file_id,
                    maximum,
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)?;
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
                .target_head
                .workspace
                .volume
                .fs
                .workspace_conflict_bytes(&self.target_head.workspace.volume, record)
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
            return Ok(JoinOutcome::StaleTarget(
                self.target_head
                    .workspace
                    .head(
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)?,
            ));
        }
        let outcome = self
            .target_head
            .workspace
            .volume
            .fs
            .apply_workspace_join(crate::facade::WorkspaceJoinRequest {
                target: &self.target_head.workspace.volume,
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
                resolutions: crate::facade::WorkspaceJoinResolutions::Map(resolutions),
            })
            .await?;
        self.map_join_outcome(outcome)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn apply_resolutions_guarded_with_native_work<'original, F, Fut, E>(
        &self,
        options: ApplyOptions,
        resolutions: &[(MergeConflict, crate::kernel::MergeConflictResolution)],
        permit: crate::PublicationPermit,
        original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
        before_publication: &mut F,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut
            + Clone
            + Send
            + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
    {
        if options.if_target != self.target_head.id {
            let mut accounting =
                crate::kernel::allocation::KernelAccounting::native(original, cancellation);
            let (id, _) = self
                .target_head
                .workspace
                .volume
                .fs
                .workspace_head_state(
                    &self.target_head.workspace.volume,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            return Ok((
                JoinOutcome::StaleTarget(Generation {
                    workspace: self.target_head.workspace.clone(),
                    id,
                    publication_closure: None,
                }),
                original
                    .claim(0, 0, crate::WorkCounters::UNCHARGED)
                    .map_err(FsError::from)?,
            ));
        }
        let (outcome, allocation) = self
            .target_head
            .workspace
            .volume
            .fs
            .apply_workspace_join_guarded_with_native_work(
                crate::facade::WorkspaceJoinRequest {
                    target: &self.target_head.workspace.volume,
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
                    resolutions: crate::facade::WorkspaceJoinResolutions::Slice(resolutions),
                },
                original,
                cancellation,
                before_publication,
            )
            .await?;
        self.map_join_outcome(outcome)
            .map(|outcome| (outcome, allocation))
            .map_err(E::from)
    }

    fn map_join_outcome(
        &self,
        outcome: crate::facade::WorkspaceJoinOutcome,
    ) -> Result<JoinOutcome<A, O>, WorkspaceError> {
        let generation = |id| Generation {
            workspace: self.target_head.workspace.clone(),
            id,
            publication_closure: None,
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

impl From<FsError> for DrivenJoinError {
    fn from(error: FsError) -> Self {
        Self::Workspace(WorkspaceError::from(error))
    }
}

impl<A, O> Clone for Generation<A, O> {
    fn clone(&self) -> Self {
        Self {
            workspace: self.workspace.clone(),
            id: self.id,
            publication_closure: self.publication_closure.clone(),
        }
    }
}

impl<A, O> Generation<A, O> {
    pub(crate) fn publication_retained_bytes(&self) -> u64 {
        self.publication_closure.as_ref().map_or(0, |slot| {
            let publication = slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::size_of::<std::sync::Mutex<Option<crate::facade::VerifiedPublication<A, O>>>>(
            ) as u64
                + publication
                    .as_ref()
                    .map_or(0, crate::facade::VerifiedPublication::retained_bytes)
        })
    }

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
    #[must_use]
    pub fn workspace(&self) -> Workspace<A, O> {
        self.workspace.clone()
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Performs the operation under the supplied limits.
    pub async fn matches_fork_selection<W: crate::facade::FsWork>(
        &self,
        source: &Self,
        paths: Option<Vec<String>>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<bool, WorkspaceError> {
        self.workspace
            .volume
            .fs
            .verify_fork_selection(source, self, paths, work, cancellation)
            .await
    }

    /// Performs the operation under the supplied limits.
    pub async fn normalized_join_parent_for<W: crate::facade::FsWork>(
        &self,
        target: &Self,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<GenerationId, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .workspace
            .volume
            .fs
            .workspace_normalized_join_parent(self, target, &mut accounting, cancellation)
            .await;
        work.finish(result, &accounting)
    }

    /// Performs the operation under the supplied limits.
    pub async fn parents(
        &self,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Vec<GenerationId>>, WorkspaceError> {
        self.workspace
            .volume
            .fs
            .generation_parents(&self.workspace.volume, self.id, budget, cancellation)
            .await
            .map(|receipt| crate::OperationReceipt {
                value: receipt.value.0,
                work: receipt.work,
            })
    }

    /// Performs the operation under the supplied limits.
    pub async fn diff_to<W: crate::facade::FsWork>(
        &self,
        to: &Generation<A, O>,
        maximum_changes: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(ChangeSet<A, O>, crate::AuthorityAllocation), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let (changes, allocation) = match &mut accounting {
                #[cfg(not(target_arch = "wasm32"))]
                KernelAccounting::NativeOriginal { work: original, .. } => {
                    self.workspace
                        .volume
                        .fs
                        .workspace_join_changes(
                            self.id,
                            to,
                            maximum_changes,
                            *original,
                            cancellation,
                        )
                        .await?
                }
                _ => {
                    let result = self
                        .workspace
                        .volume
                        .fs
                        .workspace_join_changes(
                            self.id,
                            to,
                            maximum_changes,
                            accounting.remaining()?,
                            cancellation,
                        )
                        .await;
                    let (_, spent, _, _) = accounting
                        .standalone_parts()
                        .ok_or(crate::WorkError::Overflow)?;
                    merge_workspace_backend(result, spent, 0)?.value
                }
            };
            let allocation = workspace_allocation(allocation, &accounting)?;
            Ok::<_, WorkspaceError>((
                ChangeSet {
                    from: self.clone(),
                    to: to.clone(),
                    changes,
                    work: accounting.spent(),
                },
                allocation,
            ))
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Performs the operation under the supplied limits.
    pub async fn pin<W: crate::facade::FsWork>(
        &self,
        identity: impl AsRef<str>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(WorkspaceName, Generation<A, O>), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result: Result<(WorkspaceName, Generation<A, O>), WorkspaceError> = async {
            let identity = WorkspaceName::with_accounting(identity.as_ref(), &mut accounting)?;
            let mut label = copy_workspace_label(&identity, &mut accounting)?;
            if accounting.standalone_parts().is_some() {
                accounting.release_owned(&mut label.allocation, label.bytes)?;
            }
            let publication = self.publication_closure.as_ref().and_then(|slot| {
                slot.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
            });
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.workspace
                    .volume
                    .fs
                    .retain_workspace_generation(
                        &self.workspace.volume,
                        self.id,
                        crate::kernel::RetentionKind::Pin,
                        label,
                        publication,
                        original,
                        cancellation,
                    )
                    .await?;
                return Ok((identity, self.clone()));
            }
            let retained = self
                .workspace
                .volume
                .fs
                .retain_workspace_generation(
                    &self.workspace.volume,
                    self.id,
                    crate::kernel::RetentionKind::Pin,
                    label,
                    publication,
                    accounting.remaining()?,
                    cancellation,
                )
                .await;
            let (allocations, spent, budget, _) = accounting
                .standalone_parts()
                .ok_or(crate::WorkError::Overflow)?;
            merge_workspace_backend(retained, spent, allocations.live_bytes())?;
            spent.verify(budget)?;
            Ok((identity, self.clone()))
        }
        .await;
        work.finish(result, &accounting)
    }
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Materializes selected paths with shared parent and hard-link state.
    pub async fn materialize<'a>(
        &self,
        paths: impl ExactSizeIterator<Item = &'a str> + ProviderTask,
        options: &crate::MaterializeOptions,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError>
    {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let mut checkout = self
                .workspace
                .open_checkout(
                    GenerationSelector::Exact(self.id),
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let mut parsed = crate::kernel::Owned::allocate(paths.len(), &mut accounting)?;
            for path in paths {
                let (path, allocation) = customer_path_with_accounting(
                    path,
                    checkout.value.volume_config(),
                    &mut accounting,
                    cancellation,
                )?;
                let bytes = path.retained_bytes();
                Transaction::<A, O>::push_mutation(
                    &mut parsed,
                    crate::kernel::Owned::new(path, allocation, bytes),
                    &mut accounting,
                )?;
            }
            let materialized = crate::materialize_checkout_paths(
                &mut checkout.value,
                &parsed.value,
                options,
                accounting.remaining()?,
                cancellation,
            )
            .await;
            let (allocations, spent, budget, _) = accounting
                .standalone_parts()
                .ok_or(crate::WorkError::Overflow)?;
            let value =
                merge_workspace_backend(materialized, spent, allocations.live_bytes())?.value;
            spent.verify(budget)?;
            Ok(value)
        }
        .await;
        crate::facade::measured(result, &accounting)
    }

    /// Replaces or removes one host path from this exact generation.
    pub async fn restore_host_path(
        &self,
        relative: &std::path::Path,
        replacement: crate::HostPathReplacement,
        options: &crate::MaterializeOptions,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::HostPathRestore>, WorkspaceError>
    {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let mut checkout = self
                .workspace
                .open_checkout(
                    GenerationSelector::Exact(self.id),
                    CheckoutMode::read_only_pinned(),
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let restored = crate::restore_checkout_host_path(
                &mut checkout.value,
                relative,
                replacement,
                options,
                accounting.remaining()?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_error(WorkspaceError::engine));
            let (allocations, spent, budget, _) = accounting
                .standalone_parts()
                .ok_or(crate::WorkError::Overflow)?;
            let value = merge_workspace_backend(restored, spent, allocations.live_bytes())?.value;
            spent.verify(budget)?;
            Ok(value)
        }
        .await;
        crate::facade::measured(result, &accounting)
    }
}

/// Exact source selection for a cheap workspace fork.
#[derive(Clone)]
pub struct ForkOptions<A, O> {
    pub(crate) generation: Generation<A, O>,
    pub(crate) idempotency_key: IdempotencyKey,
    pub(crate) paths: Option<Vec<String>>,
}

impl<A, O> ForkOptions<A, O> {
    /// Forks from one exact immutable generation.
    #[must_use]
    pub fn from_generation(generation: Generation<A, O>, idempotency_key: IdempotencyKey) -> Self {
        Self {
            generation,
            idempotency_key,
            paths: None,
        }
    }

    /// Keeps the pinned parent lineage but starts with an empty namespace. The initial view is the child's merge baseline: excluded parent paths are never interpreted as child-authored deletions.
    #[must_use]
    pub fn empty(self) -> Self {
        self.inherit_paths(Vec::new())
    }

    /// Carries only explicitly selected files or directory subtrees. Selection is part of the initial fork, never an authored deletion after creation.
    #[must_use]
    pub fn inherit_paths(mut self, paths: Vec<String>) -> Self {
        self.paths = Some(paths);
        self
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
    /// Canonical name storage could not be allocated.
    #[error("workspace name allocation failed")]
    Allocation,
}

/// Customer workspace operation failure.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum WorkspaceError {
    /// Friendly name was invalid.
    #[error(transparent)]
    Name(#[from] WorkspaceNameError),
    /// The canonical engine rejected or could not complete the operation.
    #[error("workspace engine failure: {0}")]
    Engine(String),
    /// The original filesystem failure.
    #[error(transparent)]
    Filesystem(FsError),
    /// Caller cancellation stopped a measured workspace operation.
    #[error(transparent)]
    Cancelled(#[from] crate::CancellationError),
    /// A measured workspace operation exhausted its admitted work budget.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Both completed work operands when their sum cannot be represented.
    #[error("workspace work merge failed: {error}")]
    WorkMerge {
        /// Work completed before the backend call.
        prior: crate::WorkCounters,
        /// Work completed by the backend.
        backend: crate::WorkCounters,
        /// Checked arithmetic failure.
        #[source]
        error: crate::WorkError,
    },
    /// A fork generation belongs to another filesystem deployment.
    #[error("fork generation belongs to another filesystem deployment")]
    ForeignGeneration,
    /// A caller-pinned target generation is no longer current.
    #[error("workspace generation is stale")]
    StaleGeneration,
    /// The source identity or epoch changed before publication.
    #[error("workspace source snapshot is stale")]
    StaleSource,
    /// A conditional mutation targeted a different filesystem identity.
    #[error("workspace file identity is stale")]
    StaleIdentity,
    /// Canonical customer path was invalid.
    #[error("invalid workspace path: {0}")]
    Path(String),
    /// A typed path or Unicode projection failure.
    #[error("invalid workspace path: {0}")]
    NamespacePath(crate::kernel::NamespacePathError),
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
    /// An exact target generation is no longer the workspace's current head.
    #[error("workspace target generation is stale")]
    StaleTarget,
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
    pub(crate) fn path(error: impl fmt::Display) -> Self {
        Self::Path(error.to_string())
    }

    pub(crate) fn engine(error: impl std::error::Error + 'static) -> Self {
        if let Some(cause) = workspace_bounded_error(&error) {
            return cause;
        }
        if let Some(failure) =
            (&error as &dyn std::any::Any).downcast_ref::<crate::OperationFailure<FsError>>()
            && let Some(cause) = workspace_bounded_error(&failure.error)
        {
            return cause;
        }
        Self::Engine(error.to_string())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<crate::NativeTreeMaterializationError> for WorkspaceError {
    fn from(error: crate::NativeTreeMaterializationError) -> Self {
        FsError::from(error).into()
    }
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
impl From<crate::MaterializeError> for WorkspaceError {
    fn from(error: crate::MaterializeError) -> Self {
        match error {
            crate::MaterializeError::Work(error) => Self::Work(error),
            crate::MaterializeError::Cancelled(error) => Self::Cancelled(error),
            crate::MaterializeError::MissingPath => Self::NotFound,
            error => Self::Engine(error.to_string()),
        }
    }
}

/// Extracts quota and cancellation causes through transparent error wrappers.
pub(crate) fn workspace_bounded_error(
    error: &(dyn std::error::Error + 'static),
) -> Option<WorkspaceError> {
    if let Some(error) = error.downcast_ref::<crate::WorkError>() {
        return Some(WorkspaceError::Work(*error));
    }
    if error.is::<crate::CancellationError>() {
        return Some(WorkspaceError::Cancelled(crate::CancellationError));
    }
    if let Some(error) = error.downcast_ref::<Box<FsError>>() {
        return workspace_bounded_error(error.as_ref());
    }
    if let Some(WorkspaceError::WorkMerge {
        prior,
        backend,
        error,
    }) = error.downcast_ref::<WorkspaceError>()
    {
        return Some(WorkspaceError::WorkMerge {
            prior: *prior,
            backend: *backend,
            error: *error,
        });
    }
    macro_rules! bounded {
        ($kind:ty; [$($nested:ident),*]; [$($cancelled:ident),*]) => {{
            type Cause = $kind;
            if let Some(cause) = error.downcast_ref::<Cause>() {
                return match cause {
                    Cause::Work(cause) => Some(WorkspaceError::Work(*cause)),
                    $(Cause::$nested(cause) => workspace_bounded_error(cause),)*
                    $(Cause::$cancelled => Some(WorkspaceError::Cancelled(crate::CancellationError)),)*
                    _ => None,
                };
            }
        }};
    }
    bounded!(WorkspaceError; [Cancelled, Filesystem, NamespacePath]; []);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(FsError::NativeKernel(cause)) = error.downcast_ref::<FsError>() {
        return workspace_bounded_error(cause);
    }
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(crate::ObjectStoreError::Native(cause)) =
        error.downcast_ref::<crate::ObjectStoreError>()
    {
        return workspace_bounded_error(cause);
    }
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(crate::AuthorityStoreError::NativeOriginal(cause)) =
        error.downcast_ref::<crate::AuthorityStoreError>()
    {
        return workspace_bounded_error(cause);
    }
    #[cfg(not(target_arch = "wasm32"))]
    bounded!(crate::NativeTreeMaterializationError; [Io]; [Cancelled]);
    bounded!(FsError;
        [Cancelled, Authority, Object, Closure, Path, Directory, FileRecord, FileRead, BlobBuild,
         BlobRead, AttributeLookup, AttributeList, AttributeMutation, Probe, Rebase, Mutation,
         DetachedMutation, Checkpoint, Publication, MergeFileTable, MergeTree]; []);
    bounded!(crate::AuthorityStoreError; []; [Cancelled]);
    bounded!(crate::ObjectStoreError; []; [Cancelled]);
    bounded!(crate::kernel::GenerationMutationError; [Plan, Path, FileRecord, Tree, FileTable, Regular, Directory]; []);
    bounded!(crate::kernel::MutationPlanError; []; []);
    bounded!(crate::kernel::PathLookupError; [FileTable, Tree, NamespacePath, Dependency]; []);
    bounded!(crate::kernel::NamespacePathError; []; [Cancelled]);
    bounded!(crate::kernel::FileRecordReadError; [Storage]; [Cancelled]);
    bounded!(crate::kernel::TreeReadError; [Storage]; [Cancelled]);
    bounded!(crate::kernel::TreeMutationError; [Storage]; []);
    bounded!(crate::kernel::FileTableMutationError; [Storage]; []);
    bounded!(crate::kernel::RegularMutationError; [BlobBuild, BlobRead, Extent, Range, Clone, Storage]; []);
    bounded!(crate::kernel::ExtentMutationError; [Storage]; []);
    bounded!(crate::kernel::ExtentCloneError; [Source, Destination]; []);
    bounded!(crate::kernel::ExtentReadError; [Storage, Context]; [Cancelled]);
    bounded!(crate::kernel::BlobBuildError; [Source, Storage]; [Cancelled]);
    bounded!(crate::kernel::BlobReadError; [Storage, Context]; [Cancelled]);
    bounded!(crate::kernel::FileRangeReadError; [Extent, Blob, Context]; [Cancelled]);
    bounded!(crate::kernel::DirectoryReadError; [Storage]; [Cancelled]);
    bounded!(crate::kernel::AttributeLookupError; [Storage]; [Cancelled]);
    bounded!(crate::kernel::AttributeMutationError; [Storage]; []);
    bounded!(crate::kernel::AuthenticatedProbeError; [Storage, FileRecord, Tree, Directory, Extent, Blob]; [Cancelled]);
    bounded!(crate::kernel::RebaseError<crate::kernel::AuthenticatedProbeError>; [Probe]; [Cancelled]);
    bounded!(crate::kernel::CheckpointError; [Storage]; []);
    bounded!(crate::kernel::ClosureError; [Storage]; []);
    bounded!(crate::kernel::PublicationError; [Closure, Objects, Authority]; []);
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    bounded!(crate::DemandError; []; [Cancelled]);
    #[cfg(feature = "native-mount")]
    if let Some(error) = error.downcast_ref::<crate::native_mount::MountSourceError>() {
        use crate::native_mount::MountSourceError as Cause;
        return match error.semantic_error() {
            Cause::Work(cause) => Some(WorkspaceError::Work(*cause)),
            Cause::Cancelled => Some(WorkspaceError::Cancelled(crate::CancellationError)),
            Cause::Filesystem(cause) => workspace_bounded_error(cause),
            Cause::NamespacePath(cause) => workspace_bounded_error(cause),
            Cause::NativeWork(cause) => workspace_bounded_error(cause),
            _ => None,
        };
    }
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        return error
            .get_ref()
            .and_then(|cause| workspace_bounded_error(cause));
    }
    error.source().and_then(workspace_bounded_error)
}

impl From<crate::AuthorityStoreError> for WorkspaceError {
    fn from(error: crate::AuthorityStoreError) -> Self {
        Self::from(FsError::from(error))
    }
}

impl From<FsError> for WorkspaceError {
    fn from(error: FsError) -> Self {
        if let Some(cause) = workspace_bounded_error(&error) {
            return cause;
        }
        match error {
            FsError::Path(crate::kernel::PathLookupError::NamespacePath(error)) => error.into(),
            FsError::SourceSnapshotStale => Self::StaleSource,
            FsError::NotFound => Self::NotFound,
            FsError::NotDirectory => Self::NotDirectory,
            FsError::FileRead(crate::kernel::FileRangeReadError::NotRegular) => {
                Self::NotRegularFile
            }
            FsError::Mutation(crate::kernel::GenerationMutationError::FileIdentityConflict) => {
                Self::StaleIdentity
            }
            error => Self::Filesystem(error),
        }
    }
}

impl From<crate::kernel::NamespacePathError> for WorkspaceError {
    fn from(error: crate::kernel::NamespacePathError) -> Self {
        match error {
            crate::kernel::NamespacePathError::Work(error) => Self::Work(error),
            crate::kernel::NamespacePathError::Cancelled => {
                Self::Cancelled(crate::CancellationError)
            }
            error => Self::NamespacePath(error),
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

pub(crate) fn workspace_vec_bytes<T>(values: &Vec<T>) -> u64 {
    (values.capacity() * std::mem::size_of::<T>()) as u64
}

pub(crate) fn workspace_owned_strings_bytes(values: &Vec<String>) -> Result<u64, WorkspaceError> {
    let bytes = values
        .capacity()
        .checked_mul(std::mem::size_of::<String>())
        .ok_or(crate::WorkError::Overflow)?;
    values.iter().try_fold(
        u64::try_from(bytes).map_err(|_| crate::WorkError::Overflow)?,
        |bytes, value| {
            bytes
                .checked_add(
                    u64::try_from(value.capacity()).map_err(|_| crate::WorkError::Overflow)?,
                )
                .ok_or_else(|| crate::WorkError::Overflow.into())
        },
    )
}

pub(crate) fn customer_path_with_accounting(
    path: &str,
    config: crate::model::VolumeConfig,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(NamespacePath, KernelOwnedAllocation), WorkspaceError> {
    NamespacePath::parse_in_profile_with_accounting(
        path,
        config.profile,
        config.limits,
        accounting,
        cancellation,
    )
    .map_err(WorkspaceError::from)
}

async fn list<A: AsyncAuthorityStore, O: AsyncObjectStore, W: crate::facade::FsWork>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    after: Option<&LogicalName>,
    maximum: u32,
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<(crate::kernel::DirectoryPage, crate::AuthorityAllocation), WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let (mut checkout, path) = workspace
            .open_read_path(selector, path, &mut accounting, cancellation)
            .await?;
        let (page, allocation) = checkout
            .value
            .list_directory_with_accounting(
                &path.value,
                after.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                maximum,
                &mut accounting,
                cancellation,
            )
            .await?;
        let allocation = workspace_allocation(allocation, &accounting)?;
        Ok::<_, WorkspaceError>((page, allocation))
    }
    .await;
    work.finish(result, &accounting)
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Returns one authenticated bounded directory page and its result allocation.
    pub async fn list_directory<W: crate::facade::FsWork>(
        &self,
        path: &str,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(crate::kernel::DirectoryPage, crate::AuthorityAllocation), WorkspaceError> {
        list(
            self,
            GenerationSelector::Head,
            path,
            after,
            maximum_entries,
            work,
            cancellation,
        )
        .await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Returns one exact-generation directory page and its result allocation.
    pub fn list_directory<'a, W: crate::facade::FsWork + 'a>(
        &'a self,
        path: &'a str,
        after: Option<&'a LogicalName>,
        maximum_entries: u32,
        work: W,
        cancellation: &'a CancellationToken,
    ) -> impl Future<
        Output = W::Output<
            (crate::kernel::DirectoryPage, crate::AuthorityAllocation),
            WorkspaceError,
        >,
    > + ProviderTask
    + 'a {
        async move {
            list(
                &self.workspace,
                GenerationSelector::Exact(self.id),
                path,
                after,
                maximum_entries,
                work,
                cancellation,
            )
            .await
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Plans clipped sparse spans without reading file bodies.
    pub async fn plan_extents(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        maximum_spans: u32,
    ) -> Result<WorkspaceExtentPlan, WorkspaceError> {
        plan(
            self,
            GenerationSelector::Head,
            path,
            offset,
            length,
            maximum_spans,
        )
        .await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Plans clipped sparse spans from this immutable generation.
    pub async fn plan_extents(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        maximum_spans: u32,
    ) -> Result<WorkspaceExtentPlan, WorkspaceError> {
        plan(
            &self.workspace,
            GenerationSelector::Exact(self.id),
            path,
            offset,
            length,
            maximum_spans,
        )
        .await
    }
}

async fn plan<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    offset: u64,
    length: u64,
    maximum_spans: u32,
) -> Result<WorkspaceExtentPlan, WorkspaceError> {
    let mut checkout = workspace
        .checkout(
            selector,
            CheckoutMode::read_only_pinned(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
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
                .ok_or_else(|| WorkspaceError::Engine("file range overflows".to_owned()))?,
            kind: WorkspaceExtentKind::Content,
        }],
    };
    Ok(WorkspaceExtentPlan { spans })
}

pub(super) fn sorted_changed_file_ids(
    file_ids: impl IntoIterator<Item = FileId>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<FileId>, KernelOwnedAllocation), WorkspaceError> {
    let mut ids = (Vec::<FileId>::new(), accounting.claim_owned(0, 0)?);
    for file_id in file_ids {
        accounting.charge(crate::WorkCounters {
            items_examined: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let Err(low) = binary_search_with_accounting(&ids.0, accounting, |id| id.cmp(&file_id))?
        else {
            continue;
        };
        reverse_grow(&mut ids.0, &mut ids.1, accounting)?;
        let copied = crate::WorkCounters {
            bytes_copied: ((ids.0.len() - low + 1) * std::mem::size_of::<FileId>()) as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        ids.0.insert(low, file_id);
        accounting.record_completed(copied)?;
    }
    Ok(ids)
}

pub(super) fn reverse_drop<T>(
    owned: (T, KernelOwnedAllocation),
    bytes: u64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let (value, mut allocation) = owned;
    drop(value);
    accounting.release_owned(&mut allocation, bytes)
}

pub(super) fn reverse_grow<T>(
    values: &mut Vec<T>,
    allocation: &mut KernelOwnedAllocation,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    if values.len() < values.capacity() {
        return Ok(());
    }
    let copied = crate::WorkCounters {
        bytes_copied: values
            .len()
            .checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(crate::WorkError::Overflow)?,
        ..crate::WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    let bytes = workspace_vec_bytes(values);
    let mut owned = crate::kernel::Owned::new(
        std::mem::take(values),
        std::mem::replace(allocation, KernelOwnedAllocation::Standalone),
        bytes,
    );
    let result = owned.ensure_for_push(usize::MAX, accounting);
    *values = owned.value;
    *allocation = owned.allocation;
    result?;
    accounting.record_completed(copied)
}

fn reverse_insert(
    records: &mut NamespaceRecords,
    allocation: &mut KernelOwnedAllocation,
    path: (NamespacePath, KernelOwnedAllocation),
    record: crate::kernel::FileRecord,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    let position =
        binary_search_with_accounting(records, accounting, |entry| entry.0.cmp(&record.file_id))?;
    let (Ok(index) | Err(index)) = position;
    if position.is_err() {
        reverse_grow(records, allocation, accounting)?;
        accounting.charge(crate::WorkCounters {
            bytes_copied: ((records.len() - index)
                * std::mem::size_of::<(FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>)>())
                as u64,
            ..crate::WorkCounters::UNCHARGED
        })?;
        records.insert(index, (record.file_id, Vec::new()));
    }
    let entries = &mut records[index].1;
    reverse_grow(entries, allocation, accounting)?;
    let mut path = path;
    allocation.absorb(&mut path.1)?;
    entries.push((path.0, record));
    Ok(())
}

fn reverse_sort(
    records: &mut NamespaceRecords,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), FsError> {
    for (_, entries) in records {
        for index in 1..entries.len() {
            let mut current = index;
            while current != 0 {
                accounting.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                if entries[current - 1].0 <= entries[current].0 {
                    break;
                }
                accounting.charge(crate::WorkCounters {
                    bytes_copied: (2 * std::mem::size_of::<(
                        NamespacePath,
                        crate::kernel::FileRecord,
                    )>()) as u64,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                entries.swap(current - 1, current);
                current -= 1;
            }
        }
    }
    Ok(())
}

async fn namespace_records_to_file_root_with_accounting<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    volume: &Volume<A, O>,
    root: &crate::kernel::GenerationRoot,
    generation_root: crate::ObjectId,
    ids: &[FileId],
    maximum_entries: u32,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<(NamespaceRecordSearch, KernelOwnedAllocation), FsError> {
    accounting.check()?;
    let allocation = accounting.claim_owned(0, 0)?;
    let mut matches = (
        NamespaceRecordSearch {
            records: NamespaceRecords::default(),
            complete: ids.is_empty(),
        },
        allocation,
    );
    if ids.is_empty() || maximum_entries == 0 {
        return Ok(matches);
    }
    let limits = volume.config.limits;
    let generation = GenerationId::new(generation_root.digest);
    let cache_key = crate::path_index::cache_key(
        generation,
        root.root_file_id,
        root.file_table,
        ids,
        accounting,
    )?;
    let (bytes, allocation) = volume.fs.path_index().load(cache_key, accounting)?;
    let bytes = (bytes, allocation);
    if let Some(payload) = bytes.0.as_ref() {
        let (cached, allocation) =
            crate::path_index::decode(payload, generation, ids, limits, accounting)?;
        let mut cached = (cached, allocation);
        if let Some(paths) = cached
            .0
            .as_ref()
            .filter(|paths| paths.len() <= maximum_entries as usize)
        {
            let allocation = accounting.claim_owned(0, 0)?;
            let mut copied = (Vec::new(), allocation);
            for (_, path) in paths {
                reverse_grow(&mut copied.0, &mut copied.1, accounting)?;
                let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
                let mut path = (path, allocation);
                copied.1.absorb(&mut path.1)?;
                copied.0.push(path.0);
            }
            let (lookup, allocation) = crate::kernel::lookup_paths_async_with_accounting(
                volume.fs.objects(),
                root,
                &copied.0,
                volume.config(),
                accounting,
                cancellation,
                None,
            )
            .await?
            .into_parts();
            let lookup = (lookup, allocation);
            let mut valid = paths.len() == lookup.0.entries.len();
            for ((expected, _), entry) in paths.iter().zip(&lookup.0.entries) {
                accounting.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                if entry
                    .record
                    .is_none_or(|record| record.file_id != *expected)
                {
                    valid = false;
                    break;
                }
            }
            let copied_bytes = workspace_vec_bytes(&copied.0)
                + copied
                    .0
                    .iter()
                    .map(NamespacePath::retained_bytes)
                    .sum::<u64>();
            reverse_drop(copied, copied_bytes, accounting)?;
            let lookup_bytes = lookup.0.retained_allocation_bytes;
            if valid {
                let paths = cached.0.take().expect("verified cache payload");
                let vector_bytes = workspace_vec_bytes(&paths);
                for ((_, path), entry) in paths.into_iter().zip(&lookup.0.entries) {
                    let allocation =
                        crate::kernel::split_allocation(&mut cached.1, path.retained_bytes())?;
                    let path = (path, allocation);
                    let record = entry.record.ok_or(FsError::NotFound)?;
                    reverse_insert(
                        &mut matches.0.records,
                        &mut matches.1,
                        path,
                        record,
                        accounting,
                    )?;
                }
                accounting.release_owned(&mut cached.1, vector_bytes)?;
                reverse_drop(lookup, lookup_bytes, accounting)?;
                matches.0.complete = true;
                let bytes_count = bytes.0.as_ref().map_or(0, workspace_vec_bytes);
                reverse_drop(bytes, bytes_count, accounting)?;
                return Ok(matches);
            }
            reverse_drop(lookup, lookup_bytes, accounting)?;
        }
        let cached_bytes = cached.0.as_ref().map_or(0, |paths| {
            workspace_vec_bytes(paths)
                + paths
                    .iter()
                    .map(|(_, path)| path.retained_bytes())
                    .sum::<u64>()
        });
        reverse_drop(cached, cached_bytes, accounting)?;
    }
    let bytes_count = bytes.0.as_ref().map_or(0, workspace_vec_bytes);
    reverse_drop(bytes, bytes_count, accounting)?;
    let namespace_root = NamespacePath::new(Vec::new(), limits).map_err(FsError::from)?;
    if let Some(record) = crate::kernel::lookup_path_async_with_accounting(
        volume.fs.objects(),
        root,
        &namespace_root,
        volume.config(),
        accounting,
        cancellation,
        None,
    )
    .await?
    .record
    {
        if binary_search_with_accounting(ids, accounting, |id| id.cmp(&record.file_id))?.is_ok() {
            let allocation = accounting.claim_owned(0, 0)?;
            reverse_insert(
                &mut matches.0.records,
                &mut matches.1,
                (
                    NamespacePath::new(Vec::new(), limits).map_err(FsError::from)?,
                    allocation,
                ),
                record,
                accounting,
            )?;
        }
    }
    let allocation = accounting.claim_owned(0, 0)?;
    let mut pending = (Vec::new(), allocation);
    reverse_grow(&mut pending.0, &mut pending.1, accounting)?;
    let allocation = accounting.claim_owned(0, 0)?;
    pending.0.push((namespace_root, allocation));
    let mut examined = 0_u32;
    while let Some(directory) = pending.0.pop() {
        let mut after: Option<(LogicalName, KernelOwnedAllocation)> = None;
        loop {
            let (page, allocation) =
                crate::facade::list_directory_records_to_file_root_with_accounting(
                    volume,
                    root,
                    &directory.0,
                    after.as_ref().map(|name| &name.0),
                    maximum_entries
                        .saturating_sub(examined)
                        .saturating_add(1)
                        .min(1_024),
                    accounting,
                    cancellation,
                )
                .await?;
            let mut page = (page, allocation);
            for entry in &page.0.entries {
                accounting.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                if examined == maximum_entries {
                    reverse_sort(&mut matches.0.records, accounting)?;
                    let page_bytes = workspace_vec_bytes(&page.0.entries)
                        + page
                            .0
                            .entries
                            .iter()
                            .map(|entry| entry.name.retained_bytes() as u64)
                            .sum::<u64>();
                    reverse_drop(page, page_bytes, accounting)?;
                    if let Some(name) = after.take() {
                        let bytes = name.0.retained_bytes() as u64;
                        reverse_drop(name, bytes, accounting)?;
                    }
                    let bytes = directory.0.retained_bytes();
                    reverse_drop(directory, bytes, accounting)?;
                    while let Some(path) = pending.0.pop() {
                        let bytes = path.0.retained_bytes();
                        reverse_drop(path, bytes, accounting)?;
                    }
                    let bytes = workspace_vec_bytes(&pending.0);
                    reverse_drop(pending, bytes, accounting)?;
                    return Ok(matches);
                }
                examined += 1;
                let found = binary_search_with_accounting(ids, accounting, |id| {
                    id.cmp(&entry.record.file_id)
                })?
                .is_ok();
                let is_directory = entry.record.kind == FileKind::Directory;
                if !found && !is_directory {
                    continue;
                }
                let (path, allocation) = directory.0.child_with_accounting(
                    &entry.name,
                    limits,
                    accounting,
                    cancellation,
                )?;
                let path = (path, allocation);
                if found && is_directory {
                    let (copy, allocation) =
                        path.0.clone_with_accounting(accounting, cancellation)?;
                    reverse_insert(
                        &mut matches.0.records,
                        &mut matches.1,
                        (copy, allocation),
                        entry.record,
                        accounting,
                    )?;
                } else if found {
                    reverse_insert(
                        &mut matches.0.records,
                        &mut matches.1,
                        path,
                        entry.record,
                        accounting,
                    )?;
                    continue;
                }
                reverse_grow(&mut pending.0, &mut pending.1, accounting)?;
                pending.0.push(path);
            }
            let next = if page.0.has_more {
                let name = page.0.entries.pop().ok_or(FsError::NotFound)?.name;
                let allocation =
                    crate::kernel::split_allocation(&mut page.1, name.retained_bytes() as u64)?;
                Some((name, allocation))
            } else {
                None
            };
            let page_bytes = workspace_vec_bytes(&page.0.entries)
                + page
                    .0
                    .entries
                    .iter()
                    .map(|entry| entry.name.retained_bytes() as u64)
                    .sum::<u64>();
            reverse_drop(page, page_bytes, accounting)?;
            if let Some(old) = after.take() {
                let bytes = old.0.retained_bytes() as u64;
                reverse_drop(old, bytes, accounting)?;
            }
            after = next;
            if after.is_none() {
                break;
            }
        }
        let bytes = directory.0.retained_bytes();
        reverse_drop(directory, bytes, accounting)?;
    }
    let pending_bytes = workspace_vec_bytes(&pending.0);
    reverse_drop(pending, pending_bytes, accounting)?;
    reverse_sort(&mut matches.0.records, accounting)?;
    let (bytes, allocation) =
        crate::path_index::encode_records(generation, ids, &matches.0.records, accounting)?;
    let bytes = (bytes, allocation);
    if let Some(payload) = &bytes.0 {
        volume
            .fs
            .path_index()
            .store(cache_key, payload, accounting)?;
    }
    let bytes_count = bytes.0.as_ref().map_or(0, workspace_vec_bytes);
    reverse_drop(bytes, bytes_count, accounting)?;
    matches.0.complete = true;
    Ok(matches)
}

/// Reverse aliases of the actual supplied root, retaining their original allocation.
pub(crate) async fn reverse_paths_to_file_root_with_accounting<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
>(
    volume: &Volume<A, O>,
    root: &crate::kernel::GenerationRoot,
    generation_root: crate::ObjectId,
    file_id: FileId,
    maximum_entries: u32,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<(Vec<NamespacePath>, KernelOwnedAllocation), WorkspaceError> {
    let mut search = namespace_records_to_file_root_with_accounting(
        volume,
        root,
        generation_root,
        &[file_id],
        maximum_entries,
        accounting,
        cancellation,
    )
    .await?;
    let allocation = accounting.claim_owned(0, 0)?;
    let mut paths = (Vec::new(), allocation);
    let entries_bytes = workspace_vec_bytes(&search.0.records);
    for (_, records) in std::mem::take(&mut search.0.records) {
        let bytes = workspace_vec_bytes(&records);
        for (path, _) in records {
            reverse_grow(&mut paths.0, &mut paths.1, accounting)?;
            paths.0.push(path);
        }
        accounting.release_owned(&mut search.1, bytes)?;
    }
    accounting.release_owned(&mut search.1, entries_bytes)?;
    paths.1.absorb(&mut search.1)?;
    Ok((paths.0, paths.1))
}

pub(super) struct NamespaceRecordSearch {
    pub(super) records: NamespaceRecords,
    pub(super) complete: bool,
}

pub(super) type NamespaceRecords = Vec<(FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>)>;

pub(super) fn namespace_records_bytes(records: &NamespaceRecords) -> u64 {
    workspace_vec_bytes(records)
        + records
            .iter()
            .map(|(_, records)| {
                workspace_vec_bytes(records)
                    + records
                        .iter()
                        .map(|(path, _)| path.retained_bytes())
                        .sum::<u64>()
            })
            .sum::<u64>()
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Generation<A, O> {
    /// Finds paths for one stable file identity under supplied work limits.
    pub async fn paths_for_file_id(
        &self,
        file_id: FileId,
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Vec<String>>, WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let search = self
                .namespace_records_with_accounting(
                    &[file_id],
                    maximum_entries,
                    &mut accounting,
                    cancellation,
                )
                .await?;
            let mut paths = crate::kernel::Owned::allocate(0, &mut accounting)?;
            if let Some(records) =
                binary_search_with_accounting(&search.0.records, &mut accounting, |record| {
                    record.0.cmp(&file_id)
                })?
                .ok()
                .map(|index| &search.0.records[index].1)
            {
                for (path, _) in records {
                    paths.ensure_for_push(records.len(), &mut accounting)?;
                    let (text, mut allocation) =
                        path.unicode_text_with_accounting(&mut accounting, cancellation)?;
                    paths.allocation.absorb(&mut allocation)?;
                    paths.bytes = paths
                        .bytes
                        .checked_add(text.capacity() as u64)
                        .ok_or(crate::WorkError::Overflow)?;
                    paths.value.push(text);
                }
            }
            Ok(paths.value)
        }
        .await;
        crate::facade::measured(result, &accounting)
    }

    /// Finds portable paths for multiple stable file identities in one namespace traversal.
    pub async fn paths_for_file_ids(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
    ) -> Result<BTreeMap<FileId, Vec<String>>, WorkspaceError> {
        self.namespace_records_for_file_ids(file_ids, maximum_entries)
            .await?
            .records
            .into_iter()
            .map(|(file_id, records)| {
                records
                    .into_iter()
                    .map(|(path, _)| namespace_path_text(&path))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|paths| (file_id, paths))
            })
            .collect()
    }

    pub(super) async fn namespace_records_for_file_ids(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
    ) -> Result<NamespaceRecordSearch, WorkspaceError> {
        let cancellation = crate::CancellationToken::new();
        let mut accounting =
            KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
        let ids = sorted_changed_file_ids(file_ids, &mut accounting)?;
        self.namespace_records_with_accounting(
            &ids.0,
            maximum_entries,
            &mut accounting,
            &cancellation,
        )
        .await
        .map(|owned| owned.0)
    }

    pub(super) async fn namespace_records_with_accounting(
        &self,
        ids: &[FileId],
        maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(NamespaceRecordSearch, KernelOwnedAllocation), WorkspaceError> {
        accounting.check()?;
        if ids.is_empty() || maximum_entries == 0 {
            return Ok((
                NamespaceRecordSearch {
                    records: NamespaceRecords::default(),
                    complete: ids.is_empty(),
                },
                accounting.claim_owned(0, 0)?,
            ));
        }
        let checkout = self
            .workspace
            .open_checkout(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
                accounting,
                cancellation,
            )
            .await?;
        let generation_root = crate::ObjectId {
            kind: crate::ObjectKind::GenerationRoot,
            digest: self.id.digest(),
        };
        let result = namespace_records_to_file_root_with_accounting(
            &self.workspace.volume,
            checkout.value.root(),
            generation_root,
            ids,
            maximum_entries,
            accounting,
            cancellation,
        )
        .await;
        checkout.retire(accounting)?;
        result.map_err(WorkspaceError::from)
    }
}

fn changed_path_position(
    paths: &[ChangedPath],
    components: &[LogicalName],
    accounting: &mut KernelAccounting<'_>,
) -> Result<Result<usize, usize>, WorkspaceError> {
    binary_search_with_accounting(paths, accounting, |path| {
        path.path.components().cmp(components)
    })
    .map_err(WorkspaceError::from)
}

fn set_changed_record(
    paths: &mut [ChangedPath],
    index: usize,
    record: Option<crate::kernel::FileRecord>,
    before: bool,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    let copied = crate::WorkCounters {
        bytes_copied: std::mem::size_of::<Option<crate::kernel::FileRecord>>() as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    if before {
        paths[index].before = record;
    } else {
        paths[index].after = record;
    }
    accounting.record_completed(copied)?;
    Ok(())
}

fn insert_changed_record(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation),
    index: usize,
    mut path: (NamespacePath, KernelOwnedAllocation),
    record: Option<crate::kernel::FileRecord>,
    before: bool,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    let copied = crate::WorkCounters {
        bytes_copied: ((paths.0.len() - index + 1) * std::mem::size_of::<ChangedPath>()) as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    paths.1.absorb(&mut path.1)?;
    paths.0.insert(
        index,
        ChangedPath {
            path: path.0,
            before: if before { record } else { None },
            after: if before { None } else { record },
        },
    );
    accounting.record_completed(copied)?;
    Ok(())
}

fn merge_changed_record(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation),
    path: &NamespacePath,
    record: Option<crate::kernel::FileRecord>,
    before: bool,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<(), WorkspaceError> {
    let index = match changed_path_position(&paths.0, path.components(), accounting)? {
        Ok(index) => return set_changed_record(&mut paths.0, index, record, before, accounting),
        Err(index) => index,
    };
    reverse_grow(&mut paths.0, &mut paths.1, accounting)?;
    let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
    insert_changed_record(paths, index, (path, allocation), record, before, accounting)
}

fn merge_binding_endpoint(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation),
    directories: &NamespaceRecords,
    change: &crate::DirectoryBindingChange,
    limits: crate::model::VolumeLimits,
    before: bool,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<(), WorkspaceError> {
    let Some(parents) = binary_search_with_accounting(directories, accounting, |record| {
        record.0.cmp(&change.directory_id)
    })?
    .ok()
    .map(|index| &directories[index].1) else {
        return Ok(());
    };
    let binding = if before {
        change.before.as_ref()
    } else {
        change.after.as_ref()
    };
    let record = match binding {
        Some(entry) => binary_search_with_accounting(directories, accounting, |record| {
            record.0.cmp(&entry.file_id)
        })?
        .ok()
        .map(|index| &directories[index].1)
        .and_then(|records| records.first().map(|(_, record)| *record)),
        None => None,
    };
    for (directory, _) in parents {
        accounting.charge(crate::WorkCounters {
            items_examined: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let position = binary_search_with_accounting(&paths.0, accounting, |path| {
            path.path.components().iter().cmp(
                directory
                    .components()
                    .iter()
                    .chain(std::iter::once(&change.name)),
            )
        })?;
        match position {
            Ok(index) => set_changed_record(&mut paths.0, index, record, before, accounting)?,
            Err(index) => {
                reverse_grow(&mut paths.0, &mut paths.1, accounting)?;
                let (path, allocation) = directory.child_with_accounting(
                    &change.name,
                    limits,
                    accounting,
                    cancellation,
                )?;
                insert_changed_record(
                    paths,
                    index,
                    (path, allocation),
                    record,
                    before,
                    accounting,
                )?;
            }
        }
    }
    Ok(())
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ChangeSet<A, O> {
    /// Resolves changed paths while retaining their actual result allocation.
    pub async fn changed_paths<W: crate::facade::FsWork>(
        &self,
        maximum_entries: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(Vec<ChangedPath>, crate::AuthorityAllocation), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if let Some((allocations, spent, budget, _)) = accounting.standalone_parts() {
                *spent = self.work;
                spent.verify(budget)?;
                allocations
                    .claim_bytes(self.retained_bytes(), 0, spent, budget)
                    .map_err(workspace_allocation_error)?;
            }
            let (paths, allocation) = self
                .changed_paths_with_accounting(maximum_entries, &mut accounting, cancellation)
                .await?;
            let allocation = workspace_allocation(allocation, &accounting)?;
            Ok::<_, WorkspaceError>((paths, allocation))
        }
        .await;
        work.finish(result, &accounting)
    }

    async fn changed_paths_with_accounting(
        &self,
        maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(Vec<ChangedPath>, KernelOwnedAllocation), WorkspaceError> {
        accounting.check()?;
        if self.changes.truncated
            || (maximum_entries == 0
                && (!self.changes.files.is_empty() || !self.changes.bindings.is_empty()))
        {
            return Err(WorkspaceError::ChangedPathLimit);
        }
        let file_ids = sorted_changed_file_ids(
            self.changes
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
                })),
            accounting,
        )?;
        let mut paths = (Vec::new(), accounting.claim_owned(0, 0)?);
        for (generation, before) in [(&self.from, true), (&self.to, false)] {
            let search = generation
                .namespace_records_with_accounting(
                    &file_ids.0,
                    maximum_entries,
                    accounting,
                    cancellation,
                )
                .await?;
            if !search.0.complete {
                return Err(WorkspaceError::ChangedPathLimit);
            }
            for (_, records) in &search.0.records {
                for (path, record) in records {
                    accounting.charge(crate::WorkCounters {
                        items_examined: 1,
                        ..crate::WorkCounters::UNCHARGED
                    })?;
                    merge_changed_record(
                        &mut paths,
                        path,
                        Some(*record),
                        before,
                        accounting,
                        cancellation,
                    )?;
                }
            }
            for change in &self.changes.bindings {
                accounting.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                merge_binding_endpoint(
                    &mut paths,
                    &search.0.records,
                    change,
                    generation.workspace.volume.config.limits,
                    before,
                    accounting,
                    cancellation,
                )?;
            }
            let bytes = namespace_records_bytes(&search.0.records);
            reverse_drop(search, bytes, accounting)?;
        }
        let id_bytes = workspace_vec_bytes(&file_ids.0);
        reverse_drop(file_ids, id_bytes, accounting)?;
        let mut retained = 0;
        let mut retired_paths = 0_u64;
        for index in 0..paths.0.len() {
            accounting.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            let changed = match (&paths.0[index].before, &paths.0[index].after) {
                (Some(before), Some(after)) => {
                    before.kind != after.kind
                        || before.metadata != after.metadata
                        || (before.kind != FileKind::Directory && before.payload != after.payload)
                }
                (None, None) => false,
                _ => true,
            };
            if changed {
                if retained != index {
                    let copied = crate::WorkCounters {
                        bytes_copied: (2 * std::mem::size_of::<ChangedPath>()) as u64,
                        ..crate::WorkCounters::UNCHARGED
                    };
                    accounting.admit(copied)?;
                    paths.0.swap(retained, index);
                    accounting.record_completed(copied)?;
                }
                retained += 1;
            } else {
                retired_paths = retired_paths
                    .checked_add(paths.0[index].path.retained_bytes())
                    .ok_or(crate::WorkError::Overflow)?;
            }
        }
        paths.0.truncate(retained);
        accounting.release_owned(&mut paths.1, retired_paths)?;
        accounting.check()?;
        Ok(paths)
    }

    /// Computes the bounded net delta of two contiguous change sets.
    pub async fn compose(
        &self,
        next: &ChangeSet<A, O>,
        maximum_changes: u32,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<ChangeSet<A, O>>, WorkspaceError> {
        if self.to.id != next.from.id
            || self.to.workspace.id != next.from.workspace.id
            || !self
                .to
                .workspace
                .volume
                .fs
                .same_deployment(&next.from.workspace.volume.fs)
        {
            return Err(crate::OperationFailure::before_work(
                WorkspaceError::ChangeSetContinuity,
            ));
        }
        self.from
            .workspace
            .diff(&self.from, &next.to, maximum_changes, budget, cancellation)
            .await
    }
}

pub(super) fn restore_absolute_path(
    relative: &str,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
    cancellation.check()?;
    let length = relative
        .len()
        .checked_add(1)
        .ok_or(crate::WorkError::Overflow)?;
    let copied = crate::WorkCounters {
        bytes_copied: length as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    let mut path = crate::kernel::Owned::<Vec<u8>>::allocate(length, accounting)?;
    path.value.push(b'/');
    path.value.extend_from_slice(relative.as_bytes());
    let path = crate::kernel::Owned::new(
        String::from_utf8(path.value).map_err(WorkspaceError::engine)?,
        path.allocation,
        path.bytes,
    );
    if let Err(error) = accounting.record_completed(copied) {
        path.retire(accounting)?;
        return Err(error.into());
    }
    Ok(path)
}

pub(super) fn copy_workspace_label(
    identity: &WorkspaceName,
    accounting: &mut KernelAccounting<'_>,
) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
    let copied = crate::WorkCounters {
        bytes_copied: identity.retained_bytes(),
        ..crate::WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    let mut label = crate::kernel::Owned::<Vec<u8>>::allocate(identity.as_str().len(), accounting)?;
    accounting.admit(copied)?;
    label.value.extend_from_slice(identity.as_str().as_bytes());
    accounting.record_completed(copied)?;
    Ok(crate::kernel::Owned::new(
        String::from_utf8(label.value).map_err(WorkspaceError::engine)?,
        label.allocation,
        label.bytes,
    ))
}

pub(super) fn merge_workspace_backend<T, E: Into<WorkspaceError>>(
    result: crate::MeasuredResult<crate::OperationReceipt<T>, E>,
    work: &mut crate::WorkCounters,
    live: u64,
) -> Result<crate::OperationReceipt<T>, WorkspaceError> {
    let backend = match &result {
        Ok(receipt) => receipt.work,
        Err(failure) => *failure.work,
    };
    *work = work
        .with_backend(backend, live)
        .map_err(|error| WorkspaceError::WorkMerge {
            prior: *work,
            backend,
            error,
        })?;
    result.map_err(|failure| failure.error.into())
}

pub(super) fn workspace_allocation_error(
    error: crate::kernel::allocation::AllocationError,
) -> WorkspaceError {
    crate::collection::handle_allocation_error(error).into()
}

pub(super) fn workspace_allocation(
    allocation: KernelOwnedAllocation,
    accounting: &KernelAccounting<'_>,
) -> Result<crate::AuthorityAllocation, WorkspaceError> {
    match allocation {
        #[cfg(not(target_arch = "wasm32"))]
        KernelOwnedAllocation::NativeOriginal(allocation) => Ok(Some(allocation)),
        KernelOwnedAllocation::Standalone => {
            #[cfg(not(target_arch = "wasm32"))]
            if accounting.native_work().is_some() {
                return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
            }
            Ok(None)
        }
    }
}

fn validate_join_side_selections(
    conflicts: &[MergeConflict],
    truncated: bool,
    selections: &BTreeMap<MergeConflict, ConflictSide>,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<(), DrivenJoinError> {
    if truncated || conflicts.len() != selections.len() {
        return Err(DrivenJoinError::StaleCandidate);
    }
    for conflict in conflicts {
        let mut found = false;
        for key in selections.keys() {
            accounting
                .charge(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })
                .map_err(WorkspaceError::from)?;
            match key.cmp(conflict) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    found = true;
                    break;
                }
                std::cmp::Ordering::Greater => break,
            }
        }
        if !found {
            return Err(DrivenJoinError::StaleCandidate);
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn native_join_side_resolutions(
    selections: &BTreeMap<MergeConflict, ConflictSide>,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<
    crate::kernel::Owned<Vec<(MergeConflict, crate::kernel::MergeConflictResolution)>>,
    FsError,
> {
    let mut resolutions = crate::kernel::Owned::allocate(selections.len(), accounting)?;
    for (conflict, side) in selections {
        let conflict = match conflict {
            MergeConflict::File(id) => MergeConflict::File(*id),
            MergeConflict::Binding { directory_id, name } => {
                let (name, allocation) = name.clone_with_accounting(accounting, cancellation)?;
                let bytes = name.retained_bytes() as u64;
                let mut name = crate::kernel::Owned::new(name, allocation, bytes);
                let retained = resolutions
                    .bytes
                    .checked_add(bytes)
                    .ok_or(crate::WorkError::Overflow)?;
                resolutions.allocation.absorb(&mut name.allocation)?;
                resolutions.bytes = retained;
                MergeConflict::Binding {
                    directory_id: *directory_id,
                    name: name.value,
                }
            }
        };
        let side = match side {
            ConflictSide::Base => crate::kernel::MergeConflictSide::Base,
            ConflictSide::Ours => crate::kernel::MergeConflictSide::Ours,
            ConflictSide::Theirs => crate::kernel::MergeConflictSide::Theirs,
        };
        let copying = crate::WorkCounters {
            bytes_copied: std::mem::size_of::<(MergeConflict, crate::kernel::MergeConflictResolution)>(
            ) as u64,
            items_examined: 1,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copying)?;
        resolutions.value.push((
            conflict,
            crate::kernel::MergeConflictResolution::Select(side),
        ));
        accounting.record_completed(copying)?;
    }
    Ok(resolutions)
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

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    async fn restore_paths_from_root_with_accounting(
        &mut self,
        paths: &[String],
        mut source_root: crate::kernel::Owned<Checkout<A, O>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), WorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.checkout.validate_native_work(original)?;
        }
        let mut parsed = crate::kernel::Owned::allocate(paths.len(), accounting)?;
        for path in paths {
            cancellation.check()?;
            accounting.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            let absolute =
                restore_absolute_path(path.trim_start_matches('/'), accounting, cancellation)?;
            let parsed_path = self.mutation_path(&absolute.value, accounting, cancellation)?;
            absolute.retire(accounting)?;
            Self::push_mutation(&mut parsed, parsed_path, accounting)?;
        }
        let source_records = match source_root
            .value
            .lookup_batch_no_follow_with_accounting(&parsed.value, true, accounting, cancellation)
            .await
        {
            Ok((records, allocation)) => {
                let bytes = records.retained_allocation_bytes;
                crate::kernel::Owned::new(records, allocation, bytes)
            }
            Err(error) => {
                source_root.retire(accounting)?;
                return Err(error.into());
            }
        };
        let mut missing_count = 0;
        for entry in &source_records.value.entries {
            cancellation.check()?;
            accounting.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            missing_count += usize::from(entry.record.is_none());
        }
        let mut missing_paths = crate::kernel::Owned::allocate(missing_count, accounting)?;
        for (path, entry) in parsed.value.iter().zip(&source_records.value.entries) {
            cancellation.check()?;
            accounting.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            if entry.record.is_none() {
                let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
                let bytes = path.retained_bytes();
                Self::push_mutation(
                    &mut missing_paths,
                    crate::kernel::Owned::new(path, allocation, bytes),
                    accounting,
                )?;
            }
        }
        let current_records = if missing_paths.value.is_empty() {
            None
        } else {
            let (records, allocation) = self
                .checkout
                .lookup_batch_no_follow_with_accounting(
                    &missing_paths.value,
                    false,
                    accounting,
                    cancellation,
                )
                .await?;
            let bytes = records.retained_allocation_bytes;
            Some(crate::kernel::Owned::new(records, allocation, bytes))
        };
        let mut removal_count = 0;
        if let Some(records) = &current_records {
            for entry in &records.value.entries {
                cancellation.check()?;
                accounting.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                removal_count += usize::from(entry.record.is_some());
            }
        }
        let mut removals = crate::kernel::Owned::allocate(removal_count, accounting)?;
        if let Some(records) = &current_records {
            for (path, entry) in missing_paths.value.drain(..).zip(&records.value.entries) {
                cancellation.check()?;
                accounting.charge(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::UNCHARGED
                })?;
                let bytes = path.retained_bytes();
                let allocation =
                    crate::kernel::split_allocation(&mut missing_paths.allocation, bytes)?;
                let path = crate::kernel::Owned::new(path, allocation, bytes);
                missing_paths.bytes = missing_paths
                    .bytes
                    .checked_sub(bytes)
                    .ok_or(crate::WorkError::Overflow)?;
                if let Some(record) = entry.record {
                    Self::push_mutation(
                        &mut removals,
                        crate::kernel::Owned::new(
                            crate::kernel::Mutation::Remove {
                                path: path.value,
                                expected_file_id: MetadataField::Value(record.file_id),
                            },
                            path.allocation,
                            path.bytes,
                        ),
                        accounting,
                    )?;
                } else {
                    path.retire(accounting)?;
                }
            }
        }
        if removals.value.is_empty() {
            removals.retire(accounting)?;
        } else {
            self.checkout
                .mutate_owned_with_accounting(removals, accounting, cancellation)
                .await?;
        }
        missing_paths.retire(accounting)?;
        let mut operations = crate::kernel::Owned::allocate(paths.len(), accounting)?;
        for ((original_path, path), entry) in paths
            .iter()
            .zip(parsed.value.drain(..))
            .zip(&source_records.value.entries)
        {
            cancellation.check()?;
            accounting.charge(crate::WorkCounters {
                items_examined: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            let relative = original_path.trim_start_matches('/');
            let bytes = path.retained_bytes();
            let allocation = crate::kernel::split_allocation(&mut parsed.allocation, bytes)?;
            let path = crate::kernel::Owned::new(path, allocation, bytes);
            parsed.bytes = parsed
                .bytes
                .checked_sub(bytes)
                .ok_or(crate::WorkError::Overflow)?;
            if let Some(record) = entry.record {
                if record.kind == FileKind::Directory {
                    let destination = self
                        .checkout
                        .lookup_no_follow_with_accounting(
                            &path.value,
                            accounting,
                            cancellation,
                            true,
                        )
                        .await?
                        .0
                        .record;
                    let absolute = restore_absolute_path(relative, accounting, cancellation)?;
                    if destination.is_some_and(|record| record.kind != FileKind::Directory) {
                        self.remove_record(&absolute.value, accounting, cancellation)
                            .await?;
                    }
                    self.create_dirs(&absolute.value, accounting, cancellation)
                        .await?;
                    absolute.retire(accounting)?;
                }
                if record.kind != FileKind::Directory
                    && let Some((parent, _)) = relative.rsplit_once('/')
                {
                    let absolute = restore_absolute_path(parent, accounting, cancellation)?;
                    self.create_dirs(&absolute.value, accounting, cancellation)
                        .await?;
                    absolute.retire(accounting)?;
                }
                Self::push_mutation(
                    &mut operations,
                    crate::kernel::Owned::new(
                        crate::kernel::Mutation::Restore {
                            path: path.value,
                            record,
                        },
                        path.allocation,
                        path.bytes,
                    ),
                    accounting,
                )?;
            } else {
                path.retire(accounting)?;
            }
        }
        parsed.retire(accounting)?;
        if let Some(records) = current_records {
            records.retire(accounting)?;
        }
        if operations.value.is_empty() {
            operations.retire(accounting)?;
        } else {
            self.checkout
                .mutate_owned_with_accounting(operations, accounting, cancellation)
                .await?;
        }
        let records_result = source_records.retire(accounting);
        let root_result = source_root.retire(accounting);
        records_result
            .and(root_result)
            .map_err(WorkspaceError::from)
    }
}

pub(super) fn logical_name_text(
    name: &LogicalName,
) -> Result<std::borrow::Cow<'_, str>, WorkspaceError> {
    name.unicode_text().ok_or(WorkspaceError::NamespacePath(
        crate::kernel::NamespacePathError::NonUnicodeProjection,
    ))
}

pub(super) fn namespace_path_text(path: &NamespacePath) -> Result<String, WorkspaceError> {
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

#[cfg(test)]
#[path = "tests/workspace/mod.rs"]
mod tests;
