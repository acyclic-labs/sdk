//! Customer workspace identities and immutable generation handles.

use acyclic_stream::ProviderTask;
use crate::kernel::allocation::{KernelAccounting, KernelOwnedAllocation};
use crate::foundation::{FileId, GenerationId, OperationId, VolumeId};
use crate::CancellationToken;
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
use crate::performance::{MeasuredResult, OperationReceipt, WorkBudget};
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
#[derive(Clone)]
pub struct WorkspaceName(
    Arc<str>,
    #[cfg(not(target_arch = "wasm32"))] Option<Arc<(Arc<str>, crate::NativeAllocation)>>,
);

impl PartialEq for WorkspaceName {
    fn eq(&self, other: &Self) -> bool { self.as_str() == other.as_str() }
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

#[cfg(not(target_arch = "wasm32"))]
impl From<crate::NativeTreeMaterializationError> for WorkspaceError {
    fn from(error: crate::NativeTreeMaterializationError) -> Self {
        Self::from(FsError::NativeKernel(error))
    }
}

fn workspace_name_scratch_witness(
    name: &str,
    accounting: &mut KernelAccounting<'_>,
) -> Result<crate::WorkCounters, WorkspaceError> {
    let mut pending = 0_u64;
    let mut maximum_pending = 0_u64;
    let mut maximum_chunk = 0_u64;
    for character in name.chars() {
        accounting.check()?;
        accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        let mut chunk = 0_u64;
        let mut failure = None;
        unicode_normalization::char::decompose_canonical(character, |scalar| {
            if failure.is_some() { return; }
            let result = (|| -> Result<(), WorkspaceError> {
                accounting.check()?;
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                chunk = chunk.checked_add(1).ok_or(crate::WorkError::Overflow)?;
                if unicode_normalization::char::canonical_combining_class(scalar) == 0 {
                    pending = 0;
                } else {
                    pending = pending.checked_add(1).ok_or(crate::WorkError::Overflow)?;
                    maximum_pending = maximum_pending.max(pending);
                }
                Ok(())
            })();
            failure = result.err();
        });
        if let Some(error) = failure { return Err(error); }
        maximum_chunk = maximum_chunk.max(chunk);
    }
    // unicode-normalization 0.1.25 uses two four-element TinyVecs. NFD
    let buffer = |elements: u64, element_bytes: u64| -> Result<u64, WorkspaceError> {
        if elements <= 4 { return Ok(0); }
        elements.checked_next_power_of_two()
            .and_then(|capacity| capacity.checked_mul(2))
            .and_then(|capacity| capacity.checked_mul(element_bytes))
            .ok_or(crate::WorkError::Overflow.into())
    };
    let nfd_bytes = buffer(
        maximum_pending.checked_add(maximum_chunk).ok_or(crate::WorkError::Overflow)?,
        std::mem::size_of::<(u8, char)>() as u64,
    )?;
    let nfc_bytes = buffer(maximum_pending, std::mem::size_of::<char>() as u64)?;
    let sort_bytes = maximum_pending.checked_mul(std::mem::size_of::<(u8, char)>() as u64)
        .ok_or(crate::WorkError::Overflow)?;
    Ok(crate::WorkCounters {
        peak_allocation_bytes: nfd_bytes.checked_add(nfc_bytes).and_then(|n| n.checked_add(sort_bytes))
            .ok_or(crate::WorkError::Overflow)?,
        ..crate::WorkCounters::UNCHARGED
    })
}

fn accounting_live_bytes(accounting: &KernelAccounting<'_>) -> u64 {
    match accounting {
        KernelAccounting::Standalone { allocations, .. } => allocations.live_bytes(),
        KernelAccounting::BorrowedStandalone { allocations, .. } => allocations.live_bytes(),
        #[cfg(not(target_arch = "wasm32"))]
        KernelAccounting::NativeOriginal { work, .. } => work.live_allocation_bytes(),
    }
}

fn normalize_workspace_name(
    name: &str,
    accounting: &mut KernelAccounting<'_>,
    custody_bytes: u64,
) -> Result<(Arc<str>, KernelOwnedAllocation), WorkspaceError> {
    accounting.check()?;
    let witness = workspace_name_scratch_witness(name, accounting)?;
    accounting.admit(crate::WorkCounters {
        peak_allocation_bytes: accounting_live_bytes(accounting)
            .checked_add(witness.peak_allocation_bytes).ok_or(crate::WorkError::Overflow)?,
        ..crate::WorkCounters::UNCHARGED
    })?;
    let mut output = [0_u8; MAXIMUM_WORKSPACE_NAME_BYTES];
    let mut length = 0;
    {
        let mut normalized = name.nfc();
        loop {
            accounting.charge(crate::WorkCounters {
                items_examined: 1, ..crate::WorkCounters::UNCHARGED
            })?;
            let Some(character) = normalized.next() else { break };
            let end = length + character.len_utf8();
            if end > output.len() { return Err(WorkspaceNameError::TooLong.into()); }
            let copied = crate::WorkCounters {
                bytes_copied: character.len_utf8() as u64, ..crate::WorkCounters::UNCHARGED
            };
            accounting.admit(copied)?;
            character.encode_utf8(&mut output[length..end]);
            length = end;
            accounting.record_completed(copied)?;
        }
    }
    accounting.charge(crate::WorkCounters {
        items_examined: length as u64, ..crate::WorkCounters::UNCHARGED
    })?;
    let canonical = std::str::from_utf8(&output[..length]).map_err(WorkspaceError::engine)?;
    if canonical.is_empty() { return Err(WorkspaceNameError::Empty.into()); }
    if canonical == "." || canonical == ".." { return Err(WorkspaceNameError::Reserved.into()); }
    for character in canonical.chars() {
        accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        if character.is_control() || matches!(character, '/' | '\\') {
            return Err(WorkspaceNameError::InvalidCharacter.into());
        }
    }
    let copied = crate::WorkCounters {
        bytes_copied: length as u64, ..crate::WorkCounters::UNCHARGED
    };
    accounting.admit(crate::WorkCounters {
        allocation_operations: 1 + u64::from(custody_bytes != 0),
        peak_allocation_bytes: accounting_live_bytes(accounting).checked_add(length as u64)
            .and_then(|bytes| bytes.checked_add(custody_bytes)).ok_or(crate::WorkError::Overflow)?,
        ..copied
    })?;
    let mut allocation = accounting.claim_owned(length as u64, 1)?;
    let name: Arc<str> = Arc::from(canonical);
    let completed = accounting.record_completed(copied).and_then(|()| {
        if custody_bytes != 0 {
            allocation.absorb(&mut accounting.claim_owned(custody_bytes, 1)?)?;
        }
        Ok(())
    });
    if let Err(error) = completed {
        drop(name);
        accounting.release_owned(&mut allocation, length as u64)?;
        return Err(error.into());
    }
    Ok((name, allocation))
}

impl WorkspaceName {
    /// Canonicalizes and validates one friendly workspace name.
    pub fn new(name: impl AsRef<str>) -> Result<Self, WorkspaceNameError> {
        static UNBOUNDED_CANCELLATION: std::sync::LazyLock<crate::CancellationToken>
            = std::sync::LazyLock::new(crate::CancellationToken::new);
        Self::new_measured(
            name, crate::WorkBudget::UNBOUNDED,
            &UNBOUNDED_CANCELLATION,
        )
            .map(|receipt| receipt.value)
            .map_err(|failure| match failure.error {
                WorkspaceError::Name(error) => error,
                _ => WorkspaceNameError::Allocation,
            })
    }

    pub(crate) fn new_measured(
        name: impl AsRef<str>,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Self>, WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = normalize_workspace_name(name.as_ref(), &mut accounting, 0)
            .map(|(name, _allocation)| Self(name, #[cfg(not(target_arch = "wasm32"))] None));
        crate::facade::measured(result, &accounting)
    }

    /// Canonicalizes one name on the native invocation's original work ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_with_native_work(
        name: &str,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Self, WorkspaceError> {
        cancellation.check()?;
        let (name, allocation) = normalize_workspace_name(
            name, &mut KernelAccounting::native(original, cancellation), std::mem::size_of::<(Arc<str>, crate::NativeAllocation)>() as u64,
        )?;
        let KernelOwnedAllocation::NativeOriginal(allocation) = allocation else { unreachable!() };
        Ok(Self(Arc::clone(&name), Some(Arc::new((name, allocation)))))
    }

    pub(crate) fn retained_bytes(&self) -> u64 {
        self.as_str().len() as u64
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

    pub(crate) fn derive_measured(
        namespace: [u8; 16],
        name: &WorkspaceName,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Self>, WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = Self::derive_with_accounting(namespace, name, &mut accounting, cancellation);
        crate::facade::measured(result, &accounting)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn derive_with_native_work(
        namespace: [u8; 16],
        name: &WorkspaceName,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Self, WorkspaceError> {
        cancellation.check()?;
        original.cancellation().check()?;
        Self::derive_with_accounting(
            namespace, name, &mut KernelAccounting::native(original, cancellation), cancellation,
        )
    }

    fn derive_with_accounting(
        namespace: [u8; 16],
        name: &WorkspaceName,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<Self, WorkspaceError> {
        cancellation.check()?;
        let hashed = WORKSPACE_ID_DOMAIN.len().checked_add(namespace.len())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<u64>()))
            .and_then(|bytes| bytes.checked_add(name.as_str().len()))
            .ok_or(crate::WorkError::Overflow)?;
        let performed = crate::WorkCounters {
            bytes_hashed: u64::try_from(hashed).map_err(|_| crate::WorkError::Overflow)?,
            bytes_copied: 16,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(performed)?;
        let id = Self::derive(namespace, name);
        accounting.record_completed(performed)?;
        Ok(id)
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

    /// Authenticated immutable configuration of this workspace's actual volume.
    #[must_use]
    pub const fn volume_config(&self) -> crate::model::VolumeConfig {
        self.volume.config
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Workspace<A, O> {
    /// Reserves the original authority head with one cumulative work allowance.
    pub async fn reserve_publication(
        &self,
        operation_id: OperationId,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::PublicationReservation>, WorkspaceError> {
        cancellation.check()
            .map_err(|error| crate::OperationFailure::before_work(WorkspaceError::from(error)))?;
        let head = self.authority().head(
            self.authority_id(), budget, cancellation,
        ).await.map_err(|failure| failure.map_with_prior_work(
            crate::WorkCounters::UNCHARGED, |error| WorkspaceError::from(FsError::Authority(error)),
        ))?;
        let reserved = self.authority().reserve_publication(
            self.authority_id(), head.value, operation_id,
            head.work.remaining(budget)
                .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), head.work))?,
            cancellation,
        ).await.map_err(|failure| failure.map_with_prior_work(
            head.work, |error| WorkspaceError::from(FsError::Authority(error)),
        ))?;
        let work = head.work.checked_add(reserved.work)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), head.work))?;
        match reserved.value {
            crate::ReservationOutcome::Reserved(reservation)
            | crate::ReservationOutcome::AlreadyReserved(reservation) => {
                if reservation.authority_id != self.authority_id()
                    || reservation.operation_id != operation_id
                {
                    return Err(crate::OperationFailure::new(WorkspaceError::IncompatibleWorkspace, work));
                }
                Ok(crate::OperationReceipt { value: reservation, work })
            }
            crate::ReservationOutcome::Conflict { .. } => {
                Err(crate::OperationFailure::new(WorkspaceError::StaleGeneration, work))
            }
        }
    }

    /// Releases only the actual reservation using the caller's cleanup token.
    pub async fn release_publication(
        &self,
        reservation: crate::PublicationReservation,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<()>, WorkspaceError> {
        if reservation.authority_id != self.authority_id() {
            return Err(crate::OperationFailure::before_work(WorkspaceError::IncompatibleWorkspace));
        }
        cancellation.check()
            .map_err(|error| crate::OperationFailure::before_work(WorkspaceError::from(error)))?;
        self.authority().release_publication(reservation, budget, cancellation).await
            .map(|receipt| crate::OperationReceipt { value: receipt.value, work: receipt.work })
            .map_err(|failure| failure.map_with_prior_work(
                crate::WorkCounters::UNCHARGED, |error| WorkspaceError::from(FsError::Authority(error)),
            ))
    }



    /// Acquires the original proof; recovery owns every check after acquisition.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn reserve_publication_with_native_work(
        &self, operation_id: OperationId, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::PublicationReservation, (WorkspaceError, Option<crate::NativeAllocation>)> {
        let result = async {
            original.admit(crate::WorkCounters::UNCHARGED).map_err(WorkspaceError::from)?;
            cancellation.check().map_err(WorkspaceError::from)?;
            let authority_id = self.authority_id();
            let head = self.authority().head_with_native_work(authority_id, original, cancellation)
                .await.map_err(|error| WorkspaceError::from(FsError::Authority(error)))?;
            let reservation = self.authority().reserve_publication_with_native_work(
                authority_id, head, operation_id, original, cancellation,
            ).await.map_err(|error| WorkspaceError::from(FsError::Authority(error)))?;
            if reservation.authority_id != authority_id || reservation.operation_id != operation_id {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            Ok(reservation)
        }.await;
        result.map_err(|error| (error, None))
    }

    /// Queries only the durable proof belonging to this exact operation.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn publication_reservation_with_native_work(
        &self, operation_id: OperationId, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Option<crate::PublicationReservation>, (WorkspaceError, Option<crate::NativeAllocation>)> {
        let result = async {
            original.admit(crate::WorkCounters::UNCHARGED).map_err(WorkspaceError::from)?;
            cancellation.check().map_err(WorkspaceError::from)?;
            let reservation = self.authority().publication_reservation_with_native_work(
                self.authority_id(), operation_id, original, cancellation,
            ).await.map_err(|error| WorkspaceError::from(FsError::Authority(error)))?;
            if let Some(proof) = reservation
                && (proof.authority_id != self.authority_id() || proof.operation_id != operation_id)
            {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            Ok(reservation)
        }.await;
        result.map_err(|error| (error, None))
    }

    /// Releases the retained original proof without adopting a newer head.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn release_publication_with_native_work(
        &self, reservation: crate::PublicationReservation, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), (WorkspaceError, Option<crate::NativeAllocation>)> {
        let result = async {
            if reservation.authority_id != self.authority_id() {
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            original.admit(crate::WorkCounters::UNCHARGED).map_err(WorkspaceError::from)?;
            cancellation.check().map_err(WorkspaceError::from)?;
            self.authority().release_publication_with_native_work(reservation, original, cancellation)
                .await.map_err(|error| WorkspaceError::from(FsError::Authority(error)))
        }.await;
        result.map_err(|error| (error, None))
    }

    /// Resolves an original committed generation without adopting the mutable
    pub async fn operation_generation(
        &self,
        idempotency_key: IdempotencyKey,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Option<Generation<A, O>>>, WorkspaceError>
    {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.volume.fs.workspace_operation_generation(
            &self.volume, idempotency_key.operation_id(), &mut accounting, cancellation,
        ).await.map(|(value, allocation)| { drop(allocation); value });
        let receipt = crate::facade::measured(result, &accounting)?;
        Ok(crate::OperationReceipt {
            value: receipt.value.map(|(id, record)| { drop(record); Generation { workspace: self.clone(), id, publication_closure: None } }),
            work: receipt.work,
        })
    }

    /// Returns the protected original operation record with its genuine
    pub(crate) async fn operation_generation_record_measured(
        &self, key: IdempotencyKey, budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Option<(Generation<A, O>, crate::DurableCommit)>>, WorkspaceError> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.volume.fs.workspace_operation_generation(
            &self.volume, key.operation_id(), &mut accounting, cancellation,
        ).await.map(|(value, allocation)| { drop(allocation); value });
        let receipt = crate::facade::measured(result, &accounting)?;
        Ok(crate::OperationReceipt {
            value: receipt.value.map(|(id, record)| (
                Generation { workspace: self.clone(), id, publication_closure: None },
                record,
            )),
            work: receipt.work,
        })
    }

    /// Resolves the exact original operation without adopting the current head.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn operation_generation_with_native_work(
        &self,
        operation_id: crate::OperationId,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Option<Generation<A, O>>, (WorkspaceError, Option<crate::NativeAllocation>)> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        let mut observed = self.volume.fs.workspace_operation_generation(
            &self.volume, operation_id, &mut accounting, cancellation,
        ).await.map_err(|error| (error, None))?;
        let id = observed.0.take().map(|(id, record)| { drop(record); id });
        drop(observed);
        Ok(id.map(|id| Generation {
            workspace: self.clone(), id, publication_closure: None,
        }))
    }

    /// Keeps original record bytes ahead of their metadata allocation until
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn operation_generation_record_with_native_work(
        &self, operation_id: crate::OperationId,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<
        (Option<(Generation<A, O>, crate::DurableCommit)>, crate::NativeAllocation),
        (WorkspaceError, Option<crate::NativeAllocation>),
    > {
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        let observed = self.volume.fs.workspace_operation_generation(
            &self.volume, operation_id, &mut accounting, cancellation,
        ).await.map_err(|error| (error, None))?;
        let crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = observed.1
            else { unreachable!("native operation retains its allocation"); };
        Ok((observed.0.map(|(id, record)| (
            Generation { workspace: self.clone(), id, publication_closure: None },
            record,
        )), allocation))
    }

    /// Proves an immutable result was committed by this workspace's exact
    pub async fn verify_join_commit(
        &self,
        witness: &crate::JoinCommitWitness,
    ) -> Result<bool, WorkspaceError> {
        self.volume
            .fs
            .verify_workspace_join_commit(&self.volume, witness, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Opens the same canonical checkout with exact success and failure work.
    pub async fn checkout(
        &self,
        selector: GenerationSelector,
        mode: CheckoutMode,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Checkout<A, O>>, WorkspaceError> {
        self.volume.checkout(selector, mode, budget, cancellation).await
            .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::default(), WorkspaceError::from))
    }

    /// Returns the attached native source, when this handle was created by
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[must_use]
    pub fn source(&self) -> Option<&crate::Source<A, O>> {
        self.source.as_ref()
    }

    /// Captures all remaining source state and returns an independent exact
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    pub async fn seal(&self) -> Result<Generation<A, O>, crate::SourceError> {
        match &self.source {
            Some(source) => source.seal().await,
            None => self.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                .map_err(|error| crate::SourceError::Engine(error.to_string())),
        }
    }
    /// Pins the authenticated head under explicit work and cancellation limits.
    pub async fn head(
        &self,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Generation<A, O>>, WorkspaceError> {
        let checkout = self.checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned(), budget, cancellation).await?;
        Ok(crate::OperationReceipt {
            value: Generation { workspace: self.clone(), id: checkout.value.generation_id(), publication_closure: None },
            work: checkout.work,
        })
    }

    /// Pins the authenticated head using the invocation's original native work.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn head_with_native_work(
        &self, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Generation<A, O>, (WorkspaceError, Option<crate::NativeAllocation>)> {
        self.head_with_native_work_value(original, cancellation).await
            .map_err(|error| (error, None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn head_with_native_work_value(
        &self, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Generation<A, O>, WorkspaceError> {
        let checkout = self.volume.checkout_with_native_work(
            GenerationSelector::Head, CheckoutMode::read_only_pinned(), original, cancellation,
        ).await.map_err(WorkspaceError::from)?;
        Ok(Generation { workspace: self.clone(), id: checkout.generation_id(), publication_closure: None })
    }

    /// Selects the head generation and reports whether its file table holds
    pub(crate) async fn head_with_file_record_measured(
        &self,
        file_id: FileId,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<(Generation<A, O>, bool)>, WorkspaceError> {
        let checkout = self.checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned(), budget, cancellation).await?;
        let reader = checkout.value.pinned_reader()
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::engine(error), checkout.work))?;
        let prior = checkout.work;
        drop(checkout.value);
        let remaining = prior.remaining(budget)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
        let records = reader.file_records_by_id(&[file_id], remaining, cancellation).await
            .map_err(|failure| failure.map_with_prior_work(prior, WorkspaceError::from))?;
        let work = prior.checked_add(records.work)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
        Ok(crate::OperationReceipt {
            value: (
                Generation { workspace: self.clone(), id: reader.generation_id(), publication_closure: None },
                records.value.first().is_some_and(Option::is_some),
            ),
            work,
        })
    }

    /// Authenticates one exact generation under explicit work and cancellation
    pub async fn generation(
        &self,
        generation_id: GenerationId,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Generation<A, O>>, WorkspaceError> {
        let checkout = self.checkout(GenerationSelector::Exact(generation_id), CheckoutMode::read_only_pinned(), budget, cancellation).await?;
        Ok(crate::OperationReceipt {
            value: Generation { workspace: self.clone(), id: checkout.value.generation_id(), publication_closure: None },
            work: checkout.work,
        })
    }

    /// Authenticates an exact generation on the caller's original native scope.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn generation_with_native_work(
        &self,
        generation_id: GenerationId,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Generation<A, O>, (WorkspaceError, Option<crate::NativeAllocation>)> {
        let checkout = self.volume.checkout_with_native_work(
            GenerationSelector::Exact(generation_id), CheckoutMode::read_only_pinned(),
            original, cancellation,
        ).await.map_err(|error| (WorkspaceError::from(error), None))?;
        Ok(Generation {
            workspace: self.clone(), id: checkout.generation_id(), publication_closure: None,
        })
    }

    /// Atomically moves this workspace head to one of its own exact immutable
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
        let restored = |id| Generation { workspace: self.clone(), id, publication_closure: None };
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

    /// Restores selected records through the original publication engine with
    pub async fn restore_paths_from<F, Fut>(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError>
    where
        F: FnMut(crate::WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        self.restore_path_records(
            source, paths, if_current, idempotency_key, permit, budget, cancellation,
            false, before_publication,
        ).await
    }

    /// Restores a caller-expanded selected subtree record union with one seal.
    pub async fn restore_selected_paths_from<F, Fut>(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError>
    where
        F: FnMut(crate::WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        self.restore_path_records(
            source, paths, if_current, idempotency_key, permit, budget, cancellation,
            true, before_publication,
        ).await
    }

    /// Restores the caller-expanded frozen selection at the original native seal.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn restore_selected_paths_from_with_permit_guarded_with_native_work<'original, E, F, Fut>(
        &self, source: &Generation<A, O>, paths: &[String], if_current: GenerationId,
        idempotency_key: IdempotencyKey, permit: crate::PublicationPermit,
        original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken, before_publication: &mut F,
    ) -> Result<TransactionCommit<A, O>, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        if !self.volume.fs.same_deployment(&source.workspace.volume.fs) {
            return Err(E::from(WorkspaceError::ForeignGeneration));
        }
        let current = self.head_with_native_work_value(original, cancellation).await.map_err(E::from)?;
        if current.id != if_current {
            return Ok(TransactionCommit::Conflict { actual: current });
        }
        let mut transaction = self.begin_pinned_transaction_with_native_work_value(
            &current, idempotency_key, original, cancellation,
        ).await.map_err(E::from)?;
        transaction.restore_selected_paths_from_with_native_work(
            source, paths, original, cancellation,
        ).await.map_err(E::from)?;
        transaction.commit_with_native_work(
            None, permit, original, cancellation, before_publication,
        ).await
    }

    async fn restore_path_records<F, Fut>(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        selected_subtrees: bool,
        mut before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError>
    where
        F: FnMut(crate::WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::FsResult<()>> + ProviderTask,
    {
        crate::heap_future::in_heap_measured(move |budget| async move {
            let mut work = crate::WorkCounters::UNCHARGED;
            let mut allocations = crate::kernel::allocation::AllocationLedger::default();
            let result = async {
                if !self.volume.fs.same_deployment(&source.workspace.volume.fs) {
                    return Err(WorkspaceError::ForeignGeneration);
                }
                let current = merge_workspace_backend(
                    self.head(budget, cancellation).await, &mut work, 0,
                )?.value;
                if current.id != if_current {
                    return Ok(TransactionCommit::Conflict { actual: current });
                }
                let source_root = {
                    let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                        &mut allocations, &mut work, budget, cancellation,
                    );
                    self.restore_source_root_with_accounting(source, &mut accounting, cancellation).await?
                };
                let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                let mut transaction = merge_workspace_backend(
                    self.begin_pinned_transaction_measured(
                        &current, idempotency_key, allowance, cancellation,
                    ).await, &mut work, allocations.live_bytes(),
                )?.value;
                allocations.claim_bytes(transaction.checkout.logical_owned_bytes(), 0, &mut work, budget)
                    .map_err(workspace_allocation_error)?;
                {
                    let mut accounting = crate::kernel::allocation::KernelAccounting::borrowed_standalone(
                        &mut allocations, &mut work, budget, cancellation,
                    );
                    transaction.restore_paths_from_root_with_accounting(
                        source, paths, selected_subtrees, source_root, &mut accounting, cancellation,
                    ).await?;
                }
                let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                let committed = merge_workspace_backend(
                    transaction.commit(None, permit, allowance, cancellation, |budget| {
                            let future = before_publication(budget);
                            async move { future.await.map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::UNCHARGED, WorkspaceError::from)) }
                        }).await, &mut work, allocations.live_bytes(),
                )?;
                Ok(committed.value)
            }.await;
            work.verify(budget)
                .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
            result.map(|value| crate::OperationReceipt { value, work })
                .map_err(|error| crate::OperationFailure::new(error, work))
        }, budget, cancellation, WorkspaceError::Cancelled(crate::CancellationError), None).await
    }



    fn restore_absolute_path_with_accounting(
        relative: &str,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
        cancellation.check()?;
        let length = relative.len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
        let requested = crate::foundation::usize_to_u64(length);
        let copied = crate::WorkCounters {
            bytes_copied: requested, ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        let mut allocation = accounting.claim_owned(requested, 1)?;
        let mut absolute = String::new();
        if let Err(error) = absolute.try_reserve_exact(length) {
            drop(absolute);
            #[cfg(not(target_arch = "wasm32"))]
            let native = matches!(&allocation,
                crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(_));
            accounting.release_owned(&mut allocation, requested)?;
            #[cfg(not(target_arch = "wasm32"))]
            if native {
                return Err(crate::NativeTreeMaterializationError::AllocationFailed(error).into());
            }
            drop(error);
            return Err(WorkspaceError::from(FsError::Decode(
                crate::kernel::CanonicalDecodeError::AllocationFailed,
            )));
        }
        let actual = crate::foundation::usize_to_u64(absolute.capacity());
        let checked = accounting.reconcile_owned(&mut allocation, requested, actual);
        let mut absolute = crate::kernel::Owned::new(absolute, allocation, actual);
        if let Err(error) = checked {
            absolute.retire(accounting)?;
            return Err(error.into());
        }
        // Both inputs are valid UTF-8; these infallible copies retain that invariant.
        absolute.value.push('/');
        absolute.value.push_str(relative);
        if let Err(error) = accounting.record_completed(copied) {
            absolute.retire(accounting)?;
            return Err(error.into());
        }
        Ok(absolute)
    }

    async fn open_checkout(
        &self, selector: GenerationSelector, mode: CheckoutMode,
        accounting: &mut KernelAccounting<'_>, cancellation: &crate::CancellationToken,
    ) -> Result<crate::kernel::Owned<Checkout<A, O>>, WorkspaceError> {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let checkout = self.volume.checkout_with_native_work(selector, mode, original, cancellation)
                .await.map_err(WorkspaceError::from)?;
            return Ok(crate::kernel::Owned::new(checkout, accounting.claim_owned(0, 0)?, 0));
        }
        let opened = self.volume.checkout(selector, mode, accounting.remaining()?, cancellation).await;
        let (allocations, work, budget, _) = accounting.standalone_parts().ok_or(crate::WorkError::Overflow)?;
        let checkout = merge_workspace_backend(opened, work, allocations.live_bytes())?.value;
        work.verify(budget)?;
        let bytes = checkout.logical_owned_bytes();
        let allocation = accounting.claim_owned(bytes, 0)?;
        Ok(crate::kernel::Owned::new(checkout, allocation, bytes))
    }

    async fn open_read_path(
        &self, selector: GenerationSelector, path: &str,
        accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken,
    ) -> Result<(crate::kernel::Owned<Checkout<A, O>>, crate::kernel::Owned<NamespacePath>), WorkspaceError> {
        let checkout = self.open_checkout(selector, CheckoutMode::read_only_pinned(), accounting, cancellation).await?;
        let (path, allocation) = customer_path_with_accounting(path, checkout.value.volume_config(), accounting, cancellation)?;
        let bytes = path.retained_bytes();
        Ok((checkout, crate::kernel::Owned::new(path, allocation, bytes)))
    }

    async fn restore_source_root_with_accounting(
        &self, source: &Generation<A, O>, accounting: &mut KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::kernel::Owned<Checkout<A, O>>, WorkspaceError> {
        if !self.volume.fs.same_deployment(&source.workspace.volume.fs) {
            return Err(WorkspaceError::ForeignGeneration);
        }
        source.workspace.open_checkout(
            GenerationSelector::Exact(source.id), CheckoutMode::read_only_pinned(), accounting, cancellation,
        ).await
    }

    /// Applies only the paths changed between `base` and `source`, rejecting
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

    /// Whether a path holds the same thing in two states: the same record,
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
        let current = self.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        if current.id != if_current {
            return Ok(WorkspacePathApply::Stale(current));
        }
        let mut base_checkout = match base {
            Some(base) => Some(
                base.workspace
                    .checkout(
                        GenerationSelector::Exact(base.id),
                        CheckoutMode::read_only_pinned(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
            ),
            None => None,
        };
        let mut source_checkout = match source {
            Some(source) => Some(
                source
                    .workspace
                    .checkout(
                        GenerationSelector::Exact(source.id),
                        CheckoutMode::read_only_pinned(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?,
            ),
            None => None,
        };
        let mut transaction = self.begin_transaction(idempotency_key, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
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
                .ok_or_else(|| WorkspaceError::manual_engine("missing batch lookup result"))?;
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
                        transaction.create_dir_all(&format!("/{parent}"), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
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
        Ok(match transaction.commit(None, permit, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)? {
            TransactionCommit::Committed(generation) => WorkspacePathApply::Applied(generation),
            TransactionCommit::AlreadyCommitted(generation) => {
                WorkspacePathApply::AlreadyApplied(generation)
            }
            TransactionCommit::Conflict { actual } => WorkspacePathApply::Stale(actual),
            TransactionCommit::Fenced => WorkspacePathApply::Fenced,
            TransactionCommit::IdempotencyConflict => WorkspacePathApply::IdempotencyConflict,
        })
    }

    /// Forks one exact generation with bounded work and a publication guard.
    pub async fn fork<F, Fut>(
        &self, destination: impl AsRef<str>, options: ForkOptions<A, O>,
        permit: crate::PublicationPermit, budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken, mut guard: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Self>, WorkspaceError>
    where
        F: FnMut(crate::WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::facade::FsResult<()>> + ProviderTask,
    {
        crate::heap_future::in_heap_measured(|budget| async move {
            let mut prior = crate::WorkCounters::UNCHARGED;
            let mut allocations = crate::kernel::allocation::AllocationLedger::default();
            let path_bytes = options.paths.as_ref().map_or(Ok(0), |paths| workspace_owned_strings_bytes(paths))
                .map_err(|error| crate::OperationFailure::new(error, prior))?;
            allocations.claim_bytes(path_bytes, 0, &mut prior, crate::WorkBudget::UNBOUNDED)
                .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), prior))?;
            if options.generation.workspace.id != self.id {
                return Err(crate::OperationFailure::new(WorkspaceError::ForeignGeneration, prior));
            }
            cancellation.check().map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
            prior.verify(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
            let named_budget = workspace_remaining(prior, budget, allocations.live_bytes())
                .map_err(|error| crate::OperationFailure::new(error, prior))?;
            let named = merge_workspace_backend(
                WorkspaceName::new_measured(destination, named_budget, cancellation),
                &mut prior, allocations.live_bytes(),
            ).map_err(|error| crate::OperationFailure::new(error, prior))?;
            prior.verify(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
            allocations.release(path_bytes)
                .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), prior))?;
            let remaining = prior.remaining(budget)
                .map_err(|error| crate::OperationFailure::new(WorkspaceError::Work(error), prior))?;
            let forked = merge_workspace_backend(self.volume.fs.fork_workspace(
                named.value, &options.generation, options.idempotency_key, options.paths,
                permit, remaining, cancellation, Some(&mut guard),
            ).await, &mut prior, 0).map_err(|error| crate::OperationFailure::new(error, prior))?;
            prior.verify(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
            Ok(crate::OperationReceipt { value: forked.value, work: prior })
        }, budget, cancellation, WorkspaceError::Cancelled(crate::CancellationError), None).await
    }

    /// Forks the supplied immutable source on the invocation's original scope.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn fork_with_native_work(
        &self,
        destination: &str,
        options: ForkOptions<A, O>,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Self, (WorkspaceError, Option<crate::NativeAllocation>)> {
        let result = async {
            let (named, options) = self.prepare_fork_with_native_work(
                destination, options, original, cancellation,
            )?;
            let ForkOptions { generation, idempotency_key, paths } = options;
            self.volume.fs.fork_workspace::<_, fn(crate::WorkBudget) -> std::future::Ready<crate::FsResult<()>>, _>(
                named, &generation, idempotency_key, paths, crate::PublicationPermit::Unrestricted,
                original, cancellation, None,
            ).await
        }.await;
        result.map_err(|error| (error, None))
    }

    /// Seals each real destination publication with the original native owner guard.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn fork_with_permit_guarded_with_native_work<'original, E, F, Fut>(
        &self, destination: &str, options: ForkOptions<A, O>,
        permit: crate::PublicationPermit, original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken, before_publication: &mut F,
    ) -> Result<Self, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let (named, options) = self.prepare_fork_with_native_work(
            destination, options, original, cancellation,
        )?;
        let ForkOptions { generation, idempotency_key, paths } = options;
        self.volume.fs.fork_workspace_guarded_with_native_work(
            named, &generation, idempotency_key, paths, permit, original, cancellation,
            before_publication,
        ).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_fork_with_native_work(
        &self, destination: &str, options: ForkOptions<A, O>,
        original: &crate::NativeMaterializationWork, cancellation: &crate::CancellationToken,
    ) -> Result<(WorkspaceName, ForkOptions<A, O>), WorkspaceError> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        let path_bytes = options.paths.as_ref().map_or(Ok(0), |paths|
            workspace_owned_strings_bytes(paths))?;
        let (allocation, admitted) = accounting.record_actual_owned(path_bytes)?;
        let mut options = (options, allocation);
        admitted?;
        if options.0.generation.workspace.id != self.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        accounting.check()?;
        let named = WorkspaceName::new_with_native_work(destination, original, cancellation)?;
        accounting.release_owned(&mut options.1, path_bytes)?;
        Ok((named, options.0))
    }

    /// Opens a head transaction under exact work and cancellation bounds.
    pub async fn begin_transaction(
        &self,
        idempotency_key: IdempotencyKey,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Transaction<A, O>>, WorkspaceError> {
        let checkout = self.checkout(GenerationSelector::Head, CheckoutMode::tracking_transaction(), budget, cancellation).await?;
        Ok(crate::OperationReceipt {
            value: Transaction::from_checkout(self, checkout.value, idempotency_key, false),
            work: checkout.work,
        })
    }

    /// Opens one sparse atomic transaction against an exact immutable generation.
    pub async fn begin_transaction_at(
        &self,
        generation: &Generation<A, O>,
        idempotency_key: IdempotencyKey,
    ) -> Result<Transaction<A, O>, WorkspaceError> {
        if generation.workspace.id != self.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let requires_rebase = self.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.id != generation.id;
        self.open_transaction_at(generation, idempotency_key, requires_rebase, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Opens a transaction at the original exact generation without reading a mutable head.
    pub async fn begin_pinned_transaction_measured(
        &self,
        generation: &Generation<A, O>,
        idempotency_key: IdempotencyKey,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Transaction<A, O>>, WorkspaceError> {
        if generation.workspace.id != self.id {
            return Err(crate::OperationFailure::before_work(WorkspaceError::ForeignGeneration));
        }
        self.open_transaction_at(generation, idempotency_key, false, budget, cancellation)
            .await
    }

    /// Opens the canonical tracking transaction on the original native scope.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn begin_transaction_with_native_work(
        &self,
        idempotency_key: IdempotencyKey,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Transaction<A, O>, (WorkspaceError, Option<crate::NativeAllocation>)> {
        let checkout = self.volume.checkout_with_native_work(
            GenerationSelector::Head, CheckoutMode::tracking_transaction(), original, cancellation,
        ).await.map_err(|error| (WorkspaceError::from(error), None))?;
        Ok(Transaction::from_checkout(self, checkout, idempotency_key, false))
    }

    /// Opens this original immutable generation without consulting a mutable head.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn begin_pinned_transaction_with_native_work(
        &self,
        generation: &Generation<A, O>,
        idempotency_key: IdempotencyKey,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Transaction<A, O>, (WorkspaceError, Option<crate::NativeAllocation>)> {
        self.begin_pinned_transaction_with_native_work_value(
            generation, idempotency_key, original, cancellation,
        ).await.map_err(|error| (error, None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn begin_pinned_transaction_with_native_work_value(
        &self, generation: &Generation<A, O>, idempotency_key: IdempotencyKey,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Transaction<A, O>, WorkspaceError> {
        original.admit(crate::WorkCounters::UNCHARGED).map_err(FsError::from)
            .map_err(WorkspaceError::from)?;
        cancellation.check()?;
        if generation.workspace.id != self.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let checkout = self.volume.checkout_with_native_work(
            GenerationSelector::Exact(generation.id), CheckoutMode::tracking_transaction(), original, cancellation,
        ).await.map_err(WorkspaceError::from)?;
        Ok(Transaction::from_checkout(self, checkout, idempotency_key, false))
    }

    async fn open_transaction_at(
        &self,
        generation: &Generation<A, O>,
        idempotency_key: IdempotencyKey,
        requires_rebase: bool,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Transaction<A, O>>, WorkspaceError> {
        let receipt = self
            .checkout(
                GenerationSelector::Exact(generation.id),
                CheckoutMode::tracking_transaction(),
                budget,
                cancellation,
            )
            .await?;
        Ok(crate::OperationReceipt {
            value: Transaction::from_checkout(
                self, receipt.value, idempotency_key, requires_rebase,
            ),
            work: receipt.work,
        })
    }

    /// Retains the current generation under one permanent human-readable label.
    pub async fn checkpoint(
        &self,
        label: impl AsRef<str>,
    ) -> Result<(WorkspaceName, Generation<A, O>), WorkspaceError> {
        async move {
                let label = WorkspaceName::new(label)?;
                let generation = self.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
                let retained_label = label.as_str().to_owned();
                let bytes = retained_label.capacity() as u64;
                self.volume
                    .fs
                    .retain_workspace_generation(
                        &self.volume,
                        generation.id,
                        crate::kernel::RetentionKind::Checkpoint,
                        crate::kernel::Owned::new(retained_label, crate::kernel::allocation::KernelOwnedAllocation::Standalone, bytes),
                        None, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
                    )
                    .await.map_err(|failure| failure.error)?;
                Ok((label, generation))
            }
            .await
    }

    /// Durably pins the current immutable generation under an opaque customer
    pub async fn pin(
        &self,
        identity: impl AsRef<str>,
    ) -> Result<(WorkspaceName, Generation<A, O>), WorkspaceError> {
        self.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?.pin(identity).await
    }

    /// Terminally deletes this workspace head while leaving independently
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
    pub async fn diff(
        &self,
        from: &Generation<A, O>,
        to: &Generation<A, O>,
        maximum_changes: u32,
    ) -> Result<ChangeSet<A, O>, WorkspaceError> {
        async move {
                if from.workspace.id != self.id || to.workspace.id != self.id
                    || !self.volume.fs.same_deployment(&from.workspace.volume.fs)
                    || !self.volume.fs.same_deployment(&to.workspace.volume.fs)
                {
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
            .await
    }

    /// Begins a side-effect-free join description from this workspace into a
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
    pub async fn live_rebase(
        &self,
        idempotency_key: IdempotencyKey,
        maximum_generations: u32,
        maximum_changes: u32,
        maximum_conflicts: u32,
    ) -> Result<WorkspaceRebase<A, O>, WorkspaceError> {
        async move {
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
                let generation = |id| Generation { workspace: self.clone(), id, publication_closure: None };
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
            .await
    }

    /// The parent head that [`Self::live_rebase`] would rebase this fork onto,
    pub(crate) async fn parent_advance(
        &self,
        maximum_generations: u32,
    ) -> Result<Option<GenerationId>, WorkspaceError> {
        self.volume
            .fs
            .parent_advance(&self.volume, maximum_generations)
            .await
    }

    /// Reads the head through the canonical engine with exact failure work.
    pub async fn read<W: crate::facade::FsWork>(
        &self,
        path: &str,
        maximum_bytes: u64,
        work: W,
        cancellation: &crate::CancellationToken,
    ) -> W::Output<acyclic_stream::OwnedBytes, WorkspaceError> {
        self.read_selected(GenerationSelector::Head, path, maximum_bytes, work, cancellation).await
    }

    /// Performs the bounded read range operation.
    pub async fn read_range<W: crate::facade::FsWork>(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        work: W,
        cancellation: &crate::CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_generation_range(
            self, GenerationSelector::Head, path, offset, length, work, cancellation,
        ).await
    }

    /// Resolves the canonical no-follow head identity under exact work bounds.
    pub async fn stat(
        &self,
        path: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<WorkspaceStat>, WorkspaceError> {
        stat_generation(self, GenerationSelector::Head, path, budget, cancellation).await
    }

    pub(crate) async fn stat_optional_measured(
        &self,
        path: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Option<WorkspaceStat>>, WorkspaceError> {
        stat_generation_optional(
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
            .checkout(GenerationSelector::Head, CheckoutMode::read_only_pinned(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
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
    pub(crate) async fn make_records_durable(
        &self,
        records: &[crate::kernel::FileRecord],
    ) -> Result<crate::PublicationHold, WorkspaceError> {
        self.make_records_durable_measured(
            records, WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    pub(crate) async fn make_records_durable_measured(
        &self,
        records: &[crate::kernel::FileRecord],
        budget: WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> MeasuredResult<OperationReceipt<crate::PublicationHold>, WorkspaceError> {
        self.volume.fs.make_records_durable(
            self.volume.config, records, budget, cancellation,
        ).await.map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::default(), WorkspaceError::engine))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn make_records_durable_with_native_work(
        &self,
        records: &[crate::kernel::FileRecord],
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::PublicationHold, WorkspaceError> {
        self.volume.fs.make_records_durable_with_native_work(
            self.volume.config, records, original, cancellation,
        ).await.map_err(WorkspaceError::from)
    }

    pub(crate) fn detached_record(
        &self,
        record: crate::kernel::FileRecord,
    ) -> crate::DetachedFile<A, O> {
        crate::DetachedFile::from_record(self.volume.clone(), record, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).expect("unbounded detached hold preserves a validated record").value
    }

    pub(crate) fn detached_record_measured(
        &self,
        record: crate::kernel::FileRecord,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::DetachedFile<A, O>>, WorkspaceError> {
        crate::DetachedFile::from_record(self.volume.clone(), record, budget, cancellation)
            .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::default(), WorkspaceError::from))
    }

    /// Returns an authenticated directory page with exact success and failure work.
    pub async fn list_directory(
        &self,
        path: &str,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<WorkspaceDirectoryPage>, WorkspaceError> {
        in_heap(move || async move {
            list_generation_directory(
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

    /// Performs the bounded read symbolic link operation.
    pub async fn read_symbolic_link<W: crate::facade::FsWork>(
        &self,
        path: &str,
        work: W,
        cancellation: &crate::CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_generation_symbolic_link(
            self, GenerationSelector::Head, path, work, cancellation,
        ).await
    }

    /// Plans sparse logical spans without reading content bodies.
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
        in_heap(|| async {
        let mut transaction = self.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.write(path, bytes).await?;
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
        }).await
    }

    /// Removes one existing path in a single atomic generation publication.
    pub async fn remove(&self, path: &str) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        let mut transaction = self.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.remove(path).await?;
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Copies one complete regular file within this workspace using immutable
    pub async fn copy(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<TransactionCommit<A, O>, WorkspaceError> {
        let mut transaction = self.begin_transaction(IdempotencyKey::new(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        transaction.copy(source, destination).await?;
        transaction.commit(None, crate::PublicationPermit::Unrestricted, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), |_| async { Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt { value: (), work: crate::WorkCounters::default() }) }).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    async fn read_selected<W: crate::facade::FsWork>(
        &self,
        selector: GenerationSelector,
        path: &str,
        maximum_bytes: u64,
        work: W,
        cancellation: &crate::CancellationToken,
    ) -> W::Output<acyclic_stream::OwnedBytes, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let mut checkout = self.open_checkout(selector, CheckoutMode::read_only_pinned(), &mut accounting, cancellation).await?;
            read_checkout_whole_with_accounting(&mut checkout.value, path, maximum_bytes, &mut accounting, cancellation).await
        }.await;
        work.finish(result, &accounting)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn engine_checkout_with_native_work(
        &self, selector: GenerationSelector, mode: CheckoutMode,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Checkout<A, O>, WorkspaceError> {
        self.volume.checkout_with_native_work(selector, mode, original, cancellation)
            .await.map_err(WorkspaceError::from)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn readonly_pinned_reader_with_native_work(
        &self,
        generation: GenerationId,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::PinnedReader<A, O>, WorkspaceError> {
        let checkout = self.volume.checkout_with_native_work(
            GenerationSelector::Exact(generation),
            CheckoutMode::read_only_pinned(),
            original,
            cancellation,
        ).await.map_err(WorkspaceError::from)?;
        checkout.into_pinned_reader_with_native_work(original, cancellation)
            .map_err(WorkspaceError::from)
    }
}


async fn read_checkout_whole_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    checkout: &mut Checkout<A, O>,
    path: &str,
    maximum_bytes: u64,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<acyclic_stream::OwnedBytes, WorkspaceError> {
    let (path, allocation) = customer_path_with_accounting(
        path, checkout.volume_config(), accounting, cancellation,
    )?;
    let retained = path.retained_bytes();
    let path = crate::kernel::Owned::new(path, allocation, retained);
    let (lookup, allocation) = checkout.lookup_no_follow_with_accounting(
        &path.value, accounting, cancellation,
    ).await.map_err(WorkspaceError::from)?;
    drop(allocation);
    let record = lookup.record.ok_or(WorkspaceError::NotFound)?;
    if record.kind != FileKind::Regular {
        return Err(WorkspaceError::NotRegularFile);
    }
    let logical_bytes = regular_file_bytes(record)?;
    if logical_bytes > maximum_bytes {
        return Err(WorkspaceError::ReadLimitExceeded);
    }
    checkout.read_file_range_with_accounting(
        &path.value,
        crate::ByteRange { offset: 0, length: logical_bytes },
        accounting,
        cancellation,
    ).await.map(|read| read.bytes).map_err(WorkspaceError::from)
}



async fn read_generation_range<A: AsyncAuthorityStore, O: AsyncObjectStore, W: crate::facade::FsWork>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    offset: u64,
    length: u64,
    work: W,
    cancellation: &crate::CancellationToken,
) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let (mut checkout, path) = workspace.open_read_path(selector, path, &mut accounting, cancellation).await?;
        checkout.value.read_file_range_with_accounting(
            &path.value, crate::ByteRange { offset, length }, &mut accounting, cancellation,
        ).await.map(|read| {
            let bytes = read.logical_owned_bytes();
            crate::kernel::OwnedSourceChunk::claimed(read.bytes, bytes)
        }).map_err(WorkspaceError::from)
    }.await;
    work.finish(result, &accounting)
}


async fn stat_generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<WorkspaceStat>, WorkspaceError> {
    let receipt =
        stat_generation_optional(workspace, selector, path, budget, cancellation).await?;
    Ok(crate::OperationReceipt {
        value: receipt.value.ok_or_else(|| crate::OperationFailure::new(WorkspaceError::NotFound, receipt.work))?,
        work: receipt.work,
    })
}

async fn stat_generation_optional<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<Option<WorkspaceStat>>, WorkspaceError> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let result = async {
        let (mut checkout, path) = workspace.open_read_path(selector, path, &mut accounting, cancellation).await?;
        checkout.value.lookup_no_follow_with_metadata_with_accounting(
            &path.value, &mut accounting, cancellation,
        ).await?.map(workspace_stat_from_lookup).transpose()
    }.await;
    let work = accounting.spent();
    result.map(|value| crate::OperationReceipt { value, work })
        .map_err(|error| crate::OperationFailure::new(error, work))
}

fn workspace_stat_from_lookup(
    lookup: crate::PathMetadataLookup,
) -> Result<WorkspaceStat, WorkspaceError> {
    let logical_bytes = match &lookup.record.payload {
        FilePayload::InlineRegular(bytes) => Some(u64::try_from(bytes.as_bytes().len())
            .map_err(|_| crate::WorkError::Overflow)?),
        FilePayload::Regular { logical_bytes, .. }
        | FilePayload::SymbolicLink { target_bytes: logical_bytes, .. }
        | FilePayload::ReparsePoint { payload_bytes: logical_bytes, .. } => Some(*logical_bytes),
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
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<WorkspaceDirectoryPage>, WorkspaceError> {
    let mut accounting = KernelAccounting::standalone(budget, cancellation);
    let result = async {
        let (mut checkout, path) = workspace.open_read_path(selector, path, &mut accounting, cancellation).await?;
        let (page, allocation) = checkout.value.list_directory_with_bound_with_accounting(
            &path.value, after, false, maximum_entries, &mut accounting, cancellation,
        ).await?;
        project_workspace_directory_with_accounting(page, allocation, &mut accounting).map(|(page, _)| page)
    }.await;
    let work = accounting.spent();
    result.map(|value| crate::OperationReceipt { value, work })
        .map_err(|error| crate::OperationFailure::new(error, work))
}

fn project_workspace_directory_with_accounting(
    page: crate::kernel::DirectoryPage,
    source_allocation: crate::kernel::allocation::KernelOwnedAllocation,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<(WorkspaceDirectoryPage, crate::kernel::allocation::KernelOwnedAllocation), WorkspaceError> {
    let source_bytes = workspace_vec_bytes(&page.entries);
    let mut page = crate::kernel::Owned::new(page, source_allocation, source_bytes);
    let mut entries = crate::kernel::Owned::<Vec<WorkspaceDirectoryEntry>>::allocate(
        page.value.entries.len(), accounting,
    )?;
    entries.allocation.absorb(&mut page.allocation)?;
    let has_more = page.value.has_more;
    let mut source = page.value.entries.into_iter();
    for entry in source.by_ref() {
        let delta = crate::WorkCounters {
            items_returned: 1,
            bytes_copied: crate::foundation::usize_to_u64(std::mem::size_of::<WorkspaceDirectoryEntry>()),
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(delta)?;
        entries.value.push(WorkspaceDirectoryEntry {
            name: entry.name, file_id: entry.file_id, kind: entry.kind,
        });
        accounting.record_completed(delta)?;
    }
    drop(source);
    accounting.release_owned(&mut entries.allocation, source_bytes)?;
    let crate::kernel::Owned { value, allocation, .. } = entries;
    Ok((WorkspaceDirectoryPage { entries: value, has_more }, allocation))
}



async fn read_generation_symbolic_link<A: AsyncAuthorityStore, O: AsyncObjectStore, W: crate::facade::FsWork>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    work: W,
    cancellation: &crate::CancellationToken,
) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
    let mut accounting = work.accounting(cancellation);
    let result = async {
        let (mut checkout, path) = workspace.open_read_path(selector, path, &mut accounting, cancellation).await?;
        checkout.value.read_symbolic_link_with_accounting(&path.value, &mut accounting, cancellation)
            .await.map_err(WorkspaceError::from)
    }.await;
    work.finish(result, &accounting)
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
        .checkout(selector, CheckoutMode::read_only_pinned(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
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
                .ok_or_else(|| WorkspaceError::manual_engine("file range overflows"))?,
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
    fn from_checkout(
        workspace: &Workspace<A, O>,
        mut checkout: Checkout<A, O>,
        idempotency_key: IdempotencyKey,
        requires_rebase: bool,
    ) -> Self {
        checkout.bind_authored_operation(idempotency_key.operation_id());
        Self {
            workspace: workspace.clone(), checkout, idempotency_key, requires_rebase,
        }
    }

    /// Reuses the workspace transaction compiler against a private checkout
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

    /// Moves a private candidate with its already-bound SDK operation identity.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn for_checkout_candidate_with_native_work(
        workspace: &Workspace<A, O>,
        checkout: Checkout<A, O>,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<Self, WorkspaceError> {
        original.admit(crate::WorkCounters::UNCHARGED).map_err(FsError::from)
            .map_err(WorkspaceError::from)?;
        cancellation.check()?;
        let operation = checkout.bound_authored_operation_id()
            .ok_or_else(|| WorkspaceError::from(FsError::MutationNotAllowed))?;
        Ok(Self {
            workspace: workspace.clone(), checkout,
            idempotency_key: IdempotencyKey::from_bytes(operation.into_bytes()),
            requires_rebase: false,
        })
    }

    pub(crate) fn into_checkout_candidate(self) -> Checkout<A, O> {
        self.checkout
    }

    pub(crate) fn workspace_id(&self) -> WorkspaceId {
        self.workspace.id()
    }

    fn mutation_path_with_accounting(
        &self,
        path: &str,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::kernel::Owned<NamespacePath>, WorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.checkout.validate_native_work(original)?;
        }
        let (path, allocation) = customer_path_with_accounting(
            path, self.checkout.volume_config(), accounting, cancellation,
        )?;
        let bytes = path.retained_bytes();
        Ok(crate::kernel::Owned::new(path, allocation, bytes))
    }

    fn push_workspace_mutation<T>(
        operations: &mut crate::kernel::Owned<Vec<T>>,
        operation: crate::kernel::Owned<T>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(), WorkspaceError> {
        let delta = crate::WorkCounters {
            bytes_copied: std::mem::size_of::<T>() as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(delta)?;
        let mut operation = operation;
        operations.allocation.absorb(&mut operation.allocation)?;
        operations.bytes = operations.bytes.checked_add(operation.bytes)
            .ok_or(crate::WorkError::Overflow)?;
        operations.value.push(operation.value);
        accounting.record_completed(delta)?;
        Ok(())
    }

    async fn apply_workspace_authored_with_accounting(
        &mut self,
        operations: crate::kernel::Owned<Vec<AuthoredMutation>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let applied = self.checkout.apply_authored_transaction_with_accounting(
            operations, accounting, cancellation,
        ).await.map_err(WorkspaceError::from)?;
        // Created IDs are unused here; destroy their data before their guard.
        applied.retire(accounting).map_err(WorkspaceError::from)
    }

    async fn apply_workspace_path_with_accounting(
        &mut self,
        path: &str,
        build: impl FnOnce(NamespacePath) -> AuthoredMutation,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let path = self.mutation_path_with_accounting(path, accounting, cancellation)?;
        let operation = crate::kernel::Owned::new(build(path.value), path.allocation, path.bytes);
        let mut operations = crate::kernel::Owned::allocate(1, accounting)?;
        Self::push_workspace_mutation(&mut operations, operation, accounting)?;
        self.apply_workspace_authored_with_accounting(operations, accounting, cancellation).await
    }

    async fn apply_workspace_path<W: crate::facade::FsWork>(
        &mut self, path: &str, build: impl FnOnce(NamespacePath) -> AuthoredMutation,
        work: W, cancellation: &crate::CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.apply_workspace_path_with_accounting(path, build, &mut accounting, cancellation).await;
        work.finish(result, &accounting)
    }

    async fn restore_record_with_accounting(
        &mut self,
        path: &str,
        record: crate::kernel::FileRecord,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let path = self.mutation_path_with_accounting(path, accounting, cancellation)?;
        let operation = crate::kernel::Owned::new(
            crate::kernel::Mutation::Restore { path: path.value, record }, path.allocation, path.bytes,
        );
        let mut operations = crate::kernel::Owned::allocate(1, accounting)?;
        Self::push_workspace_mutation(&mut operations, operation, accounting)?;
        self.checkout.mutate_owned_with_accounting(operations, accounting, cancellation)
            .await.map_err(WorkspaceError::from)
    }

    pub(crate) async fn restore_record<W: crate::facade::FsWork>(
        &mut self, path: &str, record: crate::kernel::FileRecord,
        work: W, cancellation: &crate::CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.restore_record_with_accounting(path, record, &mut accounting, cancellation).await;
        work.finish(result, &accounting)
    }

    async fn create_dir_all_with_accounting(
        &mut self, path: &str,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), WorkspaceError> {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.checkout.validate_native_work(original)?;
        }
        let depth = PortablePath::validate(path, self.checkout.volume_config().limits)
            .map_err(|error| namespace_path_error(crate::kernel::NamespacePathError::Path(error)))?;
        if depth == 0 { return Ok(()); }
        for end in path.match_indices('/').skip(1).map(|(index, _)| index)
            .chain(std::iter::once(path.len())) {
            let path = self.mutation_path_with_accounting(&path[..end], accounting, cancellation)?;
            match self.checkout.lookup_no_follow_with_accounting(&path.value, accounting, cancellation).await?.0.record {
                Some(record) if record.kind == FileKind::Directory => { path.retire(accounting)?; }
                Some(_) => { path.retire(accounting)?; return Err(WorkspaceError::NotDirectory); }
                None => {
                    let operation = crate::kernel::Owned::new(
                        AuthoredMutation::CreateDirectory { path: path.value, metadata: FileMetadata::default() },
                        path.allocation, path.bytes,
                    );
                    let mut operations = crate::kernel::Owned::allocate(1, accounting)?;
                    Self::push_workspace_mutation(&mut operations, operation, accounting)?;
                    self.apply_workspace_authored_with_accounting(operations, accounting, cancellation).await?;
                }
            }
        }
        Ok(())
    }

    /// Creates missing directory prefixes under the supplied work limits.
    pub async fn create_dir_all<W: crate::facade::FsWork>(
        &mut self, path: &str, work: W, cancellation: &crate::CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.create_dir_all_with_accounting(path, &mut accounting, cancellation).await;
        work.finish(result, &accounting)
    }

    /// Admits a bounded ordered capture as one unpublished candidate update.
    pub(crate) async fn apply_authored_bulk_measured(
        &mut self, operations: Vec<AuthoredMutation>,
        budget: crate::WorkBudget, cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        self.checkout.apply_authored_bulk_transaction(operations, budget, cancellation)
            .await.map(|receipt| receipt.work)
            .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::default(), WorkspaceError::from))
    }

    /// Creates one directory with canonical metadata and bounded work.
    pub async fn create_directory<W: crate::facade::FsWork>(
        &mut self, path: &str, work: W, cancellation: &crate::CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.apply_workspace_path(path, |path| AuthoredMutation::CreateDirectory {
            path, metadata: FileMetadata::default(),
        }, work, cancellation).await
    }

    /// Creates one new file under the supplied work limits.
    pub async fn create_file<W: crate::facade::FsWork>(
        &mut self, path: &str, bytes: impl Into<acyclic_stream::OwnedBytes>, metadata: FileMetadata,
        work: W, cancellation: &crate::CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.apply_workspace_path(path, |path| AuthoredMutation::CreateFile {
            path, bytes: bytes.into(), metadata,
        }, work, cancellation).await
    }

    /// Replaces a file range under the supplied work limits.
    pub async fn write_range<W: crate::facade::FsWork>(
        &mut self, path: &str, offset: u64, bytes: impl Into<acyclic_stream::OwnedBytes>,
        work: W, cancellation: &crate::CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.apply_workspace_path(path, |path| AuthoredMutation::Write { path, offset, bytes: bytes.into() },
            work, cancellation).await
    }

    /// Creates a symbolic link with an opaque target and bounded work.
    pub async fn create_symbolic_link<W: crate::facade::FsWork>(
        &mut self, path: &str, target: impl Into<acyclic_stream::OwnedBytes>,
        work: W, cancellation: &crate::CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.apply_workspace_path(path, |path| AuthoredMutation::CreateSymbolicLink {
            path, target: target.into(), metadata: FileMetadata::default(),
        }, work, cancellation).await
    }

    fn special_mutation(
        path: NamespacePath, kind: FileKind, device: Option<(u32, u32)>,
    ) -> AuthoredMutation {
        match device {
            None => AuthoredMutation::CreateEmptySpecial { path, kind, metadata: FileMetadata::default() },
            Some((major, minor)) => AuthoredMutation::CreateDevice {
                path, kind, major, minor, metadata: FileMetadata::default(),
            },
        }
    }

    /// Creates one special node under the supplied work limits.
    pub async fn create_special<W: crate::facade::FsWork>(
        &mut self, path: &str, kind: FileKind, device: Option<(u32, u32)>,
        work: W, cancellation: &crate::CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let path = self.mutation_path_with_accounting(path, &mut accounting, cancellation)?;
            if !matches!((kind, device),
                (FileKind::Fifo | FileKind::Socket, None) |
                (FileKind::CharacterDevice | FileKind::BlockDevice, Some(_))) {
                path.retire(&mut accounting)?;
                return Err(WorkspaceError::IncompatibleWorkspace);
            }
            let operation = crate::kernel::Owned::new(
                Self::special_mutation(path.value, kind, device), path.allocation, path.bytes,
            );
            let mut operations = crate::kernel::Owned::allocate(1, &mut accounting)?;
            Self::push_workspace_mutation(&mut operations, operation, &mut accounting)?;
            self.apply_workspace_authored_with_accounting(operations, &mut accounting, cancellation).await
        }.await;
        work.finish(result, &accounting)
    }

    /// Replaces canonical metadata under the supplied work limits.
    pub async fn set_metadata<W: crate::facade::FsWork>(
        &mut self, path: &str, metadata: FileMetadata, work: W, cancellation: &crate::CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.apply_workspace_path(path, |path| AuthoredMutation::SetMetadata { path, metadata }, work, cancellation).await
    }

    pub(crate) async fn preserve_file_identity<W: crate::facade::FsWork>(
        &mut self, path: &str, file_id: FileId, work: W, cancellation: &crate::CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        self.apply_workspace_path(path, |path| AuthoredMutation::Reidentify { path, file_id }, work, cancellation).await
    }

    fn prepare_hard_link(
        &mut self, source: &str, destination: &str,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::kernel::Owned<Vec<AuthoredMutation>>, WorkspaceError> {
        let mut source = self.mutation_path_with_accounting(source, accounting, cancellation)?;
        let mut destination = self.mutation_path_with_accounting(destination, accounting, cancellation)?;
        source.allocation.absorb(&mut destination.allocation)?;
        let bytes = source.bytes.checked_add(destination.bytes).ok_or(crate::WorkError::Overflow)?;
        let operation = crate::kernel::Owned::new(
            AuthoredMutation::HardLink { source: source.value, destination: destination.value },
            source.allocation, bytes,
        );
        let mut operations = crate::kernel::Owned::allocate(1, accounting)?;
        Self::push_workspace_mutation(&mut operations, operation, accounting)?;
        Ok(operations)
    }

    /// Creates a hard link under the supplied work limits.
    pub async fn hard_link<W: crate::facade::FsWork>(
        &mut self, source: &str, destination: &str, work: W, cancellation: &crate::CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let operations = self.prepare_hard_link(source, destination, &mut accounting, cancellation)?;
            self.apply_workspace_authored_with_accounting(operations, &mut accounting, cancellation).await
        }.await;
        work.finish(result, &accounting)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn restore_selected_paths_from_with_native_work(
        &mut self,
        source: &Generation<A, O>,
        paths: &[String],
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), WorkspaceError> {
        self.checkout.validate_native_work(original)?;
        self.restore_paths_from_with_accounting(
            source, paths, true,
            &mut crate::kernel::allocation::KernelAccounting::native(original, cancellation),
            cancellation,
        ).await
    }

    async fn restore_paths_from_with_accounting(
        &mut self,
        source: &Generation<A, O>,
        paths: &[String],
        selected_subtrees: bool,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), WorkspaceError> {
        if !self.workspace.volume.fs.same_deployment(&source.workspace.volume.fs) {
            return Err(WorkspaceError::ForeignGeneration);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.checkout.validate_native_work(original)?;
        }
        let source_root = self.workspace.restore_source_root_with_accounting(
            source, accounting, cancellation,
        ).await?;
        self.restore_paths_from_root_with_accounting(
            source, paths, selected_subtrees, source_root, accounting, cancellation,
        ).await
    }

    async fn restore_paths_from_root_with_accounting(
        &mut self,
        source: &Generation<A, O>,
        paths: &[String],
        selected_subtrees: bool,
        mut source_root: crate::kernel::Owned<Checkout<A, O>>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), WorkspaceError> {
        if !self.workspace.volume.fs.same_deployment(&source.workspace.volume.fs) {
            return Err(WorkspaceError::ForeignGeneration);
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            self.checkout.validate_native_work(original)?;
        }
        let mut parsed = crate::kernel::Owned::allocate(paths.len(), accounting)?;
        for path in paths {
            cancellation.check()?;
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let absolute = Workspace::<A, O>::restore_absolute_path_with_accounting(
                path.trim_start_matches('/'), accounting, cancellation,
            )?;
            let parsed_path = self.mutation_path_with_accounting(&absolute.value, accounting, cancellation)?;
            absolute.retire(accounting)?;
            Self::push_workspace_mutation(&mut parsed, parsed_path, accounting)?;
        }
        let source_records = match source_root.value.lookup_batch_no_follow_with_accounting(
            &parsed.value, selected_subtrees, accounting, cancellation,
        ).await {
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
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            missing_count += usize::from(entry.record.is_none());
        }
        let mut missing_paths = crate::kernel::Owned::allocate(missing_count, accounting)?;
        for (path, entry) in parsed.value.iter().zip(&source_records.value.entries) {
            cancellation.check()?;
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            if entry.record.is_none() {
                let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
                let bytes = path.retained_bytes();
                Self::push_workspace_mutation(
                    &mut missing_paths, crate::kernel::Owned::new(path, allocation, bytes), accounting,
                )?;
            }
        }
        let current_records = if missing_paths.value.is_empty() {
            None
        } else {
            let (records, allocation) = self.checkout.lookup_batch_no_follow_with_accounting(
                &missing_paths.value, false, accounting, cancellation,
            ).await?;
            let bytes = records.retained_allocation_bytes;
            Some(crate::kernel::Owned::new(records, allocation, bytes))
        };
        if selected_subtrees {
            let mut removal_count = 0;
            if let Some(records) = &current_records {
                for entry in &records.value.entries {
                    cancellation.check()?;
                    accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                    removal_count += usize::from(entry.record.is_some());
                }
            }
            let mut removals = crate::kernel::Owned::allocate(removal_count, accounting)?;
            if let Some(records) = &current_records {
                for (path, entry) in missing_paths.value.drain(..).zip(&records.value.entries) {
                    cancellation.check()?;
                    accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                    let bytes = path.retained_bytes();
                    let allocation = match &mut missing_paths.allocation {
                        crate::kernel::allocation::KernelOwnedAllocation::Standalone =>
                            crate::kernel::allocation::KernelOwnedAllocation::Standalone,
                        #[cfg(not(target_arch = "wasm32"))]
                        crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(guard) =>
                            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(guard.split_off(bytes)?),
                    };
                    let path = crate::kernel::Owned::new(path, allocation, bytes);
                    missing_paths.bytes = missing_paths.bytes.checked_sub(bytes).ok_or(crate::WorkError::Overflow)?;
                    if let Some(record) = entry.record {
                        Self::push_workspace_mutation(&mut removals, crate::kernel::Owned::new(
                            crate::kernel::Mutation::Remove {
                                path: path.value, expected_file_id: MetadataField::Value(record.file_id),
                            }, path.allocation, path.bytes,
                        ), accounting)?;
                    } else {
                        path.retire(accounting)?;
                    }
                }
            }
            if removals.value.is_empty() {
                removals.retire(accounting)?;
            } else {
                self.checkout.mutate_owned_with_accounting(removals, accounting, cancellation).await?;
            }
        }
        missing_paths.retire(accounting)?;
        let mut current_index = 0;
        let mut operations = crate::kernel::Owned::allocate(paths.len(), accounting)?;
        for ((original_path, path), entry) in paths.iter().zip(parsed.value.drain(..))
            .zip(&source_records.value.entries)
        {
            cancellation.check()?;
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let relative = original_path.trim_start_matches('/');
            let bytes = path.retained_bytes();
            let allocation = match &mut parsed.allocation {
                crate::kernel::allocation::KernelOwnedAllocation::Standalone =>
                    crate::kernel::allocation::KernelOwnedAllocation::Standalone,
                #[cfg(not(target_arch = "wasm32"))]
                crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(guard) =>
                    crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(guard.split_off(bytes)?),
            };
            let path = crate::kernel::Owned::new(path, allocation, bytes);
            parsed.bytes = parsed.bytes.checked_sub(bytes).ok_or(crate::WorkError::Overflow)?;
            if let Some(record) = entry.record {
                if selected_subtrees && record.kind == FileKind::Directory {
                    let destination = self.checkout.lookup_no_follow_with_accounting(
                        &path.value, accounting, cancellation,
                    ).await?.0.record;
                    let absolute = Workspace::<A, O>::restore_absolute_path_with_accounting(
                        relative, accounting, cancellation,
                    )?;
                    if destination.is_some_and(|record| record.kind != FileKind::Directory) {
                        self.remove_with_accounting(&absolute.value, accounting, cancellation).await?;
                    }
                    self.create_dir_all_with_accounting(&absolute.value, accounting, cancellation).await?;
                    absolute.retire(accounting)?;
                }
                if record.kind != FileKind::Directory && let Some((parent, _)) = relative.rsplit_once('/') {
                    let absolute = Workspace::<A, O>::restore_absolute_path_with_accounting(
                        parent, accounting, cancellation,
                    )?;
                    self.create_dir_all_with_accounting(&absolute.value, accounting, cancellation).await?;
                    absolute.retire(accounting)?;
                }
                Self::push_workspace_mutation(&mut operations, crate::kernel::Owned::new(
                    crate::kernel::Mutation::Restore { path: path.value, record },
                    path.allocation, path.bytes,
                ), accounting)?;
            } else {
                let current_record = current_records.as_ref()
                    .and_then(|records| records.value.entries.get(current_index))
                    .ok_or(WorkspaceError::IncompatibleWorkspace)?;
                current_index += 1;
                if !selected_subtrees && let Some(record) = current_record.record {
                    Self::push_workspace_mutation(&mut operations, crate::kernel::Owned::new(
                        crate::kernel::Mutation::Remove {
                            path: path.value, expected_file_id: MetadataField::Value(record.file_id),
                        }, path.allocation, path.bytes,
                    ), accounting)?;
                } else {
                    path.retire(accounting)?;
                }
            }
        }
        parsed.retire(accounting)?;
        if let Some(records) = current_records {
            records.retire(accounting)?;
        }
        if operations.value.is_empty() {
            operations.retire(accounting)?;
        } else {
            self.checkout.mutate_owned_with_accounting(operations, accounting, cancellation).await?;
        }
        let records_result = source_records.retire(accounting);
        let root_result = source_root.retire(accounting);
        records_result.and(root_result).map_err(WorkspaceError::from)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn apply_authored_bulk_with_native_work(
        &mut self, operations: Vec<AuthoredMutation>, allocation: crate::NativeAllocation,
        original: &crate::NativeMaterializationWork, cancellation: &crate::CancellationToken,
    ) -> Result<(), WorkspaceError> {
        self.checkout.apply_authored_bulk_transaction_with_native_work(
            operations, allocation, original, cancellation,
        ).await.map_err(WorkspaceError::from)
    }

    /// Streams immutable content under the supplied work limits.
    pub async fn stage_content<R: crate::kernel::AsyncBlobSource, W: crate::facade::FsWork>(
        &self, source: &mut R, maximum_source_bytes: u64,
        work: W, cancellation: &crate::CancellationToken,
    ) -> W::Output<crate::StagedContent, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.checkout.stage_content_with_accounting(
            source, maximum_source_bytes, &mut accounting, cancellation,
        ).await.map_err(WorkspaceError::from);
        work.finish(result, &accounting)
    }

    /// Creates or atomically replaces one complete UTF-8 file.
    pub async fn write_text(&mut self, path: &str, text: &str) -> Result<(), WorkspaceError> {
        self.write(path, Bytes::copy_from_slice(text.as_bytes()))
            .await
    }

    /// Creates or atomically replaces one complete file.
    pub async fn write(&mut self, path: &str, bytes: Bytes) -> Result<(), WorkspaceError> {
        self.write_measured(path, bytes, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await
            .map(|_| ())
            .map_err(|failure| failure.error)
    }

    /// Creates or replaces the same complete file under exact operation bounds.
    pub async fn write_measured(
        &mut self,
        path: &str,
        bytes: impl Into<acyclic_stream::OwnedBytes>,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(budget, cancellation);
        let result = self.write_with_accounting(path, bytes.into(), &mut accounting, cancellation).await;
        result.map(|()| accounting.spent())
            .map_err(|error| crate::OperationFailure::new(error, accounting.spent()))
    }

    async fn write_with_accounting(
        &mut self, path: &str, bytes: acyclic_stream::OwnedBytes,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let path = self.mutation_path_with_accounting(path, accounting, cancellation)?;
        let existing = self.checkout.lookup_no_follow_with_accounting(&path.value, accounting, cancellation).await?.0.record;
        let operations = match existing {
            None => {
                let operation = crate::kernel::Owned::new(
                    AuthoredMutation::CreateFile { path: path.value, bytes, metadata: FileMetadata::default() },
                    path.allocation, path.bytes,
                );
                let mut operations = crate::kernel::Owned::allocate(1, accounting)?;
                Self::push_workspace_mutation(&mut operations, operation, accounting)?;
                operations
            }
            Some(record) if record.kind == FileKind::Regular => {
                let (copied, allocation) = path.value.clone_with_accounting(accounting, cancellation)
                    .map_err(WorkspaceError::from)?;
                let copied_bytes = copied.retained_bytes();
                let copied = crate::kernel::Owned::new(
                    AuthoredMutation::Resize { path: copied, logical_bytes: 0 }, allocation, copied_bytes,
                );
                let mut operations = crate::kernel::Owned::allocate(2, accounting)?;
                Self::push_workspace_mutation(&mut operations, copied, accounting)?;
                Self::push_workspace_mutation(&mut operations, crate::kernel::Owned::new(
                    AuthoredMutation::Write { path: path.value, offset: 0, bytes }, path.allocation, path.bytes,
                ), accounting)?;
                operations
            }
            Some(_) => { path.retire(accounting)?; return Err(WorkspaceError::NotRegularFile); }
        };
        self.apply_workspace_authored_with_accounting(operations, accounting, cancellation).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn write_with_native_work(
        &mut self, path: &str, bytes: acyclic_stream::OwnedBytes,
        original: &crate::NativeMaterializationWork, cancellation: &crate::CancellationToken,
    ) -> Result<(), (WorkspaceError, Option<crate::NativeAllocation>)> {
        self.write_with_accounting(path, bytes,
            &mut crate::kernel::allocation::KernelAccounting::native(original, cancellation), cancellation)
            .await.map_err(|error| (error, None))
    }

    /// Creates or replaces one complete file by concatenating already staged
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
                bytes: acyclic_stream::OwnedBytes::new(),
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
    pub async fn remove(&mut self, path: &str) -> Result<(), WorkspaceError> {
        self.remove_measured(path, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await
            .map(|_| ())
            .map_err(|failure| failure.error)
    }

    /// Removes the same authenticated binding under cumulative operation bounds.
    pub async fn remove_measured(
        &mut self,
        path: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
            budget, cancellation,
        );
        let result = self.remove_with_accounting(path, &mut accounting, cancellation).await;
        result.map(|()| accounting.spent())
            .map_err(|error| crate::OperationFailure::new(error, accounting.spent()))
    }

    async fn remove_with_accounting(
        &mut self,
        path: &str,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let path = self.mutation_path_with_accounting(path, accounting, cancellation)?;
        let existing = self.checkout.lookup_no_follow_with_accounting(
            &path.value, accounting, cancellation,
        ).await?.0.record.ok_or(WorkspaceError::NotFound)?;
        let operation = crate::kernel::Owned::new(
            AuthoredMutation::Remove { path: path.value, expected_file_id: Some(existing.file_id) },
            path.allocation, path.bytes,
        );
        let mut operations = crate::kernel::Owned::allocate(1, accounting)?;
        Self::push_workspace_mutation(&mut operations, operation, accounting)?;
        self.apply_workspace_authored_with_accounting(
            operations, accounting, cancellation,
        ).await
    }

    /// Removes one binding only when its authenticated identity still matches.
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
                bytes: acyclic_stream::OwnedBytes::new(),
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
    pub async fn rename(&mut self, source: &str, destination: &str) -> Result<(), WorkspaceError> {
        self.rename_with_replace(source, destination, true).await
    }

    /// Atomically renames one namespace binding with an exact replacement policy.
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

    /// Changes one regular file's logical length while preserving sparse layout.
    pub async fn resize(&mut self, path: &str, logical_bytes: u64) -> Result<(), WorkspaceError> {
        let path = customer_path(path, self.checkout.volume_config())?;
        self.apply(vec![AuthoredMutation::Resize {
            path,
            logical_bytes,
        }])
        .await
    }

    /// Punches a hole or installs allocated zeros over one exact range.
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

    /// Performs the bounded commit operation.
    pub async fn commit<E, F, Fut>(
        &mut self, context: Option<(crate::Digest, acyclic_stream::OwnedBytes, crate::PublicationPermit)>,
        permit: crate::PublicationPermit, budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken, mut before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, E>
    where
        E: From<WorkspaceError> + From<FsError> + Send,
        F: FnMut(crate::WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = crate::MeasuredResult<crate::OperationReceipt<()>, E>> + ProviderTask,
    {
        let (context, operation_evidence, original_fingerprint_permit) = match context {
            Some((context, evidence, permit)) => (Some(context), Some(evidence), Some(permit)),
            None => (None, None, None),
        };
        crate::heap_future::in_heap_measured_with_error(move |budget| async move {
            let (outcome, mut work) = if self.requires_rebase {
                let receipt = self.checkout.retry_stale_commit(
                    self.idempotency_key.operation_id(), permit, budget, cancellation,
                    &mut before_publication, None, context, operation_evidence, original_fingerprint_permit,
                ).await?;
                (receipt.value, receipt.work)
            } else {
                let receipt = self.checkout.commit(
                    self.idempotency_key.operation_id(), context, operation_evidence,
                    original_fingerprint_permit, permit, budget, cancellation, &mut before_publication,
                ).await?;
                (Some(receipt.value), receipt.work)
            };
            let value = if let Some(value) = outcome.and_then(|outcome| self.commit_outcome_value(outcome)) {
                value
            } else {
                let actual = self.workspace.head(
                    work.remaining(budget).map_err(|error| crate::OperationFailure::new(E::from(WorkspaceError::from(error)), work))?,
                    cancellation,
                ).await.map_err(|failure| failure.map_with_prior_work(work, std::convert::identity).map_error(E::from))?;
                work = work.checked_add(actual.work)
                    .map_err(|error| crate::OperationFailure::new(E::from(WorkspaceError::from(error)), work))?;
                TransactionCommit::Conflict { actual: actual.value }
            };
            Ok(crate::OperationReceipt { value, work })
        }, budget, cancellation, E::from(WorkspaceError::Cancelled(crate::CancellationError)), None,
            |error| E::from(WorkspaceError::from(error))).await
    }

    fn commit_outcome_value(
        &self, outcome: CheckoutCommitOutcome,
    ) -> Option<TransactionCommit<A, O>> {
        Some(match outcome {
            CheckoutCommitOutcome::Committed { generation_id, .. } => {
                TransactionCommit::Committed(Generation {
                    workspace: self.workspace.clone(), id: generation_id,
                    publication_closure: self.checkout.publication_closure(generation_id),
                })
            }
            CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => {
                TransactionCommit::AlreadyCommitted(Generation {
                    workspace: self.workspace.clone(), id: generation_id,
                    publication_closure: self.checkout.publication_closure(generation_id),
                })
            }
            CheckoutCommitOutcome::Conflict { .. } => return None,
            CheckoutCommitOutcome::Fenced { .. } => TransactionCommit::Fenced,
            CheckoutCommitOutcome::IdempotencyConflict { .. } => TransactionCommit::IdempotencyConflict,
        })
    }

    /// Commits on the original native scope with the supplied publication guard.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn commit_with_native_work<'a, E, F, Fut>(
        &mut self, context: Option<(crate::Digest, acyclic_stream::OwnedBytes, crate::PublicationPermit)>,
        permit: crate::PublicationPermit, original: &'a crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken, before_publication: &mut F,
    ) -> Result<TransactionCommit<A, O>, E>
    where
        E: From<FsError> + Send + 'static,
        F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let (context, operation_evidence, original_fingerprint_permit) = match context {
            Some((context, evidence, permit)) => (Some(context), Some(evidence), Some(permit)),
            None => (None, None, None),
        };
        let outcome = if self.requires_rebase {
            self.checkout.retry_stale_commit_with_native_work(self.idempotency_key.operation_id(), permit, original, cancellation, before_publication, None, context, operation_evidence, original_fingerprint_permit).await?
        } else {
            Some(self.checkout.commit_with_native_work(self.idempotency_key.operation_id(), context, operation_evidence, original_fingerprint_permit, permit, original, cancellation, (*before_publication).clone()).await?)
        };
        if let Some(value) = outcome.and_then(|outcome| self.commit_outcome_value(outcome)) {
            return Ok(value);
        }
        let actual = self.workspace.volume.checkout_with_native_work(
            GenerationSelector::Head, CheckoutMode::read_only_pinned(), original, cancellation,
        ).await.map_err(E::from)?;
        Ok(TransactionCommit::Conflict {
            actual: Generation {
                workspace: self.workspace.clone(), id: actual.generation_id(),
                publication_closure: None,
            },
        })
    }

    /// Safely advances this retained sparse candidate to the current workspace
    pub async fn rebase(
        &mut self,
        maximum_conflicts: u32,
    ) -> Result<TransactionRebase<A, O>, WorkspaceError> {
        async move {
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
                        TransactionRebase::Rebased(Generation { workspace: self.workspace.clone(), id: generation, publication_closure: None })
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
            .await
    }

    async fn apply(&mut self, operations: Vec<AuthoredMutation>) -> Result<(), WorkspaceError> {
        self.checkout.apply_authored_transaction(
            operations, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).await.map(|_| ()).map_err(|failure| WorkspaceError::from(failure.error))
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

    /// Actual retained diff buffers, excluding shared immutable endpoints.
    pub fn retained_bytes(&self) -> u64 {
        workspace_vec_bytes(&self.changes.files)
            + workspace_vec_bytes(&self.changes.bindings)
            + self.changes.bindings.iter().map(|binding| {
                crate::foundation::usize_to_u64(binding.name.retained_bytes())
                    + binding.before.as_ref().map_or(0, |entry| {
                        crate::foundation::usize_to_u64(entry.name.retained_bytes())
                    })
                    + binding.after.as_ref().map_or(0, |entry| {
                        crate::foundation::usize_to_u64(entry.name.retained_bytes())
                    })
            }).sum::<u64>()
    }
}

fn sorted_changed_file_ids(
    file_ids: impl IntoIterator<Item = FileId>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<FileId>, KernelOwnedAllocation), WorkspaceError> {
    let mut ids = (Vec::<FileId>::new(), accounting.claim_owned(0, 0)?);
    for file_id in file_ids {
        accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        let mut low = 0;
        let mut high = ids.0.len();
        let mut present = false;
        while low < high {
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let middle = low + (high - low) / 2;
            match ids.0[middle].cmp(&file_id) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => { present = true; break; }
            }
        }
        if present { continue; }
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

fn changed_path_position<'a>(
    paths: &[ChangedPath],
    components: impl Iterator<Item = &'a LogicalName> + Clone,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(usize, bool), WorkspaceError> {
    let mut low = 0;
    let mut high = paths.len();
    while low < high {
        accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        let middle = low + (high - low) / 2;
        let mut left = paths[middle].path.components().iter();
        let mut right = components.clone();
        let ordering = loop {
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            match (left.next(), right.next()) {
                (Some(left), Some(right)) => match left.cmp(right) {
                    std::cmp::Ordering::Equal => {}
                    ordering => break ordering,
                },
                (Some(_), None) => break std::cmp::Ordering::Greater,
                (None, Some(_)) => break std::cmp::Ordering::Less,
                (None, None) => break std::cmp::Ordering::Equal,
            }
        };
        match ordering {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok((middle, true)),
        }
    }
    Ok((low, false))
}

fn set_changed_record(
    paths: &mut [ChangedPath], index: usize, record: Option<crate::kernel::FileRecord>,
    before: bool, accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    let copied = crate::WorkCounters {
        bytes_copied: std::mem::size_of::<Option<crate::kernel::FileRecord>>() as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    if before { paths[index].before = record; } else { paths[index].after = record; }
    accounting.record_completed(copied)?;
    Ok(())
}

fn insert_changed_record(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation), index: usize,
    mut path: (NamespacePath, KernelOwnedAllocation), record: Option<crate::kernel::FileRecord>,
    before: bool, accounting: &mut KernelAccounting<'_>,
) -> Result<(), WorkspaceError> {
    let copied = crate::WorkCounters {
        bytes_copied: ((paths.0.len() - index + 1) * std::mem::size_of::<ChangedPath>()) as u64,
        ..crate::WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    paths.1.absorb(&mut path.1)?;
    paths.0.insert(index, ChangedPath {
        path: path.0, before: if before { record } else { None },
        after: if before { None } else { record },
    });
    accounting.record_completed(copied)?;
    Ok(())
}

fn merge_changed_record(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation), path: &NamespacePath,
    record: Option<crate::kernel::FileRecord>, before: bool,
    accounting: &mut KernelAccounting<'_>, cancellation: &crate::CancellationToken,
) -> Result<(), WorkspaceError> {
    let (index, present) = changed_path_position(&paths.0, path.components().iter(), accounting)?;
    if present { return set_changed_record(&mut paths.0, index, record, before, accounting); }
    reverse_grow(&mut paths.0, &mut paths.1, accounting)?;
    let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
    insert_changed_record(paths, index, (path, allocation), record, before, accounting)
}

fn merge_binding_endpoint(
    paths: &mut (Vec<ChangedPath>, KernelOwnedAllocation), directories: &NamespaceRecords,
    change: &crate::DirectoryBindingChange, limits: crate::model::VolumeLimits, before: bool,
    accounting: &mut KernelAccounting<'_>, cancellation: &crate::CancellationToken,
) -> Result<(), WorkspaceError> {
    let Some(parents) = namespace_record_entries(directories, &change.directory_id, accounting)?
        else { return Ok(()); };
    let binding = if before { change.before.as_ref() } else { change.after.as_ref() };
    let record = match binding {
        Some(entry) => namespace_record_entries(directories, &entry.file_id, accounting)?
            .and_then(|records| records.first().map(|(_, record)| *record)),
        None => None,
    };
    for (directory, _) in parents {
        accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        let components = directory.components().iter().chain(std::iter::once(&change.name));
        let (index, present) = changed_path_position(&paths.0, components, accounting)?;
        if present {
            set_changed_record(&mut paths.0, index, record, before, accounting)?;
        } else {
            reverse_grow(&mut paths.0, &mut paths.1, accounting)?;
            let (path, allocation) = directory.child_with_accounting(
                &change.name, limits, accounting, cancellation,
            )?;
            insert_changed_record(paths, index, (path, allocation), record, before, accounting)?;
        }
    }
    Ok(())
}



// Query tuples drop their data before the allocation guard.
fn reverse_drop<T>(owned: (T, KernelOwnedAllocation), bytes: u64, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    let (value, mut allocation) = owned;
    drop(value);
    accounting.release_owned(&mut allocation, bytes)
}

fn reverse_grow<T>(values: &mut Vec<T>, allocation: &mut KernelOwnedAllocation, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    if values.len() < values.capacity() { return Ok(()); }
    let count = values.capacity().checked_mul(2).map(|count| count.max(1)).ok_or(crate::WorkError::Overflow)?;
    let requested = count.checked_mul(std::mem::size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?;
    let copied = crate::WorkCounters {
        bytes_copied: values.len().checked_mul(std::mem::size_of::<T>())
            .and_then(|bytes| u64::try_from(bytes).ok()).ok_or(crate::WorkError::Overflow)?,
        ..crate::WorkCounters::UNCHARGED
    };
    let guard = accounting.claim_owned(requested, 1)?;
    let mut replacement = (Vec::new(), guard);
    if replacement.0.try_reserve_exact(count).is_err() {
        reverse_drop(replacement, requested, accounting)?;
        return Err(FsError::HandleAllocationFailed);
    }
    let actual = workspace_vec_bytes(&replacement.0);
    if let Err(error) = accounting.reconcile_owned(&mut replacement.1, requested, actual) {
        reverse_drop(replacement, actual, accounting)?;
        return Err(error);
    }
    if let Err(error) = accounting.admit(copied) {
        reverse_drop(replacement, actual, accounting)?;
        return Err(error);
    }
    if let Err(error) = allocation.absorb(&mut replacement.1) {
        reverse_drop(replacement, actual, accounting)?;
        return Err(error);
    }
    let old_bytes = workspace_vec_bytes(values);
    replacement.0.append(values);
    let completed = accounting.record_completed(copied);
    let old = std::mem::replace(values, replacement.0);
    drop(old);
    accounting.release_owned(allocation, old_bytes)?;
    completed
}

fn reverse_insert(records: &mut NamespaceRecords, allocation: &mut KernelOwnedAllocation, path: (NamespacePath, KernelOwnedAllocation),
    record: crate::kernel::FileRecord, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    let mut index = 0;
    while let Some((file_id, _)) = records.get(index) {
        accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        if *file_id >= record.file_id { break; }
        index += 1;
    }
    if records.get(index).is_none_or(|(id, _)| *id != record.file_id) {
        reverse_grow(records, allocation, accounting)?;
        accounting.charge(crate::WorkCounters { bytes_copied: ((records.len() - index)
            * std::mem::size_of::<(FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>)>()) as u64,
            ..crate::WorkCounters::UNCHARGED })?;
        records.insert(index, (record.file_id, Vec::new()));
    }
    let entries = &mut records[index].1;
    reverse_grow(entries, allocation, accounting)?;
    let mut path = path;
    allocation.absorb(&mut path.1)?;
    entries.push((path.0, record));
    Ok(())
}

fn reverse_sort(records: &mut NamespaceRecords, accounting: &mut KernelAccounting<'_>) -> Result<(), FsError> {
    for (_, entries) in records {
        for index in 1..entries.len() {
            let mut current = index;
            while current != 0 {
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                if entries[current - 1].0 <= entries[current].0 { break; }
                accounting.charge(crate::WorkCounters { bytes_copied:
                    (2 * std::mem::size_of::<(NamespacePath, crate::kernel::FileRecord)>()) as u64,
                    ..crate::WorkCounters::UNCHARGED })?;
                entries.swap(current - 1, current);
                current -= 1;
            }
        }
    }
    Ok(())
}

fn reverse_contains(ids: &[FileId], id: FileId, accounting: &mut KernelAccounting<'_>) -> Result<bool, FsError> {
    for candidate in ids {
        accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        if *candidate == id { return Ok(true); }
        if *candidate > id { break; }
    }
    Ok(false)
}

async fn namespace_records_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, root: &crate::kernel::GenerationRoot,
    generation_root: crate::ObjectId, ids: &[FileId], maximum_entries: u32,
    accounting: &mut KernelAccounting<'_>, cancellation: &crate::CancellationToken,
) -> Result<(NamespaceRecordSearch, KernelOwnedAllocation), FsError> {
    accounting.check()?;
    let allocation = accounting.claim_owned(0, 0)?;
    let mut matches = (NamespaceRecordSearch { records: NamespaceRecords::default(), complete: ids.is_empty() }, allocation);
    if ids.is_empty() || maximum_entries == 0 { return Ok(matches); }
    let limits = volume.config.limits;
    let generation = GenerationId::new(generation_root.digest);
    let cache_key = crate::path_index::cache_key(
        generation, root.root_file_id, root.file_table, ids, accounting, cancellation,
    )?;
    let (bytes, allocation) = volume.fs.path_index().load(cache_key, accounting, cancellation)?;
    let bytes = (bytes, allocation);
    if let Some(payload) = bytes.0.as_ref() {
        let (cached, allocation) = crate::path_index::decode(
            payload, generation, ids, limits, accounting, cancellation,
        )?;
        let mut cached = (cached, allocation);
        if let Some(paths) = cached.0.as_ref().filter(|paths| paths.len() <= maximum_entries as usize) {
            let allocation = accounting.claim_owned(0, 0)?;
            let mut copied = (Vec::new(), allocation);
            for (_, path) in paths {
                reverse_grow(&mut copied.0, &mut copied.1, accounting)?;
                let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
                let mut path = (path, allocation);
                copied.1.absorb(&mut path.1)?;
                copied.0.push(path.0);
            }
            let (lookup, allocation) = crate::facade::lookup_file_records_to_file_root_with_accounting(
                volume, root, &copied.0, accounting, cancellation,
            ).await?;
            let lookup = (lookup, allocation);
            let mut valid = paths.len() == lookup.0.entries.len();
            for ((expected, _), entry) in paths.iter().zip(&lookup.0.entries) {
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                if entry.record.is_none_or(|record| record.file_id != *expected) { valid = false; break; }
            }
            let copied_bytes = workspace_vec_bytes(&copied.0)
                + copied.0.iter().map(NamespacePath::retained_bytes).sum::<u64>();
            reverse_drop(copied, copied_bytes, accounting)?;
            let lookup_bytes = lookup.0.retained_allocation_bytes;
            if valid {
                let paths = cached.0.take().expect("verified cache payload");
                let vector_bytes = workspace_vec_bytes(&paths);
                for ((_, path), entry) in paths.into_iter().zip(&lookup.0.entries) {
                    let allocation = match &mut cached.1 {
                        KernelOwnedAllocation::Standalone => KernelOwnedAllocation::Standalone,
                        #[cfg(not(target_arch = "wasm32"))]
                        KernelOwnedAllocation::NativeOriginal(guard) => KernelOwnedAllocation::NativeOriginal(
                            guard.split_off(path.retained_bytes())?),
                    };
                    let path = (path, allocation);
                    let record = entry.record.ok_or(FsError::NotFound)?;
                    reverse_insert(&mut matches.0.records, &mut matches.1, path, record, accounting)?;
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
        let cached_bytes = cached.0.as_ref().map_or(0, |paths| workspace_vec_bytes(paths)
            + paths.iter().map(|(_, path)| path.retained_bytes()).sum::<u64>());
        reverse_drop(cached, cached_bytes, accounting)?;
    }
    let bytes_count = bytes.0.as_ref().map_or(0, workspace_vec_bytes);
    reverse_drop(bytes, bytes_count, accounting)?;
    let namespace_root = NamespacePath::new(Vec::new(), limits).map_err(FsError::from)?;
    if let Some(record) = crate::facade::lookup_no_follow_to_file_root_with_accounting(
        volume, root, &namespace_root, accounting, cancellation,
    ).await?.record {
        if reverse_contains(ids, record.file_id, accounting)? {
            let allocation = accounting.claim_owned(0, 0)?;
            reverse_insert(&mut matches.0.records, &mut matches.1,
                (NamespacePath::new(Vec::new(), limits).map_err(FsError::from)?, allocation), record, accounting)?;
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
            let (page, allocation) = crate::facade::list_directory_records_to_file_root_with_accounting(
                volume, root, &directory.0, after.as_ref().map(|name| &name.0),
                maximum_entries.saturating_sub(examined).saturating_add(1).min(1_024),
                accounting, cancellation,
            ).await?;
            let mut page = (page, allocation);
            for entry in &page.0.entries {
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                if examined == maximum_entries {
                    reverse_sort(&mut matches.0.records, accounting)?;
                    let page_bytes = workspace_vec_bytes(&page.0.entries)
                        + page.0.entries.iter().map(|entry| entry.name.retained_bytes() as u64).sum::<u64>();
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
                let found = reverse_contains(ids, entry.record.file_id, accounting)?;
                let is_directory = entry.record.kind == FileKind::Directory;
                if !found && !is_directory { continue; }
                let (path, allocation) = directory.0.child_with_accounting(
                    &entry.name, limits, accounting, cancellation,
                )?;
                let path = (path, allocation);
                if found && is_directory {
                    let (copy, allocation) = path.0.clone_with_accounting(accounting, cancellation)?;
                    reverse_insert(&mut matches.0.records, &mut matches.1,
                        (copy, allocation), entry.record, accounting)?;
                } else if found {
                    reverse_insert(&mut matches.0.records, &mut matches.1, path, entry.record, accounting)?;
                    continue;
                }
                reverse_grow(&mut pending.0, &mut pending.1, accounting)?;
                pending.0.push(path);
            }
            let next = if page.0.has_more {
                let name = page.0.entries.pop().ok_or(FsError::NotFound)?.name;
                let allocation = match &mut page.1 {
                    KernelOwnedAllocation::Standalone => KernelOwnedAllocation::Standalone,
                    #[cfg(not(target_arch = "wasm32"))]
                    KernelOwnedAllocation::NativeOriginal(guard) => KernelOwnedAllocation::NativeOriginal(
                        guard.split_off(name.retained_bytes() as u64)?),
                };
                Some((name, allocation))
            } else { None };
            let page_bytes = workspace_vec_bytes(&page.0.entries)
                + page.0.entries.iter().map(|entry| entry.name.retained_bytes() as u64).sum::<u64>();
            reverse_drop(page, page_bytes, accounting)?;
            if let Some(old) = after.take() {
                let bytes = old.0.retained_bytes() as u64;
                reverse_drop(old, bytes, accounting)?;
            }
            after = next;
            if after.is_none() { break; }
        }
        let bytes = directory.0.retained_bytes();
        reverse_drop(directory, bytes, accounting)?;
    }
    let pending_bytes = workspace_vec_bytes(&pending.0);
    reverse_drop(pending, pending_bytes, accounting)?;
    reverse_sort(&mut matches.0.records, accounting)?;
    let (bytes, allocation) = crate::path_index::encode_records(
        generation, ids, &matches.0.records, accounting, cancellation,
    )?;
    let bytes = (bytes, allocation);
    if let Some(payload) = &bytes.0 {
        volume.fs.path_index().store(cache_key, payload, accounting, cancellation)?;
    }
    let bytes_count = bytes.0.as_ref().map_or(0, workspace_vec_bytes);
    reverse_drop(bytes, bytes_count, accounting)?;
    matches.0.complete = true;
    Ok(matches)
}

/// Reverse aliases of the actual supplied root, retaining their original allocation.
pub(crate) async fn reverse_paths_to_file_root_with_accounting<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>, root: &crate::kernel::GenerationRoot, generation_root: crate::ObjectId,
    file_id: FileId, maximum_entries: u32, accounting: &mut KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<(Vec<NamespacePath>, KernelOwnedAllocation), WorkspaceError> {
    let mut search = namespace_records_to_file_root_with_accounting(
        volume, root, generation_root, &[file_id], maximum_entries, accounting, cancellation,
    ).await?;
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

struct NamespaceRecordSearch {
    records: NamespaceRecords,
    complete: bool,
}

type NamespaceRecords = Vec<(FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>)>;

fn namespace_record_entries<'a>(
    records: &'a NamespaceRecords, file_id: &FileId, accounting: &mut KernelAccounting<'_>,
) -> Result<Option<&'a Vec<(NamespacePath, crate::kernel::FileRecord)>>, WorkspaceError> {
    let mut low = 0;
    let mut high = records.len();
    while low < high {
        accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
        let middle = low + (high - low) / 2;
        match records[middle].0.cmp(file_id) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => return Ok(Some(&records[middle].1)),
        }
    }
    Ok(None)
}

fn namespace_records_bytes(records: &NamespaceRecords) -> u64 {
    workspace_vec_bytes(records) + records.iter().map(|(_, records)|
        workspace_vec_bytes(records) + records.iter().map(|(path, _)| path.retained_bytes()).sum::<u64>()
    ).sum::<u64>()
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> ChangeSet<A, O> {
    /// Resolves changed paths under one cumulative budget and cancellation token.
    #[allow(
        clippy::too_many_lines,
        reason = "one bounded resolution of changed paths shares a single cumulative work budget"
    )]
    pub async fn changed_paths(
        &self,
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Vec<ChangedPath>>, WorkspaceError> {
        let mut work = self.work;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = async {
            work.verify(budget)?;
            // Only ordinary resolution continues the retained diff's original span.
            allocations.claim_bytes(self.retained_bytes(), 0, &mut work, budget)
                .map_err(workspace_allocation_error)?;
            let mut accounting = KernelAccounting::borrowed_standalone(&mut allocations, &mut work, budget, cancellation);
            self.changed_paths_with_accounting(maximum_entries, &mut accounting, cancellation).await.map(|owned| owned.0)
        }.await;
        result.map(|value| crate::OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))
    }

    /// Borrows this exact delta while resolving paths on the original native invocation.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn changed_paths_with_native_work(
        &self, maximum_entries: u32, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(Vec<ChangedPath>, crate::NativeAllocation), (WorkspaceError, Option<crate::NativeAllocation>)> {
        let mut accounting = KernelAccounting::native(original, cancellation);
        match self.changed_paths_with_accounting(maximum_entries, &mut accounting, cancellation).await {
            Ok((value, KernelOwnedAllocation::NativeOriginal(allocation))) => Ok((value, allocation)),
            Ok(_) => Err((WorkspaceError::from(crate::NativeTreeMaterializationError::InvalidLayout), None)),
            Err(error) => Err((error, None)),
        }
    }

    async fn changed_paths_with_accounting(
        &self, maximum_entries: u32, accounting: &mut KernelAccounting<'_>,
        cancellation: &crate::CancellationToken,
    ) -> Result<(Vec<ChangedPath>, KernelOwnedAllocation), WorkspaceError> {
        accounting.check()?;
        if self.changes.truncated || (maximum_entries == 0
            && (!self.changes.files.is_empty() || !self.changes.bindings.is_empty())) {
            return Err(WorkspaceError::ChangedPathLimit);
        }
        let file_ids = sorted_changed_file_ids(
            self.changes.files.iter().map(|change| change.file_id)
                .chain(self.changes.bindings.iter().map(|change| change.directory_id))
                .chain(self.changes.bindings.iter().flat_map(|change| {
                    change.before.iter().chain(change.after.iter()).map(|entry| entry.file_id)
                })), accounting,
        )?;
        let before = self.from.namespace_records_with_accounting(&file_ids.0, maximum_entries, accounting, cancellation).await?;
        let after = self.to.namespace_records_with_accounting(&file_ids.0, maximum_entries, accounting, cancellation).await?;
        if !before.0.complete || !after.0.complete { return Err(WorkspaceError::ChangedPathLimit); }
        let mut paths = (Vec::new(), accounting.claim_owned(0, 0)?);
        for records in before.0.records.iter().map(|(_, records)| records) {
            for (path, record) in records {
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                merge_changed_record(&mut paths, path, Some(*record), true, accounting, cancellation)?;
            }
        }
        for records in after.0.records.iter().map(|(_, records)| records) {
            for (path, record) in records {
                accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                merge_changed_record(&mut paths, path, Some(*record), false, accounting, cancellation)?;
            }
        }
        for change in &self.changes.bindings {
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            merge_binding_endpoint(&mut paths, &before.0.records, change, self.from.workspace.volume.config.limits, true, accounting, cancellation)?;
            merge_binding_endpoint(&mut paths, &after.0.records, change, self.to.workspace.volume.config.limits, false, accounting, cancellation)?;
        }
        let before_bytes = namespace_records_bytes(&before.0.records);
        let after_bytes = namespace_records_bytes(&after.0.records);
        let id_bytes = workspace_vec_bytes(&file_ids.0);
        reverse_drop(before, before_bytes, accounting)?;
        reverse_drop(after, after_bytes, accounting)?;
        reverse_drop(file_ids, id_bytes, accounting)?;
        let mut retained = 0;
        let mut retired_paths = 0_u64;
        for index in 0..paths.0.len() {
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
            let changed = match (&paths.0[index].before, &paths.0[index].after) {
                (Some(before), Some(after)) => before.kind != after.kind || before.metadata != after.metadata
                    || (before.kind != FileKind::Directory && before.payload != after.payload),
                (None, None) => false,
                _ => true,
            };
            if changed {
                if retained != index {
                    let copied = crate::WorkCounters { bytes_copied: (2 * std::mem::size_of::<ChangedPath>()) as u64, ..crate::WorkCounters::UNCHARGED };
                    accounting.admit(copied)?;
                    paths.0.swap(retained, index);
                    accounting.record_completed(copied)?;
                }
                retained += 1;
            } else {
                retired_paths = retired_paths.checked_add(paths.0[index].path.retained_bytes()).ok_or(crate::WorkError::Overflow)?;
            }
        }
        paths.0.truncate(retained);
        accounting.release_owned(&mut paths.1, retired_paths)?;
        accounting.check()?;
        Ok(paths)
    }

    /// Composes contiguous immutable deltas into their exact net semantic
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

    fn validate_workspaces(&self) -> Result<(), WorkspaceError> {
        self.validate_bounds()?;
        if !self.source.volume.fs.same_deployment(&self.target.volume.fs) {
            return Err(WorkspaceError::ForeignGeneration);
        }
        if self.source.volume.config != self.target.volume.config {
            return Err(WorkspaceError::IncompatibleWorkspace);
        }
        self.source.volume.config.validate().map_err(FsError::from)?;
        Ok(())
    }

    fn validate_endpoints(
        &self,
        source: &Generation<A, O>,
        target: &Generation<A, O>,
    ) -> Result<(), WorkspaceError> {
        self.validate_workspaces()?;
        if source.workspace.id != self.source.id || target.workspace.id != self.target.id
            || !self.source.volume.fs.same_deployment(&source.workspace.volume.fs)
            || !self.target.volume.fs.same_deployment(&target.workspace.volume.fs)
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
    /// Authenticates endpoints and discovers one bounded common ancestor
    pub async fn plan(self) -> Result<JoinPlan<A, O>, WorkspaceError> {
        self.validate_workspaces()?;
        let cancellation = CancellationToken::new();
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED, &cancellation,
        );
        let source_head = self.source.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        let (target_head_id, target_authority_head) = self.target.volume.fs
            .workspace_head_state(&self.target.volume, &mut accounting, &cancellation)
            .await?;
        let target_head = self.target.generation(target_head_id, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?;
        self.plan_generations_with_accounting(
            source_head, target_head, target_authority_head, &mut accounting, &cancellation,
        ).await
    }

    /// Plans a join from exact immutable endpoints while requiring `target`
    pub async fn plan_at(
        self,
        source: &Generation<A, O>,
        target: &Generation<A, O>,
    ) -> Result<JoinPlan<A, O>, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED, &cancellation,
        );
        self.plan_at_with_accounting(source, target, &mut accounting, &cancellation)
            .await.map(|(plan, allocation)| { drop(allocation); plan })
    }

    /// Plans the same bounded join on the caller's original native invocation.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn plan_at_with_native_work(
        self,
        source: &Generation<A, O>,
        target: &Generation<A, O>,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(JoinPlan<A, O>, crate::NativeAllocation), (WorkspaceError, Option<crate::NativeAllocation>)> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        let result = self.plan_at_with_accounting(source, target, &mut accounting, cancellation).await;
        result.map(|(plan, allocation)| match allocation {
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => (plan, allocation),
            crate::kernel::allocation::KernelOwnedAllocation::Standalone =>
                unreachable!("native planning retains original invocation custody"),
        }).map_err(|error| (error, None))
    }

    async fn plan_at_with_accounting(
        self,
        source: &Generation<A, O>,
        target: &Generation<A, O>,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(JoinPlan<A, O>, crate::kernel::allocation::KernelOwnedAllocation), WorkspaceError> {
        self.validate_endpoints(source, target)?;
        let allocation = accounting.claim_owned(0, 0)?;
        let (target_head_id, target_authority_head) = self.target.volume.fs
            .workspace_head_state(&self.target.volume, accounting, cancellation).await?;
        if target.id != target_head_id { return Err(WorkspaceError::StaleTarget); }
        let copied = crate::WorkCounters {
            bytes_copied: (2 * std::mem::size_of::<Generation<A, O>>()) as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        let source = source.clone();
        let target = target.clone();
        accounting.record_completed(copied)?;
        let plan = self.plan_generations_with_accounting(
            source, target, target_authority_head, accounting, cancellation,
        ).await?;
        Ok((plan, allocation))
    }

    /// Plans against caller-pinned immutable source and target generations.
    pub async fn plan_pinned(
        self,
        source_head: Generation<A, O>,
        target_head: Generation<A, O>,
    ) -> Result<JoinPlan<A, O>, WorkspaceError> {
        self.validate_endpoints(&source_head, &target_head)?;
        let cancellation = CancellationToken::new();
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED, &cancellation,
        );
        let (current_target, target_authority_head) = self.target.volume.fs
            .workspace_head_state(&self.target.volume, &mut accounting, &cancellation).await?;
        if current_target != target_head.id { return Err(WorkspaceError::StaleGeneration); }
        self.plan_generations_with_accounting(
            source_head, target_head, target_authority_head, &mut accounting, &cancellation,
        ).await
    }

    pub(crate) async fn plan_generations(
        self,
        source_head: Generation<A, O>,
        target_head: Generation<A, O>,
        target_authority_head: crate::Head,
    ) -> Result<JoinPlan<A, O>, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED, &cancellation,
        );
        self.plan_generations_with_accounting(
            source_head, target_head, target_authority_head, &mut accounting, &cancellation,
        ).await
    }

    async fn plan_generations_with_accounting(
        self,
        source_head: Generation<A, O>,
        target_head: Generation<A, O>,
        target_authority_head: crate::Head,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<JoinPlan<A, O>, WorkspaceError> {
        self.validate_endpoints(&source_head, &target_head)?;
        let base = self.source.volume.fs.workspace_common_ancestor_with_accounting(
            &source_head, &target_head, self.maximum_generations, accounting, cancellation,
        ).await?;
        let copied = crate::WorkCounters {
            bytes_copied: std::mem::size_of::<JoinPlan<A, O>>() as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        let plan = JoinPlan {
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
        };
        accounting.record_completed(copied)?;
        Ok(plan)
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
    pub fn commit_witness(
        &self,
        application: &JoinApplication<A, O>,
        idempotency_key: IdempotencyKey,
    ) -> Result<crate::JoinCommitWitness, WorkspaceError> {
        let cancellation = CancellationToken::new();
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED, &cancellation,
        );
        self.commit_witness_with_accounting(application, idempotency_key, &mut accounting)
            .map(|(witness, allocation)| { drop(allocation); witness })
    }

    /// Copies the same durable witness fields on the original native invocation.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn commit_witness_with_native_work(
        &self,
        application: &JoinApplication<A, O>,
        idempotency_key: IdempotencyKey,
        original: &crate::NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<(crate::JoinCommitWitness, crate::NativeAllocation), (WorkspaceError, Option<crate::NativeAllocation>)> {
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        self.commit_witness_with_accounting(application, idempotency_key, &mut accounting)
            .map(|(witness, allocation)| match allocation {
                crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => (witness, allocation),
                crate::kernel::allocation::KernelOwnedAllocation::Standalone =>
                    unreachable!("native witness retains original invocation custody"),
            }).map_err(|error| (error, None))
    }

    fn commit_witness_with_accounting(
        &self,
        application: &JoinApplication<A, O>,
        idempotency_key: IdempotencyKey,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    ) -> Result<(crate::JoinCommitWitness, crate::kernel::allocation::KernelOwnedAllocation), WorkspaceError> {
        let examined = crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED };
        accounting.admit(examined)?;
        let incompatible = application.generation.workspace.id() != self.target.id()
            || application.generation.id() == self.target_head.id();
        accounting.record_completed(examined)?;
        if incompatible { return Err(WorkspaceError::IncompatibleWorkspace); }
        let allocation = accounting.claim_owned(0, 0)?;
        let copied = crate::WorkCounters {
            bytes_copied: std::mem::size_of::<crate::JoinCommitWitness>() as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copied)?;
        let witness = crate::JoinCommitWitness {
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
        };
        accounting.record_completed(copied)?;
        Ok((witness, allocation))
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
    ) -> Result<crate::FsReceipt<GenerationDiff>, WorkspaceError> {
        self.source
            .volume
            .fs
            .workspace_join_changes(self.base.id, &self.source_head, self.maximum_changes, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| crate::FsReceipt { value: receipt.value.0, work: receipt.work }).map_err(|failure| failure.error)
    }

    /// Applies this immutable plan through one target-head CAS.
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

    /// Applies this plan on the invocation's original native work.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn apply_with_native_work(
        &self, options: ApplyOptions, original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), (WorkspaceError, Option<crate::NativeAllocation>)> {
        self.apply_with_permit_with_native_work(
            options, crate::PublicationPermit::Unrestricted, original, cancellation,
        ).await
    }

    /// Applies the same join under its actual writer permit and original work.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn apply_with_permit_with_native_work(
        &self, options: ApplyOptions, permit: crate::PublicationPermit,
        original: &crate::NativeMaterializationWork, cancellation: &crate::CancellationToken,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), (WorkspaceError, Option<crate::NativeAllocation>)> {
        let mut before_publication = |_: &crate::NativeMaterializationWork| std::future::ready(Ok::<(), WorkspaceError>(()));
        self.apply_with_permit_guarded_with_native_work(
            options, permit, original, cancellation, &mut before_publication,
        ).await.map_err(|error| (error, None))
    }

    /// Seals publication through the canonical native publisher before its CAS.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn apply_with_permit_guarded_with_native_work<'original, F, Fut, E>(
        &self, options: ApplyOptions, permit: crate::PublicationPermit,
        original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken, before_publication: &mut F,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
    {
        self.apply_resolutions_guarded_with_native_work(
            options, &[], permit, original, cancellation, before_publication,
        ).await
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
        &self, options: ApplyOptions, registry: &MergeDriverRegistry,
        cache: &mut C, replanning: bool,
    ) -> Result<JoinOutcome<A, O>, DrivenJoinError> {
        let initial = self.apply(options).await?;
        let JoinOutcome::Conflicted { conflicts, truncated } = initial else { return Ok(initial); };
        let plan = self.typed_merge_plan(conflicts, truncated).await?;
        let candidate = resolve_merge_plan(plan, registry, cache, replanning)?;
        self.apply_candidate(options, &candidate).await
    }

    /// Applies caller-selected immutable sides after validating that they
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
        let cancellation = crate::CancellationToken::new();
        validate_join_side_selections(
            &conflicts, truncated, &selections,
            &mut crate::kernel::allocation::KernelAccounting::standalone(
                WorkBudget::UNBOUNDED, &cancellation,
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
        &self, options: ApplyOptions, selections: &BTreeMap<MergeConflict, ConflictSide>,
        permit: crate::PublicationPermit, original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken, before_publication: &mut F,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + From<DrivenJoinError> + Send + 'static,
    {
        let (initial, allocation) = self.apply_with_permit_guarded_with_native_work(
            options, permit, original, cancellation, before_publication,
        ).await?;
        let JoinOutcome::Conflicted { conflicts, truncated } = initial else {
            return Ok((initial, allocation));
        };
        let bytes = allocation.bytes();
        let conflicts = crate::kernel::Owned::new(
            conflicts, crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation), bytes,
        );
        let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
        let validation = validate_join_side_selections(&conflicts.value, truncated, selections, &mut accounting);
        conflicts.retire(&mut accounting).map_err(E::from)?;
        validation.map_err(E::from)?;
        let resolutions = native_join_side_resolutions(selections, &mut accounting, cancellation)
            .map_err(E::from)?;
        let applied = self.apply_resolutions_guarded_with_native_work(
            options, &resolutions.value, permit, original, cancellation, before_publication,
        ).await;
        resolutions.retire(&mut accounting).map_err(E::from)?;
        applied
    }

    /// Validates caller-selected declarations against this exact immutable
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
            return Ok(JoinOutcome::StaleTarget(self.target.head(crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?));
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
        self.map_join_outcome(outcome)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn apply_resolutions_guarded_with_native_work<'original, F, Fut, E>(
        &self, options: ApplyOptions,
        resolutions: &[(MergeConflict, crate::kernel::MergeConflictResolution)],
        permit: crate::PublicationPermit, original: &'original crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken, before_publication: &mut F,
    ) -> Result<(JoinOutcome<A, O>, crate::NativeAllocation), E>
    where
        F: for<'materialization> FnMut(&'materialization crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
    {
        async {
            if options.if_target != self.target_head.id {
                let mut accounting = crate::kernel::allocation::KernelAccounting::native(original, cancellation);
                let (id, _) = self.target.volume.fs.workspace_head_state(
                    &self.target.volume, &mut accounting, cancellation,
                ).await?;
                return Ok((JoinOutcome::StaleTarget(Generation {
                    workspace: self.target.clone(), id, publication_closure: None,
                }), original.claim_allocation(0, 0, crate::WorkCounters::UNCHARGED).map_err(FsError::from)?));
            }
            let (outcome, allocation) = self.target.volume.fs.apply_workspace_join_guarded_with_native_work(
                crate::facade::WorkspaceJoinRequest {
                    target: &self.target.volume, base: self.base, source: &self.source_head,
                    expected_target: options.if_target, expected_head: self.target_authority_head,
                    history: self.history, operation_id: options.idempotency_key.operation_id(),
                    permit, maximum_generations: self.maximum_generations,
                    maximum_changes: self.maximum_changes, maximum_conflicts: self.maximum_conflicts,
                    resolutions,
                },
                original, cancellation, before_publication,
            ).await?;
            self.map_join_outcome(outcome).map(|outcome| (outcome, allocation)).map_err(E::from)
        }.await
    }

    fn map_join_outcome(
        &self, outcome: crate::facade::WorkspaceJoinOutcome,
    ) -> Result<JoinOutcome<A, O>, WorkspaceError> {
        let generation = |id| Generation { workspace: self.target.clone(), id, publication_closure: None };
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

fn validate_join_side_selections(
    conflicts: &[MergeConflict], truncated: bool,
    selections: &BTreeMap<MergeConflict, ConflictSide>,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<(), DrivenJoinError> {
    if truncated || conflicts.len() != selections.len() {
        return Err(DrivenJoinError::StaleCandidate);
    }
    for conflict in conflicts {
        let mut found = false;
        for key in selections.keys() {
            accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })
                .map_err(WorkspaceError::from)?;
            match key.cmp(conflict) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => { found = true; break; }
                std::cmp::Ordering::Greater => break,
            }
        }
        if !found { return Err(DrivenJoinError::StaleCandidate); }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn native_join_side_resolutions(
    selections: &BTreeMap<MergeConflict, ConflictSide>,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<crate::kernel::Owned<Vec<(MergeConflict, crate::kernel::MergeConflictResolution)>>, FsError> {
    let mut resolutions = crate::kernel::Owned::allocate(selections.len(), accounting)?;
    for (conflict, side) in selections {
        let conflict = match conflict {
            MergeConflict::File(id) => MergeConflict::File(*id),
            MergeConflict::Binding { directory_id, name } => {
                let (name, allocation) = name.clone_with_accounting(accounting, cancellation)?;
                let bytes = name.retained_bytes() as u64;
                let mut name = crate::kernel::Owned::new(name, allocation, bytes);
                let retained = resolutions.bytes.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
                resolutions.allocation.absorb(&mut name.allocation)?;
                resolutions.bytes = retained;
                MergeConflict::Binding { directory_id: *directory_id, name: name.value }
            }
        };
        let side = match side {
            ConflictSide::Base => crate::kernel::MergeConflictSide::Base,
            ConflictSide::Ours => crate::kernel::MergeConflictSide::Ours,
            ConflictSide::Theirs => crate::kernel::MergeConflictSide::Theirs,
        };
        let copying = crate::WorkCounters {
            bytes_copied: std::mem::size_of::<(MergeConflict, crate::kernel::MergeConflictResolution)>() as u64,
            items_examined: 1,
            ..crate::WorkCounters::UNCHARGED
        };
        accounting.admit(copying)?;
        resolutions.value.push((conflict, crate::kernel::MergeConflictResolution::Select(side)));
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

fn logical_name_text(name: &LogicalName) -> Result<std::borrow::Cow<'_, str>, WorkspaceError> {
    name.unicode_text().ok_or(WorkspaceError::NamespacePath(
        crate::kernel::NamespacePathError::NonUnicodeProjection,
    ))
}

fn namespace_path_text(path: &NamespacePath) -> Result<String, WorkspaceError> {
    let cancellation = crate::CancellationToken::new();
    path.unicode_text_with_accounting(
        &mut KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation), &cancellation,
    ).map(|(text, _allocation)| text).map_err(WorkspaceError::from)
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
    /// Uses the original finite selection builder and preserves every failed prefix.
    pub async fn matches_fork_selection(
        &self, source: &Self, paths: Option<Vec<String>>,
        budget: crate::WorkBudget, cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<bool>, WorkspaceError> {
        self.workspace.volume.fs.verify_fork_selection_measured(
            source, self, paths, budget, cancellation,
        ).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn matches_fork_selection_with_native_work(
        &self, source: &Self, paths: Option<Vec<String>>,
        original: &crate::NativeMaterializationWork, cancellation: &crate::CancellationToken,
    ) -> Result<bool, (WorkspaceError, Option<crate::NativeAllocation>)> {
        self.workspace.volume.fs.verify_fork_selection_with_native_work(
            source, self, paths, original, cancellation,
        ).await.map_err(|error| (error, None))
    }

    /// Computes the exact normalized second-parent identity that a merge of
    pub async fn normalized_join_parent_for(
        &self,
        target: &Generation<A, O>,
    ) -> Result<GenerationId, WorkspaceError> {
        let cancellation = crate::CancellationToken::new();
        let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
        self.workspace
            .volume
            .fs
            .workspace_normalized_join_parent(self, target, &mut accounting, &cancellation)
            .await
    }

    /// Opens one cheap immutable reader pinned to this exact generation.
    pub async fn reader(&self) -> Result<crate::PinnedReader<A, O>, WorkspaceError> {
        self.workspace
            .checkout(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(), crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)?
            .pinned_reader()
            .map_err(WorkspaceError::engine)
    }

    /// Computes a semantic delta under explicit work and cancellation limits.
    pub async fn diff_to(
        &self,
        to: &Generation<A, O>,
        maximum_changes: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<ChangeSet<A, O>, WorkspaceError> {
        let receipt = self
            .workspace
            .volume
            .fs
            .workspace_join_changes(self.id, to, maximum_changes, budget, cancellation)
            .await.map(|receipt| crate::FsReceipt { value: receipt.value.0, work: receipt.work })?;
        Ok(ChangeSet {
            from: self.clone(),
            to: to.clone(),
            changes: receipt.value,
            work: receipt.work,
        })
    }

    /// Computes the same semantic delta under the caller's original native work.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn diff_to_with_native_work(
        &self,
        to: &Generation<A, O>,
        maximum_changes: u32,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(ChangeSet<A, O>, crate::NativeAllocation), (WorkspaceError, Option<crate::NativeAllocation>)> {
        let (changes, allocation) = self.workspace.volume.fs
            .workspace_join_changes(self.id, to, maximum_changes, original, cancellation)
            .await.and_then(|(diff, allocation)| match allocation {
                crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => Ok((diff, allocation)),
                crate::kernel::allocation::KernelOwnedAllocation::Standalone => Err(WorkspaceError::Filesystem(
                    FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout),
                )),
            }).map_err(|error| (error, None))?;
        Ok((ChangeSet {
            from: self.clone(),
            to: to.clone(),
            changes,
            work: original.receipt(),
        }, allocation))
    }

    /// Materializes this immutable generation into an empty host directory.
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub async fn materialize(
        &self, options: &crate::MaterializeOptions, budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError> {
        self.materialize_selected_paths(std::iter::once("/"), options, budget, cancellation).await
    }

    /// Materializes one path into an empty host directory.
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub async fn materialize_path(
        &self, path: &str, options: &crate::MaterializeOptions, budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError> {
        self.materialize_selected_paths(std::iter::once(path), options, budget, cancellation).await
    }

    /// Materializes selected paths with shared parent and hard-link state.
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub async fn materialize_paths(
        &self, paths: &[String], options: &crate::MaterializeOptions, budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError> {
        self.materialize_selected_paths(paths.iter().map(String::as_str), options, budget, cancellation).await
    }

    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    async fn materialize_selected_paths<'a>(
        &self, paths: impl ExactSizeIterator<Item = &'a str> + ProviderTask,
        options: &crate::MaterializeOptions, budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError> {
        let opened = self.workspace.checkout(
            GenerationSelector::Exact(self.id), CheckoutMode::read_only_pinned(), budget, cancellation,
        ).await?;
        let mut checkout = opened.value;
        let mut work = opened.work;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = async {
            allocations.claim_bytes(checkout.logical_owned_bytes(), 0, &mut work, budget)
                .map_err(workspace_allocation_error)?;
            let mut accounting = KernelAccounting::borrowed_standalone(
                &mut allocations, &mut work, budget, cancellation,
            );
            let mut parsed = crate::kernel::Owned::allocate(paths.len(), &mut accounting)?;
            for path in paths {
                let (path, allocation) = customer_path_with_accounting(
                    path, checkout.volume_config(), &mut accounting, cancellation,
                )?;
                let bytes = path.retained_bytes();
                Transaction::<A, O>::push_workspace_mutation(
                    &mut parsed, crate::kernel::Owned::new(path, allocation, bytes), &mut accounting,
                )?;
            }
            drop(accounting);
            let live = allocations.live_bytes();
            let remaining = workspace_remaining(work, budget, live)?;
            let materialized = merge_workspace_backend(crate::materialize_checkout_paths(
                &mut checkout, &parsed.value, options, remaining, cancellation,
            ).await, &mut work, live)?;
            work.verify(budget)?;
            Ok(materialized.value)
        }.await;
        result.map(|value| crate::OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))
    }

    /// Restores one path from this immutable generation into a host tree.
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub async fn restore_host_path(
        &self,
        relative: &std::path::Path,
        replacement: crate::HostPathReplacement,
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::HostPathRestore>, WorkspaceError> {
        let checkout = self
            .workspace
            .checkout(
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
                .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), checkout.work))?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(checkout.work, WorkspaceError::engine))?;
        merge_workspace_work(checkout.work, receipt, budget)
    }



    /// Returns the exact immutable parent generation identities.
    pub async fn parents(&self) -> Result<Vec<GenerationId>, WorkspaceError> {
        self.workspace
            .volume
            .fs
            .generation_parents(&self.workspace.volume, self.id, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value.0).map_err(|failure| failure.error)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn parents_with_native_work(
        &self, original: &crate::NativeMaterializationWork, cancellation: &crate::CancellationToken,
    ) -> Result<(Vec<GenerationId>, crate::NativeAllocation), (WorkspaceError, Option<crate::NativeAllocation>)> {
        self.workspace.volume.fs.generation_parents(
            &self.workspace.volume, self.id, original, cancellation,
        ).await.map(|(parents, allocation)| match allocation {
            crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal(allocation) => (parents, allocation),
            crate::kernel::allocation::KernelOwnedAllocation::Standalone => unreachable!("native root retains parent allocation"),
        }).map_err(|error| (error, None))
    }

    /// Reads this exact immutable generation under complete operation bounds.
    pub async fn read<W: crate::facade::FsWork>(
        &self,
        path: &str,
        maximum_bytes: u64,
        work: W,
        cancellation: &crate::CancellationToken,
    ) -> W::Output<acyclic_stream::OwnedBytes, WorkspaceError> {
        self.workspace.read_selected(
            GenerationSelector::Exact(self.id), path, maximum_bytes, work, cancellation,
        ).await
    }



    /// Opens an immutable root carrier on the original native scope, without
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn pinned_reader_with_native_work(
        &self,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<crate::PinnedReader<A, O>, (WorkspaceError, Option<crate::NativeAllocation>)> {
        self.workspace.readonly_pinned_reader_with_native_work(
            self.id, original, cancellation,
        ).await.map_err(|error| (error, None))
    }

    /// Performs the bounded read range operation.
    pub async fn read_range<W: crate::facade::FsWork>(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        work: W,
        cancellation: &crate::CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_generation_range(
            &self.workspace, GenerationSelector::Exact(self.id), path, offset, length,
            work, cancellation,
        ).await
    }

    /// Resolves the canonical no-follow identity in this exact generation.
    pub async fn stat(
        &self,
        path: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<WorkspaceStat>, WorkspaceError> {
        stat_generation(&self.workspace, GenerationSelector::Exact(self.id), path, budget, cancellation).await
    }

    /// Resolves the same exact no-follow identity on the original native scope.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn stat_with_native_work(
        &self,
        path: &str,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<WorkspaceStat, (WorkspaceError, Option<crate::NativeAllocation>)> {
        let result = async {
            let mut accounting = KernelAccounting::native(original, cancellation);
            let (mut checkout, path) = self.workspace.open_read_path(
                GenerationSelector::Exact(self.id), path, &mut accounting, cancellation,
            ).await?;
            let lookup = checkout.value.lookup_no_follow_with_metadata_with_accounting(
                &path.value, &mut accounting, cancellation,
            ).await?.ok_or(WorkspaceError::NotFound)?;
            workspace_stat_from_lookup(lookup)
        }.await;
        result.map_err(|error| (error, None))
    }

    /// Resolves an ordered exact path batch while sharing authenticated directory and file-table
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
            let mut work = checkout.work;
            let mut checkout = checkout.value;
            let mut allocations = crate::kernel::allocation::AllocationLedger::default();
            let result = async {
                allocations.claim_bytes(checkout.logical_owned_bytes(), 0, &mut work, budget)
                    .map_err(crate::collection::handle_allocation_error)?;
                let mut accounting = KernelAccounting::borrowed_standalone(
                    &mut allocations, &mut work, budget, cancellation,
                );
                let (lookup, allocation) = checkout.lookup_batch_no_follow_with_accounting(
                    paths, false, &mut accounting, cancellation,
                ).await?;
                let bytes = lookup.retained_allocation_bytes;
                let lookup = crate::kernel::Owned::new(lookup, allocation, bytes);
                let requested = lookup.value.entries.len().checked_mul(std::mem::size_of::<Option<crate::kernel::FileRecord>>())
                    .ok_or(crate::WorkError::Overflow)? as u64;
                accounting.claim_owned(requested, u64::from(requested != 0))?;
                let mut records = Vec::new();
                records.try_reserve_exact(lookup.value.entries.len())
                    .map_err(|_| crate::FsError::HandleAllocationFailed)?;
                for entry in lookup.value.entries {
                    accounting.charge(crate::WorkCounters { items_examined: 1, ..crate::WorkCounters::UNCHARGED })?;
                    records.push(entry.record);
                }
                Ok(records)
            }.await;
            result.map(|value| crate::FsReceipt { value, work })
                .map_err(|error| crate::OperationFailure::new(error, work))
        })
        .await
    }

    /// Lists this exact generation with authenticated bounded work.
    pub fn list_directory<'a>(
        &'a self,
        path: &'a str,
        after: Option<&'a LogicalName>,
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &'a crate::CancellationToken,
    ) -> impl std::future::Future<Output = crate::MeasuredResult<
        crate::OperationReceipt<WorkspaceDirectoryPage>, WorkspaceError,
    >> + ProviderTask + 'a {
        list_generation_directory(
            &self.workspace, GenerationSelector::Exact(self.id), path, after,
            maximum_entries, budget, cancellation,
        )
    }

    /// Returns the same exact-generation tree page with original native custody.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn list_after_with_native_work(
        &self,
        path: &str,
        after: Option<&LogicalName>,
        maximum: usize,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(WorkspaceDirectoryPage, crate::NativeAllocation), (WorkspaceError, Option<crate::NativeAllocation>)> {
        let result = async {
            let maximum = u32::try_from(maximum).map_err(|_| crate::WorkError::Overflow)?;
            let mut accounting = KernelAccounting::native(original, cancellation);
            let (mut checkout, path) = self.workspace.open_read_path(
                GenerationSelector::Exact(self.id), path, &mut accounting, cancellation,
            ).await?;
            let (page, allocation) = checkout.value.list_directory_with_bound_with_accounting(
                &path.value, after, false, maximum, &mut accounting, cancellation,
            ).await?;
            let (page, allocation) = project_workspace_directory_with_accounting(page, allocation, &mut accounting)?;
            let KernelOwnedAllocation::NativeOriginal(allocation) = allocation else { unreachable!() };
            Ok((page, allocation))
        }.await;
        result.map_err(|error| (error, None))
    }

    /// Finds portable paths bound to one stable file identity in this generation.
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<Vec<String>>, WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let search = self.namespace_records_with_accounting(
                &[file_id], maximum_entries, &mut accounting, cancellation,
            ).await?;
            let mut paths = crate::kernel::Owned::allocate(0, &mut accounting)?;
            if let Some(records) = namespace_record_entries(&search.0.records, &file_id, &mut accounting)? {
                for (path, _) in records {
                    paths.ensure_for_push(records.len(), &mut accounting)?;
                    let (text, mut allocation) = path.unicode_text_with_accounting(&mut accounting, cancellation)?;
                    paths.allocation.absorb(&mut allocation)?;
                    paths.bytes = paths.bytes.checked_add(text.capacity() as u64).ok_or(crate::WorkError::Overflow)?;
                    paths.value.push(text);
                }
            }
            Ok(paths.value)
        }.await;
        crate::facade::measured(result, &accounting)
    }

    /// Finds portable paths for multiple stable file identities in one namespace traversal.
    pub async fn paths_for_file_ids(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
    ) -> Result<BTreeMap<FileId, Vec<String>>, WorkspaceError> {
        self.namespace_records_for_file_ids(file_ids, maximum_entries).await?
            .records.into_iter().map(|(file_id, records)| {
                records.into_iter().map(|(path, _)| namespace_path_text(&path))
                    .collect::<Result<Vec<_>, _>>().map(|paths| (file_id, paths))
            }).collect()
    }

    async fn namespace_records_for_file_ids(
        &self,
        file_ids: impl IntoIterator<Item = FileId>,
        maximum_entries: u32,
    ) -> Result<NamespaceRecordSearch, WorkspaceError> {
        let cancellation = crate::CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(crate::WorkBudget::UNBOUNDED, &cancellation);
        let ids = sorted_changed_file_ids(file_ids, &mut accounting)?;
        self.namespace_records_with_accounting(
            &ids.0, maximum_entries, &mut accounting, &cancellation,
        ).await.map(|owned| owned.0)
    }

    async fn namespace_records_with_accounting(
        &self, ids: &[FileId], maximum_entries: u32,
        accounting: &mut KernelAccounting<'_>, cancellation: &crate::CancellationToken,
    ) -> Result<(NamespaceRecordSearch, KernelOwnedAllocation), WorkspaceError> {
        accounting.check()?;
        if ids.is_empty() || maximum_entries == 0 {
            return Ok((NamespaceRecordSearch { records: NamespaceRecords::default(), complete: ids.is_empty() },
                accounting.claim_owned(0, 0)?));
        }
        let checkout = self.workspace.open_checkout(
            GenerationSelector::Exact(self.id), CheckoutMode::read_only_pinned(), accounting, cancellation,
        ).await?;
        let generation_root = crate::ObjectId { kind: crate::ObjectKind::GenerationRoot, digest: self.id.digest() };
        let result = namespace_records_to_file_root_with_accounting(
            &self.workspace.volume, checkout.value.root(), generation_root,
            ids, maximum_entries, accounting, cancellation,
        ).await;
        checkout.retire(accounting)?;
        result.map_err(WorkspaceError::from)
    }


    /// Performs the bounded read symbolic link operation.
    pub async fn read_symbolic_link<W: crate::facade::FsWork>(
        &self,
        path: &str,
        work: W,
        cancellation: &crate::CancellationToken,
    ) -> W::Output<crate::kernel::OwnedSourceChunk, WorkspaceError> {
        read_generation_symbolic_link(
            &self.workspace, GenerationSelector::Exact(self.id), path, work, cancellation,
        ).await
    }

    /// Plans sparse logical spans without reading content bodies.
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
    pub async fn pin(
        &self,
        identity: impl AsRef<str>,
    ) -> Result<(WorkspaceName, Generation<A, O>), WorkspaceError> {
        self.pin_measured(identity, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Retains this original generation under the same permanent opaque label.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn pin_with_native_work(
        &self,
        identity: impl AsRef<str>,
        original: &crate::NativeMaterializationWork,
        cancellation: &crate::CancellationToken,
    ) -> Result<(WorkspaceName, Generation<A, O>), (WorkspaceError, Option<crate::NativeAllocation>)> {
        let result = async {
            let identity = WorkspaceName::new_with_native_work(
                identity.as_ref(), original, cancellation,
            )?;
            let mut accounting = crate::kernel::allocation::KernelAccounting::native(
                original, cancellation,
            );
            let label = copy_workspace_label(&identity, &mut accounting)?;
            let publication = self.publication_closure.as_ref().and_then(|slot| {
                slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take()
            });
            self.workspace.volume.fs.retain_workspace_generation(
                &self.workspace.volume, self.id, crate::kernel::RetentionKind::Pin,
                label, publication, original, cancellation,
            ).await?;
            Ok((identity, self.clone()))
        }.await;
        result.map_err(|error| (error, None))
    }

    /// Actual original publication payload still retained by this generation's shared slot.
    pub(crate) fn publication_retained_bytes(&self) -> u64 {
        self.publication_closure.as_ref().map_or(0, |slot| {
            let publication = slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::size_of::<std::sync::Mutex<Option<crate::facade::VerifiedPublication<A, O>>>>() as u64
                + publication.as_ref().map_or(0, crate::facade::VerifiedPublication::retained_bytes)
        })
    }

    /// Retains the original generation through the canonical retention engine.
    pub async fn pin_measured(
        &self,
        identity: impl AsRef<str>,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<(WorkspaceName, Generation<A, O>)>, WorkspaceError> {
        let named = WorkspaceName::new_measured(identity, budget, cancellation)?;
        let identity = named.value;
        let mut work = named.work;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let prepared = (|| {
            cancellation.check()?;
            allocations.claim_bytes(identity.retained_bytes(), 0, &mut work, budget)
                .map_err(workspace_allocation_error)?;
            let mut accounting = KernelAccounting::borrowed_standalone(&mut allocations, &mut work, budget, cancellation);
            let mut label = copy_workspace_label(&identity, &mut accounting)?;
            // Transfer the label backing to the retainer's ledger.
            accounting.release_owned(&mut label.allocation, label.bytes)?;
            Ok(label)
        })();
        let label = prepared.map_err(|error| crate::OperationFailure::new(error, work))?;
        let live = allocations.live_bytes();
        let allowance = workspace_remaining(work, budget, live)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        // Clones share one slot; consume before awaiting, releasing the
        let publication = self.publication_closure.as_ref().and_then(|slot| {
            slot.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        });
        merge_workspace_backend(self.workspace.volume.fs.retain_workspace_generation(
            &self.workspace.volume, self.id, crate::kernel::RetentionKind::Pin,
            label, publication, allowance, cancellation,
        ).await, &mut work, live).map_err(|error| crate::OperationFailure::new(error, work))?;
        work.verify(budget)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::Work(error), work))?;
        Ok(crate::OperationReceipt {
            value: (identity, self.clone()),
            work,
        })
    }
}

fn copy_workspace_label(
    identity: &WorkspaceName, accounting: &mut KernelAccounting<'_>,
) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
    let requested = identity.retained_bytes();
    let copied = crate::WorkCounters { bytes_copied: requested, ..crate::WorkCounters::UNCHARGED };
    accounting.admit(copied)?;
    let allocation = accounting.claim_owned(requested, 1)?;
    let mut label = crate::kernel::Owned::new(String::new(), allocation, requested);
    if let Err(error) = label.value.try_reserve_exact(identity.as_str().len()) {
        #[cfg(not(target_arch = "wasm32"))]
        if accounting.native_work().is_some() {
            return Err(FsError::NativeKernel(crate::NativeTreeMaterializationError::AllocationFailed(error)).into());
        }
        label.retire(accounting)?;
        return Err(WorkspaceError::engine(error));
    }
    let actual = label.value.capacity() as u64;
    let reconciled = accounting.reconcile_owned(&mut label.allocation, requested, actual);
    label.bytes = actual;
    reconciled?;
    accounting.admit(copied)?;
    label.value.push_str(identity.as_str());
    accounting.record_completed(copied)?;
    Ok(label)
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

    /// Keeps the pinned parent lineage but starts with an empty namespace.
    #[must_use]
    pub fn empty(self) -> Self {
        self.inherit_paths(Vec::new())
    }

    /// Carries only explicitly selected files or directory subtrees. Selection
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
    /// Canonical name buffers could not be allocated or represented.
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
    /// A canonical filesystem failure retains its original typed cause.
    #[error(transparent)]
    Filesystem(FsError),
    /// Caller cancellation stopped a measured workspace operation.
    #[error(transparent)]
    Cancelled(#[from] crate::CancellationError),
    /// A measured workspace operation exhausted its admitted work budget.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Both actual work operands remain available when their sum is unrepresentable.
    #[error("workspace work merge failed: {error}")]
    WorkMerge {
        /// Work already completed by the caller.
        prior: crate::WorkCounters,
        /// Actual work returned by the completed backend operation.
        backend: crate::WorkCounters,
        /// The original checked arithmetic failure.
        #[source]
        error: crate::WorkError,
    },
    /// A fork generation belongs to another filesystem deployment.
    #[error("fork generation belongs to another filesystem deployment")]
    ForeignGeneration,
    /// A caller-pinned target generation is no longer current.
    #[error("workspace generation is stale")]
    StaleGeneration,
    /// A frozen source identity or epoch changed before canonical publication.
    #[error("workspace source snapshot is stale")]
    StaleSource,
    /// A conditional mutation targeted a different filesystem identity.
    #[error("workspace file identity is stale")]
    StaleIdentity,
    /// A manually diagnosed path-related semantic failure.
    #[error("invalid workspace path: {0}")]
    Path(String),
    /// A canonical path or Unicode projection failed without formatting a buffer.
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
    pub(crate) fn engine(error: impl std::error::Error + 'static) -> Self {
        if let Some(bounded) = workspace_bounded_error(&error) {
            return bounded;
        }
        // OperationFailure's display field is not an Error::source edge.
        let erased = &error as &dyn std::any::Any;
        if let Some(failure) = erased.downcast_ref::<crate::OperationFailure<FsError>>() {
            if let Some(bounded) = workspace_bounded_error(&failure.error) {
                return bounded;
            }
        }
        Self::Engine(error.to_string())
    }

    /// Intentional human-authored diagnostics are not canonical error aliases.
    pub(crate) fn manual_engine(message: &'static str) -> Self {
        Self::Engine(message.to_owned())
    }

    /// Only intentional human-authored semantic diagnostics use this buffer.
    pub(crate) fn path(message: &'static str) -> Self {
        Self::Path(message.to_owned())
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

/// Transparent error wrappers can hide leaf quota and unit cancellation
pub(crate) fn workspace_bounded_error(
    error: &(dyn std::error::Error + 'static),
) -> Option<WorkspaceError> {
    if let Some(WorkspaceError::WorkMerge { prior, backend, error }) =
        error.downcast_ref::<WorkspaceError>()
    {
        return Some(WorkspaceError::WorkMerge {
            prior: *prior, backend: *backend, error: *error,
        });
    }
    if let Some(error) = error.downcast_ref::<crate::WorkError>() {
        return Some(WorkspaceError::Work(*error));
    }
    if error.is::<crate::CancellationError>() {
        return Some(WorkspaceError::Cancelled(crate::CancellationError));
    }
    if let Some(context) = error.downcast_ref::<Box<crate::FsError>>() {
        return workspace_bounded_error(context.as_ref());
    }
    if let Some(crate::FsError::RootReadWorkMerge { prior, backend, error }) = error.downcast_ref::<crate::FsError>() {
        return Some(WorkspaceError::Filesystem(crate::FsError::RootReadWorkMerge {
            prior: *prior, backend: *backend, error: *error,
        }));
    }
    macro_rules! bounded {
        ($kind:ty; [$($nested:ident),*]; [$($cancelled:ident),*]) => {{
            type ErrorKind = $kind;
            if let Some(error) = error.downcast_ref::<ErrorKind>() {
                return match error {
                    ErrorKind::Work(error) => Some(WorkspaceError::Work(*error)),
                    $(ErrorKind::$nested(error) => workspace_bounded_error(error),)*
                    $(ErrorKind::$cancelled => Some(WorkspaceError::Cancelled(crate::CancellationError)),)*
                    _ => None,
                };
            }
        }};
    }
    bounded!(WorkspaceError; [Cancelled, Filesystem, NamespacePath]; []);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(crate::FsError::NativeKernel(native)) = error.downcast_ref::<crate::FsError>() {
        return workspace_bounded_error(native);
    }
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(crate::ObjectStoreError::Native(native)) = error.downcast_ref::<crate::ObjectStoreError>() {
        return workspace_bounded_error(native);
    }
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(crate::AuthorityStoreError::NativeOriginal(native)) = error.downcast_ref::<crate::AuthorityStoreError>() {
        return workspace_bounded_error(native);
    }
    #[cfg(not(target_arch = "wasm32"))]
    bounded!(crate::NativeTreeMaterializationError; [Io]; [Cancelled]);
    bounded!(crate::FsError;
        [Cancelled, Authority, Object, Closure, Path, Directory, FileRecord, FileRead,
         BlobBuild, BlobRead, AttributeLookup, AttributeList, AttributeMutation,
         Probe, Rebase, Mutation, DetachedMutation, Checkpoint, Publication,
         MergeFileTable, MergeTree]; []);
    bounded!(crate::AuthorityStoreError; []; [Cancelled]);
    bounded!(crate::ObjectStoreError; []; [Cancelled]);
    bounded!(crate::kernel::GenerationMutationError;
        [Plan, Path, FileRecord, Tree, FileTable, Regular, Directory]; []);
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
    bounded!(crate::kernel::AttributeListError; [Storage]; [Cancelled]);
    bounded!(crate::kernel::AttributeMutationError; [Storage]; []);
    bounded!(crate::kernel::AuthenticatedProbeError;
        [Storage, FileRecord, Tree, Directory, Extent, Blob]; [Cancelled]);
    bounded!(crate::kernel::RebaseError<crate::kernel::AuthenticatedProbeError>; [Probe]; [Cancelled]);
    bounded!(crate::kernel::CheckpointError; [Storage]; []);
    bounded!(crate::kernel::ClosureError; [Storage]; []);
    bounded!(crate::kernel::PublicationError; [Closure, Objects, Authority]; []);
    #[cfg(not(target_arch = "wasm32"))]
    bounded!(crate::materializer::NativeTreeMaterializationError; [Io]; [Cancelled]);
    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    bounded!(crate::DemandError; []; [Cancelled]);
    #[cfg(feature = "native-mount")]
    if let Some(error) = error.downcast_ref::<crate::native_mount::MountSourceError>() {
        use crate::native_mount::MountSourceError;
        return match error.semantic_error() {
            MountSourceError::Work(error) => Some(WorkspaceError::Work(*error)),
            MountSourceError::Cancelled => Some(WorkspaceError::Cancelled(crate::CancellationError)),
            MountSourceError::Filesystem(error) => workspace_bounded_error(error),
            MountSourceError::NamespacePath(error) => workspace_bounded_error(error),
            MountSourceError::NativeWork(error) => workspace_bounded_error(error),
            _ => None,
        };
    }
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        return error.get_ref().and_then(|error| workspace_bounded_error(error));
    }
    error.source().and_then(workspace_bounded_error)
}

impl From<FsError> for WorkspaceError {
    fn from(error: FsError) -> Self {
        if matches!(&error, FsError::RootReadWorkMerge { .. }) {
            return Self::Filesystem(error);
        }
        if let Some(bounded) = workspace_bounded_error(&error) {
            return bounded;
        }
        match error {
            FsError::Path(crate::kernel::PathLookupError::NamespacePath(error)) => namespace_path_error(error),
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
        namespace_path_error(error)
    }
}

/// Exact backing transferred by an owned string-vector input. This is ownership
pub(crate) fn workspace_owned_strings_bytes(values: &Vec<String>) -> Result<u64, WorkspaceError> {
    let vector_bytes = values.capacity().checked_mul(std::mem::size_of::<String>())
        .ok_or(crate::WorkError::Overflow)?;
    values.iter().try_fold(u64::try_from(vector_bytes).map_err(|_| crate::WorkError::Overflow)?, |bytes, value| {
        bytes.checked_add(u64::try_from(value.capacity()).map_err(|_| crate::WorkError::Overflow)?)
            .ok_or_else(|| crate::WorkError::Overflow.into())
    })
}

pub(crate) fn workspace_vec_bytes<T>(values: &Vec<T>) -> u64 {
    (values.capacity() * std::mem::size_of::<T>()) as u64
}

fn workspace_allocation_error(error: crate::kernel::allocation::AllocationError) -> WorkspaceError {
    crate::collection::handle_allocation_error(error).into()
}

pub(crate) fn new_workspace_vec<T>(
    capacity: usize,
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut crate::WorkCounters,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<Vec<T>, WorkspaceError> {
    cancellation.check()?;
    let claimed = allocations.claim_elements::<T>(capacity, work, budget)
        .map_err(workspace_allocation_error)?;
    let mut values = Vec::new();
    if let Err(error) = values.try_reserve_exact(capacity) {
        allocations.release(claimed).map_err(workspace_allocation_error)?;
        return Err(WorkspaceError::engine(error));
    }
    let actual = workspace_vec_bytes(&values);
    if actual > claimed {
        if let Err(error) = allocations.claim_bytes(actual - claimed, 0, work, crate::WorkBudget::UNBOUNDED) {
            allocations.release(claimed).map_err(workspace_allocation_error)?;
            return Err(workspace_allocation_error(error));
        }
        if let Err(error) = work.verify_ref(&budget) {
            allocations.release(actual).map_err(workspace_allocation_error)?;
            return Err(WorkspaceError::Work(error));
        }
    }
    Ok(values)
}

pub(crate) fn grow_workspace_vec<T>(
    values: &mut Vec<T>,
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut crate::WorkCounters,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<(), WorkspaceError> {
    if values.len() < values.capacity() { return Ok(()); }
    cancellation.check()?;
    let capacity = values.capacity().checked_mul(2).map(|value| value.max(1))
        .ok_or(crate::WorkError::Overflow)?;
    let copied = values.len().checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::WorkError::Overflow)? as u64;
    let delta = crate::WorkCounters { bytes_copied: copied, ..crate::WorkCounters::default() };
    work.admit(&delta, &budget)?;
    let mut replacement = new_workspace_vec(capacity, allocations, work, budget, cancellation)?;
    work.charge(&delta, &budget)?;
    let old_bytes = workspace_vec_bytes(values);
    replacement.append(values);
    *values = replacement;
    allocations.release(old_bytes).map_err(workspace_allocation_error)?;
    Ok(())
}

pub(crate) fn workspace_remaining(
    work: crate::WorkCounters,
    budget: crate::WorkBudget,
    live_bytes: u64,
) -> Result<crate::WorkBudget, WorkspaceError> {
    let mut remaining = work.remaining(budget)?;
    remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.checked_sub(live_bytes)
        .ok_or(crate::WorkError::BudgetExceeded {
            counter: "peak_allocation_bytes", observed: live_bytes,
            maximum: remaining.peak_allocation_bytes,
        })?;
    Ok(remaining)
}

fn merge_workspace_backend<T, E: Into<WorkspaceError>>(
    result: crate::MeasuredResult<crate::OperationReceipt<T>, E>,
    work: &mut crate::WorkCounters,
    live_bytes: u64,
) -> Result<crate::OperationReceipt<T>, WorkspaceError> {
    match result {
        Ok(receipt) => {
            *work = work.with_backend(receipt.work, live_bytes)
                .map_err(|error| WorkspaceError::WorkMerge {
                    prior: *work, backend: receipt.work, error,
                })?;
            Ok(receipt)
        }
        Err(failure) => {
            *work = work.with_backend(*failure.work, live_bytes)
                .map_err(|error| WorkspaceError::WorkMerge {
                    prior: *work, backend: *failure.work, error,
                })?;
            Err(failure.error.into())
        }
    }
}

pub(crate) fn customer_path(
    path: &str,
    config: crate::model::VolumeConfig,
) -> Result<NamespacePath, WorkspaceError> {
    customer_path_measured(
        path, config, &mut crate::kernel::allocation::AllocationLedger::default(),
        &mut crate::WorkCounters::default(), crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
}

/// Uses the caller's cumulative full budget and shared live ledger.
pub(crate) fn customer_path_measured(
    path: &str,
    config: crate::model::VolumeConfig,
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut crate::WorkCounters,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<NamespacePath, WorkspaceError> {
    NamespacePath::parse_in_profile_with_ledger(
        path, config.profile, config.limits, allocations, work, budget, cancellation,
    ).map_err(namespace_path_error)
}

/// Borrows the caller's original accounting owner; no SDK path producer is
pub(crate) fn customer_path_with_accounting(
    path: &str,
    config: crate::model::VolumeConfig,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &crate::CancellationToken,
) -> Result<(NamespacePath, crate::kernel::allocation::KernelOwnedAllocation), WorkspaceError> {
    NamespacePath::parse_in_profile_with_accounting(
        path, config.profile, config.limits, accounting, cancellation,
    ).map_err(WorkspaceError::from)
}

pub(crate) fn namespace_path_error(error: crate::kernel::NamespacePathError) -> WorkspaceError {
    match error {
        crate::kernel::NamespacePathError::Work(error) => WorkspaceError::Work(error),
        crate::kernel::NamespacePathError::Cancelled => WorkspaceError::Cancelled(crate::CancellationError),
        error => WorkspaceError::NamespacePath(error),
    }
}

#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
fn merge_workspace_work<T>(
    prior: crate::WorkCounters,
    receipt: crate::OperationReceipt<T>,
    budget: crate::WorkBudget,
) -> crate::MeasuredResult<crate::OperationReceipt<T>, WorkspaceError> {
    let work = prior
        .checked_add(receipt.work)
        .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
    work.verify(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
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
