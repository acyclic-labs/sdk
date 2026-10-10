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
///
/// Names are NFC-normalized, case-sensitive UTF-8 and cannot be reused for a
/// different workspace in one filesystem namespace.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkspaceName(Arc<str>);

fn workspace_name_scratch_witness(
    name: &str,
    work: &mut crate::WorkCounters,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<crate::WorkCounters, WorkspaceError> {
    let mut pending = 0_u64;
    let mut maximum_pending = 0_u64;
    let mut maximum_chunk = 0_u64;
    for character in name.chars() {
        cancellation.check()?;
        work.charge_items(1, &budget)?;
        let mut chunk = 0_u64;
        let mut failure = None;
        unicode_normalization::char::decompose_canonical(character, |scalar| {
            if failure.is_some() { return; }
            let result = (|| -> Result<(), WorkspaceError> {
                cancellation.check()?;
                work.charge_items(1, &budget)?;
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
    // temporarily retains one complete decomposition beside its pending run;
    // NFC retains only uncomposed marks. Power-of-two growth plus old/new
    // overlap bounds their opaque buffers; stable ordering may need one run.
    // This is admission evidence, never fabricated actual-allocation counters.
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

impl WorkspaceName {
    /// Canonicalizes and validates one friendly workspace name.
    ///
    /// # Errors
    ///
    /// Rejects empty, reserved, path-like, control-containing, or oversized
    /// names before deriving any durable identity.
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
        let mut work = crate::WorkCounters::UNCHARGED;
        let result = (|| {
            cancellation.check()?;
            let witness = workspace_name_scratch_witness(name.as_ref(), &mut work, budget, cancellation)?;
            work.admit(&witness, &budget)?;
            let mut producing_budget = budget;
            producing_budget.peak_allocation_bytes = producing_budget.peak_allocation_bytes
                .checked_sub(witness.peak_allocation_bytes).ok_or(crate::WorkError::Overflow)?;
            let mut allocations = crate::kernel::allocation::AllocationLedger::default();
            let mut output = Vec::new();
            {
                let mut normalized = name.as_ref().nfc();
                loop {
                    cancellation.check()?;
                    work.charge_items(1, &producing_budget)?;
                    let Some(character) = normalized.next() else { break };
                    let mut encoded = [0; 4];
                    for byte in character.encode_utf8(&mut encoded).as_bytes() {
                        grow_workspace_vec(&mut output, &mut allocations, &mut work, producing_budget, cancellation)?;
                        work.charge(&crate::WorkCounters {
                            bytes_copied: 1, ..crate::WorkCounters::UNCHARGED
                        }, &producing_budget)?;
                        output.push(*byte);
                    }
                }
            }
            cancellation.check()?;
            work.charge_items(output.len() as u64, &budget)?;
            let canonical = std::str::from_utf8(&output).map_err(WorkspaceError::engine)?;
            if canonical.is_empty() { return Err(WorkspaceNameError::Empty.into()); }
            if canonical == "." || canonical == ".." { return Err(WorkspaceNameError::Reserved.into()); }
            if canonical.len() > MAXIMUM_WORKSPACE_NAME_BYTES { return Err(WorkspaceNameError::TooLong.into()); }
            for character in canonical.chars() {
                cancellation.check()?;
                work.charge_items(1, &budget)?;
                if character.is_control() || matches!(character, '/' | '\\') {
                    return Err(WorkspaceNameError::InvalidCharacter.into());
                }
            }
            let copied = crate::WorkCounters { bytes_copied: canonical.len() as u64, ..crate::WorkCounters::UNCHARGED };
            work.admit(&copied, &budget)?;
            allocations.claim_bytes(canonical.len() as u64, 1, &mut work, budget)
                .map_err(workspace_allocation_error)?;
            work.charge(&copied, &budget)?;
            Ok(Self(Arc::from(canonical)))
        })();
        result
            .map(|value| crate::OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))
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

    pub(crate) fn derive_measured(
        namespace: [u8; 16],
        name: &WorkspaceName,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Self>, WorkspaceError> {
        let mut work = crate::WorkCounters::UNCHARGED;
        let result = (|| {
            cancellation.check()?;
            work.charge(&crate::WorkCounters {
                bytes_hashed: (WORKSPACE_ID_DOMAIN.len() + namespace.len()
                    + std::mem::size_of::<u64>() + name.as_str().len()) as u64,
                bytes_copied: 16,
                ..crate::WorkCounters::UNCHARGED
            }, &budget)?;
            Ok(Self::derive(namespace, name))
        })();
        result
            .map(|value| crate::OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))
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
        self.reserve_publication_measured(
            operation_id, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Reserves the original authority head with one cumulative work allowance.
    /// The actual returned reservation must be retained before caller-side
    /// post-reservation checks; its expected head is never repinned here.
    pub async fn reserve_publication_measured(
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

    /// Releases only this workspace's exact durable publication reservation.
    /// An ambiguous release response can be retried with the same proof.
    pub async fn release_publication(
        &self,
        reservation: crate::PublicationReservation,
    ) -> Result<(), WorkspaceError> {
        self.release_publication_measured(
            reservation, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Releases only the actual reservation using the caller's cleanup token.
    /// Failure does not establish release: retain the same proof for recovery.
    pub async fn release_publication_measured(
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
            .map_err(|failure| failure.map_with_prior_work(
                crate::WorkCounters::UNCHARGED, |error| WorkspaceError::from(FsError::Authority(error)),
            ))
    }

    /// Reads only an existing reservation for the original operation identity.
    /// This never reserves, renews, releases, or adopts a current authority head.
    pub async fn publication_reservation_measured(
        &self,
        operation_id: OperationId,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Option<crate::PublicationReservation>>, WorkspaceError> {
        cancellation.check()
            .map_err(|error| crate::OperationFailure::before_work(WorkspaceError::from(error)))?;
        let receipt = self.authority().publication_reservation(
            self.authority_id(), operation_id, budget, cancellation,
        ).await.map_err(|failure| failure.map_with_prior_work(
            crate::WorkCounters::UNCHARGED, |error| WorkspaceError::from(FsError::Authority(error)),
        ))?;
        if let Some(reservation) = receipt.value
            && (reservation.authority_id != self.authority_id()
                || reservation.operation_id != operation_id)
        {
            return Err(crate::OperationFailure::new(WorkspaceError::IncompatibleWorkspace, receipt.work));
        }
        Ok(receipt)
    }

    /// Resolves the exact generation published by one prior workspace operation.
    ///
    /// This is the recovery boundary for adapters that persisted an
    /// idempotency key before publication but lost the successful result.
    pub async fn operation_generation(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<Generation<A, O>>, WorkspaceError> {
        self.operation_generation_measured(
            idempotency_key,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
    }

    /// Resolves an original committed generation without adopting the mutable
    /// head, under explicit work bounds and the original cancellation token.
    ///
    /// # Errors
    /// Retains actual lookup and authentication work on storage, malformed
    /// record, cancellation and budget failures. Absence grants no authority.
    pub async fn operation_generation_measured(
        &self,
        idempotency_key: IdempotencyKey,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Option<Generation<A, O>>>, WorkspaceError>
    {
        let receipt = self
            .volume
            .fs
            .workspace_operation_generation_measured(
                &self.volume,
                idempotency_key.operation_id(),
                budget,
                cancellation,
            )
            .await?;
        Ok(crate::OperationReceipt {
            value: receipt.value.map(|id| Generation { workspace: self.clone(), id, publication_closure: None }),
            work: receipt.work,
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

    /// Opens the same canonical checkout with exact success and failure work.
    pub async fn checkout_measured(
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
        .map_err(|failure| failure.error)
    }

    /// Pins the authenticated head under explicit work and cancellation limits.
    /// Failed admission retains the work already spent by the checkout engine.
    ///
    /// # Errors
    /// Returns a measured storage, authentication, cancellation or work failure.
    pub async fn head_measured(
        &self,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Generation<A, O>>, WorkspaceError> {
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
                .map_err(|failure| {
                    crate::OperationFailure::new(WorkspaceError::from(failure.error), *failure.work)
                })?;
            Ok(crate::OperationReceipt {
                value: Generation { workspace: self.clone(), id: receipt.value.generation_id(), publication_closure: None },
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<(Generation<A, O>, bool)>, WorkspaceError> {
        let reader = self.head_reader_measured(budget, cancellation).await?;
        let records = reader
            .value
            .file_records_by_id(
                &[file_id],
                reader
                    .work
                    .remaining(budget)
                    .map_err(|error| crate::OperationFailure::new(WorkspaceError::engine(error), reader.work))?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(reader.work, WorkspaceError::from))?;
        let work = reader
            .work
            .checked_add(records.work)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::engine(error), reader.work))?;
        Ok(crate::OperationReceipt {
            value: (
                Generation { workspace: self.clone(), id: reader.value.generation_id(), publication_closure: None },
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::PinnedReader<A, O>>, WorkspaceError> {
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
                .map_err(|error| crate::OperationFailure::new(WorkspaceError::engine(error), checkout.work))?,
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
        .map_err(|failure| failure.error)
    }

    /// Authenticates one exact generation under explicit work and cancellation
    /// limits, retaining failure work without substituting the mutable head.
    ///
    /// # Errors
    /// Returns a measured storage, authentication, cancellation or work failure.
    pub async fn generation_measured(
        &self,
        generation_id: GenerationId,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Generation<A, O>>, WorkspaceError> {
        let checkout = self
            .volume
            .checkout(
                GenerationSelector::Exact(generation_id),
                CheckoutMode::read_only_pinned(),
                budget,
                cancellation,
            )
            .await
            .map_err(|failure| {
                crate::OperationFailure::new(WorkspaceError::from(failure.error), *failure.work)
            })?;
        Ok(crate::OperationReceipt {
            value: Generation { workspace: self.clone(), id: checkout.value.generation_id(), publication_closure: None },
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
        self.restore_paths_from_with_permit_measured(
            source, paths, if_current, idempotency_key, permit,
            crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Restores the same selected records and tombstones with finite work receipts.
    pub async fn restore_paths_from_with_permit_measured(
        &self,
        source: &Generation<A, O>,
        paths: &[String],
        if_current: GenerationId,
        idempotency_key: IdempotencyKey,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError> {
        self.restore_paths_from_with_permit_measured_guarded(
            source, paths, if_current, idempotency_key, permit, budget, cancellation,
            |_| async { Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) },
        ).await
    }

    /// Restores selected records through the original publication engine with
    /// the caller's measured owner check at the actual authority seal.
    /// Paths select exact namespace records. Existing destination directories
    /// retain their child set while restoring metadata. Caller-expanded subtree
    /// unions use [`Self::restore_selected_paths_from_with_permit_measured_guarded`].
    pub async fn restore_paths_from_with_permit_measured_guarded<F, Fut>(
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
        F: FnMut(crate::WorkBudget) -> Fut,
        Fut: Future<Output = crate::FsResult<()>> + Send,
    {
        self.restore_path_records_measured_guarded(
            source, paths, if_current, idempotency_key, permit, budget, cancellation,
            false, before_publication,
        ).await
    }

    /// Restores a caller-expanded selected subtree record union with one seal.
    /// Source-absent records must be ordered children first, and source-present
    /// records parents first. Preparatory removals and empty directories remain
    /// private to the same transaction until the final owner-guarded commit.
    pub async fn restore_selected_paths_from_with_permit_measured_guarded<F, Fut>(
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
        F: FnMut(crate::WorkBudget) -> Fut,
        Fut: Future<Output = crate::FsResult<()>> + Send,
    {
        self.restore_path_records_measured_guarded(
            source, paths, if_current, idempotency_key, permit, budget, cancellation,
            true, before_publication,
        ).await
    }

    async fn restore_path_records_measured_guarded<F, Fut>(
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
        F: FnMut(crate::WorkBudget) -> Fut,
        Fut: Future<Output = crate::FsResult<()>> + Send,
    {
        crate::heap_future::in_heap_measured(move |budget| async move {
            let mut work = crate::WorkCounters::UNCHARGED;
            let mut allocations = crate::kernel::allocation::AllocationLedger::default();
            let result = async {
                if !self.volume.fs.same_deployment(&source.workspace.volume.fs) {
                    return Err(WorkspaceError::ForeignGeneration);
                }
                let current = merge_workspace_backend(
                    self.head_measured(budget, cancellation).await, &mut work, 0,
                )?.value;
                if current.id != if_current {
                    return Ok(TransactionCommit::Conflict { actual: current });
                }
                let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                let mut source_checkout = merge_workspace_backend(
                    source.workspace.engine_checkout_measured(
                        GenerationSelector::Exact(source.id), CheckoutMode::read_only_pinned(),
                        allowance, cancellation,
                    ).await, &mut work, allocations.live_bytes(),
                )?.value;
                let source_checkout_bytes = source_checkout.logical_owned_bytes();
                allocations.claim_bytes(source_checkout_bytes, 0, &mut work, budget)
                    .map_err(workspace_allocation_error)?;
                let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                let mut transaction = merge_workspace_backend(
                    self.begin_pinned_transaction_measured(
                        &current, idempotency_key, allowance, cancellation,
                    ).await, &mut work, allocations.live_bytes(),
                )?.value;
                allocations.claim_bytes(transaction.checkout.logical_owned_bytes(), 0, &mut work, budget)
                    .map_err(workspace_allocation_error)?;
                let mut parsed = new_workspace_vec(
                    paths.len(), &mut allocations, &mut work, budget, cancellation,
                )?;
                for path in paths {
                    cancellation.check()?;
                    work.charge_items(1, &budget)?;
                    let relative = path.trim_start_matches('/');
                    let absolute = Self::restore_absolute_path_measured(
                        relative, &mut allocations, &mut work, budget, cancellation,
                    )?;
                    let path = customer_path_measured(
                        &absolute, self.volume.config, &mut allocations, &mut work, budget, cancellation,
                    )?;
                    let absolute_bytes = absolute.capacity() as u64;
                    drop(absolute);
                    allocations.release(absolute_bytes).map_err(workspace_allocation_error)?;
                    parsed.push((relative, path));
                }
                let parsed_bytes = workspace_vec_bytes(&parsed);
                let mut lookup_paths = new_workspace_vec(
                    parsed.len(), &mut allocations, &mut work, budget, cancellation,
                )?;
                for (_, path) in &parsed {
                    cancellation.check()?;
                    work.charge_items(1, &budget)?;
                    lookup_paths.push(path.clone_with_ledger(
                        &mut allocations, &mut work, budget, cancellation,
                    ).map_err(namespace_path_error)?);
                }
                let lookup_bytes = workspace_vec_bytes(&lookup_paths)
                    + lookup_paths.iter().map(NamespacePath::retained_bytes).sum::<u64>();
                let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                let source_lookup = if selected_subtrees {
                    source_checkout.lookup_batch_no_follow_selected(
                        &lookup_paths, allowance, cancellation,
                    ).await
                } else {
                    source_checkout.lookup_batch_no_follow(
                        &lookup_paths, allowance, cancellation,
                    ).await
                };
                let source_records = merge_workspace_backend(
                    source_lookup,
                    &mut work, allocations.live_bytes(),
                )?.value;
                allocations.claim_bytes(source_records.retained_allocation_bytes, 0, &mut work, budget)
                    .map_err(workspace_allocation_error)?;
                let source_records_bytes = source_records.retained_allocation_bytes;
                let source_records = source_records.entries;
                drop(lookup_paths);
                allocations.release(lookup_bytes).map_err(workspace_allocation_error)?;
                let mut missing_count = 0;
                for entry in &source_records {
                    cancellation.check()?;
                    work.charge_items(1, &budget)?;
                    missing_count += usize::from(entry.record.is_none());
                }
                let mut missing_paths = new_workspace_vec(
                    missing_count, &mut allocations, &mut work, budget, cancellation,
                )?;
                for ((_, path), entry) in parsed.iter().zip(&source_records) {
                    cancellation.check()?;
                    work.charge_items(1, &budget)?;
                    if entry.record.is_none() {
                        missing_paths.push(path.clone_with_ledger(
                            &mut allocations, &mut work, budget, cancellation,
                        ).map_err(namespace_path_error)?);
                    }
                }
                let missing_bytes = workspace_vec_bytes(&missing_paths)
                    + missing_paths.iter().map(NamespacePath::retained_bytes).sum::<u64>();
                let (current_records, current_records_bytes) = if missing_paths.is_empty() {
                    (Vec::new(), 0)
                } else {
                    let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                    let records = merge_workspace_backend(
                        transaction.checkout.lookup_batch_no_follow(
                            &missing_paths, allowance, cancellation,
                        ).await, &mut work, allocations.live_bytes(),
                    )?.value;
                    allocations.claim_bytes(records.retained_allocation_bytes, 0, &mut work, budget)
                        .map_err(workspace_allocation_error)?;
                    (records.entries, records.retained_allocation_bytes)
                };
                drop(source_checkout);
                allocations.release(source_checkout_bytes).map_err(workspace_allocation_error)?;
                if selected_subtrees {
                    let mut removal_count = 0;
                    for entry in &current_records {
                        cancellation.check()?;
                        work.charge_items(1, &budget)?;
                        removal_count += usize::from(entry.record.is_some());
                    }
                    let mut removals = new_workspace_vec(
                        removal_count, &mut allocations, &mut work, budget, cancellation,
                    )?;
                    let mut removal_path_bytes = 0_u64;
                    let missing_vector_bytes = workspace_vec_bytes(&missing_paths);
                    for (path, entry) in missing_paths.into_iter().zip(&current_records) {
                        cancellation.check()?;
                        work.charge_items(1, &budget)?;
                        if let Some(record) = entry.record {
                            removal_path_bytes += path.retained_bytes();
                            removals.push(crate::kernel::Mutation::Remove {
                                path, expected_file_id: MetadataField::Value(record.file_id),
                            });
                        } else {
                            let bytes = path.retained_bytes();
                            drop(path);
                            allocations.release(bytes).map_err(workspace_allocation_error)?;
                        }
                    }
                    allocations.release(missing_vector_bytes).map_err(workspace_allocation_error)?;
                    if removals.is_empty() {
                        let bytes = workspace_vec_bytes(&removals);
                        drop(removals);
                        allocations.release(bytes).map_err(workspace_allocation_error)?;
                    } else {
                        allocations.release(workspace_vec_bytes(&removals) + removal_path_bytes)
                            .map_err(workspace_allocation_error)?;
                        let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                        merge_workspace_backend(
                            transaction.checkout.mutate(removals, allowance, cancellation).await,
                            &mut work, allocations.live_bytes(),
                        )?;
                    }
                } else {
                    drop(missing_paths);
                    allocations.release(missing_bytes).map_err(workspace_allocation_error)?;
                }
                let mut current_records = current_records.into_iter();
                let mut operations = new_workspace_vec(
                    paths.len(), &mut allocations, &mut work, budget, cancellation,
                )?;
                let mut operation_path_bytes = 0_u64;
                for ((relative, path), entry) in parsed.into_iter().zip(source_records) {
                    cancellation.check()?;
                    work.charge_items(1, &budget)?;
                    if let Some(record) = entry.record {
                        if selected_subtrees && record.kind == FileKind::Directory {
                            let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                            let destination = merge_workspace_backend(
                                transaction.checkout.lookup_no_follow(&path, allowance, cancellation).await,
                                &mut work, allocations.live_bytes(),
                            )?.value.record;
                            let absolute = Self::restore_absolute_path_measured(
                                relative, &mut allocations, &mut work, budget, cancellation,
                            )?;
                            if destination.is_some_and(|record| record.kind != FileKind::Directory) {
                                let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                                merge_workspace_backend(
                                    transaction.remove_measured(&absolute, allowance, cancellation).await
                                        .map(|work| crate::OperationReceipt { value: (), work }),
                                    &mut work, allocations.live_bytes(),
                                )?;
                            }
                            let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                            merge_workspace_backend(
                                transaction.create_dir_all_measured(&absolute, allowance, cancellation).await
                                    .map(|work| crate::OperationReceipt { value: (), work }),
                                &mut work, allocations.live_bytes(),
                            )?;
                            let absolute_bytes = absolute.capacity() as u64;
                            drop(absolute);
                            allocations.release(absolute_bytes).map_err(workspace_allocation_error)?;
                        }
                        if record.kind != FileKind::Directory
                            && let Some((parent, _)) = relative.rsplit_once('/')
                        {
                            let absolute = Self::restore_absolute_path_measured(
                                parent, &mut allocations, &mut work, budget, cancellation,
                            )?;
                            let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                            let live = allocations.live_bytes();
                            merge_workspace_backend(
                                transaction.create_dir_all_measured(&absolute, allowance, cancellation).await
                                    .map(|work| crate::OperationReceipt { value: (), work }),
                                &mut work, live,
                            )?;
                            let absolute_bytes = absolute.capacity() as u64;
                            drop(absolute);
                            allocations.release(absolute_bytes).map_err(workspace_allocation_error)?;
                        }
                        operation_path_bytes += path.retained_bytes();
                        operations.push(crate::kernel::Mutation::Restore { path, record });
                    } else {
                        let current_record = current_records.next()
                            .ok_or_else(|| WorkspaceError::manual_engine("missing batch lookup result"))?;
                        if !selected_subtrees && let Some(record) = current_record.record {
                            operation_path_bytes += path.retained_bytes();
                            operations.push(crate::kernel::Mutation::Remove {
                                path, expected_file_id: MetadataField::Value(record.file_id),
                            });
                        } else {
                            let path_bytes = path.retained_bytes();
                            drop(path);
                            allocations.release(path_bytes).map_err(workspace_allocation_error)?;
                        }
                    }
                }
                drop(current_records);
                allocations.release(parsed_bytes + source_records_bytes + current_records_bytes)
                    .map_err(workspace_allocation_error)?;
                if !operations.is_empty() {
                    let transferred = workspace_vec_bytes(&operations) + operation_path_bytes;
                    allocations.release(transferred).map_err(workspace_allocation_error)?;
                    let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                    merge_workspace_backend(
                        transaction.checkout.mutate(operations, allowance, cancellation).await,
                        &mut work, allocations.live_bytes(),
                    )?;
                } else {
                    let operation_bytes = workspace_vec_bytes(&operations);
                    drop(operations);
                    allocations.release(operation_bytes).map_err(workspace_allocation_error)?;
                }
                let allowance = workspace_remaining(work, budget, allocations.live_bytes())?;
                let committed = merge_workspace_backend(
                    transaction.commit_with_permit_measured_guarded(
                        permit, allowance, cancellation, &mut before_publication,
                    ).await, &mut work, allocations.live_bytes(),
                )?;
                Ok(committed.value)
            }.await;
            work.verify(budget)
                .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
            result.map(|value| crate::OperationReceipt { value, work })
                .map_err(|error| crate::OperationFailure::new(error, work))
        }, budget, cancellation, WorkspaceError::Cancelled(crate::CancellationError), None).await
    }

    fn restore_absolute_path_measured(
        relative: &str,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<String, WorkspaceError> {
        let length = relative.len().checked_add(1).ok_or(crate::WorkError::Overflow)?;
        let mut bytes = new_workspace_vec::<u8>(length, allocations, work, budget, cancellation)?;
        let admitted = cancellation.check().map_err(WorkspaceError::from).and_then(|()| {
            work.charge(&crate::WorkCounters {
                bytes_copied: length as u64, ..crate::WorkCounters::UNCHARGED
            }, &budget).map_err(WorkspaceError::from)
        });
        if let Err(error) = admitted {
            let claimed = workspace_vec_bytes(&bytes);
            drop(bytes);
            allocations.release(claimed).map_err(workspace_allocation_error)?;
            return Err(error);
        }
        bytes.push(b'/');
        bytes.extend_from_slice(relative.as_bytes());
        String::from_utf8(bytes).map_err(WorkspaceError::engine)
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

    /// Whether a path holds the same thing in two states: the same record,
    /// or one file state under another identity or timestamps (see
    /// `Fs::same_file_state`), as when a capture records afresh a file a
    /// publication wrote. A hard link's identity is its other names, so it
    /// must match exactly.
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
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.workspace.sync",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            workspace_id = crate::obs::hex(&self.id.0),
        );
        crate::obs::outcome_on(
            &span,
            crate::obs::in_span(&span, async move {
                Ok(WorkspaceSync {
                    generation: self.head().await?,
                })
            })
            .await,
        )
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
        self.fork_measured(
            destination,
            options,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
    }

    /// Forks one exact generation under caller-supplied work and cancellation
    /// limits, returning exact work on success and on every partial failure.
    /// Reuse the same destination, pinned generation, selection and idempotency
    /// key to reconcile an uncertain publication. A failed call can leave staged
    /// objects, or a committed workspace if cancellation follows publication.
    ///
    /// # Errors
    ///
    /// Rejects foreign generations, invalid destinations, exhausted work,
    /// cancellation, incompatible retries and storage/publication failures.
    pub async fn fork_measured(
        &self,
        destination: impl AsRef<str>,
        options: ForkOptions<A, O>,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Self>, WorkspaceError> {
        self.fork_measured_inner::<
            fn(crate::WorkBudget) -> std::future::Ready<crate::facade::FsResult<()>>,
            std::future::Ready<crate::facade::FsResult<()>>,
        >(destination, options, crate::PublicationPermit::Unrestricted, budget, cancellation, None).await
    }

    /// Forks the supplied exact generation with the original owner guard at each destination seal.
    /// The guard is awaited after actual preparation, never merely before planning.
    pub async fn fork_measured_guarded<F, Fut>(
        &self, destination: impl AsRef<str>, options: ForkOptions<A, O>,
        permit: crate::PublicationPermit, budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken, mut guard: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Self>, WorkspaceError>
    where
        F: FnMut(crate::WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::facade::FsResult<()>> + Send,
    {
        self.fork_measured_inner(destination, options, permit, budget, cancellation, Some(&mut guard)).await
    }

    async fn fork_measured_inner<F, Fut>(
        &self, destination: impl AsRef<str>, options: ForkOptions<A, O>,
        permit: crate::PublicationPermit, budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken, guard: Option<&mut F>,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Self>, WorkspaceError>
    where
        F: FnMut(crate::WorkBudget) -> Fut + Send,
        Fut: Future<Output = crate::facade::FsResult<()>> + Send,
    {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.workspace.fork",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            workspace_id = crate::obs::hex(&self.id.0),
        );
        crate::obs::measured_on(
            &span,
            crate::obs::in_span(&span, async move {
                let mut prior = crate::WorkCounters::UNCHARGED;
                let mut allocations = crate::kernel::allocation::AllocationLedger::default();
                let path_bytes = if let Some(paths) = &options.paths {
                    workspace_owned_strings_bytes(paths)
                        .map_err(|error| crate::OperationFailure::new(error, prior))?
                } else { 0 };
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
                let forked = if let Some(guard) = guard {
                    self.volume.fs.fork_workspace_measured_guarded(
                        named.value, &options.generation, options.idempotency_key, options.paths,
                        permit, remaining, cancellation, guard,
                    ).await
                } else {
                    self.volume.fs.fork_workspace_measured(
                        named.value, &options.generation, options.idempotency_key, options.paths,
                        remaining, cancellation,
                    ).await
                }.map_err(|failure| failure.map_with_prior_work(prior, |error| error))?;
                let work = prior.checked_add(forked.work)
                    .map_err(|error| crate::OperationFailure::new(WorkspaceError::Work(error), prior))?;
                work.verify(budget)
                    .map_err(|error| crate::OperationFailure::new(WorkspaceError::Work(error), work))?;
                Ok(crate::OperationReceipt { value: forked.value, work })
            })
            .await,
            |receipt| &receipt.work,
        )
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
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.workspace.begin_transaction",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            workspace_id = crate::obs::hex(&self.id.0),
        );
        crate::obs::outcome_on(
            &span,
            crate::obs::in_span(
                &span,
                self.begin_transaction_measured(
                    idempotency_key,
                    crate::WorkBudget::UNBOUNDED,
                    &crate::CancellationToken::new(),
                ),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error),
        )
    }

    /// Opens a head transaction under exact work and cancellation bounds.
    pub async fn begin_transaction_measured(
        &self,
        idempotency_key: IdempotencyKey,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Transaction<A, O>>, WorkspaceError> {
        let head = self
            .head_measured(budget, cancellation)
            .await?;
        let transaction = self
            .open_transaction_at_measured(
                &head.value,
                idempotency_key,
                false,
                head.work
                    .remaining(budget)
                    .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), head.work))?,
                cancellation,
            )
            .await
            .map_err(|failure| failure.map_with_prior_work(head.work, |error| error))?;
        Ok(crate::OperationReceipt {
            value: transaction.value,
            work: head
                .work
                .checked_add(transaction.work)
                .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), head.work))?,
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
        .map_err(|failure| failure.error)
    }

    async fn open_transaction_at_measured(
        &self,
        generation: &Generation<A, O>,
        idempotency_key: IdempotencyKey,
        requires_rebase: bool,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Transaction<A, O>>, WorkspaceError> {
        let receipt = self
            .checkout_measured(
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
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.workspace.checkpoint",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            workspace_id = crate::obs::hex(&self.id.0),
        );
        crate::obs::outcome_on(
            &span,
            crate::obs::in_span(&span, async move {
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
            })
            .await,
        )
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
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.workspace.diff",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            workspace_id = crate::obs::hex(&self.id.0),
        );
        crate::obs::outcome_on(
            &span,
            crate::obs::in_span(&span, async move {
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
            })
            .await,
        )
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
            })
            .await,
        )
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

    /// Reads the head through the canonical engine with exact failure work.
    pub async fn read_measured(
        &self,
        path: &str,
        maximum_bytes: u64,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Bytes>, WorkspaceError> {
        self.read_selected_measured(GenerationSelector::Head, path, maximum_bytes, budget, cancellation).await
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

    /// Reads one head range with actual checkout, allocation, and failed-work receipts.
    pub async fn read_range_measured(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Bytes>, WorkspaceError> {
        read_generation_range_measured(
            self, GenerationSelector::Head, path, offset, length, budget, cancellation,
        ).await
    }

    pub(crate) async fn read_range_owned_measured(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::kernel::OwnedSourceChunk>, WorkspaceError> {
        read_generation_range_owned_measured(
            self, GenerationSelector::Head, path, offset, length, budget, cancellation,
        ).await
    }

    /// Returns complete path identity, kind, size, link count, and metadata.
    ///
    /// # Errors
    ///
    /// Rejects invalid or absent paths and authenticated backend failures.
    pub async fn stat(&self, path: &str) -> Result<WorkspaceStat, WorkspaceError> {
        stat_generation(self, GenerationSelector::Head, path).await
    }

    /// Resolves the canonical no-follow head identity under exact work bounds.
    pub async fn stat_measured(
        &self,
        path: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<WorkspaceStat>, WorkspaceError> {
        stat_generation_measured(self, GenerationSelector::Head, path, budget, cancellation).await
    }

    pub(crate) async fn stat_optional_measured(
        &self,
        path: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Option<WorkspaceStat>>, WorkspaceError> {
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
        self.volume.fs.make_records_durable_measured(
            self.volume.config, records, budget, cancellation,
        ).await.map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::default(), WorkspaceError::engine))
    }

    pub(crate) fn detached_record(
        &self,
        record: crate::kernel::FileRecord,
    ) -> crate::DetachedFile<A, O> {
        crate::DetachedFile::from_record(self.volume.clone(), record)
    }

    pub(crate) fn detached_record_measured(
        &self,
        record: crate::kernel::FileRecord,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::DetachedFile<A, O>>, WorkspaceError> {
        crate::DetachedFile::from_record_measured(self.volume.clone(), record, budget, cancellation)
            .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::default(), WorkspaceError::from))
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

    /// Returns an authenticated directory page with exact success and failure work.
    pub async fn list_directory_measured(
        &self,
        path: &str,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<WorkspaceDirectoryPage>, WorkspaceError> {
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

    /// Reads a symbolic-link target with the original finite work span.
    ///
    /// # Errors
    ///
    /// Preserves actual checkout, lookup and target-read work on every failure.
    pub async fn read_symbolic_link_measured(
        &self, path: &str, budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Bytes>, WorkspaceError> {
        read_generation_symbolic_link_measured(
            self, GenerationSelector::Head, path, budget, cancellation,
        ).await
    }

    pub(crate) async fn read_symbolic_link_owned_measured(
        &self,
        path: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::kernel::OwnedSourceChunk>, WorkspaceError> {
        read_generation_symbolic_link_owned_measured(
            self, GenerationSelector::Head, path, budget, cancellation,
        ).await
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
        self.read_selected_measured(selector, path, maximum_bytes, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    }

    async fn read_selected_measured(
        &self,
        selector: GenerationSelector,
        path: &str,
        maximum_bytes: u64,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Bytes>, WorkspaceError> {
        let opened = self.checkout_measured(selector, CheckoutMode::read_only_pinned(), budget, cancellation).await?;
        let mut work = opened.work;
        let mut checkout = opened.value;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        allocations.claim_bytes(checkout.logical_owned_bytes(), 0, &mut work, budget)
            .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), work))?;
        let path = customer_path_measured(path, checkout.volume_config(), &mut allocations, &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        let lookup = checkout.lookup_no_follow(
            &path,
            workspace_remaining(work, budget, allocations.live_bytes()).map_err(|error| crate::OperationFailure::new(error, work))?,
            cancellation,
        ).await.map_err(|failure| {
            match work.with_backend(*failure.work, allocations.live_bytes()) {
                Ok(failed) => crate::OperationFailure::new(WorkspaceError::from(failure.error), failed),
                Err(error) => crate::OperationFailure::new(WorkspaceError::from(error), work),
            }
        })?;
        work = work.with_backend(lookup.work, allocations.live_bytes())
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
        let record = lookup.value.record
            .ok_or_else(|| crate::OperationFailure::new(WorkspaceError::NotFound, work))?;
        if record.kind != FileKind::Regular {
            return Err(crate::OperationFailure::new(WorkspaceError::NotRegularFile, work));
        }
        let logical_bytes = regular_file_bytes(record)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        if logical_bytes > maximum_bytes {
            return Err(crate::OperationFailure::new(WorkspaceError::ReadLimitExceeded, work));
        }
        let read = checkout.read_file_range(
            &path,
            crate::ByteRange { offset: 0, length: logical_bytes },
            workspace_remaining(work, budget, allocations.live_bytes()).map_err(|error| crate::OperationFailure::new(error, work))?,
            cancellation,
        ).await.map_err(|failure| {
            match work.with_backend(*failure.work, allocations.live_bytes()) {
                Ok(failed) => crate::OperationFailure::new(WorkspaceError::from(failure.error), failed),
                Err(error) => crate::OperationFailure::new(WorkspaceError::from(error), work),
            }
        })?;
        let work = work.with_backend(read.work, allocations.live_bytes())
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
        Ok(crate::OperationReceipt { value: read.value.bytes, work })
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
        .map_err(|failure| failure.error)
    }

    pub(crate) async fn engine_checkout_measured(
        &self,
        selector: GenerationSelector,
        mode: CheckoutMode,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Checkout<A, O>>, WorkspaceError> {
        self.volume
            .checkout(selector, mode, budget, cancellation)
            .await
            .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::default(), WorkspaceError::from))
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
    read_generation_range_measured(
        workspace, selector, path, offset, length,
        crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
    ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
}

async fn read_generation_range_measured<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    offset: u64,
    length: u64,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<Bytes>, WorkspaceError> {
    let receipt = read_generation_range_owned_measured(
        workspace, selector, path, offset, length, budget, cancellation,
    ).await?;
    Ok(crate::OperationReceipt { value: receipt.value.bytes, work: receipt.work })
}

async fn read_generation_range_owned_measured<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    offset: u64,
    length: u64,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<crate::kernel::OwnedSourceChunk>, WorkspaceError> {
    let opened = workspace
        .engine_checkout_measured(selector, CheckoutMode::read_only_pinned(), budget, cancellation)
        .await?;
    let mut work = opened.work;
    let mut checkout = opened.value;
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    allocations.claim_bytes(checkout.logical_owned_bytes(), 0, &mut work, budget)
        .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), work))?;
    let path = customer_path_measured(path, checkout.volume_config(), &mut allocations, &mut work, budget, cancellation)
        .map_err(|error| crate::OperationFailure::new(error, work))?;
    let read = checkout.read_file_range(
        &path,
        crate::ByteRange { offset, length },
        workspace_remaining(work, budget, allocations.live_bytes())
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?,
        cancellation,
    ).await.map_err(|failure| {
        match work.with_backend(*failure.work, allocations.live_bytes()) {
            Ok(failed) => crate::OperationFailure::new(WorkspaceError::from(failure.error), failed),
            Err(error) => crate::OperationFailure::new(WorkspaceError::from(error), work),
        }
    })?;
    let work = work.with_backend(read.work, allocations.live_bytes())
        .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
    let logical_owned_bytes = read.value.logical_owned_bytes();
    Ok(crate::OperationReceipt {
        value: crate::kernel::OwnedSourceChunk { bytes: read.value.bytes, logical_owned_bytes }, work,
    })
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
    .map_err(|failure| failure.error)
}

async fn stat_generation_measured<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<WorkspaceStat>, WorkspaceError> {
    let receipt =
        stat_generation_optional_measured(workspace, selector, path, budget, cancellation).await?;
    Ok(crate::OperationReceipt {
        value: receipt.value.ok_or_else(|| crate::OperationFailure::new(WorkspaceError::NotFound, receipt.work))?,
        work: receipt.work,
    })
}

async fn stat_generation_optional_measured<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<Option<WorkspaceStat>>, WorkspaceError> {
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
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    allocations.claim_bytes(checkout.logical_owned_bytes(), 0, &mut work, budget)
        .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), work))?;
    let path = customer_path_measured(path, checkout.volume_config(), &mut allocations, &mut work, budget, cancellation)
        .map_err(|error| crate::OperationFailure::new(error, work))?;
    let lookup = checkout
        .lookup_no_follow_with_metadata(
            &path,
            workspace_remaining(work, budget, allocations.live_bytes()).map_err(|error| crate::OperationFailure::new(error, work))?,
            cancellation,
        )
        .await
        .map_err(|failure| {
            match work.with_backend(*failure.work, allocations.live_bytes()) {
                Ok(failed) => crate::OperationFailure::new(WorkspaceError::from(failure.error), failed),
                Err(error) => crate::OperationFailure::new(WorkspaceError::from(error), work),
            }
        })?;
    work = work.with_backend(lookup.work, allocations.live_bytes())
        .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
    let Some(lookup) = lookup.value else {
        return Ok(crate::OperationReceipt { value: None, work });
    };
    let logical_bytes = match &lookup.record.payload {
        FilePayload::InlineRegular(bytes) => {
            Some(u64::try_from(bytes.as_bytes().len())
                .map_err(|error| crate::OperationFailure::new(WorkspaceError::engine(error), work))?)
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
    .map_err(|failure| failure.error)
}

async fn list_generation_directory_measured<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    after: Option<&LogicalName>,
    maximum_entries: u32,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<WorkspaceDirectoryPage>, WorkspaceError> {
    let checkout = workspace
        .engine_checkout_measured(
            selector,
            CheckoutMode::read_only_pinned(),
            budget,
            cancellation,
        )
        .await?;
    let mut prior = checkout.work;
    let mut checkout = checkout.value;
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    allocations.claim_bytes(checkout.logical_owned_bytes(), 0, &mut prior, budget)
        .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), prior))?;
    let path = customer_path_measured(path, checkout.volume_config(), &mut allocations, &mut prior, budget, cancellation)
        .map_err(|error| crate::OperationFailure::new(error, prior))?;
    let page = checkout
        .list_directory(
            &path,
            after,
            maximum_entries,
            workspace_remaining(prior, budget, allocations.live_bytes()).map_err(|error| crate::OperationFailure::new(error, prior))?,
            cancellation,
        )
        .await
        .map_err(|failure| {
            match prior.with_backend(*failure.work, allocations.live_bytes()) {
                Ok(failed) => crate::OperationFailure::new(WorkspaceError::from(failure.error), failed),
                Err(error) => crate::OperationFailure::new(WorkspaceError::from(error), prior),
            }
        })?;
    let mut work = prior.with_backend(page.work, allocations.live_bytes())
        .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
    let source_entries = page.value.entries;
    let source_bytes = workspace_vec_bytes(&source_entries)
        + source_entries.iter().map(|entry| entry.name.retained_bytes() as u64).sum::<u64>();
    allocations.claim_bytes(source_bytes, 0, &mut work, budget)
        .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), work))?;
    cancellation.check().map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
    allocations.claim_elements::<WorkspaceDirectoryEntry>(source_entries.len(), &mut work, budget)
        .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), work))?;
    let mut entries = Vec::new();
    entries.try_reserve_exact(source_entries.len())
        .map_err(|error| crate::OperationFailure::new(WorkspaceError::engine(error), work))?;
    for entry in source_entries {
        cancellation.check().map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
        work.charge_items(1, &budget)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
        entries.push(WorkspaceDirectoryEntry {
            name: entry.name, file_id: entry.file_id, kind: entry.kind,
        });
    }
    Ok(crate::OperationReceipt {
        value: WorkspaceDirectoryPage {
            entries,
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
    read_generation_symbolic_link_measured(
        workspace, selector, path, crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
}

async fn read_generation_symbolic_link_measured<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<Bytes>, WorkspaceError> {
    let receipt = read_generation_symbolic_link_owned_measured(
        workspace, selector, path, budget, cancellation,
    ).await?;
    Ok(crate::OperationReceipt { value: receipt.value.bytes, work: receipt.work })
}

async fn read_generation_symbolic_link_owned_measured<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    selector: GenerationSelector,
    path: &str,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> crate::MeasuredResult<crate::OperationReceipt<crate::kernel::OwnedSourceChunk>, WorkspaceError> {
    let opened = workspace.engine_checkout_measured(
        selector, CheckoutMode::read_only_pinned(), budget, cancellation,
    ).await?;
    let mut work = opened.work;
    let mut checkout = opened.value;
    let mut allocations = crate::kernel::allocation::AllocationLedger::default();
    allocations.claim_bytes(checkout.logical_owned_bytes(), 0, &mut work, budget)
        .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), work))?;
    let path = customer_path_measured(
        path, checkout.volume_config(), &mut allocations, &mut work, budget, cancellation,
    ).map_err(|error| crate::OperationFailure::new(error, work))?;
    let allowance = workspace_remaining(work, budget, allocations.live_bytes())
        .map_err(|error| crate::OperationFailure::new(error, work))?;
    let read = checkout.read_symbolic_link_owned(&path, allowance, cancellation).await
        .map_err(|failure| match work.with_backend(*failure.work, allocations.live_bytes()) {
            Ok(failed) => crate::OperationFailure::new(WorkspaceError::from(failure.error), failed),
            Err(error) => crate::OperationFailure::new(WorkspaceError::Work(error), work),
        })?;
    work = work.with_backend(read.work, allocations.live_bytes())
        .map_err(|error| crate::OperationFailure::new(WorkspaceError::Work(error), work))?;
    work.verify(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::Work(error), work))?;
    Ok(crate::OperationReceipt { value: read.value, work })
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
    /// Reuses the workspace transaction compiler against a private checkout
    /// candidate without publishing a second workspace head.
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
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        let mut work = crate::WorkCounters::default();
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let path = customer_path_measured(path, self.checkout.volume_config(),
            &mut allocations, &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        let mut operations = new_workspace_vec(1, &mut allocations, &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        operations.push(crate::kernel::Mutation::Restore { path, record });
        allocations.release(allocations.live_bytes())
            .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), work))?;
        self.apply_engine_measured(
            operations,
            work.remaining(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?,
            cancellation,
        )
        .await.map_err(|failure| failure.map_with_prior_work(work, |error| error))
            .and_then(|applied| work.checked_add(applied).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work)))
    }

    /// Creates the same parent directories under cumulative engine bounds.
    pub async fn create_dir_all_measured(
        &mut self,
        path: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        let mut work = crate::WorkCounters::default();
        let result = async {
        cancellation.check().map_err(WorkspaceError::from)?;
        let depth = PortablePath::validate(path, self.checkout.volume_config().limits)
            .map_err(|error| namespace_path_error(crate::kernel::NamespacePathError::Path(error)))?;
        if depth == 0 { return Ok(work); }
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        for end in path.match_indices('/').skip(1).map(|(index, _)| index)
            .chain(std::iter::once(path.len())) {
            let path = customer_path_measured(&path[..end], self.checkout.volume_config(),
                &mut allocations, &mut work, budget, cancellation)?;
            let retained = path.retained_bytes();
            let lookup = self
                .checkout
                .lookup_no_follow(
                    &path,
                    path_remaining(work, budget, &path).map_err(WorkspaceError::from)?,
                    cancellation,
                )
                .await
                .map_err(|failure| {
                    match work.with_backend(*failure.work, retained) {
                        Ok(failed) => { work = failed; WorkspaceError::from(failure.error) }
                        Err(error) => WorkspaceError::from(error),
                    }
                })?;
            work = work.with_backend(lookup.work, retained).map_err(WorkspaceError::from)?;
            let mut transferred = false;
            match lookup.value.record {
                Some(value) if value.kind == FileKind::Directory => {}
                Some(_) => return Err(WorkspaceError::NotDirectory),
                None => {
                    let mut operations = new_workspace_vec(1, &mut allocations, &mut work, budget, cancellation)?;
                    operations.push(AuthoredMutation::CreateDirectory {
                        path, metadata: FileMetadata::default(),
                    });
                    allocations.release(retained + workspace_vec_bytes(&operations))
                        .map_err(workspace_allocation_error)?;
                    transferred = true;
                    let applied = self.checkout.apply_authored_transaction(
                            operations,
                            work.remaining(budget).map_err(WorkspaceError::from)?,
                            cancellation,
                        )
                        .await
                        .map_err(|failure| accumulate_workspace_failure(failure, &mut work, WorkspaceError::from))?;
                    work = work.checked_add(applied.work).map_err(WorkspaceError::from)?;
                }
            }
            if !transferred {
                allocations.release(retained).map_err(workspace_allocation_error)?;
            }
        }
        Ok(work)
        }.await;
        result.map_err(|error| crate::OperationFailure::new(error, work))
    }

    pub(crate) async fn apply_authored_measured(
        &mut self,
        operation: AuthoredMutation,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        let mut work = crate::WorkCounters::default();
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        cancellation.check().map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
        allocations.claim_bytes(operation.logical_owned_bytes(), 0, &mut work, budget)
            .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), work))?;
        let mut operations = new_workspace_vec(1, &mut allocations, &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        operations.push(operation);
        allocations.release(allocations.live_bytes())
            .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), work))?;
        let remaining = work.remaining(budget)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
        let applied = self.apply_measured(operations, remaining, cancellation).await
            .map_err(|failure| failure.map_with_prior_work(work, |error| error))?;
        work.checked_add(applied)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))
    }

    // The mutation owns the freshly constructed paths. Transfer their live
    // storage into the engine's input scope: keep the remaining peak limit
    // intact and merge receipts without adding a borrowed-path offset.
    async fn apply_authored_after_path_measured(
        &mut self,
        operation: AuthoredMutation,
        prior: crate::WorkCounters,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        let remaining = prior.remaining(budget)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
        let applied = self.apply_authored_measured(operation, remaining, cancellation).await
            .map_err(|failure| failure.map_with_prior_work(prior, |error| error))?;
        prior.checked_add(applied)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))
    }

    /// Admits a bounded ordered capture as one unpublished candidate update.
    pub(crate) async fn apply_authored_bulk_measured(
        &mut self,
        operations: Vec<AuthoredMutation>,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        self.checkout
            .apply_authored_bulk_transaction(operations, budget, cancellation)
            .await
            .map(|receipt| receipt.work)
            .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::default(), WorkspaceError::from))
    }

    pub(crate) async fn create_directory_measured(
        &mut self,
        path: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        let mut work = crate::WorkCounters::default();
        let path = customer_path_measured(path, self.checkout.volume_config(),
            &mut crate::kernel::allocation::AllocationLedger::default(), &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        self.apply_authored_after_path_measured(
            AuthoredMutation::CreateDirectory {
                path,
                metadata: FileMetadata::default(),
            },
            work,
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
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        let mut work = crate::WorkCounters::default();
        let path = customer_path_measured(path, self.checkout.volume_config(),
            &mut crate::kernel::allocation::AllocationLedger::default(), &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        self.apply_authored_after_path_measured(
            AuthoredMutation::CreateFile {
                path,
                bytes,
                metadata: FileMetadata::default(),
            },
            work,
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
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        let mut work = crate::WorkCounters::default();
        let path = customer_path_measured(path, self.checkout.volume_config(),
            &mut crate::kernel::allocation::AllocationLedger::default(), &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        self.apply_authored_after_path_measured(
            AuthoredMutation::Write {
                path,
                offset,
                bytes,
            },
            work,
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
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        let mut work = crate::WorkCounters::default();
        let path = customer_path_measured(path, self.checkout.volume_config(),
            &mut crate::kernel::allocation::AllocationLedger::default(), &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        self.apply_authored_after_path_measured(
            AuthoredMutation::CreateSymbolicLink {
                path,
                target,
                metadata: FileMetadata::default(),
            },
            work,
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
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        if !matches!((kind, device),
            (FileKind::Fifo | FileKind::Socket, None) |
            (FileKind::CharacterDevice | FileKind::BlockDevice, Some(_))) {
            return Err(crate::OperationFailure::before_work(WorkspaceError::IncompatibleWorkspace));
        }
        let mut work = crate::WorkCounters::default();
        let path = customer_path_measured(path, self.checkout.volume_config(),
            &mut crate::kernel::allocation::AllocationLedger::default(), &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
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
            _ => return Err(crate::OperationFailure::new(WorkspaceError::IncompatibleWorkspace, work)),
        };
        self.apply_authored_after_path_measured(operation, work, budget, cancellation)
            .await
    }

    pub(crate) async fn set_metadata_measured(
        &mut self,
        path: &str,
        metadata: FileMetadata,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        let mut work = crate::WorkCounters::default();
        let path = customer_path_measured(path, self.checkout.volume_config(),
            &mut crate::kernel::allocation::AllocationLedger::default(), &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        self.apply_authored_after_path_measured(
            AuthoredMutation::SetMetadata { path, metadata },
            work,
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
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        let mut work = crate::WorkCounters::default();
        let path = customer_path_measured(path, self.checkout.volume_config(),
            &mut crate::kernel::allocation::AllocationLedger::default(), &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        self.apply_authored_after_path_measured(
            AuthoredMutation::Reidentify { path, file_id },
            work,
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
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        let config = self.checkout.volume_config();
        let mut work = crate::WorkCounters::default();
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let source = customer_path_measured(source, config, &mut allocations, &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        let destination = customer_path_measured(destination, config, &mut allocations, &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        self.apply_authored_after_path_measured(
            AuthoredMutation::HardLink { source, destination },
            work,
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
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        self.checkout
            .mutate(operations, budget, cancellation)
            .await
            .map(|receipt| receipt.work)
            .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::default(), WorkspaceError::from))
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

    pub(crate) async fn stage_content_measured<R: crate::kernel::AsyncBlobSource>(&self,
    source: &mut R,
    maximum_source_bytes: u64,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,) -> crate::MeasuredResult<crate::OperationReceipt<crate::StagedContent>, WorkspaceError> { self.checkout.stage_content(source, maximum_source_bytes, budget, cancellation).await.map(|receipt| crate::OperationReceipt { value: receipt.value, work: receipt.work }).map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::default(), WorkspaceError::from)) }

    /// Creates every missing directory on one canonical absolute path.
    ///
    /// # Errors
    ///
    /// Rejects invalid paths, a non-directory prefix, or a bounded engine failure.
    pub async fn create_dir_all(&mut self, path: &str) -> Result<(), WorkspaceError> {
        self.create_dir_all_measured(
            path, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).await.map(|_| ()).map_err(|failure| failure.error)
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
        self.write_measured(path, bytes, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await
            .map(|_| ())
            .map_err(|failure| failure.error)
    }

    /// Creates or replaces the same complete file under exact operation bounds.
    /// Parent directories must already exist, as for [`Self::write`].
    /// Transfers the original immutable byte buffer without copying its body.
    /// Counters include path construction, lookup, and atomic mutation work;
    /// failures retain that prefix and leave the pending candidate unpublished.
    pub async fn write_measured(
        &mut self,
        path: &str,
        bytes: Bytes,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        let mut work = crate::WorkCounters::UNCHARGED;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let path = customer_path_measured(path, self.checkout.volume_config(),
            &mut allocations, &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        let remaining = path_remaining(work, budget, &path)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
        let lookup = self.checkout.lookup_no_follow(&path, remaining, cancellation).await
            .map_err(|failure| {
                match work.with_backend(*failure.work, path.retained_bytes()) {
                    Ok(failed) => crate::OperationFailure::new(WorkspaceError::from(failure.error), failed),
                    Err(error) => crate::OperationFailure::new(WorkspaceError::from(error), work),
                }
            })?;
        work = work.with_backend(lookup.work, path.retained_bytes())
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
        let existing = lookup.value.record;
        let operations = match existing {
            None => {
                let mut operations = new_workspace_vec(1, &mut allocations, &mut work, budget, cancellation)
                    .map_err(|error| crate::OperationFailure::new(error, work))?;
                operations.push(AuthoredMutation::CreateFile {
                    path, bytes, metadata: FileMetadata::default(),
                });
                operations
            },
            Some(value) if value.kind == FileKind::Regular => {
                let copied = path.clone_with_ledger(
                    &mut allocations, &mut work, budget, cancellation,
                ).map_err(|error| crate::OperationFailure::new(namespace_path_error(error), work))?;
                let mut operations = new_workspace_vec(2, &mut allocations, &mut work, budget, cancellation)
                    .map_err(|error| crate::OperationFailure::new(error, work))?;
                operations.push(AuthoredMutation::Resize { path: copied, logical_bytes: 0 });
                operations.push(AuthoredMutation::Write { path, offset: 0, bytes });
                operations
            },
            Some(_) => return Err(crate::OperationFailure::new(WorkspaceError::NotRegularFile, work)),
        };
        allocations.release(allocations.live_bytes())
            .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), work))?;
        let applied = self.checkout.apply_authored_transaction(
            operations,
            work.remaining(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?,
            cancellation,
        ).await.map_err(|failure| failure.map_with_prior_work(work, WorkspaceError::from))?;
        work.checked_add(applied.work)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))
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
        self.remove_measured(path, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await
            .map(|_| ())
            .map_err(|failure| failure.error)
    }

    /// Removes the same authenticated binding under cumulative operation bounds.
    /// A failed mutation retains the transaction's original pending candidate.
    /// Counters include canonical path construction, identity lookup, and the
    /// atomic removal, including completed work when a later stage fails.
    pub async fn remove_measured(
        &mut self,
        path: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        let mut work = crate::WorkCounters::UNCHARGED;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let path = customer_path_measured(path, self.checkout.volume_config(),
            &mut allocations, &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        let remaining = path_remaining(work, budget, &path)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
        let lookup = self.checkout.lookup_no_follow(&path, remaining, cancellation).await
            .map_err(|failure| {
                match work.with_backend(*failure.work, path.retained_bytes()) {
                    Ok(failed) => crate::OperationFailure::new(WorkspaceError::from(failure.error), failed),
                    Err(error) => crate::OperationFailure::new(WorkspaceError::from(error), work),
                }
            })?;
        work = work.with_backend(lookup.work, path.retained_bytes())
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
        let existing = lookup.value.record
            .ok_or_else(|| crate::OperationFailure::new(WorkspaceError::NotFound, work))?;
        let mut operations = new_workspace_vec(1, &mut allocations, &mut work, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, work))?;
        operations.push(AuthoredMutation::Remove {
            path,
            expected_file_id: Some(existing.file_id),
        });
        allocations.release(allocations.live_bytes())
            .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), work))?;
        let remaining = work.remaining(budget)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
        let applied = self.apply_measured(operations, remaining, cancellation).await
            .map_err(|failure| failure.map_with_prior_work(work, |error| error))?;
        work.checked_add(applied)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))
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
        self.commit_with_permit_measured(
            permit,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
    }

    /// Publishes this transaction under an exact work budget and cancellation token.
    pub async fn commit_with_permit_measured(
        &mut self,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError> {
        self.commit_with_permit_measured_guarded(
            permit, budget, cancellation,
            |_| async { Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) },
        ).await
    }

    /// Publishes through the original seal with the caller's asynchronous owner
    /// check after closure preparation and before authoritative publication.
    /// The check receives the current allowance and contributes its complete
    /// receipt, including failed work, to this same transaction operation.
    pub async fn commit_with_permit_measured_guarded<F, Fut>(
        &mut self,
        permit: crate::PublicationPermit,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        mut before_publication: F,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError>
    where
        F: FnMut(crate::WorkBudget) -> Fut,
        Fut: Future<Output = crate::FsResult<()>> + Send,
    {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.transaction.commit",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty
        );
        let result = crate::obs::in_span(
            &span,
            crate::heap_future::in_heap_measured(move |budget| async move {
                if self.requires_rebase {
                    let receipt = self
                        .checkout
                        .retry_stale_commit_guarded(
                            self.idempotency_key.operation_id(),
                            permit,
                            budget,
                            cancellation,
                            &mut before_publication,
                            None,
                        )
                        .await
                        .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::UNCHARGED, WorkspaceError::from))?;
                    let work = receipt.work;
                    let Some(outcome) = receipt.value else {
                        let actual = self
                            .workspace
                            .head_measured(
                                work.remaining(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?,
                                cancellation,
                            )
                            .await
                            .map_err(|failure| failure.map_with_prior_work(work, |error| error))?;
                        return Ok(crate::OperationReceipt {
                            value: TransactionCommit::Conflict {
                                actual: actual.value,
                            },
                            work: work
                                .checked_add(actual.work)
                                .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?,
                        });
                    };
                    return self
                        .commit_outcome_measured(outcome, work, budget, cancellation)
                        .await;
                }
                let receipt = self
                    .checkout
                    .commit_with_permit_guarded(
                        self.idempotency_key.operation_id(),
                        permit,
                        budget,
                        cancellation,
                        &mut before_publication,
                    )
                    .await
                    .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::UNCHARGED, WorkspaceError::from))?;
                self.commit_outcome_measured(receipt.value, receipt.work, budget, cancellation)
                    .await
            }, budget, cancellation, WorkspaceError::Cancelled(crate::CancellationError), None),
        ).await;
        crate::obs::measured_on(&span, result, |receipt| &receipt.work)
    }

    async fn commit_outcome_measured(
        &self,
        outcome: CheckoutCommitOutcome,
        work: crate::WorkCounters,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<TransactionCommit<A, O>>, WorkspaceError> {
        // This outcome is part of the already-admitted commit future. Do not
        // allocate another unbounded future after authority publication.
        let value = match outcome {
            CheckoutCommitOutcome::Committed { generation_id, .. } => {
                TransactionCommit::Committed(Generation {
                    workspace: self.workspace.clone(),
                    id: generation_id,
                    publication_closure: self.checkout.publication_closure(generation_id),
                })
            }
            CheckoutCommitOutcome::AlreadyCommitted { generation_id, .. } => {
                TransactionCommit::AlreadyCommitted(Generation {
                    workspace: self.workspace.clone(),
                    id: generation_id,
                    publication_closure: self.checkout.publication_closure(generation_id),
                })
            }
            CheckoutCommitOutcome::Conflict { .. } => {
                let actual = self
                    .workspace
                    .head_measured(
                        work.remaining(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| failure.map_with_prior_work(work, |error| error))?;
                return Ok(crate::OperationReceipt {
                    value: TransactionCommit::Conflict {
                        actual: actual.value,
                    },
                    work: work
                        .checked_add(actual.work)
                        .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?,
                });
            }
            CheckoutCommitOutcome::Fenced { .. } => TransactionCommit::Fenced,
            CheckoutCommitOutcome::IdempotencyConflict { .. } => {
                TransactionCommit::IdempotencyConflict
            }
        };
        Ok(crate::OperationReceipt { value, work })
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
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.transaction.rebase",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty
        );
        crate::obs::outcome_on(
            &span,
            crate::obs::in_span(&span, async move {
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
            })
            .await,
        )
    }

    async fn apply(&mut self, operations: Vec<AuthoredMutation>) -> Result<(), WorkspaceError> {
        self.apply_measured(
            operations,
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .map(|_| ())
        .map_err(|failure| failure.error)
    }

    async fn apply_measured(
        &mut self,
        operations: Vec<AuthoredMutation>,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::WorkCounters, WorkspaceError> {
        self.checkout
            .apply_authored_transaction(operations, budget, cancellation)
            .await
            .map(|receipt| receipt.work)
            .map_err(|failure| failure.map_with_prior_work(crate::WorkCounters::default(), WorkspaceError::from))
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
    pub(crate) publication_closure: Option<crate::facade::PublicationClosure<A, O>>,
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
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut crate::WorkCounters,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<Vec<FileId>, WorkspaceError> {
    let mut ids: Vec<FileId> = Vec::new();
    for file_id in file_ids {
        cancellation.check()?;
        work.charge_items(1, &budget)?;
        let mut low = 0;
        let mut high = ids.len();
        let mut present = false;
        while low < high {
            cancellation.check()?;
            work.charge_items(1, &budget)?;
            let middle = low + (high - low) / 2;
            match ids[middle].cmp(&file_id) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => { present = true; break; }
            }
        }
        if present { continue; }
        grow_workspace_vec(&mut ids, allocations, work, budget, cancellation)?;
        work.charge(&crate::WorkCounters {
            bytes_copied: ((ids.len() - low + 1) * std::mem::size_of::<FileId>()) as u64,
            ..crate::WorkCounters::default()
        }, &budget)?;
        ids.insert(low, file_id);
    }
    Ok(ids)
}

fn changed_path_position<'a>(
    paths: &[ChangedPath],
    components: impl Iterator<Item = &'a LogicalName> + Clone,
    work: &mut crate::WorkCounters,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<(usize, bool), WorkspaceError> {
    let mut low = 0;
    let mut high = paths.len();
    while low < high {
        cancellation.check()?;
        work.charge_items(1, &budget)?;
        let middle = low + (high - low) / 2;
        let mut left = paths[middle].path.components().iter();
        let mut right = components.clone();
        let ordering = loop {
            cancellation.check()?;
            work.charge_items(1, &budget)?;
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

#[allow(clippy::too_many_arguments, reason = "one record merge shares the canonical planner's work and live allocation scope")]
fn merge_changed_record(
    paths: &mut Vec<ChangedPath>,
    path: &NamespacePath,
    record: Option<crate::kernel::FileRecord>,
    before: bool,
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut crate::WorkCounters,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<(), WorkspaceError> {
    let (index, present) = changed_path_position(
        paths, path.components().iter(), work, budget, cancellation,
    )?;
    if present {
        work.charge(&crate::WorkCounters {
            bytes_copied: std::mem::size_of::<Option<crate::kernel::FileRecord>>() as u64,
            ..crate::WorkCounters::default()
        }, &budget)?;
        if before { paths[index].before = record; } else { paths[index].after = record; }
        return Ok(());
    }
    grow_workspace_vec(paths, allocations, work, budget, cancellation)?;
    let path = path.clone_with_ledger(allocations, work, budget, cancellation)
        .map_err(namespace_path_error)?;
    work.charge(&crate::WorkCounters {
        bytes_copied: ((paths.len() - index + 1) * std::mem::size_of::<ChangedPath>()) as u64,
        ..crate::WorkCounters::default()
    }, &budget)?;
    paths.insert(index, ChangedPath {
        path, before: if before { record } else { None },
        after: if before { None } else { record },
    });
    Ok(())
}

#[allow(clippy::too_many_arguments, reason = "one bounded endpoint merge shares the canonical path planner and allocation scope")]
fn merge_binding_endpoint(
    paths: &mut Vec<ChangedPath>,
    directories: &NamespaceRecords,
    change: &crate::DirectoryBindingChange,
    limits: crate::model::VolumeLimits,
    before: bool,
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut crate::WorkCounters,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<(), WorkspaceError> {
    let Some(parents) = directories.get_measured(
        &change.directory_id, work, budget, cancellation,
    )? else { return Ok(()); };
    let binding = if before { change.before.as_ref() } else { change.after.as_ref() };
    let record = match binding {
        Some(entry) => directories.get_measured(
            &entry.file_id, work, budget, cancellation,
        )?.and_then(|records| records.first().map(|(_, record)| *record)),
        None => None,
    };
    for (directory, _) in parents {
        cancellation.check()?;
        work.charge_items(1, &budget)?;
        let components = directory.components().iter().chain(std::iter::once(&change.name));
        let (index, present) = changed_path_position(
            paths, components, work, budget, cancellation,
        )?;
        if present {
            work.charge(&crate::WorkCounters {
                bytes_copied: std::mem::size_of::<Option<crate::kernel::FileRecord>>() as u64,
                ..crate::WorkCounters::default()
            }, &budget)?;
            if before { paths[index].before = record; } else { paths[index].after = record; }
        } else {
            grow_workspace_vec(paths, allocations, work, budget, cancellation)?;
            let path = directory.child_with_ledger(
                &change.name, limits, allocations, work, budget, cancellation,
            ).map_err(namespace_path_error)?;
            work.charge(&crate::WorkCounters {
                bytes_copied: ((paths.len() - index + 1) * std::mem::size_of::<ChangedPath>()) as u64,
                ..crate::WorkCounters::default()
            }, &budget)?;
            paths.insert(index, ChangedPath {
                path, before: if before { record } else { None },
                after: if before { None } else { record },
            });
        }
    }
    Ok(())
}

struct NamespaceRecordSearch {
    records: NamespaceRecords,
    complete: bool,
}

#[derive(Default)]
struct NamespaceRecords {
    entries: Vec<(FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>)>,
}

impl NamespaceRecords {
    fn get_measured(
        &self,
        file_id: &FileId,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<Option<&Vec<(NamespacePath, crate::kernel::FileRecord)>>, WorkspaceError> {
        let mut low = 0;
        let mut high = self.entries.len();
        while low < high {
            cancellation.check()?;
            work.charge_items(1, &budget)?;
            let middle = low + (high - low) / 2;
            match self.entries[middle].0.cmp(file_id) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return Ok(Some(&self.entries[middle].1)),
            }
        }
        Ok(None)
    }

    fn iter(&self) -> impl Iterator<Item = (&FileId, &Vec<(NamespacePath, crate::kernel::FileRecord)>)> {
        self.entries.iter().map(|(file_id, records)| (file_id, records))
    }

    fn values(&self) -> impl Iterator<Item = &Vec<(NamespacePath, crate::kernel::FileRecord)>> {
        self.entries.iter().map(|(_, records)| records)
    }

    fn retained_bytes(&self) -> u64 {
        workspace_vec_bytes(&self.entries) + self.entries.iter().map(|(_, records)|
            workspace_vec_bytes(records) + records.iter().map(|(path, _)| path.retained_bytes()).sum::<u64>()
        ).sum::<u64>()
    }

    fn insert_measured(
        &mut self,
        path: NamespacePath,
        record: crate::kernel::FileRecord,
        allocations: &mut crate::kernel::allocation::AllocationLedger,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let mut index = 0;
        while let Some((file_id, _)) = self.entries.get(index) {
            cancellation.check()?;
            work.charge_items(1, &budget)?;
            if *file_id >= record.file_id { break; }
            index += 1;
        }
        if self.entries.get(index).is_none_or(|(id, _)| *id != record.file_id) {
            grow_workspace_vec(&mut self.entries, allocations, work, budget, cancellation)?;
            let copied = (self.entries.len() - index) * std::mem::size_of::<(FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>)>();
            work.charge(&crate::WorkCounters { bytes_copied: copied as u64,
                ..crate::WorkCounters::default() }, &budget)?;
            self.entries.insert(index, (record.file_id, Vec::new()));
        }
        let records = self.entries.get_mut(index).map(|(_, records)| records)
            .ok_or(WorkspaceError::IncompatibleWorkspace)?;
        grow_workspace_vec(records, allocations, work, budget, cancellation)?;
        records.push((path, record));
        Ok(())
    }

    fn sort_measured(
        &mut self,
        work: &mut crate::WorkCounters,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> Result<(), WorkspaceError> {
        for (_, records) in &mut self.entries {
            for index in 1..records.len() {
                let mut current = index;
                while current != 0 {
                    cancellation.check()?;
                    work.charge_items(1, &budget)?;
                    let right = records.get(current).ok_or(WorkspaceError::IncompatibleWorkspace)?;
                    let left = records.get(current - 1).ok_or(WorkspaceError::IncompatibleWorkspace)?;
                    if left.0 <= right.0 { break; }
                    work.charge(&crate::WorkCounters {
                        bytes_copied: (2 * std::mem::size_of::<(NamespacePath, crate::kernel::FileRecord)>()) as u64,
                        ..crate::WorkCounters::default()
                    }, &budget)?;
                    records.swap(current - 1, current);
                    current -= 1;
                }
            }
        }
        Ok(())
    }
}

impl IntoIterator for NamespaceRecords {
    type Item = (FileId, Vec<(NamespacePath, crate::kernel::FileRecord)>);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
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
        .map_err(|failure| failure.error)
    }

    /// Resolves changed paths with one cumulative work budget and explicit
    /// cancellation token. Partial traversal is never reported as exact.
    #[allow(
        clippy::too_many_lines,
        reason = "one bounded resolution of changed paths shares a single cumulative work budget"
    )]
    pub async fn changed_paths_bounded(
        &self,
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Vec<ChangedPath>>, WorkspaceError> {
        let mut work = self.work;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = async {
            if self.changes.truncated {
                return Err(WorkspaceError::ChangedPathLimit);
            }
            cancellation.check()?;
            work.verify(budget)?;
            if maximum_entries == 0
                && (!self.changes.files.is_empty() || !self.changes.bindings.is_empty())
            {
                return Err(WorkspaceError::ChangedPathLimit);
            }
            // Resolution continues the original diff span, including its
            // still-live buffers, rather than charging its receipt twice.
            allocations.claim_bytes(self.retained_bytes(), 0, &mut work, budget)
                .map_err(workspace_allocation_error)?;
            let file_ids = sorted_changed_file_ids(
                self.changes.files.iter().map(|change| change.file_id)
                    .chain(self.changes.bindings.iter().map(|change| change.directory_id))
                    .chain(self.changes.bindings.iter().flat_map(|change| {
                        change.before.iter().chain(change.after.iter()).map(|entry| entry.file_id)
                    })),
                &mut allocations, &mut work, budget, cancellation,
            )?;
            let live = allocations.live_bytes();
            let remaining = workspace_remaining(work, budget, live)?;
            let before_receipt = merge_workspace_backend(
                self.from.namespace_records_for_file_ids_bounded(
                    &file_ids, maximum_entries, remaining, cancellation,
                ).await,
                &mut work, live,
            )?;
            allocations.claim_bytes(before_receipt.value.records.retained_bytes(), 0, &mut work, budget)
                .map_err(workspace_allocation_error)?;
            let live = allocations.live_bytes();
            let remaining = workspace_remaining(work, budget, live)?;
            let after_receipt = merge_workspace_backend(
                self.to.namespace_records_for_file_ids_bounded(
                    &file_ids, maximum_entries, remaining, cancellation,
                ).await,
                &mut work, live,
            )?;
            allocations.claim_bytes(after_receipt.value.records.retained_bytes(), 0, &mut work, budget)
                .map_err(workspace_allocation_error)?;
            let before = before_receipt.value;
            let after = after_receipt.value;
            if !before.complete || !after.complete {
                return Err(WorkspaceError::ChangedPathLimit);
            }
            let before = before.records;
            let after = after.records;
            let mut paths = Vec::new();
            for records in before.values() {
                for (path, record) in records {
                    cancellation.check()?;
                    work.charge_items(1, &budget)?;
                    merge_changed_record(
                        &mut paths, path, Some(*record), true,
                        &mut allocations, &mut work, budget, cancellation,
                    )?;
                }
            }
            for records in after.values() {
                for (path, record) in records {
                    cancellation.check()?;
                    work.charge_items(1, &budget)?;
                    merge_changed_record(
                        &mut paths, path, Some(*record), false,
                        &mut allocations, &mut work, budget, cancellation,
                    )?;
                }
            }
            for change in &self.changes.bindings {
                cancellation.check()?;
                work.charge_items(1, &budget)?;
                merge_binding_endpoint(
                    &mut paths, &before, change, self.from.workspace.volume.config.limits, true,
                    &mut allocations, &mut work, budget, cancellation,
                )?;
                merge_binding_endpoint(
                    &mut paths, &after, change, self.to.workspace.volume.config.limits, false,
                    &mut allocations, &mut work, budget, cancellation,
                )?;
            }
            let retired = before.retained_bytes() + after.retained_bytes() + workspace_vec_bytes(&file_ids);
            drop(before);
            drop(after);
            drop(file_ids);
            allocations.release(retired).map_err(workspace_allocation_error)?;
            let mut retained = 0;
            let mut retired_paths = 0_u64;
            for index in 0..paths.len() {
                cancellation.check()?;
                work.charge_items(1, &budget)?;
                let changed = match (&paths[index].before, &paths[index].after) {
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
                        work.charge(&crate::WorkCounters {
                            bytes_copied: (2 * std::mem::size_of::<ChangedPath>()) as u64,
                            ..crate::WorkCounters::default()
                        }, &budget)?;
                        paths.swap(retained, index);
                    }
                    retained += 1;
                } else {
                    retired_paths = retired_paths.checked_add(paths[index].path.retained_bytes())
                        .ok_or(crate::WorkError::Overflow)?;
                }
            }
            paths.truncate(retained);
            allocations.release(retired_paths).map_err(workspace_allocation_error)?;
            work.verify(budget)?;
            Ok(crate::OperationReceipt { value: paths, work })
        }.await;
        result.map_err(|error| crate::OperationFailure::new(error, work))
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
        let source_head = self.source.head().await?;
        let (target_head_id, target_authority_head) = self
            .target
            .volume
            .fs
            .workspace_head_state(&self.target.volume)
            .await?;
        let target_head = self.target.generation(target_head_id).await?;
        self.plan_generations(source_head, target_head, target_authority_head)
            .await
    }

    /// Plans a join from exact immutable endpoints while requiring `target`
    /// to remain the target workspace's current published generation.
    ///
    /// # Errors
    ///
    /// Returns [`WorkspaceError::ForeignGeneration`] for an endpoint from the
    /// wrong workspace, or [`WorkspaceError::StaleTarget`] if the target has
    /// advanced. Lineage and semantic failures match [`Self::plan`].
    pub async fn plan_at(
        self,
        source: &Generation<A, O>,
        target: &Generation<A, O>,
    ) -> Result<JoinPlan<A, O>, WorkspaceError> {
        self.validate_bounds()?;
        if source.workspace.id != self.source.id || target.workspace.id != self.target.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let (target_head_id, target_authority_head) = self
            .target
            .volume
            .fs
            .workspace_head_state(&self.target.volume)
            .await?;
        if target.id != target_head_id {
            return Err(WorkspaceError::StaleTarget);
        }
        self.plan_generations(source.clone(), target.clone(), target_authority_head)
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
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.join.apply",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty
        );
        crate::obs::outcome_on(
            &span,
            crate::obs::in_span(&span, async move {
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
            })
            .await,
        )
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
    namespace_path_text_measured(
        path, &mut crate::kernel::allocation::AllocationLedger::default(),
        &mut crate::WorkCounters::default(), crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
}

pub(crate) fn namespace_path_text_measured(
    path: &NamespacePath,
    allocations: &mut crate::kernel::allocation::AllocationLedger,
    work: &mut crate::WorkCounters,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<String, WorkspaceError> {
    let initial_live = allocations.live_bytes();
    let result = (|| {
    cancellation.check()?;
    let mut length = usize::from(path.is_root());
    for name in path.components() {
        cancellation.check()?;
        work.charge_items(1, &budget)?;
        let bytes = name.unicode_utf8_bytes().ok_or(WorkspaceError::NamespacePath(
            crate::kernel::NamespacePathError::NonUnicodeProjection,
        ))?;
        length = length.checked_add(1).and_then(|value| value.checked_add(bytes))
            .ok_or(crate::WorkError::Overflow)?;
    }
    let delta = crate::WorkCounters { bytes_encoded: length as u64,
        output_bytes: length as u64, ..crate::WorkCounters::default() };
    work.admit(&delta, &budget)?;
    let claimed = allocations.claim_elements::<u8>(length, work, budget)
        .map_err(workspace_allocation_error)?;
    let mut output = String::new();
    if let Err(error) = output.try_reserve_exact(length) {
        allocations.release(claimed).map_err(workspace_allocation_error)?;
        return Err(WorkspaceError::engine(error));
    }
    if output.capacity() as u64 > claimed {
        allocations.claim_bytes(output.capacity() as u64 - claimed, 0, work, crate::WorkBudget::UNBOUNDED)
            .map_err(workspace_allocation_error)?;
        work.verify_ref(&budget)?;
    }
    if path.is_root() {
        output.push('/');
        work.charge(&crate::WorkCounters {
            bytes_encoded: 1, output_bytes: 1, ..crate::WorkCounters::default()
        }, &budget)?;
    }
    for name in path.components() {
        cancellation.check()?;
        let before = output.len();
        output.push('/');
        name.append_unicode(&mut output)
            .map_err(|error| namespace_path_error(crate::kernel::NamespacePathError::Name(error)))?;
        let encoded = (output.len() - before) as u64;
        work.charge(&crate::WorkCounters {
            bytes_encoded: encoded, output_bytes: encoded, ..crate::WorkCounters::default()
        }, &budget)?;
    }
    Ok(output)
    })();
    if result.is_err() {
        allocations.release(allocations.live_bytes() - initial_live)
            .map_err(workspace_allocation_error)?;
    }
    result
}

fn child_namespace_path(
    directory: &NamespacePath,
    name: LogicalName,
    limits: crate::model::VolumeLimits,
) -> Result<NamespacePath, WorkspaceError> {
    directory.child_measured(
        &name, limits, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(), 0,
    ).map(|receipt| receipt.value).map_err(|failure| namespace_path_error(failure.error))
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
    /// Verifies an initial fork against the same immutable selection builder used
    /// for creation. May stage content-addressed objects; publishes no authority.
    pub async fn matches_fork_selection(
        &self,
        source: &Self,
        paths: Option<Vec<String>>,
    ) -> Result<bool, WorkspaceError> {
        self.matches_fork_selection_measured(
            source, paths, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new(),
        ).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Uses the original finite selection builder and preserves every failed prefix.
    pub async fn matches_fork_selection_measured(
        &self, source: &Self, paths: Option<Vec<String>>,
        budget: crate::WorkBudget, cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<bool>, WorkspaceError> {
        self.workspace.volume.fs.verify_fork_selection_measured(
            source, self, paths, budget, cancellation,
        ).await
    }

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
        .map_err(|failure| failure.error)
    }

    /// Computes one semantic delta with explicit cumulative work and
    /// cancellation bounds.
    pub async fn diff_to_bounded(
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError>
    {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.generation.materialize",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty
        );
        crate::obs::measured_on(
            &span,
            crate::obs::in_span(
                &span,
                self.materialize_with_mode(
                    options,
                    budget,
                    cancellation,
                    crate::native_mount::MaterializeMode::DurableOutput,
                ),
            )
            .await,
            |receipt| &receipt.work,
        )
    }

    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub(crate) async fn materialize_with_mode(
        &self,
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        mode: crate::native_mount::MaterializeMode,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError>
    {
        let checkout = self
            .workspace
            .volume
            .checkout(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
                budget,
                cancellation,
            )
            .await
            .map_err(|failure| {
                failure.map_with_prior_work(crate::WorkCounters::default(), WorkspaceError::from)
            })?;
        let mut value = checkout.value;
        let receipt = crate::native_mount::materialize_checkout_with_mode(
            &mut value,
            options,
            checkout.work.remaining(budget).map_err(|error| {
                crate::OperationFailure::new(WorkspaceError::from(error), checkout.work)
            })?,
            cancellation,
            mode,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(checkout.work, WorkspaceError::from))?;
        let work = checkout.work.checked_add(receipt.work).map_err(|error| {
            crate::OperationFailure::new(WorkspaceError::from(error), checkout.work)
        })?;
        work.verify(budget)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
        Ok(crate::OperationReceipt {
            value: receipt.value,
            work,
        })
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError>
    {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.generation.materialize_path",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty
        );
        crate::obs::measured_on(
            &span,
            crate::obs::in_span(
                &span,
                self.materialize_path_with_mode(
                    path,
                    options,
                    budget,
                    cancellation,
                    crate::native_mount::MaterializeMode::DurableOutput,
                ),
            )
            .await,
            |receipt| &receipt.work,
        )
    }

    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub(crate) async fn materialize_path_with_mode(
        &self,
        path: &str,
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
        mode: crate::native_mount::MaterializeMode,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError>
    {
        let checkout = self
            .workspace
            .volume
            .checkout(
                GenerationSelector::Exact(self.id),
                CheckoutMode::read_only_pinned(),
                budget,
                cancellation,
            )
            .await
            .map_err(|failure| {
                failure.map_with_prior_work(crate::WorkCounters::default(), WorkspaceError::from)
            })?;
        let mut value = checkout.value;
        let mut prior = checkout.work;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        allocations.claim_bytes(value.logical_owned_bytes(), 0, &mut prior, budget)
            .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), prior))?;
        let path = customer_path_measured(path, value.volume_config(),
            &mut allocations, &mut prior, budget, cancellation)
            .map_err(|error| crate::OperationFailure::new(error, prior))?;
        let retained = allocations.live_bytes();
        let remaining = workspace_remaining(prior, budget, retained)
            .map_err(|error| crate::OperationFailure::new(error, prior))?;
        let receipt = crate::native_mount::materialize_checkout_paths_with_mode(
            &mut value,
            &[path],
            options,
            remaining,
            cancellation,
            mode,
        )
        .await
        .map_err(|failure| {
            match prior.with_backend(*failure.work, retained) {
                Ok(failed) => crate::OperationFailure::new(WorkspaceError::from(failure.error), failed),
                Err(error) => crate::OperationFailure::new(WorkspaceError::from(error), prior),
            }
        })?;
        let work = prior.with_backend(receipt.work, retained).map_err(|error| {
            crate::OperationFailure::new(WorkspaceError::from(error), prior)
        })?;
        work.verify(budget)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
        Ok(crate::OperationReceipt {
            value: receipt.value,
            work,
        })
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::MaterializationReceipt>, WorkspaceError>
    {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.generation.materialize_paths",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty,
            work.bytes = crate::obs::Empty,
            work.durability = crate::obs::Empty
        );
        crate::obs::measured_on(
            &span,
            crate::obs::in_span(&span, async move {
                let checkout = self
                    .workspace
                    .volume
                    .checkout(
                        GenerationSelector::Exact(self.id),
                        CheckoutMode::read_only_pinned(),
                        budget,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| {
                        failure.map_with_prior_work(
                            crate::WorkCounters::default(),
                            WorkspaceError::from,
                        )
                    })?;
                let mut value = checkout.value;
                let mut prior = checkout.work;
                let mut allocations = crate::kernel::allocation::AllocationLedger::default();
                allocations.claim_bytes(value.logical_owned_bytes(), 0, &mut prior, budget)
                    .map_err(|error| crate::OperationFailure::new(workspace_allocation_error(error), prior))?;
                cancellation.check().map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
                allocations.claim_elements::<NamespacePath>(paths.len(), &mut prior, budget)
                    .map_err(|error| crate::OperationFailure::new(crate::collection::handle_allocation_error(error).into(), prior))?;
                let mut parsed_paths = Vec::new();
                parsed_paths.try_reserve_exact(paths.len())
                    .map_err(|error| crate::OperationFailure::new(WorkspaceError::engine(error), prior))?;
                for path in paths {
                    parsed_paths.push(customer_path_measured(path, value.volume_config(),
                        &mut allocations, &mut prior, budget, cancellation)
                        .map_err(|error| crate::OperationFailure::new(error, prior))?);
                }
                let retained = allocations.live_bytes();
                let mut remaining = prior.remaining(budget)
                    .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), prior))?;
                remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.checked_sub(retained)
                    .ok_or_else(|| crate::OperationFailure::new(WorkspaceError::Work(crate::WorkError::Overflow), prior))?;
                let receipt = crate::materialize_checkout_paths(
                    &mut value,
                    &parsed_paths,
                    options,
                    remaining,
                    cancellation,
                )
                .await
                .map_err(|failure| {
                    match prior.with_backend(*failure.work, retained) {
                        Ok(failed) => crate::OperationFailure::new(WorkspaceError::from(failure.error), failed),
                        Err(error) => crate::OperationFailure::new(WorkspaceError::from(error), prior),
                    }
                })?;
                let work = prior.with_backend(receipt.work, retained).map_err(|error| {
                    crate::OperationFailure::new(WorkspaceError::from(error), prior)
                })?;
                work.verify(budget).map_err(|error| {
                    crate::OperationFailure::new(WorkspaceError::from(error), work)
                })?;
                Ok(crate::OperationReceipt {
                    value: receipt.value,
                    work,
                })
            })
            .await,
            |receipt| &receipt.work,
        )
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::HostPathRestore>, WorkspaceError> {
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
                .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), checkout.work))?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(checkout.work, WorkspaceError::engine))?;
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
        self.restore_host_paths_measured(paths, replacement, options, crate::WorkBudget::UNBOUNDED, cancellation)
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Restores a sequence through one exact checkout with cumulative failure work.
    #[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
    pub async fn restore_host_paths_measured(
        &self,
        paths: &[std::path::PathBuf],
        replacement: crate::HostPathReplacement,
        options: &crate::MaterializeOptions,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Vec<crate::HostPathRestore>>, WorkspaceError> {
        let checkout = self.workspace.engine_checkout_measured(
            GenerationSelector::Exact(self.id), CheckoutMode::read_only_pinned(), budget, cancellation,
        ).await?;
        let mut work = checkout.work;
        let mut checkout = checkout.value;
        let mut restored = Vec::new();
        restored.try_reserve_exact(paths.len())
            .map_err(|_| crate::OperationFailure::new(WorkspaceError::manual_engine("restore path result allocation failed"), work))?;
        for path in paths {
            let receipt = crate::restore_checkout_host_path(
                &mut checkout, path, replacement, options,
                work.remaining(budget).map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?,
                cancellation,
            ).await.map_err(|failure| failure.map_with_prior_work(work, WorkspaceError::engine))?;
            work = work.checked_add(receipt.work)
                .map_err(|error| crate::OperationFailure::new(WorkspaceError::from(error), work))?;
            restored.push(receipt.value);
        }
        Ok(crate::OperationReceipt { value: restored, work })
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

    /// Reads this exact immutable generation under complete operation bounds.
    pub async fn read_measured(
        &self,
        path: &str,
        maximum_bytes: u64,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Bytes>, WorkspaceError> {
        self.workspace.read_selected_measured(
            GenerationSelector::Exact(self.id), path, maximum_bytes, budget, cancellation,
        ).await
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

    /// Reads this immutable generation's exact range with all failed-work receipts.
    pub async fn read_range_measured(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Bytes>, WorkspaceError> {
        read_generation_range_measured(
            &self.workspace, GenerationSelector::Exact(self.id), path, offset, length,
            budget, cancellation,
        ).await
    }

    pub(crate) async fn read_range_owned_measured(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::kernel::OwnedSourceChunk>, WorkspaceError> {
        read_generation_range_owned_measured(
            &self.workspace, GenerationSelector::Exact(self.id), path, offset, length,
            budget, cancellation,
        ).await
    }

    /// Returns complete metadata for one path in this immutable generation.
    ///
    /// # Errors
    ///
    /// Rejects invalid or absent paths and authenticated backend failures.
    pub async fn stat(&self, path: &str) -> Result<WorkspaceStat, WorkspaceError> {
        stat_generation(&self.workspace, GenerationSelector::Exact(self.id), path).await
    }

    /// Resolves the canonical no-follow identity in this exact generation.
    pub async fn stat_measured(
        &self,
        path: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<WorkspaceStat>, WorkspaceError> {
        stat_generation_measured(&self.workspace, GenerationSelector::Exact(self.id), path, budget, cancellation).await
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
            let mut work = checkout.work;
            let mut checkout = checkout.value;
            let mut allocations = crate::kernel::allocation::AllocationLedger::default();
            allocations.claim_bytes(checkout.logical_owned_bytes(), 0, &mut work, budget)
                .map_err(|error| crate::OperationFailure::new(crate::collection::handle_allocation_error(error), work))?;
            let mut remaining = work.remaining(budget)
                .map_err(|error| crate::OperationFailure::new(error.into(), work))?;
            remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.checked_sub(allocations.live_bytes())
                .ok_or_else(|| crate::OperationFailure::new(crate::WorkError::Overflow.into(), work))?;
            let live = allocations.live_bytes();
            let lookup = checkout.lookup_batch_no_follow(paths, remaining, cancellation).await
                .map_err(|failure| match work.with_backend(*failure.work, live) {
                    Ok(failed) => crate::OperationFailure::new(failure.error, failed),
                    Err(error) => crate::OperationFailure::new(error.into(), work),
                })?;
            work = work.with_backend(lookup.work, live)
                .map_err(|error| crate::OperationFailure::new(error.into(), work))?;
            let entries = lookup.value.entries;
            allocations.claim_bytes(workspace_vec_bytes(&entries), 0, &mut work, budget)
                .map_err(|error| crate::OperationFailure::new(crate::collection::handle_allocation_error(error), work))?;
            cancellation.check().map_err(|error| crate::OperationFailure::new(error.into(), work))?;
            allocations.claim_elements::<Option<crate::kernel::FileRecord>>(entries.len(), &mut work, budget)
                .map_err(|error| crate::OperationFailure::new(crate::collection::handle_allocation_error(error), work))?;
            let mut records = Vec::new();
            records.try_reserve_exact(entries.len())
                .map_err(|_| crate::OperationFailure::new(crate::FsError::HandleAllocationFailed, work))?;
            for entry in entries {
                cancellation.check().map_err(|error| crate::OperationFailure::new(error.into(), work))?;
                work.charge_items(1, &budget)
                    .map_err(|error| crate::OperationFailure::new(error.into(), work))?;
                records.push(entry.record);
            }
            Ok(crate::FsReceipt { value: records, work })
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

    /// Lists this exact generation with authenticated bounded work.
    pub async fn list_directory_measured(
        &self,
        path: &str,
        after: Option<&LogicalName>,
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<WorkspaceDirectoryPage>, WorkspaceError> {
        list_generation_directory_measured(
            &self.workspace, GenerationSelector::Exact(self.id), path, after,
            maximum_entries, budget, cancellation,
        ).await
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<Vec<String>>, WorkspaceError> {
        let receipt = self.namespace_records_for_file_ids_bounded(
            &[file_id], maximum_entries, budget, cancellation,
        ).await?;
        let mut work = receipt.work;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = (|| {
            cancellation.check()?;
            allocations.claim_bytes(receipt.value.records.retained_bytes(), 0, &mut work, budget)
                .map_err(workspace_allocation_error)?;
            let mut paths = Vec::new();
            if let Some(records) = receipt.value.records.get(&file_id) {
                for (path, _) in records {
                    grow_workspace_vec(&mut paths, &mut allocations, &mut work, budget, cancellation)?;
                    paths.push(namespace_path_text_measured(
                        path, &mut allocations, &mut work, budget, cancellation,
                    )?);
                }
            }
            Ok(paths)
        })();
        result.map(|value| crate::OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))
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
        let cancellation = crate::CancellationToken::new();
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let mut work = crate::WorkCounters::default();
        let ids = sorted_changed_file_ids(
            file_ids, &mut allocations, &mut work, crate::WorkBudget::UNBOUNDED, &cancellation,
        )?;
        self.namespace_records_for_file_ids_bounded(
            &ids,
            maximum_entries,
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one bounded reverse lookup resolves every requested identity under a single work budget"
    )]
    async fn namespace_records_for_file_ids_bounded(
        &self,
        ids: &[FileId],
        maximum_entries: u32,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<NamespaceRecordSearch>, WorkspaceError> {
        let mut work = crate::WorkCounters::default();
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let result = async {
            cancellation.check()?;
            if ids.is_empty() || maximum_entries == 0 {
                return Ok(NamespaceRecordSearch { records: NamespaceRecords::default(), complete: ids.is_empty() });
            }
            let limits = self.workspace.volume.config.limits;
            let cache_key = crate::path_index::cache_key_measured(
                self.id, ids, &mut work, budget, cancellation,
            ).map_err(path_index_error)?;
            let before_cache = allocations.live_bytes();
            let cached_result = async {
                let Some(bytes) = self.workspace.volume.fs.path_index()
                    .load_measured(cache_key, &mut allocations, &mut work, budget, cancellation)
                    .map_err(path_index_error)? else { return Ok(None); };
                let cached = crate::path_index::GenerationPathQuery::decode_measured(
                    &bytes, self.id, ids, limits, &mut allocations, &mut work, budget, cancellation,
                ).map_err(path_index_error)?;
                let byte_capacity = workspace_vec_bytes(&bytes);
                drop(bytes);
                allocations.release(byte_capacity).map_err(workspace_allocation_error)?;
                let Some(cached) = cached else { return Ok(None); };
                if cached.len() > maximum_entries as usize { return Ok(None); }
                if cached.is_empty() { return Ok(Some(NamespaceRecords::default())); }
                let mut paths = Vec::new();
                for (_, path) in &cached {
                    grow_workspace_vec(&mut paths, &mut allocations, &mut work, budget, cancellation)?;
                    let copied = path.clone_with_ledger(
                        &mut allocations, &mut work, budget, cancellation,
                    ).map_err(namespace_path_error)?;
                    paths.push(copied);
                }
                let live = allocations.live_bytes();
                let remaining = workspace_remaining(work, budget, live)?;
                let lookup = merge_workspace_backend(
                    self.lookup_paths(&paths, remaining, cancellation).await, &mut work, live,
                )?;
                let path_capacity = workspace_vec_bytes(&paths)
                    + paths.iter().map(NamespacePath::retained_bytes).sum::<u64>();
                drop(paths);
                allocations.release(path_capacity).map_err(workspace_allocation_error)?;
                allocations.claim_bytes(workspace_vec_bytes(&lookup.value), 0, &mut work, budget)
                    .map_err(workspace_allocation_error)?;
                if cached.len() != lookup.value.len() { return Ok(None); }
                for ((expected, _), record) in cached.iter().zip(&lookup.value) {
                    cancellation.check()?;
                    work.charge_items(1, &budget)?;
                    if record.is_none_or(|record| record.file_id != *expected) {
                        return Ok(None);
                    }
                }
                let mut records = NamespaceRecords::default();
                let cached_capacity = workspace_vec_bytes(&cached);
                let lookup_capacity = workspace_vec_bytes(&lookup.value);
                for ((_, path), record) in cached.into_iter().zip(lookup.value) {
                    let record = record.ok_or(WorkspaceError::NotFound)?;
                    records.insert_measured(path, record, &mut allocations, &mut work, budget, cancellation)?;
                }
                allocations.release(cached_capacity).map_err(workspace_allocation_error)?;
                allocations.release(lookup_capacity).map_err(workspace_allocation_error)?;
                Ok::<_, WorkspaceError>(Some(records))
            }.await?;
            if let Some(records) = cached_result {
                return Ok(NamespaceRecordSearch { records, complete: true });
            }
            allocations.release(allocations.live_bytes() - before_cache).map_err(workspace_allocation_error)?;
            let live = allocations.live_bytes();
            let remaining = workspace_remaining(work, budget, live)?;
            let opened = merge_workspace_backend(
                self.workspace.engine_checkout_measured(
                    GenerationSelector::Exact(self.id), CheckoutMode::read_only_pinned(),
                    remaining, cancellation,
                ).await, &mut work, live,
            )?;
            let mut checkout = opened.value;
            allocations.claim_bytes(checkout.logical_owned_bytes(), 0, &mut work, budget)
                .map_err(workspace_allocation_error)?;
            let root = NamespacePath::new(Vec::new(), limits).map_err(namespace_path_error)?;
            let live = allocations.live_bytes();
            let remaining = workspace_remaining(work, budget, live)?;
            let root_lookup = merge_workspace_backend(
                checkout.lookup_no_follow(&root, remaining, cancellation).await, &mut work, live,
            )?;
            let mut matches = NamespaceRecords::default();
            if let Some(record) = root_lookup.value.record {
                let mut found = false;
                for file_id in ids {
                    cancellation.check()?;
                    work.charge_items(1, &budget)?;
                    if *file_id == record.file_id { found = true; break; }
                    if *file_id > record.file_id { break; }
                }
                if found {
                    matches.insert_measured(
                        NamespacePath::new(Vec::new(), limits).map_err(namespace_path_error)?,
                        record, &mut allocations, &mut work, budget, cancellation,
                    )?;
                }
            }
            let mut pending = Vec::new();
            grow_workspace_vec(&mut pending, &mut allocations, &mut work, budget, cancellation)?;
            pending.push(root);
            let mut examined = 0_u32;
            while let Some(directory) = pending.pop() {
                let directory_bytes = directory.retained_bytes();
                let mut after: Option<LogicalName> = None;
                loop {
                    let live = allocations.live_bytes();
                    let remaining = workspace_remaining(work, budget, live)?;
                    let page_receipt = merge_workspace_backend(
                        checkout.list_directory_records(
                            &directory, after.as_ref(),
                            maximum_entries.saturating_sub(examined).saturating_add(1).min(1_024),
                            remaining, cancellation,
                        ).await, &mut work, live,
                    )?;
                    let mut page = page_receipt.value;
                    let page_bytes = workspace_vec_bytes(&page.entries)
                        + page.entries.iter().map(|entry| entry.name.retained_bytes() as u64).sum::<u64>();
                    allocations.claim_bytes(page_bytes, 0, &mut work, budget)
                        .map_err(workspace_allocation_error)?;
                    for entry in &page.entries {
                        cancellation.check()?;
                        work.charge_items(1, &budget)?;
                        if examined == maximum_entries {
                            matches.sort_measured(&mut work, budget, cancellation)?;
                            return Ok(NamespaceRecordSearch { records: matches, complete: false });
                        }
                        examined += 1;
                        let mut found = false;
                        for file_id in ids {
                            work.charge_items(1, &budget)?;
                            if *file_id == entry.record.file_id { found = true; break; }
                            if *file_id > entry.record.file_id { break; }
                        }
                        let is_directory = entry.record.kind == FileKind::Directory;
                        if !found && !is_directory { continue; }
                        let path = directory.child_with_ledger(
                            &entry.name, limits, &mut allocations, &mut work, budget, cancellation,
                        ).map_err(namespace_path_error)?;
                        if found && is_directory {
                            let copied = path.clone_with_ledger(
                                &mut allocations, &mut work, budget, cancellation,
                            ).map_err(namespace_path_error)?;
                            matches.insert_measured(copied, entry.record,
                                &mut allocations, &mut work, budget, cancellation)?;
                        } else if found {
                            matches.insert_measured(path, entry.record,
                                &mut allocations, &mut work, budget, cancellation)?;
                            continue;
                        }
                        grow_workspace_vec(&mut pending, &mut allocations, &mut work, budget, cancellation)?;
                        pending.push(path);
                    }
                    let has_more = page.has_more;
                    let next = if has_more { page.entries.pop().map(|entry| entry.name) } else { None };
                    let next_bytes = next.as_ref().map_or(0, |name| name.retained_bytes() as u64);
                    drop(page);
                    allocations.release(page_bytes - next_bytes).map_err(workspace_allocation_error)?;
                    let old_bytes = after.as_ref().map_or(0, |name| name.retained_bytes() as u64);
                    drop(after);
                    allocations.release(old_bytes).map_err(workspace_allocation_error)?;
                    after = next;
                    if !has_more { break; }
                }
                let after_bytes = after.as_ref().map_or(0, |name| name.retained_bytes() as u64);
                drop(after);
                allocations.release(after_bytes).map_err(workspace_allocation_error)?;
                drop(directory);
                allocations.release(directory_bytes).map_err(workspace_allocation_error)?;
            }
            matches.sort_measured(&mut work, budget, cancellation)?;
            if let Some(bytes) = crate::path_index::encode_records_measured(
                self.id, ids, &matches.entries, &mut allocations, &mut work, budget, cancellation,
            ).map_err(path_index_error)? {
                self.workspace.volume.fs.path_index()
                    .store_measured(cache_key, &bytes, &mut allocations, &mut work, budget, cancellation)
                    .map_err(path_index_error)?;
            }
            Ok(NamespaceRecordSearch { records: matches, complete: true })
        }.await;
        result.map(|value| crate::OperationReceipt { value, work })
            .map_err(|error| crate::OperationFailure::new(error, work))
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

    /// Reads this exact generation's target under one measured work budget.
    ///
    /// # Errors
    ///
    /// Preserves actual authentication, lookup and target-read work on failure.
    pub async fn read_symbolic_link_measured(
        &self, path: &str, budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<Bytes>, WorkspaceError> {
        read_generation_symbolic_link_measured(
            &self.workspace, GenerationSelector::Exact(self.id), path, budget, cancellation,
        ).await
    }

    pub(crate) async fn read_symbolic_link_owned_measured(
        &self,
        path: &str,
        budget: crate::WorkBudget,
        cancellation: &crate::CancellationToken,
    ) -> crate::MeasuredResult<crate::OperationReceipt<crate::kernel::OwnedSourceChunk>, WorkspaceError> {
        read_generation_symbolic_link_owned_measured(
            &self.workspace, GenerationSelector::Exact(self.id), path, budget, cancellation,
        ).await
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
        self.pin_measured(identity, crate::WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
    }

    /// Actual original publication payload still retained by this generation's shared slot.
    /// Observes custody without consuming it or repinning the generation.
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
    ) -> crate::MeasuredResult<crate::OperationReceipt<GenerationPin<A, O>>, WorkspaceError> {
        let named = WorkspaceName::new_measured(identity, budget, cancellation)?;
        let identity = named.value;
        let mut work = named.work;
        let mut allocations = crate::kernel::allocation::AllocationLedger::default();
        let prepared = (|| {
            cancellation.check()?;
            allocations.claim_bytes(identity.retained_bytes(), 0, &mut work, budget)
                .map_err(workspace_allocation_error)?;
            let copied = crate::WorkCounters { bytes_copied: identity.retained_bytes(), ..crate::WorkCounters::UNCHARGED };
            work.admit(&copied, &budget)?;
            let claimed = allocations.claim_elements::<u8>(identity.as_str().len(), &mut work, budget)
                .map_err(workspace_allocation_error)?;
            let mut label = String::new();
            if let Err(error) = label.try_reserve_exact(identity.as_str().len()) {
                allocations.release(claimed).map_err(workspace_allocation_error)?;
                return Err(WorkspaceError::engine(error));
            }
            let actual = label.capacity() as u64;
            if actual > claimed {
                allocations.claim_bytes(actual - claimed, 0, &mut work, crate::WorkBudget::UNBOUNDED)
                    .map_err(workspace_allocation_error)?;
                work.verify(budget)?;
            }
            cancellation.check()?;
            work.charge(&copied, &budget)?;
            label.push_str(identity.as_str());
            // The retainer owns this String; the pin keeps only its canonical identity.
            allocations.release(actual).map_err(workspace_allocation_error)?;
            Ok(label)
        })();
        let label = prepared.map_err(|error| crate::OperationFailure::new(error, work))?;
        let live = allocations.live_bytes();
        let mut allowance = work;
        allowance.peak_allocation_bytes = 0;
        let mut allowance = allowance.remaining(budget)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::Work(error), work))?;
        allowance.peak_allocation_bytes = budget.peak_allocation_bytes.checked_sub(live)
            .ok_or_else(|| crate::OperationFailure::new(WorkspaceError::Work(crate::WorkError::Overflow), work))?;
        // Clones share one slot; consume before awaiting, releasing the
        // historical closure after this pin attempt.
        let publication = self.publication_closure.as_ref().and_then(|slot| {
            slot.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        });
        let retained = self.workspace
            .volume
            .fs
            .retain_workspace_generation_proven(
                &self.workspace.volume,
                self.id,
                crate::kernel::RetentionKind::Pin,
                label,
                publication,
                allowance,
                cancellation,
            )
            .await.map_err(|failure| {
                match work.with_backend(*failure.work, live) {
                    Ok(spent) => crate::OperationFailure::new(failure.error, spent),
                    Err(error) => crate::OperationFailure::new(WorkspaceError::Work(error), work),
                }
            })?;
        work = work.with_backend(retained.work, live)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::Work(error), work))?;
        work.verify(budget)
            .map_err(|error| crate::OperationFailure::new(WorkspaceError::Work(error), work))?;
        Ok(crate::OperationReceipt {
            value: GenerationPin { identity, generation: self.clone() },
            work,
        })
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

    /// Keeps the pinned parent lineage but starts with an empty namespace.
    /// The initial view is the child's merge baseline: excluded parent paths
    /// are never interpreted as child-authored deletions.
    #[must_use]
    pub fn empty(self) -> Self {
        self.inherit_paths(Vec::new())
    }

    /// Carries only explicitly selected files or directory subtrees. Selection
    /// is part of the initial fork, never an authored deletion after creation.
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
        // Inspect the canonical SDK failure wrapper without cloning/boxing it.
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
    /// Canonical parser failures retain their typed `NamespacePath` cause.
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
/// variants from `Error::source`; inspect the canonical typed fields directly.
pub(crate) fn workspace_bounded_error(
    error: &(dyn std::error::Error + 'static),
) -> Option<WorkspaceError> {
    if let Some(error) = error.downcast_ref::<crate::WorkError>() {
        return Some(WorkspaceError::Work(*error));
    }
    if error.is::<crate::CancellationError>() {
        return Some(WorkspaceError::Cancelled(crate::CancellationError));
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
    bounded!(crate::kernel::ExtentReadError; [Storage]; [Cancelled]);
    bounded!(crate::kernel::BlobBuildError; [Source, Storage]; [Cancelled]);
    bounded!(crate::kernel::BlobReadError; [Storage]; [Cancelled]);
    bounded!(crate::kernel::FileRangeReadError; [Extent, Blob]; [Cancelled]);
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
/// bookkeeping, not namespace examination; adoption performs no new allocation.
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

fn path_index_error(error: crate::path_index::PathIndexError) -> WorkspaceError {
    match error {
        crate::path_index::PathIndexError::Work(error) => WorkspaceError::Work(error),
        crate::path_index::PathIndexError::Cancelled => WorkspaceError::Cancelled(crate::CancellationError),
        error => WorkspaceError::engine(error),
    }
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
            *work = work.with_backend(receipt.work, live_bytes)?;
            Ok(receipt)
        }
        Err(failure) => {
            *work = work.with_backend(*failure.work, live_bytes)?;
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
/// Borrowed, already-retained checkout storage belongs to the containing
/// operation; freshly opened checkouts must first be adopted into this ledger.
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

pub(crate) fn namespace_path_error(error: crate::kernel::NamespacePathError) -> WorkspaceError {
    match error {
        crate::kernel::NamespacePathError::Work(error) => WorkspaceError::Work(error),
        crate::kernel::NamespacePathError::Cancelled => WorkspaceError::Cancelled(crate::CancellationError),
        error => WorkspaceError::NamespacePath(error),
    }
}

fn path_remaining(
    work: crate::WorkCounters,
    budget: crate::WorkBudget,
    path: &NamespacePath,
) -> Result<crate::WorkBudget, crate::WorkError> {
    let mut remaining = work.remaining(budget)?;
    remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
        .checked_sub(path.retained_bytes())
        .ok_or(crate::WorkError::BudgetExceeded { counter: "peak_allocation_bytes",
            observed: path.retained_bytes(), maximum: remaining.peak_allocation_bytes })?;
    Ok(remaining)
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

fn accumulate_workspace_failure<E>(
    failure: crate::OperationFailure<E>,
    work: &mut crate::WorkCounters,
    map: impl FnOnce(E) -> WorkspaceError,
) -> WorkspaceError {
    let failure = failure.map_with_prior_work(*work, map);
    *work = *failure.work;
    failure.error
}
