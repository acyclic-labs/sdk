//! Atomic generation publication after bounded whole-closure authentication.

use super::{
    CanonicalDecodeError, ClosureError, ClosureLimits, GenerationProof, codec::Decoder,
    volume_authority_id,
};
use acyclic_stream::ProviderTask;
use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, Digest, Epoch, Head, OperationId, ProposedCommit, VolumeId, usize_to_u64,
};
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{AppendOutcome, AuthorityStoreError, GuardedAppend, ObjectId, ObjectKind, PublicationPermit};
use bytes::Bytes;
use thiserror::Error;

const PAYLOAD_DOMAIN: &[u8] = b"acyclic-fs-publish-generation-v1\0";
const FINGERPRINT_DOMAIN: &[u8] = b"acyclic-fs-publish-fingerprint-v1\0";
const CONTEXTUAL_FINGERPRINT_DOMAIN: &[u8] = b"acyclic-fs-publish-fingerprint-v2\0";
const EVIDENCE_DOMAIN: &[u8] = b"acyclic-fs-publication-original-evidence-v1\0";

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type PublicationRootPreparation<'a, E> =
    &'a mut (dyn FnMut(&super::GenerationRoot, WorkBudget) -> crate::MeasuredResult<crate::OperationReceipt<u64>, E> + Send);
#[cfg(target_arch = "wasm32")]
pub(crate) type PublicationRootPreparation<'a, E> =
    &'a mut dyn FnMut(&super::GenerationRoot, WorkBudget) -> crate::MeasuredResult<crate::OperationReceipt<u64>, E>;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type NativePublicationRootPreparation<'a, E> =
    &'a mut (dyn FnMut(&super::GenerationRoot, &mut crate::NativeAllocation, &crate::NativeMaterializationWork) -> Result<(), E> + Send);

enum PublicationProofGuard {
    Standalone(super::closure::ProofGuard),
    #[cfg(not(target_arch = "wasm32"))]
    Native(crate::NativeAllocation),
}

struct PublicationOutput {
    receipt: PublicationReceipt,
    guard: PublicationProofGuard,
}

struct PublicationAppend {
    outcome: AppendOutcome,
    #[cfg(not(target_arch = "wasm32"))]
    allocation: Option<crate::NativeAllocation>,
}

struct ProvenPublication {
    proof: GenerationProof,
    guard: PublicationProofGuard,
}

impl ProvenPublication {
    fn finish(self, outcome: AppendOutcome, work: WorkCounters) -> PublicationOutput {
        PublicationOutput {
            receipt: PublicationReceipt { proof: self.proof, outcome, work },
            guard: self.guard,
        }
    }
}

struct PublicationStepFailure<E> {
    cause: E,
    work: Option<WorkCounters>,
}

#[cfg(not(target_arch = "wasm32"))]
enum NativePublicationOwnerFailure<E> {
    Owner(E),
    Authority(AuthorityStoreError),
}

#[cfg(not(target_arch = "wasm32"))]
impl<E> From<AuthorityStoreError> for NativePublicationOwnerFailure<E> {
    fn from(error: AuthorityStoreError) -> Self { Self::Authority(error) }
}

enum PublicationAccounting<'a> {
    Standalone { work: WorkCounters, budget: WorkBudget, retained: u64, cancellation: &'a CancellationToken },
    #[cfg(not(target_arch = "wasm32"))]
    Native(&'a crate::NativeMaterializationWork),
}

impl<'a> PublicationAccounting<'a> {
    fn standalone(budget: WorkBudget, cancellation: &'a CancellationToken) -> Self {
        Self::Standalone { work: WorkCounters::UNCHARGED, budget, retained: 0, cancellation }
    }

    fn work(&self) -> WorkCounters {
        match self {
            Self::Standalone { work, .. } => *work,
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(original) => original.receipt(),
        }
    }

    fn remaining(&self) -> Result<WorkBudget, crate::FsError> {
        match self {
            Self::Standalone { work, budget, retained, cancellation } => {
                cancellation.check()?;
                let mut remaining = work.remaining(*budget)?;
                remaining.peak_allocation_bytes = remaining.peak_allocation_bytes.checked_sub(*retained)
                    .ok_or(WorkError::BudgetExceeded {
                        counter: "peak_allocation_bytes", observed: *retained,
                        maximum: remaining.peak_allocation_bytes,
                    })?;
                Ok(remaining)
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(_) => unreachable!("original callers pass the native meter, never a reconstructed budget"),
        }
    }

    fn accept(&mut self, more: WorkCounters) -> Result<(), crate::FsError> {
        match self {
            Self::Standalone { work, budget, retained, .. } => {
                *work = work.with_backend(more, *retained)?;
                work.verify(*budget)?;
                Ok(())
            }
            #[cfg(not(target_arch = "wasm32"))]
            Self::Native(_) => unreachable!("native producers charge the original ledger directly"),
        }
    }

    fn retain(&mut self, bytes: u64) -> Result<(), crate::FsError> {
        if let Self::Standalone { retained, .. } = self {
            *retained = retained.checked_add(bytes).ok_or(WorkError::Overflow)?;
            self.remaining()?;
        }
        Ok(())
    }

    fn accept_step<T, E: From<crate::FsError>>(
        &mut self, result: Result<(T, Option<WorkCounters>), PublicationStepFailure<E>>,
    ) -> Result<T, E> {
        match result {
            Ok((value, work)) => {
                if let Some(work) = work { self.accept(work).map_err(E::from)?; }
                Ok(value)
            }
            Err(failure) => {
                if let Some(work) = failure.work {
                    // Preserve the provider/owner's original cause, including
                    // a lost acknowledgement after actual durable effects.
                    let _ = self.accept(work);
                }
                Err(failure.cause)
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativePublicationPayload {
    pub(crate) bytes: Vec<u8>,
    pub(crate) _original_evidence: Option<acyclic_stream::OwnedBytes>,
    pub(crate) _allocation: crate::NativeAllocation,
    pub(crate) _control: crate::NativeAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl AsRef<[u8]> for NativePublicationPayload {
    fn as_ref(&self) -> &[u8] { &self.bytes }
}

struct PublicationEvidencePayload {
    bytes: Vec<u8>,
    _original_evidence: acyclic_stream::OwnedBytes,
    _control: super::allocation::KernelOwnedAllocation,
}

impl AsRef<[u8]> for PublicationEvidencePayload {
    fn as_ref(&self) -> &[u8] { &self.bytes }
}

/// Complete caller preconditions for publishing one immutable generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublishGenerationRequest {
    /// Storage authority that owns this volume's ordered head.
    pub authority_id: AuthorityId,
    /// Volume expected inside the authenticated generation root.
    pub volume_id: VolumeId,
    /// Active writer-fencing epoch.
    pub epoch: Epoch,
    /// Exact authority head observed by the caller.
    pub expected: Head,
    /// Stable retry identity.
    pub operation_id: OperationId,
    /// Immutable generation-root object to authenticate and publish.
    pub generation_root: ObjectId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PublicationIntent {
    pub(crate) operation_context: Option<Digest>,
    pub(crate) original_evidence: Option<acyclic_stream::OwnedBytes>,
    fingerprint_permit: Option<PublicationPermit>,
    pub(crate) permit: PublicationPermit,
}

impl PublicationIntent {
    pub(crate) const fn unrestricted(operation_context: Option<Digest>) -> Self {
        Self {
            operation_context,
            original_evidence: None,
            fingerprint_permit: None,
            permit: PublicationPermit::Unrestricted,
        }
    }

    pub(crate) const fn guarded(
        operation_context: Option<Digest>,
        permit: PublicationPermit,
    ) -> Self {
        Self {
            operation_context,
            original_evidence: None,
            fingerprint_permit: None,
            permit,
        }
    }

    pub(crate) fn with_original_evidence(mut self, evidence: Option<acyclic_stream::OwnedBytes>) -> Self {
        self.original_evidence = evidence;
        self
    }

    pub(crate) fn with_original_fingerprint_permit(mut self, original_permit: Option<PublicationPermit>) -> Self {
        self.fingerprint_permit = original_permit;
        self
    }
}

/// Canonical semantic fact stored in one generation-publication authority record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublishedGeneration {
    /// Volume whose ordered authority admitted the generation.
    pub volume_id: VolumeId,
    /// Authenticated immutable generation-root object.
    pub generation_root: ObjectId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BorrowedPublicationEvidence<'a> {
    pub(crate) publication: PublishedGeneration,
    pub(crate) original_evidence: Option<&'a [u8]>,
    pub(crate) evidence_digest: Option<Digest>,
}

/// Decodes one canonical publication with before-effect admission.
pub fn decode_published_generation<E: From<CanonicalDecodeError>>(
    bytes: &[u8],
    maximum_payload_bytes: u64,
    observe: impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<PublishedGeneration, E> {
    decode_publication_evidence(bytes, maximum_payload_bytes, observe).map(|decoded| decoded.publication)
}

pub(crate) fn decode_publication_evidence<'a, E: From<CanonicalDecodeError>>(
    bytes: &'a [u8],
    maximum_payload_bytes: u64,
    mut observe: impl FnMut(super::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<BorrowedPublicationEvidence<'a>, E> {
    let (mut decoder, version) = Decoder::new_recognized_versions(
        bytes, PAYLOAD_DOMAIN, &[1, 2], maximum_payload_bytes, &mut observe,
    )?;
    let volume_id = VolumeId::from_bytes(decoder.fixed::<16, E>(&mut observe)?);
    let kind = ObjectKind::from_canonical_tag(decoder.u8(&mut observe)?).map_err(|_| {
        E::from(CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static("unknown publication object kind")))
    })?;
    if kind != ObjectKind::GenerationRoot {
        return Err(E::from(CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static(
            "publication object is not a generation root",
        ))));
    }
    let generation_root = ObjectId {
        kind,
        digest: Digest::from_bytes(decoder.fixed::<32, E>(&mut observe)?),
    };
    let publication = PublishedGeneration { volume_id, generation_root };
    if version == 1 {
        decoder.finish().map_err(E::from)?;
        return Ok(BorrowedPublicationEvidence { publication, original_evidence: None, evidence_digest: None });
    }
    let length = decoder.u32(&mut observe)?;
    if length == 0 {
        return Err(E::from(CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static(
            "empty original publication evidence",
        ))));
    }
    let envelope_bytes = usize_to_u64(publication_payload_length()).checked_add(4 + 32)
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    let maximum = u32::try_from(maximum_payload_bytes.checked_sub(envelope_bytes).unwrap_or(0)).unwrap_or(u32::MAX);
    if length > maximum {
        return Err(E::from(CanonicalDecodeError::FieldTooLarge { observed: length, maximum }));
    }
    let evidence = decoder.take_exact(usize::try_from(length).map_err(|_| CanonicalDecodeError::LengthOverflow)?)?;
    let stored_digest = Digest::from_bytes(decoder.fixed::<32, E>(&mut observe)?);
    decoder.finish().map_err(E::from)?;
    let hashed = usize_to_u64(EVIDENCE_DOMAIN.len()).checked_add(u64::from(length))
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    observe(super::codec::ReadAllocationStep::BeforeHash(hashed))?;
    let actual_digest = original_evidence_digest(evidence);
    observe(super::codec::ReadAllocationStep::Hashed(hashed))?;
    if actual_digest != stored_digest {
        return Err(E::from(CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static(
            "original publication evidence digest mismatch",
        ))));
    }
    Ok(BorrowedPublicationEvidence { publication, original_evidence: Some(evidence), evidence_digest: Some(stored_digest) })
}

pub(crate) fn decode_publication_evidence_with_accounting<'a>(
    bytes: &'a [u8],
    maximum_payload_bytes: u64,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<BorrowedPublicationEvidence<'a>, crate::FsError> {
    super::file_read::decode_owned(accounting, |observe| decode_publication_evidence(bytes, maximum_payload_bytes, observe))
        .map(|(value, _guard)| value)
}

/// Successful or semantically rejected generation publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationReceipt {
    /// Whole-generation authentication evidence.
    pub proof: GenerationProof,
    /// Atomic authority result, including retry/conflict/fencing outcomes.
    pub outcome: AppendOutcome,
    /// Exact closure, encoding, hashing, and authority work.
    pub work: WorkCounters,
}

/// Publication failure retaining every unit of work spent before rejection.
pub type PublicationFailure = OperationFailure<PublicationError>;

/// Fail-closed generation-publication errors.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum PublicationError {
    /// The request targeted an authority other than the volume's canonical authority.
    #[error("authority {actual:?} does not own volume {volume_id:?}; expected {expected:?}")]
    AuthorityMismatch {
        /// Requested volume.
        volume_id: VolumeId,
        /// Sole authority derived from the volume identity.
        expected: AuthorityId,
        /// Authority supplied by the caller.
        actual: AuthorityId,
    },
    /// The authenticated generation belongs to another volume.
    #[error("generation belongs to volume {actual:?}, expected {expected:?}")]
    VolumeMismatch {
        /// Requested volume.
        expected: VolumeId,
        /// Volume authenticated by the generation root.
        actual: VolumeId,
    },
    /// Whole-generation authentication failed.
    #[error(transparent)]
    Closure(#[from] ClosureError),
    /// Staged immutable bodies could not be durably admitted before publication.
    #[error(transparent)]
    Objects(#[from] crate::storage::ObjectStoreError),
    /// Atomic authority evaluation failed.
    #[error(transparent)]
    Authority(#[from] AuthorityStoreError),
    /// Exact work overflowed or exceeded the admitted request budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

/// Authenticates an immutable generation's complete closure, then atomically submits it to the volume authority.
pub async fn publish_generation<O: crate::AsyncObjectStore, A: crate::AsyncAuthorityStore>(
    objects: &O,
    authority: &A,
    request: PublishGenerationRequest,
    closure_limits: ClosureLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PublicationReceipt, PublicationFailure> {
    publish_generation_async_with_permit(
        objects, authority, request, PublicationIntent::unrestricted(None),
        closure_limits, budget, cancellation,
    ).await
}

pub(crate) async fn publish_generation_async_with_permit<
    O: crate::AsyncObjectStore,
    A: crate::AsyncAuthorityStore,
>(
    objects: &O,
    authority: &A,
    request: PublishGenerationRequest,
    intent: PublicationIntent,
    closure_limits: ClosureLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PublicationReceipt, PublicationFailure> {
    publish_generation_async_guarded(
        objects, authority, request, intent, closure_limits, budget, cancellation,
        &mut |_| async { Ok::<_, OperationFailure<crate::FsError>>(crate::OperationReceipt {
            value: (), work: WorkCounters::UNCHARGED,
        }) }, None, None,
    ).await.map_err(|failure| OperationFailure::new(publication_error(failure.error), *failure.work))
}

pub(crate) async fn publish_generation_async_guarded<O, A, E, F, Fut>(
    objects: &O,
    authority: &A,
    request: PublishGenerationRequest,
    intent: PublicationIntent,
    closure_limits: ClosureLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
    before_publication: &mut F,
    resolved: Option<crate::foundation::DurableCommit>,
    mut prepare_roots: Option<PublicationRootPreparation<'_, E>>,
) -> Result<PublicationReceipt, OperationFailure<E>>
where
    O: crate::AsyncObjectStore,
    A: crate::AsyncAuthorityStore,
    E: From<crate::FsError> + Send,
    F: FnMut(WorkBudget) -> Fut + ProviderTask,
    Fut: Future<Output = crate::MeasuredResult<crate::OperationReceipt<()>, E>> + ProviderTask,
{
    let mut accounting = PublicationAccounting::standalone(budget, cancellation);
    let mut seal = |allowance: Option<WorkBudget>| {
        let future = before_publication(allowance.expect("the ordinary engine supplies its remaining budget"));
        async move {
            future.await.map(|receipt| Some(receipt.work)).map_err(|failure| PublicationStepFailure {
                cause: failure.error, work: Some(*failure.work),
            })
        }
    };
    let mut dispatch = |request: GuardedAppend, allowance: Option<WorkBudget>| {
        let future = authority.compare_and_append_guarded(
            request, allowance.expect("the ordinary engine supplies its remaining budget"), cancellation,
        );
        async move {
            future.await.map(|receipt| (
                PublicationAppend {
                    outcome: receipt.value,
                    #[cfg(not(target_arch = "wasm32"))]
                    allocation: None,
                },
                Some(receipt.work),
            )).map_err(|failure| PublicationStepFailure {
                cause: E::from(crate::FsError::Authority(failure.error)), work: Some(*failure.work),
            })
        }
    };
    let mut roots = |root: &super::GenerationRoot, _: &mut PublicationProofGuard, accounting: &mut PublicationAccounting<'_>| {
        if let Some(prepare) = prepare_roots.as_mut() {
            let result = prepare(root, accounting.remaining().map_err(E::from)?)
                .map(|receipt| (receipt.value, Some(receipt.work)))
                .map_err(|failure| PublicationStepFailure {
                    cause: failure.error, work: Some(*failure.work),
                });
            let bytes = accounting.accept_step(result)?;
            accounting.retain(bytes).map_err(E::from)?;
        }
        Ok(())
    };
    let result = publish_generation_async_inner(
        objects, request, intent, closure_limits, cancellation,
        &mut accounting, &mut seal, &mut dispatch, resolved, &mut roots,
    ).await;
    result.map(|output| output.receipt)
        .map_err(|error| OperationFailure::new(error, accounting.work()))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn publish_generation_async_guarded_with_native_work<'a, O, A, E, F, Fut>(
    objects: &O,
    authority: &A,
    request: PublishGenerationRequest,
    intent: PublicationIntent,
    closure_limits: ClosureLimits,
    original: &'a crate::NativeMaterializationWork,
    cancellation: &CancellationToken,
    before_publication: &mut F,
    resolved: Option<crate::foundation::DurableCommit>,
    mut prepare_roots: Option<NativePublicationRootPreparation<'_, E>>,
) -> Result<(PublicationReceipt, crate::NativeAllocation), E>
where
    O: crate::AsyncObjectStore,
    A: crate::AsyncAuthorityStore,
    E: From<crate::FsError> + Send + 'static,
    F: for<'m> FnMut(&'m crate::NativeMaterializationWork) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
{
    let mut accounting = PublicationAccounting::Native(original);
    let mut factory = before_publication.clone();
    let owner_original = original.clone();
    let owned_owner = move |same_original: &crate::NativeMaterializationWork| {
        let admitted = if owner_original.shares_ledger(same_original) {
            owner_original.admit(WorkCounters::UNCHARGED).map_err(crate::FsError::from).map_err(E::from)
        } else {
            Err(E::from(crate::FsError::NativeKernel(crate::NativeTreeMaterializationError::InvalidLayout)))
        };
        let future = admitted.map(|()| factory(same_original));
        async move {
            let future = future.map_err(NativePublicationOwnerFailure::Owner)?;
            future.await.map_err(NativePublicationOwnerFailure::Owner)
        }
    };
    let mut dispatch = move |request: GuardedAppend, _: Option<WorkBudget>| {
        let mut owner = owned_owner.clone();
        async move {
            let (outcome, allocation) = authority.compare_and_append_guarded_with_native_owner(
                request, original, cancellation, &mut owner,
            ).await.map_err(|error| PublicationStepFailure {
                cause: match error {
                    NativePublicationOwnerFailure::Owner(error) => error,
                    NativePublicationOwnerFailure::Authority(error) => E::from(native_authority_error(error)),
                },
                work: None,
            })?;
            Ok((PublicationAppend { outcome, allocation: Some(allocation) }, None))
        }
    };
    let mut seal = |_| {
        let future = before_publication(original);
        async move {
            future.await.map(|()| None).map_err(|cause| PublicationStepFailure { cause, work: None })
        }
    };
    let mut roots = |root: &super::GenerationRoot, guard: &mut PublicationProofGuard, _: &mut PublicationAccounting<'_>| {
        if let Some(prepare) = prepare_roots.as_mut() {
            match guard {
                PublicationProofGuard::Native(guard) => prepare(root, guard, original)?,
                PublicationProofGuard::Standalone(_) => unreachable!("native preparation owns a native proof"),
            }
        }
        Ok(())
    };
    let output = publish_generation_async_inner(
        objects, request, intent, closure_limits, cancellation,
        &mut accounting, &mut seal, &mut dispatch, resolved, &mut roots,
    ).await?;
    match output.guard {
        PublicationProofGuard::Native(guard) => Ok((output.receipt, guard)),
        PublicationProofGuard::Standalone(_) => unreachable!("original proof retains its original guard"),
    }
}

async fn publish_generation_async_inner<O, E, F, Fut, G, Dispatch, R>(
    objects: &O,
    request: PublishGenerationRequest,
    intent: PublicationIntent,
    closure_limits: ClosureLimits,
    cancellation: &CancellationToken,
    accounting: &mut PublicationAccounting<'_>,
    before_publication: &mut F,
    dispatch: &mut G,
    resolved: Option<crate::foundation::DurableCommit>,
    prepare_roots: &mut R,
) -> Result<PublicationOutput, E>
where
    O: crate::AsyncObjectStore,
    E: From<crate::FsError> + Send,
    F: FnMut(Option<WorkBudget>) -> Fut + ProviderTask,
    Fut: Future<Output = Result<Option<WorkCounters>, PublicationStepFailure<E>>> + ProviderTask,
    G: FnMut(GuardedAppend, Option<WorkBudget>) -> Dispatch + acyclic_stream::ProviderTask,
    Dispatch: Future<Output = Result<(PublicationAppend, Option<WorkCounters>), PublicationStepFailure<E>>> + acyclic_stream::ProviderTask,
    R: FnMut(&super::GenerationRoot, &mut PublicationProofGuard, &mut PublicationAccounting<'_>) -> Result<(), E>,
{
    let expected_authority = volume_authority_id(request.volume_id);
    if request.authority_id != expected_authority {
        return Err(E::from(crate::FsError::Publication(PublicationError::AuthorityMismatch {
            volume_id: request.volume_id, expected: expected_authority, actual: request.authority_id,
        })));
    }
    cancellation.check().map_err(crate::FsError::from).map_err(E::from)?;
    #[cfg(not(target_arch = "wasm32"))]
    if let PublicationAccounting::Native(original) = accounting {
        original.admit(WorkCounters::UNCHARGED).map_err(crate::FsError::from).map_err(E::from)?;
    }
    let proven_at = objects.collection_sweeps();
    let (proof, guard) = match accounting {
        PublicationAccounting::Standalone { work, budget, .. } => {
            let result = super::closure::prove_generation_closure_owned_async(
                objects, request.generation_root, closure_limits, *budget, cancellation,
            ).await;
            match result {
                Ok((proof, guard)) => {
                    *work = proof.work;
                    (proof, PublicationProofGuard::Standalone(guard))
                }
                Err(failure) => {
                    *work = *failure.work;
                    return Err(E::from(crate::FsError::Closure(failure.error)));
                }
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        PublicationAccounting::Native(original) => {
            let (proof, guard) = super::closure::prove_generation_closure_with_native_work(
                objects, request.generation_root, closure_limits, original, cancellation,
            ).await.map_err(E::from)?;
            (proof, PublicationProofGuard::Native(guard))
        }
    };
    // Field order keeps the concrete proof data physically ahead of its guard.
    let mut output = ProvenPublication { proof, guard };
    if let PublicationProofGuard::Standalone(guard) = &output.guard {
        accounting.retain(guard.retained_bytes()).map_err(E::from)?;
    }
    if output.proof.root.volume_id != request.volume_id {
        return Err(E::from(crate::FsError::Publication(PublicationError::VolumeMismatch {
            expected: request.volume_id, actual: output.proof.root.volume_id,
        })));
    }
    let fingerprint_permit = intent.fingerprint_permit.unwrap_or(intent.permit);
    let operation_context = match accounting {
        PublicationAccounting::Standalone { work, budget, .. } => {
            let mut allocations = super::allocation::AllocationLedger::default();
            let mut binding = super::allocation::KernelAccounting::borrowed_standalone(
                &mut allocations, work, *budget, cancellation,
            );
            permit_context_with_accounting(intent.operation_context, fingerprint_permit, &mut binding)
        }
        #[cfg(not(target_arch = "wasm32"))]
        PublicationAccounting::Native(original) => {
            let mut binding = super::allocation::KernelAccounting::native(original, cancellation);
            permit_context_with_accounting(intent.operation_context, fingerprint_permit, &mut binding)
        }
    }.map_err(E::from)?;
    let commit = prepare_publication_commit(
        request, operation_context, intent.original_evidence, accounting, cancellation,
    ).map_err(E::from)?;
    if let Some(resolved) = resolved.as_ref()
        && resolved.fingerprint != commit.fingerprint
    {
        return Ok(output.finish(AppendOutcome::IdempotencyConflict {
            committed_fingerprint: resolved.fingerprint,
        }, accounting.work()));
    }
    prepare_roots(&output.proof.root, &mut output.guard, accounting)?;
    if let Some(resolved) = resolved {
        return Ok(output.finish(AppendOutcome::AlreadyCommitted(resolved), accounting.work()));
    }
    let scope = crate::PublicationScope::Closure { objects: &output.proof.objects, proven_at };
    let _publication_hold = match accounting {
        PublicationAccounting::Standalone { .. } => {
            let result = objects.flush_before_publish(scope, accounting.remaining().map_err(E::from)?, cancellation).await
                .map(|receipt| (receipt.value, Some(receipt.work)))
                .map_err(|failure| PublicationStepFailure {
                    cause: E::from(crate::FsError::Object(failure.error)), work: Some(*failure.work),
                });
            accounting.accept_step(result)?
        }
        #[cfg(not(target_arch = "wasm32"))]
        PublicationAccounting::Native(original) => objects.flush_before_publish_with_native_work(
            scope, original, cancellation,
        ).await.map_err(native_object_error).map_err(E::from)?,
    };
    let append = crate::GuardedAppend {
        authority_id: request.authority_id, epoch: request.epoch, expected: request.expected,
        commit, permit: intent.permit,
    };
    // PR237's genuine provider custody hook is retained on this sole engine,
    // with the exact authenticated closure and durability hold still standing.
    match accounting {
        PublicationAccounting::Standalone { .. } => {
            let result = objects.prepare_generation_publication(
                &request, &output.proof, &append,
                crate::PublicationScope::Closure { objects: &output.proof.objects, proven_at },
                accounting.remaining().map_err(E::from)?, cancellation,
            ).await.map(|receipt| ((), Some(receipt.work))).map_err(|failure| PublicationStepFailure {
                cause: E::from(crate::FsError::Object(failure.error)), work: Some(*failure.work),
            });
            accounting.accept_step(result)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        PublicationAccounting::Native(original) => objects.prepare_generation_publication_with_native_work(
            &request, &output.proof, &append,
            crate::PublicationScope::Closure { objects: &output.proof.objects, proven_at },
            original, cancellation,
        ).await.map_err(native_object_error).map_err(E::from)?,
    }
    if matches!(accounting, PublicationAccounting::Standalone { .. }) {
        let checked = before_publication(Some(accounting.remaining().map_err(E::from)?)).await;
        accounting.accept_step(checked.map(|work| ((), work)))?;
    }
    cancellation.check().map_err(crate::FsError::from).map_err(E::from)?;
    let allowance = match accounting {
        PublicationAccounting::Standalone { .. } => Some(accounting.remaining().map_err(E::from)?),
        #[cfg(not(target_arch = "wasm32"))]
        PublicationAccounting::Native(_) => None,
    };
    let appended = accounting.accept_step(dispatch(append, allowance).await)?;
    #[cfg(not(target_arch = "wasm32"))]
    let mut appended = appended;
    #[cfg(not(target_arch = "wasm32"))]
    if let PublicationProofGuard::Native(proof_guard) = &mut output.guard {
        let allocation = appended.allocation.as_mut().ok_or_else(|| E::from(crate::FsError::NativeKernel(
            crate::NativeTreeMaterializationError::InvalidLayout,
        )))?;
        proof_guard.absorb(allocation).map_err(crate::FsError::from).map_err(E::from)?;
    }
    Ok(output.finish(appended.outcome, accounting.work()))
}

fn publication_error(error: crate::FsError) -> PublicationError {
    match error {
        crate::FsError::Publication(error) => error,
        crate::FsError::Closure(error) => error.into(),
        crate::FsError::Decode(error) => ClosureError::Decode(error).into(),
        crate::FsError::Object(error) => error.into(),
        crate::FsError::Authority(error) => error.into(),
        crate::FsError::Work(error) => error.into(),
        crate::FsError::Cancelled(_) => AuthorityStoreError::Cancelled.into(),
        _ => unreachable!("unguarded publication has no caller-owned error"),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_object_error(error: crate::storage::ObjectStoreError) -> crate::FsError {
    match error {
        crate::storage::ObjectStoreError::Native(error) => crate::FsError::NativeKernel(error),
        error => crate::FsError::Object(error),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_authority_error(error: AuthorityStoreError) -> crate::FsError {
    match error {
        AuthorityStoreError::NativeOriginal(error) => crate::FsError::NativeKernel(error),
        error => crate::FsError::Authority(error),
    }
}

pub(crate) fn permit_context_with_accounting(
    context: Option<Digest>,
    permit: PublicationPermit,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<Option<Digest>, crate::FsError> {
    accounting.check()?;
    let permit_hash_bytes = match permit {
        PublicationPermit::Unrestricted => 0,
        PublicationPermit::Lease { .. } =>
            usize_to_u64(b"acyclic-fs-publication-lease-v1\0".len()) + 1 + 16 + 16 + 16 + 8
                + if context.is_some() { 32 } else { 0 },
        PublicationPermit::Reservation { .. } =>
            usize_to_u64(b"acyclic-fs-publication-reservation-v1\0".len()) + 1 + 16 + 8 + 8 + 8 + 32
                + if context.is_some() { 32 } else { 0 },
    };
    let hashing = WorkCounters { bytes_hashed: permit_hash_bytes, ..WorkCounters::UNCHARGED };
    accounting.admit(hashing)?;
    let bound = permit_context(context, permit);
    accounting.record_completed(hashing)?;
    Ok(bound)
}

fn permit_context(context: Option<Digest>, permit: PublicationPermit) -> Option<Digest> {
    match permit {
        PublicationPermit::Unrestricted => context,
        PublicationPermit::Lease {
            authority_id,
            workspace_id,
            lease_id,
            expires_at_millis,
        } => {
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"acyclic-fs-publication-lease-v1\0");
            match context {
                Some(context) => {
                    hasher.update(&[1]);
                    hasher.update(context.as_bytes());
                }
                None => {
                    hasher.update(&[0]);
                }
            };
            hasher.update(&authority_id);
            hasher.update(&workspace_id);
            hasher.update(&lease_id);
            hasher.update(&expires_at_millis.to_be_bytes());
            Some(Digest::from_bytes(*hasher.finalize().as_bytes()))
        }
        PublicationPermit::Reservation {
            operation_id,
            gate_tail,
            expected,
        } => {
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"acyclic-fs-publication-reservation-v1\0");
            match context {
                Some(context) => {
                    hasher.update(&[1]);
                    hasher.update(context.as_bytes());
                }
                None => {
                    hasher.update(&[0]);
                }
            };
            hasher.update(&operation_id);
            hasher.update(&gate_tail.to_be_bytes());
            hasher.update(&expected.epoch.get().to_be_bytes());
            hasher.update(&expected.sequence.get().to_be_bytes());
            hasher.update(expected.digest.as_bytes());
            Some(Digest::from_bytes(*hasher.finalize().as_bytes()))
        }
    }
}

fn prepare_publication_commit(
    request: PublishGenerationRequest,
    operation_context: Option<Digest>,
    original_evidence: Option<acyclic_stream::OwnedBytes>,
    accounting: &mut PublicationAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ProposedCommit, crate::FsError> {
    match accounting {
        PublicationAccounting::Standalone { work, budget, retained, .. } => {
            let mut allocations = super::allocation::AllocationLedger::default();
            allocations.claim_bytes(*retained, 0, work, *budget)
                .map_err(publication_allocation_error)?;
            let mut construction = super::allocation::KernelAccounting::borrowed_standalone(
                &mut allocations, work, *budget, cancellation,
            );
            let (payload, fingerprint) = construct_publication_commit(
                request, operation_context, original_evidence.as_ref().map(|evidence| evidence.as_slice()), &mut construction,
            )?;
            let payload = if let Some(evidence) = original_evidence {
                let control_bytes = super::allocation::bytes_owner_control_bytes::<PublicationEvidencePayload>()?;
                let control = construction.claim_owned(control_bytes, 1)?;
                *retained = retained.checked_add(payload.bytes)
                    .and_then(|bytes| bytes.checked_add(control_bytes)).ok_or(WorkError::Overflow)?;
                Bytes::from_owner(PublicationEvidencePayload {
                    bytes: payload.value, _original_evidence: evidence, _control: control,
                })
            } else {
                *retained = retained.checked_add(payload.bytes).ok_or(WorkError::Overflow)?;
                Bytes::from(payload.value)
            };
            Ok(ProposedCommit { operation_id: request.operation_id, fingerprint, payload })
        }
        #[cfg(not(target_arch = "wasm32"))]
        PublicationAccounting::Native(original) => {
            let mut construction = super::allocation::KernelAccounting::native(original, cancellation);
            let built = construct_publication_commit(
                request, operation_context, original_evidence.as_ref().map(|evidence| evidence.as_slice()), &mut construction,
            )?;
            cancellation.check()?;
            original.admit(WorkCounters::UNCHARGED)?;
            let control = { let claim_bytes = super::allocation::bytes_owner_control_bytes::<NativePublicationPayload>()?; original.claim(claim_bytes, u64::from(claim_bytes != 0), crate::WorkCounters::UNCHARGED) }?;
            let (payload, fingerprint) = built;
            let super::persistent_point::Owned { value: payload, allocation, .. } = payload;
            let super::allocation::KernelOwnedAllocation::NativeOriginal(allocation) = allocation else {
                unreachable!("the original constructor preserves its native allocation owner");
            };
            let payload = Bytes::from_owner(NativePublicationPayload {
                bytes: payload, _original_evidence: original_evidence, _allocation: allocation, _control: control,
            });
            Ok(ProposedCommit { operation_id: request.operation_id, fingerprint, payload })
        }
    }
}

fn publication_allocation_error(error: super::allocation::AllocationError) -> crate::FsError {
    error.into_work_or(crate::FsError::Decode(CanonicalDecodeError::AllocationFailed))
}

fn construct_publication_commit(
    request: PublishGenerationRequest,
    operation_context: Option<Digest>,
    original_evidence: Option<&[u8]>,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<(super::persistent_point::Owned<Vec<u8>>, Digest), crate::FsError> {
    use super::persistent_point::Owned;
    accounting.check()?;
    let payload_length = publication_payload_length_with_evidence(original_evidence)?;
    let input_length = fingerprint_input_length(operation_context)
        .checked_sub(publication_payload_length()).and_then(|length| length.checked_add(payload_length))
        .ok_or(WorkError::Overflow)?;
    let evidence = if let Some(bytes) = original_evidence {
        let hashing = WorkCounters {
            bytes_hashed: usize_to_u64(EVIDENCE_DOMAIN.len()).checked_add(usize_to_u64(bytes.len())).ok_or(WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(hashing)?;
        let digest = original_evidence_digest(bytes);
        accounting.record_completed(hashing)?;
        Some((bytes, digest))
    } else { None };
    let mut payload = Owned::<Vec<u8>>::allocate(payload_length, accounting)?;
    let mut input = Owned::<Vec<u8>>::allocate(input_length, accounting)?;
    let encoded = usize_to_u64(payload_length).checked_add(usize_to_u64(input_length)).ok_or(WorkError::Overflow)?;
    let encoding = WorkCounters { bytes_encoded: encoded, bytes_copied: encoded, ..WorkCounters::UNCHARGED };
    accounting.admit(encoding)?;
    write_publication_payload_with_evidence(&mut payload.value, request.volume_id, request.generation_root, evidence);
    write_fingerprint_input(&mut input.value, &request, operation_context, &payload.value);
    accounting.record_completed(encoding)?;
    let hashed = usize_to_u64(fingerprint_domain(operation_context).len())
        .checked_add(usize_to_u64(input.value.len())).ok_or(WorkError::Overflow)?;
    let hashing = WorkCounters { bytes_hashed: hashed, ..WorkCounters::UNCHARGED };
    accounting.admit(hashing)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(fingerprint_domain(operation_context));
    hasher.update(&input.value);
    let fingerprint = Digest::from_bytes(*hasher.finalize().as_bytes());
    accounting.record_completed(hashing)?;
    input.retire(accounting)?;
    Ok((payload, fingerprint))
}

pub(crate) fn contextual_publication_fingerprint(
    request: PublishGenerationRequest,
    operation_context: Digest,
) -> Digest {
    let payload = encode_publication_payload(request.volume_id, request.generation_root);
    publication_fingerprint(request, Some(operation_context), &payload)
}

pub(crate) fn contextual_publication_fingerprint_with_accounting(
    request: PublishGenerationRequest,
    operation_context: Digest,
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<Digest, crate::FsError> {
    let (payload, fingerprint) = construct_publication_commit(
        request, Some(operation_context), None, accounting,
    )?;
    payload.retire(accounting)?;
    Ok(fingerprint)
}

pub(crate) fn contextual_publication_fingerprint_from_payload_with_accounting(
    request: PublishGenerationRequest,
    operation_context: Digest,
    payload: &[u8],
    accounting: &mut super::allocation::KernelAccounting<'_>,
) -> Result<Digest, crate::FsError> {
    accounting.check()?;
    let context = Some(operation_context);
    let input_bytes = usize_to_u64(fingerprint_input_length(context))
        .checked_sub(usize_to_u64(publication_payload_length()))
        .and_then(|bytes| bytes.checked_add(usize_to_u64(payload.len()))).ok_or(WorkError::Overflow)?;
    let hashing = WorkCounters {
        bytes_hashed: usize_to_u64(fingerprint_domain(context).len()).checked_add(input_bytes).ok_or(WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(hashing)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(fingerprint_domain(context));
    feed_fingerprint_input(&request, context, payload, |part| { hasher.update(part); });
    let fingerprint = Digest::from_bytes(*hasher.finalize().as_bytes());
    accounting.record_completed(hashing)?;
    accounting.check()?;
    Ok(fingerprint)
}

fn publication_fingerprint(
    request: PublishGenerationRequest,
    operation_context: Option<Digest>,
    payload: &[u8],
) -> Digest {
    let fingerprint_input = encode_fingerprint_input(&request, operation_context, payload);
    hash_publication_fingerprint(operation_context, &fingerprint_input)
}

fn hash_publication_fingerprint(operation_context: Option<Digest>, input: &[u8]) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(fingerprint_domain(operation_context));
    hasher.update(input);
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

const fn publication_payload_length() -> usize {
    PAYLOAD_DOMAIN.len() + 2 + 16 + 1 + 32
}

fn publication_payload_length_with_evidence(evidence: Option<&[u8]>) -> Result<usize, crate::FsError> {
    let Some(evidence) = evidence else { return Ok(publication_payload_length()) };
    if evidence.is_empty() {
        return Err(CanonicalDecodeError::Invariant(super::codec::CanonicalInvariant::Static("empty original publication evidence")).into());
    }
    u32::try_from(evidence.len()).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    publication_payload_length().checked_add(4).and_then(|length| length.checked_add(evidence.len()))
        .and_then(|length| length.checked_add(32)).ok_or_else(|| WorkError::Overflow.into())
}

fn original_evidence_digest(evidence: &[u8]) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(EVIDENCE_DOMAIN);
    hasher.update(evidence);
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

const fn fingerprint_input_length(operation_context: Option<Digest>) -> usize {
    16 + 8
        + 8
        + 8
        + 32
        + publication_payload_length()
        + if operation_context.is_some() { 32 } else { 0 }
}

pub(crate) fn encode_publication_payload(
    volume_id: VolumeId,
    generation_root: ObjectId,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(publication_payload_length());
    write_publication_payload(&mut bytes, volume_id, generation_root);
    bytes
}

fn write_publication_payload(bytes: &mut Vec<u8>, volume_id: VolumeId, generation_root: ObjectId) {
    write_publication_payload_with_evidence(bytes, volume_id, generation_root, None);
}

fn write_publication_payload_with_evidence(
    bytes: &mut Vec<u8>, volume_id: VolumeId, generation_root: ObjectId,
    evidence: Option<(&[u8], Digest)>,
) {
    bytes.extend_from_slice(PAYLOAD_DOMAIN);
    bytes.extend_from_slice(&(if evidence.is_some() { 2_u16 } else { 1_u16 }).to_le_bytes());
    bytes.extend_from_slice(&volume_id.into_bytes());
    bytes.push(generation_root.kind.canonical_tag());
    bytes.extend_from_slice(generation_root.digest.as_bytes());
    if let Some((evidence, digest)) = evidence {
        bytes.extend_from_slice(&(evidence.len() as u32).to_le_bytes());
        bytes.extend_from_slice(evidence);
        bytes.extend_from_slice(digest.as_bytes());
    }
}

fn fingerprint_domain(operation_context: Option<Digest>) -> &'static [u8] {
    if operation_context.is_some() {
        CONTEXTUAL_FINGERPRINT_DOMAIN
    } else {
        FINGERPRINT_DOMAIN
    }
}

fn encode_fingerprint_input(
    request: &PublishGenerationRequest,
    operation_context: Option<Digest>,
    payload: &[u8],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(fingerprint_input_length(operation_context));
    write_fingerprint_input(&mut bytes, request, operation_context, payload);
    bytes
}

fn write_fingerprint_input(
    bytes: &mut Vec<u8>, request: &PublishGenerationRequest,
    operation_context: Option<Digest>, payload: &[u8],
) {
    feed_fingerprint_input(request, operation_context, payload, |part| bytes.extend_from_slice(part));
}

fn feed_fingerprint_input(
    request: &PublishGenerationRequest,
    operation_context: Option<Digest>,
    payload: &[u8],
    mut feed: impl FnMut(&[u8]),
) {
    feed(&request.authority_id.into_bytes());
    feed(&request.epoch.get().to_le_bytes());
    feed(&request.expected.epoch.get().to_le_bytes());
    feed(&request.expected.sequence.get().to_le_bytes());
    feed(request.expected.digest.as_bytes());
    if let Some(context) = operation_context {
        feed(context.as_bytes());
    }
    feed(payload);
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/publication.rs"]
mod tests;
