//! Atomic generation publication after bounded whole-closure authentication.

use super::{
    CanonicalDecodeError, ClosureError, ClosureLimits, GenerationProof, codec::Decoder,
    prove_generation_closure, prove_generation_closure_async, volume_authority_id,
};
use crate::cancellation::CancellationToken;
use crate::foundation::{AuthorityId, Digest, Epoch, Head, OperationId, ProposedCommit, VolumeId};
use crate::performance::{OperationFailure, WorkBudget, WorkCounters, WorkError};
use crate::storage::{AppendOutcome, AuthorityStoreError, ObjectId, ObjectKind, PublicationPermit};
use bytes::Bytes;
use thiserror::Error;

const PAYLOAD_DOMAIN: &[u8] = b"acyclic-fs-publish-generation-v1\0";
const FINGERPRINT_DOMAIN: &[u8] = b"acyclic-fs-publish-fingerprint-v1\0";
const CONTEXTUAL_FINGERPRINT_DOMAIN: &[u8] = b"acyclic-fs-publish-fingerprint-v2\0";

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PublicationIntent {
    pub(crate) operation_context: Option<Digest>,
    pub(crate) permit: PublicationPermit,
}

impl PublicationIntent {
    pub(crate) const fn unrestricted(operation_context: Option<Digest>) -> Self {
        Self {
            operation_context,
            permit: PublicationPermit::Unrestricted,
        }
    }

    pub(crate) const fn guarded(
        operation_context: Option<Digest>,
        permit: PublicationPermit,
    ) -> Self {
        Self {
            operation_context,
            permit,
        }
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

/// Decodes one canonical generation-publication authority payload.
///
/// The authority store deliberately treats payloads as opaque. This decoder is
/// the filesystem projection boundary used to reconstruct the current volume
/// generation without scanning immutable storage or confusing the authority
/// hash-chain digest with a generation identity.
///
/// # Errors
///
/// Rejects the wrong domain/version, truncation, trailing bytes, an unknown
/// object-kind tag, or any object kind other than `GenerationRoot`.
pub fn decode_published_generation(
    bytes: &[u8],
    maximum_payload_bytes: u64,
) -> Result<PublishedGeneration, CanonicalDecodeError> {
    let mut decoder = Decoder::new(bytes, PAYLOAD_DOMAIN, 1, maximum_payload_bytes)?;
    let volume_id = VolumeId::from_bytes(decoder.fixed()?);
    let kind = ObjectKind::from_canonical_tag(decoder.u8()?).map_err(|_| {
        CanonicalDecodeError::Invariant("unknown publication object kind".to_owned())
    })?;
    if kind != ObjectKind::GenerationRoot {
        return Err(CanonicalDecodeError::Invariant(
            "publication object is not a generation root".to_owned(),
        ));
    }
    let generation_root = ObjectId {
        kind,
        digest: Digest::from_bytes(decoder.fixed()?),
    };
    decoder.finish()?;
    Ok(PublishedGeneration {
        volume_id,
        generation_root,
    })
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

struct PreparedPublication {
    proof: GenerationProof,
    commit: ProposedCommit,
    work: WorkCounters,
}

/// Fail-closed generation-publication errors.
#[derive(Debug, Error)]
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

/// Authenticates an immutable generation's complete closure, then atomically
/// submits it to the volume authority.
///
/// No authority operation occurs unless closure and volume validation succeed.
/// Semantic stale-head, fenced-writer, and idempotency outcomes are returned as
/// values so callers can resolve retries without guessing whether publication
/// became durable.
///
/// # Errors
///
/// Fails with an exact work receipt on closure, volume, authority-backend, or
/// work-admission failure.
pub fn publish_generation<O: crate::ImmediateObjectStore, A: crate::ImmediateAuthorityStore>(
    objects: &O,
    authority: &A,
    request: PublishGenerationRequest,
    closure_limits: ClosureLimits,
    budget: WorkBudget,
) -> Result<PublicationReceipt, PublicationFailure> {
    validate_authority(request)?;
    let proof = prove_generation_closure(objects, request.generation_root, closure_limits, budget)
        .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
    let prepared = prepare_publication(proof, request, None, budget)?;
    let mut work = prepared.work;
    let remaining = work
        .remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let receipt = authority
        .compare_and_append(
            request.authority_id,
            request.epoch,
            request.expected,
            prepared.commit,
            remaining,
        )
        .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
    work = work
        .checked_add(receipt.work)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    Ok(PublicationReceipt {
        proof: prepared.proof,
        outcome: receipt.value,
        work,
    })
}

/// Asynchronously authenticates and atomically publishes one generation.
///
/// This is the browser/remote execution surface for the same canonical closure
/// proof, publication payload, fingerprint, and authority state machine used by
/// [`publish_generation`].
///
/// # Errors
///
/// Returns exact closure, cancellation, authority, and work failures without
/// exposing a generation before its complete immutable closure is proven.
pub async fn publish_generation_async<O: crate::AsyncObjectStore, A: crate::AsyncAuthorityStore>(
    objects: &O,
    authority: &A,
    request: PublishGenerationRequest,
    closure_limits: ClosureLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PublicationReceipt, PublicationFailure> {
    publish_generation_async_inner(
        objects,
        authority,
        request,
        PublicationIntent::unrestricted(None),
        closure_limits,
        budget,
        cancellation,
    )
    .await
}

pub(crate) async fn publish_generation_async_with_context<
    O: crate::AsyncObjectStore,
    A: crate::AsyncAuthorityStore,
>(
    objects: &O,
    authority: &A,
    request: PublishGenerationRequest,
    operation_context: Digest,
    closure_limits: ClosureLimits,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<PublicationReceipt, PublicationFailure> {
    publish_generation_async_inner(
        objects,
        authority,
        request,
        PublicationIntent::unrestricted(Some(operation_context)),
        closure_limits,
        budget,
        cancellation,
    )
    .await
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
    publish_generation_async_inner(
        objects,
        authority,
        request,
        intent,
        closure_limits,
        budget,
        cancellation,
    )
    .await
}

async fn publish_generation_async_inner<
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
    validate_authority(request)?;
    let proof = prove_generation_closure_async(
        objects,
        request.generation_root,
        closure_limits,
        budget,
        cancellation,
    )
    .await
    .map_err(|failure| OperationFailure::new(failure.error.into(), *failure.work))?;
    let operation_context = permit_context(intent.operation_context, intent.permit);
    let prepared = prepare_publication(proof, request, operation_context, budget)?;
    let mut work = prepared.work;
    // The authority CAS is the only point at which immutable bodies become
    // reachable after a crash. Drain any private staged bodies first; a failed
    // drain leaves no published generation and can be retried idempotently.
    let drained = objects
        .flush_before_publish(
            work.remaining(budget)
                .map_err(|error| OperationFailure::new(error.into(), work))?,
            cancellation,
        )
        .await
        .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
    work = work
        .checked_add(drained.work)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let remaining = work
        .remaining(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let receipt = crate::AsyncAuthorityStore::compare_and_append_guarded(
        authority,
        crate::GuardedAppend {
            authority_id: request.authority_id,
            epoch: request.epoch,
            expected: request.expected,
            commit: prepared.commit,
            permit: intent.permit,
        },
        remaining,
        cancellation,
    )
    .await
    .map_err(|failure| failure.map_with_prior_work(work, Into::into))?;
    work = work
        .checked_add(receipt.work)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    Ok(PublicationReceipt {
        proof: prepared.proof,
        outcome: receipt.value,
        work,
    })
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

fn prepare_publication(
    proof: GenerationProof,
    request: PublishGenerationRequest,
    operation_context: Option<Digest>,
    budget: WorkBudget,
) -> Result<PreparedPublication, PublicationFailure> {
    let mut work = proof.work;
    if proof.root.volume_id != request.volume_id {
        return Err(OperationFailure::new(
            PublicationError::VolumeMismatch {
                expected: request.volume_id,
                actual: proof.root.volume_id,
            },
            work,
        ));
    }
    let payload_bytes = u64::try_from(publication_payload_length()).unwrap_or(u64::MAX);
    let fingerprint_bytes =
        u64::try_from(fingerprint_input_length(operation_context)).unwrap_or(u64::MAX);
    let fingerprint_domain = fingerprint_domain(operation_context);
    let peak_allocation_bytes = payload_bytes
        .checked_add(fingerprint_bytes)
        .ok_or_else(|| OperationFailure::new(WorkError::Overflow.into(), work))?;
    let semantic_work = WorkCounters {
        bytes_encoded: payload_bytes
            .checked_add(fingerprint_bytes)
            .ok_or_else(|| OperationFailure::new(WorkError::Overflow.into(), work))?,
        bytes_hashed: u64::try_from(fingerprint_domain.len())
            .unwrap_or(u64::MAX)
            .checked_add(fingerprint_bytes)
            .ok_or_else(|| OperationFailure::new(WorkError::Overflow.into(), work))?,
        bytes_copied: payload_bytes,
        allocation_operations: 2,
        peak_allocation_bytes,
        ..WorkCounters::default()
    };
    work = work
        .checked_add(semantic_work)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    work.verify(budget)
        .map_err(|error| OperationFailure::new(error.into(), work))?;
    let payload = encode_publication_payload(request.volume_id, request.generation_root);
    let fingerprint = publication_fingerprint(request, operation_context, &payload);
    Ok(PreparedPublication {
        proof,
        commit: ProposedCommit {
            operation_id: request.operation_id,
            fingerprint,
            payload: Bytes::from(payload),
        },
        work,
    })
}

pub(crate) fn contextual_publication_fingerprint(
    request: PublishGenerationRequest,
    operation_context: Digest,
) -> Digest {
    let payload = encode_publication_payload(request.volume_id, request.generation_root);
    publication_fingerprint(request, Some(operation_context), &payload)
}

fn publication_fingerprint(
    request: PublishGenerationRequest,
    operation_context: Option<Digest>,
    payload: &[u8],
) -> Digest {
    let fingerprint_input = encode_fingerprint_input(&request, operation_context, payload);
    let mut hasher = blake3::Hasher::new();
    hasher.update(fingerprint_domain(operation_context));
    hasher.update(&fingerprint_input);
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

fn validate_authority(request: PublishGenerationRequest) -> Result<(), PublicationFailure> {
    let expected = volume_authority_id(request.volume_id);
    if request.authority_id == expected {
        Ok(())
    } else {
        Err(OperationFailure::before_work(
            PublicationError::AuthorityMismatch {
                volume_id: request.volume_id,
                expected,
                actual: request.authority_id,
            },
        ))
    }
}

const fn publication_payload_length() -> usize {
    PAYLOAD_DOMAIN.len() + 2 + 16 + 1 + 32
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
    bytes.extend_from_slice(PAYLOAD_DOMAIN);
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&volume_id.into_bytes());
    bytes.push(generation_root.kind.canonical_tag());
    bytes.extend_from_slice(generation_root.digest.as_bytes());
    bytes
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
    bytes.extend_from_slice(&request.authority_id.into_bytes());
    bytes.extend_from_slice(&request.epoch.get().to_le_bytes());
    bytes.extend_from_slice(&request.expected.epoch.get().to_le_bytes());
    bytes.extend_from_slice(&request.expected.sequence.get().to_le_bytes());
    bytes.extend_from_slice(request.expected.digest.as_bytes());
    if let Some(context) = operation_context {
        bytes.extend_from_slice(context.as_bytes());
    }
    bytes.extend_from_slice(payload);
    bytes
}

#[cfg(all(test, feature = "memory"))]
#[path = "tests/publication.rs"]
mod tests;
