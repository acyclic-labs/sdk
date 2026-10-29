use super::*;
use crate::kernel::allocation::retained::{Retained, RetainedAllocation};

struct PublicationPayload {
    bytes: Retained<Vec<u8>>,
    _original_evidence: Option<acyclic_stream::OwnedBytes>,
    _allocation: RetainedAllocation,
}
impl AsRef<[u8]> for PublicationPayload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes.value
    }
}
pub(super) fn prepare_publication_commit(
    request: PublishGenerationRequest,
    effective: Option<ObjectId>,
    operation_context: Option<Digest>,
    original_evidence: Option<acyclic_stream::OwnedBytes>,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ProposedCommit, crate::FsError> {
    let (mut payload, fingerprint) = construct_publication_commit(
        request,
        operation_context,
        original_evidence
            .as_ref()
            .map(|evidence| evidence.as_slice()),
        accounting,
    )?;
    if let Some(root) = effective.filter(|root| *root != request.generation_root) {
        payload.retire(accounting)?;
        payload = construct_payload(
            PublishedGeneration {
                volume_id: request.volume_id,
                generation_root: root,
            },
            original_evidence.as_ref().map(|data| data.as_slice()),
            Some(PublicationOrigin {
                expected: request.expected,
                generation_root: request.generation_root,
            }),
            accounting,
        )?;
    }
    let bytes = Retained::from_owned(payload, accounting)?;
    cancellation.check()?;
    accounting.check()?;
    let control_bytes =
        crate::kernel::allocation::bytes_owner_control_bytes::<PublicationPayload>()?;
    let allocation = RetainedAllocation::claim(control_bytes, 1, accounting)?;
    let payload = Bytes::from_owner(PublicationPayload {
        bytes,
        _original_evidence: original_evidence,
        _allocation: allocation,
    });
    Ok(ProposedCommit {
        publication: None,
        operation_id: request.operation_id,
        fingerprint,
        payload,
    })
}

pub(super) fn construct_publication_commit(
    request: PublishGenerationRequest,
    operation_context: Option<Digest>,
    original_evidence: Option<&[u8]>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Owned<Vec<u8>>, Digest), crate::FsError> {
    accounting.check()?;
    let payload_length =
        publication_payload_length_with_evidence(request.generation_root, original_evidence, None)?;
    let input_length = fingerprint_input_length(operation_context)
        .checked_sub(publication_payload_length())
        .and_then(|length| length.checked_add(payload_length))
        .ok_or(WorkError::Overflow)?;
    let payload = construct_payload(
        PublishedGeneration {
            volume_id: request.volume_id,
            generation_root: request.generation_root,
        },
        original_evidence,
        None,
        accounting,
    )?;
    let mut input = Owned::<Vec<u8>>::allocate(input_length, accounting)?;
    let encoding = WorkCounters {
        bytes_encoded: usize_to_u64(input_length),
        bytes_copied: usize_to_u64(input_length),
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(encoding)?;
    write_fingerprint_input(
        &mut input.value,
        &request,
        operation_context,
        &payload.value,
    );
    accounting.record_completed(encoding)?;
    let hashed = usize_to_u64(fingerprint_domain(operation_context).len())
        .checked_add(usize_to_u64(input.value.len()))
        .ok_or(WorkError::Overflow)?;
    let hashing = WorkCounters {
        bytes_hashed: hashed,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(hashing)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(fingerprint_domain(operation_context));
    hasher.update(&input.value);
    let fingerprint = Digest::from_bytes(*hasher.finalize().as_bytes());
    accounting.record_completed(hashing)?;
    input.retire(accounting)?;
    Ok((payload, fingerprint))
}

fn construct_payload(
    publication: PublishedGeneration,
    original_evidence: Option<&[u8]>,
    origin: Option<PublicationOrigin>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<Vec<u8>>, crate::FsError> {
    let payload_length = publication_payload_length_with_evidence(
        publication.generation_root,
        original_evidence,
        origin,
    )?;
    let evidence = if let Some(bytes) = original_evidence {
        let hashing = WorkCounters {
            bytes_hashed: usize_to_u64(EVIDENCE_DOMAIN.len())
                .checked_add(usize_to_u64(bytes.len()))
                .ok_or(WorkError::Overflow)?,
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(hashing)?;
        let digest = original_evidence_digest(bytes);
        accounting.record_completed(hashing)?;
        Some((bytes, digest))
    } else {
        None
    };
    let mut payload = Owned::<Vec<u8>>::allocate(payload_length, accounting)?;
    let encoded = usize_to_u64(payload_length);
    let encoding = WorkCounters {
        bytes_encoded: encoded,
        bytes_copied: encoded,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(encoding)?;
    write_publication_payload(
        &mut payload.value,
        publication.volume_id,
        publication.generation_root,
        evidence,
        origin,
    );
    accounting.record_completed(encoding)?;
    Ok(payload)
}

pub(crate) fn contextual_publication_fingerprint(
    request: PublishGenerationRequest,
    operation_context: Digest,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Digest, crate::FsError> {
    let (payload, fingerprint) =
        construct_publication_commit(request, Some(operation_context), None, accounting)?;
    payload.retire(accounting)?;
    Ok(fingerprint)
}
