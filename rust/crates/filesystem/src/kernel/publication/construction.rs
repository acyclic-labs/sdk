use super::*;
use crate::kernel::allocation::retained::Retained;
pub(super) fn prepare_publication_commit(
    request: PublishGenerationRequest,
    effective: Option<ObjectId>,
    operation_context: Option<Digest>,
    (original_evidence, custody): (Option<OriginalEvidence>, Option<bytes::Bytes>),
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ProposedCommit, crate::FsError> {
    let (payload, fingerprint) = construct_publication_commit(
        request,
        effective,
        operation_context,
        original_evidence,
        accounting,
    )?;
    let bytes = Retained::from_owned(payload, accounting)?;
    cancellation.check()?;
    accounting.check()?;
    let payload = bytes.into_bytes(custody, accounting)?;
    Ok(ProposedCommit {
        publication: None,
        operation_id: request.operation_id,
        fingerprint,
        payload,
    })
}

pub(super) fn construct_publication_commit(
    request: PublishGenerationRequest,
    effective: Option<ObjectId>,
    operation_context: Option<Digest>,
    original_evidence: Option<OriginalEvidence>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Owned<Vec<u8>>, Digest), crate::FsError> {
    accounting.check()?;
    let generation_root = effective.unwrap_or(request.generation_root);
    let origin = (generation_root != request.generation_root).then_some(PublicationOrigin {
        expected: request.expected,
        generation_root: request.generation_root,
    });
    let payload = construct_payload(
        PublishedGeneration {
            volume_id: request.volume_id,
            generation_root,
        },
        original_evidence,
        origin,
        accounting,
    )?;
    let fingerprint =
        publication_fingerprint(request, operation_context, original_evidence, accounting)?;
    Ok((payload, fingerprint))
}

fn publication_fingerprint(
    request: PublishGenerationRequest,
    operation_context: Option<Digest>,
    original_evidence: Option<OriginalEvidence>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Digest, crate::FsError> {
    let input_length = fingerprint_input_length(operation_context)
        .checked_add(request.generation_root.encoded_len() - 33)
        .and_then(|length| length.checked_add(if original_evidence.is_some() { 40 } else { 0 }))
        .ok_or(WorkError::Overflow)?;
    let domain = fingerprint_domain(operation_context);
    let input_bytes = usize_to_u64(input_length);
    let work = WorkCounters {
        bytes_encoded: input_bytes,
        bytes_hashed: usize_to_u64(domain.len())
            .checked_add(input_bytes)
            .ok_or(WorkError::Overflow)?,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(work)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain);
    hash_fingerprint_input(&mut hasher, &request, operation_context, original_evidence);
    let fingerprint = Digest::from_bytes(*hasher.finalize().as_bytes());
    accounting.record_completed(work)?;
    Ok(fingerprint)
}

fn construct_payload(
    publication: PublishedGeneration,
    original_evidence: Option<OriginalEvidence>,
    origin: Option<PublicationOrigin>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<Vec<u8>>, crate::FsError> {
    let payload_length = publication_payload_length_with_evidence(
        publication.generation_root,
        original_evidence,
        origin,
    )?;
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
        original_evidence,
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
    accounting.check()?;
    publication_fingerprint(request, Some(operation_context), None, accounting)
}
