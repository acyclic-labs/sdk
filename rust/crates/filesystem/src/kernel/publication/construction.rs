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
    let (mut payload, fingerprint) =
        construct_publication_commit(request, operation_context, original_evidence, accounting)?;
    if let Some(root) = effective.filter(|root| *root != request.generation_root) {
        payload.retire(accounting)?;
        payload = construct_payload(
            PublishedGeneration {
                volume_id: request.volume_id,
                generation_root: root,
            },
            original_evidence,
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
    operation_context: Option<Digest>,
    original_evidence: Option<OriginalEvidence>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Owned<Vec<u8>>, Digest), crate::FsError> {
    accounting.check()?;
    let input_length = fingerprint_input_length(operation_context)
        .checked_add(request.generation_root.encoded_len() - 33)
        .and_then(|length| length.checked_add(if original_evidence.is_some() { 40 } else { 0 }))
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
        original_evidence,
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
    let (payload, fingerprint) =
        construct_publication_commit(request, Some(operation_context), None, accounting)?;
    payload.retire(accounting)?;
    Ok(fingerprint)
}
