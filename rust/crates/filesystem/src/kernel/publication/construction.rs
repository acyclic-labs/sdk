use super::*;

struct PublicationPayload {
    bytes: Vec<u8>,
    _original_evidence: Option<acyclic_stream::OwnedBytes>,
    _allocation: KernelOwnedAllocation,
}
impl AsRef<[u8]> for PublicationPayload {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}
pub(super) fn prepare_publication_commit(
    request: PublishGenerationRequest,
    operation_context: Option<Digest>,
    original_evidence: Option<acyclic_stream::OwnedBytes>,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<ProposedCommit, crate::FsError> {
    let (payload, fingerprint) = construct_publication_commit(
        request,
        operation_context,
        original_evidence
            .as_ref()
            .map(|evidence| evidence.as_slice()),
        accounting,
    )?;
    #[cfg(not(target_arch = "wasm32"))]
    let native = accounting.native_work().is_some();
    #[cfg(target_arch = "wasm32")]
    let native = false;
    let payload = if native || original_evidence.is_some() {
        cancellation.check()?;
        accounting.check()?;
        let control_bytes =
            crate::kernel::allocation::bytes_owner_control_bytes::<PublicationPayload>()?;
        let mut control = accounting.claim_owned(control_bytes, 1)?;
        let Owned {
            value: bytes,
            mut allocation,
            ..
        } = payload;
        allocation.absorb(&mut control)?;
        Bytes::from_owner(PublicationPayload {
            bytes,
            _original_evidence: original_evidence,
            _allocation: allocation,
        })
    } else {
        Bytes::from(payload.value)
    };
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
    let payload_length = publication_payload_length_with_evidence(original_evidence)?;
    let input_length = fingerprint_input_length(operation_context)
        .checked_sub(publication_payload_length())
        .and_then(|length| length.checked_add(payload_length))
        .ok_or(WorkError::Overflow)?;
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
    let mut input = Owned::<Vec<u8>>::allocate(input_length, accounting)?;
    let encoded = usize_to_u64(payload_length)
        .checked_add(usize_to_u64(input_length))
        .ok_or(WorkError::Overflow)?;
    let encoding = WorkCounters {
        bytes_encoded: encoded,
        bytes_copied: encoded,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(encoding)?;
    write_publication_payload(
        &mut payload.value,
        request.volume_id,
        request.generation_root,
        evidence,
    );
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
