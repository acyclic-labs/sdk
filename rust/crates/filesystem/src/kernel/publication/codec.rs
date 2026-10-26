use super::*;

pub(super) const PAYLOAD_DOMAIN: &[u8] = b"acyclic-fs-publish-generation-v1\0";
pub(super) const FINGERPRINT_DOMAIN: &[u8] = b"acyclic-fs-publish-fingerprint-v1\0";
pub(super) const CONTEXTUAL_FINGERPRINT_DOMAIN: &[u8] = b"acyclic-fs-publish-fingerprint-v2\0";
pub(super) const EVIDENCE_DOMAIN: &[u8] = b"acyclic-fs-publication-original-evidence-v1\0";

/// Original semantic request retained when an independent operation is composed
/// onto another member's durable prefix. Fingerprints bind this request; the
/// outer publication root is the effective, authenticated serial tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PublicationOrigin {
    pub(crate) expected: Head,
    pub(crate) generation_root: ObjectId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BorrowedPublicationEvidence<'a> {
    pub(crate) publication: PublishedGeneration,
    pub(crate) original_evidence: Option<&'a [u8]>,
    pub(crate) origin: Option<PublicationOrigin>,
}

/// Decodes one canonical publication with before-effect admission.
pub fn decode_published_generation<E: From<CanonicalDecodeError>>(
    bytes: &[u8],
    maximum_payload_bytes: u64,
    observe: impl FnMut(crate::kernel::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<PublishedGeneration, E> {
    decode_publication_evidence(bytes, maximum_payload_bytes, observe)
        .map(|decoded| decoded.publication)
}

pub(crate) fn decode_publication_evidence<'a, E: From<CanonicalDecodeError>>(
    bytes: &'a [u8],
    maximum_payload_bytes: u64,
    mut observe: impl FnMut(crate::kernel::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<BorrowedPublicationEvidence<'a>, E> {
    let (mut decoder, version) = Decoder::new_recognized_versions(
        bytes,
        PAYLOAD_DOMAIN,
        &[1, 2, 3],
        maximum_payload_bytes,
        &mut observe,
    )?;
    let volume_id = VolumeId::from_bytes(decoder.fixed::<16, E>(&mut observe)?);
    let kind = ObjectKind::from_canonical_tag(decoder.u8(&mut observe)?).map_err(|_| {
        E::from(CanonicalDecodeError::Invariant(
            crate::kernel::codec::CanonicalInvariant::Static("unknown publication object kind"),
        ))
    })?;
    if kind != ObjectKind::GenerationRoot {
        return Err(E::from(CanonicalDecodeError::Invariant(
            crate::kernel::codec::CanonicalInvariant::Static(
                "publication object is not a generation root",
            ),
        )));
    }
    let generation_root = ObjectId {
        kind,
        digest: Digest::from_bytes(decoder.fixed::<32, E>(&mut observe)?),
    };
    let publication = PublishedGeneration {
        volume_id,
        generation_root,
    };
    let origin = read_origin(version, &mut decoder, &mut observe)?;
    if version == 1 {
        decoder.finish().map_err(E::from)?;
        return Ok(BorrowedPublicationEvidence {
            publication,
            original_evidence: None,
            origin,
        });
    }
    let length = decoder.u32(&mut observe)?;
    if version == 3 && length == 0 {
        decoder.finish().map_err(E::from)?;
        return Ok(BorrowedPublicationEvidence {
            publication,
            original_evidence: None,
            origin,
        });
    }
    if length == 0 {
        return Err(E::from(CanonicalDecodeError::Invariant(
            crate::kernel::codec::CanonicalInvariant::Static("empty original publication evidence"),
        )));
    }
    let envelope_bytes = usize_to_u64(publication_payload_length())
        .checked_add(4 + 32 + if origin.is_some() { 80 } else { 0 })
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    let maximum =
        u32::try_from(maximum_payload_bytes.saturating_sub(envelope_bytes)).unwrap_or(u32::MAX);
    if length > maximum {
        return Err(E::from(CanonicalDecodeError::FieldTooLarge {
            observed: length,
            maximum,
        }));
    }
    let evidence = decoder
        .take_exact(usize::try_from(length).map_err(|_| CanonicalDecodeError::LengthOverflow)?)?;
    let stored_digest = Digest::from_bytes(decoder.fixed::<32, E>(&mut observe)?);
    decoder.finish().map_err(E::from)?;
    let hashed = usize_to_u64(EVIDENCE_DOMAIN.len())
        .checked_add(u64::from(length))
        .ok_or(CanonicalDecodeError::LengthOverflow)?;
    observe(crate::kernel::codec::ReadAllocationStep::BeforeHash(hashed))?;
    let actual_digest = original_evidence_digest(evidence);
    observe(crate::kernel::codec::ReadAllocationStep::Hashed(hashed))?;
    if actual_digest != stored_digest {
        return Err(E::from(CanonicalDecodeError::Invariant(
            crate::kernel::codec::CanonicalInvariant::Static(
                "original publication evidence digest mismatch",
            ),
        )));
    }
    Ok(BorrowedPublicationEvidence {
        publication,
        original_evidence: Some(evidence),
        origin,
    })
}

fn read_origin<E: From<CanonicalDecodeError>>(
    version: u16,
    decoder: &mut Decoder<'_>,
    observe: &mut impl FnMut(crate::kernel::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<Option<PublicationOrigin>, E> {
    if version != 3 {
        return Ok(None);
    }
    let epoch = Epoch::new(decoder.u64(observe)?).map_err(|_| {
        E::from(CanonicalDecodeError::Invariant(
            crate::kernel::codec::CanonicalInvariant::Static("zero original publication epoch"),
        ))
    })?;
    Ok(Some(PublicationOrigin {
        expected: Head {
            epoch,
            sequence: crate::Sequence::new(decoder.u64(observe)?),
            digest: Digest::from_bytes(decoder.fixed::<32, E>(observe)?),
        },
        generation_root: ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes(decoder.fixed::<32, E>(observe)?),
        },
    }))
}

pub(crate) fn decode_publication_evidence_with_accounting<'a>(
    bytes: &'a [u8],
    maximum_payload_bytes: u64,
    accounting: &mut KernelAccounting<'_>,
) -> Result<BorrowedPublicationEvidence<'a>, crate::FsError> {
    crate::kernel::file_read::decode_owned(accounting, |observe| {
        decode_publication_evidence(bytes, maximum_payload_bytes, observe)
    })
    .map(|(value, _guard)| value)
}

pub(super) fn permit_context(
    context: Option<Digest>,
    permit: PublicationPermit,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Option<Digest>, crate::FsError> {
    accounting.check()?;
    let permit_hash_bytes = match permit {
        PublicationPermit::Unrestricted => 0,
        PublicationPermit::Lease { .. } => {
            usize_to_u64(b"acyclic-fs-publication-lease-v1\0".len())
                + 1
                + 16
                + 16
                + 16
                + 8
                + if context.is_some() { 32 } else { 0 }
        }
        PublicationPermit::Reservation { .. } => {
            usize_to_u64(b"acyclic-fs-publication-reservation-v1\0".len())
                + 1
                + 16
                + 8
                + 8
                + 8
                + 32
                + if context.is_some() { 32 } else { 0 }
        }
    };
    let hashing = WorkCounters {
        bytes_hashed: permit_hash_bytes,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(hashing)?;
    let bound = match permit {
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
    };
    accounting.record_completed(hashing)?;
    Ok(bound)
}

pub(super) const fn publication_payload_length() -> usize {
    PAYLOAD_DOMAIN.len() + 2 + 16 + 1 + 32
}

pub(super) fn publication_payload_length_with_evidence(
    evidence: Option<&[u8]>,
    origin: Option<PublicationOrigin>,
) -> Result<usize, crate::FsError> {
    let base = publication_payload_length()
        .checked_add(if origin.is_some() { 80 } else { 0 })
        .ok_or(WorkError::Overflow)?;
    let Some(evidence) = evidence else {
        return base
            .checked_add(if origin.is_some() { 4 } else { 0 })
            .ok_or_else(|| WorkError::Overflow.into());
    };
    if evidence.is_empty() {
        return Err(CanonicalDecodeError::Invariant(
            crate::kernel::codec::CanonicalInvariant::Static("empty original publication evidence"),
        )
        .into());
    }
    u32::try_from(evidence.len()).map_err(|_| CanonicalDecodeError::LengthOverflow)?;
    base.checked_add(4)
        .and_then(|length| length.checked_add(evidence.len()))
        .and_then(|length| length.checked_add(32))
        .ok_or_else(|| WorkError::Overflow.into())
}

pub(super) fn original_evidence_digest(evidence: &[u8]) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(EVIDENCE_DOMAIN);
    hasher.update(evidence);
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

pub(super) const fn fingerprint_input_length(operation_context: Option<Digest>) -> usize {
    16 + 8
        + 8
        + 8
        + 32
        + publication_payload_length()
        + if operation_context.is_some() { 32 } else { 0 }
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "Payload admission checks the evidence length fits u32 before encoding."
)]
pub(super) fn write_publication_payload(
    bytes: &mut Vec<u8>,
    volume_id: VolumeId,
    generation_root: ObjectId,
    evidence: Option<(&[u8], Digest)>,
    origin: Option<PublicationOrigin>,
) {
    bytes.extend_from_slice(PAYLOAD_DOMAIN);
    bytes.extend_from_slice(
        &(if origin.is_some() {
            3_u16
        } else if evidence.is_some() {
            2_u16
        } else {
            1_u16
        })
        .to_le_bytes(),
    );
    bytes.extend_from_slice(&volume_id.into_bytes());
    bytes.push(generation_root.kind.canonical_tag());
    bytes.extend_from_slice(generation_root.digest.as_bytes());
    if let Some(origin) = origin {
        bytes.extend_from_slice(&origin.expected.epoch.get().to_le_bytes());
        bytes.extend_from_slice(&origin.expected.sequence.get().to_le_bytes());
        bytes.extend_from_slice(origin.expected.digest.as_bytes());
        bytes.extend_from_slice(origin.generation_root.digest.as_bytes());
    }
    if let Some((evidence, digest)) = evidence {
        bytes.extend_from_slice(&(evidence.len() as u32).to_le_bytes());
        bytes.extend_from_slice(evidence);
        bytes.extend_from_slice(digest.as_bytes());
    } else if origin.is_some() {
        bytes.extend_from_slice(&0_u32.to_le_bytes());
    }
}

pub(super) fn fingerprint_domain(operation_context: Option<Digest>) -> &'static [u8] {
    if operation_context.is_some() {
        CONTEXTUAL_FINGERPRINT_DOMAIN
    } else {
        FINGERPRINT_DOMAIN
    }
}

pub(super) fn write_fingerprint_input(
    bytes: &mut Vec<u8>,
    request: &PublishGenerationRequest,
    operation_context: Option<Digest>,
    payload: &[u8],
) {
    bytes.extend_from_slice(&request.authority_id.into_bytes());
    bytes.extend_from_slice(&request.epoch.get().to_le_bytes());
    bytes.extend_from_slice(&request.expected.epoch.get().to_le_bytes());
    bytes.extend_from_slice(&request.expected.sequence.get().to_le_bytes());
    bytes.extend_from_slice(request.expected.digest.as_bytes());
    if let Some(context) = operation_context {
        bytes.extend_from_slice(context.as_bytes());
    }
    bytes.extend_from_slice(payload);
}
