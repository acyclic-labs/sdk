use super::*;

pub(super) const PAYLOAD_DOMAIN: &[u8] = b"acyclic-fs-publish-generation-v1\0";
pub(super) const PAYLOAD_VERSION: u16 = 5;
pub(super) const FINGERPRINT_DOMAIN: &[u8] = b"acyclic-fs-publish-fingerprint-v3\0";
pub(super) const CONTEXTUAL_FINGERPRINT_DOMAIN: &[u8] =
    b"acyclic-fs-publish-contextual-fingerprint-v3\0";
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
pub(crate) struct OriginalEvidence {
    pub(crate) root: ObjectId,
    pub(crate) bytes: u64,
    pub(crate) digest: Digest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PublicationEvidence {
    pub(crate) publication: PublishedGeneration,
    pub(crate) original_evidence: Option<OriginalEvidence>,
    pub(crate) origin: Option<PublicationOrigin>,
}

/// Decodes one canonical publication with before-effect admission.
pub fn decode_published_generation<E: From<CanonicalDecodeError>>(
    bytes: &[u8],
    maximum_payload_bytes: u64,
    observe: impl FnMut(crate::kernel::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<PublishedGeneration, E> {
    decode_publication_evidence(bytes, maximum_payload_bytes, None, observe)
        .map(|decoded| decoded.publication)
}

pub(crate) fn decode_publication_evidence<E: From<CanonicalDecodeError>>(
    bytes: &[u8],
    maximum_payload_bytes: u64,
    maximum_evidence_bytes: Option<u64>,
    mut observe: impl FnMut(crate::kernel::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<PublicationEvidence, E> {
    let mut decoder = Decoder::new(
        bytes,
        PAYLOAD_DOMAIN,
        PAYLOAD_VERSION,
        maximum_payload_bytes,
        &mut observe,
    )?;
    let volume_id = VolumeId::from_bytes(decoder.fixed::<16, E>(&mut observe)?);
    let generation_root = decoder.object(ObjectKind::GenerationRoot, &mut observe)?;
    let publication = PublishedGeneration {
        volume_id,
        generation_root,
    };
    let flags = decoder.u8(&mut observe)?;
    if flags & !3 != 0 {
        return Err(E::from(CanonicalDecodeError::Invariant(
            crate::kernel::codec::CanonicalInvariant::Static("invalid publication option flags"),
        )));
    }
    let origin = if flags & 1 != 0 {
        Some(read_origin(&mut decoder, &mut observe)?)
    } else {
        None
    };
    if flags & 2 == 0 {
        decoder.finish().map_err(E::from)?;
        return Ok(PublicationEvidence {
            publication,
            original_evidence: None,
            origin,
        });
    }
    let bytes = decoder.u64(&mut observe)?;
    if bytes == 0 {
        return Err(E::from(CanonicalDecodeError::Invariant(
            crate::kernel::codec::CanonicalInvariant::Static("empty original publication evidence"),
        )));
    }
    if let Some(maximum) = maximum_evidence_bytes
        && bytes > maximum
    {
        return Err(E::from(CanonicalDecodeError::ObjectTooLarge {
            observed: bytes,
            maximum,
        }));
    }
    let root = decoder.object(ObjectKind::Blob, &mut observe)?;
    let digest = Digest::from_bytes(decoder.fixed::<32, E>(&mut observe)?);
    decoder.finish().map_err(E::from)?;
    Ok(PublicationEvidence {
        publication,
        original_evidence: Some(OriginalEvidence {
            root,
            bytes,
            digest,
        }),
        origin,
    })
}

fn read_origin<E: From<CanonicalDecodeError>>(
    decoder: &mut Decoder<'_>,
    observe: &mut impl FnMut(crate::kernel::codec::ReadAllocationStep) -> Result<(), E>,
) -> Result<PublicationOrigin, E> {
    let epoch = Epoch::new(decoder.u64(observe)?).map_err(|_| {
        E::from(CanonicalDecodeError::Invariant(
            crate::kernel::codec::CanonicalInvariant::Static("zero original publication epoch"),
        ))
    })?;
    Ok(PublicationOrigin {
        expected: Head {
            epoch,
            sequence: crate::Sequence::new(decoder.u64(observe)?),
            digest: Digest::from_bytes(decoder.fixed::<32, E>(observe)?),
        },
        generation_root: decoder.object(ObjectKind::GenerationRoot, observe)?,
    })
}

pub(crate) fn decode_publication_evidence_with_accounting(
    bytes: &[u8],
    maximum_payload_bytes: u64,
    maximum_evidence_bytes: Option<u64>,
    accounting: &mut KernelAccounting<'_>,
) -> Result<PublicationEvidence, crate::FsError> {
    crate::kernel::file_read::decode_owned(accounting, |observe| {
        decode_publication_evidence(
            bytes,
            maximum_payload_bytes,
            maximum_evidence_bytes,
            observe,
        )
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
    PAYLOAD_DOMAIN.len() + 2 + 16 + 1 + 32 + 1
}

pub(super) fn publication_payload_length_with_evidence(
    generation_root: ObjectId,
    evidence: Option<OriginalEvidence>,
    origin: Option<PublicationOrigin>,
) -> Result<usize, crate::FsError> {
    let base = (publication_payload_length() + generation_root.encoded_len() - 33)
        .checked_add(origin.map_or(0, |origin| 48 + origin.generation_root.encoded_len()))
        .ok_or(WorkError::Overflow)?;
    let Some(evidence) = evidence else {
        return Ok(base);
    };
    if evidence.bytes == 0 || evidence.root.kind != ObjectKind::Blob {
        return Err(CanonicalDecodeError::Invariant(
            crate::kernel::codec::CanonicalInvariant::Static(
                "invalid original publication evidence reference",
            ),
        )
        .into());
    }
    base.checked_add(8 + evidence.root.encoded_len() + 32)
        .ok_or_else(|| WorkError::Overflow.into())
}

pub(super) fn original_evidence_digest(evidence: &[u8]) -> Digest {
    let mut hasher = blake3::Hasher::new();
    hasher.update(EVIDENCE_DOMAIN);
    hasher.update(evidence);
    Digest::from_bytes(*hasher.finalize().as_bytes())
}

pub(super) const fn fingerprint_input_length(operation_context: Option<Digest>) -> usize {
    16 + 8 + 8 + 8 + 32 + 16 + 33 + 1 + if operation_context.is_some() { 32 } else { 0 }
}

pub(super) fn write_publication_payload(
    bytes: &mut Vec<u8>,
    volume_id: VolumeId,
    generation_root: ObjectId,
    evidence: Option<OriginalEvidence>,
    origin: Option<PublicationOrigin>,
) {
    bytes.extend_from_slice(PAYLOAD_DOMAIN);
    bytes.extend_from_slice(&PAYLOAD_VERSION.to_le_bytes());
    bytes.extend_from_slice(&volume_id.into_bytes());
    bytes.extend_from_slice(&generation_root.to_bytes());
    bytes.push(u8::from(origin.is_some()) | (u8::from(evidence.is_some()) << 1));
    if let Some(origin) = origin {
        bytes.extend_from_slice(&origin.expected.epoch.get().to_le_bytes());
        bytes.extend_from_slice(&origin.expected.sequence.get().to_le_bytes());
        bytes.extend_from_slice(origin.expected.digest.as_bytes());
        bytes.extend_from_slice(&origin.generation_root.to_bytes());
    }
    if let Some(evidence) = evidence {
        bytes.extend_from_slice(&evidence.bytes.to_le_bytes());
        bytes.extend_from_slice(&evidence.root.to_bytes());
        bytes.extend_from_slice(evidence.digest.as_bytes());
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
    evidence: Option<OriginalEvidence>,
) {
    bytes.extend_from_slice(&request.authority_id.into_bytes());
    bytes.extend_from_slice(&request.epoch.get().to_le_bytes());
    bytes.extend_from_slice(&request.expected.epoch.get().to_le_bytes());
    bytes.extend_from_slice(&request.expected.sequence.get().to_le_bytes());
    bytes.extend_from_slice(request.expected.digest.as_bytes());
    if let Some(context) = operation_context {
        bytes.extend_from_slice(context.as_bytes());
    }
    bytes.extend_from_slice(&request.volume_id.into_bytes());
    bytes.extend_from_slice(&request.generation_root.to_bytes());
    bytes.push(u8::from(evidence.is_some()));
    if let Some(evidence) = evidence {
        bytes.extend_from_slice(&evidence.bytes.to_le_bytes());
        bytes.extend_from_slice(evidence.digest.as_bytes());
    }
}
