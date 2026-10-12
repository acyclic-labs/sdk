//! Provider-owned original generation preparation projected against exact settlement.

use super::codec::{Decoder, Encoder};
use super::publication_evidence::{decode_publication_permit, encode_publication_permit};
use super::{
    DecodeLimits, GenerationExportManifest, GenerationProof, GenerationRoot,
    PublicationEvidenceError, decode_generation_export_manifest, decode_generation_publication_evidence,
    decode_generation_root, decode_retention_created,
    decode_volume_created, encode_generation_export_manifest, encode_generation_root,
    generation_root_id, retention_authority_id, volume_authority_id,
};
use crate::foundation::{AuthorityId, Digest, DurableCommit, Epoch, Head, OperationId, ProposedCommit, Sequence};
use crate::model::VolumeConfig;
use crate::storage::GuardedAppend;
use bytes::Bytes;
use sha2::{Digest as _, Sha256};

const DOMAIN: &[u8] = b"acyclic-fs-original-generation-record-v1\0";
const PUBLICATION_DOMAIN: &[u8] = b"acyclic-fs-generation-publication-evidence-v1\0";

/// Bytes and SHA256 supplied by a provider's protected original custody.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoricalGenerationEvidence {
    /// The exact original pre-CAS canonical preparation bytes.
    pub bytes: Bytes,
    /// Digest read from protected custody, never reconstructed from untrusted bytes.
    pub sha256: [u8; 32],
}

impl From<super::GenerationPublicationEvidence> for HistoricalGenerationEvidence {
    fn from(value: super::GenerationPublicationEvidence) -> Self {
        Self { bytes: value.bytes, sha256: value.sha256 }
    }
}

/// Original metadata, not a newly authenticated closure proof or accepted result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoricalGenerationProjection {
    /// Exact originally proposed append, including permit and preconditions.
    pub append: GuardedAppend,
    /// Complete original manifest and admitted configuration.
    pub manifest: GenerationExportManifest,
    /// Original authenticated immutable root.
    pub root: GenerationRoot,
    /// Original proof's logical regular-file byte total, including sparse holes.
    pub logical_file_bytes: u128,
}

/// Encodes original initial-volume or retention preparation before authority admission.
pub fn encode_original_generation_evidence(
    proof: &GenerationProof,
    append: &GuardedAppend,
    config: VolumeConfig,
    maximum_bytes: u64,
) -> Result<HistoricalGenerationEvidence, PublicationEvidenceError> {
    let minimum = proof.objects.len().checked_mul(33)
        .and_then(|size| size.checked_add(append.commit.payload.len()))
        .ok_or(PublicationEvidenceError::TooLarge)?;
    if maximum_bytes == 0 || u64::try_from(minimum).unwrap_or(u64::MAX) > maximum_bytes {
        return Err(PublicationEvidenceError::TooLarge);
    }
    let (generation_root, generation_id) = generation_root_id(&proof.root)?;
    if proof.generation_id != generation_id
        || proof.object_count != u64::try_from(proof.objects.len()).unwrap_or(u64::MAX)
    {
        return Err(PublicationEvidenceError::Mismatch);
    }
    let projection = HistoricalGenerationProjection {
        append: append.clone(),
        manifest: GenerationExportManifest {
            volume_id: proof.root.volume_id,
            config,
            generation_root,
            generation_id,
            objects: proof.objects.clone(),
            file_count: proof.file_count,
        },
        root: proof.root.clone(),
        logical_file_bytes: proof.logical_file_bytes,
    };
    validate_projection(&projection, maximum_bytes)?;
    encode_projection(&projection, maximum_bytes)
}

/// Decodes protected original metadata and binds it to this exact authoritative record.
pub fn decode_historical_generation_evidence(
    evidence: &HistoricalGenerationEvidence,
    authority_id: AuthorityId,
    record: &DurableCommit,
    maximum_bytes: u64,
    maximum_objects: u64,
) -> Result<HistoricalGenerationProjection, PublicationEvidenceError> {
    if maximum_bytes == 0 || u64::try_from(evidence.bytes.len()).unwrap_or(u64::MAX) > maximum_bytes {
        return Err(PublicationEvidenceError::TooLarge);
    }
    let projection = if evidence.bytes.starts_with(PUBLICATION_DOMAIN) {
        let publication = decode_generation_publication_evidence(
            &evidence.bytes, &evidence.sha256, maximum_bytes, maximum_objects,
        )?;
        HistoricalGenerationProjection {
            append: publication.append,
            manifest: publication.manifest,
            root: publication.root,
            logical_file_bytes: publication.logical_file_bytes,
        }
    } else {
        if <[u8; 32]>::from(Sha256::digest(&evidence.bytes)) != evidence.sha256 {
            return Err(PublicationEvidenceError::Mismatch);
        }
        let mut decoder = Decoder::new(&evidence.bytes, DOMAIN, 1, maximum_bytes, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
        let authority_id = AuthorityId::from_bytes(decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?);
        let epoch = Epoch::new(decoder.u64(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?).map_err(|_| PublicationEvidenceError::Mismatch)?;
        let expected = Head {
            epoch: Epoch::new(decoder.u64(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?).map_err(|_| PublicationEvidenceError::Mismatch)?,
            sequence: Sequence::new(decoder.u64(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?),
            digest: Digest::from_bytes(decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?),
        };
        let operation_id = OperationId::from_bytes(decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?);
        let fingerprint = Digest::from_bytes(decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?);
        let logical_file_bytes = u128::from_le_bytes(decoder.fixed(&mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?);
        let field_bound = u32::try_from(maximum_bytes).unwrap_or(u32::MAX);
        let manifest = decode_generation_export_manifest(
            decoder.bounded_slice(field_bound, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?, maximum_bytes, maximum_objects,
        )?;
        let root = decode_generation_root(decoder.bounded_slice(field_bound, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?, DecodeLimits { maximum_object_bytes: maximum_bytes, ..DecodeLimits::default() }, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?;
        let payload = Bytes::copy_from_slice(decoder.bounded_slice(field_bound, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?);
        let permit = decode_publication_permit(decoder.bounded_slice(field_bound, &mut |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))?, maximum_bytes)?;
        decoder.finish()?;
        let projection = HistoricalGenerationProjection {
            append: GuardedAppend {
                authority_id, epoch, expected,
                commit: ProposedCommit { operation_id, fingerprint, payload }, permit,
            },
            manifest, root, logical_file_bytes,
        };
        validate_projection(&projection, maximum_bytes)?;
        if encode_projection(&projection, maximum_bytes)?.bytes != evidence.bytes {
            return Err(PublicationEvidenceError::Mismatch);
        }
        projection
    };
    let append = &projection.append;
    if append.authority_id != authority_id
        || append.epoch != record.epoch
        || append.expected.epoch != record.epoch
        || append.expected.sequence.checked_next().ok() != Some(record.sequence)
        || append.expected.digest != record.previous_digest
        || append.commit.operation_id != record.operation_id
        || append.commit.fingerprint != record.fingerprint
        || append.commit.payload != record.payload
        || crate::authority_commit_digest(
            authority_id, record.epoch, record.sequence, record.operation_id,
            record.fingerprint, record.previous_digest, &record.payload,
        ) != record.digest
    {
        return Err(PublicationEvidenceError::Mismatch);
    }
    Ok(projection)
}

fn validate_projection(
    value: &HistoricalGenerationProjection,
    maximum_bytes: u64,
) -> Result<(), PublicationEvidenceError> {
    let (root_id, generation_id) = generation_root_id(&value.root)?;
    if value.manifest.volume_id != value.root.volume_id
        || value.manifest.generation_root != root_id
        || value.manifest.generation_id != generation_id
    {
        return Err(PublicationEvidenceError::Mismatch);
    }
    let append = &value.append;
    let valid = if let Ok(created) = decode_volume_created(&append.commit.payload, maximum_bytes, &mut |_| Ok::<_, crate::kernel::VolumeCreatedError>(())) {
        append.authority_id == volume_authority_id(created.volume_id)
            && append.expected.sequence == Sequence::GENESIS
            && append.expected.digest == Digest::ZERO
            && created.volume_id == value.root.volume_id
            && created.initial_generation_root == root_id
            && created.config == value.manifest.config
    } else if let Ok(retained) = decode_retention_created(&append.commit.payload, maximum_bytes) {
        append.authority_id == retention_authority_id(retained.volume_id, retained.kind, &retained.label)
            && append.expected.sequence == Sequence::GENESIS
            && append.expected.digest == Digest::ZERO
            && retained.volume_id == value.root.volume_id
            && retained.generation_root == root_id
            && retained.config == value.manifest.config
    } else { false };
    if !valid { return Err(PublicationEvidenceError::Mismatch); }
    Ok(())
}

fn encode_projection(
    value: &HistoricalGenerationProjection,
    maximum_bytes: u64,
) -> Result<HistoricalGenerationEvidence, PublicationEvidenceError> {
    let manifest = encode_generation_export_manifest(&value.manifest)?;
    let root = encode_generation_root(&value.root, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?;
    let permit = encode_publication_permit(value.append.permit);
    let capacity = [DOMAIN.len(), 2, 16, 8, 8, 8, 32, 16, 32, 16,
        4, manifest.len(), 4, root.len(), 4, value.append.commit.payload.len(), 4, permit.len()]
        .into_iter().try_fold(0_usize, |total, size| total.checked_add(size))
        .ok_or(PublicationEvidenceError::TooLarge)?;
    if maximum_bytes == 0 || u64::try_from(capacity).unwrap_or(u64::MAX) > maximum_bytes {
        return Err(PublicationEvidenceError::TooLarge);
    }
    let mut encoder = Encoder::with_exact_capacity(DOMAIN, 1, capacity, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()), |_| crate::kernel::CanonicalDecodeError::AllocationFailed)?;
    let append = &value.append;
    encoder.fixed(&append.authority_id.into_bytes());
    encoder.u64(append.epoch.get());
    encoder.u64(append.expected.epoch.get());
    encoder.u64(append.expected.sequence.get());
    encoder.fixed(append.expected.digest.as_bytes());
    encoder.fixed(&append.commit.operation_id.into_bytes());
    encoder.fixed(append.commit.fingerprint.as_bytes());
    encoder.fixed(&value.logical_file_bytes.to_le_bytes());
    encoder.bounded_bytes(&manifest)?;
    encoder.bounded_bytes(&root)?;
    encoder.bounded_bytes(&append.commit.payload)?;
    encoder.bounded_bytes(&permit)?;
    let bytes = Bytes::from(encoder.finish());
    let sha256 = Sha256::digest(&bytes).into();
    Ok(HistoricalGenerationEvidence { bytes, sha256 })
}
