//! Canonical native preparation evidence, not a public transport or grant.

use super::codec::Encoder;
use super::{
    CanonicalDecodeError, GenerationExportManifest, GenerationExportManifestError, GenerationProof,
    PublishGenerationRequest, decode_published_generation, encode_generation_export_manifest,
    encode_generation_root, generation_root_id,
};
use crate::model::VolumeConfig;
use crate::{GuardedAppend, PublicationPermit};
use bytes::Bytes;
use sha2::{Digest as _, Sha256};
use thiserror::Error;

/// Exact canonical bytes to retain in the provider's existing native journal.
/// The SHA256 names these bytes only; neither field establishes archive custody,
/// final authority acceptance, settlement time, or permission to release roots.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationPublicationEvidence {
    /// Original immutable native preparation bytes.
    pub bytes: Bytes,
    /// SHA256 of exactly `bytes`, distinct from the filesystem BLAKE3 identity.
    pub sha256: [u8; 32],
}

/// Decoded original preparation, not a fresh closure proof or accepted outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenerationPublicationProjection {
    /// Exact original publication preconditions.
    pub request: PublishGenerationRequest,
    /// Canonical complete closure manifest and original volume configuration.
    pub manifest: GenerationExportManifest,
    /// Original authenticated root representation.
    pub root: super::GenerationRoot,
    /// Original proof's logical regular-file byte total.
    pub logical_file_bytes: u128,
    /// Original commit, permit, epoch and expected authority head.
    pub append: GuardedAppend,
}

/// Evidence encoding fails closed before admitting a mismatched preparation.
#[derive(Debug, Error)]
pub enum PublicationEvidenceError {
    /// Existing canonical object validation failed.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Existing complete manifest validation failed.
    #[error(transparent)]
    Manifest(#[from] GenerationExportManifestError),
    /// Request, proof, or exact append disagree.
    #[error("generation publication preparation does not match")]
    Mismatch,
    /// Evidence exceeds the caller's explicit byte bound.
    #[error("generation publication evidence exceeds its byte bound")]
    TooLarge,
}

/// Encodes the original request, root, complete manifest, logical-byte proof,
/// exact proposed commit and permit using Rust-owned canonical encoders.
/// Configuration must come from the admitted volume, not the customer request.
/// Store these exact bytes before publishing; retries must reuse accepted bytes.
///
/// The proof's runtime work counters and collection sweep count are excluded:
/// they are not durable generation facts. Original closure objects and their
/// exact Objects references still require protected provider-owned retention.
/// Final settlement must come from the eventual exact committed Stream envelope,
/// retained alongside this preparation by the existing product journal.
pub fn encode_generation_publication_evidence(
    request: &PublishGenerationRequest,
    proof: &GenerationProof,
    append: &GuardedAppend,
    config: VolumeConfig,
    maximum_bytes: u64,
) -> Result<GenerationPublicationEvidence, PublicationEvidenceError> {
    let maximum = usize::try_from(maximum_bytes).map_err(|_| PublicationEvidenceError::TooLarge)?;
    let minimum = proof
        .objects
        .len()
        .checked_mul(33)
        .and_then(|size| size.checked_add(append.commit.payload.len()))
        .ok_or(PublicationEvidenceError::TooLarge)?;
    if maximum == 0 || minimum > maximum {
        return Err(PublicationEvidenceError::TooLarge);
    }
    let (root_id, generation_id) = generation_root_id(&proof.root)?;
    let published = decode_published_generation(&append.commit.payload, maximum_bytes)?;
    if request.authority_id != super::volume_authority_id(request.volume_id)
        || request.authority_id != append.authority_id
        || request.epoch != append.epoch
        || request.expected != append.expected
        || request.operation_id != append.commit.operation_id
        || request.volume_id != proof.root.volume_id
        || request.generation_root != root_id
        || proof.generation_id != generation_id
        || published.volume_id != request.volume_id
        || published.generation_root != root_id
        || proof.object_count != u64::try_from(proof.objects.len()).unwrap_or(u64::MAX)
    {
        return Err(PublicationEvidenceError::Mismatch);
    }
    let manifest = encode_generation_export_manifest(&GenerationExportManifest {
        volume_id: request.volume_id,
        config,
        generation_root: root_id,
        generation_id,
        objects: proof.objects.clone(),
        file_count: proof.file_count,
    })?;
    let root = encode_generation_root(&proof.root)?;
    let mut permit = Encoder::new(b"acyclic-fs-publication-permit-v1\0", 1);
    match append.permit {
        PublicationPermit::Unrestricted => permit.u8(0),
        PublicationPermit::Lease {
            authority_id,
            workspace_id,
            lease_id,
            expires_at_millis,
        } => {
            permit.u8(1);
            permit.fixed(&authority_id);
            permit.fixed(&workspace_id);
            permit.fixed(&lease_id);
            permit.u64(expires_at_millis);
        }
        PublicationPermit::Reservation {
            operation_id,
            gate_tail,
            expected,
        } => {
            permit.u8(2);
            permit.fixed(&operation_id);
            permit.u64(gate_tail);
            permit.u64(expected.epoch.get());
            permit.u64(expected.sequence.get());
            permit.fixed(expected.digest.as_bytes());
        }
    }
    let permit = permit.finish();
    const DOMAIN: &[u8] = b"acyclic-fs-generation-publication-evidence-v1\0";
    let capacity = [
        DOMAIN.len(),
        2,
        16,
        8,
        8,
        8,
        32,
        16,
        32,
        16,
        16,
        4,
        manifest.len(),
        4,
        root.len(),
        4,
        append.commit.payload.len(),
        4,
        permit.len(),
    ]
    .into_iter()
    .try_fold(0_usize, |total, size| total.checked_add(size))
    .ok_or(PublicationEvidenceError::TooLarge)?;
    if capacity > maximum {
        return Err(PublicationEvidenceError::TooLarge);
    }
    let mut encoder = Encoder::with_exact_capacity(DOMAIN, 1, capacity)?;
    encoder.fixed(&request.authority_id.into_bytes());
    encoder.u64(request.epoch.get());
    encoder.u64(request.expected.epoch.get());
    encoder.u64(request.expected.sequence.get());
    encoder.fixed(request.expected.digest.as_bytes());
    encoder.fixed(&request.operation_id.into_bytes());
    encoder.fixed(append.commit.fingerprint.as_bytes());
    encoder.fixed(&request.volume_id.into_bytes());
    encoder.fixed(&proof.logical_file_bytes.to_le_bytes());
    encoder.bounded_bytes(&manifest)?;
    encoder.bounded_bytes(&root)?;
    encoder.bounded_bytes(&append.commit.payload)?;
    encoder.bounded_bytes(&permit)?;
    let bytes = Bytes::from(encoder.finish());
    let sha256 = Sha256::digest(&bytes).into();
    Ok(GenerationPublicationEvidence { bytes, sha256 })
}

/// Projects bounded original evidence against its retained SHA256. The expected
/// digest must come from protected original custody, never the request under
/// inspection. Hash matching alone does not prove that custody or acceptance.
pub fn decode_generation_publication_evidence(
    bytes: &[u8],
    expected_sha256: &[u8; 32],
    maximum_bytes: u64,
    maximum_objects: u64,
) -> Result<GenerationPublicationProjection, PublicationEvidenceError> {
    use super::codec::Decoder;
    use crate::foundation::{
        AuthorityId, Digest, Epoch, Head, OperationId, ProposedCommit, Sequence, VolumeId,
    };
    if bytes.len() as u64 > maximum_bytes || maximum_bytes == 0 {
        return Err(PublicationEvidenceError::TooLarge);
    }
    if Sha256::digest(bytes).as_slice() != expected_sha256 {
        return Err(PublicationEvidenceError::Mismatch);
    }
    let mut decoder = Decoder::new(
        bytes,
        b"acyclic-fs-generation-publication-evidence-v1\0",
        1,
        maximum_bytes,
    )?;
    let epoch = |value| Epoch::new(value).map_err(|_| PublicationEvidenceError::Mismatch);
    let authority_id = AuthorityId::from_bytes(decoder.fixed()?);
    let writer_epoch = epoch(decoder.u64()?)?;
    let expected = Head {
        epoch: epoch(decoder.u64()?)?,
        sequence: Sequence::new(decoder.u64()?),
        digest: Digest::from_bytes(decoder.fixed()?),
    };
    let operation_id = OperationId::from_bytes(decoder.fixed()?);
    let fingerprint = Digest::from_bytes(decoder.fixed()?);
    let volume_id = VolumeId::from_bytes(decoder.fixed()?);
    let logical_file_bytes = u128::from_le_bytes(decoder.fixed()?);
    let field_bound = u32::try_from(maximum_bytes).unwrap_or(u32::MAX);
    let manifest_bytes = decoder.bounded_bytes(field_bound)?;
    let manifest =
        super::decode_generation_export_manifest(&manifest_bytes, maximum_bytes, maximum_objects)?;
    let root_bytes = decoder.bounded_bytes(field_bound)?;
    let root = super::decode_generation_root(
        &root_bytes,
        super::DecodeLimits {
            maximum_object_bytes: maximum_bytes,
            ..super::DecodeLimits::default()
        },
    )?;
    let payload = Bytes::from(decoder.bounded_bytes(field_bound)?);
    let permit_bytes = decoder.bounded_bytes(field_bound)?;
    decoder.finish()?;
    let mut permission = Decoder::new(
        &permit_bytes,
        b"acyclic-fs-publication-permit-v1\0",
        1,
        maximum_bytes,
    )?;
    let permit = match permission.u8()? {
        0 => PublicationPermit::Unrestricted,
        1 => PublicationPermit::Lease {
            authority_id: permission.fixed()?,
            workspace_id: permission.fixed()?,
            lease_id: permission.fixed()?,
            expires_at_millis: permission.u64()?,
        },
        2 => PublicationPermit::Reservation {
            operation_id: permission.fixed()?,
            gate_tail: permission.u64()?,
            expected: Head {
                epoch: epoch(permission.u64()?)?,
                sequence: Sequence::new(permission.u64()?),
                digest: Digest::from_bytes(permission.fixed()?),
            },
        },
        _ => return Err(PublicationEvidenceError::Mismatch),
    };
    permission.finish()?;
    let request = PublishGenerationRequest {
        authority_id,
        volume_id,
        epoch: writer_epoch,
        expected,
        operation_id,
        generation_root: manifest.generation_root,
    };
    let append = GuardedAppend {
        authority_id,
        epoch: writer_epoch,
        expected,
        commit: ProposedCommit {
            operation_id,
            fingerprint,
            payload,
        },
        permit,
    };
    let proof = GenerationProof {
        root: root.clone(),
        generation_id: manifest.generation_id,
        object_count: manifest.objects.len() as u64,
        file_count: manifest.file_count,
        logical_file_bytes,
        objects: manifest.objects.clone(),
        work: crate::WorkCounters::default(),
    };
    let canonical = encode_generation_publication_evidence(
        &request,
        &proof,
        &append,
        manifest.config,
        maximum_bytes,
    )?;
    if canonical.bytes.as_ref() != bytes {
        return Err(PublicationEvidenceError::Mismatch);
    }
    Ok(GenerationPublicationProjection {
        request,
        manifest,
        root,
        logical_file_bytes,
        append,
    })
}
