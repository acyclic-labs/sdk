//! Evidence projection preserves original facts; it grants no storage custody.
use acyclic_fs::{
    AuthorityId, Digest, DurableCommit, Epoch, OperationId, Sequence, authority_commit_digest,
    kernel::*,
};
use sha2::{Digest as _, Sha256};
mod original;

pub(super) fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    futures::executor::block_on(original::seeds())
}

const DOMAIN: &[u8] = b"acyclic-fs-original-generation-record-v1\0";

fn evidence(bytes: &[u8]) -> HistoricalGenerationEvidence {
    // This self-bound fuzz input reaches syntax, never protected custody.
    HistoricalGenerationEvidence {
        bytes: bytes.to_vec().into(),
        sha256: Sha256::digest(bytes).into(),
    }
}

fn digest(authority: AuthorityId, record: &DurableCommit) -> Digest {
    authority_commit_digest(
        authority,
        record.epoch,
        record.sequence,
        record.operation_id,
        record.fingerprint,
        record.previous_digest,
        &record.payload,
    )
}

fn different_digest(original: Digest) -> Digest {
    let mut bytes = *original.as_bytes();
    bytes[0] ^= 1;
    Digest::from_bytes(bytes)
}

pub(super) fn check(bytes: &[u8]) {
    let input = evidence(bytes);
    let authority = AuthorityId::from_bytes([0; 16]);
    let mut record = DurableCommit {
        epoch: Epoch::GENESIS,
        sequence: Sequence::new(1),
        operation_id: OperationId::from_bytes([0; 16]),
        fingerprint: Digest::ZERO,
        previous_digest: Digest::ZERO,
        digest: Digest::ZERO,
        payload: Default::default(),
        settled_at_micros: None,
    };
    record.digest = digest(authority, &record);
    // Every arbitrary input reaches the actual historical parser, including
    // original initial-volume/retention evidence, before caller binding.
    let _ = decode_historical_generation_evidence(&input, authority, &record, u64::MAX, u64::MAX);
    // The original format admits its version before needing a body. Hashing
    // the supplied evidence remains part of the original protected-byte check.
    if bytes.starts_with(DOMAIN) && bytes.len() >= DOMAIN.len() + 2 {
        super::refuses_versions(bytes, DOMAIN.len(), |header, version| {
            matches!(
                decode_historical_generation_evidence(
                    &evidence(header), authority, &record, u64::MAX, u64::MAX,
                ),
                Err(PublicationEvidenceError::Decode(CanonicalDecodeError::UnsupportedVersion(found)))
                    if found == version
            )
        });
    }
}

pub(super) fn publication(bytes: &[u8], original: &GenerationPublicationProjection) {
    let append = &original.append;
    let input = evidence(bytes);
    let sequence = append.expected.sequence.checked_next().ok();
    let mut record = DurableCommit {
        epoch: append.epoch,
        sequence: sequence.unwrap_or(Sequence::GENESIS),
        operation_id: append.commit.operation_id,
        fingerprint: append.commit.fingerprint,
        previous_digest: append.expected.digest,
        digest: Digest::ZERO,
        payload: append.commit.payload.clone(),
        settled_at_micros: None,
    };
    record.digest = digest(append.authority_id, &record);
    let decoded = decode_historical_generation_evidence(
        &input,
        append.authority_id,
        &record,
        u64::MAX,
        u64::MAX,
    );
    if sequence.is_none() || append.expected.epoch != append.epoch {
        assert!(matches!(decoded, Err(PublicationEvidenceError::Mismatch)));
        return;
    }
    // A canonical digest constructed for this oracle is no settlement receipt.
    let expected = HistoricalGenerationProjection {
        append: original.append.clone(),
        manifest: original.manifest.clone(),
        root: original.root.clone(),
        logical_file_bytes: original.logical_file_bytes,
    };
    assert_eq!(decoded.expect("original record context binds"), expected);
    verify(&input, append.authority_id, &record, &expected);
}

fn verify(
    input: &HistoricalGenerationEvidence,
    authority: AuthorityId,
    record: &DurableCommit,
    projected: &HistoricalGenerationProjection,
) {
    let byte_bound = u64::try_from(input.bytes.len()).expect("admitted byte width");
    let object_bound =
        u64::try_from(projected.manifest.objects.len()).expect("admitted object width");
    assert_eq!(
        decode_historical_generation_evidence(input, authority, record, byte_bound, object_bound)
            .expect("exact original bounds admit"),
        *projected,
    );
    assert!(matches!(
        decode_historical_generation_evidence(
            input,
            authority,
            record,
            byte_bound - 1,
            object_bound
        ),
        Err(PublicationEvidenceError::TooLarge)
    ));
    let mut foreign = input.clone();
    foreign.sha256[0] ^= 1;
    assert!(matches!(
        decode_historical_generation_evidence(&foreign, authority, record, u64::MAX, u64::MAX),
        Err(PublicationEvidenceError::Mismatch)
    ));
    for field in 0..8 {
        let mut changed = record.clone();
        match field {
            0 => {
                changed.epoch =
                    Epoch::new(if record.epoch == Epoch::GENESIS { 2 } else { 1 }).unwrap()
            }
            1 => changed.sequence = Sequence::new(record.sequence.get() ^ 1),
            2 => {
                let mut bytes = changed.operation_id.into_bytes();
                bytes[0] ^= 1;
                changed.operation_id = OperationId::from_bytes(bytes);
            }
            3 => changed.fingerprint = different_digest(changed.fingerprint),
            4 => changed.previous_digest = different_digest(changed.previous_digest),
            5 => {
                let mut bytes = changed.payload.to_vec();
                bytes.push(0);
                changed.payload = bytes.into();
            }
            6 => changed.digest = different_digest(changed.digest),
            7 => {
                let mut bytes = authority.into_bytes();
                bytes[0] ^= 1;
                let foreign_authority = AuthorityId::from_bytes(bytes);
                changed.digest = digest(foreign_authority, &changed);
                assert!(matches!(
                    decode_historical_generation_evidence(
                        input,
                        foreign_authority,
                        &changed,
                        u64::MAX,
                        u64::MAX,
                    ),
                    Err(PublicationEvidenceError::Mismatch)
                ));
                continue;
            }
            _ => unreachable!(),
        }
        if field != 6 {
            // Refusal must still hold for an internally canonical foreign record.
            changed.digest = digest(authority, &changed);
        }
        assert!(matches!(
            decode_historical_generation_evidence(input, authority, &changed, u64::MAX, u64::MAX),
            Err(PublicationEvidenceError::Mismatch)
        ));
    }
    let mut timestamp = record.clone();
    timestamp.settled_at_micros = Some(u64::MAX);
    assert_eq!(
        decode_historical_generation_evidence(input, authority, &timestamp, u64::MAX, u64::MAX)
            .expect("observation timestamp does not replace original commit facts"),
        *projected,
    );
}
