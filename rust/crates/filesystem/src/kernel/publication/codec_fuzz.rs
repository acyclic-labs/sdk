//! Pure format controls; decoded facts grant no publication or closure authority.
use super::*;

pub fn check(bytes: &[u8]) -> Result<(), crate::FsError> {
    let mut steps = 0;
    let Ok(value) =
        decode_publication_evidence::<CanonicalDecodeError>(bytes, u64::MAX, None, |_| {
            steps += 1;
            Ok(())
        })
    else {
        return Ok(());
    };
    let length = publication_payload_length_with_evidence(
        value.publication.generation_root,
        value.original_evidence,
        value.origin,
    )?;
    let mut encoded = Vec::with_capacity(length);
    write_publication_payload(
        &mut encoded,
        value.publication.volume_id,
        value.publication.generation_root,
        value.original_evidence,
        value.origin,
    );
    if !(&(encoded.len()) == &(length)) {
        return Err(CanonicalDecodeError::Invariant(
            crate::kernel::codec::CanonicalInvariant::Static("filesystem oracle invariant failed"),
        )
        .into());
    };
    if !(&(encoded) == &(bytes)) {
        return Err(CanonicalDecodeError::Invariant(
            crate::kernel::codec::CanonicalInvariant::Static(
                "retain original evidence, origin and both complete root identities",
            ),
        )
        .into());
    };
    if !(steps > 0) {
        return Err(CanonicalDecodeError::Invariant(
            crate::kernel::codec::CanonicalInvariant::Static("filesystem oracle invariant failed"),
        )
        .into());
    };
    for stop in 0..steps {
        let mut calls = 0;
        let refused =
            CanonicalDecodeError::Invariant(crate::kernel::codec::CanonicalInvariant::Static(
                "original publication admission refusal",
            ));
        let result =
            decode_publication_evidence::<CanonicalDecodeError>(bytes, u64::MAX, None, |_| {
                let current = calls;
                calls += 1;
                if current == stop {
                    Err(refused.clone())
                } else {
                    Ok(())
                }
            });
        if !(&(result.err()) == &(Some(refused))) {
            return Err(CanonicalDecodeError::Invariant(
                crate::kernel::codec::CanonicalInvariant::Static(
                    "filesystem oracle invariant failed",
                ),
            )
            .into());
        };
        if !(&(calls) == &(stop + 1)) {
            return Err(CanonicalDecodeError::Invariant(
                crate::kernel::codec::CanonicalInvariant::Static(
                    "filesystem oracle invariant failed",
                ),
            )
            .into());
        };
    }
    let mut header = bytes[..PAYLOAD_DOMAIN.len() + 2].to_vec();
    for version in [0_u16, 1, 2, 3, 4, u16::MAX] {
        header[PAYLOAD_DOMAIN.len()..].copy_from_slice(&version.to_le_bytes());
        if !(&(decode_publication_evidence::<CanonicalDecodeError>(
            &header,
            u64::MAX,
            None,
            |_| Ok(()),
        )
        .err())
            == &(Some(CanonicalDecodeError::UnsupportedVersion {
                found: version,
                expected: 5,
            })))
        {
            return Err(CanonicalDecodeError::Invariant(
                crate::kernel::codec::CanonicalInvariant::Static(
                    "filesystem oracle invariant failed",
                ),
            )
            .into());
        };
    }
    let mut wrong = bytes.to_vec();
    wrong[PAYLOAD_DOMAIN.len() + 2 + 16 + value.publication.generation_root.encoded_len()] = 4;
    if !(decode_publication_evidence::<CanonicalDecodeError>(&wrong, u64::MAX, None, |_| Ok(()))
        .is_err())
    {
        return Err(CanonicalDecodeError::Invariant(
            crate::kernel::codec::CanonicalInvariant::Static("filesystem oracle invariant failed"),
        )
        .into());
    };
    Ok(())
}

/// Actual writer output covers all optional fields and both reference widths.
/// Synthetic semantic inputs provide syntax coverage, never authenticated proof.
pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, crate::FsError> {
    let mut seeds = Vec::new();
    for (names, storage) in [
        (
            [
                "publication-plain-v5",
                "publication-plain-evidence-v5",
                "publication-plain-origin-v5",
                "publication-plain-origin-evidence-v5",
            ],
            None,
        ),
        (
            [
                "publication-sealed-v5",
                "publication-sealed-evidence-v5",
                "publication-sealed-origin-v5",
                "publication-sealed-origin-evidence-v5",
            ],
            Some(crate::ObjectStorage {
                locator: Digest::from_bytes([11; 32]),
                root_file_id: crate::FileId::from_bytes([12; 16]),
                origin_generation: Digest::from_bytes([13; 32]),
            }),
        ),
    ] {
        let root = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([6; 32]),
            storage,
        };
        let original = PublicationOrigin {
            expected: Head {
                epoch: Epoch::GENESIS,
                sequence: crate::Sequence::new(17),
                digest: Digest::from_bytes([8; 32]),
            },
            generation_root: ObjectId {
                digest: Digest::from_bytes([7; 32]),
                ..root
            },
        };
        for (index, name) in names.into_iter().enumerate() {
            let evidence = (index & 1 != 0).then_some(OriginalEvidence {
                root: ObjectId {
                    kind: ObjectKind::Blob,
                    digest: Digest::from_bytes([14; 32]),
                    storage,
                },
                bytes: b"original operation bytes".len() as u64,
                digest: original_evidence_digest(b"original operation bytes"),
            });
            let origin = (index & 2 != 0).then_some(original);
            let length = publication_payload_length_with_evidence(root, evidence, origin)?;
            let mut bytes = Vec::with_capacity(length);
            write_publication_payload(
                &mut bytes,
                VolumeId::from_bytes([5; 16]),
                root,
                evidence,
                origin,
            );
            check(&bytes)?;
            seeds.push((name, bytes));
        }
    }
    Ok(seeds)
}
