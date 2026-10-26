use super::*;

#[test]
fn publication_payload_versions_are_canonical_bounded_and_validate_the_complete_frame() -> TestResult
{
    let original = b"immutable original operation data";
    let large = vec![7; 4096];
    for (volume, root_digest, evidence) in [
        (31, 32, None),
        (52, 53, None),
        (52, 53, Some(original.as_slice())),
        (52, 53, Some(large.as_slice())),
    ] {
        let volume_id = VolumeId::from_bytes([volume; 16]);
        let root = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([root_digest; 32]),
        };
        let expected = PublishedGeneration {
            volume_id,
            generation_root: root,
        };
        let mut bytes = Vec::new();
        write_publication_payload(
            &mut bytes,
            volume_id,
            root,
            evidence.map(|data| (data, original_evidence_digest(data))),
            None,
        );
        let length = bytes.len() as u64;
        let decoded = decode_publication_evidence::<CanonicalDecodeError>(&bytes, length, accept)?;
        assert_eq!(decoded.publication, expected);
        assert_eq!(decoded.original_evidence, evidence);
        assert_eq!(
            decode_published_generation(&bytes, length, accept)?,
            expected
        );
        assert!(matches!(
            decode_published_generation(&bytes, length - 1, accept),
            Err(CanonicalDecodeError::ObjectTooLarge { .. })
        ));
        let mut suffixed = bytes.clone();
        suffixed.push(0);
        assert!(matches!(
            decode_published_generation(&suffixed, u64::MAX, accept),
            Err(CanonicalDecodeError::TrailingBytes)
        ));
        assert!(decode_published_generation(&bytes[..bytes.len() - 1], u64::MAX, accept).is_err());
        for kind in [ObjectKind::Blob.canonical_tag(), u8::MAX] {
            let mut wrong = bytes.clone();
            wrong[PAYLOAD_DOMAIN.len() + 2 + 16] = kind;
            assert!(matches!(
                decode_published_generation(&wrong, u64::MAX, accept),
                Err(CanonicalDecodeError::Invariant(_))
            ));
        }
        if evidence.is_some() {
            for offset in [
                PAYLOAD_DOMAIN.len(),
                PAYLOAD_DOMAIN.len() + 2 + 16,
                publication_payload_length() + 4,
                bytes.len() - 1,
            ] {
                let mut wrong = bytes.clone();
                wrong[offset] = u8::MAX;
                assert!(decode_published_generation(&wrong, u64::MAX, accept).is_err());
            }
            for length in [0_u32, u32::MAX] {
                let mut wrong = bytes.clone();
                let offset = publication_payload_length();
                wrong[offset..offset + 4].copy_from_slice(&length.to_le_bytes());
                assert!(decode_published_generation(&wrong, bytes.len() as u64, accept).is_err());
            }
        } else {
            let mut suffixed = bytes.clone();
            suffixed.extend_from_slice(original);
            assert!(matches!(
                decode_published_generation(&suffixed, u64::MAX, accept),
                Err(CanonicalDecodeError::TrailingBytes)
            ));
        }
        if evidence == Some(large.as_slice()) {
            assert!(matches!(
                decode_published_generation(&bytes, 4096, accept),
                Err(CanonicalDecodeError::ObjectTooLarge { .. })
            ));
        }
    }
    Ok(())
}

#[test]
fn composed_publication_retains_the_original_retry_request() -> TestResult {
    let volume = VolumeId::from_bytes([71; 16]);
    let effective = ObjectId {
        kind: ObjectKind::GenerationRoot,
        digest: Digest::from_bytes([72; 32]),
    };
    let origin = PublicationOrigin {
        expected: Head {
            epoch: Epoch::GENESIS,
            sequence: crate::Sequence::new(17),
            digest: Digest::from_bytes([73; 32]),
        },
        generation_root: ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([74; 32]),
        },
    };
    for evidence in [None, Some(b"original evidence".as_slice())] {
        let mut bytes = Vec::new();
        write_publication_payload(
            &mut bytes,
            volume,
            effective,
            evidence.map(|data| (data, original_evidence_digest(data))),
            Some(origin),
        );
        assert_eq!(
            bytes.len(),
            publication_payload_length_with_evidence(evidence, Some(origin))?
        );
        let decoded = decode_publication_evidence::<CanonicalDecodeError>(
            &bytes,
            bytes.len() as u64,
            accept,
        )?;
        assert_eq!(decoded.publication.generation_root, effective);
        assert_eq!(decoded.origin, Some(origin));
        assert_eq!(decoded.original_evidence, evidence);
        for length in 0..bytes.len() {
            assert!(
                decode_publication_evidence::<CanonicalDecodeError>(
                    &bytes[..length],
                    u64::MAX,
                    accept
                )
                .is_err()
            );
        }
        let mut invalid_epoch = bytes.clone();
        invalid_epoch[publication_payload_length()..publication_payload_length() + 8].fill(0);
        assert!(
            decode_publication_evidence::<CanonicalDecodeError>(&invalid_epoch, u64::MAX, accept)
                .is_err()
        );
        bytes.push(0);
        assert!(matches!(
            decode_publication_evidence::<CanonicalDecodeError>(&bytes, u64::MAX, accept),
            Err(CanonicalDecodeError::TrailingBytes)
        ));
    }
    Ok(())
}
