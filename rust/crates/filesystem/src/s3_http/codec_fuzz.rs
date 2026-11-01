//! On-demand multipart record admission through the original replay decoder.
use super::*;

fn created() -> S3Result<Bytes> {
    let limits = FilesystemS3Limits::default();
    encode_multipart_created("key", 2, limits.maximum_multipart_part_bytes, 0)
}

fn etag() -> String {
    format!("\"{}\"", hex::encode([5; 32]))
}

/// Decode in an explicit upload context, then compare the owning writer bytes.
/// The context is codec input, not evidence of stored content or publication.
pub fn check(bytes: &[u8]) {
    let limits = FilesystemS3Limits::default();
    let input: acyclic_stream::OwnedBytes = Bytes::copy_from_slice(bytes).into();
    let initial =
        created().expect("multipart created writer; recovery: cargo run --example capture");
    if let Ok((key, parts, maximum, minimum)) = decode_multipart_header(&input, limits) {
        assert_eq!(
            encode_multipart_created(&key, parts, maximum, minimum)
                .unwrap()
                .as_ref(),
            bytes,
        );
    }
    if let Ok(snapshot) = decode_multipart(vec![initial.clone().into(), input.clone()], 2, limits) {
        match snapshot.terminal {
            None => {
                assert_eq!(snapshot.operations.len(), 1);
                let (operation, (number, etag)) = snapshot.operations.first_key_value().unwrap();
                let part = snapshot.parts.get(number).unwrap();
                assert_eq!(
                    encode_multipart_part(
                        *number,
                        part.staged,
                        IdempotencyKey::from_bytes(*operation),
                        etag
                    )
                    .as_ref(),
                    bytes,
                );
            }
            Some(MultipartTerminal::Completing(parts)) => {
                assert_eq!(encode_multipart_completing(&parts).unwrap().as_ref(), bytes,)
            }
            Some(MultipartTerminal::Aborted) => {
                assert_eq!(encode_multipart_aborted().as_ref(), bytes)
            }
            Some(MultipartTerminal::Completed(_)) => panic!(
                "completed record requires its original completing predecessor; recovery: cargo run --example capture"
            ),
        }
    }
    let completing = encode_multipart_completing(&[(1, etag())]).unwrap();
    if let Ok(snapshot) = decode_multipart(
        vec![initial.clone().into(), completing.clone().into(), input],
        3,
        limits,
    ) {
        if let Some(MultipartTerminal::Completed(generation)) = snapshot.terminal {
            assert_eq!(encode_multipart_completed(generation).as_ref(), bytes);
        }
    }
    check_versions();
}

fn check_versions() {
    for version in [0, 2, u8::MAX] {
        let mut header = RecordCursor::new(std::slice::from_ref(&version));
        assert!(
            matches!(header.any_header(), Err(MultipartDecodeError::UnsupportedVersion(found)) if found == version)
        );
        assert_eq!(
            header.offset, 0,
            "unsupported version must not consume a payload field"
        );
        let error: s3s::S3Error = MultipartDecodeError::UnsupportedVersion(version).into();
        assert_eq!(error.code(), &s3s::S3ErrorCode::NotImplemented);
        let record = Bytes::copy_from_slice(&[version]).into();
        assert!(matches!(
            decode_multipart(vec![record], 1, FilesystemS3Limits::default()),
            Err(MultipartDecodeError::UnsupportedVersion(found)) if found == version
        ));
    }
    let mut missing = RecordCursor::new(&[]);
    assert!(matches!(
        missing.any_header(),
        Err(MultipartDecodeError::UnsupportedVersion(0))
    ));
    let mut truncated = RecordCursor::new(&[MULTIPART_RECORD_VERSION]);
    assert!(matches!(
        truncated.any_header(),
        Err(MultipartDecodeError::Malformed)
    ));
    // The declared count cannot allocate beyond the actual fixed-width entries,
    // even when the caller configured the widest representable part bound.
    let limits = FilesystemS3Limits {
        maximum_multipart_parts: u32::MAX,
        ..FilesystemS3Limits::default()
    };
    let initial = encode_multipart_created("key", u32::MAX, 1, 0).unwrap();
    let mut count_only = encode_multipart_completing(&[(1, etag())])
        .unwrap()
        .to_vec();
    count_only.truncate(6);
    count_only[2..6].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        decode_multipart(
            vec![initial.into(), Bytes::from(count_only).into()],
            2,
            limits
        ),
        Err(MultipartDecodeError::Malformed)
    ));
    let malformed: s3s::S3Error = MultipartDecodeError::Malformed.into();
    assert_eq!(malformed.code(), &s3s::S3ErrorCode::InvalidRequest);
}

/// Capture each production writer with complete plain and sealed references.
pub fn seeds() -> S3Result<Vec<(&'static str, Vec<u8>)>> {
    let mut seeds = vec![
        ("multipart-created", created()?.to_vec()),
        (
            "multipart-completing",
            encode_multipart_completing(&[(1, etag())])?.to_vec(),
        ),
        ("multipart-aborted", encode_multipart_aborted().to_vec()),
    ];
    for sealed in [false, true] {
        let root = ObjectId {
            kind: crate::ObjectKind::Blob,
            digest: crate::Digest::from_bytes([1; 32]),
            storage: sealed.then_some(crate::ObjectStorage {
                locator: crate::Digest::from_bytes([2; 32]),
                root_file_id: crate::FileId::from_bytes([3; 16]),
                origin_generation: crate::Digest::from_bytes([4; 32]),
            }),
        };
        seeds.push((
            if sealed {
                "multipart-part-sealed"
            } else {
                "multipart-part"
            },
            encode_multipart_part(
                1,
                StagedContent::from_canonical_parts(root, 4),
                IdempotencyKey::from_bytes([6; 16]),
                &etag(),
            )
            .to_vec(),
        ));
        let generation = crate::GenerationId::from_root(ObjectId {
            kind: crate::ObjectKind::GenerationRoot,
            ..root
        });
        seeds.push((
            if sealed {
                "multipart-completed-sealed"
            } else {
                "multipart-completed"
            },
            encode_multipart_completed(generation).to_vec(),
        ));
    }
    Ok(seeds)
}
