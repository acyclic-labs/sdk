//! On-demand multipart record admission through the original replay decoder.
use super::*;

fn created() -> MultipartStorageResult<Bytes> {
    let limits = FilesystemS3Limits::default();
    encode_multipart_created("key", Some(2), limits.maximum_multipart_part_bytes, 0)
}

fn etag() -> String {
    format!("\"{}\"", hex::encode([5; 32]))
}

/// Decode in an explicit upload context, then compare the owning writer bytes.
/// The context is codec input, not evidence of stored content or publication.
pub fn check(bytes: &[u8]) {
    let result = check_inner(bytes);
    assert!(result.is_ok(), "multipart codec check failed: {result:?}");
}

fn check_inner(bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let limits = FilesystemS3Limits::default();
    let input: acyclic_stream::OwnedBytes = Bytes::copy_from_slice(bytes).into();
    let initial = created()?;
    if let Ok((key, parts, maximum, minimum)) = decode_multipart_header(&input, limits) {
        assert_eq!(
            encode_multipart_created(&key, parts, maximum, minimum)?.as_ref(),
            bytes,
        );
    }
    if let Ok(snapshot) = decode_multipart(vec![initial.clone().into(), input.clone()], 2, limits) {
        match snapshot.terminal {
            None => {
                assert_eq!(snapshot.operations.len(), 1);
                let (operation, (number, etag)) = snapshot
                    .operations
                    .first_key_value()
                    .ok_or("missing operation")?;
                let part = snapshot.parts.get(number).ok_or("missing part")?;
                assert_eq!(
                    encode_multipart_part(
                        *number,
                        part.staged.root(),
                        part.staged.logical_bytes(),
                        IdempotencyKey::from_bytes(*operation),
                        etag
                    )
                    .as_ref(),
                    bytes,
                );
            }
            Some(MultipartTerminal::Completing(parts)) => {
                assert_eq!(encode_multipart_completing(&parts)?.as_ref(), bytes,)
            }
            Some(MultipartTerminal::Aborted) => {
                assert_eq!(encode_multipart_aborted().as_ref(), bytes)
            }
            Some(MultipartTerminal::Completed(_)) => {
                return Err("completed record requires its original completing predecessor".into());
            }
        }
    }
    let completing = encode_multipart_completing(&[(1, etag())])?;
    if let Ok(snapshot) = decode_multipart(
        vec![initial.clone().into(), completing.clone().into(), input],
        3,
        limits,
    ) {
        if let Some(MultipartTerminal::Completed(generation)) = snapshot.terminal {
            assert_eq!(encode_multipart_completed(generation).as_ref(), bytes);
        }
    }
    check_versions()?;
    Ok(())
}

fn check_versions() -> Result<(), Box<dyn std::error::Error>> {
    for version in [0, 2, u8::MAX] {
        let (tag, consumed) = inspect_multipart_header(std::slice::from_ref(&version));
        assert!(
            matches!(tag, Err(MultipartStorageError::UnsupportedVersion(found)) if found == version)
        );
        assert_eq!(
            consumed, 0,
            "unsupported version must not consume a payload field"
        );
        let record = Bytes::copy_from_slice(&[version]).into();
        assert!(matches!(
            decode_multipart(vec![record], 1, FilesystemS3Limits::default()),
            Err(MultipartStorageError::UnsupportedVersion(found)) if found == version
        ));
    }
    assert!(matches!(
        inspect_multipart_header(&[]).0,
        Err(MultipartStorageError::UnsupportedVersion(0))
    ));
    assert!(matches!(
        inspect_multipart_header(&[MULTIPART_RECORD_VERSION]).0,
        Err(MultipartStorageError::Malformed)
    ));
    // The declared count cannot allocate beyond the actual fixed-width entries,
    // even when the caller configured the widest representable part bound.
    let limits = FilesystemS3Limits {
        maximum_multipart_parts: Some(u32::MAX),
        ..FilesystemS3Limits::default()
    };
    let initial = encode_multipart_created("key", Some(u32::MAX), Some(1), 0)?;
    let mut count_only = encode_multipart_completing(&[(1, etag())])?.to_vec();
    count_only.truncate(6);
    count_only[2..6].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        decode_multipart(
            vec![initial.into(), Bytes::from(count_only).into()],
            2,
            limits
        ),
        Err(MultipartStorageError::Malformed)
    ));
    Ok(())
}

/// Capture each production writer with complete plain and sealed references.
pub fn seeds() -> MultipartStorageResult<Vec<(&'static str, Vec<u8>)>> {
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
            encode_multipart_part(1, root, 4, IdempotencyKey::from_bytes([6; 16]), &etag())
                .to_vec(),
        ));
        let generation = acyclic_fs_format::object_reference::GenerationId::from_root(ObjectId {
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
