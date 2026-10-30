//! Immutable filesystem decoders use their production admission and encoders.
use acyclic_fs::{FsError, kernel::*};

fn allocation_error(_: std::collections::TryReserveError) -> FsError {
    CanonicalDecodeError::AllocationFailed.into()
}

fn refuses_other_versions(mut decode: impl FnMut(&[u8]) -> Result<(), FsError>, bytes: &[u8]) {
    // These seven immutable object formats share an eight-byte domain followed
    // by a little-endian u16 version. No payload is needed to refuse a version.
    let mut header: [u8; 10] = bytes[..10].try_into().unwrap();
    for version in [0_u16, 2, u16::MAX] {
        header[8..].copy_from_slice(&version.to_le_bytes());
        assert!(matches!(
            decode(&header),
            Err(FsError::Decode(CanonicalDecodeError::UnsupportedVersion(found))) if found == version
        ));
    }
}

/// Check canonical identity and admission before any version-specific body.
pub fn check(bytes: &[u8]) {
    let limits = DecodeLimits::default();
    macro_rules! page {
        ($decode:ident, $encode:ident) => {
            if let Ok(value) = $decode::<FsError>(bytes, limits, &mut |_| Ok(())) {
                let encoded = $encode(
                    &value,
                    limits.maximum_page_items,
                    |_| Ok(()),
                    allocation_error,
                )
                .unwrap();
                assert_eq!(encoded, bytes);
                refuses_other_versions(
                    |input| $decode::<FsError>(input, limits, &mut |_| Ok(())).map(|_| ()),
                    bytes,
                );
            }
        };
    }
    page!(decode_file_table_page, encode_file_table_page);
    page!(decode_tree_page, encode_tree_page);
    page!(decode_extent_page, encode_extent_page);
    page!(decode_attribute_page, encode_attribute_page);
    page!(decode_blob_page, encode_blob_page);
    if let Ok(value) = decode_file_metadata::<FsError>(bytes, limits, &mut |_| Ok(())) {
        assert_eq!(
            encode_file_metadata(value, |_| Ok(()), allocation_error).unwrap(),
            bytes
        );
        refuses_other_versions(
            |input| decode_file_metadata::<FsError>(input, limits, &mut |_| Ok(())).map(|_| ()),
            bytes,
        );
    }
    if let Ok(value) = decode_generation_root::<FsError>(bytes, limits, &mut |_| Ok(())) {
        assert_eq!(
            encode_generation_root(&value, |_| Ok(()), allocation_error).unwrap(),
            bytes
        );
        refuses_other_versions(
            |input| decode_generation_root::<FsError>(input, limits, &mut |_| Ok(())).map(|_| ()),
            bytes,
        );
    }
    let _ = decode_volume_created::<VolumeCreatedError>(bytes, u64::MAX, &mut |_| Ok(()));
    let _ = decode_published_generation::<CanonicalDecodeError>(bytes, u64::MAX, |_| Ok(()));
    let _ = decode_retention_created(bytes, u64::MAX);
    let _ = decode_workspace_deleted(bytes, u64::MAX);
    let _ = decode_generation_export_manifest(bytes, u64::MAX, u64::MAX);
    let sha256: [u8; 32] = <sha2::Sha256 as sha2::Digest>::digest(bytes).into();
    let _ = decode_generation_publication_evidence(bytes, &sha256, u64::MAX, u64::MAX);
}

/// Produce SDK encoder output; the capture executable records its provenance.
pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, FsError> {
    use acyclic_fs::{Digest, FileId, ObjectId, ObjectKind, VolumeId};
    let maximum = DecodeLimits::default().maximum_page_items;
    let mut seeds = Vec::new();
    macro_rules! page {
        ($name:literal, $encode:ident, $value:expr) => {
            seeds.push((
                $name,
                $encode(&$value, maximum, |_| Ok(()), allocation_error)?,
            ));
        };
    }
    page!(
        "files-empty",
        encode_file_table_page,
        FileTablePage::Leaf(Vec::new())
    );
    page!("tree-empty", encode_tree_page, TreePage::Leaf(Vec::new()));
    page!(
        "extents-empty",
        encode_extent_page,
        ExtentPage::Leaf(Vec::new())
    );
    page!(
        "attributes-empty",
        encode_attribute_page,
        AttributePage::Leaf(Vec::new())
    );
    page!(
        "blob-chunk",
        encode_blob_page,
        BlobPage {
            first_offset: 0,
            end_offset: 7,
            node: BlobNode::Leaf(vec![BlobChunkRef {
                first_offset: 0,
                end_offset: 7,
                chunk: ObjectId {
                    kind: ObjectKind::BlobChunk,
                    digest: Digest::from_bytes([1; 32]),
                    storage: None
                },
            }]),
        }
    );
    seeds.push((
        "metadata",
        encode_file_metadata(FileMetadata::default(), |_| Ok(()), allocation_error)?,
    ));
    seeds.push((
        "generation",
        encode_generation_root(
            &GenerationRoot {
                volume_id: VolumeId::from_bytes([1; 16]),
                root_file_id: FileId::from_bytes([2; 16]),
                file_table: ObjectId {
                    kind: ObjectKind::FileTablePage,
                    digest: Digest::from_bytes([3; 32]),
                    storage: None,
                },
                parents: Vec::new(),
                required_features: 0,
            },
            |_| Ok(()),
            allocation_error,
        )?,
    ));
    seeds.push((
        "workspace-deleted",
        encode_workspace_deleted(VolumeId::from_bytes([1; 16]), |_| Ok(()), allocation_error)?,
    ));
    Ok(seeds)
}
