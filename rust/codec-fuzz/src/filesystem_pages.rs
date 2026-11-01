//! Immutable filesystem decoders use their production admission and encoders.
use acyclic_fs::{FsError, kernel::*};
mod evidence;
mod historical;
mod populated;

#[derive(Debug)]
enum DecodeFailure {
    Canonical,
    Original,
}

impl From<CanonicalDecodeError> for DecodeFailure {
    fn from(_: CanonicalDecodeError) -> Self {
        Self::Canonical
    }
}

impl From<VolumeCreatedError> for DecodeFailure {
    fn from(_: VolumeCreatedError) -> Self {
        Self::Canonical
    }
}

macro_rules! interrupt {
    ($decode:ident, $bytes:expr, $limits:expr, $steps:expr) => {{
        assert!($steps > 0, "admitted decoding accounts for its work");
        for before in [0, $bytes.len() % $steps] {
            let mut calls = 0;
            let result = $decode::<DecodeFailure>($bytes, $limits, &mut |_| {
                let current = calls;
                calls += 1;
                if current == before {
                    Err(DecodeFailure::Original)
                } else {
                    Ok(())
                }
            });
            assert!(matches!(result, Err(DecodeFailure::Original)));
            assert_eq!(
                calls,
                before + 1,
                "the original refusal ends decoding immediately"
            );
        }
    }};
}

fn allocation_error(_: std::collections::TryReserveError) -> FsError {
    CanonicalDecodeError::AllocationFailed.into()
}

fn refuses_other_versions(mut decode: impl FnMut(&[u8]) -> Result<(), FsError>, bytes: &[u8]) {
    // These seven immutable object formats share an eight-byte domain followed
    // by a little-endian u16 version. No payload is needed to refuse a version.
    refuses_versions(bytes, 8, |header, version| {
        matches!(decode(header),
            Err(FsError::Decode(CanonicalDecodeError::UnsupportedVersion { found, expected: 1 })) if found == version)
    });
}

fn refuses_versions(
    bytes: &[u8],
    version_offset: usize,
    mut unsupported: impl FnMut(&[u8], u16) -> bool,
) {
    let mut header = bytes[..version_offset + 2].to_vec();
    for version in [0_u16, 2, u16::MAX] {
        header[version_offset..].copy_from_slice(&version.to_le_bytes());
        assert!(unsupported(&header, version));
    }
}

/// Check canonical identity and admission before any version-specific body.
pub fn check(bytes: &[u8]) {
    let limits = DecodeLimits::default();
    macro_rules! page {
        ($decode:ident, $encode:ident) => {
            let mut steps = 0;
            if let Ok(value) = $decode::<FsError>(bytes, limits, &mut |_| {
                steps += 1;
                Ok(())
            }) {
                let encoded = $encode(
                    &value,
                    limits.maximum_page_items,
                    |_| Ok(()),
                    allocation_error,
                )
                .unwrap();
                assert_eq!(encoded, bytes);
                interrupt!($decode, bytes, limits, steps);
                assert!(
                    $decode::<FsError>(&bytes[..bytes.len() - 1], limits, &mut |_| Ok(())).is_err()
                );
                let mut trailing = bytes.to_vec();
                trailing.push(0);
                assert!($decode::<FsError>(&trailing, limits, &mut |_| Ok(())).is_err());
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
    let mut steps = 0;
    if let Ok(value) = decode_file_metadata::<FsError>(bytes, limits, &mut |_| {
        steps += 1;
        Ok(())
    }) {
        interrupt!(decode_file_metadata, bytes, limits, steps);
        assert_eq!(
            encode_file_metadata(value, |_| Ok(()), allocation_error).unwrap(),
            bytes
        );
        refuses_other_versions(
            |input| decode_file_metadata::<FsError>(input, limits, &mut |_| Ok(())).map(|_| ()),
            bytes,
        );
    }
    let mut steps = 0;
    if let Ok(value) = decode_generation_root::<FsError>(bytes, limits, &mut |_| {
        steps += 1;
        Ok(())
    }) {
        interrupt!(decode_generation_root, bytes, limits, steps);
        assert_eq!(
            encode_generation_root(&value, |_| Ok(()), allocation_error).unwrap(),
            bytes
        );
        refuses_other_versions(
            |input| decode_generation_root::<FsError>(input, limits, &mut |_| Ok(())).map(|_| ()),
            bytes,
        );
    }
    let mut steps = 0;
    if let Ok(value) = decode_volume_created::<VolumeCreatedError>(bytes, u64::MAX, &mut |_| {
        steps += 1;
        Ok(())
    }) {
        interrupt!(decode_volume_created, bytes, u64::MAX, steps);
        assert_eq!(
            encode_volume_created(
                value,
                |_| Ok::<_, VolumeCreatedError>(()),
                |_error| { CanonicalDecodeError::AllocationFailed.into() }
            )
            .unwrap(),
            bytes,
        );
        refuses_versions(
            bytes,
            b"acyclic-fs-volume-created-v1\0".len(),
            |header, version| {
                matches!(decode_volume_created::<VolumeCreatedError>(header, u64::MAX, &mut |_| Ok(())),
                Err(VolumeCreatedError::Decode(CanonicalDecodeError::UnsupportedVersion { found, expected: 1 })) if found == version)
            },
        );
    }
    publication_codec_fuzz::check(bytes);
    if let Ok(value) = decode_retention_created(bytes, u64::MAX) {
        assert_eq!(
            encode_retention_created(
                &value,
                |_| Ok::<_, RetentionCreatedError>(()),
                |_error| { CanonicalDecodeError::AllocationFailed.into() }
            )
            .unwrap(),
            bytes,
        );
        refuses_versions(
            bytes,
            b"acyclic-fs-retention-created-v1\0".len(),
            |header, version| {
                matches!(decode_retention_created(header, u64::MAX),
                Err(RetentionCreatedError::Decode(CanonicalDecodeError::UnsupportedVersion { found, expected: 1 })) if found == version)
            },
        );
    }
    if let Ok(value) = decode_workspace_deleted(bytes, u64::MAX) {
        assert_eq!(
            encode_workspace_deleted(value, |_| Ok::<_, FsError>(()), allocation_error).unwrap(),
            bytes
        );
        refuses_versions(
            bytes,
            b"acyclic-fs-workspace-deleted-v1\0".len(),
            |header, version| {
                matches!(decode_workspace_deleted(header, u64::MAX),
                Err(CanonicalDecodeError::UnsupportedVersion { found, expected: 1 }) if found == version)
            },
        );
    }
    if let Ok(value) = decode_generation_export_manifest(bytes, u64::MAX, u64::MAX) {
        assert_eq!(encode_generation_export_manifest(&value).unwrap(), bytes);
        refuses_versions(
            bytes,
            b"acyclic-fs-generation-export-v1\0".len(),
            |header, version| {
                matches!(decode_generation_export_manifest(header, u64::MAX, u64::MAX),
                Err(GenerationExportManifestError::Decode(CanonicalDecodeError::UnsupportedVersion { found, expected: 1 })) if found == version)
            },
        );
    }
    evidence::check(bytes);
    historical::check(bytes);
}

/// Produce SDK encoder output; the capture executable records its provenance.
pub fn seeds() -> Result<Vec<(&'static str, Vec<u8>)>, Box<dyn std::error::Error>> {
    use acyclic_fs::{
        Digest, FileId, GenerationId, Lifecycle, ObjectId, ObjectKind, ObjectStorage, VolumeConfig,
        VolumeId,
    };
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
    let volume_id = VolumeId::from_bytes([1; 16]);
    let config = VolumeConfig::portable(Lifecycle::Durable);
    for sealed in [false, true] {
        let generation_root = ObjectId {
            kind: ObjectKind::GenerationRoot,
            digest: Digest::from_bytes([2; 32]),
            storage: sealed.then_some(ObjectStorage {
                locator: Digest::from_bytes([3; 32]),
                root_file_id: FileId::from_bytes([4; 16]),
                origin_generation: Digest::from_bytes([5; 32]),
            }),
        };
        seeds.push((
            if sealed {
                "volume-created-sealed"
            } else {
                "volume-created"
            },
            encode_volume_created(
                VolumeCreated {
                    volume_id,
                    config,
                    initial_generation_root: generation_root,
                },
                |_| Ok::<_, VolumeCreatedError>(()),
                |_error| CanonicalDecodeError::AllocationFailed.into(),
            )?,
        ));
        for (name, kind) in [
            (
                if sealed {
                    "retention-checkpoint-sealed"
                } else {
                    "retention-checkpoint"
                },
                RetentionKind::Checkpoint,
            ),
            (
                if sealed {
                    "retention-pin-sealed"
                } else {
                    "retention-pin"
                },
                RetentionKind::Pin,
            ),
            (
                if sealed {
                    "retention-fork-sealed"
                } else {
                    "retention-fork"
                },
                RetentionKind::ForkBase,
            ),
        ] {
            seeds.push((
                name,
                encode_retention_created(
                    &RetentionCreated {
                        volume_id,
                        config,
                        generation_root,
                        kind,
                        label: "retained".into(),
                    },
                    |_| Ok::<_, RetentionCreatedError>(()),
                    |_error| CanonicalDecodeError::AllocationFailed.into(),
                )?,
            ));
        }
        seeds.push((
            if sealed { "export-sealed" } else { "export" },
            encode_generation_export_manifest(&GenerationExportManifest {
                volume_id,
                config,
                generation_root,
                generation_id: GenerationId::from_root(generation_root),
                objects: vec![generation_root],
                file_count: 0,
            })?,
        ));
    }
    seeds.extend(populated::seeds()?);
    seeds.extend(publication_codec_fuzz::seeds()?);
    seeds.extend(historical::seeds()?);
    Ok(seeds)
}
