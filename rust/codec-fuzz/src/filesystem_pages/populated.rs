//! Populated writer outputs reach every page shape and both reference widths.
use super::*;
use acyclic_fs::{Digest, FileId, GenerationId, ObjectId, ObjectKind, ObjectStorage, VolumeId};

type Seeds = Vec<(&'static str, Vec<u8>)>;

pub(super) fn seeds() -> Result<Seeds, Box<dyn std::error::Error>> {
    let limits = DecodeLimits::default();
    let maximum = limits.maximum_page_items;
    let mut outputs = Vec::new();
    let kinds = [
        FileKind::Regular,
        FileKind::Directory,
        FileKind::SymbolicLink,
        FileKind::Fifo,
        FileKind::Socket,
        FileKind::CharacterDevice,
        FileKind::BlockDevice,
        FileKind::ReparsePoint,
        FileKind::MountBoundary,
    ];
    let names = [
        LogicalName::new(
            NameEncoding::Utf8,
            "café".as_bytes().to_vec(),
            limits.maximum_name_bytes,
        )?,
        LogicalName::new(
            NameEncoding::PosixBytes,
            b"name\xff".to_vec(),
            limits.maximum_name_bytes,
        )?,
        LogicalName::new(
            NameEncoding::WindowsUtf16Le,
            vec![b'n', 0],
            limits.maximum_name_bytes,
        )?,
    ];
    let mut entries = Vec::new();
    for (index, kind) in kinds.iter().copied().enumerate() {
        entries.push(TreeEntry {
            name: LogicalName::new(
                NameEncoding::Utf8,
                format!("kind-{index}").into_bytes(),
                limits.maximum_name_bytes,
            )?,
            file_id: FileId::from_bytes([u8::try_from(index + 1)?; 16]),
            kind,
        });
    }
    entries.extend(
        names
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, name)| TreeEntry {
                name,
                file_id: FileId::from_bytes([20 + index as u8; 16]),
                kind: FileKind::Regular,
            }),
    );
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    let tree = TreePage::Leaf(entries);
    let encoded = encode_tree_page(&tree, maximum, |_| Ok(()), allocation_error)?;
    assert_eq!(
        decode_tree_page::<FsError>(&encoded, limits, &mut |_| Ok(()))?,
        tree
    );
    outputs.push(("tree-populated", encoded));

    for sealed in [false, true] {
        let object = |kind, digest| ObjectId {
            kind,
            digest: Digest::from_bytes([digest; 32]),
            storage: sealed.then_some(ObjectStorage {
                locator: Digest::from_bytes([11; 32]),
                root_file_id: FileId::from_bytes([12; 16]),
                origin_generation: Digest::from_bytes([13; 32]),
            }),
        };
        macro_rules! capture {
            ($name:literal, $encode:ident, $decode:ident, $value:expr) => {{
                let value = $value;
                let bytes = $encode(&value, maximum, |_| Ok(()), allocation_error)?;
                assert_eq!($decode::<FsError>(&bytes, limits, &mut |_| Ok(()))?, value);
                outputs.push((
                    if sealed {
                        concat!($name, "-sealed")
                    } else {
                        $name
                    },
                    bytes,
                ));
            }};
        }
        let payloads = [
            (
                FileKind::Regular,
                FilePayload::InlineRegular(InlineFileData::new(b"inline")?),
            ),
            (
                FileKind::Regular,
                FilePayload::Regular {
                    logical_bytes: 15,
                    extents: object(ObjectKind::ExtentPage, 1),
                },
            ),
            (
                FileKind::Directory,
                FilePayload::Directory {
                    entries: object(ObjectKind::TreePage, 2),
                },
            ),
            (
                FileKind::SymbolicLink,
                FilePayload::SymbolicLink {
                    target_bytes: 3,
                    target: object(ObjectKind::Blob, 3),
                },
            ),
            (FileKind::Fifo, FilePayload::Empty),
            (FileKind::Socket, FilePayload::Empty),
            (
                FileKind::CharacterDevice,
                FilePayload::Device { major: 1, minor: 2 },
            ),
            (
                FileKind::BlockDevice,
                FilePayload::Device { major: 3, minor: 4 },
            ),
            (
                FileKind::ReparsePoint,
                FilePayload::ReparsePoint {
                    payload_bytes: 4,
                    payload: object(ObjectKind::Blob, 4),
                },
            ),
            (FileKind::MountBoundary, FilePayload::Empty),
        ];
        let mut records = Vec::new();
        for (index, (kind, payload)) in payloads.into_iter().enumerate() {
            records.push(FileRecord {
                file_id: FileId::from_bytes([u8::try_from(index + 1)?; 16]),
                kind,
                payload,
                link_count: 1,
                metadata: object(ObjectKind::Metadata, 5),
                bindings: None,
            });
        }
        capture!(
            "files-populated",
            encode_file_table_page,
            decode_file_table_page,
            FileTablePage::Leaf(records)
        );
        capture!(
            "files-internal",
            encode_file_table_page,
            decode_file_table_page,
            FileTablePage::Internal(vec![Child {
                first: FileId::from_bytes([1; 16]),
                page: object(ObjectKind::FileTablePage, 6)
            }])
        );
        capture!(
            "tree-internal",
            encode_tree_page,
            decode_tree_page,
            TreePage::Internal(
                names
                    .iter()
                    .cloned()
                    .map(|first| Child {
                        first,
                        page: object(ObjectKind::TreePage, 7)
                    })
                    .collect()
            )
        );
        capture!(
            "extents-populated",
            encode_extent_page,
            decode_extent_page,
            ExtentPage::Leaf(vec![
                Extent {
                    offset: 0,
                    length: 3,
                    kind: ExtentKind::Hole
                },
                Extent {
                    offset: 3,
                    length: 4,
                    kind: ExtentKind::AllocatedZero
                },
                Extent {
                    offset: 7,
                    length: 8,
                    kind: ExtentKind::Content {
                        object: object(ObjectKind::Blob, 8),
                        object_offset: 2
                    }
                },
            ])
        );
        capture!(
            "extents-internal",
            encode_extent_page,
            decode_extent_page,
            ExtentPage::Internal(vec![
                ExtentChild {
                    first: 0,
                    page: object(ObjectKind::ExtentPage, 9)
                },
                ExtentChild {
                    first: 7,
                    page: object(ObjectKind::ExtentPage, 10)
                },
            ])
        );
        let mut attributes = Vec::new();
        for class in [
            AttributeClass::PosixXattr,
            AttributeClass::WindowsStream,
            AttributeClass::MacResourceFork,
        ] {
            attributes.push(AttributeEntry {
                name: AttributeName::new(class, b"attribute".to_vec(), limits.maximum_name_bytes)?,
                value_bytes: 7,
                value: object(ObjectKind::Blob, 14),
            });
        }
        capture!(
            "attributes-internal",
            encode_attribute_page,
            decode_attribute_page,
            AttributePage::Internal(
                attributes
                    .iter()
                    .map(|entry| Child {
                        first: entry.name.clone(),
                        page: object(ObjectKind::AttributePage, 15)
                    })
                    .collect()
            )
        );
        capture!(
            "attributes-populated",
            encode_attribute_page,
            decode_attribute_page,
            AttributePage::Leaf(attributes)
        );
        capture!(
            "blob-populated",
            encode_blob_page,
            decode_blob_page,
            BlobPage {
                first_offset: 0,
                end_offset: 15,
                node: BlobNode::Leaf(vec![
                    BlobChunkRef {
                        first_offset: 0,
                        end_offset: 7,
                        chunk: object(ObjectKind::BlobChunk, 16)
                    },
                    BlobChunkRef {
                        first_offset: 7,
                        end_offset: 15,
                        chunk: object(ObjectKind::BlobChunk, 17)
                    },
                ]),
            }
        );
        capture!(
            "blob-internal",
            encode_blob_page,
            decode_blob_page,
            BlobPage {
                first_offset: 0,
                end_offset: 15,
                node: BlobNode::Internal(vec![
                    BlobChild {
                        first_offset: 0,
                        end_offset: 7,
                        page: object(ObjectKind::Blob, 18)
                    },
                    BlobChild {
                        first_offset: 7,
                        end_offset: 15,
                        page: object(ObjectKind::Blob, 19)
                    },
                ]),
            }
        );
        let metadata = FileMetadata {
            posix_mode: MetadataField::Value(0o100644),
            posix_uid: MetadataField::Value(0),
            posix_gid: MetadataField::Value(u32::MAX),
            posix_flags: MetadataField::Value(u64::MAX),
            windows_attributes: MetadataField::Value(0),
            created_ns: MetadataField::Value(i64::MIN),
            modified_ns: MetadataField::Value(-1),
            accessed_ns: MetadataField::Value(0),
            changed_ns: MetadataField::Value(i64::MAX),
            named_attributes: MetadataField::Value(object(ObjectKind::AttributePage, 20)),
            acl: MetadataField::Value(object(ObjectKind::Blob, 21)),
            security_descriptor: MetadataField::Value(object(ObjectKind::Blob, 22)),
        };
        let bytes = encode_file_metadata(metadata, |_| Ok(()), allocation_error)?;
        assert_eq!(
            decode_file_metadata::<FsError>(&bytes, limits, &mut |_| Ok(()))?,
            metadata
        );
        outputs.push((
            if sealed {
                "metadata-populated-sealed"
            } else {
                "metadata-populated"
            },
            bytes,
        ));
        for filtered in [false, true] {
            let mut parents = vec![GenerationId::from_root(object(
                ObjectKind::GenerationRoot,
                23,
            ))];
            if !filtered {
                parents.push(GenerationId::from_root(object(
                    ObjectKind::GenerationRoot,
                    24,
                )));
            }
            let generation = GenerationRoot {
                volume_id: VolumeId::from_bytes([1; 16]),
                root_file_id: FileId::from_bytes([2; 16]),
                file_table: object(ObjectKind::FileTablePage, 25),
                parents,
                required_features: if filtered {
                    GenerationRoot::FILTERED_FORK
                } else {
                    0
                },
            };
            let bytes = encode_generation_root(&generation, |_| Ok(()), allocation_error)?;
            assert_eq!(
                decode_generation_root::<FsError>(&bytes, limits, &mut |_| Ok(()))?,
                generation
            );
            outputs.push((
                match (sealed, filtered) {
                    (false, false) => "generation-parents",
                    (true, false) => "generation-parents-sealed",
                    (false, true) => "generation-filtered",
                    (true, true) => "generation-filtered-sealed",
                },
                bytes,
            ));
        }
    }
    Ok(outputs)
}
