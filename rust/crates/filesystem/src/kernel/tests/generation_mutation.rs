use super::*;
use crate::async_storage::poll_ready;
use crate::foundation::{Digest, VolumeId};
use crate::kernel::{
    BlobBuildOptions, BlobNode, ExtentKind, ExtentMutationError, ExtentPage, FileMetadata,
    FileTablePage, InlineFileData, MAXIMUM_INLINE_FILE_BYTES, NameEncoding, TreePage,
    TreeReadError, build_blob, decode_blob_page, decode_extent_page, decode_file_table_page,
    encode_file_metadata, encode_file_table_page, encode_tree_page, lookup_file_record,
    lookup_path,
};
use crate::memory::MemoryObjectStore;
use crate::model::{
    CaseSensitivity, ConcurrencyMode, FilesystemProfile, Lifecycle, UnicodePolicy, VolumeLimits,
};
use crate::storage::{ObjectId, ObjectKind, ObjectStore, object_digest};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

struct Fixture {
    generation: GenerationRoot,
    metadata: ObjectId,
    changed_metadata: ObjectId,
    empty_tree: ObjectId,
}

fn config() -> VolumeConfig {
    VolumeConfig {
        profile: FilesystemProfile::Portable,
        concurrency: ConcurrencyMode::Optimistic,
        lifecycle: Lifecycle::Ephemeral,
        case_sensitivity: CaseSensitivity::Sensitive,
        unicode: UnicodePolicy::Preserve,
        symbolic_links: true,
        hard_links: true,
        sparse_files: true,
        limits: VolumeLimits::default(),
    }
}

fn metadata_value(modified: MetadataField<i64>) -> FileMetadata {
    FileMetadata {
        posix_mode: MetadataField::Unavailable,
        posix_uid: MetadataField::Unavailable,
        posix_gid: MetadataField::Unavailable,
        posix_flags: MetadataField::Unavailable,
        windows_attributes: MetadataField::Unavailable,
        created_ns: MetadataField::Unavailable,
        modified_ns: modified,
        accessed_ns: MetadataField::Unavailable,
        changed_ns: MetadataField::Unavailable,
        named_attributes: MetadataField::Unavailable,
        acl: MetadataField::Unavailable,
        security_descriptor: MetadataField::Unavailable,
    }
}

fn put(
    store: &MemoryObjectStore,
    kind: ObjectKind,
    bytes: Vec<u8>,
) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let object = ObjectId {
        kind,
        digest: object_digest(kind, &bytes),
    };
    ObjectStore::put(store, object, Bytes::from(bytes), WorkBudget::UNBOUNDED)?;
    Ok(object)
}

fn name(value: &str) -> Result<LogicalName, Box<dyn std::error::Error>> {
    Ok(LogicalName::new(
        NameEncoding::Utf8,
        value.as_bytes().to_vec(),
        config().limits.maximum_component_bytes,
    )?)
}

fn path(values: &[&str]) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    Ok(NamespacePath::new(
        values
            .iter()
            .map(|value| name(value))
            .collect::<Result<Vec<_>, _>>()?,
        config().limits,
    )?)
}

fn regular(file_id: FileId, metadata: ObjectId) -> Result<FileRecord, Box<dyn std::error::Error>> {
    Ok(FileRecord {
        file_id,
        kind: FileKind::Regular,
        link_count: 1,
        metadata,
        payload: FilePayload::InlineRegular(InlineFileData::new(b"payload")?),
    })
}

fn blob(store: &MemoryObjectStore, bytes: &[u8]) -> Result<ObjectId, Box<dyn std::error::Error>> {
    let mut source = std::io::Cursor::new(bytes);
    Ok(build_blob(
        store,
        &mut source,
        BlobBuildOptions {
            chunk_bytes: 64,
            page_items: 8,
            page_bytes: 4096,
            maximum_blob_bytes: u64::try_from(bytes.len())?,
        },
        WorkBudget::UNBOUNDED,
    )?
    .root)
}

fn inline_at(
    store: &MemoryObjectStore,
    generation: &GenerationRoot,
    value: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let record = lookup_path(
        store,
        generation,
        &path(&[value])?,
        config(),
        WorkBudget::UNBOUNDED,
    )?
    .record
    .ok_or("expected regular-file path is absent")?;
    let FilePayload::InlineRegular(data) = record.payload else {
        return Err("expected inline regular-file payload".into());
    };
    Ok(data.as_bytes().to_vec())
}

fn fixture(store: &MemoryObjectStore) -> Result<Fixture, Box<dyn std::error::Error>> {
    let limits = config().limits;
    let metadata = put(
        store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata_value(MetadataField::Value(1)))?,
    )?;
    let changed_metadata = put(
        store,
        ObjectKind::Metadata,
        encode_file_metadata(metadata_value(MetadataField::Value(2)))?,
    )?;
    let empty_tree = put(
        store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(Vec::new()),
            limits.maximum_directory_page_entries,
        )?,
    )?;
    let root_id = FileId::from_bytes([1; 16]);
    let file_id = FileId::from_bytes([2; 16]);
    let directory_id = FileId::from_bytes([3; 16]);
    let root_tree = put(
        store,
        ObjectKind::TreePage,
        encode_tree_page(
            &TreePage::Leaf(vec![
                TreeEntry {
                    name: name("a")?,
                    file_id,
                    kind: FileKind::Regular,
                },
                TreeEntry {
                    name: name("dir")?,
                    file_id: directory_id,
                    kind: FileKind::Directory,
                },
            ]),
            limits.maximum_directory_page_entries,
        )?,
    )?;
    let file_table = put(
        store,
        ObjectKind::FileTablePage,
        encode_file_table_page(
            &FileTablePage::Leaf(vec![
                FileRecord {
                    file_id: root_id,
                    kind: FileKind::Directory,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::Directory { entries: root_tree },
                },
                regular(file_id, metadata)?,
                FileRecord {
                    file_id: directory_id,
                    kind: FileKind::Directory,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::Directory {
                        entries: empty_tree,
                    },
                },
            ]),
            limits.maximum_directory_page_entries,
        )?,
    )?;
    Ok(Fixture {
        generation: GenerationRoot {
            volume_id: VolumeId::from_bytes([9; 16]),
            root_file_id: root_id,
            file_table,
            parents: Vec::new(),
            required_features: 0,
        },
        metadata,
        changed_metadata,
        empty_tree,
    })
}

fn missing_file_table_generation(fixture: &Fixture) -> GenerationRoot {
    GenerationRoot {
        file_table: ObjectId {
            kind: ObjectKind::FileTablePage,
            digest: Digest::from_bytes([0xff; 32]),
        },
        ..fixture.generation.clone()
    }
}

#[derive(Clone, Debug)]
struct GenerationModel {
    bindings: BTreeMap<String, FileId>,
    bytes: BTreeMap<FileId, Vec<u8>>,
    identities_seen: BTreeSet<FileId>,
    metadata: ObjectId,
}

impl GenerationModel {
    fn fixture(metadata: ObjectId) -> Self {
        Self {
            bindings: BTreeMap::from([("a".to_owned(), FileId::from_bytes([2; 16]))]),
            bytes: BTreeMap::from([(FileId::from_bytes([2; 16]), b"payload".to_vec())]),
            identities_seen: BTreeSet::from([FileId::from_bytes([2; 16])]),
            metadata,
        }
    }

    fn prune(&mut self, file_id: FileId) {
        if !self
            .bindings
            .values()
            .any(|candidate| *candidate == file_id)
        {
            self.bytes.remove(&file_id);
        }
    }

    fn link_count(&self, file_id: FileId) -> u64 {
        u64::try_from(
            self.bindings
                .values()
                .filter(|candidate| **candidate == file_id)
                .count(),
        )
        .unwrap_or(u64::MAX)
    }
}

fn generated_next(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(2_862_933_555_777_941_757)
        .wrapping_add(3_037_000_493);
    *seed
}

fn selected_name<'a>(names: &'a [String], seed: &mut u64) -> &'a str {
    let index = usize::try_from(generated_next(seed) % u64::try_from(names.len()).unwrap_or(1))
        .unwrap_or(0);
    &names[index]
}

fn append_blob_bytes(
    store: &MemoryObjectStore,
    root: ObjectId,
    output: &mut Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let encoded = ObjectStore::read(store, root, u64::MAX, WorkBudget::UNBOUNDED)?.value;
    let page = decode_blob_page(&encoded, DecodeLimits::default())?;
    match page.node {
        BlobNode::Leaf(chunks) => {
            for chunk in chunks {
                let bytes =
                    ObjectStore::read(store, chunk.chunk, u64::MAX, WorkBudget::UNBOUNDED)?.value;
                output.extend_from_slice(&bytes);
            }
        }
        BlobNode::Internal(children) => {
            for child in children {
                append_blob_bytes(store, child.page, output)?;
            }
        }
    }
    Ok(())
}

fn collect_extents(
    store: &MemoryObjectStore,
    root: ObjectId,
    output: &mut Vec<crate::kernel::Extent>,
) -> Result<(), Box<dyn std::error::Error>> {
    let encoded = ObjectStore::read(store, root, u64::MAX, WorkBudget::UNBOUNDED)?.value;
    match decode_extent_page(&encoded, DecodeLimits::default())? {
        ExtentPage::Leaf(extents) => output.extend(extents),
        ExtentPage::Internal(children) => {
            for child in children {
                collect_extents(store, child.page, output)?;
            }
        }
    }
    Ok(())
}

fn record_bytes(
    store: &MemoryObjectStore,
    record: FileRecord,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    match record.payload {
        FilePayload::InlineRegular(data) => Ok(data.as_bytes().to_vec()),
        FilePayload::Regular {
            logical_bytes,
            extents,
        } => {
            let mut output = vec![0; usize::try_from(logical_bytes)?];
            let mut decoded = Vec::new();
            collect_extents(store, extents, &mut decoded)?;
            for extent in decoded {
                if let ExtentKind::Content {
                    object,
                    object_offset,
                } = extent.kind
                {
                    let mut blob = Vec::new();
                    append_blob_bytes(store, object, &mut blob)?;
                    let source_start = usize::try_from(object_offset)?;
                    let source_end = source_start
                        .checked_add(usize::try_from(extent.length)?)
                        .ok_or("modeled extent source overflow")?;
                    let destination_start = usize::try_from(extent.offset)?;
                    let destination_end = destination_start
                        .checked_add(usize::try_from(extent.length)?)
                        .ok_or("modeled extent destination overflow")?;
                    output[destination_start..destination_end]
                        .copy_from_slice(&blob[source_start..source_end]);
                }
            }
            Ok(output)
        }
        _ => Err("modeled identity stopped being a regular file".into()),
    }
}

fn collect_file_table_records(
    store: &MemoryObjectStore,
    root: ObjectId,
    records: &mut BTreeMap<FileId, FileRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    let encoded = ObjectStore::read(store, root, u64::MAX, WorkBudget::UNBOUNDED)?.value;
    match decode_file_table_page(&encoded, DecodeLimits::default())? {
        FileTablePage::Leaf(values) => {
            for record in values {
                if records.insert(record.file_id, record).is_some() {
                    return Err("file table repeated one stable identity".into());
                }
            }
        }
        FileTablePage::Internal(children) => {
            for child in children {
                collect_file_table_records(store, child.page, records)?;
            }
        }
    }
    Ok(())
}

fn assert_generation_matches_model(
    store: &MemoryObjectStore,
    generation: &GenerationRoot,
    model: &GenerationModel,
    names: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(generation.root_file_id, FileId::from_bytes([1; 16]));
    for candidate in names {
        let found = lookup_path(
            store,
            generation,
            &path(&[candidate])?,
            config(),
            WorkBudget::UNBOUNDED,
        )?
        .record;
        match (model.bindings.get(candidate), found) {
            (None, None) => {}
            (Some(expected), Some(record)) => {
                assert_eq!(record.file_id, *expected, "binding {candidate}");
                assert_eq!(record.kind, FileKind::Regular);
                assert_eq!(record.metadata, model.metadata);
                assert_eq!(record.link_count, model.link_count(*expected));
                if model.bytes[expected].len() > MAXIMUM_INLINE_FILE_BYTES {
                    assert!(matches!(record.payload, FilePayload::Regular { .. }));
                }
                assert_eq!(record_bytes(store, record)?, model.bytes[expected]);
            }
            state => return Err(format!("binding {candidate} diverged: {state:?}").into()),
        }
    }
    for file_id in &model.identities_seen {
        let found = lookup_file_record(
            store,
            generation.file_table,
            *file_id,
            DecodeLimits::default(),
            WorkBudget::UNBOUNDED,
        )?
        .record;
        match (model.bytes.get(file_id), found) {
            (None, None) => {}
            (Some(expected), Some(record)) => {
                assert_eq!(record.kind, FileKind::Regular);
                assert_eq!(record.metadata, model.metadata);
                assert_eq!(record.link_count, model.link_count(*file_id));
                if expected.len() > MAXIMUM_INLINE_FILE_BYTES {
                    assert!(matches!(record.payload, FilePayload::Regular { .. }));
                }
                assert_eq!(record_bytes(store, record)?, *expected);
            }
            state => return Err(format!("identity {file_id:?} diverged: {state:?}").into()),
        }
    }
    let mut actual_records = BTreeMap::new();
    collect_file_table_records(store, generation.file_table, &mut actual_records)?;
    let expected_ids = model
        .bytes
        .keys()
        .copied()
        .chain([FileId::from_bytes([1; 16]), FileId::from_bytes([3; 16])])
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual_records.keys().copied().collect::<BTreeSet<_>>(),
        expected_ids
    );
    Ok(())
}

#[test]
fn malformed_file_table_preserves_nested_failure_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let generation = missing_file_table_generation(&fixture);
    let cases = [
        Mutation::Create {
            path: path(&["new"])?,
            record: regular(FileId::from_bytes([40; 16]), fixture.metadata)?,
        },
        Mutation::File {
            file_id: FileId::from_bytes([2; 16]),
            mutation: FileMutation::Resize { logical_bytes: 0 },
        },
        Mutation::Rename {
            source: path(&["a"])?,
            destination: path(&["renamed"])?,
            replace: false,
        },
    ];
    for mutation in cases {
        let failure = apply_generation_mutations(
            &store,
            &generation,
            vec![mutation],
            config(),
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("malformed file table mutation unexpectedly succeeded")?;
        assert!(matches!(
            failure.error,
            GenerationMutationError::FileRecord(_) | GenerationMutationError::Path(_)
        ));
        assert!(failure.work.backend_read_operations > 0);
        assert_eq!(failure.work.backend_write_operations, 0);
    }
    Ok(())
}

#[test]
fn mixed_namespace_batch_uses_candidate_state_and_preserves_base()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let new_id = FileId::from_bytes([4; 16]);
    let receipt = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![
            Mutation::Create {
                path: path(&["new"])?,
                record: regular(new_id, fixture.metadata)?,
            },
            Mutation::Rename {
                source: path(&["a"])?,
                destination: path(&["renamed"])?,
                replace: false,
            },
            Mutation::Link {
                source: path(&["renamed"])?,
                destination: path(&["alias"])?,
            },
            Mutation::SetMetadata {
                path: path(&["alias"])?,
                metadata: fixture.changed_metadata,
            },
            Mutation::Remove {
                path: path(&["alias"])?,
                expected_file_id: MetadataField::Unavailable,
            },
            Mutation::Remove {
                path: path(&["dir"])?,
                expected_file_id: MetadataField::Value(FileId::from_bytes([3; 16])),
            },
        ],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    let candidate = receipt.root.clone();
    let found_new = lookup_path(
        &store,
        &candidate,
        &path(&["new"])?,
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(found_new.record.map(|record| record.file_id), Some(new_id));
    let renamed = lookup_path(
        &store,
        &candidate,
        &path(&["renamed"])?,
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(renamed.record.map(|record| record.link_count), Some(1));
    assert_eq!(
        renamed.record.map(|record| record.metadata),
        Some(fixture.changed_metadata)
    );
    for absent in ["a", "alias", "dir"] {
        assert_eq!(
            lookup_path(
                &store,
                &candidate,
                &path(&[absent])?,
                config(),
                WorkBudget::UNBOUNDED,
            )?
            .record,
            None
        );
    }
    assert!(
        lookup_path(
            &store,
            &fixture.generation,
            &path(&["a"])?,
            config(),
            WorkBudget::UNBOUNDED,
        )?
        .record
        .is_some()
    );
    assert_eq!(
        lookup_path(
            &store,
            &fixture.generation,
            &path(&["new"])?,
            config(),
            WorkBudget::UNBOUNDED,
        )?
        .record,
        None
    );
    assert!(receipt.work.backend_write_operations > 0);
    Ok(())
}

#[test]
fn nested_create_uses_new_parent_without_scanning_other_subtrees()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let parent_id = FileId::from_bytes([4; 16]);
    let child_id = FileId::from_bytes([5; 16]);
    let parent = FileRecord {
        file_id: parent_id,
        kind: FileKind::Directory,
        link_count: 1,
        metadata: fixture.metadata,
        payload: FilePayload::Directory {
            entries: fixture.empty_tree,
        },
    };
    let receipt = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![
            Mutation::Create {
                path: path(&["parent"])?,
                record: parent,
            },
            Mutation::Create {
                path: path(&["parent", "child"])?,
                record: regular(child_id, fixture.metadata)?,
            },
        ],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    let candidate = receipt.root.clone();
    assert_eq!(
        lookup_path(
            &store,
            &candidate,
            &path(&["parent", "child"])?,
            config(),
            WorkBudget::UNBOUNDED,
        )?
        .record
        .map(|record| record.file_id),
        Some(child_id)
    );
    assert!(receipt.work.page_reads < 20);
    Ok(())
}

#[test]
fn content_executes_and_identity_rejections_precede_candidate_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let mut source = std::io::Cursor::new(b"X");
    let content = build_blob(
        &store,
        &mut source,
        BlobBuildOptions {
            chunk_bytes: 64,
            page_items: 8,
            page_bytes: 4096,
            maximum_blob_bytes: 1,
        },
        WorkBudget::UNBOUNDED,
    )?
    .root;
    let content_receipt = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::Write {
            path: path(&["a"])?,
            offset: 0,
            length: 1,
            content,
            content_offset: 0,
        }],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    let content_record = lookup_path(
        &store,
        &content_receipt.root,
        &path(&["a"])?,
        config(),
        WorkBudget::UNBOUNDED,
    )?
    .record
    .ok_or("content-mutated file disappeared")?;
    let FilePayload::InlineRegular(data) = content_record.payload else {
        return Err("small content mutation unexpectedly promoted".into());
    };
    assert_eq!(data.as_bytes(), b"Xayload");
    assert!(content_receipt.work.backend_write_operations > 0);

    let collision = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::Create {
            path: path(&["collision"])?,
            record: regular(FileId::from_bytes([2; 16]), fixture.metadata)?,
        }],
        config(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("created identity collision unexpectedly succeeded")?;
    assert!(matches!(
        collision.error,
        GenerationMutationError::FileIdentityAlreadyExists
    ));
    assert_eq!(collision.work.backend_write_operations, 0);
    Ok(())
}

#[test]
fn mixed_content_batch_observes_candidate_paths_and_hard_link_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let x = blob(&store, b"X")?;
    let y = blob(&store, b"Y")?;
    let z = blob(&store, b"Z")?;
    let destination_id = FileId::from_bytes([4; 16]);
    let mut destination = regular(destination_id, fixture.metadata)?;
    destination.payload = FilePayload::InlineRegular(InlineFileData::new(b"0000000")?);
    let receipt = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![
            Mutation::Create {
                path: path(&["b"])?,
                record: destination,
            },
            Mutation::Write {
                path: path(&["b"])?,
                offset: 0,
                length: 1,
                content: x,
                content_offset: 0,
            },
            Mutation::Rename {
                source: path(&["a"])?,
                destination: path(&["renamed"])?,
                replace: false,
            },
            Mutation::Write {
                path: path(&["renamed"])?,
                offset: 0,
                length: 1,
                content: y,
                content_offset: 0,
            },
            Mutation::Link {
                source: path(&["renamed"])?,
                destination: path(&["alias"])?,
            },
            Mutation::Write {
                path: path(&["alias"])?,
                offset: 1,
                length: 1,
                content: z,
                content_offset: 0,
            },
            Mutation::CloneRange {
                source: path(&["renamed"])?,
                source_offset: 0,
                destination: path(&["b"])?,
                destination_offset: 2,
                length: 2,
            },
        ],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(inline_at(&store, &receipt.root, "renamed")?, b"YZyload");
    assert_eq!(inline_at(&store, &receipt.root, "alias")?, b"YZyload");
    assert_eq!(inline_at(&store, &receipt.root, "b")?, b"X0YZ000");
    assert_eq!(inline_at(&store, &fixture.generation, "a")?, b"payload");
    Ok(())
}

#[test]
fn identity_mutation_observes_same_batch_link_count_transitions()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let file_id = FileId::from_bytes([2; 16]);
    let replacement = blob(&store, b"X")?;

    let surviving_link = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![
            Mutation::Link {
                source: path(&["a"])?,
                destination: path(&["alias"])?,
            },
            Mutation::Remove {
                path: path(&["a"])?,
                expected_file_id: MetadataField::Value(file_id),
            },
            Mutation::File {
                file_id,
                mutation: FileMutation::Write {
                    offset: 0,
                    length: 1,
                    content: replacement,
                    content_offset: 0,
                },
            },
        ],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(
        inline_at(&store, &surviving_link.root, "alias")?,
        b"Xayload"
    );
    assert!(
        lookup_path(
            &store,
            &surviving_link.root,
            &path(&["a"])?,
            config(),
            WorkBudget::UNBOUNDED,
        )?
        .record
        .is_none()
    );

    let final_unlink = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![
            Mutation::Remove {
                path: path(&["a"])?,
                expected_file_id: MetadataField::Value(file_id),
            },
            Mutation::File {
                file_id,
                mutation: FileMutation::Write {
                    offset: 0,
                    length: 1,
                    content: replacement,
                    content_offset: 0,
                },
            },
        ],
        config(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("identity mutation resurrected a final-unlinked file")?;
    assert!(matches!(
        final_unlink.error,
        GenerationMutationError::MissingSource
    ));
    assert_eq!(inline_at(&store, &fixture.generation, "a")?, b"payload");
    Ok(())
}

#[test]
fn identity_clone_uses_two_ids_with_a_one_operation_batch_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let destination_id = FileId::from_bytes([44; 16]);
    let mut one_operation = config();
    one_operation.limits.maximum_mutations_per_batch = 1;
    let created = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::Create {
            path: path(&["destination"])?,
            record: regular(destination_id, fixture.metadata)?,
        }],
        one_operation,
        WorkBudget::UNBOUNDED,
    )?;
    let cloned = apply_generation_mutations(
        &store,
        &created.root,
        vec![Mutation::CloneFileRange {
            source_file_id: FileId::from_bytes([2; 16]),
            source_offset: 0,
            destination_file_id: destination_id,
            destination_offset: 0,
            length: 7,
        }],
        one_operation,
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(inline_at(&store, &cloned.root, "destination")?, b"payload");
    Ok(())
}

#[test]
fn rename_over_same_hard_link_identity_is_a_noop() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let linked = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::Link {
            source: path(&["a"])?,
            destination: path(&["alias"])?,
        }],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    let renamed = apply_generation_mutations(
        &store,
        &linked.root,
        vec![Mutation::Rename {
            source: path(&["a"])?,
            destination: path(&["alias"])?,
            replace: true,
        }],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    let original = lookup_path(
        &store,
        &renamed.root,
        &path(&["a"])?,
        config(),
        WorkBudget::UNBOUNDED,
    )?
    .record
    .ok_or("same-identity rename removed source alias")?;
    let alias = lookup_path(
        &store,
        &renamed.root,
        &path(&["alias"])?,
        config(),
        WorkBudget::UNBOUNDED,
    )?
    .record
    .ok_or("same-identity rename removed destination alias")?;
    assert_eq!(original.file_id, alias.file_id);
    assert_eq!(original.link_count, 2);
    assert_eq!(alias.link_count, 2);
    Ok(())
}

#[test]
fn overlapping_same_file_clone_uses_preoperation_snapshot() -> Result<(), Box<dyn std::error::Error>>
{
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let receipt = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::CloneRange {
            source: path(&["a"])?,
            source_offset: 0,
            destination: path(&["a"])?,
            destination_offset: 2,
            length: 4,
        }],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    assert_eq!(inline_at(&store, &receipt.root, "a")?, b"papayld");
    assert!(receipt.work.bytes_copied >= 4);
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn semantic_capabilities_and_create_identity_reject_before_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let mut disabled = config();
    disabled.symbolic_links = false;
    let target = blob(&store, b"target")?;
    let symbolic = FileRecord {
        file_id: FileId::from_bytes([31; 16]),
        kind: FileKind::SymbolicLink,
        link_count: 1,
        metadata: fixture.metadata,
        payload: FilePayload::SymbolicLink {
            target_bytes: 6,
            target,
        },
    };
    let symbolic_failure = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::Create {
            path: path(&["symbolic"])?,
            record: symbolic,
        }],
        disabled,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("disabled symbolic link unexpectedly succeeded")?;
    assert!(matches!(
        symbolic_failure.error,
        GenerationMutationError::UnsupportedSymbolicLink
    ));
    assert_eq!(*symbolic_failure.work, WorkCounters::default());

    let fifo = FileRecord {
        file_id: FileId::from_bytes([35; 16]),
        kind: FileKind::Fifo,
        link_count: 1,
        metadata: fixture.metadata,
        payload: FilePayload::Empty,
    };
    let profile_failure = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::Create {
            path: path(&["fifo"])?,
            record: fifo,
        }],
        config(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("portable FIFO unexpectedly succeeded")?;
    assert!(matches!(
        profile_failure.error,
        GenerationMutationError::UnsupportedFileKind
    ));
    assert_eq!(*profile_failure.work, WorkCounters::default());

    let mut invalid_links = regular(FileId::from_bytes([32; 16]), fixture.metadata)?;
    invalid_links.link_count = 2;
    let link_failure = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::Create {
            path: path(&["invalid-links"])?,
            record: invalid_links,
        }],
        config(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("invalid initial link count unexpectedly succeeded")?;
    assert!(matches!(
        link_failure.error,
        GenerationMutationError::InvalidInitialLinkCount
    ));
    assert_eq!(*link_failure.work, WorkCounters::default());

    let mut dense = config();
    dense.sparse_files = false;
    let sparse_failure = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::ZeroRange {
            path: path(&["a"])?,
            offset: 0,
            length: 1,
            allocated: false,
            extend: false,
        }],
        dense,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("disabled sparse mutation unexpectedly succeeded")?;
    assert!(matches!(
        sparse_failure.error,
        GenerationMutationError::UnsupportedSparseMutation
    ));
    assert_eq!(*sparse_failure.work, WorkCounters::default());

    let mut one_path = config();
    one_path.limits.maximum_paths_per_batch = 1;
    let path_failure = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::Rename {
            source: path(&["a"])?,
            destination: path(&["renamed"])?,
            replace: false,
        }],
        one_path,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("excessive endpoint count unexpectedly succeeded")?;
    assert!(matches!(
        path_failure.error,
        GenerationMutationError::TooManyPaths
    ));
    assert_eq!(*path_failure.work, WorkCounters::default());
    Ok(())
}

#[test]
fn removing_directory_after_candidate_child_insert_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let failure = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![
            Mutation::Create {
                path: path(&["dir", "child"])?,
                record: regular(FileId::from_bytes([4; 16]), fixture.metadata)?,
            },
            Mutation::Remove {
                path: path(&["dir"])?,
                expected_file_id: MetadataField::Unavailable,
            },
        ],
        config(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("non-empty candidate directory removal unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        GenerationMutationError::DirectoryNotEmpty
    ));
    assert!(failure.work.backend_write_operations > 0);
    assert!(
        lookup_path(
            &store,
            &fixture.generation,
            &path(&["dir"])?,
            config(),
            WorkBudget::UNBOUNDED,
        )?
        .record
        .is_some()
    );
    Ok(())
}

#[test]
fn disabled_hard_links_reject_before_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let mut disabled = config();
    disabled.hard_links = false;
    let failure = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::Link {
            source: path(&["a"])?,
            destination: path(&["alias"])?,
        }],
        disabled,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("disabled hard link unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        GenerationMutationError::UnsupportedHardLink
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    assert!(
        lookup_path(
            &store,
            &fixture.generation,
            &path(&["a"])?,
            config(),
            WorkBudget::UNBOUNDED,
        )?
        .record
        .is_some()
    );
    assert!(
        lookup_path(
            &store,
            &fixture.generation,
            &path(&["alias"])?,
            config(),
            WorkBudget::UNBOUNDED,
        )?
        .record
        .is_none()
    );
    Ok(())
}

#[test]
fn rename_replace_drops_the_destination_identity_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let replaced_id = FileId::from_bytes([4; 16]);
    let receipt = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![
            Mutation::Create {
                path: path(&["b"])?,
                record: regular(replaced_id, fixture.metadata)?,
            },
            Mutation::Rename {
                source: path(&["a"])?,
                destination: path(&["b"])?,
                replace: true,
            },
        ],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    assert!(receipt.work.backend_write_operations > 0);
    assert!(
        lookup_path(
            &store,
            &receipt.root,
            &path(&["a"])?,
            config(),
            WorkBudget::UNBOUNDED,
        )?
        .record
        .is_none()
    );
    let replacement = lookup_path(
        &store,
        &receipt.root,
        &path(&["b"])?,
        config(),
        WorkBudget::UNBOUNDED,
    )?
    .record
    .ok_or("renamed destination is absent")?;
    assert_eq!(replacement.file_id, FileId::from_bytes([2; 16]));
    assert_eq!(replacement.link_count, 1);
    assert_eq!(inline_at(&store, &receipt.root, "b")?, b"payload");
    assert!(
        lookup_file_record(
            &store,
            receipt.root.file_table,
            replaced_id,
            decode_limits(config()),
            WorkBudget::UNBOUNDED,
        )?
        .record
        .is_none()
    );
    assert_eq!(inline_at(&store, &fixture.generation, "a")?, b"payload");
    Ok(())
}

#[test]
fn directory_rename_below_itself_is_rejected_without_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let failure = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::Rename {
            source: path(&["dir"])?,
            destination: path(&["dir", "child"])?,
            replace: false,
        }],
        config(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("directory cycle unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        GenerationMutationError::DirectoryCycle
    ));
    assert_eq!(failure.work.backend_write_operations, 0);
    assert_ne!(*failure.work, WorkCounters::default());
    assert!(
        lookup_path(
            &store,
            &fixture.generation,
            &path(&["dir"])?,
            config(),
            WorkBudget::UNBOUNDED,
        )?
        .record
        .is_some()
    );
    Ok(())
}

#[allow(clippy::too_many_lines)]
#[test]
fn namespace_and_identity_preconditions_fail_with_exact_terminal_errors()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let created_id = FileId::from_bytes([40; 16]);
    let cases = vec![
        (
            vec![Mutation::Create {
                path: path(&["a", "child"])?,
                record: regular(created_id, fixture.metadata)?,
            }],
            GenerationMutationError::Path(PathLookupError::NotDirectory),
        ),
        (
            vec![Mutation::Create {
                path: path(&["missing", "child"])?,
                record: regular(created_id, fixture.metadata)?,
            }],
            GenerationMutationError::MissingParent,
        ),
        (
            vec![Mutation::Create {
                path: path(&["a"])?,
                record: regular(created_id, fixture.metadata)?,
            }],
            GenerationMutationError::AlreadyExists,
        ),
        (
            vec![Mutation::Remove {
                path: path(&["missing"])?,
                expected_file_id: MetadataField::Unavailable,
            }],
            GenerationMutationError::MissingSource,
        ),
        (
            vec![Mutation::Remove {
                path: path(&["a"])?,
                expected_file_id: MetadataField::Value(FileId::from_bytes([99; 16])),
            }],
            GenerationMutationError::FileIdentityConflict,
        ),
        (
            vec![Mutation::Rename {
                source: path(&["missing"])?,
                destination: path(&["renamed"])?,
                replace: false,
            }],
            GenerationMutationError::MissingSource,
        ),
        (
            vec![Mutation::Rename {
                source: path(&["a"])?,
                destination: path(&["dir"])?,
                replace: false,
            }],
            GenerationMutationError::AlreadyExists,
        ),
        (
            vec![Mutation::Link {
                source: path(&["dir"])?,
                destination: path(&["directory-link"])?,
            }],
            GenerationMutationError::UnsupportedHardLink,
        ),
        (
            vec![Mutation::Link {
                source: path(&["missing"])?,
                destination: path(&["link"])?,
            }],
            GenerationMutationError::MissingSource,
        ),
        (
            vec![Mutation::Link {
                source: path(&["a"])?,
                destination: path(&["dir"])?,
            }],
            GenerationMutationError::AlreadyExists,
        ),
    ];
    for (operations, expected) in cases {
        let failure = apply_generation_mutations(
            &store,
            &fixture.generation,
            operations,
            config(),
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("invalid namespace operation unexpectedly succeeded")?;
        assert_eq!(
            std::mem::discriminant(&failure.error),
            std::mem::discriminant(&expected)
        );
        assert_eq!(failure.work.backend_write_operations, 0);
    }

    let duplicate = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![
            Mutation::Create {
                path: path(&["first"])?,
                record: regular(created_id, fixture.metadata)?,
            },
            Mutation::Create {
                path: path(&["second"])?,
                record: regular(created_id, fixture.metadata)?,
            },
        ],
        config(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("duplicate created identity unexpectedly succeeded")?;
    assert!(matches!(
        duplicate.error,
        GenerationMutationError::FileIdentityAlreadyExists
    ));
    assert_eq!(duplicate.work.backend_write_operations, 0);

    let missing_identity = FileId::from_bytes([41; 16]);
    let identity = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::File {
            file_id: missing_identity,
            mutation: FileMutation::Resize { logical_bytes: 0 },
        }],
        config(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("missing identity mutation unexpectedly succeeded")?;
    assert!(matches!(
        identity.error,
        GenerationMutationError::MissingSource
    ));
    assert_eq!(identity.work.backend_write_operations, 0);
    let identity_clone = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::CloneFileRange {
            source_file_id: FileId::from_bytes([2; 16]),
            source_offset: 0,
            destination_file_id: missing_identity,
            destination_offset: 0,
            length: 1,
        }],
        config(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("missing identity clone unexpectedly succeeded")?;
    assert!(matches!(
        identity_clone.error,
        GenerationMutationError::MissingSource
    ));
    assert_eq!(identity_clone.work.backend_write_operations, 0);

    for mutation in [
        Mutation::ValidateRegular {
            path: path(&["dir"])?,
        },
        Mutation::File {
            file_id: FileId::from_bytes([3; 16]),
            mutation: FileMutation::ValidateRegular,
        },
        Mutation::CloneFileRange {
            source_file_id: FileId::from_bytes([3; 16]),
            source_offset: 0,
            destination_file_id: FileId::from_bytes([2; 16]),
            destination_offset: 0,
            length: 1,
        },
        Mutation::CloneFileRange {
            source_file_id: FileId::from_bytes([2; 16]),
            source_offset: 0,
            destination_file_id: FileId::from_bytes([3; 16]),
            destination_offset: 0,
            length: 1,
        },
    ] {
        let failure = apply_generation_mutations(
            &store,
            &fixture.generation,
            vec![mutation],
            config(),
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("non-regular identity operation unexpectedly succeeded")?;
        assert!(matches!(
            failure.error,
            GenerationMutationError::InconsistentState
        ));
        assert_eq!(failure.work.backend_write_operations, 0);
    }

    assert_eq!(inline_at(&store, &fixture.generation, "a")?, b"payload");
    assert!(
        lookup_path(
            &store,
            &fixture.generation,
            &path(&["first"])?,
            config(),
            WorkBudget::UNBOUNDED,
        )?
        .record
        .is_none()
    );
    Ok(())
}

#[test]
fn nested_regular_identity_failures_preserve_prior_generation_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let mut budget = WorkBudget::UNBOUNDED;
    budget.bytes_copied = 0;
    for mutation in [
        Mutation::File {
            file_id: FileId::from_bytes([2; 16]),
            mutation: FileMutation::Resize { logical_bytes: 128 },
        },
        Mutation::CloneFileRange {
            source_file_id: FileId::from_bytes([2; 16]),
            source_offset: 0,
            destination_file_id: FileId::from_bytes([2; 16]),
            destination_offset: 0,
            length: 1,
        },
    ] {
        let failure = apply_generation_mutations(
            &store,
            &fixture.generation,
            vec![mutation],
            config(),
            budget,
        )
        .err()
        .ok_or("copy-limited regular mutation unexpectedly succeeded")?;
        assert!(
            matches!(
                &failure.error,
                GenerationMutationError::Regular(
                    RegularMutationError::Work(WorkError::BudgetExceeded {
                        counter: "bytes_copied",
                        observed: 1..,
                        maximum: 0,
                    }) | RegularMutationError::Extent(ExtentMutationError::Work(
                        WorkError::BudgetExceeded {
                            counter: "bytes_copied",
                            observed: 1..,
                            maximum: 0,
                        }
                    ))
                )
            ),
            "unexpected nested regular failure: {:?}",
            failure.error
        );
        assert!(failure.work.backend_read_operations > 0);
        assert_eq!(failure.work.bytes_copied, 0);
    }
    Ok(())
}

#[test]
fn ordered_path_and_identity_regular_mutations_share_one_candidate()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let file_id = FileId::from_bytes([2; 16]);
    let receipt = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![
            Mutation::ValidateRegular {
                path: path(&["a"])?,
            },
            Mutation::Resize {
                path: path(&["a"])?,
                logical_bytes: 128,
            },
            Mutation::Preallocate {
                path: path(&["a"])?,
                offset: 8,
                length: 56,
                keep_size: true,
            },
            Mutation::ZeroRange {
                path: path(&["a"])?,
                offset: 0,
                length: 1,
                allocated: true,
                extend: false,
            },
            Mutation::File {
                file_id,
                mutation: FileMutation::SetMetadata {
                    metadata: fixture.changed_metadata,
                },
            },
            Mutation::File {
                file_id,
                mutation: FileMutation::ValidateRegular,
            },
            Mutation::File {
                file_id,
                mutation: FileMutation::Preallocate {
                    offset: 64,
                    length: 64,
                    keep_size: true,
                },
            },
        ],
        config(),
        WorkBudget::UNBOUNDED,
    )?;
    let record = lookup_file_record(
        &store,
        receipt.root.file_table,
        file_id,
        decode_limits(config()),
        WorkBudget::UNBOUNDED,
    )?
    .record
    .ok_or("mutated file identity disappeared")?;
    assert_eq!(record.metadata, fixture.changed_metadata);
    let FilePayload::Regular { logical_bytes, .. } = record.payload else {
        return Err("ordered sparse operations did not preserve sparse payload".into());
    };
    assert_eq!(logical_bytes, 128);
    assert!(receipt.work.backend_write_operations > 0);
    assert_eq!(inline_at(&store, &fixture.generation, "a")?, b"payload");
    Ok(())
}

#[test]
fn every_sparse_operation_variant_rejects_before_work_when_sparse_files_are_disabled()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let file_id = FileId::from_bytes([2; 16]);
    let operations = [
        Mutation::Preallocate {
            path: path(&["a"])?,
            offset: 0,
            length: 1,
            keep_size: true,
        },
        Mutation::CloneRange {
            source: path(&["a"])?,
            source_offset: 0,
            destination: path(&["a"])?,
            destination_offset: 0,
            length: 1,
        },
        Mutation::File {
            file_id,
            mutation: FileMutation::ZeroRange {
                offset: 0,
                length: 1,
                allocated: false,
                extend: false,
            },
        },
        Mutation::File {
            file_id,
            mutation: FileMutation::Preallocate {
                offset: 0,
                length: 1,
                keep_size: true,
            },
        },
        Mutation::CloneFileRange {
            source_file_id: file_id,
            source_offset: 0,
            destination_file_id: file_id,
            destination_offset: 0,
            length: 1,
        },
    ];
    let mut dense = config();
    dense.sparse_files = false;
    for operation in operations {
        let failure = apply_generation_mutations(
            &store,
            &fixture.generation,
            vec![operation],
            dense,
            WorkBudget::UNBOUNDED,
        )
        .err()
        .ok_or("disabled sparse operation unexpectedly succeeded")?;
        assert!(matches!(
            failure.error,
            GenerationMutationError::UnsupportedSparseMutation
        ));
        assert_eq!(*failure.work, WorkCounters::default());
    }
    Ok(())
}

#[test]
fn empty_generation_batch_and_precancellation_reject_before_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store)?;
    let empty = apply_generation_mutations(
        &store,
        &fixture.generation,
        Vec::new(),
        config(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("empty generation batch unexpectedly succeeded")?;
    assert!(matches!(
        empty.error,
        GenerationMutationError::Plan(MutationPlanError::Empty)
    ));
    assert_eq!(*empty.work, WorkCounters::default());

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let failure = poll_ready(apply_generation_mutations_async(
        &store,
        &fixture.generation,
        vec![Mutation::ValidateRegular {
            path: path(&["a"])?,
        }],
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .ok_or("pre-cancelled generation mutation blocked")?
    .err()
    .ok_or("pre-cancelled generation mutation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        GenerationMutationError::Path(PathLookupError::Tree(TreeReadError::Cancelled))
    ));
    assert_eq!(*failure.work, WorkCounters::default());
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn generated_generation_histories_match_independent_namespace_and_identity_model()
-> Result<(), Box<dyn std::error::Error>> {
    let names = std::iter::once("a".to_owned())
        .chain((0..8).map(|index| format!("f{index}")))
        .collect::<Vec<_>>();
    for history in 0..96_u64 {
        let store = MemoryObjectStore::default();
        let fixture = fixture(&store)?;
        let mut generation = fixture.generation.clone();
        let mut model = GenerationModel::fixture(fixture.metadata);
        let mut seed = history + 1;
        let mut created_identity = 10_u8;
        assert_generation_matches_model(&store, &generation, &model, &names)?;

        for step in 0..40_u64 {
            let existing = model.bindings.keys().cloned().collect::<Vec<_>>();
            let absent = names
                .iter()
                .filter(|candidate| !model.bindings.contains_key(*candidate))
                .cloned()
                .collect::<Vec<_>>();
            let mut operation_kind = generated_next(&mut seed) % 8;
            if existing.is_empty() {
                operation_kind = 0;
            } else if absent.is_empty() && matches!(operation_kind, 0 | 1) {
                operation_kind = 3;
            }
            let mut next_model = model.clone();
            let operation = match operation_kind {
                0 => {
                    let destination = selected_name(&absent, &mut seed).to_owned();
                    let file_id = FileId::from_bytes([created_identity; 16]);
                    created_identity = created_identity
                        .checked_add(1)
                        .ok_or("generated identity space exhausted")?;
                    let initial = vec![
                        u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0);
                        usize::try_from(generated_next(&mut seed) % 4 + 1)?
                    ];
                    next_model.bindings.insert(destination.clone(), file_id);
                    next_model.bytes.insert(file_id, initial.clone());
                    next_model.identities_seen.insert(file_id);
                    Mutation::Create {
                        path: path(&[&destination])?,
                        record: FileRecord {
                            file_id,
                            kind: FileKind::Regular,
                            link_count: 1,
                            metadata: fixture.metadata,
                            payload: FilePayload::InlineRegular(InlineFileData::new(&initial)?),
                        },
                    }
                }
                1 => {
                    let source = selected_name(&existing, &mut seed).to_owned();
                    let destination = selected_name(&absent, &mut seed).to_owned();
                    let file_id = model.bindings[&source];
                    next_model.bindings.insert(destination.clone(), file_id);
                    Mutation::Link {
                        source: path(&[&source])?,
                        destination: path(&[&destination])?,
                    }
                }
                2 => {
                    let source = selected_name(&existing, &mut seed).to_owned();
                    let destinations = names
                        .iter()
                        .filter(|candidate| **candidate != source)
                        .cloned()
                        .collect::<Vec<_>>();
                    let destination = selected_name(&destinations, &mut seed).to_owned();
                    let source_id = model.bindings[&source];
                    let destination_id = model.bindings.get(&destination).copied();
                    if destination_id != Some(source_id) {
                        next_model.bindings.remove(&source);
                        next_model.bindings.insert(destination.clone(), source_id);
                        if let Some(replaced) = destination_id {
                            next_model.prune(replaced);
                        }
                    }
                    Mutation::Rename {
                        source: path(&[&source])?,
                        destination: path(&[&destination])?,
                        replace: destination_id.is_some(),
                    }
                }
                3 => {
                    let source = selected_name(&existing, &mut seed).to_owned();
                    let file_id = model.bindings[&source];
                    next_model.bindings.remove(&source);
                    next_model.prune(file_id);
                    Mutation::Remove {
                        path: path(&[&source])?,
                        expected_file_id: MetadataField::Value(file_id),
                    }
                }
                4 => {
                    let source = selected_name(&existing, &mut seed).to_owned();
                    let file_id = model.bindings[&source];
                    let current = next_model
                        .bytes
                        .get(&file_id)
                        .ok_or("modeled resize identity missing")?
                        .len();
                    let logical_bytes =
                        usize::try_from(generated_next(&mut seed) % u64::try_from(current + 1)?)?;
                    next_model
                        .bytes
                        .get_mut(&file_id)
                        .ok_or("modeled resize identity missing")?
                        .resize(logical_bytes, 0);
                    Mutation::Resize {
                        path: path(&[&source])?,
                        logical_bytes: u64::try_from(logical_bytes)?,
                    }
                }
                5 => {
                    let source = selected_name(&existing, &mut seed).to_owned();
                    let file_id = model.bindings[&source];
                    let current = next_model
                        .bytes
                        .get(&file_id)
                        .ok_or("modeled write identity missing")?
                        .len();
                    let maximum_offset = current.min(MAXIMUM_INLINE_FILE_BYTES - 1);
                    let offset = usize::try_from(
                        generated_next(&mut seed) % u64::try_from(maximum_offset + 1)?,
                    )?;
                    let maximum_length = (MAXIMUM_INLINE_FILE_BYTES - offset).min(4);
                    let length = usize::try_from(
                        generated_next(&mut seed) % u64::try_from(maximum_length)? + 1,
                    )?;
                    let content = (0..length)
                        .map(|_| u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0))
                        .collect::<Vec<_>>();
                    let content_root = blob(&store, &content)?;
                    let bytes = next_model
                        .bytes
                        .get_mut(&file_id)
                        .ok_or("modeled write identity disappeared")?;
                    bytes.resize(bytes.len().max(offset + length), 0);
                    bytes[offset..offset + length].copy_from_slice(&content);
                    Mutation::Write {
                        path: path(&[&source])?,
                        offset: u64::try_from(offset)?,
                        length: u64::try_from(length)?,
                        content: content_root,
                        content_offset: 0,
                    }
                }
                6 => {
                    let source = selected_name(&existing, &mut seed).to_owned();
                    let file_id = model.bindings[&source];
                    let current = next_model
                        .bytes
                        .get(&file_id)
                        .ok_or("modeled identity resize missing")?
                        .len();
                    let logical_bytes =
                        usize::try_from(generated_next(&mut seed) % u64::try_from(current + 1)?)?;
                    next_model
                        .bytes
                        .get_mut(&file_id)
                        .ok_or("modeled identity resize missing")?
                        .resize(logical_bytes, 0);
                    Mutation::File {
                        file_id,
                        mutation: FileMutation::Resize {
                            logical_bytes: u64::try_from(logical_bytes)?,
                        },
                    }
                }
                _ => {
                    let nonempty = existing
                        .iter()
                        .filter(|candidate| {
                            let file_id = model.bindings[*candidate];
                            !model.bytes[&file_id].is_empty()
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    if nonempty.is_empty() {
                        continue;
                    }
                    let source = selected_name(&nonempty, &mut seed).to_owned();
                    let destination = selected_name(&existing, &mut seed).to_owned();
                    let source_id = model.bindings[&source];
                    let destination_id = model.bindings[&destination];
                    let source_bytes = model.bytes[&source_id].clone();
                    let source_offset = usize::try_from(
                        generated_next(&mut seed) % u64::try_from(source_bytes.len())?,
                    )?;
                    let length = usize::try_from(
                        generated_next(&mut seed)
                            % u64::try_from((source_bytes.len() - source_offset).min(4))?
                            + 1,
                    )?;
                    let destination_bytes = next_model
                        .bytes
                        .get_mut(&destination_id)
                        .ok_or("modeled clone destination missing")?;
                    let maximum_destination = destination_bytes
                        .len()
                        .min(MAXIMUM_INLINE_FILE_BYTES - length);
                    let destination_offset = usize::try_from(
                        generated_next(&mut seed) % u64::try_from(maximum_destination + 1)?,
                    )?;
                    let snapshot = source_bytes[source_offset..source_offset + length].to_vec();
                    destination_bytes
                        .resize(destination_bytes.len().max(destination_offset + length), 0);
                    destination_bytes[destination_offset..destination_offset + length]
                        .copy_from_slice(&snapshot);
                    Mutation::CloneRange {
                        source: path(&[&source])?,
                        source_offset: u64::try_from(source_offset)?,
                        destination: path(&[&destination])?,
                        destination_offset: u64::try_from(destination_offset)?,
                        length: u64::try_from(length)?,
                    }
                }
            };
            let receipt = apply_generation_mutations(
                &store,
                &generation,
                vec![operation.clone()],
                config(),
                WorkBudget::UNBOUNDED,
            )
            .map_err(|error| {
                format!("history {history}, step {step}, operation {operation:?}: {error:?}")
            })?;
            generation = receipt.root;
            model = next_model;
            assert_generation_matches_model(&store, &generation, &model, &names)
                .map_err(|error| format!("history {history}, step {step}: {error}"))?;
        }

        assert_eq!(inline_at(&store, &fixture.generation, "a")?, b"payload");
    }
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn generated_multi_operation_transactions_observe_the_candidate_in_order()
-> Result<(), Box<dyn std::error::Error>> {
    let names = ["a", "x", "y", "alias", "z", "large"]
        .map(str::to_owned)
        .to_vec();
    let use_before_create_store = MemoryObjectStore::default();
    let use_before_create_fixture = fixture(&use_before_create_store)?;
    let use_before_create_id = FileId::from_bytes([13; 16]);
    let use_before_create = apply_generation_mutations(
        &use_before_create_store,
        &use_before_create_fixture.generation,
        vec![
            Mutation::File {
                file_id: use_before_create_id,
                mutation: FileMutation::ValidateRegular,
            },
            Mutation::Create {
                path: path(&["later"])?,
                record: regular(use_before_create_id, use_before_create_fixture.metadata)?,
            },
        ],
        config(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("identity use before its ordered create unexpectedly succeeded")?;
    assert!(matches!(
        use_before_create.error,
        GenerationMutationError::MissingSource
    ));
    assert_eq!(use_before_create.work.backend_write_operations, 0);
    for history in 0..128_u64 {
        let store = MemoryObjectStore::default();
        let fixture = fixture(&store)?;
        let mut seed = history + 1;
        let x_id = FileId::from_bytes([10; 16]);
        let y_id = FileId::from_bytes([11; 16]);
        let z_id = FileId::from_bytes([12; 16]);
        let large_id = FileId::from_bytes([14; 16]);
        let x_initial = (0..4)
            .map(|_| u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0))
            .collect::<Vec<_>>();
        let y_initial = (0..4)
            .map(|_| u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0))
            .collect::<Vec<_>>();
        let z_initial = (0..4)
            .map(|_| u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0))
            .collect::<Vec<_>>();
        let patch = (0..2)
            .map(|_| u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0))
            .collect::<Vec<_>>();
        let patch_root = blob(&store, &patch)?;
        let large_patch = [
            u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0),
            u8::try_from(generated_next(&mut seed) & 0xff).unwrap_or(0),
        ];
        let large_patch_root = blob(&store, &large_patch)?;
        let operations = vec![
            Mutation::Create {
                path: path(&["x"])?,
                record: FileRecord {
                    file_id: x_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata: fixture.metadata,
                    payload: FilePayload::InlineRegular(InlineFileData::new(&x_initial)?),
                },
            },
            Mutation::Create {
                path: path(&["y"])?,
                record: FileRecord {
                    file_id: y_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata: fixture.metadata,
                    payload: FilePayload::InlineRegular(InlineFileData::new(&y_initial)?),
                },
            },
            Mutation::Link {
                source: path(&["x"])?,
                destination: path(&["alias"])?,
            },
            Mutation::Write {
                path: path(&["alias"])?,
                offset: 1,
                length: 2,
                content: patch_root,
                content_offset: 0,
            },
            Mutation::Rename {
                source: path(&["x"])?,
                destination: path(&["alias"])?,
                replace: true,
            },
            Mutation::Rename {
                source: path(&["x"])?,
                destination: path(&["y"])?,
                replace: true,
            },
            Mutation::File {
                file_id: x_id,
                mutation: FileMutation::Resize { logical_bytes: 3 },
            },
            Mutation::CloneFileRange {
                source_file_id: x_id,
                source_offset: 0,
                destination_file_id: x_id,
                destination_offset: 1,
                length: 2,
            },
            Mutation::Create {
                path: path(&["z"])?,
                record: FileRecord {
                    file_id: z_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata: fixture.metadata,
                    payload: FilePayload::InlineRegular(InlineFileData::new(&z_initial)?),
                },
            },
            Mutation::CloneFileRange {
                source_file_id: x_id,
                source_offset: 0,
                destination_file_id: z_id,
                destination_offset: 1,
                length: 3,
            },
            Mutation::Create {
                path: path(&["large"])?,
                record: FileRecord {
                    file_id: large_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata: fixture.metadata,
                    payload: FilePayload::InlineRegular(InlineFileData::new(b"seed")?),
                },
            },
            Mutation::Write {
                path: path(&["large"])?,
                offset: 63,
                length: 2,
                content: large_patch_root,
                content_offset: 0,
            },
            Mutation::Remove {
                path: path(&["alias"])?,
                expected_file_id: MetadataField::Value(x_id),
            },
            Mutation::ValidateRegular {
                path: path(&["y"])?,
            },
        ];
        let receipt = apply_generation_mutations(
            &store,
            &fixture.generation,
            operations,
            config(),
            WorkBudget::UNBOUNDED,
        )
        .map_err(|error| format!("history {history}: {error:?}"))?;

        let mut expected_x = x_initial;
        expected_x[1..3].copy_from_slice(&patch);
        expected_x.truncate(3);
        let x_snapshot = expected_x[..2].to_vec();
        expected_x[1..3].copy_from_slice(&x_snapshot);
        let mut expected_z = z_initial;
        expected_z[1..4].copy_from_slice(&expected_x);
        let mut expected_large = b"seed".to_vec();
        expected_large.resize(65, 0);
        expected_large[63..65].copy_from_slice(&large_patch);
        let model = GenerationModel {
            bindings: BTreeMap::from([
                ("a".to_owned(), FileId::from_bytes([2; 16])),
                ("y".to_owned(), x_id),
                ("z".to_owned(), z_id),
                ("large".to_owned(), large_id),
            ]),
            bytes: BTreeMap::from([
                (FileId::from_bytes([2; 16]), b"payload".to_vec()),
                (x_id, expected_x),
                (z_id, expected_z),
                (large_id, expected_large),
            ]),
            identities_seen: BTreeSet::from([
                FileId::from_bytes([2; 16]),
                x_id,
                y_id,
                z_id,
                large_id,
            ]),
            metadata: fixture.metadata,
        };
        assert_generation_matches_model(&store, &receipt.root, &model, &names)
            .map_err(|error| format!("history {history}: {error}"))?;
        assert_eq!(inline_at(&store, &fixture.generation, "a")?, b"payload");
    }
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn generation_nested_work_and_allocation_boundaries_are_total()
-> Result<(), Box<dyn std::error::Error>> {
    let prior = WorkCounters {
        bytes_copied: 2,
        peak_allocation_bytes: 3,
        ..WorkCounters::default()
    };
    let nested = WorkCounters {
        bytes_hashed: 4,
        peak_allocation_bytes: 5,
        ..WorkCounters::default()
    };
    let budget = WorkBudget::UNBOUNDED;
    let remaining = nested_budget(prior, budget, 7)?;
    assert_eq!(remaining.bytes_copied, u64::MAX - 2);
    assert_eq!(remaining.peak_allocation_bytes, u64::MAX - 7);
    let merged = merge_nested(prior, nested, 7, budget)?;
    assert_eq!(merged.bytes_copied, 2);
    assert_eq!(merged.bytes_hashed, 4);
    assert_eq!(merged.peak_allocation_bytes, 12);

    let insufficient_peak = nested_budget(prior, prior, 4)
        .err()
        .ok_or("nested budget admitted unreserved live bytes")?;
    assert!(matches!(
        insufficient_peak.error,
        GenerationMutationError::Work(WorkError::Overflow)
    ));
    let merge_peak_overflow = merge_nested(
        prior,
        WorkCounters {
            peak_allocation_bytes: u64::MAX,
            ..WorkCounters::default()
        },
        1,
        budget,
    )
    .err()
    .ok_or("nested peak overflow succeeded")?;
    assert!(matches!(
        merge_peak_overflow.error,
        GenerationMutationError::Work(WorkError::Overflow)
    ));
    let merge_counter_overflow = merge_nested(
        WorkCounters {
            bytes_hashed: u64::MAX,
            ..WorkCounters::default()
        },
        WorkCounters {
            bytes_hashed: 1,
            ..WorkCounters::default()
        },
        0,
        budget,
    )
    .err()
    .ok_or("nested counter overflow succeeded")?;
    assert!(matches!(
        merge_counter_overflow.error,
        GenerationMutationError::Work(WorkError::Overflow)
    ));

    let normal = nested_failure(prior, nested, 7, GenerationMutationError::MissingSource);
    assert!(matches!(
        normal.error,
        GenerationMutationError::MissingSource
    ));
    assert_eq!(normal.work.peak_allocation_bytes, 12);
    let failure_peak_overflow = nested_failure(
        prior,
        WorkCounters {
            peak_allocation_bytes: u64::MAX,
            ..WorkCounters::default()
        },
        1,
        GenerationMutationError::MissingSource,
    );
    assert!(matches!(
        failure_peak_overflow.error,
        GenerationMutationError::Work(WorkError::Overflow)
    ));
    let failure_counter_overflow = nested_failure(
        WorkCounters {
            bytes_hashed: u64::MAX,
            ..WorkCounters::default()
        },
        WorkCounters {
            bytes_hashed: 1,
            ..WorkCounters::default()
        },
        0,
        GenerationMutationError::MissingSource,
    );
    assert!(matches!(
        failure_counter_overflow.error,
        GenerationMutationError::Work(WorkError::Overflow)
    ));

    let mut item_work = WorkCounters::default();
    charge_items(&mut item_work, 2, budget)?;
    assert_eq!(item_work.items_examined, 2);
    let exact = item_work;
    let item_rejection = charge_items(&mut item_work, 1, exact)
        .err()
        .ok_or("item budget overflow succeeded")?;
    assert!(matches!(
        item_rejection.error,
        GenerationMutationError::Work(WorkError::BudgetExceeded { .. })
    ));

    let allocation_work = WorkCounters {
        object_probes: 1,
        ..WorkCounters::default()
    };
    assert!(matches!(
        allocation_failure(AllocationError::Work(WorkError::Overflow), allocation_work).error,
        GenerationMutationError::Work(WorkError::Overflow)
    ));
    for allocation in [AllocationError::Overflow, AllocationError::ReleaseInvariant] {
        assert!(matches!(
            allocation_failure(allocation, allocation_work).error,
            GenerationMutationError::Work(WorkError::Overflow)
        ));
    }
    for allocation in [
        AllocationError::InvalidCapacity,
        AllocationError::CapacityExceeded,
        AllocationError::AllocationFailed,
    ] {
        assert!(matches!(
            allocation_failure(allocation, allocation_work).error,
            GenerationMutationError::AllocationFailed
        ));
    }
    assert!(matches!(
        path_cancelled(),
        GenerationMutationError::Path(PathLookupError::Tree(TreeReadError::Cancelled))
    ));

    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let values = reserve_exact::<u64>(3, &mut allocations, &mut work, budget)?;
    assert_eq!(values.capacity(), 3);
    assert_eq!(logical_vec_bytes(&values)?, 24);
    assert_eq!(allocations.live_bytes(), 24);
    Ok(())
}

fn defensive_transaction_state(
    paths: Vec<PathState>,
    records: Vec<RecordState>,
    directory_edits: Vec<DirectoryEdit>,
    removed_directories: Vec<FileId>,
) -> TransactionState {
    TransactionState {
        paths,
        operation_paths: Vec::new(),
        records,
        directory_edits,
        removed_directories,
        allocations: AllocationLedger::default(),
        work: WorkCounters::default(),
        budget: WorkBudget::UNBOUNDED,
        config: config(),
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn defensive_transaction_invariants_fail_closed_before_backend_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let file_id = FileId::from_bytes([91; 16]);
    let metadata = ObjectId {
        kind: ObjectKind::Metadata,
        digest: Digest::ZERO,
    };
    let zero_link = FileRecord {
        file_id,
        kind: FileKind::Fifo,
        link_count: 0,
        metadata,
        payload: FilePayload::Empty,
    };
    let parent_path = PathState {
        operation: 0,
        endpoint: 0,
        binding: Some(Binding {
            file_id,
            kind: FileKind::Fifo,
        }),
        base_parent: None,
        base_parent_exact: false,
        parent_state: None,
    };
    let child_path = PathState {
        operation: 0,
        endpoint: 1,
        binding: None,
        base_parent: None,
        base_parent_exact: false,
        parent_state: Some(0),
    };
    let record = RecordState {
        base: Some(zero_link),
        working: zero_link,
        present: true,
    };

    let mut invalid_parent = defensive_transaction_state(
        vec![parent_path, child_path],
        vec![record],
        Vec::new(),
        Vec::new(),
    );
    assert!(matches!(
        invalid_parent
            .parent_directory(1)
            .map_err(|failure| failure.error),
        Err(GenerationMutationError::MissingParent)
    ));
    assert!(matches!(
        invalid_parent
            .drop_link(Binding {
                file_id,
                kind: FileKind::Fifo,
            })
            .map_err(|failure| failure.error),
        Err(GenerationMutationError::InconsistentState)
    ));

    let directory_id = FileId::from_bytes([92; 16]);
    let directory = FileRecord {
        file_id: directory_id,
        kind: FileKind::Directory,
        link_count: 1,
        metadata,
        payload: FilePayload::Directory {
            entries: ObjectId {
                kind: ObjectKind::TreePage,
                digest: Digest::ZERO,
            },
        },
    };
    let mut missing_mutation = defensive_transaction_state(
        Vec::new(),
        vec![RecordState {
            base: Some(directory),
            working: directory,
            present: true,
        }],
        vec![DirectoryEdit {
            directory_id,
            order: 0,
            retained_name_bytes: 0,
            mutation: None,
        }],
        Vec::new(),
    );
    let failure = poll_ready(missing_mutation.rewrite_directories(
        &MemoryObjectStore::default(),
        config(),
        &CancellationToken::new(),
    ))
    .ok_or("defensive directory rewrite unexpectedly suspended")?
    .err()
    .ok_or("missing directory mutation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        GenerationMutationError::InconsistentState
    ));

    let mut wrong_payload = defensive_transaction_state(
        Vec::new(),
        vec![record],
        vec![DirectoryEdit {
            directory_id: file_id,
            order: 0,
            retained_name_bytes: 0,
            mutation: Some(TreeMutation::Remove {
                name: name("missing")?,
                expected_file_id: None,
            }),
        }],
        vec![file_id],
    );
    let failure = poll_ready(wrong_payload.rewrite_directories(
        &MemoryObjectStore::default(),
        config(),
        &CancellationToken::new(),
    ))
    .ok_or("wrong-payload directory rewrite unexpectedly suspended")?
    .err()
    .ok_or("non-directory mutation unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        GenerationMutationError::MissingParent
    ));
    let failure = poll_ready(wrong_payload.verify_removed_directories(
        &MemoryObjectStore::default(),
        config(),
        &CancellationToken::new(),
    ))
    .ok_or("removed-directory proof unexpectedly suspended")?
    .err()
    .ok_or("non-directory removal unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        GenerationMutationError::InconsistentState
    ));
    Ok(())
}

#[test]
#[allow(clippy::too_many_lines)]
fn transaction_construction_rejects_every_contradictory_sparse_seed()
-> Result<(), Box<dyn std::error::Error>> {
    let metadata = ObjectId {
        kind: ObjectKind::Metadata,
        digest: Digest::ZERO,
    };
    let first_id = FileId::from_bytes([101; 16]);
    let second_id = FileId::from_bytes([102; 16]);
    let first = regular(first_id, metadata)?;
    let second = regular(second_id, metadata)?;
    let limits = config().limits;

    let one = MutationPlan::compile(
        vec![Mutation::Create {
            path: path(&["one"])?,
            record: first,
        }],
        limits,
        WorkBudget::UNBOUNDED,
    )?;
    let failure = TransactionState::new(
        &one,
        InitialLookups {
            paths: Vec::new(),
            path_bytes: 0,
            identities: Vec::new(),
            identity_bytes: 0,
        },
        config(),
        AllocationLedger::default(),
        WorkCounters::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("lookup cardinality mismatch unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        GenerationMutationError::InconsistentState
    ));

    let duplicate_path = MutationPlan::compile(
        vec![
            Mutation::ValidateRegular {
                path: path(&["same"])?,
            },
            Mutation::ValidateRegular {
                path: path(&["same"])?,
            },
        ],
        limits,
        WorkBudget::UNBOUNDED,
    )?;
    let failure = TransactionState::new(
        &duplicate_path,
        InitialLookups {
            paths: vec![
                PathBatchEntry {
                    record: Some(first),
                    parent: None,
                    resolved_components: 1,
                },
                PathBatchEntry {
                    record: Some(second),
                    parent: None,
                    resolved_components: 1,
                },
            ],
            path_bytes: 0,
            identities: Vec::new(),
            identity_bytes: 0,
        },
        config(),
        AllocationLedger::default(),
        WorkCounters::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("contradictory duplicate path lookup unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        GenerationMutationError::InconsistentState
    ));

    let duplicate_created = MutationPlan::compile(
        vec![
            Mutation::Create {
                path: path(&["first"])?,
                record: first,
            },
            Mutation::Create {
                path: path(&["second"])?,
                record: first,
            },
        ],
        limits,
        WorkBudget::UNBOUNDED,
    )?;
    let absent = PathBatchEntry {
        record: None,
        parent: None,
        resolved_components: 0,
    };
    let failure = TransactionState::new(
        &duplicate_created,
        InitialLookups {
            paths: vec![absent, absent],
            path_bytes: 0,
            identities: Vec::new(),
            identity_bytes: 0,
        },
        config(),
        AllocationLedger::default(),
        WorkCounters::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("duplicate created identity unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        GenerationMutationError::FileIdentityAlreadyExists
    ));

    let distinct_paths = MutationPlan::compile(
        vec![
            Mutation::ValidateRegular {
                path: path(&["left"])?,
            },
            Mutation::ValidateRegular {
                path: path(&["right"])?,
            },
        ],
        limits,
        WorkBudget::UNBOUNDED,
    )?;
    let conflicting_first = FileRecord {
        metadata: ObjectId {
            kind: ObjectKind::Metadata,
            digest: Digest::from_bytes([1; 32]),
        },
        ..first
    };
    let failure = TransactionState::new(
        &distinct_paths,
        InitialLookups {
            paths: vec![
                PathBatchEntry {
                    record: Some(first),
                    parent: None,
                    resolved_components: 1,
                },
                PathBatchEntry {
                    record: Some(conflicting_first),
                    parent: None,
                    resolved_components: 1,
                },
            ],
            path_bytes: 0,
            identities: Vec::new(),
            identity_bytes: 0,
        },
        config(),
        AllocationLedger::default(),
        WorkCounters::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("contradictory base records unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        GenerationMutationError::FileIdentityAlreadyExists
    ));

    let failure = TransactionState::new(
        &one,
        InitialLookups {
            paths: vec![PathBatchEntry {
                record: Some(first),
                parent: None,
                resolved_components: 1,
            }],
            path_bytes: 0,
            identities: Vec::new(),
            identity_bytes: 0,
        },
        config(),
        AllocationLedger::default(),
        WorkCounters::default(),
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("base and created identity collision unexpectedly succeeded")?;
    assert!(matches!(
        failure.error,
        GenerationMutationError::FileIdentityAlreadyExists
    ));
    Ok(())
}

#[test]
#[cfg(target_pointer_width = "64")]
fn impossible_transaction_vector_allocation_releases_its_complete_claim()
-> Result<(), Box<dyn std::error::Error>> {
    let mut allocations = AllocationLedger::default();
    let mut work = WorkCounters::default();
    let failure = reserve_exact::<u8>(
        usize::MAX,
        &mut allocations,
        &mut work,
        WorkBudget::UNBOUNDED,
    )
    .err()
    .ok_or("impossible transaction vector allocation succeeded")?;
    assert!(matches!(
        failure.error,
        GenerationMutationError::AllocationFailed
    ));
    assert_eq!(allocations.live_bytes(), 0);
    Ok(())
}
