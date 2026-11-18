use super::*;
use crate::kernel::{Child, lookup_path_refs};

async fn deep_directory_fixture(
    store: &MemoryObjectStore,
    depth: usize,
) -> Result<(GenerationRoot, ObjectId), Box<dyn std::error::Error>> {
    let component = name("d")?;
    let metadata = put_file_metadata(store, FileMetadata::default()).await?;
    let empty_tree = put_tree_page(store, &TreePage::Leaf(Vec::new()), 8).await?;
    let identity = |index: usize| -> Result<FileId, Box<dyn std::error::Error>> {
        Ok(FileId::from_bytes(u128::try_from(index)?.to_be_bytes()))
    };
    // Construct authenticated pages directly in linear work. Repeated public
    // mkdir would spend quadratic work walking every already-created prefix.
    let mut records = Vec::with_capacity(depth + 1);
    for index in 0..=depth {
        let entries = if index == depth {
            empty_tree
        } else {
            put_tree_page(
                store,
                &TreePage::Leaf(vec![TreeEntry {
                    name: component.clone(),
                    file_id: identity(index + 1)?,
                    kind: FileKind::Directory,
                }]),
                8,
            )
            .await?
        };
        let bindings = if index == 0 {
            None
        } else {
            Some(stored_bindings(store, &[(identity(index - 1)?, "d")]).await?)
        };
        records.push(FileRecord {
            bindings,
            file_id: identity(index)?,
            kind: FileKind::Directory,
            link_count: 1,
            metadata,
            payload: FilePayload::Directory { entries },
        });
    }
    let mut level = Vec::new();
    for records in records.chunks(8) {
        let first = records.first().ok_or("empty file-table leaf")?.file_id;
        let page = put_file_table_page(store, &FileTablePage::Leaf(records.to_vec()), 8).await?;
        level.push(Child { first, page });
    }
    while level.len() > 1 {
        let mut parents = Vec::new();
        for children in level.chunks(8) {
            let first = children.first().ok_or("empty file-table branch")?.first;
            let page =
                put_file_table_page(store, &FileTablePage::Internal(children.to_vec()), 8).await?;
            parents.push(Child { first, page });
        }
        level = parents;
    }
    let generation = GenerationRoot {
        volume_id: VolumeId::from_bytes([9; 16]),
        root_file_id: identity(0)?,
        file_table: level.first().ok_or("missing file-table root")?.page,
        parents: Vec::new(),
        required_features: 0,
    };
    Ok((generation, metadata))
}

#[tokio::test]
async fn deep_directory_lookup_and_mutation_keep_exact_component_counts()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let depth = usize::from(u16::MAX) + 1;
    let (generation, metadata) = deep_directory_fixture(&store, depth).await?;
    let component = name("d")?;
    let config = config();
    let identity = |index: usize| -> Result<FileId, Box<dyn std::error::Error>> {
        Ok(FileId::from_bytes(u128::try_from(index)?.to_be_bytes()))
    };
    let deepest = NamespacePath::new(vec![component.clone(); depth], config.limits)?;
    let mut child = deepest.components().to_vec();
    child.push(name("child")?);
    let child = NamespacePath::new(child, config.limits)?;
    let mut renamed = vec![component; depth - 1];
    renamed.push(name("renamed")?);
    let renamed = NamespacePath::new(renamed, config.limits)?;
    let cancellation = CancellationToken::new();
    let lookup = lookup_path(
        &store,
        &generation,
        &deepest,
        config,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_eq!(lookup.resolved_components, usize_to_u64(depth));
    assert_eq!(
        lookup.record.map(|record| record.file_id),
        Some(identity(depth)?)
    );
    // The real transaction authenticates both the existing depth-65,536
    // directory and its absent child in one shared batch. Either count being
    // truncated makes the corresponding rename or create fail MissingParent.
    let child_id = identity(depth + 1)?;
    let receipt = apply_generation_mutations(
        &store,
        &generation,
        vec![
            Mutation::Create {
                path: child.clone(),
                record: regular(child_id, metadata)?,
            },
            Mutation::Remove {
                path: child,
                expected_file_id: MetadataField::Value(child_id),
            },
            Mutation::Rename {
                source: deepest.clone(),
                destination: renamed.clone(),
                replace: false,
            },
            Mutation::Remove {
                path: renamed.clone(),
                expected_file_id: MetadataField::Value(identity(depth)?),
            },
        ],
        config,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    // Both removed names are gone from the deepest surviving directory. Its
    // record and entries are read by identity in logarithmic work: the counts
    // above already crossed the 16-bit boundary, and a second complete walk to
    // depth 65,535 (which equals u16::MAX, so it could not detect saturation
    // anyway) would only repeat that work.
    let parent = candidate_record(&store, &receipt.root, identity(depth - 1)?).await?;
    let FilePayload::Directory { entries } = parent.payload else {
        return Err("the deepest surviving record is not a directory".into());
    };
    for removed in [name("d")?, name("renamed")?] {
        let entry = crate::kernel::lookup_tree_entry(
            &store,
            entries,
            &removed,
            DecodeLimits::for_volume(config),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert_eq!(entry.value, None);
    }
    // Check the pinned original record through its immutable file-table root,
    // without repeating the complete 65,536-component namespace walk.
    assert_eq!(
        Some(candidate_record(&store, &generation, identity(depth)?).await?),
        lookup.record,
    );
    Ok(())
}

#[tokio::test]
async fn a_batched_path_reads_each_object_once_like_a_single_path()
-> Result<(), Box<dyn std::error::Error>> {
    // An 8,192-deep path reads about 9,400 distinct objects, more than any
    // small fixed cache holds, so a batch cache bounded below the single path's
    // would read file-table pages again that the single path reads once.
    let store = MemoryObjectStore::default();
    let depth = 8_192;
    let (generation, _) = deep_directory_fixture(&store, depth).await?;
    let config = config();
    let path = NamespacePath::new(vec![name("d")?; depth], config.limits)?;
    let cancellation = CancellationToken::new();
    let single = lookup_path(
        &store,
        &generation,
        &path,
        config,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    let batch = lookup_path_refs(
        &store,
        &generation,
        &[&path],
        config,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_eq!(
        batch.entries.first().and_then(|entry| entry.record),
        single.record
    );
    assert_eq!(
        batch.work.backend_read_operations,
        single.work.backend_read_operations
    );
    assert_eq!(batch.work.object_bytes_read, single.work.object_bytes_read);
    Ok(())
}
