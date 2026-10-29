use super::*;
use crate::kernel::{
    CanonicalDecodeError, FileMetadata, FilePayload, FileRecord, FileTablePage, GenerationRoot,
    TreePage,
};
use crate::{FileId, VolumeId};

#[tokio::test]
async fn parent_first_scope_drains_spilled_children_before_resident_parents()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let directory = tempfile::tempdir()?;
    let provider = Arc::new(acyclic_objects::MemoryObjects::memory());
    let store = StagedObjects::open(
        LogicalObjectStore::new(provider.clone()),
        directory.path().to_path_buf(),
    );
    let cancellation = CancellationToken::new();
    let mut work = AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation);
    let observe = |_| Ok::<_, CanonicalDecodeError>(());
    let reserve = |_| CanonicalDecodeError::AllocationFailed;
    let metadata = HashedObject::new(
        ObjectKind::Metadata,
        crate::kernel::encode_file_metadata(FileMetadata::default(), observe, reserve)?.into(),
    );
    let tree = HashedObject::new(
        ObjectKind::TreePage,
        crate::kernel::encode_tree_page(&TreePage::Leaf(Vec::new()), 8, observe, reserve)?.into(),
    );
    let metadata_id = metadata.object_id();
    let tree_id = tree.object_id();
    store.put_hashed(metadata, &mut work).await?;
    store.put_hashed(tree, &mut work).await?;
    // Real spill pressure leaves the old children on disk and their new parent resident.
    for marker in [1, 2] {
        store
            .put_hashed(
                HashedObject::new(ObjectKind::BlobChunk, vec![marker; 3 * 1_048_576].into()),
                &mut work,
            )
            .await?;
    }
    assert!(matches!(store.lookup(metadata_id), Lookup::Spilled { .. }));
    let root_file_id = FileId::from_bytes([9; 16]);
    let table = FileTablePage::Leaf(vec![FileRecord {
        file_id: root_file_id,
        kind: crate::kernel::FileKind::Directory,
        link_count: 1,
        metadata: metadata_id,
        payload: FilePayload::Directory { entries: tree_id },
    }]);
    let table = HashedObject::new(
        ObjectKind::FileTablePage,
        crate::kernel::encode_file_table_page(&table, 8, observe, reserve)?.into(),
    );
    let table_id = table.object_id();
    store.put_hashed(table, &mut work).await?;
    let root = GenerationRoot {
        volume_id: VolumeId::from_bytes([8; 16]),
        root_file_id,
        file_table: table_id,
        parents: Vec::new(),
        required_features: 0,
    };
    let root = HashedObject::new(
        ObjectKind::GenerationRoot,
        crate::kernel::encode_generation_root(&root, observe, reserve)?.into(),
    );
    let root_id = root.object_id();
    store.put_hashed(root, &mut work).await?;
    let hold = store
        .flush_before_publish(
            PublicationScope::Closure {
                objects: &[root_id, table_id, metadata_id, tree_id],
                proven_at: 0,
            },
            &mut work,
        )
        .await?;
    let digest = |id: ObjectId| acyclic_objects::Digest::from_bytes(*id.digest.as_bytes());
    assert_eq!(
        provider.head(digest(root_id)).await?.references,
        vec![digest(table_id)]
    );
    assert_eq!(
        provider.head(digest(table_id)).await?.references,
        vec![digest(metadata_id), digest(tree_id)]
    );
    drop(hold);
    Ok(())
}
