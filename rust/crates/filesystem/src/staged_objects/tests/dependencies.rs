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
    // Ordering is shared scratch on the original frame, including ordinary
    // callers. Its returned vector stays funded until the drain releases it.
    let resident_targets = [metadata_id, tree_id];
    let no_spill = None;
    for native in [false, true] {
        let original = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
        let mut ordering = if native {
            AuthorityWork::native(&original, &cancellation)
        } else {
            AuthorityWork::new(WorkBudget::UNBOUNDED, &cancellation)
        };
        let mut earlier = ordering.vec_owned::<u8>(31)?;
        earlier.value.resize(31, 7);
        let independent = ordering.vec_owned::<u8>(17)?;
        let mut independent = crate::kernel::allocation::retained::Retained::from_owned(
            independent,
            &mut ordering.accounting(),
        )?;
        independent.value.resize(17, 9);
        let baseline = if native {
            original.live_allocation_bytes()
        } else {
            ordering
                .accounting()
                .standalone_parts()
                .unwrap()
                .0
                .live_bytes()
        };
        let ordered = {
            use std::future::Future;
            use std::task::{Context, Poll, Waker};
            let mut future =
                std::pin::pin!(store.child_first(&no_spill, &resident_targets, &mut ordering));
            let Poll::Ready(result) = future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            else {
                panic!("resident dependency ordering waited for an unrelated producer");
            };
            result?
        };
        assert_eq!(ordered.value, [metadata_id, tree_id]);
        assert_eq!(ordering.spent().bytes_copied, size_of::<ObjectId>() as u64);
        assert_eq!(ordering.spent().backend_read_operations, 0);
        let retained = if native {
            original.live_allocation_bytes()
        } else {
            ordering
                .accounting()
                .standalone_parts()
                .unwrap()
                .0
                .live_bytes()
        };
        assert!(
            retained > baseline,
            "returned ordering lost its original backing"
        );
        drop(ordered);
        let retired = if native {
            original.live_allocation_bytes()
        } else {
            ordering
                .accounting()
                .standalone_parts()
                .unwrap()
                .0
                .live_bytes()
        };
        assert_eq!(
            retired, baseline,
            "ordering leaked scratch or caller credit"
        );
        assert_eq!(earlier.value, [7; 31]);
        assert_eq!(independent.value, [9; 17]);
        drop(independent);
        earlier.retire(&mut ordering.accounting())?;
    }
    for budget in [
        WorkBudget {
            allocation_operations: 0,
            ..WorkBudget::UNBOUNDED
        },
        WorkBudget {
            bytes_copied: 0,
            ..WorkBudget::UNBOUNDED
        },
    ] {
        let mut denied = AuthorityWork::new(budget, &cancellation);
        let result = store
            .child_first(&no_spill, &resident_targets, &mut denied)
            .await;
        assert!(matches!(result, Err(ObjectStoreError::Work(
            crate::WorkError::BudgetExceeded { counter, .. }
        )) if counter == if budget.allocation_operations == 0 {
            "allocation_operations"
        } else {
            "bytes_copied"
        }));
        assert_eq!(denied.spent().backend_read_operations, 0);
        assert_eq!(denied.spent().backend_write_operations, 0);
        assert!(matches!(store.lookup(metadata_id), Lookup::Resident(_)));
        assert!(matches!(store.lookup(tree_id), Lookup::Resident(_)));
    }
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
        bindings: None,
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
            PublicationScope {
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
