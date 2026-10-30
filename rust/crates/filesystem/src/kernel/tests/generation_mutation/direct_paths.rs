use super::*;
use crate::kernel::direct_paths::DirectPaths;

struct DirectOnly<'a>(&'a MemoryObjectStore);

impl DirectOnly<'_> {
    fn check(id: ObjectId) {
        assert!(
            matches!(id.kind, ObjectKind::FileTablePage | ObjectKind::BindingPage),
            "path query traversed the forward namespace: {id:?}"
        );
    }
}

impl AsyncObjectStore for DirectOnly<'_> {
    async fn put(
        &self,
        id: ObjectId,
        bytes: OwnedBytes,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(), crate::ObjectStoreError> {
        self.0.put(id, bytes, work).await
    }

    async fn read(
        &self,
        id: ObjectId,
        maximum: u64,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<crate::ObjectRead, crate::ObjectStoreError> {
        Self::check(id);
        self.0.read(id, maximum, work).await
    }

    async fn read_many(
        &self,
        requests: &[crate::ObjectReadRequest],
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<(Vec<crate::ObjectRead>, crate::AuthorityAllocation), crate::ObjectStoreError> {
        for request in requests {
            Self::check(request.object_id);
        }
        self.0.read_many(requests, work).await
    }

    async fn contains(
        &self,
        id: ObjectId,
        work: &mut crate::AuthorityWork<'_>,
    ) -> Result<bool, crate::ObjectStoreError> {
        Self::check(id);
        self.0.contains(id, work).await
    }
}

async fn collect_paths(
    store: &MemoryObjectStore,
    root: &GenerationRoot,
    record: FileRecord,
) -> Result<Vec<NamespacePath>, Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancel);
    let mut cursor = DirectPaths::new(root, record, config(), &mut work)?;
    let mut observed = Vec::new();
    while let Some(value) = cursor.next(&DirectOnly(store), &mut work, &cancel).await? {
        observed.push(value.value.clone());
        value.retire(&mut work)?;
    }
    cursor.retire(&mut work)?;
    assert_eq!(work.standalone_parts().unwrap().0.live_bytes(), 0);
    Ok(observed)
}

#[tokio::test]
async fn aliases_follow_pinned_parent_edges_and_refusals_keep_original_custody()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
    let file = FileId::from_bytes([2; 16]);
    let directory = FileId::from_bytes([3; 16]);
    let linked = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::Link {
            source: path(&["a"])?,
            destination: path(&["dir", "alias"])?,
        }],
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .root;
    let record = candidate_record(&store, &linked, file).await?;
    assert_eq!(
        collect_paths(&store, &linked, record).await?,
        [path(&["a"])?, path(&["dir", "alias"])?]
    );
    let moved = apply_generation_mutations(
        &store,
        &linked,
        vec![
            Mutation::Rename {
                source: path(&["dir"])?,
                destination: path(&["moved"])?,
                replace: false,
            },
            Mutation::File {
                file_id: file,
                mutation: FileMutation::Resize { logical_bytes: 1 },
            },
        ],
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .root;
    let changed = candidate_record(&store, &moved, file).await?;
    assert_eq!(
        record.bindings, changed.bindings,
        "moving a parent rewrote descendant edges"
    );
    assert_eq!(
        collect_paths(&store, &moved, changed).await?,
        [path(&["a"])?, path(&["moved", "alias"])?]
    );
    // The old endpoint remains exact after moving and changing the new endpoint.
    assert_eq!(
        collect_paths(&store, &linked, record).await?,
        [path(&["a"])?, path(&["dir", "alias"])?]
    );
    let root_record = candidate_record(&store, &moved, moved.root_file_id).await?;
    assert_eq!(
        collect_paths(&store, &moved, root_record).await?,
        [path(&[])?]
    );

    let cancel = CancellationToken::new();
    let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancel);
    let mut cursor = DirectPaths::new(&moved, changed, config(), &mut work)?;
    let first = cursor
        .next(&DirectOnly(&store), &mut work, &cancel)
        .await?
        .ok_or("first alias absent")?;
    let reads = work.spent().page_reads;
    cancel.cancel();
    assert!(matches!(
        cursor.next(&DirectOnly(&store), &mut work, &cancel).await,
        Err(FsError::Cancelled(_))
    ));
    assert_eq!(work.spent().page_reads, reads);
    first.retire(&mut work)?;
    cursor.retire(&mut work)?;
    assert_eq!(work.standalone_parts().unwrap().0.live_bytes(), 0);

    let cancel = CancellationToken::new();
    let mut work = KernelAccounting::standalone(
        WorkBudget {
            page_reads: 0,
            ..WorkBudget::UNBOUNDED
        },
        &cancel,
    );
    let mut cursor = DirectPaths::new(&moved, changed, config(), &mut work)?;
    assert!(matches!(
        cursor.next(&DirectOnly(&store), &mut work, &cancel).await,
        Err(FsError::Work(WorkError::BudgetExceeded {
            counter: "page_reads",
            ..
        }))
    ));
    cursor.retire(&mut work)?;
    assert_eq!(work.standalone_parts().unwrap().0.live_bytes(), 0);

    let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancel);
    assert!(matches!(
        DirectPaths::new(
            &moved,
            FileRecord {
                bindings: None,
                ..changed
            },
            config(),
            &mut work
        ),
        Err(FsError::InvalidDiff)
    ));
    assert!(matches!(
        DirectPaths::new(
            &moved,
            FileRecord {
                bindings: changed.bindings,
                ..root_record
            },
            config(),
            &mut work
        ),
        Err(FsError::InvalidDiff)
    ));
    // A canonical but inconsistent parent edge must terminate without scanning
    // the namespace or allocating an unbounded visited-identity history.
    let mut directory_record = candidate_record(&store, &moved, directory).await?;
    directory_record.bindings = Some(stored_bindings(&store, &[(directory, "cycle")]).await?);
    let bad_table = put_file_table_page(
        &store,
        &FileTablePage::Leaf(vec![root_record, changed, directory_record]),
        config().limits.maximum_directory_page_entries,
    )
    .await?;
    let bad = GenerationRoot {
        file_table: bad_table,
        ..moved.clone()
    };
    let mut cursor = DirectPaths::new(&bad, directory_record, config(), &mut work)?;
    assert!(matches!(
        cursor.next(&DirectOnly(&store), &mut work, &cancel).await,
        Err(FsError::InvalidDiff)
    ));
    cursor.retire(&mut work)?;
    assert_eq!(work.standalone_parts().unwrap().0.live_bytes(), 0);
    Ok(())
}
