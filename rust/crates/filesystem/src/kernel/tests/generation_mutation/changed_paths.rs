use super::*;
use crate::FsError;
use crate::kernel::changed_paths::{ChangedPath, Cursor};

async fn changed(
    store: &MemoryObjectStore,
    root: &GenerationRoot,
    mutations: Vec<Mutation>,
) -> Result<GenerationRoot, Box<dyn std::error::Error>> {
    Ok(apply_generation_mutations(
        store,
        root,
        mutations,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .root)
}

async fn expected(
    store: &MemoryObjectStore,
    roots: [Option<&GenerationRoot>; 2],
    names: &[&[&str]],
) -> Result<BTreeMap<NamespacePath, ChangedPath>, Box<dyn std::error::Error>> {
    let mut result = BTreeMap::new();
    for components in names {
        let path = path(components)?;
        let mut records = [None, None];
        for (side, root) in roots.into_iter().enumerate() {
            if let Some(root) = root {
                records[side] = lookup_path(
                    store,
                    root,
                    &path,
                    config(),
                    WorkBudget::UNBOUNDED,
                    &CancellationToken::new(),
                )
                .await?
                .record;
            }
        }
        let include = match records {
            [None, None] => false,
            [Some(a), Some(b)] => {
                a.file_id != b.file_id
                    || a.kind != b.kind
                    || a.metadata != b.metadata
                    || !(a.kind == FileKind::Directory && b.kind == FileKind::Directory)
                        && a.payload != b.payload
            }
            _ => true,
        };
        if include {
            result.insert(
                path.clone(),
                ChangedPath {
                    path,
                    before: records[0],
                    after: records[1],
                },
            );
        }
    }
    Ok(result)
}

async fn drain(
    mut cursor: Cursor<'_>,
    store: &MemoryObjectStore,
    roots: [Option<&GenerationRoot>; 2],
    work: &mut KernelAccounting<'_>,
    cancel: &CancellationToken,
) -> Result<BTreeMap<NamespacePath, ChangedPath>, FsError> {
    let mut actual = BTreeMap::new();
    let result = async {
        while let Some(row) = cursor.next(store, roots, work, cancel).await? {
            assert!(
                actual
                    .insert(row.value.path.clone(), row.value.clone())
                    .is_none(),
                "one path was emitted more than once: {:?}",
                row.value.path
            );
            row.retire(work)?;
        }
        Ok(())
    }
    .await;
    let retired = cursor.retire(work);
    result.and(retired)?;
    Ok(actual)
}

async fn verify(
    store: &MemoryObjectStore,
    roots: [Option<&GenerationRoot>; 2],
    names: &[&[&str]],
) -> Result<(), Box<dyn std::error::Error>> {
    let expected = expected(store, roots, names).await?;
    let cancel = CancellationToken::new();
    let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancel);
    let cursor = Cursor::new(roots, config(), &mut work)?;
    assert_eq!(
        drain(cursor, store, roots, &mut work, &cancel).await?,
        expected
    );
    assert_eq!(work.standalone_parts().unwrap().0.live_bytes(), 0);

    let views = roots.map(|root| {
        root.map(|root| crate::workspace::NamespaceView {
            objects: store,
            config: config(),
            root,
        })
    });
    let (rows, allocation) =
        crate::workspace::complete_changed_paths(views, u32::MAX, &mut work, &cancel).await?;
    assert_eq!(rows, expected.values().cloned().collect::<Vec<_>>());
    let bytes = rows.capacity() * std::mem::size_of::<ChangedPath>();
    let bytes = bytes as u64
        + rows
            .iter()
            .map(|row| row.path.retained_bytes())
            .sum::<u64>();
    Owned::new(rows, allocation, bytes).retire(&mut work)?;
    assert_eq!(work.standalone_parts().unwrap().0.live_bytes(), 0);

    if let [Some(before), Some(after)] = roots {
        let (delta, allocation) = crate::kernel::generation_diff::diff_generation_file_tables(
            store,
            before.file_table,
            after.file_table,
            u32::MAX,
            DecodeLimits::for_volume(config()),
            &mut work,
            &cancel,
        )
        .await?;
        assert!(!delta.truncated);
        let cursor = Cursor::from_records(roots, config(), &delta.files)?;
        assert_eq!(
            drain(cursor, store, roots, &mut work, &cancel).await?,
            expected
        );
        let bytes = delta.retained_bytes()?;
        Owned::new(delta, allocation, bytes).retire(&mut work)?;
        assert_eq!(work.standalone_parts().unwrap().0.live_bytes(), 0);
    }
    Ok(())
}

#[tokio::test]
async fn retained_changed_paths_cover_aliases_moves_replacements_and_refused_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
    let file = FileId::from_bytes([2; 16]);
    let sub = FileId::from_bytes([4; 16]);
    let linked = changed(
        &store,
        &fixture.generation,
        vec![
            Mutation::Create {
                path: path(&["dir", "sub"])?,
                record: FileRecord {
                    file_id: sub,
                    bindings: None,
                    kind: FileKind::Directory,
                    link_count: 1,
                    metadata: fixture.metadata,
                    payload: FilePayload::Directory {
                        entries: fixture.empty_tree,
                    },
                },
            },
            Mutation::Link {
                source: path(&["a"])?,
                destination: path(&["dir", "alias"])?,
            },
            Mutation::Link {
                source: path(&["a"])?,
                destination: path(&["dir", "sub", "deep"])?,
            },
        ],
    )
    .await?;
    let names: &[&[&str]] = &[
        &[],
        &["a"],
        &["dir"],
        &["dir", "alias"],
        &["dir", "sub"],
        &["dir", "sub", "deep"],
        &["moved"],
        &["moved", "alias"],
        &["moved", "sub"],
        &["moved", "sub", "deep"],
        &["moved", "sub", "new"],
        &["elsewhere"],
        &["elsewhere", "deep"],
    ];
    let moved = changed(
        &store,
        &linked,
        vec![Mutation::Rename {
            source: path(&["dir"])?,
            destination: path(&["moved"])?,
            replace: false,
        }],
    )
    .await?;
    verify(&store, [Some(&linked), Some(&moved)], names).await?;
    let content = changed(
        &store,
        &moved,
        vec![Mutation::File {
            file_id: file,
            mutation: FileMutation::Resize { logical_bytes: 1 },
        }],
    )
    .await?;
    verify(&store, [Some(&linked), Some(&content)], names).await?;
    let nested = changed(
        &store,
        &moved,
        vec![Mutation::Create {
            path: path(&["moved", "sub", "new"])?,
            record: regular(FileId::from_bytes([6; 16]), fixture.metadata)?,
        }],
    )
    .await?;
    verify(&store, [Some(&linked), Some(&nested)], names).await?;
    let separately_moved = changed(
        &store,
        &moved,
        vec![Mutation::Rename {
            source: path(&["moved", "sub"])?,
            destination: path(&["elsewhere"])?,
            replace: false,
        }],
    )
    .await?;
    verify(&store, [Some(&linked), Some(&separately_moved)], names).await?;
    let replacement = changed(
        &store,
        &linked,
        vec![
            Mutation::Create {
                path: path(&["fresh"])?,
                record: regular(FileId::from_bytes([5; 16]), fixture.changed_metadata)?,
            },
            Mutation::Rename {
                source: path(&["fresh"])?,
                destination: path(&["a"])?,
                replace: true,
            },
        ],
    )
    .await?;
    verify(&store, [Some(&linked), Some(&replacement)], names).await?;
    verify(&store, [Some(&replacement), Some(&linked)], names).await?;
    verify(&store, [None, Some(&linked)], names).await?;
    verify(&store, [Some(&linked), None], names).await?;
    verify(&store, [Some(&linked), Some(&linked)], names).await?;

    let roots = [Some(&linked), Some(&nested)];
    let cancel = CancellationToken::new();
    let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancel);
    let cursor = Cursor::new(roots, config(), &mut work)?;
    drain(cursor, &store, roots, &mut work, &cancel).await?;
    let allocations = work.spent().allocation_operations;
    for maximum in 0..allocations {
        let mut work = KernelAccounting::standalone(
            WorkBudget {
                allocation_operations: maximum,
                ..WorkBudget::UNBOUNDED
            },
            &cancel,
        );
        let result = match Cursor::new(roots, config(), &mut work) {
            Ok(cursor) => drain(cursor, &store, roots, &mut work, &cancel).await,
            Err(error) => Err(error),
        };
        assert!(
            matches!(
                result,
                Err(FsError::Work(crate::WorkError::BudgetExceeded {
                    counter: "allocation_operations",
                    ..
                }))
            ),
            "allocation {maximum}/{allocations}: {result:?}"
        );
        assert_eq!(
            work.standalone_parts().unwrap().0.live_bytes(),
            0,
            "allocation {maximum}/{allocations} lost original custody"
        );
    }
    let mut work = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancel);
    let mut cursor = Cursor::new(roots, config(), &mut work)?;
    cursor
        .next(&store, roots, &mut work, &cancel)
        .await?
        .ok_or("missing first row")?
        .retire(&mut work)?;
    let before = work.spent().backend_read_operations;
    cancel.cancel();
    assert!(
        cursor
            .next(&store, roots, &mut work, &cancel)
            .await
            .is_err()
    );
    assert_eq!(work.spent().backend_read_operations, before);
    assert!(matches!(
        cursor.next(&store, roots, &mut work, &cancel).await,
        Err(FsError::InvalidDiff)
    ));
    cursor.retire(&mut work)?;
    assert_eq!(work.standalone_parts().unwrap().0.live_bytes(), 0);
    Ok(())
}
