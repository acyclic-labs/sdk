use super::*;

#[tokio::test]
async fn customer_reads_are_bounded_sparse_link_aware_and_generation_exact()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("read-shapes"))?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    unbounded!(transaction, create_directory("/tree"))?;
    unbounded!(
        transaction,
        write("/tree/a", OwnedBytes::from_static(b"abcdef"))
    )?;
    unbounded!(
        transaction,
        write("/tree/sparse", OwnedBytes::from(vec![1; 300]))
    )?;
    unbounded!(transaction, hard_link("/tree/a", "/tree/b"))?;
    unbounded!(
        transaction,
        create_symbolic_link("/tree/link", OwnedBytes::from_static(b"a"))
    )?;
    unbounded!(
        transaction,
        zero_range(
            "/tree/a",
            crate::ByteRange {
                offset: 2,
                length: 2,
            },
            false,
            false
        )
    )?;
    unbounded!(transaction, resize("/tree/a", 8))?;
    unbounded!(
        transaction,
        zero_range(
            "/tree/sparse",
            crate::ByteRange {
                offset: 100,
                length: 20,
            },
            true,
            false
        )
    )?;
    unbounded!(transaction, resize("/tree/sparse", 400))?;
    let TransactionCommit::Committed(exact) = commit(&mut transaction).await? else {
        return Err("shape publication did not commit".into());
    };

    let file_id = unbounded!(workspace, stat("/tree/a"))?.file_id;
    let directory_id = unbounded!(workspace, stat("/tree"))?.file_id;
    let missing_id = FileId::from_bytes([0xff; 16]);
    let paths = exact
        .paths_for_file_ids([file_id, directory_id, missing_id], 32)
        .await?;
    assert_eq!(
        paths.get(&file_id),
        Some(&vec!["/tree/a".to_owned(), "/tree/b".to_owned()])
    );
    assert_eq!(paths.get(&directory_id), Some(&vec!["/tree".to_owned()]));
    assert!(!paths.contains_key(&missing_id));

    assert_eq!(
        unbounded!(workspace, read_range("/tree/a", 1, 5))?.bytes,
        OwnedBytes::from_static(b"b\0\0ef")
    );
    let a = unbounded!(workspace, stat("/tree/a"))?;
    let b = unbounded!(workspace, stat("/tree/b"))?;
    assert_eq!(a.file_id, b.file_id);
    assert_eq!(a.link_count, 2);
    assert_eq!(a.logical_bytes, Some(8));
    assert_eq!(a.kind, FileKind::Regular);
    assert_eq!(
        unbounded!(workspace, read_symbolic_link("/tree/link"))?.bytes,
        OwnedBytes::from_static(b"a")
    );

    let listing = unbounded!(workspace, head())?;
    let first = unbounded!(listing, list_directory("/tree", None, 1))?.0;
    assert_eq!(first.entries.len(), 1);
    assert!(first.has_more);
    workspace.write_text("/tree/late", "new").await?;
    let second = unbounded!(
        listing,
        list_directory("/tree", Some(&first.entries[0].name), 16)
    )?
    .0;
    assert_eq!(second.entries.len(), 3);
    assert!(!second.has_more);

    let extents = workspace.plan_extents("/tree/sparse", 0, 400, 16).await?;
    assert!(
        extents
            .spans
            .iter()
            .any(|span| span.kind == WorkspaceExtentKind::AllocatedZero)
    );
    assert!(
        extents
            .spans
            .iter()
            .any(|span| span.kind == WorkspaceExtentKind::Content)
    );

    workspace.write_text("/tree/a", "changed").await?;
    assert_eq!(
        unbounded!(exact, read("/tree/a", 16))?,
        OwnedBytes::from_static(b"ab\0\0ef\0\0")
    );
    assert_eq!(
        unbounded!(workspace, read("/tree/a", 16))?,
        OwnedBytes::from_static(b"changed")
    );
    Ok(())
}

#[tokio::test]
async fn generation_lookup_paths_preserves_order_absence_and_duplicates()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("lookup-paths"))?;
    workspace.write_text("/present", "body").await?;
    let generation = unbounded!(workspace, head())?;
    let config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    let present = customer_path("/present", config)?;
    let absent = customer_path("/absent", config)?;
    let records = generation
        .lookup_paths(
            &[present.clone(), absent, present.clone()],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert_eq!(records.value.len(), 3);
    assert!(records.value[0].is_some());
    assert!(records.value[1].is_none());
    assert_eq!(records.value[0], records.value[2]);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancelled_failure = generation
        .lookup_paths(
            std::slice::from_ref(&present),
            WorkBudget::UNBOUNDED,
            &cancelled,
        )
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("pre-cancelled lookup must fail"))?;
    assert!(matches!(cancelled_failure.error, FsError::Cancelled(_)));
    let budget_failure = generation
        .lookup_paths(&[present], WorkBudget::default(), &CancellationToken::new())
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("zero-budget checkout must fail"))?;
    assert!(matches!(
        budget_failure.error,
        FsError::Work(crate::WorkError::BudgetExceeded { .. })
            | FsError::Object(crate::ObjectStoreError::Work(
                crate::WorkError::BudgetExceeded { .. }
            ))
    ));
    Ok(())
}

/// An immutable generation's cached absence is exact: asking again for
/// identities that have no paths answers empty rather than validating an
/// empty path batch.
#[tokio::test]
async fn repeated_lookups_of_absent_identities_answer_empty() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("absent-identities"))?;
    workspace.write_text("/kept", "kept").await?;
    workspace.write_text("/gone", "gone").await?;
    let gone = unbounded!(workspace, stat("/gone"))?.file_id;
    workspace.remove("/gone").await?;
    let head = unbounded!(workspace, head())?;
    for _ in 0..2 {
        assert!(head.paths_for_file_ids([gone], 32).await?.is_empty());
    }
    Ok(())
}
