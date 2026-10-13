use super::*;

#[tokio::test]
async fn pinned_directory_discovery_survives_ten_thousand_retained_workspaces()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("discovered-agent"))?;
    workspace.write_text("/visible.txt", "visible").await?;
    let pinned = unbounded!(workspace, head())?;
    let cancellation = CancellationToken::new();
    let before = list_generation_directory(
        &workspace,
        GenerationSelector::Exact(pinned.id()),
        "/",
        None,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    let mut retained = Vec::with_capacity(10_000);
    for index in 0..10_000 {
        let other = unbounded!(fs, create_workspace(&format!("retained-agent-{index}")))?;
        retained.push(unbounded!(other, head())?);
    }
    workspace
        .write_text("/late.txt", "not in pinned view")
        .await?;
    let after = list_generation_directory(
        &workspace,
        GenerationSelector::Exact(pinned.id()),
        "/",
        None,
        1,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_eq!(retained.len(), 10_000);
    assert_eq!(before.value, after.value);
    assert_eq!(after.value.entries.len(), 1);
    Ok(())
}

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
    transaction
        .zero_range(
            "/tree/a",
            crate::ByteRange {
                offset: 2,
                length: 2,
            },
            false,
            false,
        )
        .await?;
    transaction.resize("/tree/a", 8).await?;
    transaction
        .zero_range(
            "/tree/sparse",
            crate::ByteRange {
                offset: 100,
                length: 20,
            },
            true,
            false,
        )
        .await?;
    transaction.resize("/tree/sparse", 400).await?;
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
    let bounded = exact.paths_for_file_ids([file_id], 1).await?;
    assert_eq!(bounded.get(&file_id).map(Vec::len), Some(1));

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
        unbounded!(workspace, read_symbolic_link("/tree/link"))?,
        OwnedBytes::from_static(b"a")
    );

    let listing = unbounded!(workspace, head())?;
    let first = unbounded!(listing, list_directory("/tree", None, 1))?;
    assert_eq!(first.entries.len(), 1);
    assert!(first.has_more);
    workspace.write_text("/tree/late", "new").await?;
    let second = unbounded!(
        listing,
        list_directory("/tree", Some(&first.entries[0].name), 16)
    )?;
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
    assert!(matches!(
        cancelled_failure.error,
        WorkspaceError::Cancelled(_)
    ));
    let budget_failure = generation
        .lookup_paths(&[present], WorkBudget::default(), &CancellationToken::new())
        .await
        .err()
        .ok_or_else(|| std::io::Error::other("zero-budget checkout must fail"))?;

    assert!(budget_failure.work.verify(WorkBudget::default()).is_err());
    Ok(())
}

#[tokio::test]
async fn generation_reader_resolves_many_paths_from_one_pinned_root() -> Result<(), Box<dyn Error>>
{
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("generation-reader"))?;
    workspace.write_text("/present", "body").await?;
    let generation = unbounded!(workspace, head())?;
    let config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    let present = customer_path("/present", config)?;
    let absent = customer_path("/absent", config)?;

    let reader = unbounded!(
        generation.workspace(),
        checkout(
            GenerationSelector::Exact(generation.id()),
            CheckoutMode::read_only_pinned()
        )
    )?
    .pinned_reader()
    .map_err(WorkspaceError::engine)?;
    let descriptions = reader
        .describe_files(
            &[present, absent],
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;

    assert_eq!(descriptions.len(), 2);
    assert_eq!(
        descriptions[0].as_ref().map(|file| file.logical_bytes),
        Some(4)
    );
    assert!(descriptions[1].is_none());
    Ok(())
}
