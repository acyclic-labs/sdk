#[tokio::test]
async fn exact_path_reads_only_required_frontiers_and_authenticates_absence()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, path) = fixture(&store).await?;
    let found = lookup_path(
        &store,
        &generation,
        &path,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        found.record.map(|record| record.kind),
        Some(FileKind::Regular)
    );
    assert_eq!(
        found.parent.map(|record| record.kind),
        Some(FileKind::Directory)
    );
    assert_eq!(found.resolved_components, 2);
    assert_eq!(found.work.page_reads, 5);
    assert_eq!(found.work.backend_read_operations, 3);
    let observed = (observe_path_async(
        &store,
        &generation,
        &path,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await?;
    assert_eq!(observed.lookup.record, found.record);
    assert_eq!(observed.dependencies.len(), 3);
    assert!(matches!(
        observed.dependencies.last(),
        Some(Dependency {
            region: DependencyRegion::FileRecord(_),
            expected: DependencyState::Present(_),
        })
    ));

    let missing = NamespacePath::new(
        vec![
            LogicalName::new(NameEncoding::Utf8, b"src".to_vec(), 255)?,
            LogicalName::new(NameEncoding::Utf8, b"missing".to_vec(), 255)?,
        ],
        config().limits,
    )?;
    let absent = lookup_path(
        &store,
        &generation,
        &missing,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(absent.record, None);
    assert_eq!(
        absent.parent.map(|record| record.kind),
        Some(FileKind::Directory)
    );
    assert_eq!(absent.resolved_components, 1);
    let observed_absent = (observe_path_async(
        &store,
        &generation,
        &missing,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    ))
    .await?;
    assert_eq!(observed_absent.lookup.record, None);
    assert_eq!(observed_absent.dependencies.len(), 2);
    assert!(matches!(
        observed_absent.dependencies.last(),
        Some(Dependency {
            region: DependencyRegion::DirectoryName { .. },
            expected: DependencyState::Absent,
        })
    ));
    Ok(())
}

#[tokio::test]
async fn authenticated_bindings_fail_closed_on_kind_or_record_mismatch()
-> Result<(), Box<dyn std::error::Error>> {
    for (declared, recorded) in [
        (FileKind::Regular, Some(FileKind::Directory)),
        (FileKind::Directory, Some(FileKind::Regular)),
        (FileKind::Regular, None),
    ] {
        let store = MemoryObjectStore::default();
        let (generation, path) = binding_fixture(&store, declared, recorded).await?;
        let failure = lookup_path(
            &store,
            &generation,
            &path,
            config(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("inconsistent authenticated binding resolved")?;
        assert!(matches!(failure.error, PathLookupError::KindMismatch));
        assert!(failure.work.backend_read_operations > 0);
        assert!(failure.work.page_reads > 0);

        let batch_failure = lookup_paths(
            &store,
            &generation,
            std::slice::from_ref(&path),
            config(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("batch accepted inconsistent authenticated binding")?;
        assert!(matches!(batch_failure.error, PathLookupError::KindMismatch));
        assert!(batch_failure.work.backend_read_operations > 0);

        let observed_failure = (observe_path_async(
            &store,
            &generation,
            &path,
            config(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        ))
        .await
        .err()
        .ok_or("observation accepted inconsistent binding")?;
        assert!(matches!(
            observed_failure.error,
            PathLookupError::KindMismatch
        ));
        assert!(observed_failure.work.backend_read_operations > 0);
    }
    Ok(())
}

#[tokio::test]
async fn unsupported_name_encoding_rejects_before_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, _) = fixture(&store).await?;
    let path = NamespacePath::new(
        vec![LogicalName::new(NameEncoding::PosixBytes, vec![0xff], 255)?],
        config().limits,
    )?;
    let failure = lookup_path(
        &store,
        &generation,
        &path,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("unsupported encoding succeeded")?;
    assert!(matches!(
        failure.error,
        PathLookupError::UnsupportedNameEncoding
    ));
    assert_eq!(
        *failure.work,
        WorkCounters {
            items_examined: 1,
            ..WorkCounters::UNCHARGED
        }
    );
    Ok(())
}

#[tokio::test]
async fn profile_specific_name_encodings_reach_authenticated_absence()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, _) = fixture(&store).await?;
    for (profile, encoding, bytes) in [
        (
            FilesystemProfile::Posix,
            NameEncoding::PosixBytes,
            vec![0xff],
        ),
        (
            FilesystemProfile::Windows,
            NameEncoding::WindowsUtf16Le,
            vec![b'x', 0],
        ),
    ] {
        let mut profile_config = config();
        profile_config.profile = profile;
        let candidate = NamespacePath::new(
            vec![LogicalName::new(encoding, bytes, 255)?],
            profile_config.limits,
        )?;
        let lookup = lookup_path(
            &store,
            &generation,
            &candidate,
            profile_config,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        assert!(lookup.record.is_none());
        assert_eq!(lookup.resolved_components, 0);
        assert!(lookup.work.backend_read_operations > 0);
        assert!(lookup.work.page_reads > 0);
    }
    Ok(())
}

#[tokio::test]
async fn root_observation_and_missing_root_identity_are_exact()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let (generation, path) = fixture(&store).await?;
    let root = NamespacePath::new(Vec::new(), config().limits)?;
    let lookup = lookup_path(
        &store,
        &generation,
        &root,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(lookup.parent, None);
    assert_eq!(lookup.resolved_components, 0);
    assert_eq!(
        lookup.record.map(|record| record.file_id),
        Some(generation.root_file_id)
    );

    let cancellation = CancellationToken::new();
    let observed_root = (observe_path_async(
        &store,
        &generation,
        &root,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let root_edges = (async {
        let cancellation = &cancellation;
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation);
        observe_path_async_with_accounting(
            &store,
            &generation,
            &root,
            config(),
            &mut accounting,
            cancellation,
            None,
            false,
        )
        .await
        .map(|result| result.0)
    })
    .await?;
    assert_eq!(observed_root.lookup.record, root_edges.lookup.record);
    assert_eq!(observed_root.lookup.parent, root_edges.lookup.parent);
    assert_eq!(
        observed_root.lookup.resolved_components,
        root_edges.lookup.resolved_components
    );
    assert_eq!(observed_root.dependencies.len(), 1);
    assert!(root_edges.dependencies.is_empty());

    let observed = (observe_path_async(
        &store,
        &generation,
        &path,
        config(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let edges = (async {
        let cancellation = &cancellation;
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, cancellation);
        observe_path_async_with_accounting(
            &store,
            &generation,
            &path,
            config(),
            &mut accounting,
            cancellation,
            None,
            false,
        )
        .await
        .map(|result| result.0)
    })
    .await?;
    assert_eq!(observed.lookup.record, edges.lookup.record);
    assert_eq!(observed.lookup.parent, edges.lookup.parent);
    assert_eq!(
        observed.lookup.resolved_components,
        edges.lookup.resolved_components
    );
    assert_eq!(observed.dependencies.len(), edges.dependencies.len() + 1);
    assert!(matches!(
        observed.dependencies.last(),
        Some(Dependency {
            region: DependencyRegion::FileRecord(_),
            expected: DependencyState::Present(_),
        })
    ));

    let mut missing = generation.clone();
    missing.root_file_id = FileId::from_bytes([99; 16]);
    let failure = lookup_path(
        &store,
        &missing,
        &root,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("missing root record unexpectedly succeeded")?;
    assert!(matches!(failure.error, PathLookupError::MissingRootRecord));
    assert!(failure.work.page_reads > 0);
    Ok(())
}

#[tokio::test]
async fn exact_lookup_succeeds_regardless_of_case_and_unicode_policy()
-> Result<(), Box<dyn std::error::Error>> {
    // `case_sensitivity`/`unicode` are folding/normalization policy for the
    // facade layer above this one; an exact-key lookup at this level must not
    // reject a config merely for selecting `ProfileFolded`/`RequireNfc`.
    let store = MemoryObjectStore::default();
    let (generation, path) = fixture(&store).await?;
    let mut folded = config();
    folded.case_sensitivity = CaseSensitivity::ProfileFolded;
    let mut normalized = config();
    normalized.unicode = UnicodePolicy::RequireNfc;
    for candidate in [folded, normalized] {
        let lookup = lookup_path(
            &store,
            &generation,
            &path,
            candidate,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
        assert!(lookup.record.is_some());
    }
    Ok(())
}
