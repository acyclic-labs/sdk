use super::*;

#[tokio::test]
async fn semantic_capabilities_and_create_identity_reject_before_backend_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
    let mut disabled = config();
    disabled.symbolic_links = false;
    let target = blob(&store, b"target").await?;
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
        &CancellationToken::new(),
    )
    .await
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
        &CancellationToken::new(),
    )
    .await
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
        &CancellationToken::new(),
    )
    .await
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
        &CancellationToken::new(),
    )
    .await
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
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("excessive endpoint count unexpectedly succeeded")?;
    assert!(matches!(
        path_failure.error,
        GenerationMutationError::TooManyPaths
    ));
    assert_eq!(*path_failure.work, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn removing_directory_after_candidate_child_insert_is_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
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
        &CancellationToken::new(),
    )
    .await
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
            &CancellationToken::new(),
        )
        .await?
        .record
        .is_some()
    );
    Ok(())
}

#[tokio::test]
async fn disabled_hard_links_reject_before_work() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
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
        &CancellationToken::new(),
    )
    .await
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
            &CancellationToken::new(),
        )
        .await?
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
            &CancellationToken::new(),
        )
        .await?
        .record
        .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn rename_replace_drops_the_destination_identity_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
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
        &CancellationToken::new(),
    )
    .await?;
    assert!(receipt.work.backend_write_operations > 0);
    assert!(
        lookup_path(
            &store,
            &receipt.root,
            &path(&["a"])?,
            config(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .record
        .is_none()
    );
    let replacement = lookup_path(
        &store,
        &receipt.root,
        &path(&["b"])?,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .record
    .ok_or("renamed destination is absent")?;
    assert_eq!(replacement.file_id, FileId::from_bytes([2; 16]));
    assert_eq!(replacement.link_count, 1);
    assert_eq!(inline_at(&store, &receipt.root, "b").await?, b"payload");
    assert!(
        lookup_file_record(
            &store,
            receipt.root.file_table,
            replaced_id,
            DecodeLimits::for_volume(config()),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value
        .is_none()
    );
    assert_eq!(
        inline_at(&store, &fixture.generation, "a").await?,
        b"payload"
    );
    Ok(())
}

#[tokio::test]
async fn directory_rename_below_itself_is_rejected_without_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
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
        &CancellationToken::new(),
    )
    .await
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
            &CancellationToken::new(),
        )
        .await?
        .record
        .is_some()
    );
    Ok(())
}

#[tokio::test]
async fn namespace_and_identity_preconditions_fail_with_exact_terminal_errors()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
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
            &CancellationToken::new(),
        )
        .await
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
        &CancellationToken::new(),
    )
    .await
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
        &CancellationToken::new(),
    )
    .await
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
        &CancellationToken::new(),
    )
    .await
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
            &CancellationToken::new(),
        )
        .await
        .err()
        .ok_or("non-regular identity operation unexpectedly succeeded")?;
        assert!(matches!(
            failure.error,
            GenerationMutationError::InconsistentState
        ));
        assert_eq!(failure.work.backend_write_operations, 0);
    }

    assert_eq!(
        inline_at(&store, &fixture.generation, "a").await?,
        b"payload"
    );
    assert!(
        lookup_path(
            &store,
            &fixture.generation,
            &path(&["first"])?,
            config(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .record
        .is_none()
    );
    Ok(())
}

