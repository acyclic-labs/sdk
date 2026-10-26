use super::*;

#[tokio::test]
async fn malformed_file_table_preserves_nested_failure_work()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
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
            &CancellationToken::new(),
        )
        .await
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

#[tokio::test]
async fn mixed_namespace_batch_uses_candidate_state_and_preserves_base()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
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
        &CancellationToken::new(),
    )
    .await?;
    let candidate = receipt.root.clone();
    let found_new = lookup_path(
        &store,
        &candidate,
        &path(&["new"])?,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(found_new.record.map(|record| record.file_id), Some(new_id));
    let renamed = lookup_path(
        &store,
        &candidate,
        &path(&["renamed"])?,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
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
                &CancellationToken::new(),
            )
            .await?
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
            &CancellationToken::new(),
        )
        .await?
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
            &CancellationToken::new(),
        )
        .await?
        .record,
        None
    );
    assert!(receipt.work.backend_write_operations > 0);
    Ok(())
}

#[tokio::test]
async fn nested_create_uses_new_parent_without_scanning_other_subtrees()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
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
        &CancellationToken::new(),
    )
    .await?;
    let candidate = receipt.root.clone();
    assert_eq!(
        lookup_path(
            &store,
            &candidate,
            &path(&["parent", "child"])?,
            config(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .record
        .map(|record| record.file_id),
        Some(child_id)
    );
    assert!(receipt.work.page_reads < 20);
    Ok(())
}

#[tokio::test]
async fn content_executes_and_identity_rejections_precede_candidate_writes()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
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
        &CancellationToken::new(),
    )
    .await?
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
        &CancellationToken::new(),
    )
    .await?;
    let content_record = lookup_path(
        &store,
        &content_receipt.root,
        &path(&["a"])?,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
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
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("created identity collision unexpectedly succeeded")?;
    assert!(matches!(
        collision.error,
        GenerationMutationError::FileIdentityAlreadyExists
    ));
    assert_eq!(collision.work.backend_write_operations, 0);
    Ok(())
}

#[tokio::test]
async fn mixed_content_batch_observes_candidate_paths_and_hard_link_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
    let x = blob(&store, b"X").await?;
    let y = blob(&store, b"Y").await?;
    let z = blob(&store, b"Z").await?;
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
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        inline_at(&store, &receipt.root, "renamed").await?,
        b"YZyload"
    );
    assert_eq!(inline_at(&store, &receipt.root, "alias").await?, b"YZyload");
    assert_eq!(inline_at(&store, &receipt.root, "b").await?, b"X0YZ000");
    assert_eq!(
        inline_at(&store, &fixture.generation, "a").await?,
        b"payload"
    );
    Ok(())
}

#[tokio::test]
async fn identity_mutation_observes_same_batch_link_count_transitions()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
    let file_id = FileId::from_bytes([2; 16]);
    let replacement = blob(&store, b"X").await?;

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
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        inline_at(&store, &surviving_link.root, "alias").await?,
        b"Xayload"
    );
    assert!(
        lookup_path(
            &store,
            &surviving_link.root,
            &path(&["a"])?,
            config(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
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
        &CancellationToken::new(),
    )
    .await
    .err()
    .ok_or("identity mutation resurrected a final-unlinked file")?;
    assert!(matches!(
        final_unlink.error,
        GenerationMutationError::MissingSource
    ));
    assert_eq!(
        inline_at(&store, &fixture.generation, "a").await?,
        b"payload"
    );
    Ok(())
}

#[tokio::test]
async fn identity_clone_uses_two_ids_with_a_one_operation_batch_limit()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
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
        &CancellationToken::new(),
    )
    .await?;
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
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(
        inline_at(&store, &cloned.root, "destination").await?,
        b"payload"
    );
    Ok(())
}

#[tokio::test]
async fn rename_over_same_hard_link_identity_is_a_noop() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
    let linked = apply_generation_mutations(
        &store,
        &fixture.generation,
        vec![Mutation::Link {
            source: path(&["a"])?,
            destination: path(&["alias"])?,
        }],
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
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
        &CancellationToken::new(),
    )
    .await?;
    let original = lookup_path(
        &store,
        &renamed.root,
        &path(&["a"])?,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .record
    .ok_or("same-identity rename removed source alias")?;
    let alias = lookup_path(
        &store,
        &renamed.root,
        &path(&["alias"])?,
        config(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?
    .record
    .ok_or("same-identity rename removed destination alias")?;
    assert_eq!(original.file_id, alias.file_id);
    assert_eq!(original.link_count, 2);
    assert_eq!(alias.link_count, 2);
    Ok(())
}

#[tokio::test]
async fn overlapping_same_file_clone_uses_preoperation_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let fixture = fixture(&store).await?;
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
        &CancellationToken::new(),
    )
    .await?;
    assert_eq!(inline_at(&store, &receipt.root, "a").await?, b"papayld");
    assert!(receipt.work.bytes_copied >= 4);
    Ok(())
}

