use super::*;

#[tokio::test]
async fn profile_folded_admission_covers_directories_links_and_renames()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation))
        .await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;

    (checkout.create(
        crate::AuthoredMutation::CreateDirectory {
            path: path("Directory")?,
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let directory_collision = (checkout.create(
        crate::AuthoredMutation::CreateDirectory {
            path: path("directory")?,
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("directory collision unexpectedly succeeded")?;
    assert!(matches!(directory_collision.error, FsError::NameCollision));

    (checkout.create(
        crate::AuthoredMutation::CreateSymbolicLink {
            path: path("Link")?,
            target: (bytes::Bytes::from_static(b"target")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let link_collision = (checkout.create(
        crate::AuthoredMutation::CreateSymbolicLink {
            path: path("link")?,
            target: (bytes::Bytes::from_static(b"other")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("link collision unexpectedly succeeded")?;
    assert!(matches!(link_collision.error, FsError::NameCollision));

    let source = (checkout.create(
        crate::AuthoredMutation::CreateFile {
            path: path("Source")?,
            bytes: (bytes::Bytes::from_static(b"body")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    (checkout.mutate(
        vec![crate::kernel::Mutation::Rename {
            source: path("Source")?,
            destination: path("SOURCE")?,
            replace: false,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let renamed =
        (checkout.lookup_no_follow(&path("source")?, WorkBudget::UNBOUNDED, &cancellation))
            .await?
            .value
            .record
            .ok_or("renamed source absent")?;
    assert_eq!(renamed.file_id, source);

    (checkout.create(
        crate::AuthoredMutation::CreateFile {
            path: path("Other")?,
            bytes: (bytes::Bytes::from_static(b"other")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert!(matches!(
        (checkout.create(
            crate::AuthoredMutation::CreateFile {
                path: path("other")?,
                bytes: bytes::Bytes::from_static(b"collision").into(),
                metadata: crate::kernel::FileMetadata::default(),
            },
            WorkBudget::UNBOUNDED,
            &cancellation
        ))
        .await,
        Err(OperationFailure {
            error: FsError::NameCollision,
            ..
        })
    ));
    let rename_collision = (checkout.mutate(
        vec![crate::kernel::Mutation::Rename {
            source: path("SOURCE")?,
            destination: path("other")?,
            replace: false,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("rename collision unexpectedly succeeded")?;
    assert!(matches!(rename_collision.error, FsError::NameCollision));

    let link_destination_collision = (checkout.mutate(
        vec![crate::kernel::Mutation::Link {
            source: path("SOURCE")?,
            destination: path("source")?,
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("same-fold link unexpectedly succeeded")?;
    assert!(matches!(
        link_destination_collision.error,
        FsError::NameCollision
    ));

    (checkout.create(
        crate::AuthoredMutation::CreateFile {
            path: path("Removed")?,
            bytes: (bytes::Bytes::from_static(b"removed")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    (checkout.mutate(
        vec![crate::kernel::Mutation::Remove {
            path: path("Removed")?,
            expected_file_id: (None).map_or(
                crate::kernel::MetadataField::Unavailable,
                crate::kernel::MetadataField::Value,
            ),
        }],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    (checkout.create(
        crate::AuthoredMutation::CreateFile {
            path: path("removed")?,
            bytes: (bytes::Bytes::from_static(b"reused")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    Ok(())
}

#[tokio::test]
async fn profile_folded_authored_batches_reject_intra_batch_collisions()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(folded_config(), WorkBudget::UNBOUNDED, &cancellation))
        .await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let failure = (checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: path("Batch")?,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: path("batch")?,
                bytes: OwnedBytes::from_static(b"body"),
                metadata: FileMetadata::default(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("authored collision unexpectedly succeeded")?;
    assert!(matches!(failure.error, FsError::NameCollision));
    assert!(!checkout.has_pending_mutations());

    let ordered_aliases = (checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateFile {
                path: path("Source")?,
                bytes: OwnedBytes::from_static(b"source"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::Rename {
                source: path("source")?,
                destination: path("Renamed")?,
                replace: false,
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    let created_id = ordered_aliases.value.created_file_ids[0].ok_or("created id missing")?;
    let renamed =
        (checkout.lookup_no_follow(&path("renamed")?, WorkBudget::UNBOUNDED, &cancellation))
            .await?;
    assert_eq!(
        renamed
            .value
            .record
            .ok_or("renamed record missing")?
            .file_id,
        created_id
    );

    let nested = (checkout.apply_authored_transaction(
        vec![
            AuthoredMutation::CreateDirectory {
                path: path("Nested")?,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateFile {
                path: path_parts(&["nested", "Child"])?,
                bytes: OwnedBytes::from_static(b"nested"),
                metadata: FileMetadata::default(),
            },
        ],
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(nested.value.created_file_ids.len(), 2);
    let nested_read = (checkout.read_file_range(
        &path_parts(&["NESTED", "child"])?,
        ByteRange {
            offset: 0,
            length: 6,
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    assert_eq!(nested_read.value.bytes.as_ref(), b"nested");
    Ok(())
}

#[tokio::test]
async fn sensitive_volume_still_permits_case_distinct_siblings()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(config(), WorkBudget::UNBOUNDED, &cancellation))
        .await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    (checkout.create(
        crate::AuthoredMutation::CreateFile {
            path: path("Foo")?,
            bytes: (bytes::Bytes::from_static(b"first")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    (checkout.create(
        crate::AuthoredMutation::CreateFile {
            path: path("foo")?,
            bytes: (bytes::Bytes::from_static(b"second")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    Ok(())
}

#[tokio::test]
async fn require_nfc_volume_rejects_non_normalized_names() -> Result<(), Box<dyn std::error::Error>>
{
    let precomposed = "\u{00e9}";
    let decomposed = "e\u{0301}";
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(nfc_config(), WorkBudget::UNBOUNDED, &cancellation))
        .await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?
    .value;
    let rejected = (checkout.create(
        crate::AuthoredMutation::CreateFile {
            path: path(decomposed)?,
            bytes: (bytes::Bytes::from_static(b"body")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await;
    assert!(
        rejected.is_err(),
        "a non-normalized (NFD) name must be rejected under RequireNfc"
    );
    (checkout.create(
        crate::AuthoredMutation::CreateFile {
            path: path(precomposed)?,
            bytes: (bytes::Bytes::from_static(b"body")).into(),
            metadata: crate::kernel::FileMetadata::default(),
        },
        WorkBudget::UNBOUNDED,
        &cancellation,
    ))
    .await?;
    Ok(())
}
