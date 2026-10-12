use super::*;

#[tokio::test]
async fn profile_folded_admission_covers_directories_links_and_renames()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let volume = (fs.create_volume(folded_config(), B, &cancellation))
        .await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;

    (checkout.create(
        AuthoredMutation::CreateDirectory {
            path: path("Directory")?,
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let directory_collision = (checkout.create(
        AuthoredMutation::CreateDirectory {
            path: path("directory")?,
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("directory collision unexpectedly succeeded")?;
    assert!(matches!(directory_collision.error, FsError::NameCollision));

    (checkout.create(
        AuthoredMutation::CreateSymbolicLink {
            path: path("Link")?,
            target: OwnedBytes::from_static(b"target"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let link_collision = (checkout.create(
        AuthoredMutation::CreateSymbolicLink {
            path: path("link")?,
            target: OwnedBytes::from_static(b"other"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("link collision unexpectedly succeeded")?;
    assert!(matches!(link_collision.error, FsError::NameCollision));

    let source = (checkout.create(
        AuthoredMutation::CreateFile {
            path: path("Source")?,
            bytes: OwnedBytes::from_static(b"body"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?
    .value;
    (checkout.mutate(
        vec![Mutation::Rename {
            source: path("Source")?,
            destination: path("SOURCE")?,
            replace: false,
        }],
        B,
        &cancellation,
    ))
    .await?;
    let renamed = (checkout.lookup_no_follow(&path("source")?, B, &cancellation))
        .await?
        .value
        .record
        .ok_or("renamed source absent")?;
    assert_eq!(renamed.file_id, source);

    (checkout.create(
        AuthoredMutation::CreateFile {
            path: path("Other")?,
            bytes: OwnedBytes::from_static(b"other"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    let rename_collision = (checkout.mutate(
        vec![Mutation::Rename {
            source: path("SOURCE")?,
            destination: path("other")?,
            replace: false,
        }],
        B,
        &cancellation,
    ))
    .await
    .err()
    .ok_or("rename collision unexpectedly succeeded")?;
    assert!(matches!(rename_collision.error, FsError::NameCollision));

    let link_destination_collision = (checkout.mutate(
        vec![Mutation::Link {
            source: path("SOURCE")?,
            destination: path("source")?,
        }],
        B,
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
        AuthoredMutation::CreateFile {
            path: path("Removed")?,
            bytes: OwnedBytes::from_static(b"removed"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    (checkout.mutate(
        vec![Mutation::Remove {
            path: path("Removed")?,
            expected_file_id: None.map_or(MetadataField::Unavailable, MetadataField::Value),
        }],
        B,
        &cancellation,
    ))
    .await?;
    (checkout.create(
        AuthoredMutation::CreateFile {
            path: path("removed")?,
            bytes: OwnedBytes::from_static(b"reused"),
            metadata: FileMetadata::default(),
        },
        B,
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
    let volume = (fs.create_volume(folded_config(), B, &cancellation))
        .await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
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
        B,
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
        B,
        &cancellation,
    ))
    .await?;
    let created_id = ordered_aliases.value.created_file_ids[0].ok_or("created id missing")?;
    let renamed = (checkout.lookup_no_follow(&path("renamed")?, B, &cancellation)).await?;
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
        B,
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
        B,
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
    let volume = (fs.create_volume(config(), B, &cancellation)).await?.value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    (checkout.create(
        AuthoredMutation::CreateFile {
            path: path("Foo")?,
            bytes: OwnedBytes::from_static(b"first"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    (checkout.create(
        AuthoredMutation::CreateFile {
            path: path("foo")?,
            bytes: OwnedBytes::from_static(b"second"),
            metadata: FileMetadata::default(),
        },
        B,
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
    let volume = (fs.create_volume(nfc_config(), B, &cancellation))
        .await?
        .value;
    let mut checkout = (volume.checkout(
        GenerationSelector::Head,
        writable_pinned(),
        B,
        &cancellation,
    ))
    .await?
    .value;
    let rejected = (checkout.create(
        AuthoredMutation::CreateFile {
            path: path(decomposed)?,
            bytes: OwnedBytes::from_static(b"body"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await;
    assert!(
        rejected.is_err(),
        "a non-normalized (NFD) name must be rejected under RequireNfc"
    );
    (checkout.create(
        AuthoredMutation::CreateFile {
            path: path(precomposed)?,
            bytes: OwnedBytes::from_static(b"body"),
            metadata: FileMetadata::default(),
        },
        B,
        &cancellation,
    ))
    .await?;
    Ok(())
}
