use super::*;

#[tokio::test]
async fn profile_folded_admission_covers_directories_links_and_renames() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(folded_config(), &cancel).await?;
    for (original, collision) in [
        (
            AuthoredMutation::CreateDirectory {
                path: path("Directory")?,
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateDirectory {
                path: path("directory")?,
                metadata: FileMetadata::default(),
            },
        ),
        (
            AuthoredMutation::CreateSymbolicLink {
                path: path("Link")?,
                target: OwnedBytes::from_static(b"target"),
                metadata: FileMetadata::default(),
            },
            AuthoredMutation::CreateSymbolicLink {
                path: path("link")?,
                target: OwnedBytes::from_static(b"other"),
                metadata: FileMetadata::default(),
            },
        ),
    ] {
        checkout.create(original, B, &cancel).await?;
        assert!(matches!(
            checkout.create(collision, B, &cancel).await,
            Err(OperationFailure {
                error: FsError::NameCollision,
                ..
            })
        ));
    }
    let source = create(
        &mut checkout,
        "Source",
        OwnedBytes::from_static(b"body"),
        &cancel,
    )
    .await?;
    checkout
        .mutate(
            vec![Mutation::Rename {
                source: path("Source")?,
                destination: path("SOURCE")?,
                replace: false,
            }],
            B,
            &cancel,
        )
        .await?;
    assert_eq!(
        checkout
            .lookup_no_follow(&path("source")?, B, &cancel)
            .await?
            .value
            .record
            .unwrap()
            .file_id,
        source
    );
    create(
        &mut checkout,
        "Other",
        OwnedBytes::from_static(b"other"),
        &cancel,
    )
    .await?;
    for mutation in [
        Mutation::Rename {
            source: path("SOURCE")?,
            destination: path("other")?,
            replace: false,
        },
        Mutation::Link {
            source: path("SOURCE")?,
            destination: path("source")?,
        },
    ] {
        assert!(matches!(
            checkout.mutate(vec![mutation], B, &cancel).await,
            Err(OperationFailure {
                error: FsError::NameCollision,
                ..
            })
        ));
    }
    create(
        &mut checkout,
        "Removed",
        OwnedBytes::from_static(b"removed"),
        &cancel,
    )
    .await?;
    checkout
        .mutate(
            vec![Mutation::Remove {
                path: path("Removed")?,
                expected_file_id: MetadataField::Unavailable,
            }],
            B,
            &cancel,
        )
        .await?;
    create(
        &mut checkout,
        "removed",
        OwnedBytes::from_static(b"reused"),
        &cancel,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn profile_folded_authored_batches_reject_intra_batch_collisions() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(folded_config(), &cancel).await?;
    assert!(matches!(
        checkout
            .apply_authored_transaction(
                vec![
                    AuthoredMutation::CreateDirectory {
                        path: path("Batch")?,
                        metadata: FileMetadata::default()
                    },
                    AuthoredMutation::CreateFile {
                        path: path("batch")?,
                        bytes: OwnedBytes::from_static(b"body"),
                        metadata: FileMetadata::default()
                    },
                ],
                B,
                &cancel
            )
            .await,
        Err(OperationFailure {
            error: FsError::NameCollision,
            ..
        })
    ));
    assert!(
        checkout
            .lookup_no_follow(&path("batch")?, B, &cancel)
            .await?
            .value
            .record
            .is_none()
    );
    let ordered = checkout
        .apply_authored_transaction(
            vec![
                AuthoredMutation::CreateFile {
                    path: path("Ordered")?,
                    bytes: OwnedBytes::from_static(b"body"),
                    metadata: FileMetadata::default(),
                },
                AuthoredMutation::Rename {
                    source: path("ordered")?,
                    destination: path("renamed")?,
                    replace: false,
                },
            ],
            B,
            &cancel,
        )
        .await?
        .value;
    assert_eq!(
        checkout
            .lookup_no_follow(&path("renamed")?, B, &cancel)
            .await?
            .value
            .record
            .unwrap()
            .file_id,
        ordered.created_file_ids[0].unwrap()
    );
    let nested = checkout
        .apply_authored_transaction(
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
            &cancel,
        )
        .await?
        .value;
    assert_eq!(nested.created_file_ids.len(), 2);
    assert_eq!(
        checkout
            .read_file_range(
                &path_parts(&["NESTED", "child"])?,
                ByteRange {
                    offset: 0,
                    length: 6
                },
                B,
                &cancel
            )
            .await?
            .value
            .bytes
            .as_ref(),
        b"nested"
    );
    Ok(())
}

#[tokio::test]
async fn sensitive_volume_still_permits_case_distinct_siblings() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancel).await?;
    create(
        &mut checkout,
        "Foo",
        OwnedBytes::from_static(b"first"),
        &cancel,
    )
    .await?;
    create(
        &mut checkout,
        "foo",
        OwnedBytes::from_static(b"second"),
        &cancel,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn require_nfc_volume_rejects_non_normalized_names() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(nfc_config(), &cancel).await?;
    assert!(
        create(
            &mut checkout,
            "e\u{0301}",
            OwnedBytes::from_static(b"body"),
            &cancel
        )
        .await
        .is_err()
    );
    create(
        &mut checkout,
        "\u{00e9}",
        OwnedBytes::from_static(b"body"),
        &cancel,
    )
    .await?;
    Ok(())
}
