use super::*;

#[tokio::test]
async fn named_retention_preserves_the_exact_reference_without_a_cached_proof() -> TestResult {
    let cancellation = CancellationToken::new();
    let objects = Arc::new(crate::MemoryObjectStore::default());
    let authority = Arc::new(crate::MemoryAuthorityStore::default());
    let fs = Fs::new(
        authority.clone(),
        objects.clone(),
        EmbeddedCapabilities::MEMORY,
    );
    let volume = fs.create_volume(config(), B, &cancellation).await?.value;
    let checkout = volume
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::read_only_pinned(),
            B,
            &cancellation,
        )
        .await?
        .value;
    let mut work = crate::AuthorityWork::new(B, &cancellation);
    let plain = checkout.generation_id().object_id();
    let body = objects
        .read(plain, config().limits.maximum_object_bytes, &mut work)
        .await?;
    let root = crate::kernel::decode_generation_root::<CanonicalDecodeError>(
        &body.bytes,
        Default::default(),
        |_| Ok(()),
    )?;
    let mut reference = plain;
    reference.storage = Some(crate::ObjectStorage {
        locator: Digest::from_bytes([181; 32]),
        root_file_id: root.root_file_id,
        origin_generation: plain.digest,
    });
    objects.put(reference, body.bytes, &mut work).await?;
    for (label, target, succeeds) in [
        ("exact", reference, true),
        (
            "alias",
            ObjectId {
                storage: Some(crate::ObjectStorage {
                    locator: Digest::from_bytes([182; 32]),
                    ..reference.storage.ok_or("missing association")?
                }),
                ..reference
            },
            false,
        ),
    ] {
        let label = label.to_owned();
        let bytes = label.capacity() as u64;
        let result = fs
            .retain_workspace_generation(
                &volume,
                GenerationId::from_root(target),
                crate::kernel::RetentionKind::Pin,
                Owned::new(label.clone(), KernelOwnedAllocation::Standalone, bytes),
                None,
                B,
                &cancellation,
            )
            .await;
        assert_eq!(result.is_ok(), succeeds);
        if succeeds {
            let id = crate::kernel::retention_authority_id(
                volume.id(),
                crate::kernel::RetentionKind::Pin,
                &label,
            );
            let ((_, payload), _) = authority
                .head_payload(id, MAXIMUM_VOLUME_EVENT_BYTES, &mut work)
                .await?;
            let retained = crate::kernel::decode_retention_created(
                &payload.ok_or("missing retention record")?,
                MAXIMUM_VOLUME_EVENT_BYTES,
            )?;
            assert_eq!(retained.generation_root, reference);
        }
    }
    Ok(())
}

#[tokio::test]
async fn identity_writes_rebase_without_namespace_lookup_and_conflict_by_exact_range() -> TestResult
{
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    let id = create(&mut seed, "identity", vec![b'a'; 128].into(), &cancel).await?;
    commit(&mut seed, &cancel).await?;
    for (local_offset, remote_offset, safe) in [(0, 64, true), (10, 12, false)] {
        let mut local = open_checkout(&volume, writable_tracking(), &cancel).await?;
        let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
        local
            .change_content(
                id,
                ContentChange::Write {
                    offset: local_offset,
                    bytes: OwnedBytes::from_static(b"LOCAL"),
                },
                ContentTimes::Preserve,
                B,
                &cancel,
            )
            .await?;
        remote
            .change_content(
                path("identity")?,
                ContentChange::Write {
                    offset: remote_offset,
                    bytes: OwnedBytes::from_static(b"REMOTE"),
                },
                ContentTimes::Preserve,
                B,
                &cancel,
            )
            .await?;
        commit(&mut remote, &cancel).await?;
        let decision = local.rebase_head(8, B, &cancel).await?.value;
        if safe {
            assert!(matches!(decision, RebaseDecision::Safe { .. }));
            let bytes = read(&mut local, id, 0, 70, &cancel).await?;
            assert_eq!(&bytes[..5], b"LOCAL");
            assert_eq!(&bytes[64..70], b"REMOTE");
            assert!(
                local
                    .plan_file_extents(
                        id,
                        ByteRange {
                            offset: 0,
                            length: 70
                        },
                        8,
                        B,
                        &cancel
                    )
                    .await?
                    .value
                    .is_some()
            );
        } else {
            assert!(
                matches!(decision, RebaseDecision::Conflicted { ref conflicts, .. } if conflicts.iter().any(|conflict| matches!(conflict.region, DependencyRegion::ContentRange { file_id, .. } if file_id == id)))
            );
        }
    }
    let mut extending = open_checkout(&volume, writable_tracking(), &cancel).await?;
    for change in [
        ContentChange::Write {
            offset: 128,
            bytes: OwnedBytes::from_static(b"TAIL"),
        },
        ContentChange::CloneFrom {
            source: id,
            source_offset: 0,
            offset: 140,
            length: 4,
        },
    ] {
        extending
            .change_content(id, change, ContentTimes::Preserve, B, &cancel)
            .await?;
    }
    let extended = read(&mut extending, id, 128, 16, &cancel).await?;
    assert_eq!(&extended[..4], b"TAIL");
    assert_eq!(&extended[4..12], &[0; 8]);
    assert_eq!(&extended[12..], b"aaaa");
    Ok(())
}

#[tokio::test]
async fn public_identity_reads_content_metadata_and_attributes_share_one_candidate() -> TestResult {
    let cancel = CancellationToken::new();
    let mut posix = config();
    posix.profile = FilesystemProfile::Posix;
    let (_, mut checkout) = fixture(posix, &cancel).await?;
    let id = create(
        &mut checkout,
        "identity",
        OwnedBytes::from_static(b"abcdefgh"),
        &cancel,
    )
    .await?;
    let clone = create(
        &mut checkout,
        "clone",
        OwnedBytes::from_static(b"........"),
        &cancel,
    )
    .await?;
    let record = checkout.read_file_record_by_id(id, B, &cancel).await?.value;
    assert_eq!((record.file_id, record.kind), (id, FileKind::Regular));
    assert_eq!(
        read(&mut checkout, id, 2, 3, &cancel).await?.as_ref(),
        b"cde"
    );
    assert!(
        checkout
            .plan_file_extents(
                id,
                ByteRange {
                    offset: 0,
                    length: 8
                },
                8,
                B,
                &cancel
            )
            .await?
            .value
            .is_none()
    );
    for (offset, target, expected) in [
        (0, ExtentSeekTarget::Data, Some(0)),
        (0, ExtentSeekTarget::Hole, Some(8)),
        (9, ExtentSeekTarget::Data, None),
    ] {
        assert_eq!(
            checkout
                .seek_file_extent(id, offset, target, B, &cancel)
                .await?
                .value,
            expected
        );
    }
    assert_eq!(
        checkout
            .seek_file_extent(&path("identity")?, 9, ExtentSeekTarget::Data, B, &cancel)
            .await?
            .value,
        None
    );
    let mut metadata = checkout
        .read_metadata(&path("identity")?, B, &cancel)
        .await?
        .value;
    metadata.posix_mode = MetadataField::Value(0o100_660);
    checkout
        .set_attributes(path("identity")?, metadata, Some(10), B, &cancel)
        .await?;
    assert_eq!(
        read(&mut checkout, id, 8, 2, &cancel).await?.as_ref(),
        &[0, 0]
    );
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.identity".to_vec(),
        128,
    )?;
    assert!(
        checkout
            .read_named_attribute(id, &attribute, B, &cancel)
            .await?
            .value
            .is_none()
    );
    assert!(
        checkout
            .list_named_attributes(id, None, 8, B, &cancel)
            .await?
            .value
            .entries
            .is_empty()
    );
    for (bytes, mode) in [
        (b"first".as_slice(), NamedAttributeWriteMode::Create),
        (b"second".as_slice(), NamedAttributeWriteMode::Replace),
    ] {
        checkout
            .change_named_attribute(
                id,
                &attribute,
                Some((&OwnedBytes::copy_from_slice(bytes), mode)),
                B,
                &cancel,
            )
            .await?;
        assert_eq!(
            checkout
                .read_named_attribute(id, &attribute, B, &cancel)
                .await?
                .value
                .unwrap()
                .bytes
                .as_ref(),
            bytes
        );
        assert_eq!(
            checkout
                .list_named_attributes(id, None, 8, B, &cancel)
                .await?
                .value
                .entries
                .len(),
            1
        );
    }
    metadata = checkout.read_metadata(id, B, &cancel).await?.value;
    for (mode, size) in [(0o100_640, None), (0o100_600, Some(12))] {
        metadata.posix_mode = MetadataField::Value(mode);
        checkout
            .set_attributes(id, metadata, size, B, &cancel)
            .await?;
        assert_eq!(
            checkout
                .read_metadata(id, B, &cancel)
                .await?
                .value
                .posix_mode,
            MetadataField::Value(mode)
        );
    }
    for change in [
        ContentChange::Write {
            offset: 8,
            bytes: OwnedBytes::from_static(b"tail"),
        },
        ContentChange::ZeroRange {
            range: ByteRange {
                offset: 4,
                length: 2,
            },
            allocated: false,
            extend: false,
        },
        ContentChange::Preallocate {
            range: ByteRange {
                offset: 12,
                length: 4,
            },
            keep_size: false,
        },
    ] {
        checkout
            .change_content(id, change, ContentTimes::Preserve, B, &cancel)
            .await?;
    }
    checkout
        .change_content(
            clone,
            ContentChange::Resize { logical_bytes: 16 },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    checkout
        .change_content(
            clone,
            ContentChange::CloneFrom {
                source: id,
                source_offset: 0,
                offset: 4,
                length: 8,
            },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    assert_eq!(
        read(&mut checkout, id, 0, 16, &cancel).await?.as_ref(),
        b"abcd\0\0ghtail\0\0\0\0"
    );
    assert_eq!(
        read(&mut checkout, clone, 0, 16, &cancel).await?.as_ref(),
        b"....abcd\0\0gh\0\0\0\0"
    );
    checkout
        .change_named_attribute(id, &attribute, None, B, &cancel)
        .await?;
    assert!(
        checkout
            .read_named_attribute(id, &attribute, B, &cancel)
            .await?
            .value
            .is_none()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn original_identity_mutation_preserves_source_and_cancelled_candidate() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut source) = fixture(config(), &cancel).await?;
    let id = create(
        &mut source,
        "source",
        OwnedBytes::from_static(b"actual source bytes"),
        &cancel,
    )
    .await?;
    commit(&mut source, &cancel).await?;
    let original = crate::NativeMaterializationWork::new(B, &cancel).map_err(|(cause, _)| cause)?;
    let mut candidate = source.private_candidate(
        &mut crate::kernel::allocation::KernelAccounting::native(&original, &cancel),
        &cancel,
    )?;
    candidate
        .change_content(
            id,
            ContentChange::Resize { logical_bytes: 0 },
            ContentTimes::Preserve,
            &original,
            &cancel,
        )
        .await?;
    assert_eq!(
        read(&mut source, id, 0, 19, &cancel).await?.as_ref(),
        b"actual source bytes"
    );
    let before = candidate
        .read_file_record_by_id(id, B, &cancel)
        .await?
        .value;
    cancel.cancel();
    assert!(
        candidate
            .change_content(
                id,
                ContentChange::Resize { logical_bytes: 7 },
                ContentTimes::Preserve,
                &original,
                &cancel
            )
            .await
            .is_err()
    );
    let fresh = CancellationToken::new();
    assert_eq!(
        candidate.read_file_record_by_id(id, B, &fresh).await?.value,
        before
    );
    Ok(())
}

#[tokio::test]
async fn facade_kind_and_read_bounds_fail_without_creating_files() -> TestResult {
    let cancel = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 1;
    let (_, mut checkout) = fixture(limited, &cancel).await?;
    assert!(
        create(
            &mut checkout,
            "oversized",
            OwnedBytes::from_static(b"xx"),
            &cancel
        )
        .await
        .is_err()
    );
    assert!(
        checkout
            .lookup_no_follow(&path("oversized")?, B, &cancel)
            .await?
            .value
            .record
            .is_none()
    );
    checkout
        .create(
            AuthoredMutation::CreateDirectory {
                path: path("directory")?,
                metadata: FileMetadata::default(),
            },
            B,
            &cancel,
        )
        .await?;
    assert!(matches!(
        checkout
            .detach_regular_file(&path("directory")?, B, &cancel)
            .await,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::NotRegular),
            ..
        })
    ));
    Ok(())
}

#[tokio::test]
async fn detached_payload_bounds_and_authority_volume_identity_fail_closed() -> TestResult {
    let cancel = CancellationToken::new();
    let mut limited = config();
    limited.limits.maximum_read_bytes = 1;
    let (_, mut checkout) = fixture(limited, &cancel).await?;
    let id = create(&mut checkout, "bounded", OwnedBytes::new(), &cancel).await?;
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.bounded".to_vec(),
        128,
    )?;
    checkout
        .change_named_attribute(
            id,
            &attribute,
            Some((
                &OwnedBytes::from_static(b"xx"),
                NamedAttributeWriteMode::Create,
            )),
            B,
            &cancel,
        )
        .await?;
    let mut detached = checkout
        .detach_regular_file(&path("bounded")?, B, &cancel)
        .await?
        .value;
    assert!(matches!(
        detached.read_named_attribute(&attribute, B, &cancel).await,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));
    assert!(matches!(
        detached
            .change_content(
                ContentChange::Write {
                    offset: 0,
                    bytes: OwnedBytes::from_static(b"xx")
                },
                None,
                B,
                &cancel
            )
            .await,
        Err(OperationFailure {
            error: FsError::FileRead(FileRangeReadError::InvalidRange),
            ..
        })
    ));
    assert_authority_volume_identity_fails_closed().await
}
