use super::*;

type TestVolume = Volume<
    crate::StreamAuthorityStore<acyclic_stream::MemoryStream>,
    crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
>;
type TestCheckout = Checkout<
    crate::StreamAuthorityStore<acyclic_stream::MemoryStream>,
    crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
>;
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn fixture(
    config: VolumeConfig,
    cancel: &CancellationToken,
) -> TestResult<(TestVolume, TestCheckout)> {
    let volume = Fs::memory().create_volume(config, B, cancel).await?.value;
    let checkout = volume
        .checkout(GenerationSelector::Head, writable_pinned(), B, cancel)
        .await?
        .value;
    Ok((volume, checkout))
}
async fn create(
    checkout: &mut TestCheckout,
    name: &str,
    bytes: OwnedBytes,
    cancel: &CancellationToken,
) -> TestResult<FileId> {
    Ok(checkout
        .create(
            AuthoredMutation::CreateFile {
                path: path(name)?,
                bytes,
                metadata: FileMetadata::default(),
            },
            B,
            cancel,
        )
        .await?
        .value)
}
async fn commit(checkout: &mut TestCheckout, cancel: &CancellationToken) -> TestResult {
    checkout
        .commit(
            OperationId::new(),
            None,
            None,
            None,
            PublicationPermit::Unrestricted,
            B,
            cancel,
            allow::<FsError>,
        )
        .await?;
    Ok(())
}
async fn read(
    checkout: &mut TestCheckout,
    id: FileId,
    offset: u64,
    length: u64,
    cancel: &CancellationToken,
) -> TestResult<OwnedBytes> {
    Ok(checkout
        .read_file_range_by_id(id, ByteRange { offset, length }, B, cancel)
        .await?
        .value
        .bytes)
}

#[tokio::test]
async fn identity_writes_rebase_without_namespace_lookup_and_conflict_by_exact_range() -> TestResult
{
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    let id = create(&mut seed, "identity", vec![b'a'; 128].into(), &cancel).await?;
    commit(&mut seed, &cancel).await?;
    for (local_offset, remote_offset, safe) in [(0, 64, true), (10, 12, false)] {
        let mut local = volume
            .checkout(GenerationSelector::Head, writable_tracking(), B, &cancel)
            .await?
            .value;
        let mut remote = volume
            .checkout(GenerationSelector::Head, writable_pinned(), B, &cancel)
            .await?
            .value;
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
                    .plan_file_extents_by_id(
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
    let mut extending = volume
        .checkout(GenerationSelector::Head, writable_tracking(), B, &cancel)
        .await?
        .value;
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
            .plan_file_extents_by_id(
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
                .seek_file_extent_by_id(id, offset, target, B, &cancel)
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
            .read_named_attribute_by_id(id, &attribute, B, &cancel)
            .await?
            .value
            .is_none()
    );
    assert!(
        checkout
            .list_named_attributes_by_id(id, None, 8, B, &cancel)
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
            .change_named_attribute_by_id(
                id,
                &attribute,
                Some((&OwnedBytes::copy_from_slice(bytes), mode)),
                B,
                &cancel,
            )
            .await?;
        assert_eq!(
            checkout
                .read_named_attribute_by_id(id, &attribute, B, &cancel)
                .await?
                .value
                .unwrap()
                .bytes
                .as_ref(),
            bytes
        );
        assert_eq!(
            checkout
                .list_named_attributes_by_id(id, None, 8, B, &cancel)
                .await?
                .value
                .entries
                .len(),
            1
        );
    }
    for (mode, size) in [(0o100_640, None), (0o100_600, Some(12))] {
        metadata.posix_mode = MetadataField::Value(mode);
        checkout
            .set_attributes_by_id(id, metadata, size, B, &cancel)
            .await?;
        assert_eq!(
            checkout
                .read_metadata_by_id(id, B, &cancel)
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
        .change_named_attribute_by_id(id, &attribute, None, B, &cancel)
        .await?;
    assert!(
        checkout
            .read_named_attribute_by_id(id, &attribute, B, &cancel)
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
    let mut candidate = source.private_candidate(&original, &cancel)?;
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
    let before = candidate.pinned_reader()?;
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
        before.file_records_by_id(&[id], B, &fresh).await?.value[0].unwrap()
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
    create(&mut checkout, "bounded", OwnedBytes::new(), &cancel).await?;
    let attribute = AttributeName::new(
        crate::kernel::AttributeClass::PosixXattr,
        b"user.bounded".to_vec(),
        128,
    )?;
    checkout
        .change_named_attribute(
            path("bounded")?,
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

#[tokio::test]
async fn one_reader_answers_a_batch_of_identities() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut writer) = fixture(config(), &cancel).await?;
    let mut identities = Vec::new();
    for name in ["a", "b"] {
        let id = create(&mut writer, name, OwnedBytes::from_static(b"x"), &cancel).await?;
        identities.push(writer.read_file_record_by_id(id, B, &cancel).await?.value);
    }
    let reader = writer.pinned_reader()?;
    let records = reader
        .file_records_by_id(
            &[
                identities[1].file_id,
                FileId::from_bytes([192; 16]),
                identities[0].file_id,
            ],
            B,
            &cancel,
        )
        .await?;
    assert_eq!(
        records.value,
        vec![Some(identities[1]), None, Some(identities[0])]
    );
    Ok(())
}
