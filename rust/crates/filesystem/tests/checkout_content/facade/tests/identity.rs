use acyclic_fs::kernel::{
    AttributeClass, AttributeName, FileKind, FileRangeReadError, RebaseDecision,
};
use acyclic_fs::{
    AuthoredMutation, ByteRange, CancellationToken, ContentChange, ContentTimes, DependencyRegion,
    ExtentSeekTarget, FsError, NamedAttributeWriteMode, OperationFailure,
};
use acyclic_fs_format::metadata::{FileMetadata, MetadataField};
use acyclic_fs_format::model::CheckoutMode;
use acyclic_fs_format::model::FilesystemProfile;
use acyclic_stream::OwnedBytes;

use super::B;
use super::checkout_fixture::{config, path, writable_pinned};
use super::fixtures::{TestResult, create, fixture, open_checkout};
use super::volume_commit::commit;
use super::volume_read::read;

#[tokio::test]
async fn identity_writes_rebase_without_namespace_lookup_and_conflict_by_exact_range() -> TestResult
{
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    let id = create(&mut seed, "identity", vec![b'a'; 128].into(), &cancel).await?;
    commit(&mut seed, &cancel).await?;
    for (local_offset, remote_offset, safe) in [(0, 64, true), (10, 12, false)] {
        let mut local =
            open_checkout(&volume, CheckoutMode::tracking_transaction(), &cancel).await?;
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
        let decision = local.rebase_head(Some(8), B, &cancel).await?.value;
        if safe {
            assert!(matches!(decision, RebaseDecision::Safe { .. }));
            let bytes = read(&mut local, id, 0, 70, &cancel).await?;
            assert_eq!(bytes.get(..5), Some(b"LOCAL".as_slice()));
            assert_eq!(bytes.get(64..70), Some(b"REMOTE".as_slice()));
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
    let mut extending =
        open_checkout(&volume, CheckoutMode::tracking_transaction(), &cancel).await?;
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
    assert_eq!(extended.as_ref(), b"TAIL\0\0\0\0\0\0\0\0aaaa");
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
    let attribute = AttributeName::new(AttributeClass::PosixXattr, b"user.identity".to_vec())?;
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
                .ok_or("missing named attribute")?
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

#[tokio::test]
async fn facade_kind_guards_reject_detaching_directories() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancel).await?;
    checkout
        .create(
            AuthoredMutation::CreateDirectory {
                file_id: None,
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
