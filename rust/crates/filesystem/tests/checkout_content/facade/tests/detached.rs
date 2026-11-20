use acyclic_fs::kernel::{AttributeClass, AttributeName, Mutation};
use acyclic_fs::{
    ByteRange, CancellationToken, ContentChange, ExtentSeekTarget, FsError, NamedAttributeWriteMode,
};
use acyclic_fs_format::metadata::MetadataField;
use acyclic_fs_work::performance::OperationFailure;
use acyclic_stream::OwnedBytes;

use super::B;
use super::checkout_fixture::{config, path};
use super::fixtures::{TestResult, create, fixture};

#[tokio::test]
async fn detached_open_file_remains_sparse_and_mutable_after_last_binding_removal() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancel).await?;
    let id = create(&mut checkout, "open-file", vec![b'a'; 128].into(), &cancel).await?;
    let file = path("open-file")?;
    let mut detached = checkout.detach_regular_file(&file, B, &cancel).await?.value;
    assert_eq!(detached.file_id(), id);
    for (offset, target, expected) in [
        (0, ExtentSeekTarget::Data, Some(0)),
        (0, ExtentSeekTarget::Hole, Some(128)),
        (128, ExtentSeekTarget::Data, None),
        (128, ExtentSeekTarget::Hole, Some(128)),
        (129, ExtentSeekTarget::Data, None),
        (129, ExtentSeekTarget::Hole, None),
    ] {
        assert_eq!(
            detached.seek(offset, target, B, &cancel).await?.value,
            expected
        );
    }
    let attribute = AttributeName::new(AttributeClass::PosixXattr, b"user.detached".to_vec())?;
    assert!(
        detached
            .read_named_attribute(&attribute, B, &cancel)
            .await?
            .value
            .is_none()
    );
    assert!(
        detached
            .list_named_attributes(None, 8, B, &cancel)
            .await?
            .value
            .entries
            .is_empty()
    );
    assert!(matches!(
        detached
            .change_named_attribute(&attribute, None, B, &cancel)
            .await,
        Err(OperationFailure {
            error: FsError::NotFound,
            ..
        })
    ));
    assert!(matches!(
        detached
            .change_named_attribute(
                &attribute,
                Some((
                    &OwnedBytes::from_static(b"replacement"),
                    NamedAttributeWriteMode::Replace
                )),
                B,
                &cancel
            )
            .await,
        Err(OperationFailure {
            error: FsError::CreationRejected,
            ..
        })
    ));
    for (bytes, mode) in [
        (b"first".as_slice(), NamedAttributeWriteMode::Create),
        (b"second".as_slice(), NamedAttributeWriteMode::Replace),
    ] {
        detached
            .change_named_attribute(
                &attribute,
                Some((&OwnedBytes::copy_from_slice(bytes), mode)),
                B,
                &cancel,
            )
            .await?;
        assert_eq!(
            detached
                .read_named_attribute(&attribute, B, &cancel)
                .await?
                .value
                .ok_or("missing detached file attribute")?
                .bytes
                .as_ref(),
            bytes
        );
        assert_eq!(
            detached
                .list_named_attributes(None, 8, B, &cancel)
                .await?
                .value
                .entries
                .len(),
            1
        );
        assert!(
            detached
                .change_named_attribute(
                    &attribute,
                    Some((
                        &OwnedBytes::from_static(b"duplicate"),
                        NamedAttributeWriteMode::Create
                    )),
                    B,
                    &cancel
                )
                .await
                .is_err()
        );
    }
    let mut metadata = detached.read_metadata(B, &cancel).await?.value;
    for mode in [0o100_640, 0o100_600] {
        metadata.posix_mode = MetadataField::Value(mode);
        detached.set_attributes(metadata, None, B, &cancel).await?;
    }
    assert_eq!(detached.logical_bytes(), 128);
    detached
        .set_attributes(metadata, Some(160), B, &cancel)
        .await?;
    assert_eq!(detached.logical_bytes(), 160);
    checkout
        .mutate(
            vec![Mutation::Remove {
                path: file.clone(),
                expected_file_id: MetadataField::Value(id),
            }],
            B,
            &cancel,
        )
        .await?;
    assert!(
        checkout
            .lookup_no_follow(&file, B, &cancel)
            .await?
            .value
            .record
            .is_none()
    );
    for change in [
        ContentChange::Write {
            offset: 0,
            bytes: OwnedBytes::from_static(b"open"),
        },
        ContentChange::Write {
            offset: 4,
            bytes: OwnedBytes::new(),
        },
        ContentChange::Preallocate {
            range: ByteRange {
                offset: 128,
                length: 64,
            },
            keep_size: false,
        },
        ContentChange::ZeroRange {
            range: ByteRange {
                offset: 64,
                length: 16,
            },
            allocated: false,
            extend: false,
        },
    ] {
        detached.change_content(change, None, B, &cancel).await?;
    }
    let read = detached
        .read_range(
            ByteRange {
                offset: 0,
                length: 80,
            },
            B,
            &cancel,
        )
        .await?
        .value;
    assert_eq!(read.bytes.get(..4), Some(b"open".as_slice()));
    assert_eq!(read.bytes.get(64..80), Some([0; 16].as_slice()));
    detached
        .change_content(
            ContentChange::Resize { logical_bytes: 192 },
            None,
            B,
            &cancel,
        )
        .await?;
    assert_eq!(detached.logical_bytes(), 192);
    detached
        .change_named_attribute(&attribute, None, B, &cancel)
        .await?;
    assert!(
        detached
            .change_named_attribute(&attribute, None, B, &cancel)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn detached_inline_file_seek_and_attribute_absence_are_exact() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancel).await?;
    create(
        &mut checkout,
        "inline-detached",
        OwnedBytes::from_static(b"abc"),
        &cancel,
    )
    .await?;
    let mut detached = checkout
        .detach_regular_file(&path("inline-detached")?, B, &cancel)
        .await?
        .value;
    assert_eq!(detached.logical_bytes(), 3);
    for (offset, target, expected) in [
        (0, ExtentSeekTarget::Data, Some(0)),
        (3, ExtentSeekTarget::Data, None),
        (0, ExtentSeekTarget::Hole, Some(3)),
        (3, ExtentSeekTarget::Hole, Some(3)),
        (4, ExtentSeekTarget::Hole, None),
    ] {
        assert_eq!(
            detached.seek(offset, target, B, &cancel).await?.value,
            expected
        );
    }
    let attribute = AttributeName::new(AttributeClass::PosixXattr, b"user.inline".to_vec())?;
    detached
        .change_named_attribute(
            &attribute,
            Some((
                &OwnedBytes::from_static(b"value"),
                NamedAttributeWriteMode::Create,
            )),
            B,
            &cancel,
        )
        .await?;
    detached
        .change_named_attribute(&attribute, None, B, &cancel)
        .await?;
    assert!(
        detached
            .read_named_attribute(&attribute, B, &cancel)
            .await?
            .value
            .is_none()
    );
    Ok(())
}
