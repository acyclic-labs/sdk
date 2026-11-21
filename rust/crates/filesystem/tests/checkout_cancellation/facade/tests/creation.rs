use acyclic_fs::kernel::{AttributeClass, AttributeName, FileRangeReadError};
use acyclic_fs::model::{CheckoutMode, GenerationSelector};
use acyclic_fs::{
    AuthoredMutation, ByteRange, CancellationError, CancellationToken, ContentChange, ContentTimes,
    ExtentSeekTarget, Fs, FsError, PublicationPermit,
};
use acyclic_fs_format::{
    identity::{OperationId, VolumeId},
    metadata::FileMetadata,
};
use acyclic_fs_work::performance::{OperationFailure, OperationReceipt, WorkCounters};
use acyclic_stream::OwnedBytes;

use super::B;
use super::allow_publication::allow;
use super::checkout_fixture::{config, path, path_parts};
use super::fixtures::{TestResult, create, fixture, open_checkout};

fn cancelled<T>(result: Result<OperationReceipt<T>, OperationFailure<FsError>>) -> TestResult {
    let failure = result.err().ok_or("cancelled operation succeeded")?;
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&failure.error);
    let mut cancellation = matches!(
        failure.error,
        FsError::Cancelled(_) | FsError::FileRead(FileRangeReadError::Cancelled)
    );
    while let Some(error) = source {
        cancellation |= error.is::<CancellationError>();
        source = error.source();
    }
    assert!(cancellation, "unexpected error: {:?}", failure.error);
    assert_eq!(*failure.work()?, WorkCounters::default());
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_facade_operations_perform_zero_work() -> TestResult {
    let cancel = CancellationToken::new();
    cancel.cancel();
    cancelled(
        Fs::memory()
            .create_volume_with_id(VolumeId::from_bytes([47; 16]), config(), B, &cancel)
            .await,
    )?;
    Ok(())
}

#[tokio::test]
async fn pre_cancelled_existing_volume_surfaces_fail_before_visible_work() -> TestResult {
    let active = CancellationToken::new();
    let (volume, _) = fixture(config(), &active).await?;
    let mut checkout =
        open_checkout(&volume, CheckoutMode::tracking_transaction(), &active).await?;
    let file = path("cancelled-file")?;
    let id = create(
        &mut checkout,
        "cancelled-file",
        OwnedBytes::from_static(b"abcdefgh"),
        &active,
    )
    .await?;
    let mut detached = checkout.detach_regular_file(&file, B, &active).await?.value;
    let cancel = CancellationToken::new();
    cancel.cancel();
    let range = ByteRange {
        offset: 0,
        length: 1,
    };
    let attribute = AttributeName::new(AttributeClass::PosixXattr, b"user.cancelled".to_vec())?;
    cancelled(
        volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                B,
                &cancel,
            )
            .await,
    )?;
    cancelled(checkout.lookup_no_follow(&file, B, &cancel).await)?;
    cancelled(
        checkout
            .lookup_batch_no_follow(std::slice::from_ref(&file), B, &cancel)
            .await,
    )?;
    cancelled(
        checkout
            .list_directory(&path_parts(&[])?, None, 8, B, &cancel)
            .await,
    )?;
    cancelled(checkout.read_metadata(&file, B, &cancel).await)?;
    cancelled(checkout.read_file_range(&file, range, B, &cancel).await)?;
    cancelled(
        checkout
            .plan_file_extents(&file, range, 8, B, &cancel)
            .await,
    )?;
    cancelled(
        checkout
            .seek_file_extent(&file, 0, ExtentSeekTarget::Data, B, &cancel)
            .await,
    )?;
    cancelled(checkout.read_file_record_by_id(id, B, &cancel).await)?;
    cancelled(checkout.read_file_range(id, range, B, &cancel).await)?;
    cancelled(
        checkout
            .read_named_attribute(&file, &attribute, B, &cancel)
            .await,
    )?;
    cancelled(
        checkout
            .change_content(
                file,
                ContentChange::Write {
                    offset: 0,
                    bytes: OwnedBytes::from_static(b"x"),
                },
                ContentTimes::Preserve,
                B,
                &cancel,
            )
            .await,
    )?;
    cancelled(
        checkout
            .create(
                AuthoredMutation::CreateFile {
                    file_id: None,
                    path: path("cancelled-new-file")?,
                    bytes: OwnedBytes::from_static(b"must-not-be-encoded"),
                    metadata: FileMetadata::default(),
                },
                B,
                &cancel,
            )
            .await,
    )?;
    let mut empty = std::io::Cursor::new(OwnedBytes::new());
    cancelled(checkout.stage_content(&mut empty, 1, B, &cancel).await)?;
    cancelled(
        checkout
            .commit(
                OperationId::from_bytes([138; 16]),
                acyclic_fs::PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
                B,
                &cancel,
                allow::<FsError>,
            )
            .await,
    )?;
    cancelled(detached.read_range(range, B, &cancel).await)?;
    cancelled(
        detached
            .change_content(
                ContentChange::Write {
                    offset: 0,
                    bytes: OwnedBytes::from_static(b"x"),
                },
                None,
                B,
                &cancel,
            )
            .await,
    )?;
    Ok(())
}
