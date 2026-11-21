use acyclic_fs::{
    AuthoredMutation, ByteRange, CancellationToken, ContentChange, ContentTimes, FsError,
    LivePublicationObservation, PublicationPermit, StagedContent,
};
use acyclic_fs_format::identity::{FileId, OperationId};
use acyclic_fs_format::metadata::{FileMetadata, MetadataField};
use acyclic_stream::OwnedBytes;

use super::checkout_fixture::{config, path, writable_pinned};
use super::fixtures::{TestCheckout, TestResult, create, fixture, open_checkout};
use super::volume_commit::commit;
use super::volume_read::read;
use super::{B, allow};

#[tokio::test]
async fn stamped_content_change_equals_the_change_then_its_stamp_in_one_mutation() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    let id = create(&mut seed, "timed", vec![b'a'; 128].into(), &cancel).await?;
    create(&mut seed, "untimed", vec![b'b'; 128].into(), &cancel).await?;
    let times = FileMetadata {
        modified_ns: MetadataField::Value(1),
        accessed_ns: MetadataField::Value(2),
        changed_ns: MetadataField::Value(3),
        ..FileMetadata::default()
    };
    seed.set_attributes(id, times, None, B, &cancel).await?;
    commit(&mut seed, &cancel).await?;
    let mut stamped = open_checkout(&volume, writable_pinned(), &cancel).await?;
    let mut separate = open_checkout(&volume, writable_pinned(), &cancel).await?;
    for (checkout, stamp) in [
        (&mut stamped, ContentTimes::Stamp(99)),
        (&mut separate, ContentTimes::Preserve),
    ] {
        checkout
            .change_content(
                id,
                ContentChange::Write {
                    offset: 4,
                    bytes: OwnedBytes::from_static(b"STAMP"),
                },
                stamp,
                B,
                &cancel,
            )
            .await?;
    }
    let mut expected = separate.read_metadata(id, B, &cancel).await?.value;
    assert_eq!(expected, times);
    assert!(expected.stamp_content_change(99));
    separate
        .set_attributes(id, expected, None, B, &cancel)
        .await?;
    assert_eq!(stamped.root(), separate.root());
    assert_eq!(
        read(&mut stamped, id, 0, 128, &cancel).await?,
        read(&mut separate, id, 0, 128, &cancel).await?
    );
    assert_eq!(stamped.read_metadata(id, B, &cancel).await?.value, expected);
    let before = read(&mut stamped, id, 0, 128, &cancel).await?;
    assert!(
        stamped
            .change_content(
                id,
                ContentChange::CloneFrom {
                    source: FileId::from_bytes([200; 16]),
                    source_offset: 0,
                    offset: 0,
                    length: 1
                },
                ContentTimes::Stamp(100),
                B,
                &cancel
            )
            .await
            .is_err()
    );
    assert_eq!(read(&mut stamped, id, 0, 128, &cancel).await?, before);
    assert_eq!(stamped.read_metadata(id, B, &cancel).await?.value, expected);
    let untimed = path("untimed")?;
    let before = stamped.read_metadata(&untimed, B, &cancel).await?.value;
    stamped
        .change_content(
            untimed.clone(),
            ContentChange::Resize { logical_bytes: 7 },
            ContentTimes::Stamp(101),
            B,
            &cancel,
        )
        .await?;
    assert_eq!(
        stamped.read_metadata(&untimed, B, &cancel).await?.value,
        before
    );
    Ok(())
}

async fn stage(
    checkout: &TestCheckout,
    body: &'static [u8],
    cancel: &CancellationToken,
) -> TestResult<StagedContent> {
    let mut source = std::io::Cursor::new(OwnedBytes::from_static(body));
    Ok(checkout
        .stage_content(&mut source, 64, B, cancel)
        .await?
        .value)
}
async fn rewrite(
    checkout: &mut TestCheckout,
    body: &'static [u8],
    cancel: &CancellationToken,
) -> TestResult {
    let content = stage(checkout, body, cancel).await?;
    checkout
        .apply_authored_transaction(
            vec![
                AuthoredMutation::Resize {
                    path: path("large")?,
                    logical_bytes: 0,
                },
                AuthoredMutation::Resize {
                    path: path("large")?,
                    logical_bytes: 32,
                },
                AuthoredMutation::WriteFromContent {
                    path: path("large")?,
                    offset: 0,
                    content,
                },
            ],
            B,
            cancel,
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn whole_file_rewrites_remain_generation_fenced() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    let content = stage(&seed, b"0123456789abcdef0123456789abcdef", &cancel).await?;
    seed.apply_authored_transaction(
        vec![AuthoredMutation::CreateFileFromContent {
            path: path("large")?,
            content,
            metadata: FileMetadata::default(),
            file_id: None,
        }],
        B,
        &cancel,
    )
    .await?;
    commit(&mut seed, &cancel).await?;
    let mut replacement = open_checkout(&volume, writable_pinned(), &cancel).await?;
    rewrite(
        &mut replacement,
        b"ZYXWVUTSRQPONMLKzyxwvutsrqponmlk",
        &cancel,
    )
    .await?;
    commit(&mut replacement, &cancel).await?;
    assert_eq!(
        replacement
            .read_file_range(
                &path("large")?,
                ByteRange {
                    offset: 24,
                    length: 8
                },
                B,
                &cancel
            )
            .await?
            .value
            .bytes
            .as_ref(),
        b"rqponmlk"
    );
    let mut first = open_checkout(&volume, writable_pinned(), &cancel).await?;
    let mut stale = open_checkout(&volume, writable_pinned(), &cancel).await?;
    rewrite(&mut first, b"FIRSTFIRSTFIRSTFIRSTFIRSTFIRST12", &cancel).await?;
    assert!(matches!(
        first
            .commit(
                OperationId::new(),
                acyclic_fs::PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
                B,
                &cancel,
                allow::<FsError>
            )
            .await?
            .value,
        LivePublicationObservation::Committed { .. }
    ));
    rewrite(&mut stale, b"STALESTALESTALESTALESTALESTALE12", &cancel).await?;
    assert!(matches!(
        stale
            .commit(
                OperationId::new(),
                acyclic_fs::PublicationIntent::guarded(None, PublicationPermit::Unrestricted),
                B,
                &cancel,
                allow::<FsError>
            )
            .await?
            .value,
        LivePublicationObservation::Conflict { .. }
    ));
    Ok(())
}
