use acyclic_fs::model::{CheckoutMode, GenerationSelector};
use acyclic_fs::{
    ByteRange, CancellationToken, ContentChange, ContentTimes, FsError,
    ResolvedFileRangeReadRequest,
};
use acyclic_fs_work::performance::WorkCounters;
use acyclic_stream::OwnedBytes;

use super::B;
use super::checkout_fixture::{config, path, path_parts};
use super::fixtures::{TestResult, create, fixture};

#[tokio::test]
async fn checkout_snapshot_reader_is_private_and_stable() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, _) = fixture(config(), &cancel).await?;
    let mut checkout = volume
        .checkout(
            GenerationSelector::Head,
            CheckoutMode::tracking_transaction(),
            B,
            &cancel,
        )
        .await?
        .value;
    let id = create(
        &mut checkout,
        "private",
        OwnedBytes::from_static(b"first"),
        &cancel,
    )
    .await?;
    let snapshot = checkout.snapshot_reader();
    let resolved = snapshot
        .resolve_files(&[path("private")?], B, &cancel)
        .await?
        .value
        .pop()
        .flatten()
        .ok_or("snapshot omitted the private file")?;
    assert_eq!(resolved.file_id(), id);
    checkout
        .change_content(
            path("private")?,
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::from_static(b"later"),
            },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    assert_eq!(
        resolved
            .read_range(
                ByteRange {
                    offset: 0,
                    length: 5
                },
                B,
                &cancel
            )
            .await?
            .value
            .bytes
            .as_ref(),
        b"first"
    );
    Ok(())
}

#[tokio::test]
async fn scoped_reads_batch_uses_each_handle_after_candidate_changes() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut checkout) = fixture(config(), &cancel).await?;
    let id = create(
        &mut checkout,
        "file",
        OwnedBytes::from_static(b"old"),
        &cancel,
    )
    .await?;
    let old = checkout.snapshot_reader();
    let old_files = old.resolve_files(&[path("file")?], B, &cancel).await?.value;
    checkout
        .change_content(
            id,
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::from_static(b"new"),
            },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    let new = checkout.snapshot_reader();
    let new_files = new.resolve_files(&[path("file")?], B, &cancel).await?.value;
    let range = ByteRange {
        offset: 0,
        length: 3,
    };
    let requests = [
        ResolvedFileRangeReadRequest {
            file: old_files
                .first()
                .and_then(Option::as_ref)
                .ok_or("old snapshot omitted the file")?,
            range,
        },
        ResolvedFileRangeReadRequest {
            file: new_files
                .first()
                .and_then(Option::as_ref)
                .ok_or("new snapshot omitted the file")?,
            range,
        },
    ];
    for concurrency in [1, 2] {
        let result = new
            .read_resolved_ranges(&requests, concurrency, B, &cancel)
            .await?
            .value;
        let [older, newer] = result.as_slice() else {
            return Err("expected two scoped reads".into());
        };
        assert_eq!(older.bytes.as_ref(), b"old");
        assert_eq!(newer.bytes.as_ref(), b"new");
    }
    Ok(())
}

#[tokio::test]
async fn reverse_pages_retain_generation_names_records_and_bodies() -> TestResult {
    let cancel = CancellationToken::new();
    let (_, mut writer) = fixture(config(), &cancel).await?;
    for key in ["a", "b", "c", "d", "e", "f"] {
        create(
            &mut writer,
            key,
            OwnedBytes::copy_from_slice(key.as_bytes()),
            &cancel,
        )
        .await?;
    }
    let reader = writer.pinned_reader()?;
    create(
        &mut writer,
        "z",
        OwnedBytes::from_static(b"new generation"),
        &cancel,
    )
    .await?;
    let mut cursor = None;
    let mut names = Vec::new();
    loop {
        let page = reader
            .list_directory_records_reverse(&path_parts(&[])?, cursor.as_ref(), 2, B, &cancel)
            .await?
            .value;
        for entry in &page.entries {
            let bytes = reader
                .read_file_range(
                    &path(std::str::from_utf8(entry.name.as_bytes())?)?,
                    ByteRange {
                        offset: 0,
                        length: 1,
                    },
                    B,
                    &cancel,
                )
                .await?
                .value;
            assert_eq!(bytes.bytes.as_ref(), entry.name.as_bytes());
            assert_eq!(
                entry.record.file_id,
                reader
                    .resolve_files(
                        &[path(std::str::from_utf8(entry.name.as_bytes())?)?],
                        B,
                        &cancel
                    )
                    .await?
                    .value
                    .first()
                    .and_then(Option::as_ref)
                    .ok_or("selected file disappeared")?
                    .file_id()
            );
        }
        cursor = page.entries.last().map(|entry| entry.name.clone());
        names.extend(
            page.entries
                .iter()
                .map(|entry| entry.name.as_bytes().to_vec()),
        );
        if !page.has_more {
            break;
        }
    }
    assert_eq!(
        names,
        [b"f", b"e", b"d", b"c", b"b", b"a"].map(|key| key.to_vec())
    );
    let latest = writer
        .pinned_reader()?
        .list_directory_records_reverse(&path_parts(&[])?, None, 1, B, &cancel)
        .await?
        .value;
    assert_eq!(
        latest
            .entries
            .first()
            .ok_or("latest page was empty")?
            .name
            .as_bytes(),
        b"z"
    );
    for (path, missing) in [(path("absent")?, true), (path("a")?, false)] {
        let error = reader
            .list_directory_records_reverse(&path, None, 1, B, &cancel)
            .await
            .err()
            .ok_or("invalid directory accepted")?
            .error;
        assert!(matches!(
            (missing, error),
            (true, FsError::NotFound) | (false, FsError::NotDirectory)
        ));
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let failed = reader
        .list_directory_records_reverse(&path_parts(&[])?, None, 1, B, &cancelled)
        .await
        .err()
        .ok_or("cancelled reverse reader completed")?;
    assert_eq!(*failed.work()?, WorkCounters::default());
    Ok(())
}
