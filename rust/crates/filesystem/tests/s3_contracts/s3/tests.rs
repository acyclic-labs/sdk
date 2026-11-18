use acyclic_fs::s3::{S3Error, S3ListCursor, S3ListOptions, S3MultipartOptions};
use acyclic_fs::{CancellationToken, Fs, IdempotencyKey, TransactionCommit};
use acyclic_fs_work::performance::WorkBudget;
use bytes::Bytes;
use std::collections::BTreeSet;

#[tokio::test]
async fn s3_view_is_the_same_generation_and_copy_is_body_free()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("s3", WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value;
    let s3 = workspace.s3();
    let other = fs
        .create_workspace(
            "another-s3-workspace",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let mut foreign = other
        .begin_transaction(
            IdempotencyKey::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    assert!(matches!(
        s3.write_staged(&mut foreign, "foreign", &[]).await,
        Err(S3Error::ForeignTransaction)
    ));
    s3.put_object(
        "src/a.bin",
        Bytes::from_static(b"abcdef"),
        IdempotencyKey::new(),
    )
    .await?;
    s3.put_object(
        "root.bin",
        Bytes::from_static(b"root"),
        IdempotencyKey::new(),
    )
    .await?;
    assert_eq!(
        s3.get_object_range(
            "src/a.bin",
            ByteRange {
                offset: 2,
                length: 3
            }
        )
        .await?
        .as_slice(),
        b"cde".as_slice()
    );
    s3.copy_object("src/a.bin", "copy/a.bin", IdempotencyKey::new())
        .await?;
    assert_eq!(
        workspace
            .read(
                "/copy/a.bin",
                16,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_slice(),
        b"abcdef".as_slice()
    );

    let listing = Box::pin(s3.list_objects(S3ListOptions::default())).await?;
    assert_eq!(listing.common_prefixes, vec!["copy/", "src/"]);
    assert_eq!(
        listing
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec!["root.bin"]
    );
    let flat = Box::pin(s3.list_objects(S3ListOptions {
        delimiter: None,
        ..S3ListOptions::default()
    }))
    .await?;
    assert_eq!(
        flat.objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec!["copy/a.bin", "root.bin", "src/a.bin"]
    );
    s3.delete_objects(
        &["copy/a.bin".to_owned(), "root.bin".to_owned()],
        IdempotencyKey::new(),
    )
    .await?;
    assert!(matches!(
        s3.head_object("root.bin").await,
        Err(S3Error::NotFound)
    ));
    s3.delete_objects(
        &["copy/a.bin".to_owned(), "root.bin".to_owned()],
        IdempotencyKey::new(),
    )
    .await?;
    s3.delete_object("never-existed", IdempotencyKey::new())
        .await?;
    Ok(())
}

#[tokio::test]
async fn delimiter_pages_preserve_order_and_exact_resume() -> Result<(), Box<dyn std::error::Error>>
{
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "delimiter-pages",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let s3 = workspace.s3();
    for key in ["a/one", "a/two", "b/one", "c"] {
        s3.put_object(key, Bytes::from_static(b"x"), IdempotencyKey::new())
            .await?;
    }
    let mut options = S3ListOptions {
        delimiter: Some('/'),
        maximum_keys: 1,
        ..S3ListOptions::default()
    };
    let mut entries = Vec::new();
    let mut complete = false;
    for _ in 0..4 {
        let page = s3.list_objects(options.clone()).await?;
        entries.extend(page.objects.into_iter().map(|object| (object.key, false)));
        entries.extend(
            page.common_prefixes
                .into_iter()
                .map(|prefix| (prefix, true)),
        );
        let Some(cursor) = page.next_continuation else {
            complete = true;
            break;
        };
        options.continuation = Some(S3ListCursor::decode(&cursor.encode())?);
    }
    assert!(complete, "listing did not terminate within four pages");
    assert_eq!(
        entries,
        [
            ("a/".to_owned(), true),
            ("b/".to_owned(), true),
            ("c".to_owned(), false)
        ]
    );
    Ok(())
}

#[tokio::test]
async fn listing_pages_use_flat_key_order_without_scanning_every_entry()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "lazy-listing",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let s3 = workspace.s3();
    for key in ["a/x", "a-", "a.", "a0", "b", "z"] {
        s3.put_object(key, Bytes::from_static(b"x"), IdempotencyKey::new())
            .await?;
    }
    for index in 0..32 {
        s3.put_object(
            &format!("zz{index:02}"),
            Bytes::from_static(b"x"),
            IdempotencyKey::new(),
        )
        .await?;
    }
    for (delimiter, expected) in [
        (None, ["a-", "a.", "a/x", "a0", "b", "z"]),
        (Some('/'), ["a-", "a.", "a/", "a0", "b", "z"]),
    ] {
        let mut options = S3ListOptions {
            delimiter,
            maximum_keys: 1,
            ..S3ListOptions::default()
        };
        let mut observed = Vec::new();
        for page_index in 0..6 {
            let page = s3.list_objects(options.clone()).await?;
            if page_index == 0 {
                assert!(page.entries_examined < 38);
            }
            observed.extend(page.objects.into_iter().map(|object| object.key));
            observed.extend(page.common_prefixes);
            options.continuation = page.next_continuation;
        }
        assert_eq!(observed, expected);
    }
    Ok(())
}

#[tokio::test]
async fn listing_matches_an_independent_key_model() -> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "listing-model",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let s3 = workspace.s3();
    let keys = [
        "a/x",
        "a.",
        "a-",
        "a0",
        "b",
        "z",
        "zz/one",
        "zz/two",
        "ünicode/α",
    ];
    for key in keys {
        s3.put_object(key, Bytes::from_static(b"x"), IdempotencyKey::new())
            .await?;
    }
    for prefix in ["", "a", "a/", "zz", "ü"] {
        for delimiter in [None, Some('/')] {
            let expected = keys
                .iter()
                .filter_map(|key| {
                    let suffix = key.strip_prefix(prefix)?;
                    Some(match (delimiter, suffix.split_once('/')) {
                        (Some('/'), Some((head, _))) => (format!("{prefix}{head}/"), true),
                        _ => ((*key).to_owned(), false),
                    })
                })
                .collect::<BTreeSet<_>>();
            for maximum_keys in [1, 2, 4] {
                let mut options = S3ListOptions {
                    prefix: prefix.to_owned(),
                    delimiter,
                    maximum_keys,
                    ..S3ListOptions::default()
                };
                let mut actual = Vec::new();
                for _ in 0..=expected.len() {
                    let page = s3.list_objects(options.clone()).await?;
                    let mut entries = page
                        .objects
                        .into_iter()
                        .map(|object| (object.key, false))
                        .chain(page.common_prefixes.into_iter().map(|name| (name, true)))
                        .collect::<Vec<_>>();
                    entries.sort();
                    actual.extend(entries);
                    let Some(cursor) = page.next_continuation else {
                        break;
                    };
                    options.continuation = Some(S3ListCursor::decode(&cursor.encode())?);
                }
                assert_eq!(actual, expected.iter().cloned().collect::<Vec<_>>());
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn listing_uses_fetched_witness_before_requesting_another_page()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "listing-witness",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let s3 = workspace.s3();
    for key in ["a", "b", "c", "d", "p/x"] {
        s3.put_object(key, Bytes::from_static(b"x"), IdempotencyKey::new())
            .await?;
    }
    let first = s3
        .list_objects(S3ListOptions {
            delimiter: None,
            maximum_keys: 1,
            ..S3ListOptions::default()
        })
        .await?;
    assert_eq!(
        first
            .objects
            .iter()
            .map(|item| item.key.as_str())
            .collect::<Vec<_>>(),
        ["a"]
    );
    assert_eq!(first.entries_examined, 2);
    assert!(first.next_continuation.is_some());
    let two = s3
        .list_objects(S3ListOptions {
            delimiter: Some('/'),
            maximum_keys: 2,
            ..S3ListOptions::default()
        })
        .await?;
    assert_eq!(
        two.objects
            .iter()
            .map(|item| item.key.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(two.entries_examined, 3);
    assert!(two.next_continuation.is_some());

    let workspace = fs
        .create_workspace(
            "listing-prefix-witness",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let s3 = workspace.s3();
    for key in ["a", "b/x", "c", "d"] {
        s3.put_object(key, Bytes::from_static(b"x"), IdempotencyKey::new())
            .await?;
    }
    let with_prefix = s3
        .list_objects(S3ListOptions {
            maximum_keys: 2,
            ..S3ListOptions::default()
        })
        .await?;
    assert_eq!(
        with_prefix
            .objects
            .iter()
            .map(|item| item.key.as_str())
            .collect::<Vec<_>>(),
        ["a"]
    );
    assert_eq!(with_prefix.common_prefixes, ["b/"]);
    assert_eq!(with_prefix.entries_examined, 4);
    assert!(with_prefix.next_continuation.is_some());
    Ok(())
}

#[tokio::test]
async fn flat_continuations_seek_without_replaying_earlier_pages()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "flat-seek",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let s3 = workspace.s3();
    for index in 0..32 {
        s3.put_object(
            &format!("key{index:03}"),
            Bytes::from_static(b"x"),
            IdempotencyKey::new(),
        )
        .await?;
    }
    let mut options = S3ListOptions {
        delimiter: None,
        maximum_keys: 1,
        ..S3ListOptions::default()
    };
    for index in 0..32 {
        let page = s3.list_objects(options.clone()).await?;
        assert!(page.entries_examined <= 3);
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].key, format!("key{index:03}"));
        options.continuation = page.next_continuation;
    }
    assert!(options.continuation.is_none());
    Ok(())
}

#[tokio::test]
async fn object_cursor_seeks_past_dense_predecessor_keys() -> Result<(), Box<dyn std::error::Error>>
{
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "dense-list-cursor",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let s3 = workspace.s3();
    for index in 0..32 {
        s3.put_object(
            &format!("a9{index:03}"),
            Bytes::from_static(b"x"),
            IdempotencyKey::new(),
        )
        .await?;
    }
    for key in ["a:", "a;"] {
        s3.put_object(key, Bytes::from_static(b"x"), IdempotencyKey::new())
            .await?;
    }
    let first = s3
        .list_objects(S3ListOptions {
            delimiter: None,
            maximum_keys: 33,
            ..S3ListOptions::default()
        })
        .await?;
    assert_eq!(
        first.objects.last().map(|object| object.key.as_str()),
        Some("a:")
    );
    let page = s3
        .list_objects(S3ListOptions {
            delimiter: None,
            maximum_keys: 1,
            continuation: first.next_continuation,
            ..S3ListOptions::default()
        })
        .await?;
    assert_eq!(page.objects[0].key, "a;");
    assert!(page.entries_examined <= 2);
    Ok(())
}

#[tokio::test]
async fn listing_descends_to_the_exact_prefix_frontier_without_unrelated_subtrees()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "prefix-frontier",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let s3 = workspace.s3();
    for index in 0..32 {
        s3.put_object(
            &format!("unrelated-{index:02}/value"),
            Bytes::from_static(b"x"),
            IdempotencyKey::new(),
        )
        .await?;
    }
    s3.put_object(
        "target/value",
        Bytes::from_static(b"wanted"),
        IdempotencyKey::new(),
    )
    .await?;

    let selected = Box::pin(s3.list_objects(S3ListOptions {
        prefix: "target/".to_owned(),
        delimiter: None,
        ..S3ListOptions::default()
    }))
    .await?;
    assert_eq!(selected.entries_examined, 1);
    assert_eq!(selected.objects.len(), 1);
    assert_eq!(selected.objects[0].key, "target/value");

    let absent = Box::pin(s3.list_objects(S3ListOptions {
        prefix: "absent/".to_owned(),
        ..S3ListOptions::default()
    }))
    .await?;
    assert_eq!(absent.entries_examined, 0);
    assert!(absent.objects.is_empty());
    Ok(())
}

#[tokio::test]
async fn multipart_stages_off_authority_and_publishes_one_ordered_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "multipart",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value;
    let before = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await?
        .value
        .id();
    let mut upload = workspace
        .s3()
        .create_multipart_upload(
            "large/value.bin",
            S3MultipartOptions::default(),
            IdempotencyKey::new(),
        )
        .await?;
    upload.upload_part(2, Bytes::from_static(b"second")).await?;
    upload.upload_part(1, Bytes::from_static(b"first-")).await?;
    assert_eq!(
        workspace
            .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .await?
            .value
            .id(),
        before
    );
    assert!(matches!(
        upload.complete(&[1, 2]).await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        upload.complete(&[1, 2]).await?,
        TransactionCommit::AlreadyCommitted(_)
    ));
    assert_eq!(
        workspace
            .read(
                "/large/value.bin",
                64,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .as_slice(),
        b"first-second".as_slice()
    );

    let mut invalid = workspace
        .s3()
        .create_multipart_upload(
            "invalid.bin",
            S3MultipartOptions::default(),
            IdempotencyKey::new(),
        )
        .await?;
    invalid.upload_part(1, Bytes::from_static(b"part")).await?;
    assert!(matches!(
        invalid.complete(&[2]).await,
        Err(S3Error::MissingPart(2))
    ));
    assert!(matches!(
        workspace.s3().head_object("invalid.bin").await,
        Err(S3Error::NotFound)
    ));
    Ok(())
}
