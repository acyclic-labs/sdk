use super::*;

#[tokio::test]
async fn generation_diff_is_semantic_and_bounded() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut checkout) = fixture(config(), &cancel).await?;
    let initial = checkout.generation_id();
    create(
        &mut checkout,
        "changed",
        OwnedBytes::from_static(b"one"),
        &cancel,
    )
    .await?;
    commit(&mut checkout, &cancel).await?;
    let first = checkout.generation_id();
    let invalid = volume
        .diff_generations(initial, first, 0, B, &cancel)
        .await
        .unwrap_err();
    assert!(matches!(invalid.error, FsError::InvalidDiff));
    assert_eq!(*invalid.work, WorkCounters::default());
    checkout
        .change_content(
            path("changed")?,
            ContentChange::Write {
                offset: 0,
                bytes: OwnedBytes::from_static(b"two"),
            },
            ContentTimes::Preserve,
            B,
            &cancel,
        )
        .await?;
    commit(&mut checkout, &cancel).await?;
    let second = checkout.generation_id();
    let created = volume
        .diff_generations(initial, first, 16, B, &cancel)
        .await?
        .value;
    assert!(!created.truncated);
    assert!(created.files.len() >= 2);
    assert_eq!(created.bindings.len(), 1);
    let bounded = volume
        .diff_generations(initial, first, 1, B, &cancel)
        .await?
        .value;
    assert!(bounded.truncated);
    assert_eq!(bounded.files.len(), 1);
    assert!(bounded.bindings.is_empty());
    let modified = volume
        .diff_generations(first, second, 16, B, &cancel)
        .await?
        .value;
    assert_eq!(modified.files.len(), 1);
    assert!(modified.bindings.is_empty());
    let equal = volume
        .diff_generations(second, second, 16, B, &cancel)
        .await?;
    assert!(equal.value.files.is_empty());
    assert_eq!(equal.work, WorkCounters::default());
    let metadata = checkout
        .lookup_no_follow(&root_path()?, B, &cancel)
        .await?
        .value
        .record
        .ok_or("missing root")?
        .metadata;
    checkout
        .mutate(
            vec![Mutation::Create {
                path: path("ordered-last")?,
                record: FileRecord {
                    file_id: FileId::from_bytes([0xff; 16]),
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::InlineRegular(InlineFileData::new(b"last")?),
                },
            }],
            B,
            &cancel,
        )
        .await?;
    commit(&mut checkout, &cancel).await?;
    for maximum in [1_u32, 2] {
        let bounded = volume
            .diff_generations(second, checkout.generation_id(), maximum, B, &cancel)
            .await?
            .value;
        assert!(bounded.truncated);
        assert_eq!(
            bounded.files.len() + bounded.bindings.len(),
            maximum as usize
        );
    }
    Ok(())
}

#[tokio::test]
async fn three_way_merge_combines_bindings_and_hard_links() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut ours) = fixture(config(), &cancel).await?;
    let mut theirs = open_checkout(&volume, writable_pinned(), &cancel).await?;
    create(&mut ours, "ours", OwnedBytes::from_static(b"ours"), &cancel).await?;
    create(
        &mut theirs,
        "theirs",
        OwnedBytes::from_static(b"theirs"),
        &cancel,
    )
    .await?;
    commit(&mut theirs, &cancel).await?;
    let generation = theirs.generation_id();
    assert!(matches!(
        ours.prepare_merge(generation, 32, 8, B, &cancel)
            .await?
            .value,
        MergePreparation::Prepared { .. }
    ));
    assert!(matches!(
        ours.mutate(Vec::new(), B, &cancel).await.unwrap_err().error,
        FsError::PreparedMergePending
    ));
    assert!(
        ours.prepare_merge(generation, 32, 8, B, &cancel)
            .await
            .is_err()
    );
    assert!(ours.rebase_head(8, B, &cancel).await.is_err());
    commit(&mut ours, &cancel).await?;
    let mut merged = volume
        .checkout(
            GenerationSelector::Exact(ours.generation_id()),
            pinned(),
            B,
            &cancel,
        )
        .await?
        .value;
    for name in ["ours", "theirs"] {
        assert!(
            merged
                .lookup_no_follow(&path(name)?, B, &cancel)
                .await?
                .value
                .record
                .is_some()
        );
    }
    assert_eq!(merged.root().parents.len(), 2);
    let mut left = open_checkout(&volume, writable_pinned(), &cancel).await?;
    let mut right = open_checkout(&volume, writable_pinned(), &cancel).await?;
    for (checkout, name) in [(&mut left, "left-link"), (&mut right, "right-link")] {
        checkout
            .mutate(
                vec![Mutation::Link {
                    source: path("ours")?,
                    destination: path(name)?,
                }],
                B,
                &cancel,
            )
            .await?;
    }
    commit(&mut right, &cancel).await?;
    assert!(matches!(
        left.prepare_merge(right.generation_id(), 32, 8, B, &cancel)
            .await?
            .value,
        MergePreparation::Prepared { .. }
    ));
    commit(&mut left, &cancel).await?;
    let mut links = volume
        .checkout(
            GenerationSelector::Exact(left.generation_id()),
            pinned(),
            B,
            &cancel,
        )
        .await?
        .value;
    let source = links
        .lookup_no_follow(&path("ours")?, B, &cancel)
        .await?
        .value
        .record
        .ok_or("missing source")?;
    assert_eq!(source.link_count, 3);
    for name in ["left-link", "right-link"] {
        assert_eq!(
            links
                .lookup_no_follow(&path(name)?, B, &cancel)
                .await?
                .value
                .record
                .ok_or("missing link")?
                .file_id,
            source.file_id
        );
    }
    Ok(())
}

#[tokio::test]
async fn three_way_merge_rejects_guards_and_bounds_file_conflicts() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    let mut ids = Vec::new();
    for (name, bytes) in [
        ("conflict.txt", b"base".as_slice()),
        ("second-conflict.txt", b"base-two".as_slice()),
    ] {
        ids.push(create(&mut seed, name, OwnedBytes::copy_from_slice(bytes), &cancel).await?);
    }
    commit(&mut seed, &cancel).await?;
    let base = seed.generation_id();
    let mut ours = open_checkout(&volume, writable_pinned(), &cancel).await?;
    let mut theirs = open_checkout(&volume, writable_pinned(), &cancel).await?;
    let mut readonly = open_checkout(&volume, pinned(), &cancel).await?;
    assert!(
        readonly
            .prepare_merge(base, 8, 8, B, &cancel)
            .await
            .is_err()
    );
    for (changes, conflicts) in [(0, 8), (8, 0)] {
        assert!(
            ours.prepare_merge(base, changes, conflicts, B, &cancel)
                .await
                .is_err()
        );
    }
    for (name, our_bytes, their_bytes) in [
        ("conflict.txt", b"ours".as_slice(), b"them".as_slice()),
        (
            "second-conflict.txt",
            b"ours-two".as_slice(),
            b"them-two".as_slice(),
        ),
    ] {
        for (checkout, bytes) in [(&mut ours, our_bytes), (&mut theirs, their_bytes)] {
            checkout
                .change_content(
                    path(name)?,
                    ContentChange::Write {
                        offset: 0,
                        bytes: OwnedBytes::copy_from_slice(bytes),
                    },
                    ContentTimes::Preserve,
                    B,
                    &cancel,
                )
                .await?;
        }
    }
    commit(&mut theirs, &cancel).await?;
    assert!(ours.prepare_merge(base, 8, 8, B, &cancel).await.is_err());
    let generation = theirs.generation_id();
    let failure = ours
        .prepare_merge(generation, 1, 8, B, &cancel)
        .await
        .unwrap_err();
    assert!(matches!(failure.error, FsError::MergeChangeLimit));
    assert!(failure.work.backend_read_operations > 0);
    for maximum in [1, 8] {
        let prepared = ours
            .prepare_merge(generation, 8, maximum, B, &cancel)
            .await?
            .value;
        let MergePreparation::Conflicted {
            conflicts,
            truncated,
        } = prepared
        else {
            return Err("merge hid conflicts".into());
        };
        assert_eq!(truncated, maximum == 1);
        assert_eq!(conflicts.len(), if maximum == 1 { 1 } else { 2 });
        assert!(
            conflicts
                .iter()
                .all(|conflict| ids.iter().any(|id| *conflict == MergeConflict::File(*id)))
        );
        if maximum != 1 {
            assert!(
                ids.iter()
                    .all(|id| conflicts.contains(&MergeConflict::File(*id)))
            );
        }
    }
    assert!(ours.has_pending_mutations());
    Ok(())
}

/// One side replaces a file under its name (a new identity), the other edits
/// it. Matched by identity alone this was a delete/modify conflict; the merge
/// pairs the replacement with the identity it replaced, merges them as one
/// file and binds the name back to the original identity.
#[tokio::test]
async fn three_way_merge_pairs_a_replaced_file_with_the_identity_it_replaced() -> TestResult {
    let cancel = CancellationToken::new();
    for ours_replaces in [false, true] {
        let (volume, mut seed) = fixture(config(), &cancel).await?;
        let original = create(
            &mut seed,
            "doc.txt",
            OwnedBytes::from_static(b"one two three"),
            &cancel,
        )
        .await?;
        commit(&mut seed, &cancel).await?;
        let mut ours = open_checkout(&volume, writable_pinned(), &cancel).await?;
        let mut theirs = open_checkout(&volume, writable_pinned(), &cancel).await?;
        let (replacing, editing) = if ours_replaces {
            (&mut ours, &mut theirs)
        } else {
            (&mut theirs, &mut ours)
        };
        // Equal content under a new identity, as an atomic save writes it.
        replacing
            .mutate(
                vec![Mutation::Remove {
                    path: path("doc.txt")?,
                    expected_file_id: MetadataField::Value(original),
                }],
                B,
                &cancel,
            )
            .await?;
        let replacement = create(
            replacing,
            "doc.txt",
            OwnedBytes::from_static(b"one two three"),
            &cancel,
        )
        .await?;
        assert_ne!(replacement, original);
        editing
            .change_content(
                path("doc.txt")?,
                ContentChange::Write {
                    offset: 0,
                    bytes: OwnedBytes::from_static(b"ONE"),
                },
                ContentTimes::Preserve,
                B,
                &cancel,
            )
            .await?;
        commit(&mut theirs, &cancel).await?;
        let prepared = ours
            .prepare_merge(theirs.generation_id(), 32, 8, B, &cancel)
            .await?
            .value;
        assert!(
            matches!(prepared, MergePreparation::Prepared { .. }),
            "ours_replaces={ours_replaces}: {prepared:?}"
        );
        commit(&mut ours, &cancel).await?;
        let mut merged = volume
            .checkout(
                GenerationSelector::Exact(ours.generation_id()),
                pinned(),
                B,
                &cancel,
            )
            .await?
            .value;
        let record = merged
            .lookup_no_follow(&path("doc.txt")?, B, &cancel)
            .await?
            .value
            .record
            .ok_or("merged file is missing")?;
        assert_eq!(record.file_id, original);
        assert_eq!(record.link_count, 1);
        assert_eq!(
            read(&mut merged, original, 0, 13, &cancel).await?.as_ref(),
            b"ONE two three"
        );
        assert!(read(&mut merged, replacement, 0, 1, &cancel).await.is_err());
    }
    Ok(())
}
