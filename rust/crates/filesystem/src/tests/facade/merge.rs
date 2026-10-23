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

/// Replaces `doc.txt` the way an editor's atomic save does: the old identity
/// is removed and a new file is created under the same name.
async fn atomic_save(
    checkout: &mut TestCheckout,
    original: FileId,
    bytes: OwnedBytes,
    cancel: &CancellationToken,
) -> TestResult<FileId> {
    checkout
        .mutate(
            vec![Mutation::Remove {
                path: path("doc.txt")?,
                expected_file_id: MetadataField::Value(original),
            }],
            B,
            cancel,
        )
        .await?;
    create(checkout, "doc.txt", bytes, cancel).await
}

async fn write_doc(
    checkout: &mut TestCheckout,
    offset: u64,
    bytes: impl Into<OwnedBytes>,
    cancel: &CancellationToken,
) -> TestResult {
    checkout
        .change_content(
            path("doc.txt")?,
            ContentChange::Write {
                offset,
                bytes: bytes.into(),
            },
            ContentTimes::Preserve,
            B,
            cancel,
        )
        .await?;
    Ok(())
}

async fn resize_doc(
    checkout: &mut TestCheckout,
    logical_bytes: u64,
    cancel: &CancellationToken,
) -> TestResult {
    checkout
        .change_content(
            path("doc.txt")?,
            ContentChange::Resize { logical_bytes },
            ContentTimes::Preserve,
            B,
            cancel,
        )
        .await?;
    Ok(())
}

/// Reads the first `length` bytes of `doc.txt`.
async fn read_doc(
    checkout: &mut TestCheckout,
    length: u64,
    cancel: &CancellationToken,
) -> TestResult<Vec<u8>> {
    Ok(checkout
        .read_file_range(
            &path("doc.txt")?,
            ByteRange { offset: 0, length },
            B,
            cancel,
        )
        .await?
        .value
        .bytes
        .as_ref()
        .to_vec())
}

fn conflicts_on(decision: &RebaseDecision, original: FileId) -> bool {
    matches!(decision, RebaseDecision::Conflicted { conflicts, .. }
    if conflicts.iter().any(|conflict| match conflict.region {
        DependencyRegion::FileRecord(file_id)
        | DependencyRegion::ContentRange { file_id, .. } => file_id == original,
        _ => false,
    }))
}

/// Checkout reconciliation, head side: the head replaced `doc.txt` by an
/// atomic save while the tracking checkout edited it in place. `rebase_head`
/// pairs the replacement with the identity it replaced, so the checkout's
/// edit merges onto the new file instead of conflicting with a deletion; an
/// edit that overlaps the save still conflicts and leaves the checkout
/// untouched. A second head edit then rebases cleanly, which proves the
/// checkout's dependencies followed the replacement rather than staying on
/// the removed identity.
#[tokio::test]
async fn checkout_rebase_merges_an_edit_onto_the_file_the_head_saved_over_it() -> TestResult {
    let cancel = CancellationToken::new();
    for overlapping in [false, true] {
        let (volume, mut seed) = fixture(config(), &cancel).await?;
        let original = create(
            &mut seed,
            "doc.txt",
            OwnedBytes::from_static(b"one two three"),
            &cancel,
        )
        .await?;
        commit(&mut seed, &cancel).await?;
        let mut local = open_checkout(&volume, writable_tracking(), &cancel).await?;
        let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
        let saved: &'static [u8] = if overlapping {
            b"uno two three"
        } else {
            b"one two THREE"
        };
        atomic_save(
            &mut remote,
            original,
            OwnedBytes::from_static(saved),
            &cancel,
        )
        .await?;
        write_doc(&mut local, 0, b"ONE".as_slice(), &cancel).await?;
        commit(&mut remote, &cancel).await?;
        let before = checkout_state(&local);
        let decision = local.rebase_head(8, B, &cancel).await?.value;
        if overlapping {
            assert!(conflicts_on(&decision, original), "{decision:?}");
            assert_eq!(checkout_state(&local), before);
            continue;
        }
        assert!(
            matches!(decision, RebaseDecision::Safe { .. }),
            "{decision:?}"
        );
        assert_eq!(read_doc(&mut local, 13, &cancel).await?, b"ONE two THREE");
        write_doc(&mut remote, 4, b"TWO".as_slice(), &cancel).await?;
        commit(&mut remote, &cancel).await?;
        let decision = local.rebase_head(8, B, &cancel).await?.value;
        assert!(
            matches!(decision, RebaseDecision::Safe { .. }),
            "{decision:?}"
        );
        assert_eq!(read_doc(&mut local, 13, &cancel).await?, b"ONE TWO THREE");
    }
    Ok(())
}

/// Checkout reconciliation, head side, write-only: the checkout edited
/// bytes of `doc.txt` without reading them, the head saved over the file,
/// and the rebase merged the edit onto the save. The edit's
/// compare-before-write follows the replacement with the head's bytes as its
/// base, so a later head rewrite of those bytes is a conflict the rebase
/// reports, with the checkout untouched, rather than a broken
/// classification.
#[tokio::test]
async fn checkout_rebase_conflicts_on_a_later_head_rewrite_of_bytes_merged_onto_a_save()
-> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    let original = create(
        &mut seed,
        "doc.txt",
        OwnedBytes::from_static(b"one two three"),
        &cancel,
    )
    .await?;
    commit(&mut seed, &cancel).await?;
    let mut local = open_checkout(&volume, writable_tracking(), &cancel).await?;
    let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
    let saved = OwnedBytes::from_static(b"one two THREE");
    atomic_save(&mut remote, original, saved, &cancel).await?;
    write_doc(&mut local, 0, b"ONE".as_slice(), &cancel).await?;
    commit(&mut remote, &cancel).await?;
    let decision = local.rebase_head(8, B, &cancel).await?.value;
    assert!(
        matches!(decision, RebaseDecision::Safe { .. }),
        "{decision:?}"
    );
    write_doc(&mut remote, 0, b"uno".as_slice(), &cancel).await?;
    commit(&mut remote, &cancel).await?;
    let before = checkout_state(&local);
    let decision = local.rebase_head(8, B, &cancel).await?.value;
    assert!(
        matches!(decision, RebaseDecision::Conflicted { .. }),
        "{decision:?}"
    );
    assert_eq!(checkout_state(&local), before);
    assert_eq!(read_doc(&mut local, 13, &cancel).await?, b"ONE two THREE");
    Ok(())
}

/// Checkout reconciliation, checkout side: the tracking checkout replaced
/// `doc.txt` by an atomic save while the head edited it in place. The
/// checkout's remove depends on the record the head edited, and the kernel
/// merge's replacement rule settles it: a disjoint head edit merges into the
/// saved file, an overlapping one conflicts with the checkout untouched
/// rather than failing the rebase. The save stays a whole-file write, as an
/// in-place rewrite of every byte would: its dependency follows the head it
/// settled against, so an unrelated head change rebases while a second edit
/// of the same file conflicts.
#[tokio::test]
async fn checkout_rebase_merges_a_head_edit_into_the_file_the_checkout_saved_over_it() -> TestResult
{
    let cancel = CancellationToken::new();
    for overlapping in [false, true] {
        let (volume, mut seed) = fixture(config(), &cancel).await?;
        let original = create(
            &mut seed,
            "doc.txt",
            OwnedBytes::from_static(b"one two three"),
            &cancel,
        )
        .await?;
        commit(&mut seed, &cancel).await?;
        let mut local = open_checkout(&volume, writable_tracking(), &cancel).await?;
        let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
        let saved: &'static [u8] = if overlapping {
            b"uno two three"
        } else {
            b"one two THREE"
        };
        atomic_save(
            &mut local,
            original,
            OwnedBytes::from_static(saved),
            &cancel,
        )
        .await?;
        write_doc(&mut remote, 0, b"ONE".as_slice(), &cancel).await?;
        commit(&mut remote, &cancel).await?;
        let before = checkout_state(&local);
        let decision = local.rebase_head(8, B, &cancel).await?.value;
        if overlapping {
            assert!(conflicts_on(&decision, original), "{decision:?}");
            assert_eq!(checkout_state(&local), before);
            continue;
        }
        assert!(
            matches!(decision, RebaseDecision::Safe { .. }),
            "{decision:?}"
        );
        assert_eq!(read_doc(&mut local, 13, &cancel).await?, b"ONE two THREE");
        create(
            &mut remote,
            "other.txt",
            OwnedBytes::from_static(b"other"),
            &cancel,
        )
        .await?;
        commit(&mut remote, &cancel).await?;
        let decision = local.rebase_head(8, B, &cancel).await?.value;
        assert!(
            matches!(decision, RebaseDecision::Safe { .. }),
            "{decision:?}"
        );
        assert_eq!(read_doc(&mut local, 13, &cancel).await?, b"ONE two THREE");
        write_doc(&mut remote, 4, b"TWO".as_slice(), &cancel).await?;
        commit(&mut remote, &cancel).await?;
        let before = checkout_state(&local);
        let decision = local.rebase_head(8, B, &cancel).await?.value;
        assert!(conflicts_on(&decision, original), "{decision:?}");
        assert_eq!(checkout_state(&local), before);
    }
    Ok(())
}

/// A rebase merges bytes, not extents: one side saved a rewrite of every
/// byte (sharing no extent with the base) while the other edited both ends
/// in place. Bytes only one side changed merge, for a tiny inline file and
/// for an extent-backed file whose changes straddle the comparison window;
/// a byte both sides changed differently conflicts with the checkout
/// untouched. Checked with the save on either side.
#[tokio::test]
async fn checkout_rebase_merges_the_bytes_of_a_saved_rewrite() -> TestResult {
    let cancel = CancellationToken::new();
    for (length, middle) in [(13_usize, 6_usize), (70_000, 65_540)] {
        for (local_saves, overlapping) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let (volume, mut seed) = fixture(config(), &cancel).await?;
            let base: Vec<u8> = (b'a'..=b'z').cycle().take(length).collect();
            let original = create(&mut seed, "doc.txt", base.clone().into(), &cancel).await?;
            commit(&mut seed, &cancel).await?;
            let mut local = open_checkout(&volume, writable_tracking(), &cancel).await?;
            let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
            let saved: Vec<u8> = (base.iter().enumerate())
                .map(|(at, byte)| {
                    if at == middle || (overlapping && at == 1) {
                        b'#'
                    } else {
                        *byte
                    }
                })
                .collect();
            let merged: Vec<u8> = (saved.iter().enumerate())
                .map(|(at, byte)| {
                    if at < 3 || at >= length - 3 {
                        b'X'
                    } else {
                        *byte
                    }
                })
                .collect();
            let (saving, editing) = if local_saves {
                (&mut local, &mut remote)
            } else {
                (&mut remote, &mut local)
            };
            atomic_save(saving, original, saved.into(), &cancel).await?;
            write_doc(editing, 0, b"XXX".to_vec(), &cancel).await?;
            write_doc(editing, (length - 3) as u64, b"XXX".to_vec(), &cancel).await?;
            commit(&mut remote, &cancel).await?;
            let before = checkout_state(&local);
            let decision = local.rebase_head(8, B, &cancel).await?.value;
            let case = format!("length={length} local_saves={local_saves}");
            if overlapping {
                assert!(conflicts_on(&decision, original), "{case}: {decision:?}");
                assert_eq!(checkout_state(&local), before, "{case}");
                continue;
            }
            assert!(
                matches!(decision, RebaseDecision::Safe { .. }),
                "{case}: {decision:?}"
            );
            let read = read_doc(&mut local, length as u64, &cancel).await?;
            assert!(read == merged, "{case}");
        }
    }
    Ok(())
}

/// A rebase merges a save that changed the file's length: one side saved
/// the file with a suffix appended or cut while the other edited it in
/// place. Past a side's end a byte is absent, which the byte rule treats
/// like any other value, so an edit merges with an untouched suffix
/// appended or cut, and an edit of a cut byte conflicts with the checkout
/// untouched. Checked for an inline and an extent-backed file, with the
/// save on either side, or resized in place by the head.
#[tokio::test]
async fn checkout_rebase_merges_a_save_that_appended_or_cut_a_suffix() -> TestResult {
    let cancel = CancellationToken::new();
    for length in [13_usize, 70_000] {
        for bits in 0..16 {
            let [local_saves, appends, edits_tail, in_place] =
                [1, 2, 4, 8].map(|bit| bits & bit != 0);
            // A checkout's own in-place resize is a compare-before-write of
            // its record, which any head edit conflicts with.
            if local_saves && in_place {
                continue;
            }
            let (volume, mut seed) = fixture(config(), &cancel).await?;
            let base: Vec<u8> = (b'a'..=b'z').cycle().take(length).collect();
            let original = create(&mut seed, "doc.txt", base.clone().into(), &cancel).await?;
            commit(&mut seed, &cancel).await?;
            let mut local = open_checkout(&volume, writable_tracking(), &cancel).await?;
            let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
            let mut saved = base;
            if appends {
                saved.extend_from_slice(b"tail");
            } else {
                saved.truncate(length - 4);
            }
            let edited = |at: usize| at < 3 || (edits_tail && (length - 3..length).contains(&at));
            let merged: Vec<u8> = (saved.iter().enumerate())
                .map(|(at, byte)| if edited(at) { b'X' } else { *byte })
                .collect();
            let (saving, editing) = if local_saves {
                (&mut local, &mut remote)
            } else {
                (&mut remote, &mut local)
            };
            if !in_place {
                atomic_save(saving, original, saved.into(), &cancel).await?;
            } else if appends {
                write_doc(saving, length as u64, b"tail".as_slice(), &cancel).await?;
            } else {
                resize_doc(saving, (length - 4) as u64, &cancel).await?;
            }
            write_doc(editing, 0, b"XXX".to_vec(), &cancel).await?;
            if edits_tail {
                write_doc(editing, (length - 3) as u64, b"XXX".to_vec(), &cancel).await?;
            }
            commit(&mut remote, &cancel).await?;
            let before = checkout_state(&local);
            let decision = local.rebase_head(8, B, &cancel).await?.value;
            let case = format!("length={length} {local_saves}/{appends}/{edits_tail}/{in_place}");
            if edits_tail && !appends {
                assert!(conflicts_on(&decision, original), "{case}: {decision:?}");
                assert_eq!(checkout_state(&local), before, "{case}");
                continue;
            }
            assert!(
                matches!(decision, RebaseDecision::Safe { .. }),
                "{case}: {decision:?}"
            );
            let read = read_doc(&mut local, merged.len() as u64, &cancel).await?;
            assert!(read == merged, "{case}");
        }
    }
    Ok(())
}

/// A read stays exact across a replacement: the checkout's observation of
/// `doc.txt` is checked against the file that replaced it, so a save that
/// kept the bytes read rebases and one that changed them conflicts.
#[tokio::test]
async fn checkout_rebase_checks_a_read_against_the_file_the_head_saved_over_it() -> TestResult {
    let cancel = CancellationToken::new();
    for changed in [false, true] {
        let (volume, mut seed) = fixture(config(), &cancel).await?;
        let original = create(
            &mut seed,
            "doc.txt",
            OwnedBytes::from_static(b"one two three"),
            &cancel,
        )
        .await?;
        commit(&mut seed, &cancel).await?;
        let mut local = open_checkout(&volume, writable_tracking(), &cancel).await?;
        let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
        let read = local
            .read_file_range(
                &path("doc.txt")?,
                ByteRange {
                    offset: 0,
                    length: 3,
                },
                B,
                &cancel,
            )
            .await?
            .value
            .bytes;
        assert_eq!(read.as_ref(), b"one");
        create(
            &mut local,
            "notes.txt",
            OwnedBytes::from_static(b"one"),
            &cancel,
        )
        .await?;
        let saved: &'static [u8] = if changed {
            b"uno two three"
        } else {
            b"one two THREE"
        };
        atomic_save(
            &mut remote,
            original,
            OwnedBytes::from_static(saved),
            &cancel,
        )
        .await?;
        commit(&mut remote, &cancel).await?;
        let before = checkout_state(&local);
        let decision = local.rebase_head(8, B, &cancel).await?.value;
        if changed {
            assert!(conflicts_on(&decision, original), "{decision:?}");
            assert_eq!(checkout_state(&local), before);
        } else {
            assert!(
                matches!(decision, RebaseDecision::Safe { .. }),
                "{decision:?}"
            );
            assert_eq!(read_doc(&mut local, 13, &cancel).await?, saved);
        }
    }
    Ok(())
}

/// Pairing replacements costs each changed directory one diff, not one per
/// replaced file: 64 files replaced in one directory and edited on the other
/// side merge within a change limit far below 64 x 64.
#[tokio::test]
async fn replacement_pairing_charges_each_directory_once() -> TestResult {
    const FILES: usize = 64;
    let cancel = CancellationToken::new();
    let (volume, mut seed) = fixture(config(), &cancel).await?;
    let mut originals = Vec::with_capacity(FILES);
    for index in 0..FILES {
        originals.push(
            create(
                &mut seed,
                &format!("file-{index:02}"),
                OwnedBytes::from_static(b"one two three"),
                &cancel,
            )
            .await?,
        );
    }
    commit(&mut seed, &cancel).await?;
    let mut ours = open_checkout(&volume, writable_pinned(), &cancel).await?;
    let mut theirs = open_checkout(&volume, writable_pinned(), &cancel).await?;
    for (index, original) in originals.iter().enumerate() {
        let name = format!("file-{index:02}");
        ours.mutate(
            vec![Mutation::Remove {
                path: path(&name)?,
                expected_file_id: MetadataField::Value(*original),
            }],
            B,
            &cancel,
        )
        .await?;
        create(
            &mut ours,
            &name,
            OwnedBytes::from_static(b"one two three"),
            &cancel,
        )
        .await?;
        theirs
            .change_content(
                path(&name)?,
                ContentChange::Write {
                    offset: 0,
                    bytes: OwnedBytes::from_static(b"ONE"),
                },
                ContentTimes::Preserve,
                B,
                &cancel,
            )
            .await?;
    }
    commit(&mut theirs, &cancel).await?;
    let limit = u32::try_from(FILES * 16)?;
    let prepared = ours
        .prepare_merge(theirs.generation_id(), limit, 8, B, &cancel)
        .await?
        .value;
    assert!(
        matches!(prepared, MergePreparation::Prepared { .. }),
        "{prepared:?}"
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
    for (index, original) in originals.iter().enumerate() {
        let record = merged
            .lookup_no_follow(&path(&format!("file-{index:02}"))?, B, &cancel)
            .await?
            .value
            .record
            .ok_or("merged file is missing")?;
        assert_eq!(record.file_id, *original);
        assert_eq!(
            read(&mut merged, *original, 0, 13, &cancel).await?.as_ref(),
            b"ONE two three"
        );
    }
    Ok(())
}

/// Whether a merge where theirs edits `doc.txt` and adds `additions` files
/// beside it, and ours either replaces `doc.txt` (equal content, new
/// identity) or leaves it, prepares within `limit` changes.
async fn prepares_within(replace: bool, additions: usize, limit: u32) -> TestResult<bool> {
    let cancel = CancellationToken::new();
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
    if replace {
        ours.mutate(
            vec![Mutation::Remove {
                path: path("doc.txt")?,
                expected_file_id: MetadataField::Value(original),
            }],
            B,
            &cancel,
        )
        .await?;
        create(
            &mut ours,
            "doc.txt",
            OwnedBytes::from_static(b"one two three"),
            &cancel,
        )
        .await?;
    } else {
        create(
            &mut ours,
            "ours.txt",
            OwnedBytes::from_static(b"ours"),
            &cancel,
        )
        .await?;
    }
    theirs
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
    for index in 0..additions {
        create(
            &mut theirs,
            &format!("added-{index:03}"),
            OwnedBytes::from_static(b"added"),
            &cancel,
        )
        .await?;
    }
    commit(&mut theirs, &cancel).await?;
    match ours
        .prepare_merge(theirs.generation_id(), limit, 8, B, &cancel)
        .await
    {
        Ok(receipt) => {
            assert!(
                matches!(receipt.value, MergePreparation::Prepared { .. }),
                "{:?}",
                receipt.value
            );
            Ok(true)
        }
        Err(failure) if matches!(failure.error, FsError::MergeChangeLimit) => Ok(false),
        Err(failure) => Err(failure.error.into()),
    }
}

/// The smallest change limit within which the merge prepares.
async fn minimum_limit(replace: bool, additions: usize) -> TestResult<u32> {
    let (mut low, mut high) = (1, 4_096);
    if !prepares_within(replace, additions, high).await? {
        return Err("merge does not prepare within 4096 changes".into());
    }
    while low < high {
        let middle = low + (high - low) / 2;
        if prepares_within(replace, additions, middle).await? {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    Ok(low)
}

/// Pairing a replacement charges only the side that deleted the identity:
/// unrelated additions on the editing side cost the merge the same with or
/// without a replacement on the other side, so the replacement's overhead
/// does not grow with them.
#[tokio::test]
async fn replacement_pairing_charges_only_the_deleting_side() -> TestResult {
    let mut overheads = Vec::new();
    for additions in [0, 64] {
        let replaced = i64::from(minimum_limit(true, additions).await?);
        let edited = i64::from(minimum_limit(false, additions).await?);
        overheads.push(replaced - edited);
    }
    assert!(
        overheads[1] <= overheads[0],
        "replacement overhead grew with the editing side's additions: {overheads:?}"
    );
    Ok(())
}
