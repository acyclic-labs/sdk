use acyclic_fs::kernel::{Mutation, RebaseDecision};
use acyclic_fs::model::CheckoutMode;
use acyclic_fs::{ByteRange, CancellationToken, ContentChange, ContentTimes, DependencyRegion};
use acyclic_fs_format::{identity::FileId, metadata::MetadataField};
use acyclic_stream::OwnedBytes;

use super::checkout_fixture::{config, path, writable_pinned};
use super::fixtures::{TestCheckout, TestResult, create, fixture, open_checkout};
use super::volume_commit::commit;
use super::{B, checkout_state};

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
        let mut local =
            open_checkout(&volume, CheckoutMode::tracking_transaction(), &cancel).await?;
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
        let decision = local.rebase_head(Some(8), B, &cancel).await?.value;
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
        let decision = local.rebase_head(Some(8), B, &cancel).await?.value;
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
    let mut local = open_checkout(&volume, CheckoutMode::tracking_transaction(), &cancel).await?;
    let mut remote = open_checkout(&volume, writable_pinned(), &cancel).await?;
    let saved = OwnedBytes::from_static(b"one two THREE");
    atomic_save(&mut remote, original, saved, &cancel).await?;
    write_doc(&mut local, 0, b"ONE".as_slice(), &cancel).await?;
    commit(&mut remote, &cancel).await?;
    let decision = local.rebase_head(Some(8), B, &cancel).await?.value;
    assert!(
        matches!(decision, RebaseDecision::Safe { .. }),
        "{decision:?}"
    );
    write_doc(&mut remote, 0, b"uno".as_slice(), &cancel).await?;
    commit(&mut remote, &cancel).await?;
    let before = checkout_state(&local);
    let decision = local.rebase_head(Some(8), B, &cancel).await?.value;
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
        let mut local =
            open_checkout(&volume, CheckoutMode::tracking_transaction(), &cancel).await?;
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
        let decision = local.rebase_head(Some(8), B, &cancel).await?.value;
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
        let decision = local.rebase_head(Some(8), B, &cancel).await?.value;
        assert!(
            matches!(decision, RebaseDecision::Safe { .. }),
            "{decision:?}"
        );
        assert_eq!(read_doc(&mut local, 13, &cancel).await?, b"ONE two THREE");
        write_doc(&mut remote, 4, b"TWO".as_slice(), &cancel).await?;
        commit(&mut remote, &cancel).await?;
        let before = checkout_state(&local);
        let decision = local.rebase_head(Some(8), B, &cancel).await?.value;
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
            let mut local =
                open_checkout(&volume, CheckoutMode::tracking_transaction(), &cancel).await?;
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
            let decision = local.rebase_head(Some(8), B, &cancel).await?.value;
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
            let mut local =
                open_checkout(&volume, CheckoutMode::tracking_transaction(), &cancel).await?;
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
            let decision = local.rebase_head(Some(8), B, &cancel).await?.value;
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
        let mut local =
            open_checkout(&volume, CheckoutMode::tracking_transaction(), &cancel).await?;
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
        let decision = local.rebase_head(Some(8), B, &cancel).await?.value;
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
