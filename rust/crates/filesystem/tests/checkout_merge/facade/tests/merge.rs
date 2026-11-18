use acyclic_fs::kernel::{
    FileKind, FilePayload, FileRecord, InlineFileData, MetadataField, Mutation,
};
use acyclic_fs::model::{CheckoutMode, GenerationSelector};
use acyclic_fs::{
    CancellationToken, ContentChange, ContentTimes, FsError, MergeConflict, MergePreparation,
};
use acyclic_fs_format::identity::FileId;
use acyclic_fs_work::performance::WorkCounters;
use acyclic_stream::OwnedBytes;

use super::B;
use super::checkout_fixture::{config, path, path_parts, writable_pinned};
use super::fixtures::{TestResult, commit, create, fixture, open_checkout, read};

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
        .err()
        .ok_or("zero-limit diff was admitted")?;
    assert!(matches!(invalid.error, FsError::InvalidDiff));
    assert_eq!(*invalid.work()?, WorkCounters::default());
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
        .lookup_no_follow(&path_parts(&[])?, B, &cancel)
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
                    bindings: None,
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
        ours.prepare_merge(generation, Some(32), Some(8), B, &cancel)
            .await?
            .value,
        MergePreparation::Prepared { .. }
    ));
    assert!(matches!(
        ours.mutate(Vec::new(), B, &cancel)
            .await
            .err()
            .ok_or("mutation was admitted while a merge was pending")?
            .error,
        FsError::PreparedMergePending
    ));
    assert!(
        ours.prepare_merge(generation, Some(32), Some(8), B, &cancel)
            .await
            .is_err()
    );
    assert!(ours.rebase_head(Some(8), B, &cancel).await.is_err());
    commit(&mut ours, &cancel).await?;
    let mut merged = volume
        .checkout(
            GenerationSelector::Exact(ours.generation_id()),
            CheckoutMode::read_only_pinned(),
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
        left.prepare_merge(right.generation_id(), Some(32), Some(8), B, &cancel)
            .await?
            .value,
        MergePreparation::Prepared { .. }
    ));
    commit(&mut left, &cancel).await?;
    let mut links = volume
        .checkout(
            GenerationSelector::Exact(left.generation_id()),
            CheckoutMode::read_only_pinned(),
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
    let mut readonly = open_checkout(&volume, CheckoutMode::read_only_pinned(), &cancel).await?;
    assert!(
        readonly
            .prepare_merge(base, Some(8), Some(8), B, &cancel)
            .await
            .is_err()
    );
    for (changes, conflicts) in [(0, 8), (8, 0)] {
        assert!(
            ours.prepare_merge(base, Some(changes), Some(conflicts), B, &cancel)
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
    assert!(
        ours.prepare_merge(base, Some(8), Some(8), B, &cancel)
            .await
            .is_err()
    );
    let generation = theirs.generation_id();
    let failure = ours
        .prepare_merge(generation, Some(1), Some(8), B, &cancel)
        .await
        .err()
        .ok_or("bounded merge did not reject its excess changes")?;
    assert!(matches!(failure.error, FsError::MergeChangeLimit));
    assert!(failure.work()?.backend_read_operations > 0);
    for maximum in [1, 8] {
        let prepared = ours
            .prepare_merge(generation, Some(8), Some(maximum), B, &cancel)
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
            .prepare_merge(theirs.generation_id(), Some(32), Some(8), B, &cancel)
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
                CheckoutMode::read_only_pinned(),
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
        .prepare_merge(theirs.generation_id(), Some(limit), Some(8), B, &cancel)
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
            CheckoutMode::read_only_pinned(),
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
        .prepare_merge(theirs.generation_id(), Some(limit), Some(8), B, &cancel)
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
    let mut overheads = [0; 2];
    for (overhead, additions) in overheads.iter_mut().zip([0, 64]) {
        let replaced = i64::from(minimum_limit(true, additions).await?);
        let edited = i64::from(minimum_limit(false, additions).await?);
        *overhead = replaced - edited;
    }
    let [empty, populated] = overheads;
    assert!(
        populated <= empty,
        "replacement overhead grew with the editing side's additions: {overheads:?}"
    );
    Ok(())
}
