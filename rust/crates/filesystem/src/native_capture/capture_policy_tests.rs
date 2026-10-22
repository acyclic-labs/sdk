use super::*;

pub(super) async fn native_checkout(
    work: &NativeMaterializationWork,
) -> Result<crate::facade::MemoryCheckout, Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let cancellation = work.cancellation();
    let workspace = crate::Fs::memory()
        .create_workspace("native-capture-property", work.budget(), cancellation)
        .await?
        .value;
    Ok(workspace
        .checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
            work,
            cancellation,
        )
        .await?)
}

#[tokio::test]
async fn fragmented_prior_capture_falls_back_with_a_smaller_host_span_bound()
-> Result<(), Box<dyn std::error::Error>> {
    let budget = crate::performance::work_budget(1_000_000, 64_000_000);
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(budget, &cancellation).map_err(|(cause, _)| cause)?;
    let mut checkout = native_checkout(&work).await?;
    let file = path("/file")?;
    let mutations = vec![
        AuthoredMutation::CreateFile {
            path: file.clone(),
            bytes: acyclic_stream::OwnedBytes::from_static(b"a"),
            metadata: FileMetadata::default(),
        },
        AuthoredMutation::Resize {
            path: file.clone(),
            logical_bytes: 256,
        },
        AuthoredMutation::Write {
            path: file.clone(),
            offset: 255,
            bytes: acyclic_stream::OwnedBytes::from_static(b"z"),
        },
    ];
    let slots = work.claim(
        (mutations.capacity() * std::mem::size_of::<AuthoredMutation>()) as u64,
        1,
        WorkCounters::UNCHARGED,
    )?;
    drop(
        checkout
            .apply_borrowed_authored_transaction_with_native_work(
                &mutations,
                &slots,
                &work,
                &cancellation,
            )
            .await?,
    );
    let range = crate::ByteRange {
        offset: 0,
        length: 256,
    };
    let (lookup, lookup_custody) = checkout
        .lookup_batch_no_follow(std::slice::from_ref(&file), &work, &cancellation)
        .await?;
    let record = lookup.entries[0].record.unwrap();
    drop((lookup, lookup_custody));
    let mut accounting = crate::kernel::KernelAccounting::native(&work, &cancellation);
    let (prior, allocation) = checkout
        .plan_record_extents(record, range, 8, &mut accounting, &cancellation)
        .await?;
    assert!(prior.as_ref().unwrap().spans.len() > 1);
    drop((prior, allocation));
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("file"), [b'b'; 256])?;
    let mut options = options(source.path(), 16)?;
    options.maximum_extent_spans = 1;
    let baseline = NativeViewBaseline::new(options.expected_root_identity, true);
    let root = NamespacePath::new(Vec::new(), checkout.volume_config().limits)?;
    let result = capture_subtrees_with_native_work(
        &mut checkout,
        &[root],
        &options,
        &CapturePolicy::default(),
        &cancellation,
        Some(&baseline),
        &work,
    )
    .await?;
    assert!(result.value.changed_paths > 0);
    let actual = checkout
        .read_file_range(&file, range, &work, &cancellation)
        .await?;
    assert_eq!(actual.bytes.as_ref(), &[b'b'; 256]);
    Ok(())
}

/// A rescan of unchanged content stages nothing when the host only advanced the
/// stamps it derives from reads and unrepresented inode changes, as macOS `FSEvents`
/// full rescans observe after a lazy mount reads its source; a recorded fact still
/// makes a new version.
#[tokio::test]
async fn derived_stamps_alone_never_make_a_new_version() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    let file = source.path().join("README.md");
    // Larger than an inline record, so an unchanged body is reused rather than rewritten.
    std::fs::write(&file, "# fixture\n".repeat(8))?;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let mut initial = native_checkout(&work).await?;
    let options = options(source.path(), 8)?;
    let root = NamespacePath::new(Vec::new(), initial.volume_config().limits)?;
    let rescan = |checkout: &crate::facade::MemoryCheckout| {
        checkout.private_candidate(
            &mut crate::kernel::KernelAccounting::native(&work, &cancellation),
            &cancellation,
        )
    };
    capture_subtrees_with_native_work(
        &mut initial,
        std::slice::from_ref(&root),
        &options,
        &CapturePolicy::default(),
        &cancellation,
        None,
        &work,
    )
    .await?;
    let readme = path("/README.md")?;
    let (lookup, _custody) = initial
        .lookup_batch_no_follow(std::slice::from_ref(&readme), &work, &cancellation)
        .await?;
    let record = lookup.entries[0]
        .record
        .ok_or("README.md was not captured")?;
    let prior = initial
        .read_metadata(record.file_id, &work, &cancellation)
        .await?;

    // Setting the access time advances it as a read does and, on Unix, advances the status-change time as an unrepresented named attribute does.
    let captured = std::fs::metadata(&file)?;
    let accessed = captured.accessed()? + std::time::Duration::from_secs(3_600);
    std::fs::File::options()
        .write(true)
        .open(&file)?
        .set_times(std::fs::FileTimes::new().set_accessed(accessed))?;
    // Negative control: the host stamps now differ from the recorded ones, so whole-record equality alone would stage the file again.
    let accessed_ns = i64::try_from(accessed.duration_since(std::time::UNIX_EPOCH)?.as_nanos())?;
    assert_ne!(prior.accessed_ns, MetadataField::Value(accessed_ns));
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let changed = std::fs::metadata(&file)?;
        assert_ne!(
            prior.changed_ns,
            MetadataField::Value(changed.ctime() * 1_000_000_000 + changed.ctime_nsec())
        );
    }
    let mut unchanged = rescan(&initial)?;
    let receipt = capture_subtrees_with_native_work(
        &mut unchanged,
        std::slice::from_ref(&root),
        &options,
        &CapturePolicy::default(),
        &cancellation,
        None,
        &work,
    )
    .await?;
    assert_eq!(receipt.value.changed_paths, 0);
    assert_eq!(unchanged.root(), rescan(&initial)?.root());

    // An access time set backwards is never a read: it is an explicit fact and versions.
    let MetadataField::Value(prior_accessed) = prior.accessed_ns else {
        return Err("README.md access time was not recorded".into());
    };
    let rewound_ns = prior_accessed - 3_600_000_000_000;
    let rewound =
        std::time::UNIX_EPOCH + std::time::Duration::from_nanos(u64::try_from(rewound_ns)?);
    std::fs::File::options()
        .write(true)
        .open(&file)?
        .set_times(std::fs::FileTimes::new().set_accessed(rewound))?;
    let mut rewound_checkout = rescan(&initial)?;
    let receipt = capture_subtrees_with_native_work(
        &mut rewound_checkout,
        std::slice::from_ref(&root),
        &options,
        &CapturePolicy::default(),
        &cancellation,
        None,
        &work,
    )
    .await?;
    assert_eq!(receipt.value.changed_paths, 1);
    let (lookup, _custody) = rewound_checkout
        .lookup_batch_no_follow(std::slice::from_ref(&readme), &work, &cancellation)
        .await?;
    let record = lookup.entries[0].record.ok_or("README.md vanished")?;
    let captured_rewound = rewound_checkout
        .read_metadata(record.file_id, &work, &cancellation)
        .await?;
    assert_eq!(
        captured_rewound.accessed_ns,
        MetadataField::Value(rewound_ns)
    );

    // Positive control: a recorded stamp with the same content is still a change.
    let modified = captured.modified()? + std::time::Duration::from_secs(7_200);
    std::fs::File::options()
        .write(true)
        .open(&file)?
        .set_times(std::fs::FileTimes::new().set_modified(modified))?;
    let mut touched = rescan(&initial)?;
    let receipt = capture_subtrees_with_native_work(
        &mut touched,
        std::slice::from_ref(&root),
        &options,
        &CapturePolicy::default(),
        &cancellation,
        None,
        &work,
    )
    .await?;
    assert_eq!(receipt.value.changed_paths, 1);
    assert_ne!(touched.root(), rescan(&initial)?.root());
    Ok(())
}

#[cfg(unix)]
mod unchanged_properties {
    use super::*;
    use proptest::prelude::*;
    use std::os::unix::fs::PermissionsExt;

    proptest! {
        // Real capture must retain every byte and metadata change, including a
        // content change with restored timestamps, while exact repeats write no paths.
        #[test]
        fn repeated_regular_capture_avoids_unchanged_writes(
            body in prop::collection::vec(1_u8..=255, 1..96),
            empty in any::<bool>(),
            truncate in any::<bool>(),
            change_body in any::<bool>(), change_mode in any::<bool>(),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async {
                let source = tempfile::tempdir().unwrap();
                let file = source.path().join("file");
                let body = if empty { Vec::new() } else { body };
                std::fs::write(&file, &body).unwrap();
                std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600)).unwrap();
                let before = std::fs::metadata(&file).unwrap();
                let budget = crate::performance::work_budget(1_000_000, 64_000_000);
                let cancellation = CancellationToken::new();
                let work = NativeMaterializationWork::new(budget, &cancellation).unwrap();
                let mut checkout = native_checkout(&work).await.unwrap();
                let root = NamespacePath::new(Vec::new(), checkout.volume_config().limits).unwrap();
                let options = options(source.path(), 16).unwrap();
                capture_subtrees_with_native_work(&mut checkout, &[root.clone()], &options, &CapturePolicy::default(), &cancellation, None, &work).await.unwrap();
                let mut expected = body;
                if change_body {
                    if expected.is_empty() { expected.push(1); }
                    else if truncate { expected.clear(); }
                    else { expected[0] ^= 1; }
                    std::fs::write(&file, &expected).unwrap();
                }
                if change_mode { std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o640)).unwrap(); }
                std::fs::File::open(&file).unwrap().set_times(std::fs::FileTimes::new().set_modified(before.modified().unwrap()).set_accessed(before.accessed().unwrap())).unwrap();
                let baseline = NativeViewBaseline::new(options.expected_root_identity, true);
                let before_capture = work.receipt();
                let receipt = capture_subtrees_with_native_work(&mut checkout, &[root], &options, &CapturePolicy::default(), &cancellation, Some(&baseline), &work).await.unwrap();
                prop_assert_eq!(receipt.value.changed_paths != 0, change_body || change_mode);
                if empty && !change_body && !change_mode {
                    prop_assert_eq!(receipt.work.backend_write_operations, before_capture.backend_write_operations);
                }
                let content = checkout.read_file_range(&path("/file").unwrap(), crate::ByteRange { offset: 0, length: expected.len() as u64 }, &work, &cancellation).await.unwrap();
                prop_assert_eq!(content.bytes.as_ref(), expected.as_slice());
                Ok(())
            })?;
        }
    }
}

fn path(value: &str) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    let limits = crate::model::VolumeLimits::default();
    Ok(NamespacePath::from_portable(
        &crate::path::PortablePath::parse(value, limits)?,
        limits,
    )?)
}

fn options(root: &Path, maximum_paths: u32) -> Result<CaptureOptions, CaptureError> {
    Ok(CaptureOptions {
        source_root: root.to_path_buf(),
        expected_root_identity: capture_root_identity(root)?,
        maximum_paths,
        maximum_extent_spans: 8,
    })
}

async fn checkout(
    files: &[(&str, &str)],
) -> Result<crate::facade::MemoryCheckout, Box<dyn std::error::Error>> {
    let workspace = crate::Fs::memory()
        .create_workspace(
            "capture-policy",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    for (name, contents) in files {
        workspace.write_text(name, contents).await?;
    }
    Ok(workspace
        .checkout(
            crate::model::GenerationSelector::Head,
            crate::model::CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value)
}

async fn replay(
    checkout: &mut crate::facade::MemoryCheckout,
    options: &CaptureOptions,
    changes: Vec<WatchChange>,
) -> Result<(), Box<dyn std::error::Error>> {
    capture_watch_batch(
        checkout,
        WatchBatch::Changes {
            original: Default::default(),
            epoch: 1,
            first_sequence: 1,
            next_sequence: 1 + changes.len() as u64,
            changes,
        },
        options,
        &CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    Ok(())
}

async fn assert_records(
    checkout: &mut crate::facade::MemoryCheckout,
    expected: &[(&str, Option<FileKind>)],
) -> Result<(), Box<dyn std::error::Error>> {
    for (name, kind) in expected {
        let record = checkout
            .lookup_no_follow(
                &path(name)?,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
        assert_eq!(
            record.value.record.map(|record| record.kind),
            *kind,
            "{name}"
        );
    }
    Ok(())
}

#[test]
fn excluded_prefixes_do_not_exclude_adjacent_names() -> Result<(), Box<dyn std::error::Error>> {
    let policy = CapturePolicy::excluding(vec![
        path("/z/private/nested")?,
        path("/a/cache")?,
        path("/z/private")?,
        path("/a/cache/deeper")?,
        path("/m")?,
    ])?;
    for (name, excluded) in [
        ("/a/cache/object", true),
        ("/m", true),
        ("/z/private/nested/object", true),
        ("/a/cached", false),
        ("/n", false),
        ("/z/public", false),
    ] {
        assert_eq!(policy.excludes(&path(name)?), excluded, "{name}");
    }
    Ok(())
}

#[tokio::test]
async fn nested_hints_hydrate_missing_or_stale_ancestors_without_expanding_siblings()
-> Result<(), Box<dyn std::error::Error>> {
    for stale in [false, true] {
        let source = tempfile::tempdir()?;
        std::fs::create_dir(source.path().join("sub"))?;
        std::fs::write(source.path().join("sub/changed.txt"), b"changed")?;
        std::fs::write(source.path().join("sub/unrelated.txt"), b"unrelated")?;
        let mut checkout = checkout(if stale { &[("/sub", "old")] } else { &[] }).await?;
        let child = path("/sub/changed.txt")?;
        replay(
            &mut checkout,
            &options(source.path(), 2)?,
            vec![if stale {
                WatchChange::Created(child)
            } else {
                WatchChange::Modified(child)
            }],
        )
        .await?;
        assert_records(
            &mut checkout,
            &[
                ("/sub", Some(FileKind::Directory)),
                ("/sub/changed.txt", Some(FileKind::Regular)),
                ("/sub/unrelated.txt", None),
            ],
        )
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_modified_directory_keeps_what_it_holds() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("pkg"))?;
    std::fs::write(source.path().join("pkg/shared.rs"), b"VALUE = 1")?;
    let mut checkout = checkout(&[]).await?;
    let options = options(source.path(), 8)?;
    capture_baseline(
        &mut checkout,
        &options,
        &CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    replay(
        &mut checkout,
        &options,
        vec![WatchChange::Modified(path("/pkg")?)],
    )
    .await?;
    assert_records(
        &mut checkout,
        &[("/pkg/shared.rs", Some(FileKind::Regular))],
    )
    .await
}

#[tokio::test]
async fn watch_directory_rename_then_child_change_keeps_the_renamed_directory()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("old"))?;
    std::fs::write(source.path().join("old/file.txt"), b"old")?;
    let mut checkout = checkout(&[]).await?;
    let options = options(source.path(), 3)?;
    capture_baseline(
        &mut checkout,
        &options,
        &CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await?;
    std::fs::rename(source.path().join("old"), source.path().join("new"))?;
    std::fs::write(source.path().join("new/file.txt"), b"new")?;
    replay(
        &mut checkout,
        &options,
        vec![
            WatchChange::Renamed {
                from: path("/old")?,
                to: path("/new")?,
            },
            WatchChange::Modified(path("/new/file.txt")?),
        ],
    )
    .await?;
    assert_records(
        &mut checkout,
        &[
            ("/old", None),
            ("/new", Some(FileKind::Directory)),
            ("/new/file.txt", Some(FileKind::Regular)),
        ],
    )
    .await
}

#[cfg(not(target_os = "macos"))]
#[tokio::test]
async fn watch_expands_only_directories_that_appeared() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    for directory in ["modified", "arrived"] {
        std::fs::create_dir(source.path().join(directory))?;
        std::fs::write(source.path().join(directory).join("inner.txt"), b"x")?;
    }
    let mut checkout = checkout(&[]).await?;
    replay(
        &mut checkout,
        &options(source.path(), 8)?,
        vec![
            WatchChange::Modified(path("/modified")?),
            WatchChange::Arrived(path("/arrived")?),
        ],
    )
    .await?;
    assert_records(
        &mut checkout,
        &[
            ("/modified", Some(FileKind::Directory)),
            ("/modified/inner.txt", None),
            ("/arrived", Some(FileKind::Directory)),
            ("/arrived/inner.txt", Some(FileKind::Regular)),
        ],
    )
    .await
}

#[tokio::test]
async fn watch_rename_chain_observes_intermediate_parent() -> Result<(), Box<dyn std::error::Error>>
{
    let source = tempfile::tempdir()?;
    std::fs::create_dir(source.path().join("sub"))?;
    std::fs::write(source.path().join("final.txt"), b"old")?;
    let mut checkout = checkout(&[("/old.txt", "old")]).await?;
    replay(
        &mut checkout,
        &options(source.path(), 3)?,
        vec![
            WatchChange::Renamed {
                from: path("/old.txt")?,
                to: path("/sub/intermediate.txt")?,
            },
            WatchChange::Renamed {
                from: path("/sub/intermediate.txt")?,
                to: path("/final.txt")?,
            },
        ],
    )
    .await?;
    assert_records(
        &mut checkout,
        &[("/old.txt", None), ("/final.txt", Some(FileKind::Regular))],
    )
    .await
}

#[tokio::test]
async fn content_hints_override_metadata_hints_in_either_order()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("file"), b"new")?;
    for metadata_first in [true, false] {
        let mut checkout = checkout(&[("/file", "old")]).await?;
        let mut changes = vec![
            WatchChange::MetadataChanged(path("/file")?),
            WatchChange::Modified(path("/file")?),
        ];
        if !metadata_first {
            changes.reverse();
        }
        replay(&mut checkout, &options(source.path(), 2)?, changes).await?;
        let content = checkout
            .read_file_range(
                &path("/file")?,
                crate::ByteRange {
                    offset: 0,
                    length: 3,
                },
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
        assert_eq!(content.value.bytes.as_ref(), b"new");
    }
    Ok(())
}

#[tokio::test]
async fn rename_into_new_directory_hydrates_parents_even_with_create_hints()
-> Result<(), Box<dyn std::error::Error>> {
    for hints in [vec![], vec![0], vec![1], vec![0, 1]] {
        rename_with_hints(2, hints, false).await?;
    }
    Ok(())
}

// Property: any subset/order/duplication of parent creation hints leaves the
// same renamed FileId and complete ancestry, within the event/path bound.
proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn rename_parent_hints_are_confluent(
        depth in 1usize..5,
        hints in proptest::collection::vec(0usize..4, 0..8),
        rename_first in proptest::bool::ANY,
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(rename_with_hints(depth, hints, rename_first)).unwrap();
    }
}

async fn rename_with_hints(
    depth: usize,
    hints: Vec<usize>,
    rename_first: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    let parents: Vec<_> = (1..=depth)
        .map(|length| format!("/{}", vec!["dir"; length].join("/")))
        .collect();
    let destination = format!("{}/moved.txt", parents.last().ok_or("parent")?);
    std::fs::create_dir_all(
        source
            .path()
            .join(parents.last().ok_or("parent")?.trim_start_matches('/')),
    )?;
    std::fs::write(
        source.path().join(destination.trim_start_matches('/')),
        b"new",
    )?;
    let mut checkout = checkout(&[("/old.txt", "old")]).await?;
    let before = checkout
        .lookup_no_follow(
            &path("/old.txt")?,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value
        .record
        .ok_or("original file")?;
    let mut changes = hints
        .into_iter()
        .filter_map(|index| parents.get(index))
        .map(|parent| Ok(WatchChange::Created(path(parent)?)))
        .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
    changes.push(WatchChange::Renamed {
        from: path("/old.txt")?,
        to: path(&destination)?,
    });
    if rename_first {
        changes.rotate_right(1);
    }
    replay(
        &mut checkout,
        &options(source.path(), (depth + 2).max(changes.len()) as u32)?,
        changes,
    )
    .await?;
    let mut expected = vec![
        ("/old.txt", None),
        (destination.as_str(), Some(FileKind::Regular)),
    ];
    expected.extend(
        parents
            .iter()
            .map(|parent| (parent.as_str(), Some(FileKind::Directory))),
    );
    assert_records(&mut checkout, &expected).await?;
    let after = checkout
        .lookup_no_follow(
            &path(&destination)?,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?
        .value
        .record
        .ok_or("renamed file")?;
    assert_eq!(after.file_id, before.file_id);
    Ok(())
}
