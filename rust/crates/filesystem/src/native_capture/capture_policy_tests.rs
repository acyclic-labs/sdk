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
async fn cancelled_capture_leaves_retirement_with_the_accepted_native_producer()
-> Result<(), Box<dyn std::error::Error>> {
    let setup = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .map_err(|(cause, _)| cause)?;
    let mut checkout = native_checkout(&setup).await?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("file"), b"held native input")?;
    let options = options(source.path(), 16)?;
    let roots = [path("/")?];
    let policy = CapturePolicy::default();
    let cancellation = CancellationToken::new();
    let budget = crate::performance::work_budget(1_000_000, 64_000_000);
    let original =
        NativeMaterializationWork::new(budget, &cancellation).map_err(|(cause, _)| cause)?;
    let work = original.native_volume_work(budget)?;
    let resource = work.resource.as_ref().ok_or("original volume allowance")?;
    let remaining = resource.remaining()?.peak_allocation_bytes;
    let baseline = work.live_allocation_bytes();
    let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::seeded(83);
    let mut capture = Box::pin(schedule.scope(capture_subtrees_with_native_work(
        &mut checkout,
        &roots,
        &options,
        &policy,
        &cancellation,
        None,
        &work,
    )));
    let submitted = loop {
        tokio::select! {
            biased;
            result = &mut capture => panic!("capture completed before its held producer: {result:?}"),
            event = events.recv() => {
                let event = event.ok_or("native submission event lost")?;
                if event.name == "native:submitted" {
                    break event;
                }
                event.release();
            }
        }
    };
    assert!(work.live_allocation_bytes() > baseline);
    assert!(resource.remaining()?.peak_allocation_bytes < remaining);
    cancellation.cancel();
    schedule.cancel();
    // The accepted scan owns its inputs. Caller destruction must not wait for it.
    drop(capture);
    assert!(work.live_allocation_bytes() > baseline);
    assert!(!checkout.has_pending_mutations());
    submitted.release();
    loop {
        tokio::select! {
            biased;
            () = schedule.drained() => break,
            event = events.recv() => event.ok_or("native completion event lost")?.release(),
        }
    }
    assert_eq!(work.live_allocation_bytes(), baseline);
    assert_eq!(resource.remaining()?.peak_allocation_bytes, remaining);
    assert!(!checkout.has_pending_mutations());
    Ok(())
}

#[tokio::test]
async fn retained_watch_interval_replays_after_a_rejected_capture()
-> Result<(), Box<dyn std::error::Error>> {
    let work = NativeMaterializationWork::new(
        crate::performance::work_budget(1_000_000, 64_000_000),
        &CancellationToken::new(),
    )
    .map_err(|(cause, _)| cause)?;
    let mut checkout = native_checkout(&work).await?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("file"), b"retained hint")?;
    let options = options(source.path(), 16)?;
    let interval = std::sync::Arc::new(WatchBatch::Changes {
        epoch: 1,
        first_sequence: 1,
        next_sequence: 2,
        changes: vec![WatchChange::Created(path("/file")?)],
        original: Default::default(),
    });
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(
        capture_watch_batch(
            &mut checkout,
            std::sync::Arc::clone(&interval),
            &options,
            &CapturePolicy::default(),
            &work,
            &cancelled,
        )
        .await
        .is_err()
    );
    assert!(!checkout.has_pending_mutations());
    let receipt = capture_watch_batch(
        &mut checkout,
        std::sync::Arc::clone(&interval),
        &options,
        &CapturePolicy::default(),
        &work,
        work.cancellation(),
    )
    .await?;
    assert_eq!(receipt.value.first_sequence, 1);
    assert_eq!(receipt.value.next_sequence, 2);
    assert_eq!(receipt.value.capture.changed_paths, 1);
    assert_records(&mut checkout, &[("/file", Some(FileKind::Regular))]).await?;
    assert!(
        matches!(interval.as_ref(), WatchBatch::Changes { changes, .. }
        if changes == &[WatchChange::Created(path("/file")?)])
    );
    Ok(())
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
            file_id: None,
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
    options.maximum_extent_spans = Some(1);
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

    // Setting the access time advances it as a read does. The host may also
    // advance its status-change time, but both operations can share one tick.
    let captured = std::fs::metadata(&file)?;
    let accessed = captured.accessed()? + std::time::Duration::from_secs(3_600);
    std::fs::File::options()
        .write(true)
        .open(&file)?
        .set_times(std::fs::FileTimes::new().set_accessed(accessed))?;
    // Negative control: the host stamps now differ from the recorded ones, so whole-record equality alone would stage the file again.
    let accessed_ns = i64::try_from(accessed.duration_since(std::time::UNIX_EPOCH)?.as_nanos())?;
    assert_eq!(std::fs::metadata(&file)?.accessed()?, accessed);
    assert_ne!(prior.accessed_ns, MetadataField::Value(accessed_ns));
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

    // Real capture must retain every byte and metadata change, including a
    // content change with restored timestamps, while exact repeats write no paths.
    #[test]
    fn repeated_regular_capture_avoids_unchanged_writes() -> Result<(), Box<dyn std::error::Error>>
    {
        acyclic_native_runtime::initialize_native_io_services()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let budget = crate::performance::work_budget(1_000_000, 64_000_000);
        let workspace = runtime
            .block_on(crate::Fs::memory().create_workspace(
                "native-capture-property",
                budget,
                &CancellationToken::new(),
            ))?
            .value;
        let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
            source_file: Some(file!()),
            ..ProptestConfig::default()
        });
        runner.run(
            &(
                prop::collection::vec(1_u8..=255, 1..96),
                any::<bool>(),
                any::<bool>(),
                any::<bool>(),
                any::<bool>(),
            ),
            |(body, empty, truncate, change_body, change_mode)| {
                runtime.block_on(async {
                    let source = tempfile::tempdir().unwrap();
                    let file = source.path().join("file");
                    let body = if empty { Vec::new() } else { body };
                    std::fs::write(&file, &body).unwrap();
                    std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600))
                        .unwrap();
                    let before = std::fs::metadata(&file).unwrap();
                    let cancellation = CancellationToken::new();
                    let work = NativeMaterializationWork::new(budget, &cancellation).unwrap();
                    // Each generated case owns fresh native custody and a private
                    // checkout of the shared, unchanged empty workspace.
                    let mut checkout = workspace
                        .checkout(
                            crate::model::GenerationSelector::Head,
                            crate::model::CheckoutMode::tracking_transaction(),
                            &work,
                            &cancellation,
                        )
                        .await
                        .unwrap();
                    let root =
                        NamespacePath::new(Vec::new(), checkout.volume_config().limits).unwrap();
                    let options = options(source.path(), 16).unwrap();
                    capture_subtrees_with_native_work(
                        &mut checkout,
                        &[root.clone()],
                        &options,
                        &CapturePolicy::default(),
                        &cancellation,
                        None,
                        &work,
                    )
                    .await
                    .unwrap();
                    let mut expected = body;
                    if change_body {
                        if expected.is_empty() {
                            expected.push(1);
                        } else if truncate {
                            expected.clear();
                        } else {
                            expected[0] ^= 1;
                        }
                        std::fs::write(&file, &expected).unwrap();
                    }
                    if change_mode {
                        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o640))
                            .unwrap();
                    }
                    std::fs::File::open(&file)
                        .unwrap()
                        .set_times(
                            std::fs::FileTimes::new()
                                .set_modified(before.modified().unwrap())
                                .set_accessed(before.accessed().unwrap()),
                        )
                        .unwrap();
                    let baseline = NativeViewBaseline::new(options.expected_root_identity, true);
                    let before_capture = work.receipt();
                    let receipt = capture_subtrees_with_native_work(
                        &mut checkout,
                        &[root],
                        &options,
                        &CapturePolicy::default(),
                        &cancellation,
                        Some(&baseline),
                        &work,
                    )
                    .await
                    .unwrap();
                    prop_assert_eq!(receipt.value.changed_paths != 0, change_body || change_mode);
                    if empty && !change_body && !change_mode {
                        prop_assert_eq!(
                            receipt.work.backend_write_operations,
                            before_capture.backend_write_operations
                        );
                    }
                    let content = checkout
                        .read_file_range(
                            &path("/file").unwrap(),
                            crate::ByteRange {
                                offset: 0,
                                length: expected.len() as u64,
                            },
                            &work,
                            &cancellation,
                        )
                        .await
                        .unwrap();
                    prop_assert_eq!(content.bytes.as_ref(), expected.as_slice());
                    Ok(())
                })
            },
        )?;
        Ok(())
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
        source_namespace: None,
        source_root: root.to_path_buf(),
        expected_root_identity: capture_root_identity(root)?,
        maximum_paths: Some(maximum_paths),
        maximum_extent_spans: Some(8),
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
async fn observed_moves_retain_native_directory_and_file_identities()
-> Result<(), Box<dyn std::error::Error>> {
    for (children, destination, stale_child) in [
        (false, None, false),
        (true, None, false),
        (false, Some(FileKind::Directory), false),
        (true, Some(FileKind::Directory), false),
        (true, Some(FileKind::Directory), true),
        (true, Some(FileKind::Regular), false),
    ] {
        let source = tempfile::tempdir()?;
        std::fs::create_dir(source.path().join("old"))?;
        if children {
            std::fs::write(source.path().join("old/file"), b"kept")?;
        }
        match destination {
            Some(FileKind::Directory) => std::fs::create_dir(source.path().join("new"))?,
            Some(FileKind::Regular) => std::fs::write(source.path().join("new"), b"replaced")?,
            _ => {}
        }
        if stale_child {
            std::fs::write(source.path().join("new/stale"), b"removed before the move")?;
        }
        let mut checkout = checkout(&[]).await?;
        let mut options = options(source.path(), 8)?;
        options.source_namespace = Some([97; 16]);
        let cancellation = CancellationToken::new();
        capture_baseline(
            &mut checkout,
            &options,
            &CapturePolicy::default(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let identity = checkout
            .lookup_no_follow(&path("/old")?, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value
            .record
            .ok_or("missing old directory")?
            .file_id;
        let child_identity = if children {
            Some(
                checkout
                    .lookup_no_follow(&path("/old/file")?, WorkBudget::UNBOUNDED, &cancellation)
                    .await?
                    .value
                    .record
                    .ok_or("missing original child")?
                    .file_id,
            )
        } else {
            None
        };
        // Removing the destination first also works on Windows, whose native
        // rename does not replace directories. Capture sees only final states.
        if stale_child {
            std::fs::remove_file(source.path().join("new/stale"))?;
        }
        match destination {
            Some(FileKind::Directory) => std::fs::remove_dir(source.path().join("new"))?,
            Some(FileKind::Regular) => std::fs::remove_file(source.path().join("new"))?,
            _ => {}
        }
        std::fs::rename(source.path().join("old"), source.path().join("new"))?;
        // No rename event or descendant scan: final states alone prove the move.
        capture_paths(
            &mut checkout,
            &[path("/old")?, path("/new")?],
            &options,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        assert_records(
            &mut checkout,
            &[
                ("/old", None),
                ("/new", Some(FileKind::Directory)),
                ("/new/stale", None),
            ],
        )
        .await?;
        assert_eq!(
            checkout
                .lookup_no_follow(&path("/new")?, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .record
                .ok_or("missing moved directory")?
                .file_id,
            identity
        );
        if children {
            let child = checkout
                .lookup_no_follow(&path("/new/file")?, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .record
                .ok_or("missing moved child")?
                .file_id;
            assert_eq!(Some(child), child_identity);
            std::fs::rename(
                source.path().join("new/file"),
                source.path().join("new/renamed"),
            )?;
            std::fs::rename(source.path().join("new"), source.path().join("final"))?;
            capture_paths(
                &mut checkout,
                &[path("/new")?, path("/final")?, path("/final/renamed")?],
                &options,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
            assert_eq!(
                checkout
                    .lookup_no_follow(
                        &path("/final/renamed")?,
                        WorkBudget::UNBOUNDED,
                        &cancellation
                    )
                    .await?
                    .value
                    .record
                    .ok_or("missing renamed child")?
                    .file_id,
                child
            );
            assert_records(
                &mut checkout,
                &[
                    ("/new", None),
                    ("/final/file", None),
                    ("/final/renamed", Some(FileKind::Regular)),
                ],
            )
            .await?;
            std::fs::hard_link(
                source.path().join("final/renamed"),
                source.path().join("final/alias"),
            )?;
            capture_paths(
                &mut checkout,
                &[path("/final/alias")?],
                &options,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
            let alias = checkout
                .lookup_no_follow(&path("/final/alias")?, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .record
                .ok_or("missing captured alias")?;
            assert_eq!(alias.file_id, child);
            assert_eq!(alias.link_count, 2);
            std::fs::write(
                source.path().join("final/last"),
                b"replaced alias destination",
            )?;
            capture_paths(
                &mut checkout,
                &[path("/final/last")?],
                &options,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
            // The index's first alias is outside this partial capture. Its host
            // deletion must not remove that unobserved logical binding.
            std::fs::remove_file(source.path().join("final/alias"))?;
            std::fs::rename(
                source.path().join("final/renamed"),
                source.path().join("final/last"),
            )?;
            capture_paths(
                &mut checkout,
                &[path("/final/renamed")?, path("/final/last")?],
                &options,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
            for name in ["/final/alias", "/final/last"] {
                let record = checkout
                    .lookup_no_follow(&path(name)?, WorkBudget::UNBOUNDED, &cancellation)
                    .await?
                    .value
                    .record
                    .ok_or("missing retained alias")?;
                assert_eq!(record.file_id, child);
                assert_eq!(record.link_count, 2);
            }
            assert_records(&mut checkout, &[("/final/renamed", None)]).await?;
            std::fs::create_dir_all(source.path().join("parents/nested"))?;
            std::fs::rename(
                source.path().join("final/last"),
                source.path().join("parents/nested/last"),
            )?;
            capture_paths(
                &mut checkout,
                &[
                    path("/final/last")?,
                    path("/parents")?,
                    path("/parents/nested")?,
                    path("/parents/nested/last")?,
                ],
                &options,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
            let parent = checkout
                .lookup_no_follow(&path("/parents")?, WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value
                .record
                .ok_or("missing new parent")?
                .file_id;
            std::fs::rename(source.path().join("parents"), source.path().join("moved"))?;
            capture_paths(
                &mut checkout,
                &[path("/parents")?, path("/moved")?],
                &options,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
            for (name, expected) in [("/moved", parent), ("/moved/nested/last", child)] {
                assert_eq!(
                    checkout
                        .lookup_no_follow(&path(name)?, WorkBudget::UNBOUNDED, &cancellation)
                        .await?
                        .value
                        .record
                        .ok_or("missing moved parent or file")?
                        .file_id,
                    expected
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn relocations_use_the_current_namespace_after_parent_replacement()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    for directory in ["old", "new"] {
        std::fs::create_dir(source.path().join(directory))?;
        std::fs::write(
            source.path().join(directory).join("file"),
            directory.as_bytes(),
        )?;
    }
    std::fs::write(source.path().join("third"), b"final moved file")?;
    let mut checkout = checkout(&[]).await?;
    let mut options = options(source.path(), 8)?;
    options.source_namespace = Some([98; 16]);
    let cancellation = CancellationToken::new();
    capture_baseline(
        &mut checkout,
        &options,
        &CapturePolicy::default(),
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    let identity = checkout
        .lookup_no_follow(&path("/third")?, WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value
        .record
        .ok_or("missing third file")?
        .file_id;
    std::fs::remove_file(source.path().join("new/file"))?;
    std::fs::remove_dir(source.path().join("new"))?;
    std::fs::rename(source.path().join("old"), source.path().join("new"))?;
    std::fs::rename(source.path().join("third"), source.path().join("new/file"))?;
    capture_paths(
        &mut checkout,
        &[
            path("/old")?,
            path("/new")?,
            path("/new/file")?,
            path("/third")?,
        ],
        &options,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert_records(&mut checkout, &[("/old", None), ("/third", None)]).await?;
    assert_eq!(
        checkout
            .lookup_no_follow(&path("/new/file")?, WorkBudget::UNBOUNDED, &cancellation,)
            .await?
            .value
            .record
            .ok_or("missing final moved file")?
            .file_id,
        identity
    );
    assert_eq!(
        checkout
            .read_file_range(
                &path("/new/file")?,
                crate::ByteRange {
                    offset: 0,
                    length: b"final moved file".len() as u64
                },
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value
            .bytes
            .as_ref(),
        b"final moved file"
    );
    Ok(())
}

#[tokio::test]
async fn directory_promotions_preserve_the_retained_subtree()
-> Result<(), Box<dyn std::error::Error>> {
    for nested in [false, true] {
        let source = tempfile::tempdir()?;
        let retained = if nested { "a/b/c" } else { "a/b" };
        std::fs::create_dir_all(source.path().join(retained))?;
        std::fs::write(source.path().join(retained).join("file"), b"retained")?;
        std::fs::write(source.path().join("a/gone"), b"removed")?;
        let cancellation = CancellationToken::new();
        let config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
        let workspace = crate::Fs::memory()
            .create_workspace_with_config("promotion", config, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let mut checkout = workspace
            .checkout(
                crate::model::GenerationSelector::Head,
                crate::model::CheckoutMode::tracking_transaction(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let mut options = options(source.path(), 8)?;
        options.source_namespace = Some([99; 16]);
        capture_baseline(
            &mut checkout,
            &options,
            &CapturePolicy::default(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        let mut identities = Vec::new();
        for suffix in ["", "/file"] {
            identities.push(
                checkout
                    .lookup_no_follow(
                        &path(&format!("/{retained}{suffix}"))?,
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await?
                    .value
                    .record
                    .ok_or("missing retained source")?
                    .file_id,
            );
        }
        // An occupied candidate must never be replaced or selected for capture.
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::CreateFile {
                    path: path("/\u{1}")?,
                    bytes: acyclic_stream::OwnedBytes::from_static(b"outside observation"),
                    metadata: FileMetadata::default(),
                    file_id: None,
                }],
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?;
        std::fs::rename(source.path().join(retained), source.path().join("aside"))?;
        std::fs::remove_dir_all(source.path().join("a"))?;
        std::fs::rename(source.path().join("aside"), source.path().join("a"))?;
        capture_paths(
            &mut checkout,
            &[path("/a")?, path(&format!("/{retained}"))?],
            &options,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
        for (suffix, identity) in ["", "/file"].into_iter().zip(identities) {
            assert_eq!(
                checkout
                    .lookup_no_follow(
                        &path(&format!("/a{suffix}"))?,
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await?
                    .value
                    .record
                    .ok_or("missing promoted source")?
                    .file_id,
                identity
            );
        }
        assert_records(
            &mut checkout,
            &[
                ("/a/b", None),
                ("/a/gone", None),
                ("/\u{1}", Some(FileKind::Regular)),
            ],
        )
        .await?;
        let root = checkout
            .list_directory(&path("/")?, None, 3, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        assert_eq!(
            root.entries.len(),
            2,
            "temporary bindings must not escape capture"
        );
        assert!(!root.has_more);
        for (name, expected) in [
            ("/a/file", b"retained".as_slice()),
            ("/\u{1}", b"outside observation".as_slice()),
        ] {
            assert_eq!(
                checkout
                    .read_file_range(
                        &path(name)?,
                        crate::ByteRange {
                            offset: 0,
                            length: expected.len() as u64
                        },
                        WorkBudget::UNBOUNDED,
                        &cancellation,
                    )
                    .await?
                    .value
                    .bytes
                    .as_ref(),
                expected
            );
        }
    }
    Ok(())
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

// Property: any subset/order/duplication of parent creation hints leaves the
// same renamed FileId and complete ancestry, within the event/path bound.
#[test]
fn rename_parent_hints_are_confluent() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let initial = runtime.block_on(checkout(&[("/old.txt", "old")]))?;
    for hints in [vec![], vec![0], vec![1], vec![0, 1]] {
        runtime.block_on(rename_with_hints(&initial, 2, hints, false))?;
    }
    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases: 32,
        source_file: Some(file!()),
        test_name: Some(concat!(
            module_path!(),
            "::rename_parent_hints_are_confluent"
        )),
        ..proptest::test_runner::Config::default()
    });
    runner
        .run(
            &(
                1usize..5,
                proptest::collection::vec(0usize..4, 0..8),
                proptest::bool::ANY,
            ),
            |(depth, hints, rename_first)| {
                runtime
                    .block_on(rename_with_hints(&initial, depth, hints, rename_first))
                    .unwrap();
                Ok(())
            },
        )
        .unwrap();
    Ok(())
}

async fn rename_with_hints(
    initial: &crate::facade::MemoryCheckout,
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
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let mut checkout = initial.private_candidate(
        &mut crate::kernel::KernelAccounting::native(&work, &cancellation),
        &cancellation,
    )?;
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
