use super::*;

pub(super) fn restore_gen(args: &[String]) -> Result<(), Failure> {
    let store_dir = PathBuf::from(args.first().ok_or("restore-gen: missing <store/store>")?);
    let volume_hex = args.get(1).ok_or("restore-gen: missing <volume-uuid>")?;
    let generation_hex = args.get(2).ok_or("restore-gen: missing <gen-hex>")?;
    let destination = PathBuf::from(args.get(3).ok_or("restore-gen: missing <dest>")?);
    fs::create_dir_all(&destination)?;

    let volume_uuid: [u8; 16] = {
        let clean: String = volume_hex.chars().filter(|c| *c != '-').collect();
        let bytes = hex_decode(&clean)?;
        bytes
            .as_slice()
            .try_into()
            .map_err(|_| "volume uuid must be 16 bytes")?
    };
    let digest: [u8; 32] = hex_decode(generation_hex)?
        .as_slice()
        .try_into()
        .map_err(|_| "generation must be 32 bytes")?;

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let cancel = CancellationToken::new();
        let fs_engine = LocalFs::local(local_options(&store_dir)?)
            .await
            .map_err(engine_err("open store"))?;
        let volume = fs_engine
            .open_volume(
                VolumeId::from_bytes(volume_uuid),
                WorkCounters::UNBOUNDED,
                &cancel,
            )
            .await
            .map_err(engine_err("open volume"))?
            .value;
        let mut checkout = volume
            .checkout(
                GenerationSelector::Exact(GenerationId::new(acyclic_fs::Digest::from_bytes(
                    digest,
                ))),
                CheckoutMode {
                    access: AccessMode::ReadOnly,
                    consistency: ConsistencyMode::Pinned,
                    mutations: MutationMode::None,
                },
                WorkCounters::UNBOUNDED,
                &cancel,
            )
            .await
            .map_err(engine_err("checkout exact"))?
            .value;
        let receipt = materialize_checkout(
            &mut checkout,
            &MaterializeOptions {
                destination: destination.clone(),
                expected_root_identity: None,
                maximum_directory_entries: 1_024,
                maximum_extent_spans: 65_536,
                transfer_bytes: 8 * 1024 * 1024,
            },
            WorkCounters::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(engine_err("materialize"))?
        .value;
        println!(
            "materialized {} files / {} dirs into {}",
            receipt.files,
            receipt.directories,
            destination.display()
        );
        Ok(())
    })
}

pub(super) fn hex_decode(text: &str) -> Result<Vec<u8>, Failure> {
    if !text.len().is_multiple_of(2) {
        return Err("odd hex length".into());
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).map_err(|e| format!("{e}").into()))
        .collect()
}

// ---------------------------------------------------------------------------
// mount-smoke: Launch 3 gate — writable native mount of a captured checkout
// ---------------------------------------------------------------------------

pub(super) fn bench(args: &[String]) -> Result<(), Failure> {
    let source = PathBuf::from(args.first().ok_or("bench: missing <src>")?).canonicalize()?;
    let work = PathBuf::from(args.get(1).ok_or("bench: missing <work>")?);
    let rounds: usize = args.get(2).map_or(Ok(30), |value| value.parse())?;
    let store_dir = work.join("store");
    fs::create_dir_all(&store_dir)?;
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        tokio::spawn(async move {
            bench_inner(&source, &store_dir, rounds)
                .await
                .map_err(|error| error.to_string())
        })
        .await
        .map_err(|error| -> Failure { error.into() })?
        .map_err(Into::into)
    })
}

#[allow(
    clippy::too_many_lines,
    reason = "the benchmark's setup, rounds, and report are one straight-line procedure"
)]
pub(super) async fn bench_inner(
    source: &Path,
    store_dir: &Path,
    rounds: usize,
) -> Result<(), Failure> {
    use acyclic_fs::model::VolumeLimits;
    use acyclic_fs::{NativeWatch, NativeWatchOptions, WatchBatch};

    let cancel = CancellationToken::new();
    let fs_engine = LocalFs::local(local_options(store_dir)?)
        .await
        .map_err(engine_err("open store"))?;
    let volume = fs_engine
        .create_volume(volume_config(), WorkCounters::UNBOUNDED, &cancel)
        .await
        .map_err(engine_err("create volume"))?
        .value;
    let mut checkout = volume
        .checkout(
            GenerationSelector::Head,
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::TrackingSafe,
                mutations: MutationMode::PrivateOverlay,
            },
            WorkCounters::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(engine_err("checkout"))?
        .value;

    let mut watch = NativeWatch::open(
        source,
        NativeWatchOptions {
            limits: VolumeLimits::default(),
            maximum_queued_changes: 65_536,
            recursive: true,
        },
    )
    .map_err(engine_err("watch open"))?;
    let rescan = watch.begin_rescan().map_err(engine_err("begin_rescan"))?;

    let options = CaptureOptions {
        source_root: source.to_path_buf(),
        expected_root_identity: capture_root_identity(source).map_err(engine_err("identity"))?,
        maximum_paths: 4_000_000,
        maximum_extent_spans: 65_536,
    };

    let started = Instant::now();
    let receipt = capture_baseline(
        &mut checkout,
        &options,
        &acyclic_fs::CapturePolicy::default(),
        WorkCounters::UNBOUNDED,
        &cancel,
    )
    .await
    .map_err(engine_err("baseline"))?;
    let baseline_work = receipt.work;
    let receipt = receipt.value;
    let baseline_capture = started.elapsed();
    let started = Instant::now();
    checkout
        .commit(
            OperationId::new(),
            None,
            None,
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            WorkCounters::UNBOUNDED,
            &cancel,
            |_| async {
                Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                    acyclic_fs::OperationReceipt {
                        value: (),
                        work: WorkCounters::default(),
                    },
                )
            },
        )
        .await
        .map_err(engine_err("baseline commit"))?;
    let baseline_commit = started.elapsed();
    watch
        .finish_rescan(rescan)
        .map_err(engine_err("finish_rescan"))?;

    let mut event_latency = Vec::with_capacity(rounds);
    let mut capture_only = Vec::with_capacity(rounds);
    let mut capture_checkpoint = Vec::with_capacity(rounds);
    let mut capture_commit = Vec::with_capacity(rounds);
    for round in 0..rounds {
        let target = source.join(format!("bench-mutation-{}.txt", round % 5));
        let mutated_at = Instant::now();
        fs::write(&target, format!("round {round} at {mutated_at:?}\n"))?;

        // Poll until the watcher reports the change (event-arrival latency).
        let batch = loop {
            let batch = watch
                .poll(4_096, WorkCounters::UNBOUNDED, &cancel)
                .map_err(engine_err("poll"))?
                .value;
            match &batch {
                WatchBatch::Changes { changes, .. } if !changes.is_empty() => break batch,
                WatchBatch::Changes { .. } => {
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
                WatchBatch::RescanRequired { reason, .. } => {
                    return Err(format!("rescan required mid-bench: {reason:?}").into());
                }
            }
        };
        event_latency.push(mutated_at.elapsed());

        let capture_started = Instant::now();
        acyclic_fs::capture_watch_batch(
            &mut checkout,
            batch,
            &options,
            &acyclic_fs::CapturePolicy::default(),
            WorkCounters::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(engine_err("capture_watch_batch"))?;
        let captured_at = capture_started.elapsed();
        if round % 2 == 0 {
            checkout
                .checkpoint(WorkCounters::UNBOUNDED, &cancel)
                .await
                .map_err(engine_err("incremental checkpoint"))?;
            capture_checkpoint.push(capture_started.elapsed());
        } else {
            checkout
                .commit(
                    OperationId::new(),
                    None,
                    None,
                    None,
                    acyclic_fs::PublicationPermit::Unrestricted,
                    WorkCounters::UNBOUNDED,
                    &cancel,
                    |_| async {
                        Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                            acyclic_fs::OperationReceipt {
                                value: (),
                                work: WorkCounters::default(),
                            },
                        )
                    },
                )
                .await
                .map_err(engine_err("incremental commit"))?;
            capture_commit.push(capture_started.elapsed());
        }
        capture_only.push(captured_at);
    }

    println!(
        "{}",
        serde_json::json!({
            "schema": 1,
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "rounds": rounds,
            "baseline": {
                "captureMicros": micros(baseline_capture),
                "commitMicros": micros(baseline_commit),
                "examinedPaths": receipt.examined_paths,
                "changedPaths": receipt.changed_paths,
                "stagedFileBytes": receipt.staged_file_bytes,
                "work": baseline_work,
            },
            "eventArrival": summary(&mut event_latency),
            "capture": summary(&mut capture_only),
            "captureCheckpoint": summary(&mut capture_checkpoint),
            "captureCommit": summary(&mut capture_commit),
        })
    );
    Ok(())
}

pub(super) fn micros(duration: std::time::Duration) -> u128 {
    duration.as_micros()
}

pub(super) fn summary(samples: &mut [std::time::Duration]) -> serde_json::Value {
    if samples.is_empty() {
        return serde_json::json!({ "count": 0 });
    }
    samples.sort();
    let p = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    serde_json::json!({
        "count": samples.len(),
        "p50Micros": micros(p(0.50)),
        "p95Micros": micros(p(0.95)),
        "maxMicros": micros(samples[samples.len() - 1]),
    })
}

// ---------------------------------------------------------------------------
// roundtrip
// ---------------------------------------------------------------------------

pub(super) fn roundtrip(args: &[String]) -> Result<(), Failure> {
    let source = PathBuf::from(args.first().ok_or("roundtrip: missing <src>")?)
        .canonicalize()
        .map_err(engine_err("canonicalize <src>"))?;
    let work = PathBuf::from(args.get(1).ok_or("roundtrip: missing <work>")?);
    let store_dir = work.join("store");
    let restore_dir = work.join("restore");
    fs::create_dir_all(&store_dir)?;
    fs::create_dir_all(&restore_dir)?;
    if fs::read_dir(&restore_dir)?.next().is_some() {
        return Err("roundtrip: <work>/restore must be empty".into());
    }

    let runtime = tokio::runtime::Runtime::new()?;
    let (volume_id, generation) = runtime.block_on(capture_and_commit(&source, &store_dir))?;
    runtime.block_on(materialize(&store_dir, volume_id, generation, &restore_dir))?;

    let started = Instant::now();
    let mismatches = compare_trees(&source, &restore_dir)?;
    println!("compare: {:?}", started.elapsed());

    if mismatches.is_empty() {
        println!("ROUNDTRIP OK: content + mode identical");
        Ok(())
    } else {
        for m in &mismatches {
            eprintln!("MISMATCH: {m}");
        }
        Err(format!("{} mismatches", mismatches.len()).into())
    }
}

pub(super) fn volume_config() -> VolumeConfig {
    // QUAL_PROFILE=portable switches the profile for differential debugging.
    let profile = match std::env::var("QUAL_PROFILE").as_deref() {
        Ok("portable") => FilesystemProfile::Portable,
        Ok("posix") => FilesystemProfile::Posix,
        Ok("windows") => FilesystemProfile::Windows,
        _ if cfg!(windows) => FilesystemProfile::Windows,
        _ => FilesystemProfile::Posix,
    };
    let mut config = VolumeConfig {
        profile,
        ..VolumeConfig::portable(Lifecycle::Durable)
    };
    // A baseline capture is one atomic authored transaction covering every
    // path in the repo; the default 2,048-mutation batch cap rejects any
    // real tree. Sized for large monorepos.
    config.limits.maximum_mutations_per_batch = 4_194_304;
    config.limits.maximum_paths_per_batch = 4_194_304;
    config
}

pub(super) async fn capture_and_commit(
    source: &Path,
    store_dir: &Path,
) -> Result<(VolumeId, GenerationId), Failure> {
    let cancel = CancellationToken::new();
    let fs_engine = LocalFs::local(local_options(store_dir)?)
        .await
        .map_err(engine_err("open store"))?;

    let started = Instant::now();
    let volume = fs_engine
        .create_volume(volume_config(), WorkCounters::UNBOUNDED, &cancel)
        .await
        .map_err(engine_err("create volume"))?
        .value;
    let mut checkout = volume
        .checkout(
            GenerationSelector::Head,
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::TrackingSafe,
                mutations: MutationMode::PrivateOverlay,
            },
            WorkCounters::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(engine_err("checkout head"))?
        .value;
    println!("volume + checkout: {:?}", started.elapsed());

    let options = CaptureOptions {
        source_root: source.to_path_buf(),
        expected_root_identity: capture_root_identity(source)
            .map_err(engine_err("root identity"))?,
        maximum_paths: 4_000_000,
        maximum_extent_spans: 65_536,
    };
    let started = Instant::now();
    let receipt = capture_baseline(
        &mut checkout,
        &options,
        &acyclic_fs::CapturePolicy::default(),
        WorkCounters::UNBOUNDED,
        &cancel,
    )
    .await
    .map_err(engine_err("capture_baseline"))?;
    let capture_work = receipt.work;
    let receipt = receipt.value;
    println!(
        "capture_baseline: {:?} (examined {} paths, {} changed, {} bytes staged)",
        started.elapsed(),
        receipt.examined_paths,
        receipt.changed_paths,
        receipt.staged_file_bytes
    );
    println!("capture_work: {capture_work:?}");

    let started = Instant::now();
    if std::env::var_os("QUAL_CHECKPOINT_ONLY").is_some() {
        let generation = checkout
            .checkpoint(WorkCounters::UNBOUNDED, &cancel)
            .await
            .map_err(engine_err("checkpoint"))?
            .value;
        println!("checkpoint (no publish): {:?}", started.elapsed());
        return Ok((checkout.volume_id(), generation));
    }
    let commit = checkout
        .commit(
            OperationId::new(),
            None,
            None,
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            WorkCounters::UNBOUNDED,
            &cancel,
            |_| async {
                Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                    acyclic_fs::OperationReceipt {
                        value: (),
                        work: WorkCounters::default(),
                    },
                )
            },
        )
        .await
        .map_err(engine_err("commit"))?;
    println!("commit: {:?}", started.elapsed());
    println!("commit_work: {:?}", commit.work);
    match commit.value {
        LivePublicationObservation::Committed { generation_id, .. }
        | LivePublicationObservation::AlreadyCommitted { generation_id, .. } => {
            Ok((checkout.volume_id(), generation_id))
        }
        other => Err(format!("commit not durable: {other:?}").into()),
    }
}

pub(super) async fn materialize(
    store_dir: &Path,
    volume_id: VolumeId,
    generation: GenerationId,
    destination: &Path,
) -> Result<(), Failure> {
    let cancel = CancellationToken::new();
    let fs_engine = LocalFs::local(local_options(store_dir)?)
        .await
        .map_err(engine_err("reopen store"))?;
    let volume = fs_engine
        .open_volume(volume_id, WorkCounters::UNBOUNDED, &cancel)
        .await
        .map_err(engine_err("reopen volume"))?
        .value;
    let mut checkout = volume
        .checkout(
            GenerationSelector::Exact(generation),
            CheckoutMode {
                access: AccessMode::ReadOnly,
                consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::None,
            },
            WorkCounters::UNBOUNDED,
            &cancel,
        )
        .await
        .map_err(engine_err("checkout exact"))?
        .value;

    let started = Instant::now();
    let receipt = materialize_checkout(
        &mut checkout,
        &MaterializeOptions {
            destination: destination.to_path_buf(),
            expected_root_identity: None,
            maximum_directory_entries: 1_024,
            maximum_extent_spans: 65_536,
            transfer_bytes: 8 * 1024 * 1024,
        },
        WorkCounters::UNBOUNDED,
        &cancel,
    )
    .await
    .map_err(engine_err("materialize_checkout"))?;
    let materialize_work = receipt.work;
    let receipt = receipt.value;
    println!(
        "materialize: {:?} ({} files, {} dirs, {} symlinks, {} bytes written)",
        started.elapsed(),
        receipt.files,
        receipt.directories,
        receipt.symbolic_links,
        receipt.written_bytes
    );
    println!("materialize_work: {materialize_work:?}");
    Ok(())
}

// ---------------------------------------------------------------------------
// tree comparison: content + kind + symlink target + mode bits (no mtime/uid)
// ---------------------------------------------------------------------------
