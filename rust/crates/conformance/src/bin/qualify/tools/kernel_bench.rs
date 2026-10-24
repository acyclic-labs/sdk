use super::*;

#[allow(
    clippy::too_many_lines,
    reason = "the benchmark's setup, timed loops, commit, and report are one straight-line procedure"
)]
pub(super) fn kernel_bench(args: &[String]) -> Result<(), Failure> {
    use acyclic_fs::AuthoredMutation;
    use acyclic_fs::kernel::{FileMetadata, LogicalName, NameEncoding, NamespacePath};

    let work = PathBuf::from(args.first().ok_or("kernel-bench: missing <work>")?);
    let files: u64 = args.get(1).map_or(Ok(4_000), |value| value.parse())?;
    let file_bytes: u64 = args.get(2).map_or(Ok(4_096), |value| value.parse())?;
    if files == 0 {
        return Err("kernel-bench: files must be positive".into());
    }
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let cancel = CancellationToken::new();
        let engine = LocalFs::local(local_options(work.join("store"))?)
            .await
            .map_err(engine_err("open store"))?;
        let config = VolumeConfig::native(Lifecycle::Durable);
        let volume = engine
            .create_volume(config, WorkCounters::UNBOUNDED, &cancel)
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
        let name = |value: String| {
            LogicalName::new(
                NameEncoding::Utf8,
                value.into_bytes(),
                config.limits.maximum_component_bytes,
            )
        };
        let file_path = |index: u64| -> Result<NamespacePath, Failure> {
            Ok(NamespacePath::new(
                vec![name("d".to_owned())?, name(format!("file-{index:07}.dat"))?],
                config.limits,
            )?)
        };
        checkout
            .apply_authored_transaction(
                vec![AuthoredMutation::CreateDirectory {
                    path: NamespacePath::new(vec![name("d".to_owned())?], config.limits)?,
                    metadata: FileMetadata::default(),
                }],
                WorkCounters::UNBOUNDED,
                &cancel,
            )
            .await
            .map_err(engine_err("create directory"))?;
        let payload = bytes::Bytes::from(corpus_payload(file_bytes));
        let mut create = std::time::Duration::ZERO;
        let mut write = std::time::Duration::ZERO;
        let mut last_fifth = std::time::Duration::ZERO;
        for index in 0..files {
            let started = Instant::now();
            let created = checkout
                .apply_authored_transaction(
                    vec![AuthoredMutation::CreateFile {
                        path: file_path(index)?,
                        bytes: acyclic_stream::OwnedBytes::new(),
                        metadata: FileMetadata::default(),
                    }],
                    WorkCounters::UNBOUNDED,
                    &cancel,
                )
                .await
                .map_err(engine_err("create"))?;
            let file_id = created
                .value
                .created_file_ids
                .first()
                .copied()
                .flatten()
                .ok_or("create returned no file identity")?;
            let created_at = Instant::now();
            checkout
                .change_content(
                    file_id,
                    acyclic_fs::ContentChange::Write {
                        offset: 0,
                        bytes: payload.clone().into(),
                    },
                    acyclic_fs::ContentTimes::Preserve,
                    WorkCounters::UNBOUNDED,
                    &cancel,
                )
                .await
                .map_err(engine_err("write"))?;
            create += created_at - started;
            write += created_at.elapsed();
            if index >= files - files.div_ceil(5) {
                last_fifth += started.elapsed();
            }
        }
        // Exact-path metadata lookups of every authored file, as a mount's
        // lookup and getattr callbacks issue them before publication.
        let started = Instant::now();
        for index in 0..files {
            checkout
                .lookup_no_follow_with_metadata(
                    &file_path(index)?,
                    WorkCounters::UNBOUNDED,
                    &cancel,
                )
                .await
                .map_err(engine_err("lookup"))?
                .value
                .ok_or("authored file is missing")?;
        }
        let lookup = started.elapsed();
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
            .map_err(engine_err("commit"))?;
        let commit = started.elapsed();
        let per_file =
            |spent: std::time::Duration, count: u64| spent.as_secs_f64() * 1e6 / count as f64;
        println!(
            "{}",
            serde_json::json!({
                "platform": std::env::consts::OS,
                "files": files,
                "fileBytes": file_bytes,
                "createMicrosPerFile": per_file(create, files),
                "writeMicrosPerFile": per_file(write, files),
                "lastFifthMicrosPerFile": per_file(last_fifth, files.div_ceil(5)),
                "lookupMicrosPerFile": per_file(lookup, files),
                "commitMillis": commit.as_secs_f64() * 1e3,
            })
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// par-bench: concurrent small-file creation natively and through a mount,
// from several writer processes, one directory each
// ---------------------------------------------------------------------------

pub(super) fn par_bench(args: &[String]) -> Result<(), Failure> {
    let work = PathBuf::from(args.first().ok_or("par-bench: missing <work>")?);
    let writers: u64 = args.get(1).map_or(Ok(8), |value| value.parse())?;
    let files: u64 = args.get(2).map_or(Ok(2_000), |value| value.parse())?;
    let mount = args.get(3).map_or("lazy", String::as_str);
    if writers == 0 || files < writers {
        return Err("par-bench: writers must be positive and at most files".into());
    }
    let each = files / writers;
    // Writers are child processes, as agents' tools are: a mount provider
    // does not observe its own process's I/O. Microseconds per file span the
    // first writer's start to the last writer's exit.
    let create = |root: &Path| -> Result<f64, Failure> {
        let started = Instant::now();
        let children = (0..writers)
            .map(|writer| {
                let directory = root.join("par").join(format!("w{writer:03}"));
                fs::create_dir_all(&directory)?;
                Ok(acyclic_native_runtime::ProcessTree::spawn(
                    acyclic_native_runtime::command::Command::new(std::env::current_exe()?)
                        .arg("mount-bench-writer")
                        .arg(&directory)
                        .arg(each.to_string())
                        .arg("4096")
                        .arg("1")
                        .stdout(acyclic_native_runtime::command::Stdio::null())
                        .stderr(acyclic_native_runtime::command::Stdio::piped()),
                )?)
            })
            .collect::<Result<Vec<_>, Failure>>()?;
        for mut child in children {
            let output = child
                .wait_with_output(
                    std::time::Duration::from_secs(120).saturating_sub(started.elapsed()),
                    8 * 1024 * 1024,
                    None,
                    || Ok(()),
                    |_| Ok(acyclic_native_runtime::ProcessInput::default()),
                    None,
                    None,
                )
                .map_err(|failure| failure.error)?;
            if !output.status.success() {
                return Err(format!(
                    "writer failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                )
                .into());
            }
        }
        Ok(started.elapsed().as_secs_f64() * 1e6 / (each * writers) as f64)
    };
    let native = create(&work.join("native"))?;
    let measure = |mount_dir: &Path, sync: &dyn Fn() -> Result<(), Failure>| {
        let writing = create(mount_dir)?;
        let started = Instant::now();
        sync()?;
        Ok((writing, started.elapsed().as_secs_f64() * 1e3))
    };
    let (measured, unmount) = match mount {
        "lazy" => {
            let source = work.join("src");
            fs::create_dir_all(&source)?;
            let profile = if cfg!(windows) {
                FilesystemProfile::Windows
            } else {
                FilesystemProfile::Posix
            };
            with_lazy_mount(&work, &source.canonicalize()?, profile, measure)
        }
        "workspace" => with_workspace_mount(&work, measure),
        other => return Err(format!("par-bench: unknown mount kind {other}").into()),
    };
    let (mounted, sync) = measured?;
    println!(
        "{}",
        serde_json::json!({
            "platform": std::env::consts::OS,
            "mount": mount,
            "writers": writers,
            "files": each * writers,
            "nativeMicrosPerFile": native,
            "mountMicrosPerFile": mounted,
            "nativeFilesPerSecond": 1e6 / native,
            "mountFilesPerSecond": 1e6 / mounted,
            "syncMillis": sync,
            "unmountMillis": unmount.as_ref().ok(),
        })
    );
    unmount.map(|_| ())
}

/// Runs `operation` for every index below `operations`, interleaved across
/// `threads`, and returns wall-clock microseconds per operation.
pub(super) fn timed_phase(
    operations: u64,
    threads: u64,
    operation: impl Fn(u64) -> Result<(), Failure> + Sync,
) -> Result<f64, Failure> {
    let started = Instant::now();
    std::thread::scope(|scope| {
        let workers = (0..threads)
            .map(|first| {
                let operation = &operation;
                scope.spawn(move || {
                    (first..operations)
                        .step_by(usize::try_from(threads).unwrap_or(usize::MAX))
                        .try_for_each(operation)
                })
            })
            .collect::<Vec<_>>();
        workers.into_iter().try_for_each(|worker| {
            worker
                .join()
                .map_err(|_| Failure::from("mount-bench worker panicked"))?
        })
    })?;
    Ok(started.elapsed().as_secs_f64() * 1e6 / operations.max(1) as f64)
}

pub(super) fn io_at<'a>(
    operation: &'a str,
    path: &'a Path,
) -> impl FnOnce(std::io::Error) -> Failure + 'a {
    move |error| format!("{operation} {}: {error}", path.display()).into()
}

// ---------------------------------------------------------------------------
// bench: baseline capture, then watch-driven incremental capture latency
// ---------------------------------------------------------------------------
