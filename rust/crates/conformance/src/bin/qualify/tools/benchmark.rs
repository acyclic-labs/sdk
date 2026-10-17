use super::*;

pub(super) fn mount_bench(args: &[String]) -> Result<(), Failure> {
    let work = PathBuf::from(args.first().ok_or("mount-bench: missing <work>")?);
    let files: u64 = args.get(1).map_or(Ok(2_000), |value| value.parse())?;
    let file_bytes: u64 = args.get(2).map_or(Ok(4_096), |value| value.parse())?;
    let threads: u64 = args.get(3).map_or(Ok(1), |value| value.parse())?;
    if threads == 0 {
        return Err("mount-bench: threads must be positive".into());
    }
    let source = work.join("src");
    let native_writes = work.join("native-writes");
    for directory in [&source, &native_writes] {
        fs::create_dir_all(directory)?;
    }
    write_corpus(&source, files, file_bytes)?;
    let source = source.canonicalize()?;
    let profile = if cfg!(windows) {
        FilesystemProfile::Windows
    } else {
        FilesystemProfile::Posix
    };
    let payload = corpus_payload(file_bytes);
    let native = workload(
        &source,
        &native_writes,
        files,
        &payload,
        threads,
        &|| Ok(()),
    )?;
    let (mounted, unmount) = with_lazy_mount(&work, &source, profile, |mount_dir, sync| {
        let mounted = |writes| {
            workload(
                mount_dir,
                &mount_dir.join(writes),
                files,
                &payload,
                threads,
                sync,
            )
        };
        Ok((mounted("writes-cold")?, mounted("writes-warm")?))
    });
    // Measurements stand on their own; report them before any unmount error.
    let (cold, warm) = mounted?;
    report_mount_bench(
        files,
        file_bytes,
        threads,
        [native, cold, warm],
        unmount.as_ref().ok(),
    );
    unmount.map(|_| ())
}

/// mount-run: times one external command in `<src>` itself, then `<runs>`
/// times inside a fresh lazy mount of `<src>`: the first run meets an
/// unprojected tree, later ones what earlier runs projected.
pub(super) fn mount_run(args: &[String]) -> Result<(), Failure> {
    let source = PathBuf::from(args.first().ok_or("mount-run: missing <src>")?).canonicalize()?;
    let work = PathBuf::from(args.get(1).ok_or("mount-run: missing <work>")?);
    let runs: usize = args.get(2).ok_or("mount-run: missing <runs>")?.parse()?;
    let program = args.get(3).ok_or("mount-run: missing <program>")?;
    let arguments = args.get(4..).unwrap_or_default();
    let run = |directory: &Path| -> Result<f64, Failure> {
        let started = Instant::now();
        let output = acyclic_native_runtime::process_output(
            std::process::Command::new(program)
                .args(arguments)
                .current_dir(directory),
            std::time::Duration::from_secs(120),
            8 * 1024 * 1024,
        )?;
        let elapsed = started.elapsed().as_secs_f64() * 1e3;
        if !output.status.success() {
            return Err(format!(
                "{program} failed in {}: {}",
                directory.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            )
            .into());
        }
        Ok(elapsed)
    };
    // The first native run warms the host caches the mount also reads from.
    run(&source)?;
    let native = (0..runs)
        .map(|_| run(&source))
        .collect::<Result<Vec<_>, _>>()?;
    let profile = if cfg!(windows) {
        FilesystemProfile::Windows
    } else {
        FilesystemProfile::Posix
    };
    let (mounted, unmount) = with_lazy_mount(&work, &source, profile, |mount_dir, _sync| {
        (0..runs)
            .map(|_| run(mount_dir))
            .collect::<Result<Vec<_>, _>>()
    });
    println!(
        "{}",
        serde_json::json!({
            "platform": std::env::consts::OS,
            "command": std::iter::once(program).chain(arguments).collect::<Vec<_>>(),
            "nativeMillis": native,
            "mountMillis": mounted?,
            "unmountMillis": unmount.as_ref().ok(),
        })
    );
    unmount.map(|_| ())
}

/// Mounts a lazy workspace of `source` under `work` with manual
/// publication, runs `measure` with the mount directory and its sync, and
/// unmounts. Returns the measurement and, separately, the unmount time in
/// milliseconds, which publishes every pending authored effect. Nothing may
/// leave the mount attached.
pub(super) fn with_lazy_mount<T>(
    work: &Path,
    source: &Path,
    profile: FilesystemProfile,
    measure: impl FnOnce(&Path, &dyn Fn() -> Result<(), Failure>) -> Result<T, Failure>,
) -> (Result<T, Failure>, Result<f64, Failure>) {
    use acyclic_fs::demand::native::NativeDemandSource;
    use acyclic_fs::model::VolumeLimits;
    use acyclic_fs::{LazyWorkspace, LocalCoreStateStore};
    use std::sync::Arc;

    let mount_dir = work.join("mnt");
    let mounted = (|| -> Result<_, Failure> {
        fs::create_dir_all(&mount_dir)?;
        let runtime = tokio::runtime::Runtime::new()?;
        let mount = runtime.block_on(async {
            let engine = LocalFs::local(local_options(work.join("store"))?)
                .await
                .map_err(engine_err("open store"))?;
            let state = LocalCoreStateStore::open_owned(work.join("core-state"))
                .map_err(engine_err("open core state"))?;
            let demand = NativeDemandSource::open(
                std::borrow::Cow::Owned(source.to_path_buf()),
                profile,
                VolumeLimits::default(),
                None,
                None,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(engine_err("open source"))?;
            let workspace = LazyWorkspace::attach(
                &engine,
                "mount-bench",
                Arc::new(demand),
                state,
                acyclic_fs::VolumeInitialization {
                    config: VolumeConfig::native(Lifecycle::Durable),
                    access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
                },
                acyclic_fs::OperationId::new(),
                &mut acyclic_fs::kernel::KernelAccounting::Standalone {
                    allocations: Default::default(),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                    budget: acyclic_fs::WorkBudget::UNBOUNDED,
                    cancellation: &acyclic_fs::CancellationToken::new(),
                },
            )
            .await
            .map(|(lazy, _)| lazy)
            .map_err(engine_err("attach source"))?;
            workspace
                .mount(
                    &mount_dir,
                    super::benchmark_access::options(workspace.workspace()).await?,
                )
                .await
                .map_err(engine_err("mount"))
        })?;
        Ok((runtime, mount))
    })();
    let (runtime, mount) = match mounted {
        Ok(mounted) => mounted,
        Err(error) => return (Err(error), Err("never mounted".into())),
    };
    // The mounted write boundary captures and publishes what was written.
    let sync = || runtime.block_on(mount.sync()).map_err(engine_err("sync"));
    let measured = measure(&mount_dir, &sync);
    let started = Instant::now();
    let unmounted = runtime
        .block_on(mount.unmount())
        .map_err(engine_err("unmount"))
        .map(|()| started.elapsed().as_secs_f64() * 1e3);
    (measured, unmounted)
}

/// Mounts one empty workspace under `work` directly over its checkout and
/// runs `measure` with the mount directory and its sync, exactly as
/// [`with_lazy_mount`] does.
pub(super) fn with_workspace_mount<T>(
    work: &Path,
    measure: impl FnOnce(&Path, &dyn Fn() -> Result<(), Failure>) -> Result<T, Failure>,
) -> (Result<T, Failure>, Result<f64, Failure>) {
    let mount_dir = work.join("mnt");
    let mounted = (|| -> Result<_, Failure> {
        fs::create_dir_all(&mount_dir)?;
        let runtime = tokio::runtime::Runtime::new()?;
        let mount = runtime.block_on(async {
            let engine = LocalFs::local(local_options(work.join("store"))?)
                .await
                .map_err(engine_err("open store"))?;
            let workspace = engine
                .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
                    "par-bench",
                    acyclic_fs::VolumeInitialization {
                        config: VolumeConfig::native(Lifecycle::Durable),
                        access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
                    },
                    acyclic_fs::OperationId::new(),
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                    None,
                )
                .await
                .map(|receipt| receipt.value.workspace())
                .map_err(|failure| failure.error)
                .map_err(engine_err("create workspace"))?;
            workspace
                .mount(
                    &mount_dir,
                    super::benchmark_access::options(&workspace).await?,
                )
                .await
                .map_err(engine_err("mount"))
        })?;
        Ok((runtime, mount))
    })();
    let (runtime, mount) = match mounted {
        Ok(mounted) => mounted,
        Err(error) => return (Err(error), Err("never mounted".into())),
    };
    let sync = || runtime.block_on(mount.sync()).map_err(engine_err("sync"));
    let measured = measure(&mount_dir, &sync);
    let started = Instant::now();
    let unmounted = runtime
        .block_on(mount.unmount())
        .map_err(engine_err("unmount"))
        .map(|()| started.elapsed().as_secs_f64() * 1e3);
    (measured, unmounted)
}

/// Prints one mount-bench result: per-phase native, cold, and warm costs,
/// the write phase's publication boundary on its own, and the unmount time
/// when the mount detached.
pub(super) fn report_mount_bench(
    files: u64,
    file_bytes: u64,
    threads: u64,
    [native, cold, warm]: [[f64; 5]; 3],
    unmount: Option<&f64>,
) {
    let phases = ["list", "stat", "read", "write", "sync"]
        .into_iter()
        .zip(native.into_iter().zip(cold).zip(warm))
        .map(|(phase, ((native, cold), warm))| {
            // Native writes have no publication boundary to compare against.
            let compared = phase != "sync";
            (
                phase.to_owned(),
                serde_json::json!({
                    "nativeMicrosPerOp": native,
                    "mountColdMicrosPerOp": cold,
                    "mountWarmMicrosPerOp": warm,
                    "coldRatio": compared.then(|| cold / native),
                    "warmRatio": compared.then(|| warm / native),
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    println!(
        "{}",
        serde_json::json!({
            "platform": std::env::consts::OS,
            "files": files,
            "fileBytes": file_bytes,
            "threads": threads,
            "phases": phases,
            "unmountMillis": unmount,
        })
    );
}

/// Times directory listing, stat, full reads, and fresh writes over the
/// corpus under `root`, returning wall-clock microseconds per operation for
/// each phase. Each phase's operations are split evenly across `threads`.
///
/// Writes come from one child process, as an agent's tools write: a mount
/// provider does not observe its own process's I/O. A write is complete once
/// `boundary` has made it durable in the workspace, so that is timed too; the
/// fifth phase, sync, is that boundary's share per written file.
pub(super) fn workload(
    root: &Path,
    writes: &Path,
    files: u64,
    payload: &[u8],
    threads: u64,
    boundary: &dyn Fn() -> Result<(), Failure>,
) -> Result<[f64; 5], Failure> {
    let directories = files.div_ceil(100);
    let listed = std::sync::atomic::AtomicU64::new(0);
    let list = timed_phase(directories, threads, |directory| {
        let path = root.join(corpus_directory(directory));
        let count = fs::read_dir(&path).map_err(io_at("list", &path))?.count();
        listed.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    })?;
    let listed = listed.into_inner();
    if listed != files {
        return Err(format!(
            "listed {listed} of {files} corpus files under {}",
            root.display()
        )
        .into());
    }
    let stat = timed_phase(files, threads, |index| {
        let path = root.join(corpus_file(index));
        if fs::metadata(&path).map_err(io_at("stat", &path))?.len() != payload.len() as u64 {
            return Err(format!("corpus file {index} has the wrong length").into());
        }
        Ok(())
    })?;
    let read = timed_phase(files, threads, |index| {
        let path = root.join(corpus_file(index));
        if fs::read(&path).map_err(io_at("read", &path))? != payload {
            return Err(format!("corpus file {index} differs from its payload").into());
        }
        Ok(())
    })?;
    fs::create_dir_all(writes).map_err(io_at("create", writes))?;
    let written = files.min(500);
    let output = acyclic_native_runtime::process_output(
        std::process::Command::new(std::env::current_exe()?)
            .arg("mount-bench-writer")
            .arg(writes)
            .arg(written.to_string())
            .arg(payload.len().to_string())
            .arg(threads.to_string()),
        std::time::Duration::from_secs(120),
        8 * 1024 * 1024,
    )?;
    if !output.status.success() {
        return Err(format!(
            "writer failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let writing: f64 = String::from_utf8(output.stdout)?.trim().parse()?;
    let started = Instant::now();
    boundary()?;
    let sync = started.elapsed().as_secs_f64() * 1e6 / written.max(1) as f64;
    Ok([list, stat, read, writing + sync, sync])
}

/// Child half of the write phase: writes `<count>` fresh files of
/// `<file-bytes>` into `<dir>` across `<threads>` and prints the wall-clock
/// microseconds per file that took.
pub(super) fn mount_bench_writer(args: &[String]) -> Result<(), Failure> {
    let directory = PathBuf::from(args.first().ok_or("mount-bench-writer: missing <dir>")?);
    let count: u64 = args
        .get(1)
        .ok_or("mount-bench-writer: missing <count>")?
        .parse()?;
    let file_bytes: u64 = args
        .get(2)
        .ok_or("mount-bench-writer: missing <file-bytes>")?
        .parse()?;
    let threads: u64 = args
        .get(3)
        .ok_or("mount-bench-writer: missing <threads>")?
        .parse()?;
    let payload = corpus_payload(file_bytes);
    let per_file = timed_phase(count, threads, |index| {
        let path = directory.join(format!("file-{index:07}.dat"));
        fs::write(&path, &payload).map_err(io_at("write", &path))
    })?;
    println!("{per_file}");
    Ok(())
}

// ---------------------------------------------------------------------------
// kernel-bench: create and write small files directly through one checkout,
// with no mount, look each up by path, then commit them
// ---------------------------------------------------------------------------
