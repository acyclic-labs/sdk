//! Local-only Linux qualification for exact native working-set costs.

#![cfg(target_os = "linux")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use std::time::Instant;

use acyclic_fs::demand::{DemandSource, native::NativeDemandSource};
use acyclic_fs::kernel::NamespacePath;
use acyclic_fs::model::{FilesystemProfile, VolumeLimits};
use acyclic_fs::path::PortablePath;
use acyclic_fs::{
    CancellationToken, Fs, IdempotencyKey, LazyWorkspace, LocalOptions, MaterializeOptions,
    MemoryLazyWorkspaceStore, MountOptions, MountPath, PublicationPermit, TransactionCommit,
    WorkBudget,
};
use serde_json::json;

fn component(value: &str) -> Vec<u8> {
    value.as_bytes().to_vec()
}

fn source_tree(
    directories: usize,
    files_per_directory: usize,
) -> Result<tempfile::TempDir, Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    let hot = source.path().join("hot");
    std::fs::create_dir(&hot)?;
    for directory in 0..directories {
        let target = hot.join(format!("d{directory:03}"));
        std::fs::create_dir(&target)?;
        for file in 0..files_per_directory {
            std::fs::write(
                target.join(format!("f{file:03}.txt")),
                format!("payload-{directory:03}-{file:03}\n"),
            )?;
        }
    }
    Ok(source)
}

#[tokio::test]
#[ignore = "local-only live Linux FUSE source-backed mount profile"]
#[allow(
    clippy::too_many_lines,
    reason = "one local-only source-view comparison"
)]
async fn report_linux_lazy_mount_read_costs() -> Result<(), Box<dyn std::error::Error>> {
    let source = source_tree(10, 100)?;
    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let demand = Arc::new(
        NativeDemandSource::open(
            source.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?,
    );
    let profile_source = Arc::clone(&demand);
    let started = Instant::now();
    let lazy = LazyWorkspace::attach(
        &fs,
        "linux-lazy-mount-costs",
        demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let attach_us = started.elapsed().as_micros();
    let view = tempfile::tempdir()?;
    let started = Instant::now();
    let mount = lazy.mount(view.path(), MountOptions::read_write()).await?;
    let mount_us = started.elapsed().as_micros();
    let read_tree = |root: &std::path::Path| -> Result<usize, Box<dyn std::error::Error>> {
        let mut bytes = 0;
        for index in 0..100 {
            bytes += std::fs::read(root.join(format!("hot/d000/f{index:03}.txt")))?.len();
        }
        Ok(bytes)
    };
    let parallel_read_tree = |root: &std::path::Path| -> Result<usize, Box<dyn std::error::Error>> {
        let workers = (0..8)
            .map(|worker| {
                let root = root.to_path_buf();
                std::thread::spawn(move || -> std::io::Result<usize> {
                    let mut bytes = 0;
                    for index in (worker..100).step_by(8) {
                        bytes +=
                            std::fs::read(root.join(format!("hot/d000/f{index:03}.txt")))?.len();
                    }
                    Ok(bytes)
                })
            })
            .collect::<Vec<_>>();
        let mut bytes = 0;
        for worker in workers {
            bytes += worker.join().map_err(|_| "parallel reader panicked")??;
        }
        Ok(bytes)
    };
    let started = Instant::now();
    let native_bytes = read_tree(source.path())?;
    let native_read_us = started.elapsed().as_micros();
    let started = Instant::now();
    let cold_bytes = read_tree(view.path())?;
    let cold_read_us = started.elapsed().as_micros();
    let started = Instant::now();
    let warm_bytes = read_tree(view.path())?;
    let warm_read_us = started.elapsed().as_micros();
    let started = Instant::now();
    let native_parallel_bytes = parallel_read_tree(source.path())?;
    let native_parallel_read_us = started.elapsed().as_micros();
    let started = Instant::now();
    let mounted_parallel_bytes = parallel_read_tree(view.path())?;
    let mounted_parallel_read_us = started.elapsed().as_micros();
    assert_eq!(native_bytes, cold_bytes);
    assert_eq!(native_bytes, warm_bytes);
    assert_eq!(native_bytes, native_parallel_bytes);
    assert_eq!(native_bytes, mounted_parallel_bytes);
    mount.unmount().await?;
    let source_reference = profile_source.reference();
    let mut observed_files = Vec::new();
    let started = Instant::now();
    for index in 0..100 {
        let path = PortablePath::parse(
            &format!("/hot/d000/f{index:03}.txt"),
            VolumeLimits::default(),
        )?;
        let path = NamespacePath::from_portable(&path, VolumeLimits::default())?;
        let node = profile_source
            .lookup(source_reference, &path, &CancellationToken::new())
            .await?
            .value
            .ok_or("native source lookup missed a fixture file")?;
        observed_files.push((path, node.version));
    }
    let demand_lookup_us = started.elapsed().as_micros();
    let started = Instant::now();
    let mut demand_bytes = 0;
    for (path, version) in &observed_files {
        demand_bytes += profile_source
            .read_range(
                source_reference,
                path,
                *version,
                0,
                128,
                &CancellationToken::new(),
            )
            .await?
            .value
            .len();
    }
    let demand_read_us = started.elapsed().as_micros();
    assert_eq!(native_bytes, demand_bytes);
    let started = Instant::now();
    let mut observed = 0;
    for directory in std::fs::read_dir(source.path().join("hot"))? {
        for file in std::fs::read_dir(directory?.path())? {
            file?.metadata()?;
            observed += 1;
        }
    }
    assert_eq!(observed, 1_000);
    let native_scan_us = started.elapsed().as_micros();
    let copied = tempfile::tempdir()?;
    let started = Instant::now();
    let status = std::process::Command::new("cp")
        .arg("-a")
        .arg(source.path().join("hot"))
        .arg(copied.path())
        .status()?;
    assert!(status.success());
    let native_copy_us = started.elapsed().as_micros();
    for directory in 0..10 {
        let target = copied.path().join(format!("hot/d{directory:03}"));
        for file in 0..100 {
            std::fs::File::open(target.join(format!("f{file:03}.txt")))?.sync_all()?;
        }
        std::fs::File::open(target)?.sync_all()?;
    }
    std::fs::File::open(copied.path().join("hot"))?.sync_all()?;
    std::fs::File::open(copied.path())?.sync_all()?;
    let native_copy_and_sync_us = started.elapsed().as_micros();
    println!(
        "{}",
        json!({
            "schema": "acyclic-linux-lazy-mount-read-cost-v1",
            "source_files": 1_000,
            "read_files": 100,
            "attach_us": attach_us,
            "mount_us": mount_us,
            "native_read_us": native_read_us,
            "cold_read_us": cold_read_us,
            "warm_read_us": warm_read_us,
            "native_parallel_read_us": native_parallel_read_us,
            "mounted_parallel_read_us": mounted_parallel_read_us,
            "demand_lookup_us": demand_lookup_us,
            "demand_read_us": demand_read_us,
            "native_scan_us": native_scan_us,
            "native_copy_us": native_copy_us,
            "native_copy_and_sync_us": native_copy_and_sync_us,
        })
    );
    Ok(())
}

#[tokio::test]
#[ignore = "local-only Linux native working-set performance qualification"]
async fn report_linux_working_set_costs() -> Result<(), Box<dyn std::error::Error>> {
    const DIRECTORIES: usize = 10;
    const FILES_PER_DIRECTORY: usize = 100;
    const CHANGES: usize = 100;

    let source = source_tree(DIRECTORIES, FILES_PER_DIRECTORY)?;

    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let started = Instant::now();
    let demand = Arc::new(
        NativeDemandSource::open(
            source.path(),
            FilesystemProfile::Portable,
            VolumeLimits::default(),
        )
        .await?,
    );
    let lazy = LazyWorkspace::attach(
        &fs,
        "linux-native-costs",
        demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let attach_us = started.elapsed().as_micros();

    let view = tempfile::tempdir()?;
    let started = Instant::now();
    let working_set = lazy
        .prepare_native_working_set(
            "/hot",
            &MaterializeOptions::native(view.path()),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            PublicationPermit::Unrestricted,
        )
        .await?;
    let prepare_us = started.elapsed().as_micros();
    let started = Instant::now();
    working_set.validate_for_presentation().await?;
    let activation_us = started.elapsed().as_micros();

    let mut changed = Vec::with_capacity(CHANGES);
    for index in 0..CHANGES {
        let directory = index % DIRECTORIES;
        let file = index / DIRECTORIES;
        std::fs::write(
            view.path()
                .join("hot")
                .join(format!("d{directory:03}/f{file:03}.txt")),
            format!("changed-{index}\n"),
        )?;
        changed.push(
            MountPath::root()
                .child(component(&format!("d{directory:03}")))
                .child(component(&format!("f{file:03}.txt"))),
        );
    }
    let started = Instant::now();
    working_set.capture_host_paths(&changed).await?;
    let capture_us = started.elapsed().as_micros();
    let started = Instant::now();
    working_set
        .sync_with_permit(PublicationPermit::Unrestricted)
        .await?;
    let sync_us = started.elapsed().as_micros();

    println!(
        "{}",
        json!({
            "schema": "acyclic-linux-native-working-set-cost-v1",
            "files": DIRECTORIES * FILES_PER_DIRECTORY,
            "changed_paths": CHANGES,
            "attach_us": attach_us,
            "prepare_us": prepare_us,
            "activation_us": activation_us,
            "capture_us": capture_us,
            "sync_us": sync_us,
        })
    );
    Ok(())
}

#[tokio::test]
#[ignore = "local-only small exact-generation materializer profile"]
#[allow(
    clippy::too_many_lines,
    reason = "keep one local-only profiling sample self-contained"
)]
async fn report_linux_exact_materializer_costs() -> Result<(), Box<dyn std::error::Error>> {
    fn counters() -> Result<(u64, u64, u64), Box<dyn std::error::Error>> {
        let mut cpu = 0_u64;
        for entry in std::fs::read_dir("/proc/self/task")? {
            let path = entry?.path().join("schedstat");
            let value = match std::fs::read_to_string(path) {
                Ok(value) => value,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            let runtime = value
                .split_whitespace()
                .next()
                .ok_or("missing thread CPU counter")?
                .parse::<u64>()?;
            cpu = cpu
                .checked_add(runtime)
                .ok_or("process CPU counter overflow")?;
        }
        let io = std::fs::read_to_string("/proc/self/io")?;
        let counter = |name: &str| -> Result<u64, Box<dyn std::error::Error>> {
            io.lines()
                .find_map(|line| line.strip_prefix(name))
                .ok_or("missing process I/O counter")?
                .trim()
                .parse()
                .map_err(Into::into)
        };
        Ok((cpu, counter("read_bytes:")?, counter("write_bytes:")?))
    }

    let source = source_tree(1, 100)?;
    let state = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(state.path())).await?;
    let workspace = fs
        .create_workspace("linux-exact-materializer-costs")
        .await?;
    let mut transaction = workspace.begin_transaction(IdempotencyKey::new()).await?;
    transaction.create_dir_all("/hot/d000").await?;
    for index in 0..100 {
        transaction
            .write_text(
                &format!("/hot/d000/f{index:03}.txt"),
                &format!("payload-000-{index:03}\n"),
            )
            .await?;
    }
    let TransactionCommit::Committed(exact) = transaction.commit().await? else {
        return Err("materializer fixture did not commit".into());
    };

    let mut materialize_us = Vec::new();
    let mut native_copy_us = Vec::new();
    let mut native_copy_and_sync_us = Vec::new();
    let mut cpu_ns = Vec::new();
    let mut read_bytes = Vec::new();
    let mut write_bytes = Vec::new();
    let mut largest_tick_gap_us = Vec::new();
    for _ in 0..5 {
        let destination = tempfile::tempdir()?;
        let running = Arc::new(AtomicBool::new(true));
        let largest_gap = Arc::new(AtomicU64::new(0));
        let tick_running = Arc::clone(&running);
        let tick_gap = Arc::clone(&largest_gap);
        let ticker = tokio::spawn(async move {
            let mut previous = Instant::now();
            while tick_running.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(1)).await;
                let now = Instant::now();
                tick_gap.fetch_max(
                    u64::try_from(now.duration_since(previous).as_micros()).unwrap_or(u64::MAX),
                    Ordering::Relaxed,
                );
                previous = now;
            }
        });
        let before = counters()?;
        let started = Instant::now();
        let receipt = exact
            .materialize_path(
                "/hot",
                &MaterializeOptions::native(destination.path()),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
            )
            .await?;
        materialize_us.push(u64::try_from(started.elapsed().as_micros())?);
        let after = counters()?;
        running.store(false, Ordering::Relaxed);
        ticker.await?;
        cpu_ns.push(after.0.saturating_sub(before.0));
        read_bytes.push(after.1.saturating_sub(before.1));
        write_bytes.push(after.2.saturating_sub(before.2));
        largest_tick_gap_us.push(largest_gap.load(Ordering::Relaxed));
        assert_eq!(receipt.value.files, 100);

        let native_destination = tempfile::tempdir()?;
        let started = Instant::now();
        let status = std::process::Command::new("cp")
            .arg("-a")
            .arg(source.path().join("hot"))
            .arg(native_destination.path())
            .status()?;
        assert!(status.success());
        native_copy_us.push(u64::try_from(started.elapsed().as_micros())?);
        let copied = native_destination.path().join("hot");
        for index in 0..100 {
            std::fs::File::open(copied.join(format!("d000/f{index:03}.txt")))?.sync_all()?;
        }
        std::fs::File::open(copied.join("d000"))?.sync_all()?;
        std::fs::File::open(&copied)?.sync_all()?;
        std::fs::File::open(native_destination.path())?.sync_all()?;
        native_copy_and_sync_us.push(u64::try_from(started.elapsed().as_micros())?);
    }
    materialize_us.sort_unstable();
    native_copy_us.sort_unstable();
    native_copy_and_sync_us.sort_unstable();
    println!(
        "{}",
        json!({
            "schema": "acyclic-linux-exact-materializer-cost-v1",
            "files": 100,
            "samples": 5,
            "materialize_us": materialize_us,
            "native_copy_us": native_copy_us,
            "native_copy_and_sync_us": native_copy_and_sync_us,
            "cpu_ns": cpu_ns,
            "read_bytes": read_bytes,
            "write_bytes": write_bytes,
            "largest_tick_gap_us": largest_tick_gap_us,
        })
    );
    Ok(())
}
