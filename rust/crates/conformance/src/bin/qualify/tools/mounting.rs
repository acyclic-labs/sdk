use super::*;

#[allow(
    clippy::too_many_lines,
    reason = "the gate's mount, write, unmount, and verify steps are one straight-line procedure"
)]
pub(super) fn mount_smoke(args: &[String]) -> Result<(), Failure> {
    use acyclic_fs::{
        CheckoutMountSource, NativeMountRequest, SharedCheckout, mount_native, probe_native_mount,
    };
    #[cfg(windows)]
    use acyclic_fs::{MountFilesystem, MountPath};
    use std::sync::Arc;

    let source = PathBuf::from(args.first().ok_or("mount-smoke: missing <src>")?).canonicalize()?;
    let work = PathBuf::from(args.get(1).ok_or("mount-smoke: missing <work>")?);
    let store_dir = work.join("store");
    let mount_dir = work.join("mnt");
    fs::create_dir_all(&store_dir)?;
    fs::create_dir_all(&mount_dir)?;

    let capabilities = probe_native_mount();
    println!(
        "probe: kind={:?} available={} writable={} reason={:?}",
        capabilities.kind,
        capabilities.available(),
        capabilities.available(),
        capabilities.unavailable_reason
    );
    if !capabilities.available() {
        return Err("native mount unavailable or read-only — fork engine gate FAILS".into());
    }

    // Capture the fixture and publish it, exactly like the daemon does.
    let runtime = tokio::runtime::Runtime::new()?;
    let (volume_id, _generation) = runtime.block_on(capture_and_commit(&source, &store_dir))?;

    // Fresh writable Head checkout for the mount (the fork shape).
    let config = volume_config();
    let (checkout, fs_engine) = runtime.block_on(async {
        let cancel = CancellationToken::new();
        let fs_engine = LocalFs::local(local_options(&store_dir)?)
            .await
            .map_err(engine_err("reopen store"))?;
        let volume = fs_engine
            .open_volume(volume_id, WorkCounters::UNBOUNDED, &cancel)
            .await
            .map_err(engine_err("open volume"))?
            .value;
        let checkout = volume
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
        Ok::<_, Failure>((checkout, fs_engine))
    })?;
    let _keep_engine_alive = fs_engine;

    let shared = Arc::new(SharedCheckout::new(checkout));
    let mount_source = Arc::new(
        CheckoutMountSource::new(Arc::clone(&shared), config)
            .map_err(engine_err("mount source"))?,
    );
    #[cfg(windows)]
    let mount_source_for_mount = Arc::clone(&mount_source);
    #[cfg(not(windows))]
    let mount_source_for_mount = mount_source;
    let started = Instant::now();
    let mut session = mount_native(
        NativeMountRequest {
            mount_id: acyclic_fs::MountId::new(),
            volume_id,
            destination: mount_dir.clone(),
            writable: true,
            shared: false,
        },
        mount_source_for_mount,
    )
    .map_err(engine_err("mount_native"))?;
    println!(
        "mounted at {} in {:?}",
        mount_dir.display(),
        started.elapsed()
    );

    // Everything below must not leave the mount attached on failure.
    let verdict = (|| -> Result<(), Failure> {
        // Read path: lazy listing + content identical to the fixture.
        let started = Instant::now();
        let mounted_readme = fs::read(mount_dir.join("README.md"))?;
        println!("first small read: {:?}", started.elapsed());
        if mounted_readme != fs::read(source.join("README.md"))? {
            return Err("README content mismatch through mount".into());
        }
        let started = Instant::now();
        let mounted_asset = fs::read(mount_dir.join("assets.bin"))?;
        println!(
            "8MB hydration read: {:?} ({} bytes)",
            started.elapsed(),
            mounted_asset.len()
        );
        if mounted_asset != fs::read(source.join("assets.bin"))? {
            return Err("asset content mismatch through mount".into());
        }
        if source.join("node_modules/.bin/tool").exists() {
            let mounted_link = fs::read_link(mount_dir.join("node_modules/.bin/tool"))?;
            if mounted_link != Path::new("../../tool.sh") {
                return Err(format!("symlink mismatch through mount: {mounted_link:?}").into());
            }
        }

        // Write path: overlay writes visible in the mount, invisible outside.
        fs::write(mount_dir.join("fork-note.txt"), b"written in the fork\n")?;
        fs::write(mount_dir.join("README.md"), b"edited in the fork\n")?;
        fs::create_dir(mount_dir.join("fork-dir"))?;
        if fs::read(mount_dir.join("fork-note.txt"))? != b"written in the fork\n" {
            return Err("write-then-read mismatch through mount".into());
        }
        if fs::read(mount_dir.join("README.md"))? != b"edited in the fork\n" {
            return Err("edit-then-read mismatch through mount".into());
        }
        if source.join("fork-note.txt").exists() {
            return Err("overlay write leaked into the source tree".into());
        }
        if fs::read(source.join("README.md"))? != mounted_readme {
            return Err("overlay edit leaked into the source tree".into());
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;

            // ProjFS identifies an import from outside the virtualization
            // root with an empty callback source. The provider must capture
            // the destination instead of treating that empty name as `/`.
            // Use another process because ProjFS deliberately suppresses
            // notifications for I/O issued by its provider process.
            let external = work.join("external-import.txt");
            let imported_host = mount_dir.join("imported.txt");
            fs::write(&external, b"imported through rename\n")?;
            let status = acyclic_native_runtime::process_status(std::process::Command::new("powershell.exe")
                .args(["-NoProfile", "-NonInteractive", "-Command"])
                .arg("Move-Item -LiteralPath $env:ACYCLIC_MOVE_SOURCE -Destination $env:ACYCLIC_MOVE_DESTINATION -ErrorAction Stop")
                .env("ACYCLIC_MOVE_SOURCE", &external)
                .env("ACYCLIC_MOVE_DESTINATION", &imported_host)
                .creation_flags(0x0800_0000), std::time::Duration::from_secs(120))?;
            if !status.success() {
                return Err(format!("external rename helper failed: {status}").into());
            }
            let imported = MountPath::root().child(
                "imported.txt"
                    .encode_utf16()
                    .flat_map(u16::to_le_bytes)
                    .collect(),
            );
            if mount_source.lookup(&imported)?.is_none() {
                return Err("external file rename was not captured by ProjFS".into());
            }
            let file = mount_source.open_file(&imported)?;
            if file.read_range(0, 24)?.as_ref() != b"imported through rename\n" {
                return Err("captured ProjFS import has wrong content".into());
            }
            drop(file);
            let exported = work.join("external-export.txt");
            let status = acyclic_native_runtime::process_status(std::process::Command::new("powershell.exe")
                .args(["-NoProfile", "-NonInteractive", "-Command"])
                .arg("Move-Item -LiteralPath $env:ACYCLIC_MOVE_SOURCE -Destination $env:ACYCLIC_MOVE_DESTINATION -ErrorAction Stop")
                .env("ACYCLIC_MOVE_SOURCE", &imported_host)
                .env("ACYCLIC_MOVE_DESTINATION", &exported)
                .creation_flags(0x0800_0000), std::time::Duration::from_secs(120))?;
            if !status.success() {
                return Err(format!("external export helper failed: {status}").into());
            }
            if mount_source.lookup(&imported)?.is_some() {
                return Err("external file export was not captured by ProjFS".into());
            }
            if fs::read(exported)? != b"imported through rename\n" {
                return Err("ProjFS export has wrong content".into());
            }
            let external_tree = work.join("external-tree");
            fs::create_dir(&external_tree)?;
            fs::write(external_tree.join("payload.txt"), b"rejected import\n")?;
            let status = acyclic_native_runtime::process_status(std::process::Command::new("powershell.exe")
                .args(["-NoProfile", "-NonInteractive", "-Command"])
                .arg("Move-Item -LiteralPath $env:ACYCLIC_MOVE_SOURCE -Destination $env:ACYCLIC_MOVE_DESTINATION -ErrorAction Stop")
                .env("ACYCLIC_MOVE_SOURCE", &external_tree)
                .env("ACYCLIC_MOVE_DESTINATION", mount_dir.join("imported-tree"))
                .creation_flags(0x0800_0000), std::time::Duration::from_secs(120))?;
            if status.success() {
                return Err(
                    "ProjFS admitted an external directory without an exact close boundary".into(),
                );
            }
        }
        Ok(())
    })();

    let stopped = session.stop();
    println!("unmounted cleanly: {stopped:?}");
    verdict?;
    stopped.map_err(engine_err("unmount"))?;

    println!("MOUNT SMOKE OK: writable native mount serves, isolates, and detaches");
    Ok(())
}

// ---------------------------------------------------------------------------
// mount-hold: attach one mount and hold it for <seconds> so failing
// operations can be probed interactively from a shell.
// ---------------------------------------------------------------------------

pub(super) fn mount_hold(args: &[String]) -> Result<(), Failure> {
    use acyclic_fs::{CheckoutMountSource, NativeMountRequest, SharedCheckout, mount_native};
    use std::sync::Arc;

    let source = PathBuf::from(args.first().ok_or("mount-hold: missing <src>")?).canonicalize()?;
    let work = PathBuf::from(args.get(1).ok_or("mount-hold: missing <work>")?);
    let seconds: u64 = args.get(2).map_or(Ok(60), |value| value.parse())?;
    let store_dir = work.join("store");
    let mount_dir = work.join("mnt");
    fs::create_dir_all(&store_dir)?;
    fs::create_dir_all(&mount_dir)?;

    let runtime = tokio::runtime::Runtime::new()?;
    let (volume_id, _generation) = runtime.block_on(capture_and_commit(&source, &store_dir))?;
    let config = volume_config();
    let checkout = runtime.block_on(async {
        let cancel = CancellationToken::new();
        let fs_engine = LocalFs::local(local_options(&store_dir)?)
            .await
            .map_err(engine_err("open store"))?;
        let volume = fs_engine
            .open_volume(volume_id, WorkCounters::UNBOUNDED, &cancel)
            .await
            .map_err(engine_err("open volume"))?
            .value;
        let checkout = volume
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
        std::mem::forget(fs_engine);
        Ok::<_, Failure>(checkout)
    })?;
    let shared = Arc::new(SharedCheckout::new(checkout));
    let mount_source =
        Arc::new(CheckoutMountSource::new(shared, config).map_err(engine_err("mount source"))?);
    let mut session = mount_native(
        NativeMountRequest {
            mount_id: acyclic_fs::MountId::new(),
            volume_id,
            destination: mount_dir.clone(),
            writable: true,
            shared: false,
        },
        mount_source,
    )
    .map_err(engine_err("mount"))?;
    println!("HELD: {} for {seconds}s", mount_dir.display());
    std::thread::sleep(std::time::Duration::from_secs(seconds));
    session.stop().map_err(engine_err("unmount"))?;
    println!("released");
    Ok(())
}

// ---------------------------------------------------------------------------
// source-probe: exercise MountFilesystem directly (no kernel, no NFS) to
// pinpoint which callback fails and with what typed error.
// ---------------------------------------------------------------------------

pub(super) fn source_probe(args: &[String]) -> Result<(), Failure> {
    use acyclic_fs::{CheckoutMountSource, MountFilesystem, MountPath, SharedCheckout};
    use std::sync::Arc;

    let source =
        PathBuf::from(args.first().ok_or("source-probe: missing <src>")?).canonicalize()?;
    let work = PathBuf::from(args.get(1).ok_or("source-probe: missing <work>")?);
    let store_dir = work.join("store");
    fs::create_dir_all(&store_dir)?;

    let runtime = tokio::runtime::Runtime::new()?;
    let (volume_id, _generation) = runtime.block_on(capture_and_commit(&source, &store_dir))?;
    let config = volume_config();
    let checkout = runtime.block_on(async {
        let cancel = CancellationToken::new();
        let fs_engine = LocalFs::local(local_options(&store_dir)?)
            .await
            .map_err(engine_err("open store"))?;
        let volume = fs_engine
            .open_volume(volume_id, WorkCounters::UNBOUNDED, &cancel)
            .await
            .map_err(engine_err("open volume"))?
            .value;
        let checkout = volume
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
        std::mem::forget(fs_engine);
        Ok::<_, Failure>(checkout)
    })?;
    let shared = Arc::new(SharedCheckout::new(checkout));
    let mount_source =
        CheckoutMountSource::new(shared, config).map_err(engine_err("mount source"))?;

    let readme = MountPath::root().child(b"README.md".to_vec());
    println!(
        "lookup(/):          {:?}",
        mount_source
            .lookup(&MountPath::root())
            .map(|l| l.map(|l| l.node.kind))
    );
    println!(
        "lookup(README.md):  {:?}",
        mount_source
            .lookup(&readme)
            .map(|l| l.map(|l| (l.node.kind, l.node.logical_bytes)))
    );
    println!(
        "read_directory(/):  {:?}",
        mount_source
            .read_directory(&MountPath::root(), None, 16)
            .map(|p| p.entries().len())
    );
    match mount_source.open_file(&readme) {
        Ok(file) => {
            println!("open_file(README):  Ok");
            println!(
                "  handle.lookup():  {:?}",
                file.lookup().map(|l| l.node.logical_bytes)
            );
            println!(
                "  read_range(0,13): {:?}",
                file.read_range(0, 13)
                    .map(|b| String::from_utf8_lossy(&b).into_owned())
            );
        }
        Err(error) => println!("open_file(README):  ERR {error:?}"),
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// mount-smoke2: TWO simultaneous native mounts from ONE process — the
// minimal repro for the fork engine's N>1 requirement.
// ---------------------------------------------------------------------------

pub(super) fn mount_smoke2(args: &[String]) -> Result<(), Failure> {
    use acyclic_fs::{CheckoutMountSource, NativeMountRequest, SharedCheckout, mount_native};
    use std::sync::Arc;

    let source =
        PathBuf::from(args.first().ok_or("mount-smoke2: missing <src>")?).canonicalize()?;
    let work = PathBuf::from(args.get(1).ok_or("mount-smoke2: missing <work>")?);
    let store_dir = work.join("store");
    fs::create_dir_all(&store_dir)?;

    let runtime = tokio::runtime::Runtime::new()?;
    let (volume_id, _generation) = runtime.block_on(capture_and_commit(&source, &store_dir))?;
    let config = volume_config();

    let mut sessions = Vec::new();
    for index in 0..2u32 {
        let mount_dir = work.join(format!("mnt{index}"));
        fs::create_dir_all(&mount_dir)?;
        let checkout = runtime.block_on(async {
            let cancel = CancellationToken::new();
            let fs_engine = LocalFs::local(local_options(&store_dir)?)
                .await
                .map_err(engine_err("open store"))?;
            let volume = fs_engine
                .open_volume(volume_id, WorkCounters::UNBOUNDED, &cancel)
                .await
                .map_err(engine_err("open volume"))?
                .value;
            let checkout = volume
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
            std::mem::forget(fs_engine); // keep engine alive for the mount
            Ok::<_, Failure>(checkout)
        })?;
        let shared = Arc::new(SharedCheckout::new(checkout));
        let mount_source =
            Arc::new(CheckoutMountSource::new(shared, config).map_err(engine_err("mount source"))?);
        let started = Instant::now();
        let session = mount_native(
            NativeMountRequest {
                mount_id: acyclic_fs::MountId::new(),
                volume_id,
                destination: mount_dir.clone(),
                writable: true,
                shared: false,
            },
            mount_source,
        )
        .map_err(engine_err(if index == 0 {
            "FIRST mount"
        } else {
            "SECOND mount"
        }))?;
        println!(
            "mount {index} attached at {} in {:?}",
            mount_dir.display(),
            started.elapsed()
        );
        let listing = fs::read_dir(&mount_dir)?.count();
        println!("mount {index} lists {listing} entries");
        sessions.push(session);
    }

    for (index, mut session) in sessions.into_iter().enumerate() {
        session.stop().map_err(engine_err("unmount"))?;
        println!("mount {index} detached");
    }
    println!("MOUNT SMOKE2 OK: two simultaneous sessions in one process");
    Ok(())
}

// ---------------------------------------------------------------------------
// corpus: synthetic tree of <files> files totalling <mb> MiB, 100 per dir
// ---------------------------------------------------------------------------
