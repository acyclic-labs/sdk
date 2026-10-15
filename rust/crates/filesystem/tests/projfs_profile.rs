//! End-to-end Windows ProjFS profiling workload.

#![cfg(windows)]

use acyclic_fs::kernel::FileMetadata;
use acyclic_fs::{
    CancellationToken, Fs, IdempotencyKey, LocalOptions, MountOptions, MountPublication, WorkBudget,
};
use bytes::Bytes;
use std::time::Instant;

#[tokio::test]
#[ignore = "requires a writable ProjFS host; run alone for profiling"]
async fn profile_enumeration_hydration_write_capture() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("ACYCLIC_PROJFS_PROFILE_TRACING").is_some() {
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .with_env_filter("acyclic_fs::native_mount=trace")
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
                .with_ansi(false)
                .with_writer(std::io::stderr)
                .finish(),
        )?;
    }
    acyclic_fs::initialize_native_mount_services()?;
    let work = acyclic_fs::NativeMaterializationWork::new(
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .map_err(|(cause, _)| cause)?;
    let root = tempfile::tempdir()?;
    let workspace = Fs::local(LocalOptions::new(root.path().join("state")))
        .await?
        .create_workspace("projfs-profile", WorkBudget::UNBOUNDED, work.cancellation())
        .await
        .map_err(|failure| failure.error)?
        .value;
    let payload = Bytes::from(vec![0x5a; 64 * 1024]);
    let mut transaction = workspace
        .begin_transaction(IdempotencyKey::new(), &work, work.cancellation())
        .await?;
    for index in 0..256 {
        transaction
            .create_file(
                &format!("/entry{index:03}"),
                payload.clone(),
                FileMetadata::default(),
                &work,
                work.cancellation(),
            )
            .await?;
    }
    transaction
        .commit::<acyclic_fs::WorkspaceError, _, _>(
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            work.cancellation(),
            |_| {
                std::future::ready(Ok(acyclic_fs::OperationReceipt {
                    value: (),
                    work: acyclic_fs::WorkCounters::UNCHARGED,
                }))
            },
        )
        .await
        .map_err(|failure| failure.error)?;
    let destination = root.path().join("mount");
    std::fs::create_dir(&destination)?;
    phase("mount")?;
    let started = Instant::now();
    let mount = workspace
        .mount(
            &destination,
            MountOptions::read_write().publication(MountPublication::Manual),
        )
        .await?;
    eprintln!("PROJFS_TIME mount {:?}", started.elapsed());

    phase("client-start")?;
    let started = Instant::now();
    let script = "$ErrorActionPreference = 'Stop'; \
        $root = $env:ACYCLIC_PROJFS_PROFILE_ROOT; \
        function Set-Phase([string]$name) { \
            Write-Output ('PROJFS_PHASE ' + $name); \
            if ($env:ACYCLIC_PROJFS_PROFILE_PHASE) { \
                [IO.File]::WriteAllText($env:ACYCLIC_PROJFS_PROFILE_PHASE, $name); \
            }; \
        }; \
        Set-Phase 'enumerate'; $timer = [Diagnostics.Stopwatch]::StartNew(); \
        for ($round = 0; $round -lt 8; $round++) { \
            $files = [IO.Directory]::GetFiles($root); \
            if ($files.Length -ne 256) { throw 'enumeration count' }; \
        }; \
        Write-Output ('PROJFS_TIME enumerate ' + $timer.Elapsed.TotalMilliseconds + ' ms'); \
        Set-Phase 'read'; $timer.Restart(); \
        for ($round = 0; $round -lt 8; $round++) { \
            foreach ($file in $files) { \
                $bytes = [IO.File]::ReadAllBytes($file); \
                if ($bytes.Length -ne 65536) { throw 'read length' }; \
            }; \
        }; \
        Write-Output ('PROJFS_TIME read ' + $timer.Elapsed.TotalMilliseconds + ' ms'); \
        Set-Phase 'write'; $timer.Restart(); \
        for ($round = 0; $round -lt 8; $round++) { \
            for ($index = 0; $index -lt 32; $index++) { \
                $path = Join-Path $root ('entry{0:D3}' -f $index); \
                [IO.File]::WriteAllBytes($path, [byte[]]::new(65536)); \
            }; \
        }; \
        Write-Output ('PROJFS_TIME write ' + $timer.Elapsed.TotalMilliseconds + ' ms')";
    let mut command = std::process::Command::new("powershell.exe");
    command
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .env("ACYCLIC_PROJFS_PROFILE_ROOT", &destination);
    let mut child = acyclic_native_runtime::ProcessTree::spawn(&mut command)?;
    let result = child.wait(std::time::Duration::from_secs(120));
    child.terminate(None)?;
    assert!(result?.success());
    eprintln!("PROJFS_TIME client-total {:?}", started.elapsed());

    phase("capture-sync")?;
    let started = Instant::now();
    let synced = mount.sync().await;
    eprintln!("PROJFS_TIME capture-sync {:?}", started.elapsed());
    phase("unmount")?;
    let unmounted = mount.unmount().await;
    synced?;
    unmounted?;
    phase("verify")?;
    let changed = workspace
        .read(
            "/entry000",
            65536,
            WorkBudget::UNBOUNDED,
            work.cancellation(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    assert_eq!(changed.as_ref(), &[0; 65536]);
    let unchanged = workspace
        .read(
            "/entry255",
            65536,
            WorkBudget::UNBOUNDED,
            work.cancellation(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    assert_eq!(unchanged.as_ref(), payload.as_ref());
    Ok(())
}

fn phase(name: &str) -> std::io::Result<()> {
    eprintln!("PROJFS_PHASE {name}");
    if let Some(path) = std::env::var_os("ACYCLIC_PROJFS_PROFILE_PHASE") {
        std::fs::write(path, name)?;
    }
    Ok(())
}
