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
    let (prefix, files) = profile_layout();
    if !prefix.is_empty() {
        transaction
            .create_dir_all("/nested", &work, work.cancellation())
            .await?;
    }
    for index in 0..files {
        transaction
            .create_file(
                &format!("/{prefix}entry{index:03}"),
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
    if std::env::var_os("ACYCLIC_PROJFS_PROFILE_START_GATE").is_some() {
        use std::io::Read as _;
        let mut signal = [0];
        std::io::stdin().read_exact(&mut signal)?;
        assert_eq!(signal, *b"g");
    }
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
    let mut command = std::process::Command::new(std::env::current_exe()?);
    command
        .args([
            "--ignored",
            "--exact",
            "projfs_profile::profile_external_client",
            "--nocapture",
        ])
        .env("ACYCLIC_PROJFS_PROFILE_ROOT", destination.join(prefix));
    let mut child = acyclic_native_runtime::ProcessTree::spawn(&mut command)?;
    let result = child.wait(std::time::Duration::from_secs(45));
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
            &format!("/{prefix}entry000"),
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
            &format!("/{prefix}entry{:03}", files - 1),
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

fn profile_layout() -> (&'static str, usize) {
    if std::env::var_os("ACYCLIC_PROJFS_PROFILE_NESTED").is_some() {
        ("nested/", 64)
    } else {
        ("", 256)
    }
}

fn phase(name: &str) -> std::io::Result<()> {
    eprintln!("PROJFS_PHASE {name}");
    if let Some(path) = std::env::var_os("ACYCLIC_PROJFS_PROFILE_PHASE") {
        std::fs::write(path, name)?;
    }
    Ok(())
}

#[test]
#[ignore = "contained external client for the ProjFS workload"]
fn profile_external_client() -> Result<(), Box<dyn std::error::Error>> {
    let Some(root) = std::env::var_os("ACYCLIC_PROJFS_PROFILE_ROOT") else {
        return Ok(());
    };
    let root = std::path::PathBuf::from(root);
    let (_, count) = profile_layout();
    phase("enumerate")?;
    let started = Instant::now();
    let mut files = Vec::new();
    for _ in 0..8 {
        files = std::fs::read_dir(&root)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        assert_eq!(files.len(), count);
    }
    eprintln!("PROJFS_TIME enumerate {:?}", started.elapsed());
    phase("read")?;
    let started = Instant::now();
    for _ in 0..8 {
        for file in &files {
            let bytes = std::fs::read(file)?;
            assert_eq!(bytes.len(), 65_536);
            assert!(bytes.iter().all(|byte| *byte == 0x5a));
        }
    }
    eprintln!("PROJFS_TIME read {:?}", started.elapsed());
    phase("write")?;
    let started = Instant::now();
    let bytes = [0; 65_536];
    for _ in 0..8 {
        for index in 0..32 {
            std::fs::write(root.join(format!("entry{index:03}")), &bytes)?;
        }
    }
    eprintln!("PROJFS_TIME write {:?}", started.elapsed());
    Ok(())
}
