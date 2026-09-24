#![cfg_attr(not(target_os = "macos"), allow(missing_docs))]
#![cfg(target_os = "macos")]

//! Local-only APFS native working-set preparation and capture benchmark.

use std::sync::Arc;
use std::time::Instant;

use acyclic_fs::demand::native::NativeDemandSource;
use acyclic_fs::model::{FilesystemProfile, VolumeLimits};
use acyclic_fs::{
    CancellationToken, Fs, LazyWorkspace, LocalOptions, MaterializeOptions,
    MemoryLazyWorkspaceStore, MountPath, PublicationPermit, WorkBudget,
};
use acyclic_objects::LocalDurability;
use serde_json::json;

fn component(value: &str) -> Vec<u8> {
    value.as_bytes().to_vec()
}

#[tokio::test]
#[ignore = "local-only 10k-path APFS working-set qualification"]
async fn report_apfs_working_set_costs() -> Result<(), Box<dyn std::error::Error>> {
    const FILES_PER_DIRECTORY: usize = 100;
    const CHANGES: usize = 100;
    let directories = std::env::var("ACYCLIC_APFS_BENCH_DIRECTORIES")
        .map(|value| value.parse::<usize>())
        .unwrap_or(Ok(100))?;
    assert!(directories > 0);

    let source = tempfile::tempdir()?;
    let hot = source.path().join("hot");
    std::fs::create_dir(&hot)?;
    for directory in 0..directories {
        let target = hot.join(format!("d{directory:03}"));
        std::fs::create_dir(&target)?;
        for file in 0..FILES_PER_DIRECTORY {
            std::fs::write(
                target.join(format!("f{file:03}.txt")),
                format!("payload-{directory:03}-{file:03}\n"),
            )?;
        }
    }

    let state = tempfile::tempdir()?;
    let mut local = LocalOptions::new(state.path());
    if std::env::var_os("ACYCLIC_APFS_BENCH_BARRIER").is_some() {
        local.objects.durability = LocalDurability::Barrier;
    }
    let fs = Fs::local(local).await?;
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
        "macos-apfs-costs",
        demand,
        MemoryLazyWorkspaceStore::default(),
    )
    .await?;
    let attach_us = started.elapsed().as_micros();

    let started = Instant::now();
    let exact = lazy
        .exactify_subtree_with_permit(
            "/hot",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            PublicationPermit::Unrestricted,
        )
        .await?;
    let exactify_us = started.elapsed().as_micros();
    eprintln!(
        "{}",
        json!({
            "attach_us": attach_us,
            "exactify_us": exactify_us,
            "work": exact.work,
        })
    );
    if std::env::var_os("ACYCLIC_APFS_BENCH_EXACT_ONLY").is_some() {
        return Ok(());
    }

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
    let preparation_us = started.elapsed().as_micros();
    eprintln!("apfs preparation_us={preparation_us}");

    let started = Instant::now();
    working_set.validate_for_presentation().await?;
    let activation_us = started.elapsed().as_micros();

    let mut changed = Vec::with_capacity(CHANGES);
    for index in 0..CHANGES {
        let directory = index % directories;
        let file = index / directories;
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
            "schema": "acyclic-macos-apfs-working-set-cost-v1",
            "files": directories * FILES_PER_DIRECTORY,
            "changed_paths": CHANGES,
            "attach_us": attach_us,
            "exactify_us": exactify_us,
            "preparation_us": preparation_us,
            "activation_us": activation_us,
            "capture_us": capture_us,
            "sync_us": sync_us,
        })
    );
    Ok(())
}
