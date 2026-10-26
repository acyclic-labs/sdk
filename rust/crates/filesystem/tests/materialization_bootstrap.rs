//! Cold-process native materialization must use the production library graph.
//! Unit-test accounting initializes IO and cannot establish this precondition.

use acyclic_fs::kernel::{FileMetadata, LogicalName, NameEncoding, NamespacePath};
use acyclic_fs::model::{
    AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, MutationMode,
};
use acyclic_fs::{
    AuthoredMutation, CancellationToken, Fs, HostPathReplacement, Lifecycle, MaterializeOptions,
    NativeMaterializationWork, VolumeConfig, VolumeId, WorkBudget, materialize_checkout,
    materialize_checkout_paths, restore_checkout_host_path,
};
use std::path::Path;

const BUDGET: WorkBudget = acyclic_fs::performance::work_budget(100_000, 64 * 1024 * 1024);
const MODE: &str = "ACYCLIC_MATERIALIZATION_BOOTSTRAP";

#[test]
fn standalone_materialization_bootstraps_native_io() -> Result<(), Box<dyn std::error::Error>> {
    if let Ok(mode) = std::env::var(MODE) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        return runtime.block_on(materialize_in_fresh_process(&mode));
    }
    let mut failures = Vec::new();
    for mode in ["checkout", "paths", "restore"] {
        let output = acyclic_native_runtime::process_output(
            std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "standalone_materialization_bootstraps_native_io",
                    "--test-threads=1",
                ])
                .env(MODE, mode),
            std::time::Duration::from_secs(5),
            16 * 1024,
        )?;
        if !output.status.success()
            || !String::from_utf8_lossy(&output.stdout).contains("1 passed;")
        {
            failures.push((mode, output));
        }
    }
    assert!(failures.is_empty(), "{failures:?}");
    Ok(())
}

async fn materialize_in_fresh_process(mode: &str) -> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let work =
        NativeMaterializationWork::new(BUDGET, &cancellation).map_err(|failure| failure.0)?;
    let Err((error, _)) = acyclic_native_runtime::NativeFile::from_file(
        tempfile::tempfile()?,
        Some(acyclic_native_runtime::NativeFileMeter::Owned(
            work.runtime_meter(),
        )),
    ) else {
        return Err("native services were already initialized".into());
    };
    assert_eq!(error.kind(), std::io::ErrorKind::NotConnected);
    let config = VolumeConfig::portable(Lifecycle::Ephemeral);
    let volume = Fs::memory()
        .create_volume_with_id(
            VolumeId::new(),
            config,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let mut checkout = volume
        .checkout(
            GenerationSelector::Head,
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::PrivateOverlay,
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let file = NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::Utf8,
            b"file".to_vec(),
            config.limits.maximum_component_bytes,
        )?],
        config.limits,
    )?;
    checkout
        .create(
            AuthoredMutation::CreateFile {
                path: file,
                bytes: acyclic_stream::OwnedBytes::from_static(b"cold materialization"),
                metadata: FileMetadata::default(),
            },
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let output = tempfile::tempdir()?;
    let options = MaterializeOptions::native(output.path());
    run_entry_point(mode, &mut checkout, &options, &cancellation).await?;
    assert_eq!(
        std::fs::read(output.path().join("file"))?,
        b"cold materialization"
    );
    work.receipt().verify(BUDGET)?;
    Ok(())
}

async fn run_entry_point<A: acyclic_fs::AsyncAuthorityStore, O: acyclic_fs::AsyncObjectStore>(
    mode: &str,
    checkout: &mut acyclic_fs::Checkout<A, O>,
    options: &MaterializeOptions,
    cancellation: &CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    match mode {
        "checkout" => {
            materialize_checkout(checkout, options, BUDGET, cancellation)
                .await?
                .work
                .verify(BUDGET)?;
        }
        "paths" => {
            materialize_checkout_paths(
                checkout,
                &[NamespacePath::new(
                    Vec::new(),
                    checkout.volume_config().limits,
                )?],
                options,
                BUDGET,
                cancellation,
            )
            .await?
            .work
            .verify(BUDGET)?;
        }
        "restore" => {
            restore_checkout_host_path(
                checkout,
                Path::new("file"),
                HostPathReplacement::Atomic,
                options,
                BUDGET,
                cancellation,
            )
            .await?
            .work
            .verify(BUDGET)?;
        }
        _ => return Err("unknown materialization entry point".into()),
    }
    Ok(())
}
