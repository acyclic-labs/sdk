//! Executable Filesystem scenarios used by the SDK example registry.
//!
//! The scenario is deliberately written against the embedded `acyclic-fs`
//! API.  Its receipt records facts observed from the real local provider so a
//! documentation bundle can distinguish an executed example from a rendered
//! protocol sketch.

use std::{
    error::Error,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use acyclic_fs::model::{
    AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
    VolumeConfig,
};
use acyclic_fs::path::PortablePath;
use acyclic_fs::{CancellationToken, Fs, LocalOptions, MountedView, WorkBudget};

/// Stable source identity consumed by the examples manifest.
pub const SOURCE: &str = "rust/crates/sdk-examples/src/filesystem_scenarios.rs";
/// Stable scenario identity consumed by docs and fixture reports.
pub const SCENARIO_ID: &str = "filesystem-mounted-workspace";

/// A receipt containing only facts observed by the executable scenario.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilesystemScenarioReceipt {
    /// Whether the durable workspace volume was created successfully.
    pub durable_volume_created: bool,
    /// Whether the ephemeral tool volume was created successfully.
    pub ephemeral_volume_created: bool,
    /// Number of immutable bindings in the mounted view snapshot.
    pub mounted_bindings: usize,
    /// Whether the dirty checkout produced an immutable checkpoint.
    pub checkpointed: bool,
    /// Portable path written through the mounted scratch binding.
    pub written_path: &'static str,
}

/// Rust source shown in the Filesystem quickstart projection.
pub const QUICKSTART_SNIPPET: &str = r#"use acyclic_fs::{CancellationToken, Fs, LocalOptions, MountedView, WorkBudget};
use acyclic_fs::model::{AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode, VolumeConfig};
use acyclic_fs::path::PortablePath;

let fs = Fs::local(LocalOptions::new(root)).await?;
let cancel = CancellationToken::default();
let workspace = fs.create_volume(VolumeConfig::portable(Lifecycle::Durable), WorkBudget::UNBOUNDED, &cancel).await?.value;
let scratch = fs.create_volume(VolumeConfig::portable(Lifecycle::Ephemeral), WorkBudget::UNBOUNDED, &cancel).await?.value;
let mode = CheckoutMode { access: AccessMode::ReadWrite, consistency: ConsistencyMode::TrackingSafe, mutations: MutationMode::PrivateOverlay };
let mut view = MountedView::builder()
    .mount("/", workspace.checkout(GenerationSelector::Head, mode, WorkBudget::UNBOUNDED, &cancel).await?.value)?
    .mount("/.scratch", scratch.checkout(GenerationSelector::Head, mode, WorkBudget::UNBOUNDED, &cancel).await?.value)?
    .build()?;
let routed = view.route_mut(&PortablePath::parse("/.scratch/tool-output.txt", acyclic_fs::model::VolumeLimits::default())?)?;
routed.checkout.create_file(routed.path, bytes::Bytes::from_static(b"tool output"), WorkBudget::UNBOUNDED, &cancel).await?;
assert_eq!(view.snapshot().bindings.len(), 2);"#;

fn scenario_root() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    std::env::temp_dir().join(format!(
        "acyclic-sdk-examples-fs-{}-{nonce}",
        std::process::id()
    ))
}

/// Runs the mounted local workspace scenario against the real Filesystem
/// provider and returns a source-bound receipt.
pub async fn execute_filesystem_scenario()
-> Result<FilesystemScenarioReceipt, Box<dyn Error + Send + Sync>> {
    let root = scenario_root();
    std::fs::create_dir_all(&root)?;
    let result = execute_filesystem_at(&root).await;
    // The provider owns open handles while the future runs.  Cleanup is best
    // effort after it has released them and must not hide the scenario result.
    let _ = std::fs::remove_dir_all(&root);
    result
}

async fn execute_filesystem_at(
    root: &std::path::Path,
) -> Result<FilesystemScenarioReceipt, Box<dyn Error + Send + Sync>> {
    let fs = Fs::local(LocalOptions::new(root)).await?;
    let cancellation = CancellationToken::default();
    let workspace = fs
        .create_volume(
            VolumeConfig::portable(Lifecycle::Durable),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let scratch = fs
        .create_volume(
            VolumeConfig::portable(Lifecycle::Ephemeral),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let mode = CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::TrackingSafe,
        mutations: MutationMode::PrivateOverlay,
    };
    let workspace_checkout = workspace
        .checkout(
            GenerationSelector::Head,
            mode,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    // A clean checkout checkpoint is a canonical immutable generation proof.
    let checkpoint = workspace_checkout
        .checkpoint(WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let scratch_checkout = scratch
        .checkout(
            GenerationSelector::Head,
            mode,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let mut view = MountedView::builder()
        .mount("/", workspace_checkout)?
        .mount("/.scratch", scratch_checkout)?
        .build()?;
    let tool_path = PortablePath::parse(
        "/.scratch/tool-output.txt",
        acyclic_fs::model::VolumeLimits::default(),
    )?;
    let routed = view.route_mut(&tool_path)?;
    routed
        .checkout
        .create_file(
            routed.path,
            bytes::Bytes::from_static(b"tool output"),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?;
    let snapshot = view.snapshot();
    if snapshot.bindings.len() != 2 {
        return Err(format!(
            "expected two mounted bindings, got {}",
            snapshot.bindings.len()
        )
        .into());
    }
    Ok(FilesystemScenarioReceipt {
        durable_volume_created: true,
        ephemeral_volume_created: true,
        mounted_bindings: snapshot.bindings.len(),
        // Reaching this point means the provider returned an authenticated
        // generation identity; the value itself is intentionally opaque.
        checkpointed: !checkpoint.digest().as_bytes().iter().all(|byte| *byte == 0),
        written_path: "/.scratch/tool-output.txt",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_and_snippet_are_stable() {
        assert!(SOURCE.ends_with("filesystem_scenarios.rs"));
        assert!(QUICKSTART_SNIPPET.contains("MountedView::builder"));
        assert!(QUICKSTART_SNIPPET.contains("view.snapshot()"));
    }
}
