//! Graphcoder-shaped embedded consumption with independently configured volumes.

use acyclic_fs::model::{
    AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
    VolumeConfig,
};
use acyclic_fs::path::PortablePath;
use acyclic_fs::{CancellationToken, Fs, LocalOptions, MountedView, WorkBudget};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(root.path()))?;
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
    let writable = CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::TrackingSafe,
        mutations: MutationMode::PrivateOverlay,
    };
    let workspace_checkout = workspace
        .checkout(
            GenerationSelector::Head,
            writable,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    let scratch_checkout = scratch
        .checkout(
            GenerationSelector::Head,
            writable,
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
    assert_eq!(snapshot.bindings.len(), 2);
    Ok(())
}
