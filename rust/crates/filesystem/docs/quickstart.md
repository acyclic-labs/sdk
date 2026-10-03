# Filesystem quickstart

`acyclic-fs` provides versioned workspaces. A volume owns a generation history;
a checkout reads one generation and stages changes against it. A commit creates
a new generation, so callers can retain an earlier generation as an immutable
reference.

The package identity for this guide is `acyclic-fs = 0.2.0`. Pin the exact
source revision in the generated docs bundle when publishing a guide; the
working checkout may contain unreleased changes.

The crate's default profile selects the local backend, memory support, native
watching, and native mounting on native targets. Cargo selects the target-specific
dependencies automatically, so native consumers use the local backend without a
feature flag. The `wasm32` target exposes the portable parts of the crate and
does not expose the native local or hosted service adapters.

The following is the complete executable example from
[`examples/embedded_workspace.rs`](../examples/embedded_workspace.rs). It
creates durable and ephemeral volumes, checks out both heads, mounts them at
different paths, and writes through the routed checkout.

```rust
use acyclic_fs::model::{
    AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle,
    MutationMode, VolumeConfig,
};
use acyclic_fs::path::PortablePath;
use acyclic_fs::{CancellationToken, Fs, LocalOptions, MountedView, WorkBudget};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let fs = Fs::local(LocalOptions::new(root.path())).await?;
    let cancellation = CancellationToken::default();
    let workspace = fs
        .create_volume(VolumeConfig::portable(Lifecycle::Durable), WorkBudget::UNBOUNDED, &cancellation)
        .await?.value;
    let scratch = fs
        .create_volume(VolumeConfig::portable(Lifecycle::Ephemeral), WorkBudget::UNBOUNDED, &cancellation)
        .await?.value;
    let checkout_mode = CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency: ConsistencyMode::TrackingSafe,
        mutations: MutationMode::PrivateOverlay,
    };
    let workspace_checkout = workspace.checkout(
        GenerationSelector::Head, checkout_mode, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let scratch_checkout = scratch.checkout(
        GenerationSelector::Head, checkout_mode, WorkBudget::UNBOUNDED, &cancellation,
    ).await?.value;
    let mut view = MountedView::builder()
        .mount("/", workspace_checkout)?
        .mount("/.scratch", scratch_checkout)?
        .build()?;
    let path = PortablePath::parse(
        "/.scratch/tool-output.txt", acyclic_fs::model::VolumeLimits::default(),
    )?;
    let routed = view.route_mut(&path)?;
    routed.checkout.create_file(
        routed.path, bytes::Bytes::from_static(b"tool output"),
        WorkBudget::UNBOUNDED, &cancellation,
    ).await?;
    assert_eq!(view.snapshot().bindings.len(), 2);
    Ok(())
}
```

Run that example from the crate directory with:

```sh
cargo run --example embedded_workspace
```

The installed embedded proof uses the same portable model through an in-memory
provider and checks the routed write, range read, and directory observation in
[`sdk-embedded-filesystem`](../../sdk-embedded-filesystem/src/lib.rs). The
source-owned documentation scenario in
[`filesystem_scenarios.rs`](../../sdk-examples/src/filesystem_scenarios.rs)
also exercises the native local provider and records its checkpoint receipt.

`MountedView` controls path routing only. It does not grant authority across
volumes; the checkout and its provider still enforce their configured access,
consistency, and mutation modes.
