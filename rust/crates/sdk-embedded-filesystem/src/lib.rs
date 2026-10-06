//! Small, executable proof that embedded SDK bindings can call the canonical
//! Rust Filesystem and Harness contracts without translating their behavior.

#[cfg(feature = "filesystem")]
use acyclic_fs::ByteRange;
#[cfg(feature = "filesystem")]
use acyclic_fs::model::{
    AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
    VolumeConfig, VolumeLimits,
};

/// The observable result of one bounded in-memory Filesystem scenario.
#[cfg(feature = "filesystem")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilesystemProbe {
    /// Number of mounts in the resulting Rust `MountedView` snapshot.
    pub mount_count: usize,
    /// Number of bytes written through the mounted checkout.
    pub bytes_written: usize,
    /// Bytes read back through the canonical checkout range API.
    pub bytes_read: usize,
    /// Whether the canonical range read exactly matched the write.
    pub read_back_matches: bool,
    /// Number of entries returned by the canonical root directory page.
    pub root_entry_count: usize,
}

/// Creates two canonical Rust volumes, mounts them, and writes through the
/// routed scratch checkout. This is the same behavior exposed to embedded
/// consumers; the probe only supplies a finite bound and returns observations.
#[cfg(feature = "filesystem")]
pub async fn run_filesystem_probe() -> Result<FilesystemProbe, String> {
    let fs = acyclic_fs::MemoryFs::memory();
    let cancellation = acyclic_fs::CancellationToken::default();
    let workspace = fs
        .create_volume(
            VolumeConfig::portable(Lifecycle::Ephemeral),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|error| error.to_string())?
        .value;
    let scratch = fs
        .create_volume(
            VolumeConfig::portable(Lifecycle::Ephemeral),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|error| error.to_string())?
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
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|error| error.to_string())?
        .value;
    let scratch_checkout = scratch
        .checkout(
            GenerationSelector::Head,
            writable,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|error| error.to_string())?
        .value;

    let mut view = acyclic_fs::MountedView::builder()
        .mount("/", workspace_checkout)
        .map_err(|error| error.to_string())?
        .mount("/.scratch", scratch_checkout)
        .map_err(|error| error.to_string())?
        .build()
        .map_err(|error| error.to_string())?;
    let path = acyclic_fs::path::PortablePath::parse(
        "/.scratch/embedded-output.txt",
        VolumeLimits::default(),
    )
    .map_err(|error| error.to_string())?;
    let routed = view.route_mut(&path).map_err(|error| error.to_string())?;
    let payload = bytes::Bytes::from_static(b"embedded filesystem probe");
    let expected = payload.clone();
    let bytes_written = payload.len();
    let relative_path = routed.path.clone();
    routed
        .checkout
        .create_file(
            relative_path.clone(),
            payload.clone(),
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|error| error.to_string())?;
    let read_back = routed
        .checkout
        .read_file_range(
            &relative_path,
            ByteRange {
                offset: 0,
                length: payload.len() as u64,
            },
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|error| error.error.to_string())?
        .value
        .bytes;
    drop(routed);
    let root = acyclic_fs::path::PortablePath::parse("/", VolumeLimits::default())
        .map_err(|error| error.to_string())?;
    let root_route = view.route_mut(&root).map_err(|error| error.to_string())?;
    let root_path = root_route.path.clone();
    let root_entry_count = root_route
        .checkout
        .list_directory(
            &root_path,
            None,
            32,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|error| error.error.to_string())?
        .value
        .entries
        .len();
    let snapshot = view.snapshot();
    Ok(FilesystemProbe {
        mount_count: snapshot.bindings.len(),
        bytes_written,
        bytes_read: read_back.len(),
        read_back_matches: read_back == expected,
        root_entry_count,
    })
}

/// A bounded Harness admission probe. It exercises the real Rust builder's
/// validation boundary without inventing a foreign execution implementation.
#[cfg(feature = "harness")]
pub fn run_harness_admission_probe() -> bool {
    acyclic_harness::HarnessBuilder::new()
        .name("embedded-probe")
        .build()
        .is_err()
}

#[cfg(feature = "harness")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HarnessProbe {
    /// Missing execution bindings are rejected by the canonical builder.
    pub missing_execution_rejected: bool,
    /// Empty names are rejected by the canonical builder before execution binding.
    pub empty_name_rejected: bool,
}

#[cfg(feature = "harness")]
pub fn run_harness_probe() -> HarnessProbe {
    HarnessProbe {
        missing_execution_rejected: acyclic_harness::HarnessBuilder::new().build().is_err(),
        empty_name_rejected: acyclic_harness::HarnessBuilder::new()
            .name(" ")
            .build()
            .is_err(),
    }
}

#[cfg(all(test, feature = "filesystem"))]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn memory_filesystem_probe_routes_and_writes() {
        let result = run_filesystem_probe().await.expect("memory probe succeeds");
        assert_eq!(result.mount_count, 2);
        assert_eq!(result.bytes_written, b"embedded filesystem probe".len());
        assert_eq!(result.bytes_read, result.bytes_written);
        assert!(result.read_back_matches);
        assert_eq!(result.root_entry_count, 0);
    }
}

#[cfg(all(test, feature = "harness"))]
mod harness_tests {
    use super::*;

    #[test]
    fn harness_rejects_missing_execution_bindings() {
        assert!(run_harness_admission_probe());
        assert_eq!(run_harness_probe().missing_execution_rejected, true);
        assert_eq!(run_harness_probe().empty_name_rejected, true);
    }
}
