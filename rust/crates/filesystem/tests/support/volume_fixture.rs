use acyclic_fs::{AuthoredMutation, CancellationToken, VolumeConfig};
use acyclic_fs_format::identity::FileId;
use acyclic_fs_format::metadata::FileMetadata;
use acyclic_stream::OwnedBytes;

use super::B;
use super::checkout_fixture::{path, writable_pinned};

#[path = "memory_checkout.rs"]
mod memory_checkout;
pub use memory_checkout::{TestCheckout, TestResult, TestVolume, open_checkout};

/// Creates an in-memory volume and a pinned writable checkout.
pub async fn fixture(
    config: VolumeConfig,
    cancel: &CancellationToken,
) -> TestResult<(TestVolume, TestCheckout)> {
    #[cfg(not(target_arch = "wasm32"))]
    acyclic_native_runtime::initialize_native_io_services()?;
    let volume = memory_checkout::volume(config, cancel).await?;
    let checkout = open_checkout(&volume, writable_pinned(), cancel).await?;
    Ok((volume, checkout))
}

/// Creates a regular file with default metadata in a fixture checkout.
pub async fn create(
    checkout: &mut TestCheckout,
    name: &str,
    bytes: OwnedBytes,
    cancel: &CancellationToken,
) -> TestResult<FileId> {
    Ok(checkout
        .create(
            AuthoredMutation::CreateFile {
                file_id: None,
                path: path(name)?,
                bytes,
                metadata: FileMetadata::default(),
            },
            B,
            cancel,
        )
        .await?
        .value)
}
