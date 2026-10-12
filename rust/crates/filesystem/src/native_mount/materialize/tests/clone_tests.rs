use super::*;
use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
    use crate::model::{Lifecycle, VolumeConfig, VolumeLimits};
use crate::VolumeId;

#[tokio::test]
async fn identical_regular_payloads_use_native_clone_without_aliasing()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    #[cfg(target_os = "macos")]
    let directory = tempfile::tempdir()?;
    #[cfg(windows)]
    let directory = {
        if let Some(parent) = std::env::var_os("ACYCLIC_TEST_BLOCK_CLONE_ROOT") {
            tempfile::Builder::new()
                .prefix("acyclic-materialize-clone-")
                .tempdir_in(parent)?
        } else {
            tempfile::tempdir()?
        }
    };
    let clone_available =
        crate::probe_native_storage_capabilities(directory.path())?.block_cloning;
    let limits = VolumeLimits::default();
    let cancellation = CancellationToken::new();
    let volume = super::super::tests::memory_volume(
            VolumeId::from_bytes([73; 16]),
            VolumeConfig { limits, ..VolumeConfig::portable(Lifecycle::Ephemeral) },
            &cancellation,
        )
        .await?;
    let mut checkout = super::super::tests::private_checkout(&volume, &cancellation).await?;
    // The payload deliberately ends between cluster boundaries: Windows
    // must clone the aligned body and copy the short tail.
    let bytes = Bytes::from(vec![0xA5; 128 * 1024 + 17]);
    for name in ["first.bin", "second.bin"] {
        let path = NamespacePath::new(
            vec![LogicalName::new(
                NameEncoding::Utf8,
                name.as_bytes().to_vec(),
                limits.maximum_component_bytes,
            )?],
            limits,
        )?;
        checkout
            .create(crate::AuthoredMutation::CreateFile { path: path, bytes: (bytes.clone()).into(), metadata: crate::kernel::FileMetadata::default() }, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
    }
    let destination = directory.path().join("view");
    std::fs::create_dir(&destination)?;
    let receipt = materialize_checkout(
        &mut checkout,
        &MaterializeOptions {
            destination: destination.clone(),
            expected_root_identity: None,
            maximum_directory_entries: 16,
            maximum_extent_spans: 16,
            transfer_bytes: 64 * 1024,
        },
        super::restore_recovery_tests::FINITE_MATERIALIZATION_BUDGET,
        &cancellation,
    )
    .await?;
    assert_eq!(receipt.value.files, 2);
    assert_eq!(
        receipt.value.written_bytes,
        bytes.len() as u64 * if clone_available { 1 } else { 2 }
    );
    let first = destination.join("first.bin");
    let second = destination.join("second.bin");
    assert_eq!(std::fs::read(&first)?.as_slice(), bytes.as_ref());
    assert_eq!(std::fs::read(&second)?.as_slice(), bytes.as_ref());
    std::fs::write(&first, b"changed")?;
    assert_eq!(std::fs::read(&second)?.as_slice(), bytes.as_ref());
    Ok(())
}
