use super::*;
use crate::Fs;
use crate::model::{
    AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
};
use crate::native_mount::ViewOrigin;
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStringExt;

#[cfg(target_os = "macos")]
#[allow(unsafe_code, reason = "declares the NFS transport's C test hooks")]
unsafe extern "C" {
    fn nfs4_test_exclusive_replay_identity() -> std::ffi::c_int;
    fn nfs4_test_namedattr_exclusive_replay_identity() -> std::ffi::c_int;
    fn nfs4_test_readdir_cookie_verifier() -> std::ffi::c_int;
    fn nfs4_test_sync_acknowledgement() -> std::ffi::c_int;
}

fn shared_sources_with_publication(
    profile: FilesystemProfile,
    publication: MountPublication,
) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
    let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
    config.profile = profile;
    let fs = Fs::memory();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let checkout = runtime.block_on(async {
        let cancellation = CancellationToken::new();
        let volume = fs
            .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        volume
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
            .await
            .map(|receipt| receipt.value)
    })?;
    let checkout = Arc::new(SharedCheckout::with_publication(checkout, publication));
    Ok((
        CheckoutMountSource::new(Arc::clone(&checkout), config)?,
        CheckoutMountSource::new(checkout, config)?,
    ))
}

fn tracking_sources(
    profile: FilesystemProfile,
) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
    let mut config = VolumeConfig::portable(Lifecycle::Ephemeral);
    config.profile = profile;
    let fs = Fs::memory();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let (first, second) = runtime.block_on(async {
        let cancellation = CancellationToken::new();
        let volume = fs
            .create_volume(config, WorkBudget::UNBOUNDED, &cancellation)
            .await?
            .value;
        let mode = CheckoutMode {
            access: AccessMode::ReadWrite,
            consistency: ConsistencyMode::TrackingSafe,
            mutations: MutationMode::PrivateOverlay,
        };
        let first = volume
            .checkout(
                GenerationSelector::Head,
                mode,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        let second = volume
            .checkout(
                GenerationSelector::Head,
                mode,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await?
            .value;
        Ok::<_, OperationFailure<FsError>>((first, second))
    })?;
    Ok((
        CheckoutMountSource::new(Arc::new(SharedCheckout::new(first)), config)?,
        CheckoutMountSource::new(Arc::new(SharedCheckout::new(second)), config)?,
    ))
}

fn native_test_path(name: &str) -> MountPath {
    #[cfg(target_os = "windows")]
    let bytes = name.encode_utf16().flat_map(u16::to_le_bytes).collect();
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let bytes = name.as_bytes().to_vec();
    MountPath::root().child(bytes)
}

include!("runtime.rs");
include!("namespace.rs");
include!("lifecycle.rs");
include!("platform.rs");
include!("source_limits.rs");
