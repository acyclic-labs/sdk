use super::*;
use crate::Fs;
use crate::model::{
    AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
};
use crate::native_mount::ViewOrigin;

#[cfg(target_os = "macos")]
#[allow(unsafe_code, reason = "declares the NFS transport's C test hooks")]
unsafe extern "C" {
    fn nfs4_test_exclusive_replay_identity() -> std::ffi::c_int;
    fn nfs4_test_namedattr_exclusive_replay_identity() -> std::ffi::c_int;
    fn nfs4_test_readdir_cookie_verifier() -> std::ffi::c_int;
    fn nfs4_test_sync_acknowledgement() -> std::ffi::c_int;
}

fn sources(
    profile: FilesystemProfile,
    consistency: ConsistencyMode,
    publication: MountPublication,
) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
    let config = VolumeConfig {
        profile,
        ..VolumeConfig::portable(Lifecycle::Ephemeral)
    };
    configured_sources(config, consistency, publication)
}

fn configured_sources(
    config: VolumeConfig,
    consistency: ConsistencyMode,
    publication: MountPublication,
) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let cancellation = CancellationToken::new();
    let volume = runtime
        .block_on(fs.create_volume(config, WorkBudget::UNBOUNDED, &cancellation))?
        .value;
    let mode = CheckoutMode {
        access: AccessMode::ReadWrite,
        consistency,
        mutations: MutationMode::PrivateOverlay,
    };
    let checkout = || {
        runtime
            .block_on(volume.checkout(
                GenerationSelector::Head,
                mode,
                WorkBudget::UNBOUNDED,
                &cancellation,
            ))
            .map(|receipt| receipt.value)
    };
    let first = Arc::new(SharedCheckout::with_publication(checkout()?, publication));
    let second = if consistency == ConsistencyMode::Pinned {
        Arc::clone(&first)
    } else {
        Arc::new(SharedCheckout::with_publication(checkout()?, publication))
    };
    Ok((
        bind_test_access(CheckoutMountSource::new(first, config)?, vec![], None)?,
        bind_test_access(CheckoutMountSource::new(second, config)?, vec![], None)?,
    ))
}
fn native_test_path(name: &str) -> MountPath {
    name.split('/').fold(MountPath::root(), |path, component| {
        #[cfg(target_os = "windows")]
        let bytes = component
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let bytes = component.as_bytes().to_vec();
        path.child(bytes)
    })
}

fn mount_source(
    source: &Arc<MemorySource>,
    destination: &Path,
) -> Result<crate::native_mount::NativeMountSession, NativeMountError> {
    crate::mount_native(
        crate::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: destination.to_path_buf(),
            writable: true,
        },
        source.clone(),
    )
}

include!("runtime.rs");
mod leases;
mod policy_refresh;
pub(in crate::native_mount) mod return_fence;
include!("namespace.rs");
include!("lifecycle.rs");
include!("platform.rs");
include!("source_limits.rs");

pub(in crate::native_mount) mod access;
use access::bind_test_access;
mod clone_view;
mod concurrent;
#[cfg(unix)]
mod policy;
