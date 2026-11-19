use super::*;
#[cfg(any(
    not(unit_test_split),
    unit_test_mount,
    unit_test_mount_driver,
    unit_test_mount_adapter,
    unit_test_mount_adapter_namespace
))]
use crate::Fs;
#[cfg(any(
    not(unit_test_split),
    unit_test_mount,
    unit_test_mount_driver,
    unit_test_mount_adapter,
    unit_test_mount_adapter_namespace
))]
use crate::model::{
    AccessMode, CheckoutMode, ConsistencyMode, GenerationSelector, Lifecycle, MutationMode,
};
#[cfg(any(not(unit_test_split), unit_test_mount_adapter_namespace))]
use crate::native_mount::ViewOrigin;
#[cfg(all(unix, any(not(unit_test_split), unit_test_mount_driver)))]
use std::time::Duration;

#[cfg(all(
    target_os = "macos",
    any(not(unit_test_split), unit_test_mount_adapter)
))]
#[allow(unsafe_code, reason = "declares the NFS transport's C test hooks")]
unsafe extern "C" {
    fn nfs4_test_exclusive_replay_identity() -> std::ffi::c_int;
    fn nfs4_test_namedattr_exclusive_replay_identity() -> std::ffi::c_int;
    fn nfs4_test_readdir_cookie_verifier() -> std::ffi::c_int;
    fn nfs4_test_sync_acknowledgement() -> std::ffi::c_int;
}

#[cfg(any(
    not(unit_test_split),
    unit_test_mount,
    unit_test_mount_lazy,
    unit_test_mount_driver,
    unit_test_mount_adapter,
    unit_test_mount_adapter_namespace
))]
type MemorySource =
    CheckoutMountSource<crate::facade::MemoryAuthorityBackend, crate::facade::MemoryObjectBackend>;

#[cfg(any(
    not(unit_test_split),
    unit_test_mount,
    unit_test_mount_driver,
    unit_test_mount_adapter,
    unit_test_mount_adapter_namespace
))]
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

#[cfg(any(
    not(unit_test_split),
    unit_test_mount,
    unit_test_mount_driver,
    unit_test_mount_adapter,
    unit_test_mount_adapter_namespace
))]
fn configured_sources(
    config: VolumeConfig,
    consistency: ConsistencyMode,
    publication: MountPublication,
) -> Result<(MemorySource, MemorySource), Box<dyn std::error::Error>> {
    sources_with_fs(&Fs::memory(), config, consistency, publication)
}

#[cfg(any(
    not(unit_test_split),
    unit_test_mount,
    unit_test_mount_driver,
    unit_test_mount_adapter,
    unit_test_mount_adapter_namespace
))]
fn sources_with_fs<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>(
    fs: &Fs<A, O>,
    config: VolumeConfig,
    consistency: ConsistencyMode,
    publication: MountPublication,
) -> Result<(CheckoutMountSource<A, O>, CheckoutMountSource<A, O>), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
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
pub(crate) fn native_test_path(name: &str) -> MountPath {
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

#[cfg(any(
    not(unit_test_split),
    all(unit_test_mount_driver, unix),
    all(unit_test_mount_adapter_namespace, target_os = "linux"),
    all(unit_test_mount_adapter, any(target_os = "linux", windows))
))]
fn mount_source(
    source: &Arc<MemorySource>,
    destination: &Path,
) -> Result<crate::native_mount::NativeMountSession, NativeMountError> {
    crate::mount_native(
        crate::native_mount::NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: source.volume_id()?,
            destination: destination.to_path_buf(),
            writable: true,
            shared: false,
        },
        source.clone(),
    )
}

#[cfg(any(not(unit_test_split), unit_test_mount_adapter))]
include!("runtime.rs");
#[cfg(any(not(unit_test_split), unit_test_mount_adapter))]
mod leases;
#[cfg(any(
    not(unit_test_split),
    unit_test_mount,
    all(unit_test_mount_driver, target_os = "linux")
))]
pub(in crate::native_mount) mod policy_refresh;
#[cfg(any(not(unit_test_split), unit_test_mount))]
pub(in crate::native_mount) mod return_fence;
#[cfg(any(not(unit_test_split), unit_test_mount, unit_test_mount_lazy))]
pub(in crate::native_mount) mod return_fence_control;
include!("namespace.rs");
#[cfg(any(not(unit_test_split), unit_test_mount_adapter))]
include!("lifecycle.rs");
#[cfg(any(not(unit_test_split), unit_test_mount_driver))]
include!("platform.rs");
#[cfg(any(not(unit_test_split), unit_test_mount_driver))]
include!("source_limits.rs");

#[cfg(any(not(unit_test_split), unit_test_mount_lazy, unit_test_mount_driver))]
pub(in crate::native_mount) fn native_source_io_control(
    borrowed: &MemorySource,
    original: &crate::NativeMaterializationWork,
) -> Result<Arc<MemorySource>, Box<dyn std::error::Error>> {
    Ok(CheckoutMountSource::new_at_meter(
        Arc::clone(&borrowed.checkout),
        VolumeConfig::portable(crate::model::Lifecycle::Ephemeral),
        NamespacePath::new(Vec::new(), borrowed.config.limits)?,
        original.claim(0, 0, crate::WorkCounters::UNCHARGED)?,
        original,
        Some(original),
    )?
    .with_access_view(Arc::clone(
        borrowed
            .access_view
            .as_ref()
            .ok_or("missing fixture access")?,
    ))?
    .into_arc_meter(original)?)
}

pub(in crate::native_mount) mod access;
#[cfg(any(
    not(unit_test_split),
    unit_test_mount,
    unit_test_mount_driver,
    unit_test_mount_adapter,
    unit_test_mount_adapter_namespace
))]
use access::bind_test_access;
#[cfg(any(not(unit_test_split), unit_test_mount_adapter))]
mod clone_view;
#[cfg(any(not(unit_test_split), unit_test_mount_adapter))]
mod command;
#[cfg(any(not(unit_test_split), unit_test_mount_adapter))]
mod concurrent;
#[cfg(all(unix, any(not(unit_test_split), unit_test_mount_adapter)))]
mod policy;
