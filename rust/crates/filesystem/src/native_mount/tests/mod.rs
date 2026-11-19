use super::*;
#[cfg(any(not(unit_test_split), unit_test_mount, unit_test_mount_views))]
use crate::Fs;
#[cfg(any(not(unit_test_split), unit_test_mount, unit_test_mount_views))]
use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
#[cfg(any(not(unit_test_split), unit_test_mount, unit_test_mount_views))]
use crate::model::{
    AccessMode, CheckoutMode, ConsistencyMode, FilesystemProfile, GenerationSelector, Lifecycle,
    MutationMode, VolumeConfig, VolumeLimits,
};
#[cfg(any(not(unit_test_split), unit_test_mount))]
use crate::{ByteRange, WatchBatch, WatchChange};
use crate::{CancellationToken, VolumeId, WorkBudget};

#[cfg(any(not(unit_test_split), unit_test_mount))]
pub(in crate::native_mount) use super::fixture_access;
#[cfg(any(not(unit_test_split), unit_test_mount))]
pub(super) use super::recovery::wait_for_retirement as await_recovery_worker;

#[cfg(any(not(unit_test_split), unit_test_mount, unit_test_mount_views))]
pub(super) async fn memory_volume(
    id: VolumeId,
    config: VolumeConfig,
    cancellation: &CancellationToken,
) -> Result<
    crate::Volume<impl crate::AsyncAuthorityStore + use<>, impl crate::AsyncObjectStore + use<>>,
    Box<dyn std::error::Error>,
> {
    Ok(Fs::memory()
        .create_volume_with_id(id, config, WorkBudget::UNBOUNDED, cancellation)
        .await?
        .value)
}

#[cfg(any(not(unit_test_split), unit_test_mount, unit_test_mount_views))]
pub(super) async fn private_checkout<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    volume: &crate::Volume<A, O>,
    cancellation: &CancellationToken,
) -> Result<crate::Checkout<A, O>, Box<dyn std::error::Error>> {
    Ok(volume
        .checkout(
            GenerationSelector::Head,
            CheckoutMode {
                access: AccessMode::ReadWrite,
                consistency: ConsistencyMode::Pinned,
                mutations: MutationMode::PrivateOverlay,
            },
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await?
        .value)
}

#[cfg(any(not(unit_test_split), unit_test_mount, unit_test_mount_views))]
pub(super) fn portable_path(
    limits: VolumeLimits,
    components: &[&[u8]],
) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    Ok(NamespacePath::new(
        components
            .iter()
            .map(|name| LogicalName::new(NameEncoding::Utf8, name.to_vec()))
            .collect::<Result<Vec<_>, _>>()?,
        limits,
    )?)
}

#[cfg(any(not(unit_test_split), unit_test_mount))]
mod capture;
#[cfg(any(not(unit_test_split), unit_test_mount))]
pub(super) mod fence;
#[cfg(all(target_os = "linux", not(loom)))]
#[cfg(any(not(unit_test_split), unit_test_mount))]
mod recovery_bootstrap;
pub(in crate::native_mount) mod retirement;
