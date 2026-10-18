use super::*;
use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
use crate::model::{
    AccessMode, CheckoutMode, ConsistencyMode, FilesystemProfile, GenerationSelector, Lifecycle,
    MutationMode, VolumeConfig, VolumeLimits,
};
use crate::{ByteRange, CancellationToken, Fs, VolumeId, WatchBatch, WatchChange, WorkBudget};
use std::time::{Duration, Instant};

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

pub(super) fn portable_path(
    limits: VolumeLimits,
    components: &[&[u8]],
) -> Result<NamespacePath, Box<dyn std::error::Error>> {
    Ok(NamespacePath::new(
        components
            .iter()
            .map(|name| {
                LogicalName::new(
                    NameEncoding::Utf8,
                    name.to_vec(),
                    limits.maximum_component_bytes,
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
        limits,
    )?)
}

pub(super) fn await_recovery_worker(
    owner: NativeMountRecoveryOwner,
) -> Result<(), Box<dyn std::error::Error>> {
    let shared = owner.shared.clone();
    drop(owner);
    let deadline = Instant::now() + Duration::from_secs(5);
    while shared
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .is_some()
    {
        if Instant::now() >= deadline {
            return Err("actual administrative state did not retire".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

#[test]
pub(super) fn destination_reclaim_child_waits_for_external_release()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(destination) = std::env::var_os("ACYCLIC_FS_RECLAIM_CHILD_DESTINATION") else {
        return Ok(());
    };
    let ready = std::env::var_os("ACYCLIC_FS_RECLAIM_CHILD_READY")
        .ok_or("reclaim child readiness path is absent")?;
    let release = std::env::var_os("ACYCLIC_FS_RECLAIM_CHILD_RELEASE")
        .ok_or("reclaim child release path is absent")?;
    std::fs::write(ready, b"ready")?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !Path::new(&release).is_file() {
        if Instant::now() >= deadline {
            return Err("reclaim child release timed out".into());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    reclaim_native_mount_destination_fence(Path::new(&destination))?;
    Ok(())
}

mod capture;
mod fence;
#[cfg(all(target_os = "linux", not(acyclic_fs_native_loom)))]
mod recovery_bootstrap;
