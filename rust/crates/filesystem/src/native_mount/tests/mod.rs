use super::*;
use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
use crate::model::{
    AccessMode, CheckoutMode, ConsistencyMode, FilesystemProfile, GenerationSelector, Lifecycle,
    MutationMode, VolumeConfig, VolumeLimits,
};
use crate::{ByteRange, CancellationToken, Fs, VolumeId, WatchBatch, WatchChange, WorkBudget};
use std::time::Duration;

pub(in crate::native_mount) use super::fixture_access;
pub(super) use super::recovery::wait_for_retirement as await_recovery_worker;

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

pub(super) const RECLAIM_CHILD_READY: &str = "acyclic-reclaim-ready";

#[test]
pub(super) fn destination_reclaim_child_waits_for_external_release()
-> Result<(), Box<dyn std::error::Error>> {
    use std::io::{Read as _, Write as _};
    let Some(destination) = std::env::var_os("ACYCLIC_FS_RECLAIM_CHILD_DESTINATION") else {
        return Ok(());
    };
    {
        let mut output = std::io::stdout().lock();
        writeln!(output, "\n{RECLAIM_CHILD_READY}")?;
        output.flush()?;
    }
    let mut release = [0];
    std::io::stdin().read_exact(&mut release)?;
    if release != [1] {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "reclaim child received an invalid release",
        )
        .into());
    }
    reclaim_native_mount_destination_fence(Path::new(&destination))?;
    Ok(())
}

mod capture;
pub(super) mod fence;
#[cfg(all(target_os = "linux", not(loom)))]
mod recovery_bootstrap;
pub(in crate::native_mount) mod retirement;
