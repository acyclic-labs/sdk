use super::*;

pub(super) fn mount_demand_error(error: DemandError) -> MountSourceError {
    use crate::native_mount::NativeSourceTextKind::Engine;
    match error {
        DemandError::Work(error) => MountSourceError::Work(error),
        DemandError::Cancelled => MountSourceError::Cancelled,
        DemandError::Mount(error) => error,
        DemandError::Io(error) => MountSourceError::Io(error, None),
        DemandError::Allocation(error) => MountSourceError::Allocation(error),
        DemandError::StaleSource | DemandError::StaleVersion | DemandError::StaleCursor => MountSourceError::Stale,
        DemandError::Absent => MountSourceError::NotFound,
        DemandError::NotDirectory => MountSourceError::Filesystem(crate::FsError::NotDirectory),
        #[cfg(target_os = "linux")]
        DemandError::LinuxMetadata(error) => MountSourceError::Metadata(error),
        #[cfg(target_os = "macos")]
        DemandError::MacMetadata(error) => MountSourceError::Metadata(error),
        #[cfg(windows)]
        DemandError::WindowsMetadata(error) => MountSourceError::Metadata(error),
        #[cfg(feature = "native-watch")]
        DemandError::SourceWatchStartup(owner) => MountSourceError::SourceWatchStartup(owner),
        #[cfg(feature = "native-watch")]
        DemandError::Observation(error) => match error {
            crate::watch::NativeWatchError::Work(error) => MountSourceError::Work(error),
            crate::watch::NativeWatchError::Cancelled => MountSourceError::Cancelled,
            crate::watch::NativeWatchError::Io(error) => MountSourceError::Io(error, None),
            other => MountSourceError::NativeText { kind: Engine, text: other.into() },
        },
        _ => MountSourceError::NativeText { kind: Engine, text: "source demand failed" },
    }
}

pub(super) fn mount_engine_error(error: crate::FsError) -> MountSourceError {
    match crate::workspace::workspace_bounded_error(&error) {
        Some(crate::WorkspaceError::Work(error)) => MountSourceError::Work(error),
        Some(crate::WorkspaceError::Cancelled(_)) => MountSourceError::Cancelled,
        _ => MountSourceError::Filesystem(error),
    }
}

pub(super) fn mount_wait_error(error: crate::cancellation::CancellationWaitError) -> MountSourceError {
    match error {
        crate::cancellation::CancellationWaitError::Work(error) => MountSourceError::Work(error),
        crate::cancellation::CancellationWaitError::Native(error) => MountSourceError::NativeWork(error),
        crate::cancellation::CancellationWaitError::AllocationFailed => MountSourceError::NativeText {
            kind: crate::native_mount::NativeSourceTextKind::Engine,
            text: "cancellation waiter allocation failed",
        },
    }
}

pub(super) fn mount_accept<T, E>(
    work: &mut crate::demand::DemandWork,
    result: crate::MeasuredResult<crate::OperationReceipt<T>, E>,
    map: impl FnOnce(E) -> MountSourceError,
) -> Result<T, MountSourceError> {
    match result {
        Ok(receipt) => {
            work.merge(receipt.work).map_err(mount_demand_error)?;
            Ok(receipt.value)
        }
        Err(failure) => {
            work.merge(*failure.work).map_err(mount_demand_error)?;
            Err(map(failure.error))
        }
    }
}

pub(super) fn mount_items(work: &mut crate::demand::DemandWork, count: usize) -> Result<(), MountSourceError> {
    work.charge(crate::WorkCounters { items_examined: count as u64, ..crate::WorkCounters::UNCHARGED })
        .map_err(mount_demand_error)
}

pub(super) fn mount_vec<T>(count: usize, work: &mut crate::demand::DemandWork) -> Result<Vec<T>, MountSourceError> {
    let bytes = count.checked_mul(std::mem::size_of::<T>())
        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))? as u64;
    work.allocate(bytes).map_err(mount_demand_error)?;
    let mut values = Vec::new();
    if let Err(error) = values.try_reserve_exact(count) {
        work.release(bytes).map_err(mount_demand_error)?;
        return Err(MountSourceError::Allocation(error));
    }
    Ok(values)
}

pub(super) fn mount_namespace_bytes(path: &crate::kernel::NamespacePath) -> u64 {
    (path.depth() * std::mem::size_of::<crate::kernel::LogicalName>()) as u64
        + if path.is_root() { 0 } else { u64::from(path.encoded_bytes()) - path.depth() as u64 }
}

pub(super) fn mount_clone_namespace(
    names: &[crate::kernel::LogicalName], limits: crate::VolumeLimits,
    work: &mut crate::demand::DemandWork,
) -> Result<crate::kernel::NamespacePath, MountSourceError> {
    use super::adapter::CaptureInputScratch;
    let mut scratch = super::adapter::CaptureScratchAllocation::new(work);
    let result = (|| {
        let mut owned = scratch.vec(names.len())?;
        for name in names {
            scratch.items(1)?;
            let copied = crate::WorkCounters { bytes_copied: name.as_bytes().len() as u64, ..crate::WorkCounters::UNCHARGED };
            scratch.admit(copied)?;
            let mut bytes = scratch.vec::<u8>(name.as_bytes().len())?;
            bytes.extend_from_slice(name.as_bytes());
            scratch.record(copied)?;
            owned.push(crate::kernel::LogicalName::new(name.encoding(), bytes, limits.maximum_component_bytes)
                .map_err(|error| MountSourceError::Invalid(error.to_string()))?);
        }
        crate::kernel::NamespacePath::new(owned, limits)
            .map_err(|error| MountSourceError::Invalid(error.to_string()))
    })();
    if result.is_ok() { scratch.retain(); }
    result
}
