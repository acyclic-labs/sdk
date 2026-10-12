use super::*;

pub(in crate::native_mount) fn mount_demand_error(error: DemandError) -> MountSourceError {
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
            crate::watch::NativeWatchError::Cancelled(_) => MountSourceError::Cancelled,
            crate::watch::NativeWatchError::Io(error) => MountSourceError::Io(error, None),
            other => MountSourceError::NativeText { kind: Engine, text: other.into() },
        },
        _ => MountSourceError::NativeText { kind: Engine, text: "source demand failed" },
    }
}

pub(in crate::native_mount) fn mount_engine_error(error: crate::FsError) -> MountSourceError {
    match crate::workspace::workspace_bounded_error(&error) {
        Some(crate::WorkspaceError::Work(error)) => MountSourceError::Work(error),
        Some(crate::WorkspaceError::Cancelled(_)) => MountSourceError::Cancelled,
        _ => MountSourceError::Filesystem(error),
    }
}

pub(in crate::native_mount) fn mount_wait_error(error: crate::cancellation::CancellationWaitError) -> MountSourceError {
    match error {
        crate::cancellation::CancellationWaitError::Work(error) => MountSourceError::Work(error),
        crate::cancellation::CancellationWaitError::Native(error) => MountSourceError::NativeWork(error),
        crate::cancellation::CancellationWaitError::AllocationFailed => MountSourceError::NativeText {
            kind: crate::native_mount::NativeSourceTextKind::Engine,
            text: "cancellation waiter allocation failed",
        },
    }
}

pub(in crate::native_mount) fn mount_accept<T, E>(
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

pub(in crate::native_mount) fn mount_items(work: &mut crate::demand::DemandWork, count: usize) -> Result<(), MountSourceError> {
    work.charge(crate::WorkCounters { items_examined: count as u64, ..crate::WorkCounters::UNCHARGED })
        .map_err(mount_demand_error)
}

pub(in crate::native_mount) fn mount_vec<T>(count: usize, work: &mut crate::demand::DemandWork) -> Result<Vec<T>, MountSourceError> {
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

pub(in crate::native_mount) fn mount_namespace_bytes(path: &crate::kernel::NamespacePath) -> u64 {
    (path.depth() * std::mem::size_of::<crate::kernel::LogicalName>()) as u64
        + if path.is_root() { 0 } else { u64::from(path.encoded_bytes()) - path.depth() as u64 }
}

pub(in crate::native_mount) fn mount_clone_namespace(
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

pub(super) fn mount_content_invalid(
    work: &mut crate::demand::DemandWork,
    message: &'static str,
) -> Result<MountSourceError, MountSourceError> {
    let bytes = message.len() as u64;
    let copied = crate::WorkCounters { bytes_copied: bytes, ..crate::WorkCounters::default() };
    work.work.admit(&copied, &work.budget).map_err(MountSourceError::Work)?;
    work.allocate(bytes).map_err(mount_demand_error)?;
    let message = message.to_owned();
    if let Err(error) = work.record(copied) {
        drop(message);
        work.release(bytes).map_err(mount_demand_error)?;
        return Err(mount_demand_error(error));
    }
    Ok(MountSourceError::Invalid(message))
}

pub(super) fn mount_content_invalid_with_carrier(
    carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>, message: &'static str,
) -> Result<MountSourceError, MountSourceError> {
    if carrier.original_native_work().is_some() {
        Ok(MountSourceError::NativeText { kind: crate::native_mount::NativeSourceTextKind::Invalid, text: message })
    } else {
        carrier.with_work(|work| mount_content_invalid(work, message))
    }
}

pub(in crate::native_mount::lazy) fn native_lazy_error(error: LazyWorkspaceError) -> MountSourceError {
    match error {
        LazyWorkspaceError::Demand(error) => mount_demand_error(error),
        LazyWorkspaceError::NotRegularFile => MountSourceError::NativeText {
            kind: crate::native_mount::NativeSourceTextKind::Invalid, text: "path is not a regular file",
        },
        LazyWorkspaceError::InvalidPageBound => MountSourceError::NativeText {
            kind: crate::native_mount::NativeSourceTextKind::Invalid, text: "directory page bound must be positive",
        },
        LazyWorkspaceError::UnsupportedNode => MountSourceError::NativeText {
            kind: crate::native_mount::NativeSourceTextKind::Unsupported,
            text: "source node cannot be represented by the authored workspace",
        },
        LazyWorkspaceError::TooLarge => MountSourceError::NativeText {
            kind: crate::native_mount::NativeSourceTextKind::Engine, text: "file exceeds the requested read bound",
        },
        error => lazy_error(error),
    }
}

pub(in crate::native_mount::lazy) fn accept_native_content(
    result: Result<crate::kernel::OwnedSourceChunk,
        (LazyWorkspaceError, Option<crate::NativeAllocation>)>,
) -> Result<crate::kernel::OwnedSourceChunk, MountSourceError> {
    match result {
        Ok(packet) => Ok(packet),
        Err((error, request)) => {
            let cause = native_lazy_error(error);
            drop(request);
            Err(cause)
        }
    }
}

pub(in crate::native_mount::lazy) fn source_callback_error(error: MountSourceError) -> DemandError {
    match error {
        MountSourceError::Work(error) => DemandError::Work(error),
        MountSourceError::Cancelled => DemandError::Cancelled,
        error => DemandError::Mount(error),
    }
}

pub(super) fn lazy_context<'c>(carrier: &mut adapter::NativeDemandCarrier<'_>, cancellation: &'c CancellationToken) -> Result<LazyTreapWork<'c>, MountSourceError> {
    match carrier.original_native_work() {
        Some(original) => LazyTreapWork::native_with_work(&original, cancellation).map_err(native_lazy_error),
        None => Ok(LazyTreapWork::new(carrier.with_work(|work| work.remaining()).map_err(mount_demand_error)?, cancellation)),
    }
}

pub(super) fn mount_bounded_error(error: &(dyn std::error::Error + 'static)) -> Option<MountSourceError> {
    match crate::workspace::workspace_bounded_error(error) {
        Some(crate::WorkspaceError::Work(error)) => Some(MountSourceError::Work(error)),
        Some(crate::WorkspaceError::Cancelled(_)) => Some(MountSourceError::Cancelled),
        _ => None,
    }
}

pub(super) fn lazy_result<T>(carrier: &mut adapter::NativeDemandCarrier<'_>, accounting: &LazyTreapWork<'_>, result: Result<T, LazyWorkspaceError>) -> Result<T, MountSourceError> {
    if carrier.original_native_work().is_some() { return result.map_err(native_lazy_error); }
    carrier.with_work(|work| mount_accept(work, accounting.measured(result, |error, spent| error.with_prior_work(spent).into_measured_failure()), lazy_error))
}
