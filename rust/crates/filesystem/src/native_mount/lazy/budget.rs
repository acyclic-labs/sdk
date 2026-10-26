use super::*;

impl From<DemandError> for MountSourceError {
    fn from(error: DemandError) -> Self {
        match error {
            DemandError::Work(error) => MountSourceError::Work(error),
            DemandError::Cancelled => MountSourceError::Cancelled,
            DemandError::Mount(error) => *error,
            DemandError::Io(error) => MountSourceError::Io(error, None),
            DemandError::Allocation(error) => MountSourceError::Allocation(error),
            DemandError::StaleSource | DemandError::StaleVersion | DemandError::StaleCursor => {
                MountSourceError::Stale
            }
            DemandError::Absent => MountSourceError::NotFound,
            DemandError::NotDirectory => MountSourceError::Filesystem(crate::FsError::NotDirectory),
            DemandError::NativeMetadata(error) => MountSourceError::Metadata(error),
            #[cfg(feature = "native-watch")]
            DemandError::Observation(error) => match *error {
                crate::watch::NativeWatchError::Work(error) => MountSourceError::Work(error),
                crate::watch::NativeWatchError::Cancelled(_) => MountSourceError::Cancelled,
                crate::watch::NativeWatchError::Io(error) => MountSourceError::Io(error, None),
                _ => MountSourceError::Demand(DemandError::Observation(error)),
            },
            error => MountSourceError::Demand(error),
        }
    }
}

pub(in crate::native_mount) fn mount_engine_error(error: crate::FsError) -> MountSourceError {
    match crate::workspace::workspace_bounded_error(&error) {
        Some(crate::WorkspaceError::Work(error)) => MountSourceError::Work(error),
        Some(crate::WorkspaceError::Cancelled(_)) => MountSourceError::Cancelled,
        _ => MountSourceError::Filesystem(error),
    }
}

pub(in crate::native_mount) fn mount_wait_error(
    error: crate::cancellation::CancellationWaitError,
) -> MountSourceError {
    match error {
        crate::cancellation::CancellationWaitError::Accounting(error) => mount_engine_error(error),
        crate::cancellation::CancellationWaitError::Work(error) => MountSourceError::Work(error),
        crate::cancellation::CancellationWaitError::Native(error) => {
            MountSourceError::NativeWork(error)
        }
    }
}

pub(in crate::native_mount) fn mount_items(
    work: &mut crate::demand::DemandWork,
    count: usize,
) -> Result<(), MountSourceError> {
    work.charge(crate::WorkCounters {
        items_examined: count as u64,
        ..crate::WorkCounters::UNCHARGED
    })
    .map_err(MountSourceError::from)
}
