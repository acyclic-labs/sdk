use super::*;

/// Failure from Darwin teardown controls.
#[derive(Debug)]
pub enum DarwinTeardownFailure {
    /// The provider had already exited while its mount still required detach.
    LoopExitedBeforeDetach(i32),
    /// Actual nonzero provider status after teardown began.
    LoopExitedAfterDetach(i32),
    /// The actual provider thread join reported panic.
    LoopPanicked,
    /// Actual callbacks did not settle within the existing liveness bound.
    LoopExitTimedOut,
    /// Actual diskutil process status.
    DiskutilExit(std::process::ExitStatus),
    /// The same retained diskutil process exceeded its original deadline.
    DiskutilTimedOut,
    /// A real mount-table probe still observed the namespace at the bound.
    MountStillVisible,
    /// The current mount table exceeds its genuinely prepaid birth buffer.
    MountTableCapacityExceeded,
    /// Required teardown state has not been observed.
    MissingTeardownState,
}

impl std::fmt::Display for DarwinTeardownFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LoopExitedBeforeDetach(0) => {
                formatter.write_str("Darwin mount loop exited before teardown began")
            }
            Self::LoopExitedBeforeDetach(status) | Self::LoopExitedAfterDetach(status) => {
                write!(formatter, "Darwin mount loop exited with status {status}")
            }
            Self::LoopPanicked => formatter.write_str("Darwin mount loop thread panicked"),
            Self::LoopExitTimedOut => formatter
                .write_str("Darwin mount callbacks have not finished; destination remains fenced"),
            Self::DiskutilExit(status) => {
                write!(formatter, "diskutil unmount failed with status {status}")
            }
            Self::DiskutilTimedOut => {
                formatter.write_str("diskutil unmount exceeded its 4-second bound")
            }
            Self::MountStillVisible => {
                formatter.write_str("Darwin mount remained visible after diskutil unmount")
            }
            Self::MountTableCapacityExceeded => {
                formatter.write_str("Darwin mount-table probe exceeds its prepaid birth capacity")
            }
            Self::MissingTeardownState => {
                formatter.write_str("Darwin mount teardown evidence is absent")
            }
        }
    }
}

impl std::error::Error for DarwinTeardownFailure {}
