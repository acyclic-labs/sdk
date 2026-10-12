/// Actual exit status from a Darwin teardown control.
#[derive(Debug)]
pub enum DarwinTeardownFailure {
    /// The provider exited with this status.
    LoopExited(i32),
    /// The unmount process exited with this status.
    DiskutilExit(std::process::ExitStatus),
}

impl std::fmt::Display for DarwinTeardownFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LoopExited(status) => {
                write!(formatter, "Darwin mount loop exited with status {status}")
            }
            Self::DiskutilExit(status) => {
                write!(formatter, "diskutil unmount failed with status {status}")
            }
        }
    }
}

impl std::error::Error for DarwinTeardownFailure {}
