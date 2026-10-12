use super::*;

/// Native mount admission and lifecycle failures.
#[derive(Error)]
pub enum NativeMountError {
    /// This target has no supported native namespace mechanism.
    #[error("native mounts are unsupported on this target")]
    UnsupportedTarget,
    /// Required host driver/device is unavailable.
    #[error("native mount capability is unavailable: {0}")]
    CapabilityUnavailable(&'static str),
    /// Destination must already exist and be empty.
    #[error("mount destination must be an existing empty directory")]
    InvalidDestination,
    /// Another process owns this exact native destination.
    #[error("mount destination is already owned by another process")]
    DestinationBusy,
    /// Requested writable semantics cannot be captured exactly.
    #[error("writable native mount is unavailable: {0}")]
    WritableUnavailable(String),
    /// Volume name representation cannot be projected exactly on this target.
    #[error("native mount cannot represent the {profile} volume profile on {platform}")]
    ProfileUnavailable {
        /// Declared volume profile.
        profile: &'static str,
        /// Current native target.
        platform: &'static str,
    },
    /// Platform driver operation failed.
    #[error("native mount driver failed: {0}")]
    Driver(String),
    /// A native child process exited with its actual unsuccessful OS status.
    #[error("native mount {operation} exited with {status}")]
    DriverExit {
        /// Exact driver operation whose child exited.
        operation: &'static str,
        /// Actual child status, without allocating a replacement diagnostic.
        status: std::process::ExitStatus,
    },
    /// Actual panic payload returned by the driver or thread join.
    #[error("native mount {operation} panicked")]
    DriverPanicked {
        /// Exact driver operation whose panic was observed.
        operation: &'static str,
        /// Original already-owned SDK/thread panic, moved without another Box.
        panic: std::sync::Mutex<Box<dyn std::any::Any + Send>>,
    },
    /// An admitted native constructor or mutation exceeded its exact work bound.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Bounded construction never initializes process-shared services.
    #[error("native mount services are not initialized")]
    ServicesNotInitialized,
    /// Explicit process-shared service startup failed with its original cause.
    #[error("native mount service startup failed: {0}")]
    ServiceUnavailable(#[source] Arc<std::io::Error>),
    /// Native admission, cancellation, or observation failed with its typed cause.
    #[error("native mount demand failed: {0}")]
    Demand(#[source] crate::DemandError),
    /// A native lifecycle leaf failed with its original OS kind and cause.
    #[error("native mount I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Original native materialization cause, moved without a new error box.
    #[error(transparent)]
    Materialization(#[from] crate::NativeTreeMaterializationError),
    /// Original source failure, without allocating a replacement diagnostic.
    #[error("native mount source failed: {0}")]
    Source(#[source] MountSourceError),
    /// Authentic callback authorization failure in its preallocated typed slot.
    #[error("native callback authorization failed")]
    CallbackDenied {
        /// Original typed cause in the same preallocated and unsized box.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
        /// Actual slot allocation custody, retired only after box destruction.
        custody: crate::NativeAllocation,
    },
    /// Static driver diagnostic produced by an actual lifecycle failure.
    #[error("native mount lifecycle failed: {0}")]
    Lifecycle(&'static str),
    /// Observed projected or foreign reparse residue is not disposable.
    #[error("native recovery destination is not disposable (reparse tag {reparse_tag:?})")]
    UnsafeRecoveryDestination {
        /// Actual observed reparse tag, if present.
        reparse_tag: Option<u32>,
    },
    /// An accepted native control is still retained and not terminal.
    #[error("native mount cleanup retains a pending control")]
    ControlPending,
    /// Ordinary recovery failed while retaining its actual original owner.
    #[error("native mount recovery failed: {error}")]
    RecoveryFailed {
        /// Original typed cause.
        #[source]
        error: Box<NativeMountError>,
        /// Same fence and control owner to retain for retry.
        owner: NativeMountRecoveryOwner,
    },
    /// A fallible native owned-buffer allocation failed with its actual cause.
    #[error("native mount allocation failed: {0}")]
    Allocation(#[from] std::collections::TryReserveError),
    /// Startup failed after retaining its actual driver and destination fence.
    #[error("native mount startup failed: {0}")]
    StartupFailed(#[source] Box<NativeMountStartupFailure>),
    /// No admitted startup-storage allocation exists yet.
    #[error("native mount startup admission failed: {cause}")]
    StartupAdmissionFailed {
        /// Exact original pre-storage admission cause.
        #[source]
        cause: NativeMountStartupAdmissionError,
        /// Actual supplied source, retained without allocating another owner.
        source: Arc<dyn MountFilesystem>,
    },
    /// Watch construction completed physically but a later source admission failed.
    #[error("native mount source watch startup failed: {cause}")]
    SourceWatchStartupFailed {
        /// Exact original late constructor failure.
        #[source]
        cause: crate::DemandError,
        /// Actual original watch and its admitted allocation custody.
        owner: NativeMountSourceWatchOwner,
    },
    /// The actual FUSE shutdown previously failed; it cannot be called complete.
    #[cfg(target_os = "linux")]
    #[error("FUSE shutdown previously failed")]
    FuseShutdownFailed,
    /// The actual FUSE invalidator previously failed.
    #[cfg(target_os = "linux")]
    #[error("FUSE invalidator previously failed")]
    FuseInvalidatorFailed,
    /// Exact native Darwin teardown outcome, retained without a new diagnostic.
    #[cfg(target_os = "macos")]
    #[error("Darwin native teardown failed: {0}")]
    DarwinTeardown(#[source] DarwinTeardownFailure),
    /// The actual FUSE SDK rejected these precise capability flags.
    #[cfg(target_os = "linux")]
    #[error("FUSE rejected capabilities {0:?}")]
    FuseUnsupportedCapabilities(fuser::InitFlags),
}

impl std::fmt::Debug for NativeMountError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartupAdmissionFailed { cause, .. } => formatter
                .debug_struct("StartupAdmissionFailed")
                .field("cause", cause)
                .finish_non_exhaustive(),
            _ => std::fmt::Display::fmt(self, formatter),
        }
    }
}

impl From<crate::DemandError> for NativeMountError {
    fn from(error: crate::DemandError) -> Self {
        match error {
            crate::DemandError::Work(error) => Self::Work(error),
            crate::DemandError::Io(error) => Self::Io(error),
            error => Self::Demand(error),
        }
    }
}

impl From<MountSourceError> for NativeMountError {
    fn from(error: MountSourceError) -> Self {
        match error {
            MountSourceError::Work(error) => Self::Work(error),
            MountSourceError::Io(error, None) => Self::Io(error),
            MountSourceError::NativeWork(error) => Self::Materialization(error),
            MountSourceError::Allocation(error) => Self::Allocation(error),
            MountSourceError::ServicesNotInitialized => Self::ServicesNotInitialized,
            MountSourceError::ServiceUnavailable(error) => Self::ServiceUnavailable(error),
            error => Self::Source(error),
        }
    }
}
