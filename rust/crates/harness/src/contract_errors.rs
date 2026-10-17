use super::OperationId;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
use crate::wire::ErrorCode;

/// Terminal non-approval outcomes kept distinct for tool callers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InteractionRejection {
    /// A participant explicitly refused the action.
    #[error("declined")]
    Declined,
    /// The requester or responder cancelled it.
    #[error("cancelled")]
    Cancelled,
    /// Its response deadline elapsed.
    #[error("expired")]
    Expired,
    /// Interaction policy denied it.
    #[error("denied")]
    Denied,
}

/// Errors shared by harness surfaces.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Logical task-file storage or publication failure.
    #[error(transparent)]
    TaskFiles(Box<crate::task_volume::Error>),
    /// The requested resource does not exist.
    #[error("resource not found: {0}")]
    NotFound(String),
    /// An immutable identity or revision precondition failed.
    #[error("conflict: {0}")]
    Conflict(String),
    /// A task reached a terminal failed outcome.
    #[error("task {task} failed: {message}")]
    TaskFailed {
        /// The failed durable task.
        task: crate::TaskId,
        /// The task's retained failure diagnostic.
        message: String,
    },
    /// Automatic admission stopped with a file-driven blocked goal.
    #[error("goal blocked: {0}")]
    GoalBlocked(crate::goal::GoalBlockedReason),
    /// A required capability is unavailable.
    #[error("unsupported capability: {0}")]
    Unsupported(String),
    /// A tool requires a native view whose host device or driver is missing.
    #[error("native mount unavailable: {device}")]
    MountUnavailable {
        /// Missing host mount device or driver.
        device: &'static str,
    },
    /// The request is malformed.
    #[error("invalid request: {0}")]
    Invalid(String),
    /// A definitive tool rejection; safe to journal without allocating a diagnostic.
    #[error("tool rejected: {0}")]
    ToolRejected(&'static str),
    /// The caller lacks authority.
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    /// The invocation's cancellation token fired before the operation finished.
    #[error("operation cancelled")]
    Cancelled,
    /// A bound approval resolved without permission to execute.
    #[error("interaction {0}")]
    InteractionRejected(InteractionRejection),
    /// Durable provider operation failed.
    #[error("storage failure: {0}")]
    Storage(String),
    /// Native process creation, capture, or hook configuration IO failed.
    #[error("process {kind}: {message}")]
    Process {
        /// Platform-neutral operation failure classification.
        kind: &'static str,
        /// Diagnostic returned by the process or IO runtime.
        message: String,
    },
    /// An actual SDK buffer reservation failure, without allocating a diagnostic.
    #[error(transparent)]
    Allocation(#[from] std::collections::TryReserveError),
    /// Durable admission may have committed and must be reconciled.
    #[error("operation outcome is indeterminate: {0}")]
    Indeterminate(OperationId),
    /// Search publication is retryable; its completed response remains in ready.json.
    #[error("search publication deferred; resume from ready.json")]
    SearchPublicationDeferred,
    /// An owned native failure, preserving its cause and existing allocation custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{cause}")]
    NativeWork {
        /// The actual native failure, without string conversion.
        #[source]
        cause: acyclic_fs::NativeTreeMaterializationError,
        /// Existing custody for any SDK-owned diagnostic payload.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// Failure before the original native accounting state could be allocated.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("native accounting construction failed: {cause}")]
    NativeConstruction {
        /// The actual rejection, without constructing an owned diagnostic.
        #[source]
        cause: acyclic_fs::NativeTreeMaterializationError,
        /// The actual constructor work prefix.
        work: Box<acyclic_fs::WorkCounters>,
    },
    /// A fixed native boundary rejection that needs no diagnostic allocation.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{reason}")]
    NativeBoundary {
        /// The existing wire classification of the rejection.
        code: crate::wire::ErrorCode,
        /// A fixed SDK diagnostic, not a formatted payload.
        reason: &'static str,
    },
    /// An already-owned diagnostic with its original classification and custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{message}")]
    NativeDiagnostic {
        /// The classification of the original diagnostic.
        code: crate::wire::ErrorCode,
        /// The original message, moved without formatting or copying.
        message: String,
        /// Existing custody, released after the diagnostic is dropped.
        custody: acyclic_fs::NativeAllocation,
    },
    /// A fixed JSON input failure without an allocated diagnostic.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{kind}")]
    NativeJsonAdmission {
        /// The original boundary classification.
        code: crate::wire::ErrorCode,
        /// The parser's typed source rejection.
        kind: crate::executor::json::JsonInputError,
    },
    /// A JSON failure preserving its original cause and classification.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeJson {
        /// Invalid for a request; Storage for a persisted record.
        code: crate::wire::ErrorCode,
        /// The original JSON error.
        #[source]
        source: serde_json::Error,
        /// Existing custody for SDK-owned JSON diagnostics.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual protobuf decode failure on the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeProstDecode {
        /// Invalid for request input; Storage for persisted records.
        code: ErrorCode,
        /// The original protobuf cause, without diagnostic conversion.
        #[source]
        source: prost::DecodeError,
        /// Original preclaimed diagnostic custody, released after the cause.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// Original JSON Schema failure with custody for its owned input instance.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeSchema {
        /// Invalid at a request boundary; contextual uncertainty after dispatch.
        code: ErrorCode,
        /// The original validator cause, including its instance and locations.
        #[source]
        source: Box<jsonschema::ValidationError<'static>>,
        /// Allowance admitted before validation, released after the cause.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual Stream failure in a native operation.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeStream {
        /// Preserved contract classification for the original provider failure.
        code: ErrorCode,
        /// The original provider error, moved without string conversion.
        #[source]
        source: acyclic_stream::StreamError,
        /// Existing SDK payload custody, not opaque provider allocator metadata.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual native supervisor failure.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeSupervisor {
        /// The original join error, including its actual panic or cancellation.
        #[source]
        source: tokio::task::JoinError,
        /// Existing SDK payload custody, not opaque Tokio allocator metadata.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual selected lazy-source failure.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeSource {
        /// The original source error.
        #[source]
        source: Box<acyclic_fs::LazyWorkspaceError>,
        /// Existing custody for any SDK-owned diagnostic payload.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual native workspace failure, moved without diagnostic conversion.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeWorkspace {
        /// The original workspace cause.
        #[source]
        source: Box<acyclic_fs::WorkspaceError>,
        /// Existing custody, released after the original cause is dropped.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// A physical native action whose journal completion is uncertain.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{failure}")]
    NativeStageUncertain {
        /// The original cause and durable/live work prefixes.
        #[source]
        failure: acyclic_fs::NativeStageUncertainty<Error>,
    },
    /// A driven workspace join failure with its complete original typed cause.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeDrivenJoin {
        /// The complete planning, resolution or publication failure.
        #[source]
        source: Box<acyclic_fs::DrivenJoinError>,
        /// Existing custody, released after the original cause.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// A native reservation failure retaining its original physical cleanup owner.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeReservation {
        /// The original primary failure's wire classification.
        code: ErrorCode,
        /// The complete failure in its preallocated concrete source slot.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
        /// Original slot custody, released after physical Box deallocation.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An original SDK filesystem failure without a diagnostic projection.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeFilesystem {
        /// The complete filesystem cause, including any existing payload custody.
        #[source]
        source: Box<acyclic_fs::FsError>,
        /// Existing additional diagnostic custody, released after the cause.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// A native view failure, including any retained physical cleanup owner.
    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    #[error("{cause}")]
    NativeView {
        /// The original lifecycle failure and its actual cleanup ownership.
        #[source]
        cause: Box<acyclic_fs::native_mount::MountLifecycleError>,
        /// Existing custody for SDK-owned diagnostic payloads.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// A publication failure with its actionable destination and completed work.
    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    #[error("{failure}")]
    NativePublication {
        /// The original failure, written into the preallocated slot.
        #[source]
        failure: Box<crate::filesystem::NativeViewPublicationError>,
        /// Custody for the slot, released after its Box is deallocated.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual SDK workspace publication failure, including its original prefix.
    #[cfg(all(
        feature = "filesystem-local",
        feature = "native-execution",
        not(target_arch = "wasm32")
    ))]
    #[error("{source}")]
    NativeWorkspacePublication {
        /// The original publication cause and any physical cleanup ownership.
        #[source]
        source: Box<acyclic_fs::NativeWorkspacePublicationError>,
        /// Existing custody, released after the original cause is dropped.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An SDK git operation failure retaining the complete typed cause.
    #[cfg(all(
        feature = "filesystem-local",
        feature = "native-execution",
        not(target_arch = "wasm32")
    ))]
    #[error("{source}")]
    NativeGit {
        /// Original capture, coordination, patch, lineage or durable-state cause.
        #[source]
        source: Box<crate::tools::git::Failure>,
    },
    /// An actual native process timeout, without a new boxed IO error.
    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeTimeout {
        /// The original elapsed timeout.
        #[source]
        source: tokio::time::error::Elapsed,
        /// Existing payload custody, not opaque timer allocator metadata.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn native_work_error_code(
    cause: &acyclic_fs::NativeTreeMaterializationError,
) -> ErrorCode {
    use acyclic_fs::NativeTreeMaterializationError as Native;
    match cause {
        Native::InvalidLayout | Native::PayloadTooLarge { .. } => ErrorCode::Invalid,
        Native::MissingEntry => ErrorCode::NotFound,
        Native::ExternalMutation
        | Native::UnexpectedBackup
        | Native::AllocationChanged { .. }
        | Native::PreimageConflict => ErrorCode::Conflict,
        Native::UnsupportedRename => ErrorCode::Unsupported,
        Native::Cancelled => ErrorCode::Cancelled,
        Native::Io(_)
        | Native::OriginalWorkPoisoned
        | Native::MissingCustody
        | Native::Work(_)
        | Native::AllocationFailed(_) => ErrorCode::Storage,
    }
}

impl Error {
    #[cfg(all(
        not(target_arch = "wasm32"),
        any(feature = "hooks", feature = "mcp-stdio", feature = "mcp-http")
    ))]
    pub(crate) fn process_io(error: std::io::Error) -> Self {
        let kind = match error.kind() {
            std::io::ErrorKind::TimedOut => "timeout",
            std::io::ErrorKind::NotFound => "not_found",
            std::io::ErrorKind::PermissionDenied => "permission_denied",
            _ => "io",
        };
        Self::Process {
            kind,
            message: error.to_string(),
        }
    }

    /// Stable wire classification from which every adapter derives its status.
    #[must_use]
    pub fn code(&self) -> crate::wire::ErrorCode {
        use crate::wire::ErrorCode;
        match self.cause() {
            Self::TaskFiles(error) => match error.as_ref() {
                #[cfg(feature = "filesystem")]
                crate::task_volume::Error::FileRead(error) => error.code(),
                crate::task_volume::Error::FenceConflict
                | crate::task_volume::Error::Unauthorized => ErrorCode::Unauthorized,
                crate::task_volume::Error::Conflict(_)
                | crate::task_volume::Error::ImmutableConflict(_)
                | crate::task_volume::Error::SequenceConflict(_)
                | crate::task_volume::Error::ResetRequired => ErrorCode::Conflict,
                crate::task_volume::Error::InvalidPath
                | crate::task_volume::Error::InvalidSegment
                | crate::task_volume::Error::LimitExceeded => ErrorCode::Invalid,
                crate::task_volume::Error::Storage(_) => ErrorCode::Storage,
            },
            Self::NotFound(_) => ErrorCode::NotFound,
            Self::Conflict(_) => ErrorCode::Conflict,
            Self::Unsupported(_) | Self::GoalBlocked(_) | Self::MountUnavailable { .. } => {
                ErrorCode::Unsupported
            }
            Self::Invalid(_) | Self::ToolRejected(_) => ErrorCode::Invalid,
            Self::Unauthorized(_) => ErrorCode::Unauthorized,
            Self::Cancelled => ErrorCode::Cancelled,
            Self::InteractionRejected(InteractionRejection::Declined) => {
                ErrorCode::InteractionDeclined
            }
            Self::InteractionRejected(InteractionRejection::Cancelled) => {
                ErrorCode::InteractionCancelled
            }
            Self::InteractionRejected(InteractionRejection::Expired) => {
                ErrorCode::InteractionExpired
            }
            Self::InteractionRejected(InteractionRejection::Denied) => ErrorCode::InteractionDenied,
            Self::Storage(_) | Self::Allocation(_) | Self::TaskFailed { .. } => ErrorCode::Storage,
            Self::Process { .. } => ErrorCode::Invalid,
            Self::Indeterminate(_) | Self::SearchPublicationDeferred => ErrorCode::Indeterminate,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::NativeBoundary { code, .. }
            | Self::NativeJsonAdmission { code, .. }
            | Self::NativeDiagnostic { code, .. }
            | Self::NativeJson { code, .. }
            | Self::NativeProstDecode { code, .. }
            | Self::NativeStream { code, .. }
            | Self::NativeSchema { code, .. }
            | Self::NativeReservation { code, .. } => *code,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::NativeConstruction { .. }
            | Self::NativeSupervisor { .. }
            | Self::NativeSource { .. }
            | Self::NativeWorkspace { .. }
            | Self::NativeDrivenJoin { .. }
            | Self::NativeStageUncertain { .. }
            | Self::NativeFilesystem { .. } => ErrorCode::Storage,
            #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
            Self::NativeView { .. }
            | Self::NativePublication { .. }
            | Self::NativeTimeout { .. } => ErrorCode::Storage,
            #[cfg(all(
                feature = "filesystem-local",
                feature = "native-execution",
                not(target_arch = "wasm32")
            ))]
            Self::NativeWorkspacePublication { .. } | Self::NativeGit { .. } => ErrorCode::Storage,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::NativeWork { cause, .. } => native_work_error_code(cause),
        }
    }

    /// Stable classification for tracing's `error.kind`; never the message, which can embed paths.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        self.code().as_str_name()
    }

    /// The error a task-file read wraps, peeled recursively. The wire code and the uncertainty
    /// classes all read through this one function, so a wrapped cause keeps its class.
    fn cause(&self) -> &Self {
        match self {
            #[cfg(feature = "filesystem")]
            Self::TaskFiles(error) => match error.as_ref() {
                crate::task_volume::Error::FileRead(inner) => inner.cause(),
                _ => self,
            },
            _ => self,
        }
    }

    /// An accepted effect that ended this way may have happened: reconcile it, never reject it.
    pub(crate) fn leaves_effect_uncertain(&self) -> bool {
        matches!(
            self.cause(),
            Self::Indeterminate(_) | Self::Storage(_) | Self::Cancelled
        )
    }

    /// Whether a failed command's outcome is unknown. Native-ledger errors carry their class
    /// only as a code, so a native storage, indeterminate or cancelled failure is uncertain too.
    pub(crate) fn leaves_outcome_uncertain(&self) -> bool {
        use crate::wire::ErrorCode;
        self.leaves_effect_uncertain()
            || (self.cause().is_native_failure()
                && matches!(
                    self.code(),
                    ErrorCode::Storage | ErrorCode::Indeterminate | ErrorCode::Cancelled
                ))
    }

    /// Native failures must propagate before additional provider or deadline work.
    pub(crate) fn is_native_failure(&self) -> bool {
        !matches!(
            self,
            Self::TaskFiles(_)
                | Self::NotFound(_)
                | Self::Conflict(_)
                | Self::Unsupported(_)
                | Self::Invalid(_)
                | Self::ToolRejected(_)
                | Self::Unauthorized(_)
                | Self::Cancelled
                | Self::InteractionRejected(_)
                | Self::Storage(_)
                | Self::Indeterminate(_)
                | Self::SearchPublicationDeferred
                | Self::TaskFailed { .. }
                | Self::GoalBlocked(_)
        )
    }
}

/// The only conversion of a fired cancellation token into a harness error.
impl From<acyclic_fs::CancellationError> for Error {
    fn from(_: acyclic_fs::CancellationError) -> Self {
        Self::Cancelled
    }
}

impl From<crate::task_volume::Error> for Error {
    fn from(error: crate::task_volume::Error) -> Self {
        Self::TaskFiles(Box::new(error))
    }
}

/// Moves a native cause without formatting it or manufacturing allocation custody.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl From<acyclic_fs::NativeTreeMaterializationError> for Error {
    fn from(cause: acyclic_fs::NativeTreeMaterializationError) -> Self {
        Self::NativeWork {
            cause,
            custody: None,
        }
    }
}

#[cfg(all(
    feature = "filesystem-local",
    feature = "native-execution",
    not(target_arch = "wasm32")
))]
impl From<acyclic_fs::NativeWorkspacePublicationError> for Error {
    fn from(source: acyclic_fs::NativeWorkspacePublicationError) -> Self {
        match source {
            acyclic_fs::NativeWorkspacePublicationError::Native(cause) => cause.into(),
            source => Self::NativeWorkspacePublication {
                source: Box::new(source),
                custody: None,
            },
        }
    }
}

/// Moves the complete filesystem cause and its intrinsic custody without formatting.
#[cfg(feature = "filesystem")]
impl From<acyclic_fs::FsError> for Error {
    fn from(source: acyclic_fs::FsError) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        {
            Self::NativeFilesystem {
                source: Box::new(source),
                custody: None,
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            crate::filesystem::map_error(acyclic_fs::WorkspaceError::Filesystem(source))
        }
    }
}

/// Moves the complete workspace cause and its intrinsic custody without formatting.
#[cfg(feature = "filesystem")]
impl From<acyclic_fs::WorkspaceError> for Error {
    fn from(source: acyclic_fs::WorkspaceError) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        {
            Self::NativeWorkspace {
                source: Box::new(source),
                custody: None,
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            crate::filesystem::map_error(source)
        }
    }
}

/// Moves the complete driven-join cause and its intrinsic custody without formatting.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl From<acyclic_fs::DrivenJoinError> for Error {
    fn from(source: acyclic_fs::DrivenJoinError) -> Self {
        Self::NativeDrivenJoin {
            source: Box::new(source),
            custody: None,
        }
    }
}

/// Moves the complete lazy-source failure and its intrinsic custody without formatting.
#[cfg(feature = "filesystem")]
impl From<acyclic_fs::LazyWorkspaceError> for Error {
    fn from(source: acyclic_fs::LazyWorkspaceError) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        {
            Self::NativeSource {
                source: Box::new(source),
                custody: None,
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            Self::Storage(source.to_string())
        }
    }
}

/// Moves a lifecycle failure without dropping any retained physical cleanup owner.
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
impl From<acyclic_fs::native_mount::MountLifecycleError> for Error {
    fn from(cause: acyclic_fs::native_mount::MountLifecycleError) -> Self {
        Self::NativeView {
            cause: Box::new(cause),
            custody: None,
        }
    }
}

/// Moves the complete native source cause and its intrinsic custody without formatting.
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
impl From<acyclic_fs::native_mount::MountSourceError> for Error {
    fn from(cause: acyclic_fs::native_mount::MountSourceError) -> Self {
        Self::NativeView {
            cause: Box::new(acyclic_fs::native_mount::MountLifecycleError::Source(cause)),
            custody: None,
        }
    }
}

/// Wraps the actual mount cause in the existing lifecycle error.
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
impl From<acyclic_fs::native_mount::NativeMountError> for Error {
    fn from(cause: acyclic_fs::native_mount::NativeMountError) -> Self {
        Self::NativeView {
            cause: Box::new(acyclic_fs::native_mount::MountLifecycleError::Native(cause)),
            custody: None,
        }
    }
}

/// A Stream failure below the harness is a durable-storage failure.
impl From<acyclic_stream::StreamError> for Error {
    fn from(error: acyclic_stream::StreamError) -> Self {
        Self::Storage(error.to_string())
    }
}

impl From<acyclic_actors::timers::TimerError> for Error {
    fn from(error: acyclic_actors::timers::TimerError) -> Self {
        match error {
            acyclic_actors::timers::TimerError::Stream(error) => error.into(),
            other => Self::Invalid(other.to_string()),
        }
    }
}

/// Harness result type.
pub type Result<T> = std::result::Result<T, Error>;
