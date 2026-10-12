use super::*;

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
    /// The requested resource does not exist.
    #[error("resource not found: {0}")]
    NotFound(String),
    /// An immutable identity or revision precondition failed.
    #[error("conflict: {0}")]
    Conflict(String),
    /// A required capability is unavailable.
    #[error("unsupported capability: {0}")]
    Unsupported(String),
    /// The request is malformed.
    #[error("invalid request: {0}")]
    Invalid(String),
    /// The caller lacks authority.
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    /// A bound approval resolved without permission to execute.
    #[error("interaction {0}")]
    InteractionRejected(InteractionRejection),
    /// Durable provider operation failed.
    #[error("storage failure: {0}")]
    Storage(String),
    /// An actual SDK buffer reservation failure, without allocating a diagnostic.
    #[error(transparent)]
    Allocation(#[from] std::collections::TryReserveError),
    /// Durable admission may have committed and must be reconciled.
    #[error("operation outcome is indeterminate: {0}")]
    Indeterminate(OperationId),
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
        /// The actual constructor work prefix, held inline without a receipt box.
        work: acyclic_fs::WorkCounters,
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
    /// An actual native decode failure, preserving its source and classification.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeDecode {
        /// Invalid for a request; Storage for a persisted record.
        code: crate::wire::ErrorCode,
        /// The original decoder error.
        #[source]
        source: serde_json::Error,
        /// Existing custody for SDK-owned decoder diagnostics.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual lexical or canonical codec failure in its original preallocated slot.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeCodec {
        /// The original codec failure's classification.
        code: crate::wire::ErrorCode,
        /// The whole original cause; this slot was allocated before codec work.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
        /// Existing slot custody, released after physical Box deallocation.
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
    /// An actual native encoder failure, preserving its source and classification.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeEncode {
        /// The original contract classification for the encoded record.
        code: crate::wire::ErrorCode,
        /// The original serializer error, moved without diagnostic conversion.
        #[source]
        source: serde_json::Error,
        /// Original preclaimed SDK diagnostic custody.
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
        source: jsonschema::ValidationError<'static>,
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
    /// A possibly dispatched native MCP HTTP operation with its original cause.
    #[cfg(all(feature = "filesystem", feature = "mcp-http", not(target_arch = "wasm32")))]
    #[error("MCP operation {operation} is indeterminate: {source}")]
    NativeMcpHttp {
        /// The original admitted operation; a transport failure never authorizes resending it.
        operation: OperationId,
        /// Opaque provider-owned HTTP failure, moved without diagnostic allocation.
        #[source]
        source: reqwest::Error,
    },
    /// An incomplete native MCP response after possible dispatch.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("MCP operation {operation} is indeterminate: {source}")]
    NativeMcpIncomplete {
        /// The original admitted operation whose outcome requires reconciliation.
        operation: OperationId,
        /// The original EOF decoder failure, not a fabricated success or invalid request.
        #[source]
        source: serde_json::Error,
        /// Existing SDK diagnostic custody, released after the original source.
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
        source: acyclic_fs::LazyWorkspaceError,
        /// Existing custody for any SDK-owned diagnostic payload.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// The bytes committed, but their frozen publication proof could not be retained.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("source publication {generation:?} is indeterminate: {source}")]
    NativeSourcePublicationUncertain {
        /// The actual generation returned by the completed SDK transaction.
        generation: acyclic_fs::GenerationId,
        /// The complete typed failure in the slot allocated before publication.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
        /// A distinct retention failure after the primary proof failure, if both occurred.
        retention_failure: Option<acyclic_fs::LazyWorkspaceError>,
        /// Original slot custody, released after both original causes are dropped.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual native workspace failure, moved without diagnostic conversion.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeWorkspace {
        /// The original workspace cause.
        #[source]
        source: acyclic_fs::WorkspaceError,
        /// Existing custody, released after the original cause is dropped.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual local core lineage failure with its original diagnostic custody.
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeLineage {
        /// The complete lineage and local core log cause.
        #[source]
        source: acyclic_fs::WorkspaceLineageError<acyclic_fs::LocalCoreStateStoreError>,
        /// Existing custody, released after the original cause.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// An actual local workspace-context registration failure.
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeContext {
        /// The complete context and local core log cause.
        #[source]
        source: acyclic_fs::WorkspaceContextError<acyclic_fs::LocalCoreStateStoreError>,
        /// Existing custody, released after the original cause.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// A driven workspace join failure with its complete original typed cause.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeDrivenJoin {
        /// The complete planning, resolution or publication failure.
        #[source]
        source: acyclic_fs::DrivenJoinError,
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
        source: acyclic_fs::FsError,
        /// Existing additional diagnostic custody, released after the cause.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// A native view failure, including any retained physical cleanup owner.
    #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
    #[error("{cause}")]
    NativeView {
        /// The original lifecycle failure and its actual cleanup ownership.
        #[source]
        cause: acyclic_fs::native_mount::MountLifecycleError,
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
    #[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
    #[error("{source}")]
    NativeWorkspacePublication {
        /// The original publication cause and any physical cleanup ownership.
        #[source]
        source: acyclic_fs::NativeWorkspacePublicationError,
        /// Existing custody, released after the original cause is dropped.
        custody: Option<acyclic_fs::NativeAllocation>,
    },
    /// A physical native action whose journal completion is uncertain.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[error("{failure}")]
    NativeStageUncertain {
        /// The original cause and durable/live work prefixes, with custody last.
        #[source]
        failure: acyclic_fs::NativeStageUncertainty<Error>,
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
pub(crate) fn native_work_error_code(cause: &acyclic_fs::NativeTreeMaterializationError) -> ErrorCode {
    use acyclic_fs::NativeTreeMaterializationError as Native;
    match cause {
        Native::InvalidLayout
        | Native::InvalidPreimage
        | Native::InvalidPath(..)
        | Native::PayloadTooLarge { .. } => ErrorCode::Invalid,
        Native::MissingTarget(..) | Native::MissingPreimage(..) => ErrorCode::NotFound,
        Native::ExternalMutation(..)
        | Native::UnexpectedBackup(..)
        | Native::AllocationChanged { .. }
        | Native::PreimageConflict => ErrorCode::Conflict,
        Native::UnsupportedRename | Native::UnverifiedSourceMetadata(..) => ErrorCode::Unsupported,
        Native::Io(_)
        | Native::Cancelled
        | Native::OriginalWorkPoisoned
        | Native::MissingCustody
        | Native::Work(_)
        | Native::AllocationFailed(_)
        | Native::IndeterminatePreparation => ErrorCode::Storage,
    }
}

impl Error {
    /// Stable wire classification from which every adapter derives its status.
    #[must_use]
    pub fn code(&self) -> crate::wire::ErrorCode {
        use crate::wire::ErrorCode;
        match self {
            Self::NotFound(_) => ErrorCode::NotFound,
            Self::Conflict(_) => ErrorCode::Conflict,
            Self::Unsupported(_) => ErrorCode::Unsupported,
            Self::Invalid(_) => ErrorCode::Invalid,
            Self::Unauthorized(_) => ErrorCode::Unauthorized,
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
            Self::Storage(_) | Self::Allocation(_) => ErrorCode::Storage,
            Self::Indeterminate(_) => ErrorCode::Indeterminate,
            #[cfg(all(feature = "filesystem", feature = "mcp-http", not(target_arch = "wasm32")))]
            Self::NativeMcpHttp { .. } => ErrorCode::Indeterminate,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::NativeMcpIncomplete { .. } => ErrorCode::Indeterminate,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::NativeSourcePublicationUncertain { .. } => ErrorCode::Indeterminate,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::NativeBoundary { code, .. } | Self::NativeDiagnostic { code, .. }
            | Self::NativeDecode { code, .. } | Self::NativeProstDecode { code, .. }
            | Self::NativeCodec { code, .. }
            | Self::NativeEncode { code, .. }
            | Self::NativeStream { code, .. } | Self::NativeSchema { code, .. }
            | Self::NativeReservation { code, .. } => *code,
            #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
            Self::NativeLineage { .. } | Self::NativeContext { .. } => ErrorCode::Storage,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::NativeConstruction { .. }
            | Self::NativeSupervisor { .. }
            | Self::NativeSource { .. }
            | Self::NativeWorkspace { .. }
            | Self::NativeDrivenJoin { .. }
            | Self::NativeFilesystem { .. } => ErrorCode::Storage,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::NativeStageUncertain { .. } => ErrorCode::Storage,
            #[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
            Self::NativeView { .. } | Self::NativePublication { .. }
            | Self::NativeTimeout { .. } => ErrorCode::Storage,
            #[cfg(all(feature = "filesystem-local", feature = "native-execution", not(target_arch = "wasm32")))]
            Self::NativeWorkspacePublication { .. } => ErrorCode::Storage,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::NativeWork { cause, .. } => native_work_error_code(cause),
        }
    }

    /// Stable classification for tracing's `error.kind`; never the message, which can embed paths.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        self.code().as_str_name()
    }

    /// Native failures must propagate before additional provider or deadline work.
    pub(crate) fn is_native_failure(&self) -> bool {
        !matches!(self,
            Self::NotFound(_) | Self::Conflict(_) | Self::Unsupported(_) | Self::Invalid(_)
            | Self::Unauthorized(_) | Self::InteractionRejected(_) | Self::Storage(_)
            | Self::Indeterminate(_))
    }
}

/// Moves a native cause without formatting it or manufacturing allocation custody.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl From<acyclic_fs::NativeTreeMaterializationError> for Error {
    fn from(cause: acyclic_fs::NativeTreeMaterializationError) -> Self {
        Self::NativeWork { cause, custody: None }
    }
}

/// Moves the complete filesystem cause and its intrinsic custody without formatting.
#[cfg(feature = "filesystem")]
impl From<acyclic_fs::FsError> for Error {
    fn from(source: acyclic_fs::FsError) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        { Self::NativeFilesystem { source, custody: None } }
        #[cfg(target_arch = "wasm32")]
        { crate::filesystem::map_error(acyclic_fs::WorkspaceError::Filesystem(source)) }
    }
}

/// Moves the complete workspace cause and its intrinsic custody without formatting.
#[cfg(feature = "filesystem")]
impl From<acyclic_fs::WorkspaceError> for Error {
    fn from(source: acyclic_fs::WorkspaceError) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        { Self::NativeWorkspace { source, custody: None } }
        #[cfg(target_arch = "wasm32")]
        { crate::filesystem::map_error(source) }
    }
}

/// Moves the complete driven-join cause and its intrinsic custody without formatting.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl From<acyclic_fs::DrivenJoinError> for Error {
    fn from(source: acyclic_fs::DrivenJoinError) -> Self {
        Self::NativeDrivenJoin { source, custody: None }
    }
}

/// Moves the complete lazy-source failure and its intrinsic custody without formatting.
#[cfg(feature = "filesystem")]
impl From<acyclic_fs::LazyWorkspaceError> for Error {
    fn from(source: acyclic_fs::LazyWorkspaceError) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        { Self::NativeSource { source, custody: None } }
        #[cfg(target_arch = "wasm32")]
        { Self::Storage(source.to_string()) }
    }
}

/// Moves a lifecycle failure without dropping any retained physical cleanup owner.
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
impl From<acyclic_fs::native_mount::MountLifecycleError> for Error {
    fn from(cause: acyclic_fs::native_mount::MountLifecycleError) -> Self {
        Self::NativeView { cause, custody: None }
    }
}

/// Moves the complete native source cause and its intrinsic custody without formatting.
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
impl From<acyclic_fs::native_mount::MountSourceError> for Error {
    fn from(cause: acyclic_fs::native_mount::MountSourceError) -> Self {
        Self::NativeView {
            cause: acyclic_fs::native_mount::MountLifecycleError::Source(cause),
            custody: None,
        }
    }
}

/// Wraps the actual mount cause in the existing lifecycle error, without allocation.
#[cfg(all(feature = "native-execution", not(target_arch = "wasm32")))]
impl From<acyclic_fs::native_mount::NativeMountError> for Error {
    fn from(cause: acyclic_fs::native_mount::NativeMountError) -> Self {
        Self::NativeView {
            cause: acyclic_fs::native_mount::MountLifecycleError::Native(cause),
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

/// Harness result type.
pub type Result<T> = std::result::Result<T, Error>;

