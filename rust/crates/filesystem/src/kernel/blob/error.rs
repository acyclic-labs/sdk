use super::*;

/// Streaming blob-build failure retaining exact completed work.
pub type BlobBuildFailure = OperationFailure<BlobBuildError>;

/// An actual source failure in storage funded before the source operation.
pub struct BlobDemandFailure {
    error: Box<Option<crate::DemandError>>,
    #[cfg(not(target_arch = "wasm32"))]
    _allocation: crate::NativeAllocation,
}

impl BlobDemandFailure {
    /// Borrows the actual initialized provider cause.
    #[allow(
        clippy::expect_used,
        reason = "Only initialized, prepaid source failures are exposed by this type."
    )]
    pub fn error(&self) -> &crate::DemandError {
        self.error
            .as_ref()
            .as_ref()
            .expect("published source failure is initialized")
    }
}

impl std::fmt::Debug for BlobDemandFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BlobDemandFailure")
            .field("error", self.error())
            .finish()
    }
}

impl std::fmt::Display for BlobDemandFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.error(), formatter)
    }
}

impl std::error::Error for BlobDemandFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl BlobDemandFailure {
    pub(crate) fn prepare(original: &crate::NativeMaterializationWork) -> Result<Self, FsError> {
        let bytes = usize_to_u64(size_of::<Option<crate::DemandError>>());
        let allocation = original.claim(bytes, u64::from(bytes != 0), WorkCounters::UNCHARGED)?;
        let prepared = Self {
            error: Box::new(None),
            _allocation: allocation,
        };
        original.admit(WorkCounters::UNCHARGED)?;
        Ok(prepared)
    }
    pub(crate) fn failed(mut self, error: crate::DemandError) -> Self {
        *self.error = Some(error);
        self
    }
}

/// Stable streaming blob-build errors.
#[derive(Debug, Error)]
pub enum BlobBuildError {
    /// Cooperative cancellation occurred before the next source or store operation.
    #[error("blob build was cancelled")]
    Cancelled,
    /// Chunk, page, or total-size bounds are invalid.
    #[error("blob build options are invalid")]
    InvalidOptions,
    /// Canonical blob-index page exceeds its explicit byte ceiling.
    #[error("blob index page has {observed} bytes; maximum is {maximum}")]
    PageTooLarge {
        /// Canonical encoded bytes.
        observed: u64,
        /// Configured page-byte ceiling.
        maximum: u64,
    },
    /// Fallible bounded index scratch allocation was unavailable.
    #[error("blob build scratch allocation failed")]
    AllocationFailed,
    /// The allocator's actual refusal, without replacing its cause.
    #[error(transparent)]
    ReserveFailed(std::collections::TryReserveError),
    /// A custom source has not supplied an original-accounted producer.
    #[error("blob source has no original native read primitive")]
    NativeSourceUnavailable,
    /// Exact accounting or provider cause outside the standalone error vocabulary.
    #[error(transparent)]
    Context(Box<FsError>),
    /// Incremental index finalization reached an impossible state.
    #[error("blob build index state is invalid")]
    InvalidIndexState,
    /// Input exceeded the admitted logical size.
    #[error("blob source exceeds its admitted logical size")]
    TooLarge,
    /// Source stream failed.
    #[error("blob source failed: {0}")]
    Source(#[source] std::io::Error),
    /// The original provider's typed cause in its actual preadmitted holder.
    #[error(transparent)]
    Demand(#[from] BlobDemandFailure),
    /// Canonical blob-index encoding failed.
    #[error(transparent)]
    Encode(#[from] CanonicalDecodeError),
    /// Immutable-object backend failed.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Exact work overflowed or exceeded its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
}

pub(super) fn build_failed(error: BlobBuildError, work: WorkCounters) -> BlobBuildFailure {
    OperationFailure::new(error, work)
}

pub(super) fn blob_error(error: FsError) -> BlobReadError {
    match error {
        FsError::BlobRead(error) => error,
        FsError::Work(error)
        | FsError::FileRead(crate::kernel::FileRangeReadError::Work(error)) => error.into(),
        FsError::Cancelled(_) => BlobReadError::Cancelled,
        FsError::Object(error) => BlobReadError::Storage(error),
        FsError::FileRead(crate::kernel::FileRangeReadError::AllocationFailed) => {
            BlobReadError::AllocationFailed
        }
        error => BlobReadError::Context(Box::new(error)),
    }
}

pub(super) fn read_decode_error(error: FsError) -> FsError {
    match error {
        FsError::Decode(error) => BlobReadError::Decode(error).into(),
        error => error,
    }
}

/// Authenticated blob-read failure with exact work retained on every path.
pub type BlobReadFailure = OperationFailure<BlobReadError>;

pub(super) fn failed(error: BlobReadError, work: WorkCounters) -> BlobReadFailure {
    OperationFailure::new(error, work)
}

/// Stable authenticated blob-read errors.
#[derive(Debug, Error)]
pub enum BlobReadError {
    /// Cooperative cancellation occurred before the next storage boundary.
    #[error("blob range reading was cancelled")]
    Cancelled,
    /// The private resumable reader entered an impossible transition.
    #[error("blob range transition state is invalid")]
    TraversalState,
    /// Root has the wrong typed object class.
    #[error("blob root is not a blob-index page")]
    WrongRootKind,
    /// Requested range overflows or exceeds the blob.
    #[error("requested blob range is invalid")]
    InvalidRange,
    /// Decode and traversal limits are invalid or unrepresentable.
    #[error("blob read limits are invalid")]
    InvalidLimits,
    /// Bounded output, decode, or traversal scratch allocation failed.
    #[error("blob read scratch allocation failed")]
    AllocationFailed,
    /// Graph repeats a page or exceeds its height bound.
    #[error("blob index contains a cycle or exceeds its height bound")]
    CycleOrHeight,
    /// Parent routing bounds do not match the authenticated child.
    #[error("blob child bounds do not match its page")]
    ChildBoundsMismatch,
    /// Intersecting chunks did not cover the exact request.
    #[error("blob chunks do not cover the requested range exactly")]
    IncompleteCoverage,
    /// Authenticated chunk bytes do not equal the indexed logical span.
    #[error("blob chunk length does not match its index span")]
    ChunkLengthMismatch,
    /// Immutable-object backend rejected a read.
    #[error(transparent)]
    Storage(#[from] ObjectStoreError),
    /// Canonical page failed decoding.
    #[error(transparent)]
    Decode(#[from] CanonicalDecodeError),
    /// Exact work exceeded or overflowed its budget.
    #[error(transparent)]
    Work(#[from] WorkError),
    /// Shared original-ledger read boundary failed.
    #[error(transparent)]
    Context(Box<FsError>),
}

impl From<AllocationError> for BlobReadError {
    fn from(error: AllocationError) -> Self {
        error.into_work_or(Self::AllocationFailed)
    }
}
