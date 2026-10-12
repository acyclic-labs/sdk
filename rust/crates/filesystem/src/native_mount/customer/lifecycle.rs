use super::*;

pub(crate) fn flush_session_callbacks(
    session: &Mutex<Option<NativeMountSession>>,
) -> Result<(), MountLifecycleError> {
    with_live_session(session, |session| {
        session.flush_callbacks(
            &crate::NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .map_err(|(error, _)| NativeMountError::from(error))?,
        )
    })
}

/// Waits until the kernel caches nothing a change to the source superseded.
pub(crate) fn revalidate_session(
    session: &Mutex<Option<NativeMountSession>>,
) -> Result<(), MountLifecycleError> {
    with_live_session(session, |session| {
        session.revalidate(
            &crate::NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .map_err(|(error, _)| NativeMountError::from(error))?,
        )
    })
}

pub(crate) fn with_live_session(
    session: &Mutex<Option<NativeMountSession>>,
    operation: impl FnOnce(&NativeMountSession) -> Result<(), NativeMountError>,
) -> Result<(), MountLifecycleError> {
    let owner = match session.lock() {
        Ok(owner) => owner,
        Err(poisoned) => poisoned.into_inner(),
    };
    owner
        .as_ref()
        .map_or(Ok(()), operation)
        .map_err(MountLifecycleError::Native)
}

/// Customer mount admission, publication, and lifecycle failures.
#[derive(Debug, Error)]
pub enum MountLifecycleError {
    /// Workspace path, state, or storage failed admission.
    #[error(transparent)]
    Workspace(#[from] WorkspaceError),
    /// Lazy source or sparse-overlay admission failed.
    #[error(transparent)]
    Lazy(#[from] LazyWorkspaceError),
    /// Native driver admission or teardown failed.
    #[error(transparent)]
    Native(#[from] NativeMountError),
    /// Native callback publication failed.
    #[error(transparent)]
    Source(#[from] MountSourceError),
    /// Combined exactification and materialization exceeded its work budget.
    #[error(transparent)]
    Work(#[from] crate::WorkError),
    /// Original physical preparation failure.
    #[error(transparent)]
    Materialization(#[from] crate::NativeTreeMaterializationError),
    /// Original canonical materialization failure.
    #[error(transparent)]
    Materialize(#[from] crate::MaterializeError),
    /// Original bounded demand failure.
    #[error(transparent)]
    Demand(#[from] crate::DemandError),
    /// Fixed native-view policy refusal.
    #[error("{0}")]
    NativePolicy(&'static str),
    /// Lazy-query cause drops before its original allocation.
    #[error("{source}")]
    NativeLazy {
        /// Original lazy-query failure.
        #[source]
        source: LazyWorkspaceError,
        /// Original retained capacity.
        custody: Option<NativeAllocation>,
    },
    /// Physical retirement retained an authored capture and cleanup failure.
    #[error("native capture or namespace cleanup failed: {source}")]
    NativeCaptureFailure {
        /// Original authored-capture failure.
        #[source]
        source: NativeMountError,
        /// Distinct namespace cleanup failure.
        cleanup_failure: Option<NativeMountError>,
    },
}

impl From<(LazyWorkspaceError, Option<NativeAllocation>)> for MountLifecycleError {
    fn from((source, custody): (LazyWorkspaceError, Option<NativeAllocation>)) -> Self {
        Self::NativeLazy { source, custody }
    }
}

/// SDK source retained after its view-dependent physical work has retired.
pub struct NativeMountDrainProof {
    pub(super) _source: Arc<dyn MountFilesystem>,
    #[cfg(windows)]
    pub(super) namespace_outcome: Option<super::super::projfs::ProjFsNamespaceOutcome>,
}

impl NativeMountDrainProof {
    /// Takes the preserved authored failure and any distinct namespace failure.
    pub fn take_retained_capture_failure(&mut self) -> Option<MountLifecycleError> {
        #[cfg(windows)]
        {
            self.namespace_outcome
                .as_mut()?
                .take_capture_failures()
                .map(
                    |(source, cleanup_failure)| MountLifecycleError::NativeCaptureFailure {
                        source,
                        cleanup_failure,
                    },
                )
        }
        #[cfg(not(windows))]
        {
            None
        }
    }
}
