use super::*;

pub(crate) fn with_session_work(
    session: &Mutex<Option<NativeMountSession>>,
    operation: fn(&NativeMountSession, &NativeMaterializationWork) -> Result<(), NativeMountError>,
) -> Result<(), MountLifecycleError> {
    with_live_session(session, |session| {
        operation(
            session,
            &NativeMaterializationWork::new(
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
    /// Original physical preparation failure.
    #[error(transparent)]
    Materialization(#[from] crate::NativeTreeMaterializationError),
    /// Original canonical materialization failure.
    #[error(transparent)]
    Materialize(#[from] crate::MaterializeError),
    /// Fixed native-view policy refusal.
    #[error("{0}")]
    NativePolicy(&'static str),
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

impl From<crate::FsError> for MountLifecycleError {
    fn from(error: crate::FsError) -> Self {
        Self::Source(error.into())
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
