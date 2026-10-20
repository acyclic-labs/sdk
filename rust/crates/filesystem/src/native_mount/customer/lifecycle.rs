use super::super::session::NativeSessionControl;
use super::*;

/// Runs one session control on the source's original meter, which its
/// callbacks share. A control on any other meter cannot run the invalidator,
/// so it could never see its changes settle.
pub(crate) fn with_session_work(
    session: &Mutex<Option<NativeMountSession>>,
    source: &dyn MountFilesystem,
    operation: impl FnOnce(
        &NativeMountSession,
        &NativeMaterializationWork,
    ) -> Result<(), NativeMountError>,
) -> Result<(), MountLifecycleError> {
    with_live_session(session, |session| {
        let work = source
            .native_original_work()?
            .ok_or(NativeMountError::Lifecycle(
                "mounted volume has no original native work",
            ))?;
        operation(session, &work)
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
    pub fn take_retained_capture_failure(
        &mut self,
    ) -> Option<(NativeMountError, Option<NativeMountError>)> {
        #[cfg(windows)]
        {
            self.namespace_outcome.as_mut()?.take_capture_failures()
        }
        #[cfg(not(windows))]
        {
            None
        }
    }
}

/// Admit under the session lock, then release it before waiting for the owner.
pub(in crate::native_mount) async fn with_live_session_async(
    session: &Mutex<Option<NativeMountSession>>,
    work: &NativeMaterializationWork,
    control: NativeSessionControl,
) -> Result<(), MountLifecycleError> {
    let completion = {
        let owner = session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        owner
            .as_ref()
            .map(|session| session.start_control(work, control))
            .transpose()?
            .flatten()
    };
    if let Some(completion) = completion {
        completion
            .await
            .map_err(|cause| NativeMountError::from(work.runtime_error(cause)))??;
    }
    Ok(())
}

pub(in crate::native_mount) async fn with_session_work_async(
    session: &Mutex<Option<NativeMountSession>>,
    source: &dyn MountFilesystem,
    control: NativeSessionControl,
) -> Result<(), MountLifecycleError> {
    let completion = {
        let owner = session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(session) = owner.as_ref() {
            let work = source
                .native_original_work()?
                .ok_or(MountLifecycleError::NativePolicy(
                    "mounted volume has no original native work",
                ))?;
            session
                .start_control(&work, control)?
                .map(|completion| (work, completion))
        } else {
            None
        }
    };
    if let Some((work, completion)) = completion {
        completion
            .await
            .map_err(|cause| NativeMountError::from(work.runtime_error(cause)))??;
    }
    Ok(())
}
