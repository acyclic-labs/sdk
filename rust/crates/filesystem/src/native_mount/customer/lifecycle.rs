use super::super::session::NativeSessionControl;
use super::*;

pub(in crate::native_mount) struct CustomerSession {
    slot: Mutex<Option<NativeMountSession>>,
    _allocation: NativeAllocation,
}

impl CustomerSession {
    pub(super) fn new(
        session: Option<NativeMountSession>,
        work: &NativeMaterializationWork,
    ) -> Result<Arc<Self>, MountLifecycleError> {
        let allocation = work.claim(
            acyclic_native_runtime::arc_allocation_bytes::<Self>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        Ok(Arc::new(Self {
            slot: Mutex::new(session),
            _allocation: allocation,
        }))
    }
}

impl std::ops::Deref for CustomerSession {
    type Target = Mutex<Option<NativeMountSession>>;

    fn deref(&self) -> &Self::Target {
        &self.slot
    }
}

/// Runs every asynchronous session access off the executor, including admission
/// that can contend with retained retirement. Accepted work outlives its caller.
pub(super) async fn control_session<T: Send + 'static>(
    session: Arc<CustomerSession>,
    operation: impl FnOnce(&mut Option<NativeMountSession>) -> Result<T, MountLifecycleError>
    + Send
    + 'static,
) -> Result<T, MountLifecycleError> {
    super::super::lifecycle::run_native_control(move || {
        let mut owner = session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        operation(&mut owner)
    })
    .await?
}

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

/// Makes every later retirement of this session remove its destination,
/// on the source's original meter. A view's destination is its issuance's.
pub(crate) fn prepare_destination_cleanup(
    owner: &mut Option<NativeMountSession>,
    source: &dyn MountFilesystem,
) -> Result<(), MountLifecycleError> {
    let work = source
        .native_original_work()?
        .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?;
    if let Some(session) = owner.as_mut() {
        session.prepare_destination_cleanup(&work)?;
    }
    Ok(())
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
    /// Physical retirement completed with its original terminal failures retained.
    #[error("native mount retired with retained failures: {0:?}")]
    Retired(NativeMountDrainProof),
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
    pub(super) failures: super::super::retirement::TerminalFailures,
    #[cfg(windows)]
    pub(super) _namespace_outcome: Option<super::super::projfs::ProjFsNamespaceOutcome>,
}

impl std::fmt::Debug for NativeMountDrainProof {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeMountDrainProof")
            .field("producer", &self.failures.producer)
            .field("source", &self.failures.source)
            .field("cleanup", &self.failures.cleanup)
            .finish_non_exhaustive()
    }
}

impl NativeMountDrainProof {
    /// Completes retirement without discarding any retained terminal cause.
    pub fn into_result(self) -> Result<(), MountLifecycleError> {
        if self.failures.is_empty() {
            Ok(())
        } else {
            Err(MountLifecycleError::Retired(self))
        }
    }

    /// Takes the next preserved producer, source, or cleanup failure.
    pub fn take_terminal_failure(&mut self) -> Option<NativeMountError> {
        self.failures.take()
    }
}

/// Admit on the source original invocation off the executor before awaiting control.
pub(in crate::native_mount) async fn with_session_work_async<
    F: MountFilesystem + ?Sized + 'static,
>(
    session: &Arc<CustomerSession>,
    source: &Arc<F>,
    control: NativeSessionControl,
) -> Result<(), MountLifecycleError> {
    let source = source.clone();
    let completion = control_session(session.clone(), move |owner| {
        if let Some(session) = owner.as_ref() {
            let work = source
                .native_original_work()?
                .ok_or(MountLifecycleError::NativePolicy(
                    "mounted volume has no original native work",
                ))?;
            Ok(session
                .start_control(&work, control)?
                .map(|completion| (work, completion)))
        } else {
            Ok(None)
        }
    })
    .await?;
    if let Some((work, completion)) = completion {
        completion
            .await
            .map_err(|cause| NativeMountError::from(work.runtime_error(cause)))??;
    }
    Ok(())
}
