use super::super::session::NativeSessionControl;
use super::*;

/// The original session stays owned until an accepted physical control finishes,
/// even when its async observer is cancelled.
pub(crate) struct SessionOwner {
    session: Mutex<Option<NativeMountSession>>,
    state: std::sync::atomic::AtomicU8,
    allocation: NativeAllocation,
}

impl SessionOwner {
    const ACTIVE: u8 = 0;
    const RETIRING: u8 = 1;
    const RETIRED: u8 = 2;

    pub(super) fn new(work: &NativeMaterializationWork) -> Result<Arc<Self>, MountLifecycleError> {
        let allocation = work.claim(
            (std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>()) as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        Ok(Arc::new(Self {
            session: Mutex::new(None),
            state: std::sync::atomic::AtomicU8::new(Self::ACTIVE),
            allocation,
        }))
    }

    pub(super) fn is_retiring(&self) -> bool {
        self.state.load(std::sync::atomic::Ordering::Acquire) != Self::ACTIVE
    }

    pub(super) fn is_retired(&self) -> bool {
        self.state.load(std::sync::atomic::Ordering::Acquire) == Self::RETIRED
    }

    pub(super) fn begin_retirement(&self) {
        self.state
            .store(Self::RETIRING, std::sync::atomic::Ordering::Release);
    }

    pub(super) fn finish_retirement(&self) {
        self.state
            .store(Self::RETIRED, std::sync::atomic::Ordering::Release);
    }

    pub(super) async fn run<T: Send + 'static>(
        self: &Arc<Self>,
        operation: impl FnOnce(&Self) -> Result<T, MountLifecycleError> + Send + 'static,
    ) -> Result<T, MountLifecycleError> {
        let owner = Arc::clone(self);
        let work = self.allocation.work().clone();
        acyclic_native_runtime::spawn_blocking(move || operation(&owner))
            .await
            .map_err(|cause| NativeMountError::from(work.runtime_error(cause)))?
    }
}

impl std::ops::Deref for SessionOwner {
    type Target = Mutex<Option<NativeMountSession>>;

    fn deref(&self) -> &Self::Target {
        &self.session
    }
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
    session: &Mutex<Option<NativeMountSession>>,
    source: &dyn MountFilesystem,
) -> Result<(), MountLifecycleError> {
    let work = source
        .native_original_work()?
        .ok_or(crate::NativeTreeMaterializationError::InvalidLayout)?;
    let mut owner = session
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
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
            .field("cleanup", &self.failures.cleanup)
            .finish_non_exhaustive()
    }
}

impl NativeMountDrainProof {
    /// Completes retirement without discarding either retained terminal cause.
    pub fn into_result(self) -> Result<(), MountLifecycleError> {
        if self.failures.is_empty() {
            Ok(())
        } else {
            Err(MountLifecycleError::Retired(self))
        }
    }

    /// Takes the preserved producer failure and any distinct cleanup failure.
    pub fn take_retained_capture_failure(
        &mut self,
    ) -> Option<(NativeMountError, Option<NativeMountError>)> {
        self.failures.take_pair()
    }
}

/// Admit under the session lock, then release it before waiting for the owner.
pub(in crate::native_mount) async fn with_live_session_async(
    session: &Mutex<Option<NativeMountSession>>,
    work: &NativeMaterializationWork,
    control: NativeSessionControl,
) -> Result<(), MountLifecycleError> {
    let completion = {
        let owner = try_session(session)?;
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
        let owner = try_session(session)?;
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

// A cancelled observer may leave physical retirement holding this lock. Async
// controls must disclose that owner instead of blocking its runtime worker.
fn try_session(
    session: &Mutex<Option<NativeMountSession>>,
) -> Result<std::sync::MutexGuard<'_, Option<NativeMountSession>>, MountLifecycleError> {
    match session.try_lock() {
        Ok(owner) => Ok(owner),
        Err(std::sync::TryLockError::Poisoned(error)) => Ok(error.into_inner()),
        Err(std::sync::TryLockError::WouldBlock) => Err(NativeMountError::ControlPending.into()),
    }
}
