//! One physical-root acquisition transaction for ordinary and native callers.

use super::*;
use acyclic_fs::{
    MeasuredResult, NativeAllocation, NativeMaterializationWork, NativeRootIdentity,
    OperationFailure, OperationReceipt, WorkCounters,
};
use acyclic_harness::wire::ErrorCode;
use acyclic_harness::{Error as RootError, Result as RootResult};

fn rejected(error: impl std::fmt::Display, work: WorkCounters) -> OperationFailure<String> {
    OperationFailure::new(error.to_string(), work)
}

fn combine<T, E: std::fmt::Display>(
    prior: &mut WorkCounters,
    result: MeasuredResult<OperationReceipt<T>, E>,
) -> MeasuredResult<T, String> {
    match result {
        Ok(receipt) => {
            *prior = prior
                .checked_add(receipt.work)
                .map_err(|error| rejected(error, *prior))?;
            Ok(receipt.value)
        }
        Err(failure) => {
            let work = prior
                .checked_add(*failure.work)
                .map_err(|error| rejected(error, *prior))?;
            Err(rejected(failure.error, work))
        }
    }
}

fn admit(
    work: &mut WorkCounters,
    delta: WorkCounters,
    budget: WorkBudget,
) -> MeasuredResult<(), String> {
    let next = work
        .checked_add(delta)
        .map_err(|error| rejected(error, *work))?;
    next.verify_within(&budget)
        .map_err(|error| rejected(error, *work))?;
    *work = next;
    Ok(())
}

const MISSING_REFERENCE: (ErrorCode, &str) = (
    ErrorCode::Invalid,
    "shared root registration lost its original source reference",
);
const POISONED_REFERENCE: (ErrorCode, &str) = (
    ErrorCode::Storage,
    "shared root source reference state is poisoned",
);
const POISONED_WATCHER: (ErrorCode, &str) =
    (ErrorCode::Storage, "shared root watcher state is poisoned");
const SOURCE_IDENTITY: (ErrorCode, &str) = (
    ErrorCode::Conflict,
    "physical root is already registered with another original source identity",
);
const ROOT_IDENTITY: (ErrorCode, &str) = (
    ErrorCode::Conflict,
    "physical root differs from its authenticated original allocation",
);
const PENDING_CLEANUP: (ErrorCode, &str) = (
    ErrorCode::Conflict,
    "original physical root startup cleanup has not completed",
);

trait RootInstrumentation: Send {
    type Error: Send;

    fn check(&mut self) -> Result<(), Self::Error>;
    fn authenticate(
        &mut self,
        root: &Path,
        admission: SharedRootAdmission,
    ) -> impl Future<Output = Result<Option<NativeRootIdentity>, Self::Error>> + Send;
    fn canonical(
        &mut self,
        root: &Path,
    ) -> Result<(PathBuf, Option<NativeAllocation>), Self::Error>;
    fn copy_path(
        &mut self,
        root: &Path,
    ) -> Result<(PathBuf, Option<NativeAllocation>), Self::Error>;
    fn claim(&mut self, bytes: usize) -> Result<Option<NativeAllocation>, Self::Error>;
    fn identity(
        &mut self,
        root: &Path,
    ) -> impl Future<Output = Result<NativeRootIdentity, Self::Error>> + Send;
    fn validate_live(
        &mut self,
        shared: &SharedPhysicalRoot,
        root: &Path,
    ) -> impl Future<Output = Result<Option<(SharedRootObservation, NativeAllocation)>, Self::Error>>
    + Send;
    fn settle_pending(
        &mut self,
        registration: &mut SharedRootRegistration,
        root: &Path,
    ) -> Result<(), Self::Error>;
    fn open_watcher(
        &mut self,
        root: &Path,
        pending: &mut Option<NativeWatch>,
    ) -> Result<(), Self::Error>;
    fn share_watcher(
        &mut self,
        pending: &mut Option<NativeWatch>,
    ) -> Result<Arc<Mutex<NativeWatch>>, Self::Error>;
    fn prepare_watcher(&mut self, watcher: &mut NativeWatch) -> Result<(), Self::Error>;
    fn open_source(
        &mut self,
        root: &Path,
        reference: SourceReference,
        watcher: Arc<Mutex<NativeWatch>>,
    ) -> impl Future<Output = Result<Arc<LocalLazySource>, Self::Error>> + Send;
    fn invalidate(&mut self, source: &LocalLazySource) -> Result<(), Self::Error>;
    fn rejection(&self, rejection: (ErrorCode, &'static str), root: &Path) -> Self::Error;
}

async fn authenticate<I: RootInstrumentation>(
    instrumentation: &mut I,
    root: &Path,
    admission: SharedRootAdmission,
    original: Option<NativeRootIdentity>,
) -> Result<(), I::Error> {
    if instrumentation.authenticate(root, admission).await? != original {
        return Err(instrumentation.rejection(ROOT_IDENTITY, root));
    }
    instrumentation.check()
}

/// The single registration/watch/demand transaction.
async fn acquire_engine<I: RootInstrumentation>(
    registry: &SharedRootRegistry,
    root: &Path,
    admission: SharedRootAdmission,
    instrumentation: &mut I,
) -> Result<Arc<SharedPhysicalRoot>, I::Error> {
    instrumentation.check()?;
    let original = instrumentation.authenticate(root, admission).await?;
    let canonical = instrumentation.canonical(root)?;
    let registration = {
        let mut roots = registry.roots.lock().await;
        instrumentation.check()?;
        if let Some(registration) = roots.get(&canonical.0) {
            Arc::clone(registration)
        } else {
            let key = instrumentation.copy_path(&canonical.0)?;
            let slot_custody = instrumentation.claim(
                std::mem::size_of::<SharedRootSlot>(),
                u64::from((std::mem::size_of::<SharedRootSlot>()) != 0),
                acyclic_fs::WorkCounters::UNCHARGED,
            )?;
            let slot = Arc::new(SharedRootSlot {
                state: AsyncMutex::new(SharedRootRegistration::default()),
                _native_custody: [key.1, slot_custody],
            });
            roots.insert(key.0, Arc::clone(&slot));
            slot
        }
    };
    let mut registration = registration.lock().await;
    authenticate(instrumentation, root, admission, original).await?;
    // Reject another registered source before opening even the current path.
    if let Some(reference) = &registration.reference
        && original.is_some()
    {
        let retained = *reference
            .lock()
            .map_err(|_| instrumentation.rejection(POISONED_REFERENCE, &canonical.0))?;
        if admission
            .reference()
            .is_some_and(|expected| expected.identity != retained.identity)
        {
            return Err(instrumentation.rejection(SOURCE_IDENTITY, &canonical.0));
        }
    }
    if original.is_some_and(|expected| {
        registration
            .native_identity
            .is_some_and(|identity| identity != expected.to_bytes())
    }) {
        return Err(instrumentation.rejection(ROOT_IDENTITY, &canonical.0));
    }
    instrumentation.settle_pending(&mut registration, &canonical.0)?;
    if let Some(native_identity) = registration.native_identity {
        let observed = instrumentation.identity(&canonical.0).await?;
        authenticate(instrumentation, root, admission, original).await?;
        let live = registration.live.upgrade();
        if observed.to_bytes() == native_identity {
            let retained = *registration
                .reference
                .as_ref()
                .ok_or_else(|| instrumentation.rejection(MISSING_REFERENCE, &canonical.0))?
                .lock()
                .map_err(|_| instrumentation.rejection(POISONED_REFERENCE, &canonical.0))?;
            if admission
                .reference()
                .is_some_and(|expected| expected.identity != retained.identity)
            {
                return Err(instrumentation.rejection(SOURCE_IDENTITY, &canonical.0));
            }
            if let Some(shared) = live {
                let observed = instrumentation.validate_live(&shared, &canonical.0).await?;
                authenticate(instrumentation, root, admission, original).await?;
                if original.is_some() && shared.source.reference().identity != retained.identity {
                    return Err(instrumentation.rejection(SOURCE_IDENTITY, &canonical.0));
                }
                drop(observed);
                return Ok(shared);
            }
        } else {
            if live.is_some() || matches!(admission, SharedRootAdmission::Resume(_)) {
                return Err(instrumentation.rejection(ROOT_IDENTITY, &canonical.0));
            }
            *registration = SharedRootRegistration::default();
        }
    }
    let retained = registration.reference.as_ref().map(|reference| {
        *reference
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    });
    let reference = retained
        .or(admission.reference())
        .unwrap_or_else(|| SourceReference {
            identity: uuid::Uuid::new_v4().into_bytes(),
            epoch: 0,
        });
    if original.is_some()
        && registration
            .reference
            .as_ref()
            .is_none_or(|reference| reference._native_custody.is_none())
    {
        let custody = instrumentation.claim(
            std::mem::size_of::<SharedSourceReference>(),
            u64::from((std::mem::size_of::<SharedSourceReference>()) != 0),
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        registration.reference = Some(Arc::new(SharedSourceReference {
            value: Mutex::new(reference),
            _native_custody: custody,
        }));
    }
    if let Some(expected) = original {
        registration.native_identity = Some(expected.to_bytes());
    }
    let mut ordinary_watcher = None;
    let pending = if original.is_some() {
        &mut registration.pending_raw_watcher
    } else {
        &mut ordinary_watcher
    };
    instrumentation.open_watcher(&canonical.0, pending)?;
    let watcher = instrumentation.share_watcher(pending)?;
    if original.is_some() {
        registration.pending_watcher = Some(Arc::clone(&watcher));
    }
    let watched_identity = {
        let mut watch = watcher
            .lock()
            .map_err(|_| instrumentation.rejection(POISONED_WATCHER, &canonical.0))?;
        let identity = watch.root_identity();
        if original.is_some_and(|expected| expected != identity) {
            return Err(instrumentation.rejection(ROOT_IDENTITY, &canonical.0));
        }
        instrumentation.prepare_watcher(&mut watch)?;
        identity
    };
    let source = instrumentation
        .open_source(&canonical.0, reference, Arc::clone(&watcher))
        .await?;
    if original.is_some() {
        registration.pending_source = Some(Arc::clone(&source));
    }
    authenticate(instrumentation, root, admission, original).await?;
    if source.inner().root_identity() != watched_identity
        || source.reference().identity != reference.identity
    {
        return Err(instrumentation.rejection(ROOT_IDENTITY, &canonical.0));
    }
    if matches!(admission, SharedRootAdmission::Resume(_)) {
        instrumentation.invalidate(&source)?;
    }
    let reference = if original.is_some() {
        Arc::clone(
            registration
                .reference
                .as_ref()
                .ok_or_else(|| instrumentation.rejection(MISSING_REFERENCE, &canonical.0))?,
        )
    } else {
        Arc::new(SharedSourceReference {
            value: Mutex::new(source.reference()),
            _native_custody: None,
        })
    };
    *reference
        .lock()
        .map_err(|_| instrumentation.rejection(POISONED_REFERENCE, &canonical.0))? =
        source.reference();
    let native_identity = source.inner().root_identity().to_bytes();
    let shared_custody = instrumentation.claim(
        std::mem::size_of::<SharedPhysicalRoot>(),
        u64::from((std::mem::size_of::<SharedPhysicalRoot>()) != 0),
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    let shared = Arc::new(SharedPhysicalRoot {
        source,
        watcher,
        reference: Arc::clone(&reference),
        _native_custody: shared_custody,
    });
    *registration = SharedRootRegistration {
        live: Arc::downgrade(&shared),
        reference: Some(reference),
        native_identity: Some(native_identity),
        pending_raw_watcher: None,
        pending_watcher: None,
        pending_source: None,
    };
    Ok(shared)
}

struct OrdinaryRootWork<'a> {
    work: WorkCounters,
    budget: WorkBudget,
    cancellation: &'a CancellationToken,
}

impl RootInstrumentation for OrdinaryRootWork<'_> {
    type Error = OperationFailure<String>;

    fn check(&mut self) -> Result<(), Self::Error> {
        self.cancellation
            .check()
            .map_err(|error| rejected(error, self.work))
    }
    async fn authenticate(
        &mut self,
        _: &Path,
        _: SharedRootAdmission,
    ) -> Result<Option<NativeRootIdentity>, Self::Error> {
        Ok(None)
    }
    fn canonical(
        &mut self,
        root: &Path,
    ) -> Result<(PathBuf, Option<NativeAllocation>), Self::Error> {
        admit(
            &mut self.work,
            WorkCounters {
                backend_read_operations: 1,
                source_path_components: root.components().count() as u64,
                ..WorkCounters::default()
            },
            self.budget,
        )?;
        Ok((
            root.canonicalize()
                .map_err(|error| rejected(error, self.work))?,
            None,
        ))
    }
    fn copy_path(
        &mut self,
        root: &Path,
    ) -> Result<(PathBuf, Option<NativeAllocation>), Self::Error> {
        Ok((root.to_path_buf(), None))
    }
    fn claim(&mut self, _: usize) -> Result<Option<NativeAllocation>, Self::Error> {
        Ok(None)
    }
    async fn identity(&mut self, root: &Path) -> Result<NativeRootIdentity, Self::Error> {
        admit(
            &mut self.work,
            WorkCounters {
                backend_read_operations: 1,
                source_path_components: root.components().count() as u64,
                ..WorkCounters::default()
            },
            self.budget,
        )?;
        let path = root.to_path_buf();
        tokio::task::spawn_blocking(move || HostRoot::open(&path).map(|root| root.identity()))
            .await
            .map_err(|error| {
                rejected(
                    format!("physical root validation worker failed: {error}"),
                    self.work,
                )
            })?
            .map_err(|error| rejected(error, self.work))
    }
    async fn validate_live(
        &mut self,
        shared: &SharedPhysicalRoot,
        root: &Path,
    ) -> Result<Option<(SharedRootObservation, NativeAllocation)>, Self::Error> {
        self.check()?;
        admit(
            &mut self.work,
            WorkCounters {
                backend_read_operations: 1,
                source_path_components: root.components().count() as u64,
                ..WorkCounters::default()
            },
            self.budget,
        )?;
        shared
            .validate_path_identity(root.to_path_buf())
            .await
            .map(|()| None)
            .map_err(|error| rejected(error, self.work))
    }
    fn settle_pending(
        &mut self,
        registration: &mut SharedRootRegistration,
        root: &Path,
    ) -> Result<(), Self::Error> {
        if registration.pending_raw_watcher.is_some()
            || registration.pending_watcher.is_some()
            || registration.pending_source.is_some()
        {
            return Err(self.rejection(PENDING_CLEANUP, root));
        }
        Ok(())
    }
    fn open_watcher(
        &mut self,
        root: &Path,
        pending: &mut Option<NativeWatch>,
    ) -> Result<(), Self::Error> {
        let remaining = self
            .work
            .remaining(self.budget)
            .map_err(|error| rejected(error, self.work))?;
        let mut watcher = combine(
            &mut self.work,
            NativeWatch::open_with_profile_measured(
                root,
                native_filesystem_profile(),
                NativeWatchOptions {
                    limits: VolumeLimits::default(),
                    maximum_queued_changes: 65_536,
                    recursive: !cfg!(target_os = "linux"),
                },
                remaining,
                self.cancellation,
            ),
        )?;
        watcher
            .exclude_subtrees(&reserved_root_paths().map_err(|error| rejected(error, self.work))?)
            .map_err(|error| rejected(error, self.work))?;
        let remaining = self
            .work
            .remaining(self.budget)
            .map_err(|error| rejected(error, self.work))?;
        combine(
            &mut self.work,
            watcher.accept_lazy_baseline_measured(remaining, self.cancellation),
        )?;
        *pending = Some(watcher);
        Ok(())
    }
    fn share_watcher(
        &mut self,
        pending: &mut Option<NativeWatch>,
    ) -> Result<Arc<Mutex<NativeWatch>>, Self::Error> {
        let watcher = pending.take().ok_or_else(|| {
            rejected(
                "physical root admission lost its original watcher",
                self.work,
            )
        })?;
        Ok(Arc::new(Mutex::new(watcher)))
    }
    fn prepare_watcher(&mut self, _: &mut NativeWatch) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn open_source(
        &mut self,
        root: &Path,
        reference: SourceReference,
        watcher: Arc<Mutex<NativeWatch>>,
    ) -> Result<Arc<LocalLazySource>, Self::Error> {
        let remaining = self
            .work
            .remaining(self.budget)
            .map_err(|error| rejected(error, self.work))?;
        let source = combine(
            &mut self.work,
            NativeDemandSource::open(
                std::borrow::Cow::Owned(root.to_path_buf()),
                native_filesystem_profile(),
                VolumeLimits::default(),
                Some(reference),
                Some(watcher as Arc<dyn DemandDirectoryObserver>),
                remaining,
                self.cancellation,
            )
            .await,
        )?;
        Ok(Arc::new(FilteredDemandSource::new(
            source,
            reserved_root_paths().map_err(|error| rejected(error, self.work))?,
        )))
    }
    fn invalidate(&mut self, source: &LocalLazySource) -> Result<(), Self::Error> {
        source
            .inner()
            .invalidate(&mut acyclic_fs::demand::DemandWork::new(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            ))
            .map_err(|error| rejected(error, self.work))?;
        Ok(())
    }
    fn rejection(&self, rejection: (ErrorCode, &'static str), root: &Path) -> Self::Error {
        rejected(format!("{}: {}", rejection.1, root.display()), self.work)
    }
}

pub(crate) async fn acquire(
    registry: &SharedRootRegistry,
    root: &Path,
    admission: SharedRootAdmission,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> MeasuredResult<OperationReceipt<Arc<SharedPhysicalRoot>>, String> {
    let mut instrumentation = OrdinaryRootWork {
        work: WorkCounters::default(),
        budget,
        cancellation,
    };
    let value = acquire_engine(registry, root, admission, &mut instrumentation).await?;
    Ok(OperationReceipt {
        value,
        work: instrumentation.work,
    })
}

pub(super) fn native_work_error(cause: acyclic_fs::NativeTreeMaterializationError) -> RootError {
    RootError::NativeWork {
        cause,
        custody: None,
    }
}

pub(super) fn native_source_error(source: acyclic_fs::DemandError) -> RootError {
    match source {
        acyclic_fs::DemandError::Work(cause) => {
            native_work_error(acyclic_fs::NativeTreeMaterializationError::Work(cause))
        }
        acyclic_fs::DemandError::Cancelled => {
            native_work_error(acyclic_fs::NativeTreeMaterializationError::Cancelled)
        }
        source => RootError::NativeSource {
            source: acyclic_fs::LazyWorkspaceError::Demand(source),
            custody: None,
        },
    }
}

pub(super) fn native_watch_error(source: acyclic_fs::NativeWatchError) -> RootError {
    match source {
        acyclic_fs::NativeWatchError::Work(cause) => {
            native_work_error(acyclic_fs::NativeTreeMaterializationError::Work(cause))
        }
        acyclic_fs::NativeWatchError::Cancelled(_) => {
            native_work_error(acyclic_fs::NativeTreeMaterializationError::Cancelled)
        }
        acyclic_fs::NativeWatchError::NativeWork(cause) => native_work_error(cause),
        source => native_source_error(acyclic_fs::DemandError::Observation(source)),
    }
}

/// Constructs the same private subtree selection without a formatted portable path or a second demand meter.
fn native_reserved_paths(
    work: &NativeMaterializationWork,
) -> RootResult<(Vec<NamespacePath>, NativeAllocation)> {
    use acyclic_fs::kernel::{LogicalName, NameEncoding};
    let limits = VolumeLimits::default();
    let mut custody = {
        let claim_bytes = (RESERVED_ROOT_NAMES.len() * std::mem::size_of::<NamespacePath>()) as u64;
        work.claim(
            claim_bytes,
            u64::from(claim_bytes != 0),
            acyclic_fs::WorkCounters::UNCHARGED,
        )
    }
    .map_err(native_work_error)?;
    let mut paths = Vec::new();
    paths
        .try_reserve_exact(RESERVED_ROOT_NAMES.len())
        .map_err(|cause| {
            native_work_error(acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause))
        })?;
    for text in RESERVED_ROOT_NAMES {
        #[cfg(windows)]
        let (encoding, length) = (NameEncoding::WindowsUtf16Le, text.len() * 2);
        #[cfg(unix)]
        let (encoding, length) = (NameEncoding::PosixBytes, text.len());
        #[cfg(not(any(unix, windows)))]
        let (encoding, length) = (NameEncoding::Utf8, text.len());
        work.charge(WorkCounters {
            source_path_components: 1,
            items_examined: text.len() as u64,
            ..WorkCounters::default()
        })
        .map_err(native_work_error)?;
        custody.grow(length as u64).map_err(native_work_error)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(|cause| {
            native_work_error(acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause))
        })?;
        let copied = WorkCounters {
            bytes_copied: length as u64,
            ..WorkCounters::default()
        };
        work.admit(copied).map_err(native_work_error)?;
        #[cfg(windows)]
        for byte in text.bytes() {
            bytes.extend_from_slice(&u16::from(byte).to_le_bytes());
        }
        #[cfg(not(windows))]
        bytes.extend_from_slice(text.as_bytes());
        work.spent(copied).map_err(native_work_error)?;
        let name =
            LogicalName::new(encoding, bytes, limits.maximum_component_bytes).map_err(|_| {
                RootError::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Invalid,
                    reason: "SDK private subtree name is not a valid native component",
                }
            })?;
        custody
            .grow(std::mem::size_of::<LogicalName>() as u64)
            .map_err(native_work_error)?;
        let mut names = Vec::new();
        names.try_reserve_exact(1).map_err(|cause| {
            native_work_error(acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause))
        })?;
        names.push(name);
        paths.push(
            NamespacePath::new(names, limits).map_err(|_| RootError::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "SDK private subtree selection is not a valid native path",
            })?,
        );
    }
    Ok((paths, custody))
}

struct NativeRootWork<'a, G> {
    work: &'a NativeMaterializationWork,
    guard: G,
    expected: Option<NativeRootIdentity>,
}

impl<F, G> RootInstrumentation for NativeRootWork<'_, G>
where
    G: FnMut() -> F + Send,
    F: Future<Output = RootResult<NativeRootIdentity>> + Send,
{
    type Error = RootError;

    fn check(&mut self) -> RootResult<()> {
        self.work
            .admit(WorkCounters::default())
            .map_err(native_work_error)
    }
    async fn authenticate(
        &mut self,
        _: &Path,
        _: SharedRootAdmission,
    ) -> RootResult<Option<NativeRootIdentity>> {
        self.check()?;
        let expected = (self.guard)().await?;
        self.check()?;
        if self.expected.is_some_and(|original| original != expected) {
            return Err(self.rejection(ROOT_IDENTITY, Path::new("")));
        }
        self.expected = Some(expected);
        Ok(Some(expected))
    }
    fn canonical(&mut self, root: &Path) -> RootResult<(PathBuf, Option<NativeAllocation>)> {
        if !root.is_absolute()
            || root.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::CurDir | std::path::Component::ParentDir,
                )
            })
        {
            return Err(RootError::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Invalid,
                reason: "native root acquisition requires the original canonical creator path",
            });
        }
        let expected = self
            .expected
            .ok_or_else(|| self.rejection(ROOT_IDENTITY, root))?;
        let actual = self.work.root_identity(root).map_err(native_work_error)?;
        if actual != expected {
            return Err(native_work_error(
                acyclic_fs::NativeTreeMaterializationError::AllocationChanged { expected, actual },
            ));
        }
        self.copy_path(root)
    }
    fn copy_path(&mut self, root: &Path) -> RootResult<(PathBuf, Option<NativeAllocation>)> {
        self.work
            .path(root, None)
            .map(|(path, custody)| (path, Some(custody)))
            .map_err(native_work_error)
    }
    fn claim(&mut self, bytes: usize) -> RootResult<Option<NativeAllocation>> {
        self.work
            .claim(
                bytes as u64,
                u64::from((bytes as u64) != 0),
                acyclic_fs::WorkCounters::UNCHARGED,
            )
            .map(Some)
            .map_err(native_work_error)
    }
    async fn identity(&mut self, root: &Path) -> RootResult<NativeRootIdentity> {
        self.work.root_identity(root).map_err(native_work_error)
    }
    async fn validate_live(
        &mut self,
        shared: &SharedPhysicalRoot,
        root: &Path,
    ) -> RootResult<Option<(SharedRootObservation, NativeAllocation)>> {
        let expected = self
            .expected
            .ok_or_else(|| self.rejection(ROOT_IDENTITY, root))?;
        // Ordinary-created SDK source backing cannot be silently promoted.
        shared
            .source
            .inner()
            .validate_root(
                root,
                expected,
                &mut acyclic_fs::demand::NativeSourceWork::from_native_materialization_work(
                    self.work,
                ),
            )
            .map_err(native_source_error)?;
        shared
            .watcher
            .lock()
            .map_err(|_| self.rejection(POISONED_WATCHER, root))?
            .verify_root_identity(expected, self.work)
            .map_err(native_watch_error)?;
        shared.observe_native_transaction(self.work).map(Some)
    }
    fn settle_pending(
        &mut self,
        registration: &mut SharedRootRegistration,
        root: &Path,
    ) -> RootResult<()> {
        if !shutdown_pending(registration, self.work)? {
            return Err(self.rejection(PENDING_CLEANUP, root));
        }
        Ok(())
    }
    fn open_watcher(&mut self, root: &Path, pending: &mut Option<NativeWatch>) -> RootResult<()> {
        match NativeWatch::open_with_profile_with_native_work(
            root,
            native_filesystem_profile(),
            NativeWatchOptions {
                limits: VolumeLimits::default(),
                maximum_queued_changes: 65_536,
                recursive: !cfg!(target_os = "linux"),
            },
            self.work,
        ) {
            Ok(watch) => {
                *pending = Some(watch);
                Ok(())
            }
            Err(acyclic_fs::NativeWatchError::StartupFailed { cause, watch }) => {
                *pending = Some(watch);
                Err(native_work_error(cause))
            }
            Err(acyclic_fs::NativeWatchError::SourceStartupFailed { cause, watch }) => {
                *pending = Some(watch);
                Err(native_watch_error(
                    acyclic_fs::NativeWatchError::SourceStartup(cause),
                ))
            }
            Err(cause) => Err(native_watch_error(cause)),
        }
    }
    fn share_watcher(
        &mut self,
        pending: &mut Option<NativeWatch>,
    ) -> RootResult<Arc<Mutex<NativeWatch>>> {
        let mut ownership = self
            .work
            .claim(
                std::mem::size_of::<Mutex<NativeWatch>>() as u64,
                u64::from((std::mem::size_of::<Mutex<NativeWatch>>() as u64) != 0),
                acyclic_fs::WorkCounters::UNCHARGED,
            )
            .map_err(native_work_error)?;
        let watcher = pending.as_mut().ok_or(RootError::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "native root admission lost its original raw watcher",
        })?;
        watcher
            .retain_owner_allocation(&mut ownership)
            .map_err(native_work_error)?;
        let watcher = pending.take().ok_or(RootError::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "native root admission lost its original raw watcher",
        })?;
        Ok(Arc::new(Mutex::new(watcher)))
    }
    fn prepare_watcher(&mut self, watcher: &mut NativeWatch) -> RootResult<()> {
        let excluded = native_reserved_paths(self.work)?;
        let result = watcher
            .exclude_subtrees_with_native_work(&excluded.0, self.work)
            .map_err(native_watch_error);
        drop(excluded.0);
        drop(excluded.1);
        result?;
        watcher
            .accept_lazy_baseline_with_native_work(self.work)
            .map_err(native_watch_error)?;
        Ok(())
    }
    async fn open_source(
        &mut self,
        root: &Path,
        reference: SourceReference,
        watcher: Arc<Mutex<NativeWatch>>,
    ) -> RootResult<Arc<LocalLazySource>> {
        let source = NativeDemandSource::open(
            std::borrow::Cow::Borrowed(root),
            native_filesystem_profile(),
            VolumeLimits::default(),
            Some(reference),
            Some(watcher as Arc<dyn DemandDirectoryObserver>),
            self.work,
            self.work.cancellation(),
        )
        .await
        .map_err(native_source_error)?;
        let excluded = native_reserved_paths(self.work)?;
        let mut source =
            FilteredDemandSource::new_with_native_work(source, excluded.0, excluded.1, self.work)
                .map_err(native_work_error)?;
        let mut owner = self
            .work
            .claim(
                std::mem::size_of::<LocalLazySource>() as u64,
                u64::from((std::mem::size_of::<LocalLazySource>() as u64) != 0),
                acyclic_fs::WorkCounters::UNCHARGED,
            )
            .map_err(native_work_error)?;
        source
            .retain_owner_allocation(&mut owner)
            .map_err(native_work_error)?;
        Ok(Arc::new(source))
    }
    fn invalidate(&mut self, source: &LocalLazySource) -> RootResult<()> {
        source
            .inner()
            .invalidate(
                &mut acyclic_fs::demand::NativeSourceWork::from_native_materialization_work(
                    self.work,
                ),
            )
            .map_err(native_source_error)?;
        Ok(())
    }
    fn rejection(&self, (code, reason): (ErrorCode, &'static str), _: &Path) -> RootError {
        RootError::NativeBoundary { code, reason }
    }
}

fn shutdown_pending(
    registration: &mut SharedRootRegistration,
    work: &NativeMaterializationWork,
) -> RootResult<bool> {
    if let Some(watcher) = registration.pending_raw_watcher.as_mut()
        && !watcher
            .shutdown_with_native_work(work)
            .map_err(native_watch_error)?
    {
        return Ok(false);
    }
    if let Some(watcher) = &registration.pending_watcher {
        let settled = watcher
            .lock()
            .map_err(|_| RootError::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Storage,
                reason: "original pending root watcher state is poisoned",
            })?
            .shutdown_with_native_work(work)
            .map_err(native_watch_error)?;
        if !settled {
            return Ok(false);
        }
    } else if registration.pending_source.is_some() {
        return Err(RootError::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "original pending root source lost its watcher owner",
        });
    }
    drop(registration.pending_source.take());
    drop(registration.pending_watcher.take());
    drop(registration.pending_raw_watcher.take());
    Ok(true)
}

pub(crate) async fn acquire_with_native_work<F, G>(
    registry: &SharedRootRegistry,
    root: &Path,
    admission: SharedRootAdmission,
    work: &NativeMaterializationWork,
    guard: G,
) -> RootResult<Arc<SharedPhysicalRoot>>
where
    G: FnMut() -> F + Send,
    F: Future<Output = RootResult<NativeRootIdentity>> + Send,
{
    let mut instrumentation = NativeRootWork {
        work,
        guard,
        expected: None,
    };
    acquire_engine(registry, root, admission, &mut instrumentation).await
}

pub(crate) async fn shutdown_pending_with_native_work<F, G>(
    registry: &SharedRootRegistry,
    root: &Path,
    work: &NativeMaterializationWork,
    mut guard: G,
) -> RootResult<bool>
where
    G: FnMut() -> F + Send,
    F: Future<Output = RootResult<NativeRootIdentity>> + Send,
{
    work.admit(WorkCounters::default())
        .map_err(native_work_error)?;
    let expected = guard().await?;
    let registration = {
        let roots = registry.roots.lock().await;
        roots.get(root).cloned()
    };
    let Some(registration) = registration else {
        return Ok(true);
    };
    if guard().await? != expected {
        return Err(RootError::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "pending root cleanup changed its original owner allocation",
        });
    }
    let mut registration = registration.lock().await;
    if guard().await? != expected {
        return Err(RootError::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "pending root cleanup changed its original owner allocation",
        });
    }
    if registration
        .native_identity
        .is_some_and(|identity| identity != expected.to_bytes())
    {
        return Err(RootError::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "pending root cleanup selected another original held allocation",
        });
    }
    shutdown_pending(&mut registration, work)
}
pub(crate) fn budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 4096,
        authority_records_appended: 64,
        authority_bytes_read: 8 * 1024 * 1024,
        authority_bytes_written: 1024 * 1024,
        object_probes: 4096,
        backend_read_operations: 4096,
        backend_write_operations: 4096,
        durability_operations: 4096,
        page_reads: 4096,
        page_writes: 4096,
        object_bytes_read: 8 * 1024 * 1024,
        object_bytes_written: 8 * 1024 * 1024,
        bytes_hashed: 8 * 1024 * 1024,
        bytes_copied: 8 * 1024 * 1024,
        bytes_encoded: 8 * 1024 * 1024,
        source_bytes_read: 8 * 1024 * 1024,
        source_path_components: 4096,
        source_entries_visited: 4096,
        output_bytes: 8 * 1024 * 1024,
        items_examined: 4096,
        items_returned: 4096,
        allocation_operations: 4096,
        peak_allocation_bytes: 8 * 1024 * 1024,
        materializations: 64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_or_cancelled_root_allowance_rejects_before_native_access() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("never-opened");
        let registry = SharedRootRegistry::default();
        let cancellation = CancellationToken::new();
        let failure = registry
            .acquire(
                &missing,
                SharedRootAdmission::Fresh,
                WorkBudget::default(),
                &cancellation,
            )
            .await
            .err()
            .expect("no root probe admitted");
        assert_eq!(*failure.work, WorkCounters::default());
        assert!(registry.roots.lock().await.is_empty());
        cancellation.cancel();
        let failure = registry
            .acquire(
                &missing,
                SharedRootAdmission::Fresh,
                budget(),
                &cancellation,
            )
            .await
            .err()
            .expect("cancelled root probe");
        assert_eq!(*failure.work, WorkCounters::default());
        assert!(failure.error.contains("cancel"));
        assert!(!missing.exists());
    }

    #[tokio::test]
    async fn later_root_admission_failure_preserves_earlier_native_probe_work() {
        let directory = tempfile::tempdir().unwrap();
        let registry = SharedRootRegistry::default();
        let mut allowance = budget();
        allowance.backend_read_operations = 1;
        let failure = registry
            .acquire(
                directory.path(),
                SharedRootAdmission::Fresh,
                allowance,
                &CancellationToken::new(),
            )
            .await
            .err()
            .expect("watcher cannot reset the exhausted shared allowance");
        assert_eq!(failure.work.backend_read_operations, 1);
        assert!(failure.work.source_path_components > 0);
        assert_eq!(registry.live_roots().await, 0);
    }

    fn close_native_watch(
        physical: &SharedPhysicalRoot,
        work: &NativeMaterializationWork,
    ) -> RootResult<bool> {
        // The SDK observer ABI uses std::sync::Mutex and exposes poisoning as
        // a real typed failure; do not erase it with an immediate lock unwrap.
        physical
            .watcher
            .lock()
            .map_err(|_| RootError::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Storage,
                reason: "fixture original watcher state is poisoned",
            })?
            .shutdown_with_native_work(work)
            .map_err(native_watch_error)
    }

    fn selection(
        fixture: &crate::pi_fork::tests::Fixture,
    ) -> (&Path, SourceReference, NativeRootIdentity) {
        let crate::pi_root::PiRootIntent::Project { paths, .. } = &fixture.root_request.root else {
            panic!("actual project creator fixture");
        };
        let (key, path) = paths.first_key_value().unwrap();
        let created = &fixture.root_allocation.roots[key];
        (
            Path::new(path),
            SourceReference {
                identity: created.source_identity,
                epoch: created.source_epoch,
            },
            NativeRootIdentity::from_bytes(created.native_root_identity),
        )
    }

    async fn original_root_guard(
        fixture: &crate::pi_fork::tests::Fixture,
        work: &NativeMaterializationWork,
    ) -> RootResult<NativeRootIdentity> {
        let task = fixture.root_request.binding.task_id;
        if fixture.context.durable_task_id() != Some(task) || fixture.journal.task_id() != task {
            return Err(RootError::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "fixture source changed its original admitted owner context",
            });
        }
        let (key, project) = fixture
            .root_request
            .binding
            .project_volumes
            .first_key_value()
            .unwrap();
        fixture
            .context
            .verify_original_volume_read_with_native_work(&fixture.journal, project, work)
            .await?;
        fixture
            .journal
            .verify(
                false,
                &mut Some(
                    &mut acyclic_harness::store::native_work_io(work).map_err(|cause| {
                        acyclic_harness::Error::NativeWork {
                            cause,
                            custody: None,
                        }
                    })?,
                ),
            )
            .await?;
        // Fixture::new obtained this protected result by observing the real
        // original Root effect and its actual Filesystem creator resources.
        let created = &fixture.root_allocation.roots[key];
        if created.repository_workspace_id
            != fixture.root_handle.workspace.workspace().id().into_bytes()
        {
            return Err(RootError::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "fixture source differs from its protected original creator",
            });
        }
        Ok(NativeRootIdentity::from_bytes(created.native_root_identity))
    }

    #[tokio::test]
    async fn native_root_resume_reuses_and_reopens_the_same_registered_source() {
        let fixture = crate::pi_fork::tests::Fixture::new(None).await;
        let (path, original, expected) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let work = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let before = work.receipt();
        let first = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &work, || {
                original_root_guard(&fixture, &work)
            })
            .await
            .unwrap();
        assert_eq!(first.source.inner().root_identity(), expected);
        assert_eq!(first.source.reference().identity, original.identity);
        assert!(first.source.reference().epoch > original.epoch);
        assert!(work.receipt().backend_read_operations > before.backend_read_operations);
        let query = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let warm = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &query, || {
                original_root_guard(&fixture, &query)
            })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first, &warm));
        let retained_epoch = warm.source.reference().epoch;
        assert!(close_native_watch(&warm, &query).unwrap());
        drop(warm);
        drop(first);
        assert_eq!(registry.live_roots().await, 0);
        let reopened = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &query, || {
                original_root_guard(&fixture, &query)
            })
            .await
            .unwrap();
        assert_eq!(reopened.source.inner().root_identity(), expected);
        assert_eq!(reopened.source.reference().identity, original.identity);
        assert!(reopened.source.reference().epoch > retained_epoch);
        assert!(close_native_watch(&reopened, &query).unwrap());
    }

    #[tokio::test]
    async fn native_source_backing_outlives_the_returned_physical_root() {
        let fixture = crate::pi_fork::tests::Fixture::new(None).await;
        let (path, original, expected) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let work = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let physical = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &work, || {
                original_root_guard(&fixture, &work)
            })
            .await
            .unwrap();
        let source = Arc::clone(&physical.source);
        assert!(close_native_watch(&physical, &work).unwrap());
        drop(physical);
        let held = work.live_allocation_bytes();
        assert_eq!(source.inner().root_identity(), expected);
        assert_eq!(source.reference().identity, original.identity);
        drop(source);
        assert!(work.live_allocation_bytes() < held);
        registry.prune().await;
        assert!(registry.roots.lock().await.is_empty());
    }

    #[tokio::test]
    async fn native_root_cancellation_and_quota_preserve_the_original_prefix() {
        let fixture = crate::pi_fork::tests::Fixture::new(None).await;
        let (path, original, _) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation).unwrap();
        let before = work.receipt();
        cancellation.cancel();
        let error = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &work, || {
                original_root_guard(&fixture, &work)
            })
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            RootError::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            }
        ));
        assert_eq!(work.receipt(), before);
        assert!(registry.roots.lock().await.is_empty());
        let mut denied = budget();
        denied.backend_read_operations = 0;
        let work = NativeMaterializationWork::new(denied, &CancellationToken::new()).unwrap();
        assert!(
            registry
                .acquire_with_native_work(
                    path,
                    SharedRootAdmission::Resume(original),
                    &work,
                    || original_root_guard(&fixture, &work)
                )
                .await
                .is_err()
        );
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert!(work.receipt().allocation_operations > 0);
        assert!(registry.roots.lock().await.is_empty());
        assert_eq!(
            std::fs::read(path.join("observed.txt")).unwrap(),
            b"original"
        );
    }

    #[tokio::test]
    async fn a_source_reference_never_substitutes_for_the_original_root_read_grant() {
        use acyclic_harness::conversation::{VolumeClass, VolumeOperation, VolumeOwner, VolumeRef};
        let fixture = crate::pi_fork::tests::Fixture::new(None).await;
        let (path, original, expected) = selection(&fixture);
        let project = fixture
            .root_request
            .binding
            .project_volumes
            .values()
            .next()
            .unwrap();
        let foreign = VolumeRef::new(
            project.provider().clone(),
            "not-original-source".into(),
            VolumeClass::Project,
            VolumeOwner::Project("not-original-root".into()),
            &mut None,
        )
        .unwrap();
        let registry = SharedRootRegistry::default();
        let work = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let before = work.receipt().backend_read_operations;
        let error = registry
            .acquire_with_native_work(
                path,
                SharedRootAdmission::Resume(original),
                &work,
                || async {
                    fixture.journal.require_volume_grant(
                        &foreign,
                        VolumeOperation::Read,
                        &mut Some(&mut acyclic_harness::store::native_work_io(&work).map_err(
                            |cause| acyclic_harness::Error::NativeWork {
                                cause,
                                custody: None,
                            },
                        )?),
                    )?;
                    Ok(expected)
                },
            )
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), acyclic_harness::wire::ErrorCode::Unauthorized);
        assert_eq!(work.receipt().backend_read_operations, before);
        assert!(registry.roots.lock().await.is_empty());
    }

    #[tokio::test]
    async fn native_resume_rejects_registered_source_identity_and_root_replacement() {
        let fixture = crate::pi_fork::tests::Fixture::new(None).await;
        let (path, original, expected) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let work = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let physical = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &work, || {
                original_root_guard(&fixture, &work)
            })
            .await
            .unwrap();
        let other = SourceReference {
            identity: [79; 16],
            epoch: original.epoch,
        };
        let error = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(other), &work, || {
                original_root_guard(&fixture, &work)
            })
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), acyclic_harness::wire::ErrorCode::Conflict);
        assert_eq!(physical.source.reference().identity, original.identity);
        std::fs::rename(path, path.with_file_name("original-native-root")).unwrap();
        std::fs::create_dir(path).unwrap();
        let error = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &work, || {
                original_root_guard(&fixture, &work)
            })
            .await
            .err()
            .unwrap();
        assert!(matches!(error, RootError::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::AllocationChanged { expected: old, actual },
            ..
        } if old == expected && actual != expected));
        assert_eq!(physical.source.inner().root_identity(), expected);
        assert_eq!(physical.source.reference().identity, original.identity);
        assert!(close_native_watch(&physical, &work).unwrap());
    }

    #[tokio::test]
    async fn failed_native_admission_retains_sdk_owners_until_actual_pending_shutdown() {
        let fixture = crate::pi_fork::tests::Fixture::new(None).await;
        let (path, original, _) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation).unwrap();
        let mut boundaries = 0;
        let error = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &work, || {
                boundaries += 1;
                if boundaries == 3 {
                    cancellation.cancel();
                }
                original_root_guard(&fixture, &work)
            })
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            RootError::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            }
        ));
        let prefix = work.receipt();
        assert!(prefix.backend_read_operations > 0);
        assert!(prefix.allocation_operations > 1);
        registry.prune().await;
        let slot = registry.roots.lock().await.get(path).cloned().unwrap();
        assert!(slot.lock().await.pending_watcher.is_some());
        assert_eq!(registry.live_roots().await, 0);
        let cleanup = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        assert!(
            registry
                .shutdown_pending_with_native_work(path, &cleanup, || original_root_guard(
                    &fixture, &cleanup
                ))
                .await
                .unwrap()
        );
        let state = slot.lock().await;
        assert!(state.pending_raw_watcher.is_none());
        assert!(state.pending_watcher.is_none());
        assert!(state.pending_source.is_none());
        drop(state);
        assert!(work.receipt().backend_read_operations >= prefix.backend_read_operations);
        assert!(work.receipt().allocation_operations >= prefix.allocation_operations);
        drop(slot);
        registry.prune().await;
        assert!(registry.roots.lock().await.is_empty());
    }
    #[tokio::test]
    async fn native_observation_publishes_the_same_source_with_real_batch_custody() {
        let fixture = crate::pi_fork::tests::Fixture::new(None).await;
        let (path, original, _) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let birth = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let physical = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &birth, || {
                original_root_guard(&fixture, &birth)
            })
            .await
            .unwrap();
        let prior = physical.source.reference();
        std::fs::write(path.join("observed.txt"), b"changed after native resume").unwrap();
        let query = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let (observation, allocation) = physical
            .observe_with_native_work(&query, || async {
                original_root_guard(&fixture, &query).await.map(|_| ())
            })
            .await
            .unwrap();
        assert!(observation.consumed_changes);
        assert_eq!(observation.prior_source, prior);
        assert_eq!(observation.source.identity, original.identity);
        assert!(observation.source.epoch > prior.epoch);
        assert_eq!(physical.source.reference(), observation.source);
        if let WatchBatch::Changes { changes, .. } = &observation.batch {
            assert!(!changes.is_empty());
            assert_eq!(
                allocation.bytes(),
                (changes.capacity() * std::mem::size_of::<acyclic_fs::WatchChange>()) as u64
            );
        }
        assert!(query.receipt().backend_read_operations > 0);
        let held = query.live_allocation_bytes();
        let owned = allocation.bytes();
        drop(observation);
        let remaining = query.live_allocation_bytes();
        assert!(remaining >= owned && remaining <= held);
        drop(allocation);
        assert_eq!(query.live_allocation_bytes(), remaining - owned);
        assert!(close_native_watch(&physical, &birth).unwrap());
    }

    #[tokio::test]
    async fn native_observation_denial_does_not_consume_pending_watch_changes() {
        let fixture = crate::pi_fork::tests::Fixture::new(None).await;
        let (path, original, _) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let birth = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let physical = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &birth, || {
                original_root_guard(&fixture, &birth)
            })
            .await
            .unwrap();
        let prior = physical.source.reference();
        std::fs::write(
            path.join("observed.txt"),
            b"still pending after denied observation",
        )
        .unwrap();
        let cancellation = CancellationToken::new();
        let cancelled = NativeMaterializationWork::new(budget(), &cancellation).unwrap();
        let prefix = cancelled.receipt();
        cancellation.cancel();
        let mut authorized = false;
        let error = physical
            .observe_with_native_work(&cancelled, || {
                authorized = true;
                async { original_root_guard(&fixture, &cancelled).await.map(|_| ()) }
            })
            .await
            .err()
            .unwrap();
        assert!(matches!(
            error,
            RootError::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            }
        ));
        assert!(!authorized);
        assert_eq!(cancelled.receipt(), prefix);
        assert_eq!(physical.source.reference(), prior);
        let mut denied_allowance = budget();
        denied_allowance.backend_read_operations = 0;
        let denied =
            NativeMaterializationWork::new(denied_allowance, &CancellationToken::new()).unwrap();
        assert!(
            physical
                .observe_with_native_work(&denied, || async {
                    original_root_guard(&fixture, &denied).await.map(|_| ())
                })
                .await
                .is_err()
        );
        assert_eq!(denied.receipt().backend_read_operations, 0);
        assert_eq!(physical.source.reference(), prior);
        let query = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let observed = physical
            .observe_with_native_work(&query, || async {
                original_root_guard(&fixture, &query).await.map(|_| ())
            })
            .await
            .unwrap();
        assert!(observed.0.consumed_changes);
        assert_eq!(observed.0.prior_source, prior);
        assert!(observed.0.source.epoch > prior.epoch);
        drop(observed);
        assert!(close_native_watch(&physical, &birth).unwrap());
    }

    #[tokio::test]
    async fn native_observation_rejected_after_io_keeps_actual_source_epoch_consistent() {
        let fixture = crate::pi_fork::tests::Fixture::new(None).await;
        let (path, original, _) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let birth = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let physical = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &birth, || {
                original_root_guard(&fixture, &birth)
            })
            .await
            .unwrap();
        let prior = physical.source.reference();
        std::fs::write(
            path.join("observed.txt"),
            b"published before final owner proof",
        )
        .unwrap();
        let query = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let mut boundaries = 0;
        let error = physical
            .observe_with_native_work(&query, || {
                boundaries += 1;
                let reject = boundaries == 2;
                let proof = original_root_guard(&fixture, &query);
                async move {
                    proof.await?;
                    if reject {
                        Err(RootError::NativeBoundary {
                            code: acyclic_harness::wire::ErrorCode::Unauthorized,
                            reason: "fixture original source owner withdrew publication",
                        })
                    } else {
                        Ok(())
                    }
                }
            })
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), acyclic_harness::wire::ErrorCode::Unauthorized);
        assert_eq!(boundaries, 2);
        assert_eq!(physical.source.reference().identity, original.identity);
        assert!(physical.source.reference().epoch > prior.epoch);
        let stored = physical
            .observe_transaction(
                || RootError::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Storage,
                    reason: "fixture original source reference is poisoned",
                },
                |source, reference| {
                    assert_eq!(*reference, source);
                    Ok(source)
                },
            )
            .unwrap();
        assert_eq!(stored, physical.source.reference());
        assert_eq!(query.live_allocation_bytes(), 0);
        assert!(close_native_watch(&physical, &birth).unwrap());
    }
    #[tokio::test]
    async fn native_source_invalidation_failure_preserves_a_real_required_rescan() {
        fn cancelled_source_publication(
            physical: &SharedPhysicalRoot,
            work: &NativeMaterializationWork,
            cancellation: &CancellationToken,
        ) -> RootResult<()> {
            let mut watcher = physical
                .watcher
                .lock()
                .map_err(|_| RootError::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Storage,
                    reason: "fixture original watcher state is poisoned",
                })?;
            watcher
                .fence_with_native_work(WATCH_FENCE_TIMEOUT, work)
                .map_err(native_watch_error)?;
            let observed = watcher.poll_with_native_work_guarded(65_536, work, |batch| {
                assert!(!matches!(batch, acyclic_fs::WatchBatch::Changes { changes, .. } if changes.is_empty()));
                cancellation.cancel();
                // Fail in the genuine native source invalidation, not an
                // invented error or a facade around the numeric poll API.
                physical.source.inner().invalidate(&mut acyclic_fs::demand::NativeSourceWork::from_native_materialization_work(work)).map(|_| ())
            }).map_err(native_watch_error)?;
            drop(observed);
            Ok(())
        }

        let fixture = crate::pi_fork::tests::Fixture::new(None).await;
        let (path, original, _) = selection(&fixture);
        let registry = SharedRootRegistry::default();
        let birth = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let physical = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &birth, || {
                original_root_guard(&fixture, &birth)
            })
            .await
            .unwrap();
        let prior = physical.source.reference();
        std::fs::write(
            path.join("observed.txt"),
            b"hint must not disappear on native failure",
        )
        .unwrap();
        let cancellation = CancellationToken::new();
        let failed = NativeMaterializationWork::new(budget(), &cancellation).unwrap();
        original_root_guard(&fixture, &failed).await.unwrap();
        let error = cancelled_source_publication(&physical, &failed, &cancellation)
            .err()
            .unwrap();
        assert!(matches!(
            error,
            RootError::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                ..
            }
        ));
        let prefix = failed.receipt();
        assert!(prefix.backend_read_operations > 0);
        assert_eq!(physical.source.reference(), prior);
        assert_eq!(failed.live_allocation_bytes(), 0);
        // This is a separate legitimate acquisition, never uncancelling or
        // resetting the exhausted original operation. Warm lazy restore must
        // execute the same real required-rescan transaction before exposing
        // this resident source, even without an explicit observation caller.
        let query = NativeMaterializationWork::new(budget(), &CancellationToken::new()).unwrap();
        let warm = registry
            .acquire_with_native_work(path, SharedRootAdmission::Resume(original), &query, || {
                original_root_guard(&fixture, &query)
            })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&warm, &physical));
        assert_eq!(warm.source.reference().identity, original.identity);
        assert!(warm.source.reference().epoch > prior.epoch);
        assert_eq!(failed.receipt(), prefix);
        drop(warm);
        assert!(close_native_watch(&physical, &birth).unwrap());
    }
}
