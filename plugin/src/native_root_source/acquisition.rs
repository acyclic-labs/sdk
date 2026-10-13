//! Shared physical-root acquisition using the SDK's original work object.
use super::*;

fn rejection((code, reason): (ErrorCode, &'static str)) -> RootError {
    RootError::NativeBoundary { code, reason }
}

async fn authenticate<F, G>(
    work: &NativeMaterializationWork,
    guard: &mut G,
    expected: NativeRootIdentity,
) -> RootResult<()>
where
    G: FnMut() -> F + Send,
    F: Future<Output = RootResult<NativeRootIdentity>> + Send,
{
    work.remaining()?;
    if guard().await? != expected {
        return Err(rejection(ROOT_IDENTITY));
    }
    work.remaining()?;
    Ok(())
}

pub(crate) async fn acquire(
    registry: &SharedRootRegistry,
    root: &Path,
    admission: SharedRootAdmission,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> MeasuredResult<OperationReceipt<Arc<SharedPhysicalRoot>>, String> {
    // Ordinary callers retain main's canonicalization boundary.
    cancellation
        .check()
        .map_err(|error| OperationFailure::new(error.to_string(), WorkCounters::default()))?;
    let work = NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(error, work)| OperationFailure::new(error.to_string(), work))?;
    let result = async {
        let read = WorkCounters {
            backend_read_operations: 1,
            source_path_components: root.components().count() as u64,
            ..WorkCounters::default()
        };
        work.admit(read)?;
        let canonical = root.canonicalize();
        work.spent(read)?;
        let canonical = canonical.map_err(acyclic_fs::NativeTreeMaterializationError::Io)?;
        acquire_with_native_work(registry, &canonical, admission, &work, || async {
            work.root_identity(&canonical).map_err(native_work_error)
        })
        .await
    }
    .await;
    let receipt = work.receipt();
    result
        .map(|value| OperationReceipt {
            value,
            work: receipt,
        })
        .map_err(|error| OperationFailure::new(error.to_string(), receipt))
}

pub(crate) async fn acquire_with_native_work<F, G>(
    registry: &SharedRootRegistry,
    root: &Path,
    admission: SharedRootAdmission,
    work: &NativeMaterializationWork,
    mut guard: G,
) -> RootResult<Arc<SharedPhysicalRoot>>
where
    G: FnMut() -> F + Send,
    F: Future<Output = RootResult<NativeRootIdentity>> + Send,
{
    work.remaining()?;
    let expected = guard().await?;
    work.remaining()?;
    if !root.is_absolute()
        || root.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        return Err(rejection((
            ErrorCode::Invalid,
            "native root acquisition requires the original canonical creator path",
        )));
    }
    let actual = work.root_identity(root)?;
    if actual != expected {
        return Err(native_work_error(
            acyclic_fs::NativeTreeMaterializationError::AllocationChanged { expected, actual },
        ));
    }
    let registration = {
        let mut roots = registry.roots.lock().await;
        work.remaining()?;
        if let Some(registration) = roots.get(root) {
            Arc::clone(registration)
        } else {
            let (key, key_custody) = work.copy_path(root)?;
            let slot_custody = work.claim(
                std::mem::size_of::<SharedRootSlot>() as u64,
                1,
                WorkCounters::UNCHARGED,
            )?;
            let slot = Arc::new(SharedRootSlot {
                state: AsyncMutex::new(SharedRootRegistration::default()),
                _native_custody: [Some(key_custody), Some(slot_custody)],
            });
            roots.insert(key, Arc::clone(&slot));
            slot
        }
    };
    let mut registration = registration.state.lock().await;
    authenticate(work, &mut guard, expected).await?;
    let mut retained = registration
        .reference
        .as_ref()
        .map(|reference| {
            reference
                .value
                .lock()
                .map(|reference| *reference)
                .map_err(|_| rejection(POISONED_REFERENCE))
        })
        .transpose()?;
    if admission.reference().is_some_and(|requested| {
        retained.is_some_and(|retained| requested.identity != retained.identity)
    }) {
        return Err(rejection(SOURCE_IDENTITY));
    }
    if registration
        .native_identity
        .is_some_and(|identity| identity != expected.to_bytes())
        && (registration.live.strong_count() != 0
            || matches!(admission, SharedRootAdmission::Resume(_)))
    {
        return Err(rejection(ROOT_IDENTITY));
    }
    if !shutdown_pending(&mut registration, work)? {
        return Err(rejection(PENDING_CLEANUP));
    }
    if let Some(identity) = registration.native_identity {
        let actual = work.root_identity(root)?;
        authenticate(work, &mut guard, expected).await?;
        let live = registration.live.upgrade();
        if actual.to_bytes() != identity {
            if live.is_some() || matches!(admission, SharedRootAdmission::Resume(_)) {
                return Err(rejection(ROOT_IDENTITY));
            }
            *registration = SharedRootRegistration::default();
            retained = None;
        } else {
            let retained = retained.ok_or_else(|| rejection(MISSING_REFERENCE))?;
            if let Some(shared) = live {
                shared
                    .source
                    .inner()
                    .validate_root(
                        root,
                        expected,
                        &mut acyclic_fs::demand::NativeSourceWork::from_native_materialization_work(
                            work,
                        ),
                    )
                    .map_err(native_source_error)?;
                shared
                    .watcher
                    .lock()
                    .map_err(|_| rejection(POISONED_WATCHER))?
                    .verify_root_identity(expected, work)
                    .map_err(native_watch_error)?;
                let observed = shared.observe_native_transaction(work)?;
                authenticate(work, &mut guard, expected).await?;
                if shared.source.reference().identity != retained.identity {
                    return Err(rejection(SOURCE_IDENTITY));
                }
                drop(observed);
                return Ok(shared);
            }
        }
    }
    let reference = retained
        .or(admission.reference())
        .unwrap_or_else(|| SourceReference {
            identity: uuid::Uuid::new_v4().into_bytes(),
            epoch: 0,
        });
    if registration
        .reference
        .as_ref()
        .is_none_or(|reference| reference._native_custody.is_none())
    {
        let custody = work.claim(
            std::mem::size_of::<SharedSourceReference>() as u64,
            1,
            WorkCounters::UNCHARGED,
        )?;
        registration.reference = Some(Arc::new(SharedSourceReference {
            value: Mutex::new(reference),
            _native_custody: Some(custody),
        }));
    }
    registration.native_identity = Some(expected.to_bytes());
    match NativeWatch::open_with_profile_with_native_work(
        root,
        native_filesystem_profile(),
        NativeWatchOptions {
            limits: VolumeLimits::default(),
            maximum_queued_changes: 65_536,
            recursive: !cfg!(target_os = "linux"),
        },
        work,
    ) {
        Ok(watch) => registration.pending_raw_watcher = Some(watch),
        Err(acyclic_fs::NativeWatchError::StartupFailed { cause, watch }) => {
            registration.pending_raw_watcher = Some(watch);
            return Err(native_work_error(cause));
        }
        Err(acyclic_fs::NativeWatchError::SourceStartupFailed { cause, watch }) => {
            registration.pending_raw_watcher = Some(watch);
            return Err(native_watch_error(
                acyclic_fs::NativeWatchError::SourceStartup(cause),
            ));
        }
        Err(error) => return Err(native_watch_error(error)),
    }
    let mut ownership = work.claim(
        std::mem::size_of::<Mutex<NativeWatch>>() as u64,
        1,
        WorkCounters::UNCHARGED,
    )?;
    registration
        .pending_raw_watcher
        .as_mut()
        .ok_or_else(|| rejection(MISSING_REFERENCE))?
        .retain_owner_allocation(&mut ownership)?;
    let watcher = Arc::new(Mutex::new(
        registration
            .pending_raw_watcher
            .take()
            .ok_or_else(|| rejection(MISSING_REFERENCE))?,
    ));
    registration.pending_watcher = Some(Arc::clone(&watcher));
    {
        let mut watcher = watcher.lock().map_err(|_| rejection(POISONED_WATCHER))?;
        if watcher.root_identity() != expected {
            return Err(rejection(ROOT_IDENTITY));
        }
        let excluded = native_reserved_paths(work)?;
        watcher
            .exclude_subtrees_with_native_work(&excluded.0, work)
            .map_err(native_watch_error)?;
        watcher
            .accept_lazy_baseline_with_native_work(work)
            .map_err(native_watch_error)?;
    }
    let source = NativeDemandSource::open(
        std::borrow::Cow::Borrowed(root),
        native_filesystem_profile(),
        VolumeLimits::default(),
        Some(reference),
        Some(Arc::clone(&watcher) as Arc<dyn DemandDirectoryObserver>),
        work,
        work.cancellation(),
    )
    .await
    .map_err(native_source_error)?;
    let excluded = native_reserved_paths(work)?;
    let mut source =
        FilteredDemandSource::new_with_native_work(source, excluded.0, excluded.1, work)?;
    let mut ownership = work.claim(
        std::mem::size_of::<LocalLazySource>() as u64,
        1,
        WorkCounters::UNCHARGED,
    )?;
    source.retain_owner_allocation(&mut ownership)?;
    let source = Arc::new(source);
    registration.pending_source = Some(Arc::clone(&source));
    authenticate(work, &mut guard, expected).await?;
    if source.inner().root_identity() != expected
        || source.reference().identity != reference.identity
    {
        return Err(rejection(ROOT_IDENTITY));
    }
    if matches!(admission, SharedRootAdmission::Resume(_)) {
        source
            .inner()
            .invalidate(
                &mut acyclic_fs::demand::NativeSourceWork::from_native_materialization_work(work),
            )
            .map_err(native_source_error)?;
    }
    let reference = Arc::clone(
        registration
            .reference
            .as_ref()
            .ok_or_else(|| rejection(MISSING_REFERENCE))?,
    );
    *reference
        .value
        .lock()
        .map_err(|_| rejection(POISONED_REFERENCE))? = source.reference();
    let custody = work.claim(
        std::mem::size_of::<SharedPhysicalRoot>() as u64,
        1,
        WorkCounters::UNCHARGED,
    )?;
    let shared = Arc::new(SharedPhysicalRoot {
        source,
        watcher,
        reference: Arc::clone(&reference),
        _native_custody: Some(custody),
    });
    *registration = SharedRootRegistration {
        live: Arc::downgrade(&shared),
        reference: Some(reference),
        native_identity: Some(expected.to_bytes()),
        ..SharedRootRegistration::default()
    };
    Ok(shared)
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

pub(crate) fn native_work_error(cause: acyclic_fs::NativeTreeMaterializationError) -> RootError {
    RootError::NativeWork {
        cause,
        custody: None,
    }
}

pub(crate) fn native_source_error(source: acyclic_fs::DemandError) -> RootError {
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

pub(crate) fn native_watch_error(source: acyclic_fs::NativeWatchError) -> RootError {
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
    let mut registration = registration.state.lock().await;
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
