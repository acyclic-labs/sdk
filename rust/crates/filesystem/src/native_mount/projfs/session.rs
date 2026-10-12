use super::*;

// SAFETY: the projection loop drains before its owner stops the SDK context.
unsafe impl Send for SendContext {}

pub(in crate::native_mount) fn has_pending_capture(
    failure: &Mutex<PostOperationFailures>,
    path: &MountPath,
) -> bool {
    let failure = lock_recover(failure);
    failure.unreplayable.is_some()
        || failure.pending_captures.iter().any(|(pending, capture)| {
            pending == path || capture.subtree && projfs_path_suffix(path, pending).is_some()
        })
}

pub(in crate::native_mount) struct ProjFsSession {
    pub(in crate::native_mount) controller: Option<Arc<Projector>>,
    pub(in crate::native_mount) context: Option<PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT>,
    pub(in crate::native_mount) runtime: Option<Arc<Runtime>>,
    pub(in crate::native_mount) retirement: Option<super::super::NativeMountRetirement>,
    pub(in crate::native_mount) original_stop_work: crate::NativeMaterializationWork,
    pub(in crate::native_mount) sdk_stop_credit: Option<crate::NativeWorkReservation>,
    pub(in crate::native_mount) worker_close_credit: Option<crate::NativeWorkReservation>,
    pub(in crate::native_mount) namespace_outcome: Option<ProjFsNamespaceOutcome>,
    pub(in crate::native_mount) qualification: Option<ProjFsNamespaceQualification>,
    pub(in crate::native_mount) capture_failure: Option<NativeMountError>,
    pub(in crate::native_mount) capture_attempted: bool,
}

// SAFETY: ProjFS contexts are opaque handles. The provider synchronizes callback
unsafe impl Send for ProjFsSession {}
// SAFETY: mutation of the opaque SDK token requires exclusive session access.
unsafe impl Sync for ProjFsSession {}

impl ProjFsSession {
    pub(in crate::native_mount) fn start<F>(
        request: &NativeMountRequest,
        source: Arc<dyn MountFilesystem>,
        work: &crate::NativeMaterializationWork,
        out: &mut Option<Self>,
        retirement: &mut Option<super::super::NativeMountRetirement>,
        prepare: F,
    ) -> Result<(), NativeMountError>
    where
        F: FnOnce(&mut super::super::NativeMountRetirement) -> Result<(), NativeMountError>,
    {
        if out.is_some() {
            return Err(NativeMountError::ControlPending);
        }
        let original_work = source.native_original_work()?;
        let callback_guard = source.native_callback_guard();
        if callback_guard.is_some() && original_work.is_none()
            || original_work
                .as_ref()
                .is_some_and(|original| !same_work(original, work))
        {
            return Err(NativeMountError::Lifecycle(
                "source invocation differs from startup",
            ));
        }
        let physical = work.allocation_work();
        let stop = physical.reserve_work(crate::WorkCounters {
            backend_write_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let worker_close = physical.reserve_work(crate::WorkCounters {
            backend_write_operations: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let (root, mut allocation) = work.path(&request.destination, None)?;
        let bytes = size_of::<Runtime>()
            + size_of::<HostRoot>()
            + size_of::<Projector>()
            + size_of::<Mutex<HashMap<u128, OpenMetadataState>>>()
            + size_of::<Mutex<ProjectionCache>>()
            + size_of::<Mutex<HashMap<MountPath, usize>>>()
            + size_of::<FileLocks>()
            + size_of::<Mutex<PostOperationFailures>>()
            + 7 * 2 * size_of::<usize>()
            + 64 * size_of::<Mutex<()>>();
        let mut state_allocation = work.claim(bytes as u64, 8, crate::WorkCounters::UNCHARGED)?;
        allocation.absorb(&mut state_allocation)?;
        let metadata_root = Arc::new(HostRoot::open(&root, &mut { work })?);
        let warm = retirement.as_ref().and_then(|packet| packet.warm_namespace);
        if warm.is_none() {
            reject_stale_projection(&root)?;
        }
        if warm.is_some_and(|warm| warm.root_identity != metadata_root.identity()) {
            return Err(NativeMountError::InvalidDestination);
        }
        let runtime = Arc::new(Runtime {
            source,
            work: Mutex::new(work.clone()),
            active_work: Mutex::new(Default::default()),
            original_work: Mutex::new(original_work),
            callback_guard,
            root,
            metadata_root,
            staging: Mutex::new(None),
            writable: request.writable,
            metadata_baselines: Arc::new(Mutex::new(HashMap::new())),
            projection: Arc::new(Mutex::new(ProjectionCache::default())),
            projector: Arc::new(Projector::new()),
            metadata_probes: Arc::new(Mutex::new(HashMap::new())),
            metadata_edits: Arc::new(FileLocks::default()),
            post_operation_failure: Arc::new(Mutex::new(PostOperationFailures::default())),
            callbacks: CallbackGate::start()?,
            _allocation: allocation,
        });
        *out = Some(Self {
            controller: None,
            context: None,
            runtime: Some(Arc::clone(&runtime)),
            retirement: None,
            original_stop_work: work.clone(),
            sdk_stop_credit: Some(stop),
            worker_close_credit: Some(worker_close),
            namespace_outcome: None,
            qualification: None,
            capture_failure: None,
            capture_attempted: false,
        });
        runtime.projector.attach(Arc::clone(&runtime))?;
        out.as_mut().expect("installed startup owner").controller =
            Some(Arc::clone(&runtime.projector));
        let observer: Arc<dyn ViewObserver> = runtime.projector.clone();
        runtime
            .source
            .observe_view_with_meter(Arc::downgrade(&observer), work)?;
        if let Some(packet) = retirement.as_mut() {
            prepare(packet)?;
        }
        let session = out
            .as_mut()
            .expect("startup owner installed before SDK effects");
        session.retirement = retirement.take();
        let guid = warm.map_or_else(
            || guid_from_key(u128::from_le_bytes(request.mount_id.into_bytes())),
            |warm| warm.instance_id,
        );
        let root_name = HSTRING::from(runtime.root.as_os_str());
        let mut mapping = PRJ_NOTIFICATION_MAPPING {
            NotificationBitMask: FILE_NOTIFICATIONS,
            NotificationRoot: PCWSTR::from_raw(NOTIFICATION_ROOT.as_ptr()),
        };
        let mut options = PRJ_STARTVIRTUALIZING_OPTIONS {
            Flags: PRJ_FLAG_USE_NEGATIVE_PATH_CACHE,
            ..Default::default()
        };
        if request.writable {
            options.NotificationMappings = &raw mut mapping;
            options.NotificationMappingsCount = 1;
        }
        if warm.is_none() {
            native_call(work, true, || {
                // SAFETY: the authenticated empty root and GUID remain resident.
                unsafe {
                    PrjMarkDirectoryAsPlaceholder(&root_name, PCWSTR::null(), None, &raw const guid)
                }
                .map_err(|error| std::io::Error::from_raw_os_error(error.code().0))
            })?;
        }
        let callbacks = callbacks();
        let context = native_call(work, true, || {
            // SAFETY: this Arc allocation is stable until SDK stop drains callbacks.
            unsafe {
                PrjStartVirtualizing(
                    &root_name,
                    &raw const callbacks,
                    Some(Arc::as_ptr(&runtime).cast::<c_void>()),
                    Some(&raw const options),
                )
            }
            .map_err(|error| std::io::Error::from_raw_os_error(error.code().0))
        })?;
        session.context = Some(context);
        let mut instance =
            windows::Win32::Storage::ProjectedFileSystem::PRJ_VIRTUALIZATION_INSTANCE_INFO::default(
            );
        native_call(work, false, || {
            // SAFETY: the newly started context and output slot remain resident.
            unsafe {
                windows::Win32::Storage::ProjectedFileSystem::PrjGetVirtualizationInstanceInfo(
                    context,
                    &raw mut instance,
                )
            }
            .map_err(|error| std::io::Error::from_raw_os_error(error.code().0))
        })?;
        if warm.is_some_and(|warm| warm.instance_id != instance.InstanceID) {
            return Err(NativeMountError::InvalidDestination);
        }
        session.qualification = Some(ProjFsNamespaceQualification {
            root_identity: runtime.metadata_root.identity(),
            instance_id: instance.InstanceID,
        });
        lock_recover(&runtime.projector.lifecycle).context = Some(SendContext(context));
        runtime.projector.lifecycle_changed.notify_all();
        runtime
            .projector
            .materialize(
                Projection {
                    runtime: runtime.as_ref(),
                    work,
                    context,
                    starting: true,
                },
                MountPath::root(),
            )
            .map_err(NativeMountError::Driver)?;
        runtime.projector.settle()
    }

    pub(in crate::native_mount) fn stop_physical(&mut self) -> Result<(), NativeMountError> {
        let Some(runtime) = self.runtime.as_ref().cloned() else {
            if let Some(retirement) = self.retirement.as_mut() {
                retirement.retire(None)?;
                if !retirement.is_retired() {
                    return Err(NativeMountError::ControlPending);
                }
            }
            self.retirement = None;
            return Ok(());
        };
        runtime
            .callbacks
            .drain()
            .ok_or(NativeMountError::ControlPending)?;
        if runtime.writable && self.context.is_some() && !self.capture_attempted {
            self.capture_failure = self.flush_callbacks(&self.original_stop_work).err();
            self.capture_attempted = true;
        }
        let metadata = self.original_stop_work.allocation_work();
        self.original_stop_work = metadata.clone();
        *lock_recover(&runtime.work) = metadata;
        let mut installed = lock_recover(&runtime.original_work);
        *installed = installed
            .as_ref()
            .map(crate::NativeMaterializationWork::allocation_work);
        drop(installed);
        if let Some(retirement) = self.retirement.as_mut() {
            retirement.retire_source_and_watch_with_cleanup(None)?;
        }
        if let Some(context) = self.context {
            let credit = self
                .sdk_stop_credit
                .as_mut()
                .ok_or(NativeMountError::ControlPending)?;
            credit.loan(None).started_io(crate::WorkCounters {
                backend_write_operations: 1,
                ..crate::WorkCounters::UNCHARGED
            })?;
            // SAFETY: the projection loop has drained; SDK stop drains every callback.
            unsafe { PrjStopVirtualizing(context) };
            self.context = None;
            runtime
                .callbacks
                .drain()
                .ok_or(NativeMountError::ControlPending)?;
        }
        let cleanup_failure =
            lock_recover(&runtime.staging)
                .take()
                .and_then(|(directory, mut credit)| {
                    let result = delete_through(&directory, || {
                        credit
                            .loan(None)
                            .started_io(crate::WorkCounters {
                                backend_write_operations: 1,
                                ..crate::WorkCounters::UNCHARGED
                            })
                            .map_err(NativeMountError::from)
                    });
                    credit.started_drop_io(crate::WorkCounters {
                        backend_write_operations: 1,
                        ..crate::WorkCounters::UNCHARGED
                    });
                    drop(directory);
                    result.err()
                });
        let state = std::mem::take(&mut *lock_recover(&runtime.projector.state));
        self.namespace_outcome = Some(ProjFsNamespaceOutcome {
            capture_failure: self.capture_failure.take(),
            cleanup_failure,
            qualification: self.qualification,
            state,
            callbacks: Arc::clone(&runtime.post_operation_failure),
        });
        self.runtime = None;
        drop(runtime);
        if let Some(retirement) = self.retirement.as_mut() {
            retirement.retire(None)?;
            if !retirement.is_retired() {
                return Err(NativeMountError::ControlPending);
            }
        }
        self.retirement = None;
        Ok(())
    }

    pub(in crate::native_mount) fn native_root_identity(
        &self,
    ) -> Option<crate::NativeRootIdentity> {
        self.runtime
            .as_ref()
            .map(|runtime| runtime.metadata_root.identity())
    }

    pub(in crate::native_mount) fn begin_original_work(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<NativeMountWorkScope, NativeMountError> {
        let (runtime, _) = self.live()?;
        NativeMountWorkScope::enter(Arc::clone(runtime), work)
    }

    pub(in crate::native_mount) fn retirement_ownership(&self) -> (bool, bool) {
        if self.runtime.is_none()
            && let Some(projector) = self.controller.as_ref()
        {
            return lock_recover(&projector.lifecycle).custody;
        }
        self.retirement
            .as_ref()
            .map_or((false, false), |retirement| {
                (
                    retirement.destination_guard.is_some(),
                    retirement.retained_source.is_some()
                        || retirement.startup_source_watch.is_some(),
                )
            })
    }

    pub(in crate::native_mount) fn namespace_detach_confirmed(&self) -> bool {
        if let Some(projector) = self.controller.as_ref() {
            return lock_recover(&projector.lifecycle)
                .owner
                .as_ref()
                .is_some_and(Self::namespace_detach_confirmed);
        }
        self.context.is_none() && self.runtime.is_none()
    }

    pub(in crate::native_mount) fn take_namespace_outcome(
        &mut self,
    ) -> Option<ProjFsNamespaceOutcome> {
        if let Some(projector) = self.controller.as_ref() {
            return lock_recover(&projector.lifecycle)
                .owner
                .as_mut()
                .and_then(Self::take_namespace_outcome);
        }
        self.namespace_outcome.take()
    }

    pub(in crate::native_mount) fn take_callback_failure(
        &self,
    ) -> Option<(NativeMountError, crate::WorkCounters)> {
        if let Some(projector) = self.controller.as_ref() {
            return lock_recover(&projector.lifecycle)
                .owner
                .as_ref()
                .and_then(Self::take_callback_failure);
        }
        let failures = self
            .runtime
            .as_ref()
            .map(|runtime| &runtime.post_operation_failure)
            .or_else(|| {
                self.namespace_outcome
                    .as_ref()
                    .map(|outcome| &outcome.callbacks)
            })?;
        lock_recover(failures).callback_failure.take()
    }

    pub(in crate::native_mount) fn completed_callback_work(&self) -> crate::WorkCounters {
        if let Some(projector) = self.controller.as_ref() {
            return lock_recover(&projector.lifecycle).owner.as_ref().map_or(
                crate::WorkCounters::UNCHARGED,
                Self::completed_callback_work,
            );
        }
        self.runtime
            .as_ref()
            .map(|runtime| &runtime.post_operation_failure)
            .or_else(|| {
                self.namespace_outcome
                    .as_ref()
                    .map(|outcome| &outcome.callbacks)
            })
            .map_or(crate::WorkCounters::UNCHARGED, |failures| {
                lock_recover(failures).completed_callback_work
            })
    }

    pub(in crate::native_mount) fn flush_callbacks(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or(NativeMountError::ControlPending)?;
        let source = runtime.source.as_ref();
        flush_callback_gate(
            &runtime.callbacks,
            Arc::clone(&runtime.post_operation_failure),
            |paths| {
                let mut exact = Vec::new();
                for (path, subtree) in paths {
                    let _guard = MetadataProbeGuard::enter(&runtime.metadata_probes, path);
                    let present = match runtime
                        .metadata_root
                        .stat(&host_relative_path(path)?, &mut &*work)
                    {
                        Ok(_) => {
                            capture_missing_host_ancestors(runtime, path, work)?;
                            true
                        }
                        Err(crate::NativeTreeMaterializationError::Io(error))
                            if error.kind() == std::io::ErrorKind::NotFound =>
                        {
                            false
                        }
                        Err(error) => return Err(error.into()),
                    };
                    let directory = match source.lookup_with_meter(path, work) {
                        Ok(Some(lookup)) => lookup.node.kind == MountNodeKind::Directory,
                        Ok(None) | Err(MountSourceError::NotFound) if !present => continue,
                        Ok(None) | Err(MountSourceError::NotFound) => false,
                        Err(error) => return Err(error),
                    };
                    if *subtree || !present && directory {
                        source.capture_host_subtree_with_meter(
                            &runtime.root,
                            path,
                            runtime.metadata_root.identity(),
                            work,
                        )?;
                    } else {
                        exact.push(path.clone());
                    }
                }
                let _guards = exact
                    .iter()
                    .map(|path| MetadataProbeGuard::enter(&runtime.metadata_probes, path))
                    .collect::<Vec<_>>();
                source.capture_host_paths_with_meter(
                    &runtime.root,
                    &exact,
                    runtime.metadata_root.identity(),
                    work,
                )
            },
        )
    }

    pub(in crate::native_mount) fn invalidate(
        &self,
        path: &[u8],
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let (runtime, context) = self.live()?;
        let _scope = self.begin_original_work(work)?;
        let text = std::str::from_utf8(path)
            .map_err(|_| NativeMountError::Driver("invalidation path is not UTF-8".to_owned()))?;
        let relative = text
            .split('/')
            .filter(|component| !component.is_empty())
            .collect::<PathBuf>();
        lock_recover(runtime.projection.as_ref()).clear();
        if relative.as_os_str().is_empty() {
            return Ok(());
        }
        let relative = HSTRING::from(relative.as_os_str());
        let mount_path = path_from(PCWSTR::from_raw(relative.as_ptr())).ok_or_else(|| {
            NativeMountError::Driver("invalidation path is not a mount path".to_owned())
        })?;
        // SAFETY: the relative UTF-16 name remains live for this synchronous
        match unsafe { PrjDeleteFile(context, &relative, Some(PRJ_UPDATE_ALLOW_READ_ONLY), None) } {
            Err(error)
                if ![
                    HR_FILE_NOT_FOUND,
                    HR_PATH_NOT_FOUND,
                    HR_VIRTUALIZATION_INVALID_OPERATION,
                ]
                .contains(&error.code()) =>
            {
                Err(driver_error(&error))
            }
            _ => {
                runtime.projector.invalidate(&mount_path);
                Ok(())
            }
        }
    }

    pub(in crate::native_mount) fn revalidate(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let (runtime, _context) = self.live()?;
        let _scope = self.begin_original_work(work)?;
        if runtime.source.view_stamp_with_meter(work)?.is_none() {
            runtime.projector.invalidate_all();
        }
        runtime
            .source
            .fence_changes_with_meter(work)
            .map_err(|error| NativeMountError::Driver(error.to_string()))?;
        runtime.projector.settle()
    }

    pub(in crate::native_mount) fn live(
        &self,
    ) -> Result<(&Arc<Runtime>, PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT), NativeMountError> {
        self.runtime
            .as_ref()
            .zip(self.context)
            .ok_or_else(|| NativeMountError::Driver("ProjFS session is stopped".to_owned()))
    }
}

pub(super) fn source_hresult(error: &MountSourceError) -> HRESULT {
    match error.semantic_error() {
        MountSourceError::NotFound => HR_FILE_NOT_FOUND,
        MountSourceError::AlreadyExists => HR_ALREADY_EXISTS,
        MountSourceError::Invalid(_) => HR_INVALID_DATA,
        MountSourceError::Unsupported(_) => HR_NOT_SUPPORTED,
        MountSourceError::Engine(_) => HR_IO_DEVICE,
        MountSourceError::Stale | MountSourceError::Cancelled => HR_FILE_INVALID,
        MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled) => {
            HR_FILE_INVALID
        }
        MountSourceError::Io(error, _) => {
            HRESULT::from_win32(error.raw_os_error().unwrap_or(1117) as u32)
        }
        _ => HR_IO_DEVICE,
    }
}

pub(super) fn native_hresult(error: &NativeMountError) -> HRESULT {
    match error {
        NativeMountError::Source(error) => source_hresult(error),
        NativeMountError::Io(error) => {
            HRESULT::from_win32(error.raw_os_error().unwrap_or(1117) as u32)
        }
        NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled) => {
            HR_FILE_INVALID
        }
        _ => HR_IO_DEVICE,
    }
}
