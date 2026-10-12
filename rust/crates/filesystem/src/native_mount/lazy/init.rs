use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
/// Composes lazy reads and the authored checkout on the supplied carrier.
    pub(in crate::native_mount) fn new(
        lazy: Arc<LazyWorkspace<A, O, D, S>>,
        authored: Arc<CheckoutMountSource<A, O>>,
        root: String,
        mut carrier: crate::native_mount::adapter::NativeDemandCarrier<'_>,
        allocations: Option<(crate::NativeAllocation, &mut crate::NativeAllocation)>,
    ) -> Result<Self, crate::native_mount::NativeMountError> {
        let original = carrier.original_native_work();
        if let Some(original) = &original {
            original.admit(crate::WorkCounters::UNCHARGED)
                .map_err(|error| crate::native_mount::NativeMountError::Demand(
                    DemandError::Mount(MountSourceError::NativeWork(error))))?;
        }
        let valid = match (&original, &allocations) {
            (Some(original), Some((root, source))) =>
                root.work().shares_ledger(original) && source.work().shares_ledger(original)
                && root.work().shares_resource_allowance(original)
                && source.work().shares_resource_allowance(original)
                && root.bytes().checked_add(source.bytes()).is_some(),
            (None, None) => true,
            _ => false,
        };
        if !valid {
            return Err(crate::native_mount::NativeMountError::Demand(DemandError::Mount(
                MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout))));
        }
        let (root_allocation, source_allocation) = allocations
            .map_or((None, None), |(root, source)| (Some(root), Some(source)));
        carrier.with_work(|work| work.check()).map_err(crate::native_mount::NativeMountError::Demand)?;
        // Exact source names cannot share a case-folding authored namespace.
        if authored.folds_names() {
            return Err(crate::native_mount::NativeMountError::ProfileUnavailable {
                profile: "case-folding", platform: "a lazy source projection",
            });
        }
        let runtime = CallbackRuntime::existing()?;
        let cancellation = carrier.cancellation();
        let mut retained = 0_u64;
        let result = (|| {
            macro_rules! owned_arc {
                ($value:expr) => {{
                    let value = $value;
                    if let Some(original) = &original {
                        original.claim(0, 1, crate::WorkCounters::UNCHARGED).map_err(|error|
                            crate::native_mount::NativeMountError::Demand(DemandError::Mount(MountSourceError::NativeWork(error))))?;
                    } else {
                        let bytes = std::mem::size_of_val(&value) as u64;
                        carrier.with_work(|work| work.allocate(bytes)).map_err(crate::native_mount::NativeMountError::Demand)?;
                        retained += bytes;
                    }
                    Arc::new(value)
                }}
            }
            let runtime = owned_arc!(runtime);
            
            let source_view = owned_arc!(SourceViewGate::new());
            let detached = owned_arc!(Mutex::new(DetachedTable::default()));
            let open_sources = owned_arc!(Mutex::new(BTreeMap::new()));
            let sink_bytes = if original.is_some() { 0 } else {
                std::mem::size_of::<SourceChangeRecorder<A, O, D, S>>() as u64
            };
            let sink = owned_arc!(SourceChangeRecorder {
                lazy: Arc::downgrade(&lazy), authored: Arc::downgrade(&authored),
                constructor_inputs: Mutex::new(Some((Arc::clone(&lazy), root, root_allocation))),
            });
            let custody = if original.is_some() { None } else {
                Some(carrier.with_work(crate::native_mount::NativeMountAllocationCustody::new)?)
            };
            let mut owner = NativeMountSourceWatchOwner {
                watch: None, sink: Some(Arc::clone(&sink) as Arc<dyn SourceChangeSink>),
                startup_failure: None, custody,
                bytes: 0,
            };
            if let Some(custody) = &owner.custody {
                carrier.with_work(|work| custody.retain_admitted(sink_bytes, work))?;
                owner.bytes = sink_bytes;
                retained -= sink_bytes;
            }
            if let Some(original) = &original {
                let sink = Arc::clone(owner.sink.as_ref().expect("preallocated source sink"));
                let watched = lazy.watch_source_with_native_work(sink,
                    crate::demand::NativeSourceWork::from_native_materialization_work(original));
                match watched {
                    Ok(watch) => {
                        owner.watch = watch;

                        let checked = crate::native_host::NativeHostWork::check(
                            &mut crate::demand::NativeSourceWork::from_native_materialization_work(original));
                        if let Err(cause) = checked {
                            if owner.watch.is_some() {
                                return Err(crate::native_mount::NativeMountError::SourceWatchStartupFailed { cause, owner });
                            }
                            return Err(crate::native_mount::NativeMountError::Demand(cause));
                        }
                    }
                    Err(LazyWorkspaceError::Demand(DemandError::SourceWatchStartup(failure))) => {

                        owner.startup_failure = Some(Arc::clone(&failure));
                        return Err(crate::native_mount::NativeMountError::SourceWatchStartupFailed {
                            cause: DemandError::SourceWatchStartup(failure), owner,
                        });
                    }
                    Err(LazyWorkspaceError::Demand(error)) =>
                        return Err(crate::native_mount::NativeMountError::Demand(error)),
                    Err(error) => return Err(crate::native_mount::NativeMountError::Source(lazy_error(error))),
                }
            } else {
                let prepared = carrier.with_work(|work| work.prepare_watch_custody())
                    .map_err(crate::native_mount::NativeMountError::Demand)?;
                let remaining = carrier.with_work(|work| work.remaining()
                    .map(|budget| (budget, work.live_allocation_bytes())));
                let (budget, prior_live) = match remaining {
                    Ok(remaining) => remaining,
                    Err(error) => {
                        carrier.with_work(|work| work.abandon_prepared_watch_custody(prepared));
                        return Err(crate::native_mount::NativeMountError::Demand(error));
                    }
                };
                let watched = lazy.watch_source_measured(
                    Arc::clone(owner.sink.as_ref().expect("preallocated source sink")), budget, &cancellation);
                match watched {
                    Ok(receipt) => {
                        owner.watch = receipt.value;

                        let merged = carrier.with_work(|work| work.merge_with_prior_live(receipt.work, prior_live));
                        if let Some(watch) = owner.watch.as_ref() {
                            let bytes = watch.logical_owned_bytes();
                            let adopted = carrier.with_work(|work| work.record_adoption(bytes, &None));
                            let retained_watch = carrier.with_work(|work|
                                owner.custody.as_ref().expect("ordinary receipt custody")
                                    .retain_completed_admitted(bytes, work)).map_err(DemandError::from);
                            if retained_watch.is_ok() {
                                owner.bytes = owner.bytes.checked_add(bytes)
                                    .expect("admitted watch custody has a representable physical byte total");
                            }
                            let backing = match watch.allocation_custody() {
                                Some(token) => carrier.with_work(|work| work.retain_prepared_watch_custody(prepared, &token)),
                                None => {
                                    carrier.with_work(|work| work.abandon_prepared_watch_custody(prepared));
                                    Ok(())
                                }
                            };
                            let completed = merged.and(adopted).and(retained_watch)
                                .and(backing).and(carrier.with_work(|work| work.check()));
                            if let Err(cause) = completed {
                                return Err(crate::native_mount::NativeMountError::SourceWatchStartupFailed { cause, owner });
                            }
                        } else {
                            carrier.with_work(|work| work.abandon_prepared_watch_custody(prepared));
                            merged.map_err(crate::native_mount::NativeMountError::Demand)?;
                        }
                    }
                    Err(failure) => {
                        carrier.with_work(|work| work.abandon_prepared_watch_custody(prepared));
                        let _ = carrier.with_work(|work| work.merge_with_prior_live(*failure.work, prior_live));
                        return match failure.error {
                            LazyWorkspaceError::Demand(DemandError::SourceWatchStartup(failure)) => {

                                owner.startup_failure = Some(Arc::clone(&failure));
                                Err(crate::native_mount::NativeMountError::SourceWatchStartupFailed {
                                    cause: DemandError::SourceWatchStartup(failure), owner,
                                })
                            }
                            error => Err(crate::native_mount::NativeMountError::Source(lazy_error(error))),
                        };
                    }
                }
            }
            let (retained_lazy, root, native_allocation) =
                sink.constructor_inputs.lock().unwrap_or_else(PoisonError::into_inner)
                    .take().expect("constructor retains its exact owned inputs");
            drop(retained_lazy);
            let source_watch = if owner.watch.is_some() { Some(owner) } else { drop(owner); None };
            Ok(Self {
                lazy, authored, root,
                runtime,
                resolutions: Resolutions::default(),
                source_absent: SourceAbsentDirectories::default(),
                cursors: CursorTable::new(MAXIMUM_LAZY_DIRECTORY_CURSORS),
                source_view,
                removals: Mutex::new(MountedRemovals::default()),
                detached,
                open_sources,
                source_watch,
                linked: Mutex::default(),
                native_allocation,
            })
        })();
        match result {
            Ok(mut source) => {
                if let Some(source_allocation) = source_allocation {
                    source.native_allocation.as_mut().expect("owned constructor root guard")
                        .absorb(source_allocation).expect("prechecked exact same-ledger storage transfer");
                }
                Ok(source)
            }
            Err(error) => {
                let _ = carrier.with_work(|work| work.release(retained));
                Err(error)
            }
        }
    }

pub(crate) fn physically_retired(&self) -> Result<bool, MountSourceError> {
        Ok(self.authored.capture_retired()
            && self.cursors.physically_empty()?
            && self.source_watch.as_ref().is_none_or(|watch| watch.is_stopped()))
    }

pub(crate) fn cancel_with_cleanup_wait(
        &self, wait: &crate::CancellationToken,
    ) -> Result<(), MountSourceError> {
        self.cancel_with_cleanup(Some(wait))
    }

pub(crate) fn cancel_accepted_cleanup(&self) -> Result<(), MountSourceError> {
        self.cancel_with_cleanup(None)
    }

pub(in crate::native_mount) fn cancel_with_cleanup(
        &self, wait: Option<&crate::CancellationToken>,
    ) -> Result<(), MountSourceError> {
        if self.physically_retired()? { return Ok(()); }
        self.authored.cancel_cleanup(wait)?;
        self.cursors.clear_with_cleanup(wait)?;
        if let Some(watch) = &self.source_watch { watch.stop_with_cleanup(wait)?; }
        Ok(())
    }

pub(crate) fn native_source_io_lease(
        self: &Arc<Self>,
    ) -> Result<crate::native_mount::adapter::NativeSourceIoLease, MountSourceError> {
        let owner: Arc<dyn crate::native_mount::adapter::NativeSourceIoRetirement> =
            Arc::clone(self) as Arc<dyn crate::native_mount::adapter::NativeSourceIoRetirement>;
        self.authored.native_source_io_lease_for_owner(owner)
    }

pub(super) fn cancel(
        &self, carrier: &mut crate::native_mount::adapter::NativeDemandCarrier<'_>,
    ) -> Result<(), MountSourceError> {
        if let Some(original) = carrier.original_native_work() {
            return self.cancel_with_cleanup_wait(original.cancellation());
        }
        self.authored.cancel_with_carrier(carrier)?;
        self.cursors.retire(carrier)?;
        let Some(watch) = self.source_watch.as_ref() else { return Ok(()); };
        watch.stop(carrier).map(|_| ())
    }

pub(in crate::native_mount) fn wait<T: Send, F>(&self, create: impl FnOnce() -> F + Send) -> Result<T, MountSourceError>
    where
        F: std::future::Future<Output = Result<T, MountSourceError>>,
    {
        self.runtime.wait(create)
    }
}
