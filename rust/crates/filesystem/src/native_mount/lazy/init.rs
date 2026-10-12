use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    /// Composes the source using its caller's startup context.
    pub(in crate::native_mount) fn new(
        lazy: Arc<LazyWorkspace<A, O, D, S>>,
        authored: Arc<CheckoutMountSource<A, O>>,
        root: String,
        mut carrier: crate::native_mount::adapter::NativeDemandCarrier<'_>,
        allocations: Option<(crate::NativeAllocation, &mut crate::NativeAllocation)>,
    ) -> Result<Self, NativeMountError> {
        let original = carrier.original_native_work();
        if let Some(original) = &original {
            original
                .admit(crate::WorkCounters::UNCHARGED)
                .map_err(MountSourceError::NativeWork)?;
        }
        let valid = match (&original, &allocations) {
            (Some(original), Some((root, source))) => {
                root.work().shares_ledger(original)
                    && source.work().shares_ledger(original)
                    && root.work().shares_resource_allowance(original)
                    && source.work().shares_resource_allowance(original)
                    && root.bytes().checked_add(source.bytes()).is_some()
            }
            (None, None) => true,
            _ => false,
        };
        if !valid {
            return Err(MountSourceError::NativeWork(
                crate::NativeTreeMaterializationError::InvalidLayout,
            )
            .into());
        }
        let (root_allocation, source_allocation) =
            allocations.map_or((None, None), |(root, source)| (Some(root), Some(source)));
        carrier
            .with_work(|work| work.check())
            .map_err(NativeMountError::Demand)?;
        // Exact source names cannot share a case-folding authored namespace.
        if authored.folds_names() {
            return Err(NativeMountError::ProfileUnavailable {
                profile: "case-folding",
                platform: "a lazy source projection",
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
                        original
                            .claim(0, 1, crate::WorkCounters::UNCHARGED)
                            .map_err(MountSourceError::NativeWork)?;
                    } else {
                        let bytes = std::mem::size_of_val(&value) as u64;
                        carrier
                            .with_work(|work| work.claim_allocation(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED))
                            .map_err(NativeMountError::Demand)?;
                        retained += bytes;
                    }
                    Arc::new(value)
                }};
            }
            let runtime = owned_arc!(runtime);

            let source_view = owned_arc!(ViewGate::new());
            let detached = owned_arc!(Mutex::new(BTreeMap::new()));
            let open_sources = owned_arc!(Mutex::new(BTreeMap::new()));
            let sink_bytes = if original.is_some() {
                0
            } else {
                std::mem::size_of::<SourceChangeRecorder<A, O, D, S>>() as u64
            };
            let sink = owned_arc!(SourceChangeRecorder {
                lazy: Arc::downgrade(&lazy),
                authored: Arc::downgrade(&authored),
                constructor_inputs: Mutex::new(Some((Arc::clone(&lazy), root, root_allocation))),
            });
            let custody = if original.is_some() {
                None
            } else {
                Some(carrier.with_work(crate::native_mount::NativeMountAllocationCustody::new)?)
            };
            let mut owner = NativeMountSourceWatchOwner {
                watch: None,
                sink: Some(Arc::clone(&sink) as Arc<dyn SourceChangeSink>),
                startup_failure: None,
                custody,
                bytes: 0,
            };
            if let Some(custody) = &owner.custody {
                carrier.with_work(|work| work.check().and_then(|()| custody.retain(sink_bytes, work).map_err(crate::DemandError::from)))?;
                owner.bytes = sink_bytes;
                retained -= sink_bytes;
            }
            let (budget, prior_live) = if original.is_none() {
                carrier.with_work(|work| work.remaining().map(|budget| (budget, work.live_allocation_bytes())))
                    .map_err(NativeMountError::Demand)?
            } else { (WorkBudget::UNBOUNDED, 0) };
            let mut accounting = match &original {
                Some(work) => crate::kernel::KernelAccounting::native(work, &cancellation),
                None => crate::kernel::KernelAccounting::standalone(budget, &cancellation),
            };
            let watched = lazy.watch_source(
                Arc::clone(owner.sink.as_ref().expect("preallocated source sink")),
                &mut accounting,
            );
            let watched_result = if original.is_none() {
                match crate::facade::measured(watched, &accounting) {
                    Ok(receipt) => {
                        owner.watch = receipt.value;
                        let merged = carrier
                            .with_work(|work| work.merge(receipt.work, prior_live));
                        if let Some(watch) = owner.watch.as_ref() {
                            let bytes = watch.logical_owned_bytes();
                            let adopted =
                                carrier.with_work(|work| work.record_native_owned_completed(bytes, &None).1);
                            let retained_watch = carrier
                                .with_work(|work| {
                                    owner
                                        .custody
                                        .as_ref()
                                        .expect("ordinary receipt custody")
                                        .retain(bytes, work)
                                })
                                .map_err(DemandError::from);
                            if retained_watch.is_ok() {
                                owner.bytes = owner.bytes.checked_add(bytes)
                                    .expect("admitted watch custody has a representable physical byte total");
                            }
                            let completed = merged
                                .and(adopted)
                                .and(retained_watch)
                                .and(carrier.with_work(|work| work.check()));
                            if let Err(cause) = completed {
                                return Err(NativeMountError::SourceWatchStartupFailed {
                                    cause,
                                    owner,
                                });
                            }
                        } else {

                            merged.map_err(NativeMountError::Demand)?;
                        }
                        Ok(())
                    }
                    Err(failure) => {

                        let _ = carrier.with_work(|work| {
                            work.merge(*failure.work, prior_live)
                        });
                        Err(failure.error)
                    }
                }
            } else {
                match watched {
                    Ok(watch) => {
                        owner.watch = watch;
                        if let Err(cause) = carrier.with_work(|work| work.check()) {
                            if owner.watch.is_some() {
                                return Err(NativeMountError::SourceWatchStartupFailed {
                                    cause,
                                    owner,
                                });
                            }
                            return Err(NativeMountError::Demand(cause));
                        }
                        Ok(())
                    }
                    Err(error) => Err(error),
                }
            };
            if let Err(error) = watched_result {
                return Err(match error {
                    LazyWorkspaceError::Demand(DemandError::SourceWatchStartup(failure)) => {
                        owner.startup_failure = Some(Arc::clone(&failure));
                        NativeMountError::SourceWatchStartupFailed {
                            cause: DemandError::SourceWatchStartup(failure),
                            owner,
                        }
                    }
                    LazyWorkspaceError::Demand(error) if original.is_some() => {
                        NativeMountError::Demand(error)
                    }
                    error => NativeMountError::Source(lazy_error(error)),
                });
            }
            let (retained_lazy, root, native_allocation) = sink
                .constructor_inputs
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
                .expect("constructor retains its exact owned inputs");
            drop(retained_lazy);
            let source_watch = if owner.watch.is_some() {
                Some(owner)
            } else {
                drop(owner);
                None
            };
            Ok(Self {
                lazy,
                authored,
                root,
                runtime,
                cursors: CursorTable::new(),
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
                    source
                        .native_allocation
                        .as_mut()
                        .expect("owned constructor root guard")
                        .absorb(source_allocation)
                        .expect("prechecked exact same-ledger storage transfer");
                }
                Ok(source)
            }
            Err(error) => {
                let _ = carrier.with_work(|work| work.release(retained));
                Err(error)
            }
        }
    }

    /// Cancels the authored callback adapter.
    pub fn cancel(&self) {
        self.cursors.clear();
        self.authored.cancel();
    }

    pub(crate) fn physically_retired(&self) -> Result<bool, MountSourceError> {
        Ok(self.authored.capture_retired()
            && self
                .cursors
                .entries
                .lock()
                .map_err(|_| MountSourceError::Stale)?
                .is_empty()
            && self
                .source_watch
                .as_ref()
                .is_none_or(|watch| watch.is_stopped()))
    }

    pub(crate) fn cancel_with_cleanup_wait(
        &self,
        wait: &CancellationToken,
    ) -> Result<(), MountSourceError> {
        self.cancel_cleanup(Some(wait))
    }

    pub(super) fn cancel_cleanup(
        &self,
        wait: Option<&CancellationToken>,
    ) -> Result<(), MountSourceError> {
        self.authored.cancel_cleanup(wait)?;
        if let Some(wait) = wait {
            wait.check().map_err(|_| MountSourceError::Cancelled)?;
        }
        self.cursors.clear();
        if let Some(watch) = &self.source_watch {
            watch.stop_with_cleanup(wait)?;
        }
        Ok(())
    }

    pub(crate) fn native_source_io_lease(
        self: &Arc<Self>,
    ) -> Result<adapter::NativeSourceIoLease, MountSourceError> {
        self.authored
            .native_source_io_lease_for_owner(Arc::clone(self) as Arc<dyn Send + Sync>)
    }

    /// Advances the authored checkout after an external publication.
    pub async fn advance_to_head_async(&self) -> Result<(), MountSourceError> {
        let _writer = self.source_view.write().await;
        let mut accounting = crate::kernel::KernelAccounting::standalone(
            WorkBudget::UNBOUNDED,
            self.authored.cancellation(),
        );
        if self
            .lazy
            .publication_cut(None, &mut accounting)
            .await
            .is_ok_and(|state| state.source == self.lazy.source_reference())
        {
            let result = self.authored.advance_to_head_async().await;
            self.cursors.clear();
            return result;
        }
        self.source_view.begin_transition();
        let result = async {
            self.authored.advance_to_head_async().await?;
            self.lazy
                .state(&mut accounting)
                .await
                .map_err(lazy_error)?;
            // Every unauthored node may read differently from the new epoch.
            self.authored.record_projection_change(
                &ViewChange::Everything,
                &mut adapter::NativeDemandCarrier::Borrowed(&mut crate::demand::DemandWork::new(
                    WorkBudget::UNBOUNDED,
                    self.authored.cancellation(),
                )),
                || Ok(()),
            )?;
            self.cursors.clear();
            Ok(())
        }
        .await;

        if result.is_ok() {
            self.source_view.finish_transition();
        }
        result
    }
}
